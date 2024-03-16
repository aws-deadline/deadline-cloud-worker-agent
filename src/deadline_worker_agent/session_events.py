# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
from typing import Any, TypedDict, Union
from typing_extensions import NotRequired
import json
import datetime
import re

log = logging.getLogger(__name__)


class BotoLogStatement(TypedDict):
    """
    This type is emitted as a json log for each request and response managed by boto
    This structure can be used for log searches to narrow on particular operations
    """

    log_type: str
    operation: str
    # Only in requests
    request_url: NotRequired[str]
    params: Union[dict[str, Any], str]
    # Only in responses
    error: NotRequired[str]
    status_code: NotRequired[str]
    request_id: NotRequired[str]


class Boto3ClientEvent:
    BEFORE_CALL_PREFIX = "before-call"
    AFTER_CALL_PREFIX = "after-call"


# The actual type here is recursive:
#  T = dict[str, T] | dict[str, bool]
# 1) If a key does not appear in this recursive dictionary, then it is
#    never logged.
# 2) Only those keys that have a value that is True or a dictionary are logged.
AllowableBodyKeysT = dict[str, Any]


class LoggingAllowList(TypedDict, total=False):
    # RESTful services have IDs in the url
    # set this to true to include the request URL in the logs
    log_request_url: bool
    # Parts of the request to log.
    req_log_body: AllowableBodyKeysT
    # Parts of the response to log.
    res_log_body: AllowableBodyKeysT


# The AWS API calls that we log, and the specific request/response properties that we log.
# Note: The log must NEVER contain customer personal information, or information that a customer
# might consider secret/sensitive.
LOGGING_ALLOW_LIST: dict[str, LoggingAllowList] = {
    "deadline.CreateWorker": {
        "log_request_url": True,
        "req_log_body": {"hostProperties": True},
        "res_log_body": {"workerId": True},
    },
    "deadline.AssumeFleetRoleForWorker": {
        "log_request_url": True,
        # req_log_body -- no body to log
        "res_log_body": {
            "credentials": {
                "accessKeyId": True,  # Not a secret
                "expiration": True
                # exclude: secretAccessKey & sessionToken
            }
        },
    },
    "deadline.AssumeQueueRoleForWorker": {
        "log_request_url": True,
        # req_log_body -- no body to log
        "res_log_body": {
            "credentials": {
                "accessKeyId": True,  # Not a secret
                "expiration": True
                # exclude: secretAccessKey & sessionToken
            }
        },
    },
    "deadline.UpdateWorker": {
        "log_request_url": True,
        "req_log_body": {
            "status": True,
            # capabilities are not sensitive. They're treated like AWS Tags for privacy.
            "capabilities": True,
            "hostProperties": True,
        },
        "res_log_body": {"log": True},
    },
    "deadline.UpdateWorkerSchedule": {
        "log_request_url": True,
        "req_log_body": {
            "updatedSessionActions": {
                # Key is a Session Action Id
                "*": {
                    "completedStatus": True,
                    "processExitCode": True,
                    "startedAt": True,
                    "endedAt": True,
                    "updatedAt": True,
                    "progressPercent": True,
                    # progressMessage excluded
                },
            }
        },
        "res_log_body": {
            # assignedSessions only contains non-sensitive data by design.
            "assignedSessions": {
                "*": {
                    "queueId": True,
                    "jobId": True,
                    "sessionActions": {  # is an array of objects
                        "sessionActionId": True,
                        "definition": {
                            "envEnter": {"environmentId": True},
                            "envExit": {"environmentId": True},
                            "taskRun": {
                                "taskId": True,
                                "stepId": True,
                                # parameters excluded. It's sensitive for the Agent log since the parameter
                                # names may have meaning to the customer. It should only appear in the session log.
                            },
                            "syncInputJobAttachments": {"stepId": True},
                        },
                    },
                    "logConfiguration": True,  # log the entire dictionary
                },
            },
            "cancelSessionActions": True,  # array of IDs. Log the whole thing
            "desiredWorkerStatus": True,
            "updateIntervalSeconds": True,
        },
    },
    "deadline.BatchGetJobEntity": {
        "log_request_url": True,
        "req_log_body": {
            "identifiers": True,  # only contains identifiers
        },
        "res_log_body": {
            "entities": {
                "jobDetails": {
                    "jobId": True,
                    "jobAttachmentSettings": {"s3BucketName": True, "rootPrefix": True},
                    "jobRunAsUser": True,
                    "logGroupName": True,
                    "queueRoleArn": True,
                    # parameters excluded.  It's sensitive for the Agent log since the parameter
                    # names may have meaning to the customer. It should only appear in the session log.
                    "schemaVersion": True,
                    "pathMappingRules": False,  # will contain filesystem paths; err on the side of caution
                },
                "jobAttachmentDetails": {
                    "jobId": True,
                    "attachments": {
                        "manifests": {  # array of objects
                            "fileSystemLocationName": True,
                            "rootPath": False,  # will contain filesystem paths; err on the side of caution
                            "rootPathFormat": True,
                            "outputRelativeDirectories": False,  # will contain filesystem paths; err on the side of caution
                            "inputManifestPath": True,
                            "inputManifestHash": True,
                        },
                        "fileSystem": True,
                    },
                },
                "environmentDetails": {
                    "jobId": True,
                    "environmentId": True,
                    "schemaVersion": True,
                    # template excluded. It's sensitive; it contains the commands to run
                },
                "stepDetails": {
                    "jobId": True,
                    "stepId": True,
                    "schemaVersion": True,
                    "dependencies": True,  # list of step IDs
                    # template excluded. It's sensitive; it contains the commands to run
                },
            },
            "errors": True,  # log all errors
        },
    },
    "deadline.DeleteWorker": {
        "log_request_url": True,
        # request is empty
        # response is empty
    },
    # =========================
    #  Non-Deadline Services
    "secretsmanager.GetSecretValue": {
        "log_request_url": True,
        "req_log_body": {"SecretId": True, "VersionId": True, "VersionStage": True},
        "res_log_body": {
            "ARN": True,
            "CreatedDate": True,
            "Name": True,
            "VersionId": True
            # excluding: SecretString/SecretBinary, for obvious reasons
            # excluding VersionStages; seems unnecessary
        },
    },
}

# Not logging:
#  cloudwatch.PutLogEvents -- logging that you're logging just clutters the log
#  s3.* -- For now. Very verbose to be logging these dataplane APIs; maybe when we have a verbose log mode.
_IGNORE_LIST = ["cloudwatch-logs\..*", "s3\..*"]
LOGGING_IGNORE_MATCHER = re.compile("^(" + "|".join(_IGNORE_LIST) + ")$")


def _get_loggable_parameters(
    body: dict[str, Any], allowable_keys: AllowableBodyKeysT
) -> dict[str, Any]:
    to_be_logged = dict[str, Any]()
    if body is None:  # Handle the None case
        return to_be_logged
    for k, v in body.items():
        if k not in allowable_keys:
            if "*" not in allowable_keys:
                to_be_logged[k] = "*REDACTED*"
                continue
            allow_key = "*"
        else:
            allow_key = k
        allowable = allowable_keys[allow_key]
        if isinstance(v, datetime.datetime):
            v = str(v)
        if isinstance(allowable, bool):
            if allowable:
                to_be_logged[k] = v
            else:
                to_be_logged[k] = "*REDACTED*"
        elif isinstance(v, dict):
            to_be_logged[k] = _get_loggable_parameters(v, allowable)
        elif isinstance(v, list):
            to_be_logged[k] = [_get_loggable_parameters(vv, allowable) for vv in v]
    return to_be_logged


def log_before_call(event_name, params, **kwargs) -> None:
    # event name is of the form `before_call.{operation}`
    # params looks like:
    # {
    #   "url_path": "/...",
    #   "query_string": {},
    #   "method": "POST",
    #   "headers": {
    #       "X-Amz-Client-Token": "f3a3e84d-f024-4caa-87a7-4a83e1361fef",
    #       "Content-Type": "application/json",
    #       "User-Agent": <string>
    #   },
    #   "body": b"...",
    #   "url": "https://....amazonaws.com/...",
    #   "context": {
    #       "client_region": "us-west-2",
    #       "client_config": <botocore.config.Config object at 0x7f552d8312e0>,
    #       "has_streaming_input": False,
    #       "auth_type": None
    #   }
    # }
    try:
        operation_name = event_name.split(".", 1)[1]
        if LOGGING_IGNORE_MATCHER.match(operation_name):
            return
        body = params["body"]
        if isinstance(body, bytes):
            if body == b"":
                body = None
            # Body can be string or bytes
            else:
                body = body.decode()
        if isinstance(body, str):
            body = json.loads(body)
        loggable_params: Union[dict[str, Any], str] = {}
        if operation_name in LOGGING_ALLOW_LIST:
            allow_list = LOGGING_ALLOW_LIST[operation_name]
            if allow_list.get("log_request_url", True):
                url = params.get("url")
            allowable_response_fields = allow_list.get("req_log_body", dict())
            loggable_params = _get_loggable_parameters(body, allowable_response_fields)
        else:
            loggable_params = "*REDACTED*"
            url = "*REDACTED*"
        log_statement: BotoLogStatement = {
            "log_type": "boto_request",
            "operation": operation_name,
            "params": loggable_params,
        }  # noqa
        if url is not None:
            log_statement["request_url"] = url
        log.info(json.dumps(log_statement))
    except Exception:
        log.exception(f"Error Logging Boto Request with name {event_name}!")


def log_after_call(event_name, parsed, **kwargs) -> None:
    # event name is of the form `after_call.{operation}`
    # parsed looks like:
    # {
    #   "ResponseMetadata": {
    #       "RequestId": "9885082b-3d0a-40f3-854a-223aa86f549b",
    #       "HTTPStatusCode": 200,
    #       "HTTPHeaders": {
    #           "date": "Sun,17 Mar 2024 19: 08: 19 GMT",
    #           "content-type": "application/json",
    #           "content-length": "81",
    #           "connection": "keep-alive",
    #           "x-amzn-requestid": "9885082b-3d0a-40f3-854a-223aa86f549b",
    #           "access-control-allow-origin": "*",
    #           "access-control-expose-headers-age": "86400",
    #           "x-amz-apigw-id": <string>,
    #           "cache-control": "no-cache; no-store, must-revalidate, private",
    #           "expires": "0",
    #           "access-control-allow-methods": "GET,PATCH,POST,PUT,DELETE",
    #           "access-control-expose-headers": "x-amzn-ErrorType,x-amzn-requestid,x-amzn-trace-id,x-amz-apigw-id",
    #           "x-amzn-trace-id": "Root=1-65f73fa2-3b164cfd5721593c539e68b4",
    #           "pragma": "no-cache",
    #           "access-control-max-age": "86400"
    #       },
    #       "RetryAttempts": 0
    #   },
    #   ... fields of the response.
    # }
    #
    # OR
    #
    # {
    #     "ResponseMetadata": {
    #       "RequestId": "9885082b-3d0a-40f3-854a-223aa86f549b",
    #       "HTTPStatusCode": 200,
    #       "HTTPHeaders": {
    #           "date": "Sun,17 Mar 2024 19: 08: 19 GMT",
    #           "content-type": "application/json",
    #           "content-length": "81",
    #           "connection": "keep-alive",
    #           "x-amzn-requestid": "9885082b-3d0a-40f3-854a-223aa86f549b",
    #           "access-control-allow-origin": "*",
    #           "access-control-expose-headers-age": "86400",
    #           "x-amz-apigw-id": <string>,
    #           "cache-control": "no-cache; no-store, must-revalidate, private",
    #           "expires": "0",
    #           "access-control-allow-methods": "GET,PATCH,POST,PUT,DELETE",
    #           "access-control-expose-headers": "x-amzn-ErrorType,x-amzn-requestid,x-amzn-trace-id,x-amz-apigw-id",
    #           "x-amzn-trace-id": "Root=1-65f73fa2-3b164cfd5721593c539e68b4",
    #           "pragma": "no-cache",
    #           "access-control-max-age": "86400"
    #     },
    #     "Error": {
    #         "Message": "<string>",
    #         "Code": "ConflictException"
    #     },
    #     # The following are properties of the exception raised by the service
    #     # They don't need to be redacted in logs
    #     "message": "<string",
    #     "reason": "CONFLICT_EXCEPTION",
    #     "resourceId": "fleet-75103c183347473db3300904feceb78c",
    #     "resourceType": "worker"
    # }
    try:
        operation_name = event_name.split(".", 1)[1]
        if LOGGING_IGNORE_MATCHER.match(operation_name):
            return
        loggable_params: Union[dict[str, Any], str] = {}
        if "Error" in parsed:
            loggable_params = dict(parsed)
            del loggable_params["Error"]
        else:
            if operation_name in LOGGING_ALLOW_LIST:
                allow_list = LOGGING_ALLOW_LIST[operation_name]
                allowable_response_fields = allow_list.get("res_log_body", dict())
                loggable_params = _get_loggable_parameters(parsed, allowable_response_fields)
            else:
                loggable_params = "*REDACTED*"
        # We don't want metadata to show up at all as params, so delete it from the result
        if isinstance(loggable_params, dict):
            del loggable_params["ResponseMetadata"]

        log_statement: BotoLogStatement = {
            "log_type": "boto_response",
            "operation": operation_name,
            "status_code": parsed.get("ResponseMetadata", {}).get("HTTPStatusCode"),
            "params": loggable_params,
        }
        error = parsed.get("Error")
        if error is not None:
            log_statement["error"] = error
        log_statement["request_id"] = parsed.get("ResponseMetadata", {}).get("RequestId")
        log.info(json.dumps(log_statement))
    except Exception:
        log.exception(f"Error Logging Boto Response with name {event_name}!")


def configure_session_events(botocore_session=None, boto3_session=None):
    """Configures a boto session to log api calls using before and after event hooks"""
    if botocore_session:
        botocore_session.register(
            Boto3ClientEvent.BEFORE_CALL_PREFIX,
            log_before_call,
        )
        botocore_session.register(
            Boto3ClientEvent.AFTER_CALL_PREFIX,
            log_after_call,
        )
    if boto3_session:
        boto3_session.events.register(
            Boto3ClientEvent.BEFORE_CALL_PREFIX,
            log_before_call,
        )
        boto3_session.events.register(
            Boto3ClientEvent.AFTER_CALL_PREFIX,
            log_after_call,
        )
