# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
from typing import Any, Optional, TypedDict, Union
import json
import datetime
import re
from threading import Lock

from .log_messages import ApiRequestLogEvent, ApiResponseLogEvent

log = logging.getLogger(__name__)


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
    "deadline:CreateWorker": {
        "log_request_url": True,
        "req_log_body": {"hostProperties": True},
        "res_log_body": {"workerId": True},
    },
    "deadline:AssumeFleetRoleForWorker": {
        "log_request_url": True,
        # req_log_body -- no body to log
        "res_log_body": {
            "credentials": {
                "accessKeyId": True,  # Not a secret
                "expiration": True,
                # exclude: secretAccessKey & sessionToken
            }
        },
    },
    "deadline:AssumeQueueRoleForWorker": {
        "log_request_url": True,
        # req_log_body -- no body to log
        "res_log_body": {
            "credentials": {
                "accessKeyId": True,  # Not a secret
                "expiration": True,
                # exclude: secretAccessKey & sessionToken
            }
        },
    },
    "deadline:UpdateWorker": {
        "log_request_url": True,
        "req_log_body": {
            "status": True,
            # capabilities are not sensitive. They're treated like AWS Tags for privacy.
            "capabilities": True,
            "hostProperties": True,
        },
        "res_log_body": {"log": True},
    },
    "deadline:UpdateWorkerSchedule": {
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
    "deadline:BatchGetJobEntity": {
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
    "deadline:DeleteWorker": {
        "log_request_url": True,
        # request is empty
        # response is empty
    },
    # =========================
    #  Non-Deadline Services
    "secretsmanager:GetSecretValue": {
        "log_request_url": True,
        "req_log_body": {"SecretId": True, "VersionId": True, "VersionStage": True},
        "res_log_body": {
            "ARN": True,
            "CreatedDate": True,
            "Name": True,
            "VersionId": True,
            # excluding: SecretString/SecretBinary, for obvious reasons
            # excluding VersionStages; seems unnecessary
        },
    },
}

# Not logging:
#  cloudwatch.PutLogEvents -- logging that you're logging just clutters the log
#  s3.* -- For now. Very verbose to be logging these dataplane APIs; maybe when we have a verbose log mode.
_IGNORE_LIST = [r"cloudwatch-logs:.*", r"s3:.*"]
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


_deadline_resource_patterns = (
    ("farm", r"farm-[0-9a-fA-F]+"),
    ("fleet", r"fleet-[0-9a-fA-F]+"),
    ("queue", r"queue-[0-9a-fA-F]+"),
    ("worker", r"worker-[0-9a-fA-F]+"),
)
_deadline_resource_map: dict[str, str] = {
    "farm": "farm-id",
    "fleet": "fleet-id",
    "queue": "queue-id",
    "worker": "worker-id",
}
_DEADLINE_RESOURCE_REGEX = "|".join(
    f"(?P<{pair[0]}>{pair[1]})" for pair in _deadline_resource_patterns
)
_DEADLINE_RESOURCE_MATCHER = re.compile(_DEADLINE_RESOURCE_REGEX)


class PreviousRequestRecord:
    # This is for deduplicating log entries; primarily aimed at
    # deadline:UpdateWorkerSchedule, but it could include any API in future
    # if desired. However, it should only be used for APIs that cannot be
    # queried concurrently; that, basically, means that the API must only be
    # queried within the Agent's main thread/event-loop.
    #
    # The structure's contents are:
    # {
    #    <api-name>: {
    #       "request": {
    #          <request parameters>
    #          For deadline, this is the request 'params' and
    #          deadline-resource (farm/fleet/queue/etc) merged together
    #          into one dictionary.
    #       },
    #       "response": {
    #          <response parameters>
    #       }
    #    },
    #   ...
    # }
    _api_parameters: dict[str, dict[str, Any]]

    # The API operation names that are subject to filtering
    _allowlist: set[str] = {
        "deadline:UpdateWorkerSchedule",
    }

    _lock: Lock

    def __init__(self) -> None:
        self._api_parameters = dict()
        self._lock = Lock()

    def record_request(
        self,
        *,
        operation_name: str,
        params: dict[str, Any],
        resource: Optional[dict[str, str]] = None,
    ) -> None:
        if operation_name not in self._allowlist:
            return
        with self._lock:
            if operation_name not in self._api_parameters:
                self._api_parameters[operation_name] = dict(request=None)
            self._api_parameters[operation_name]["request"] = dict(**params)
            if resource:
                self._api_parameters[operation_name]["request"].update(**resource)

    def record_response(self, *, operation_name: str, params: dict[str, Any]) -> None:
        if operation_name not in self._allowlist:
            return
        with self._lock:
            if operation_name not in self._api_parameters:
                self._api_parameters[operation_name] = dict(response=None)
            self._api_parameters[operation_name]["response"] = dict(**params)

    def filter_request(
        self,
        *,
        operation_name: str,
        params: dict[str, Any],
        resource: Optional[dict[str, str]] = None,
    ) -> bool:
        # Return true if and only if the request should be suppressed from the log.
        # i.e. That it exactly matches the previous request made.
        if operation_name not in self._allowlist:
            return False
        with self._lock:
            if operation_name not in self._api_parameters:
                return False
            if "request" not in self._api_parameters[operation_name]:
                return False
            curr_req = dict(**params)
            if resource:
                curr_req.update(**resource)
            return curr_req == self._api_parameters[operation_name]["request"]

    def filter_response(self, *, operation_name: str, params: dict[str, Any]) -> bool:
        # Return true if and only if the request should be suppressed from the log.
        # i.e. That it exactly matches the previous request made.
        if operation_name not in self._allowlist:
            return False
        with self._lock:
            if operation_name not in self._api_parameters:
                return False
            if "response" not in self._api_parameters[operation_name]:
                return False
            return params == self._api_parameters[operation_name]["response"]


API_RECORD = PreviousRequestRecord()


def _extract_deadline_resource_info(request_url: str) -> dict[str, str]:
    dd = dict[str, str]()
    for match in _DEADLINE_RESOURCE_MATCHER.finditer(request_url):
        kind = match.lastgroup
        value = match.group()
        dd[_deadline_resource_map[kind]] = value  # type: ignore
    return dd


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
        operation_name = event_name.split(".", 1)[1].replace(".", ":")
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
        log_statement = {
            "operation": operation_name,
            "request_url": url,
            "params": loggable_params,
        }  # noqa
        if operation_name.startswith("deadline"):
            log_statement["deadline_resource"] = _extract_deadline_resource_info(params["url"])
        if not API_RECORD.filter_request(
            operation_name=operation_name,
            params=body,
            resource=log_statement.get("deadline_resource"),
        ):
            log.info(ApiRequestLogEvent(**log_statement))
            API_RECORD.record_request(
                operation_name=operation_name,
                params=body,
                resource=log_statement.get("deadline_resource"),
            )
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
        operation_name = event_name.split(".", 1)[1].replace(".", ":")
        if LOGGING_IGNORE_MATCHER.match(operation_name):
            return
        loggable_params: Union[dict[str, Any], str] = dict()
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

        if "Error" not in parsed and API_RECORD.filter_response(
            operation_name=operation_name,
            params=loggable_params if isinstance(loggable_params, dict) else dict(),
        ):
            # If it's not an Error and it matches the previous response, then we just filter out the response parameters
            loggable_params = "(Duplicate removed, see previous response)"
        elif "Error" not in parsed:
            # If it's not an error, then update the record of the last recorded response
            API_RECORD.record_response(
                operation_name=operation_name,
                params=loggable_params if isinstance(loggable_params, dict) else dict(),
            )

        log_statement = {
            "operation": operation_name,
            "status_code": parsed.get("ResponseMetadata", {}).get("HTTPStatusCode", "UNKNOWN"),
            "params": loggable_params,
            "request_id": parsed.get("ResponseMetadata", {}).get("RequestId", "UNKNOWN"),
        }
        error = parsed.get("Error")
        if error is not None:
            log_statement["error"] = error
            log.error(ApiResponseLogEvent(**log_statement))
        else:
            log.info(ApiResponseLogEvent(**log_statement))
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
