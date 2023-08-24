# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
from typing import Any, Dict, List, TypedDict, Union
from typing_extensions import NotRequired
import json
from typing import Any, Dict, List, TypedDict, Union

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
    params: Union[List[Dict[str, Any]], Dict[str, Any]]
    # Only in responses
    error: NotRequired[str]
    status_code: NotRequired[str]


class Boto3ClientEvent:
    BEFORE_CALL_PREFIX = "before-call"
    AFTER_CALL_PREFIX = "after-call"


class LoggingAllowList(TypedDict, total=False):
    # RESTful services have IDs in the url
    # set this to true to include the request URL in the logs
    log_request_url: bool
    # keys to log for this operation from the request's body
    req_body_keys: List[str]
    # keys to log for this operation from the response's body
    res_body_keys: List[str]


LOGGING_ALLOW_LIST: Dict[str, LoggingAllowList] = {
    "deadline.CreateWorker": {"log_request_url": True, "res_body_keys": ["workerId"]},
    "deadline.AssumeFleetRoleForWorker": {"log_request_url": True},
    "deadline.AssumeQueueRoleForWorker": {"log_request_url": True},
    "deadline.UpdateWorker": {
        "log_request_url": True,
        "req_body_keys": ["targetStatus"],
        "res_body_keys": ["log"],
    },
    "deadline.BatchGetJobEntity": {"log_request_url": True},
    "deadline.DeleteWorker": {"log_request_url": True},
}


def log_before_call(event_name, params, **kwargs) -> None:
    # event name is of the form `before_call.{operation}`
    try:
        operation_name = event_name.split(".", 1)[1]
        if operation_name in LOGGING_ALLOW_LIST:
            allow_list = LOGGING_ALLOW_LIST[operation_name]
            body = params["body"]
            if isinstance(body, bytes):
                if body == b"":
                    body = None
                # Body can be string or bytes
                else:
                    body = body.decode()
            if isinstance(body, str):
                body = json.loads(body)
            if allow_list.get("log_request_url", True):
                url = params.get("url")
            loggable_params = {key: body.get(key) for key in allow_list.get("req_body_keys", [])}
            log_statement: BotoLogStatement = {
                "log_type": "boto_request",
                "operation": operation_name,
                "params": loggable_params,
            }  # noqa
            if url is not None:
                log_statement["request_url"] = url
            log.info(log_statement)
    except Exception:
        log.exception(f"Error Logging Boto Request with name {event_name}!")


def log_after_call(event_name, parsed, **kwargs) -> None:
    # event name is of the form `after_call.{operation}`
    try:
        operation_name = event_name.split(".", 1)[1]
        loggable_params: Dict[str, Any] = {}
        error = parsed.get("Error")
        if operation_name in LOGGING_ALLOW_LIST:
            allow_list = LOGGING_ALLOW_LIST[operation_name]
            loggable_params = {key: parsed.get(key) for key in allow_list.get("res_body_keys", [])}
            log_statement: BotoLogStatement = {
                "log_type": "boto_response",
                "operation": operation_name,
                "status_code": parsed.get("ResponseMetadata", {}).get("HTTPStatusCode"),
                "params": loggable_params,
            }
            error_code = None
            if error is not None:
                log_statement["error"] = error
                error_code = error.get("Code")  # noqa: F841
            log.info(log_statement)
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
