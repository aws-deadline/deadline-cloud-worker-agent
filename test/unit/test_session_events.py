# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from deadline_worker_agent.session_events import (
    log_after_call,
    log_before_call,
    LoggingAllowList,
    LOGGING_ALLOW_LIST,
)
from typing import Dict
from typing import Any


def test_logging_allow_list():
    test_allow_list: Dict[str, LoggingAllowList] = {
        "deadline.CreateWorker": {"log_request_url": True, "res_body_keys": ["workerId"]},
        "deadline.AssumeFleetRoleForWorker": {"log_request_url": True},
        "deadline.AssumeQueueRoleForWorker": {"log_request_url": True},
        "deadline.UpdateWorker": {
            "log_request_url": True,
            "req_body_keys": ["status"],
            "res_body_keys": ["log"],
        },
        "deadline.BatchGetJobEntity": {"log_request_url": True},
        "deadline.DeleteWorker": {"log_request_url": True},
    }

    assert test_allow_list == LOGGING_ALLOW_LIST


@pytest.mark.parametrize(
    argnames=("event_name", "params", "expected_result"),
    argvalues=(
        pytest.param(
            "before-call.deadline.CreateWorker",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers",
                "body": '{"hostProperties": {"ipAddresses": {"ipV4Addresses": ["0.0.0.0", "127.0.0.1", "0.0.0.0"], "ipV6Addresses": ["0000:0000:0000:0000:0000:0000:0000:0000", "0000:0000:0000:0000:0000:0000:0000:0000", "0000:0000:0000:0000:0000:0000:0000:0000"]}, "hostName": "host.us-west-2.amazon.com"}}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.CreateWorker', 'params': {}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers'}",
            id="CreateWorkerBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.AssumeFleetRoleForWorker",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/fleet-role",
                "query_string": {},
                "method": "GET",
                "headers": {
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12"
                },
                "body": b"",
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/fleet-role",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.AssumeFleetRoleForWorker', 'params': {}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/fleet-role'}",
            id="AssumeFleetRoleForWorkerBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.UpdateWorker",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000",
                "query_string": {},
                "method": "PATCH",
                "headers": {
                    "Content-Type": "application/json",
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12",
                },
                "body": b'{"status": "STARTED", "capabilities": {"amounts": [{"name": "amount.worker.vcpu", "value": 8.0}, {"name": "amount.worker.memory", "value": 14987.5234375}, {"name": "amount.worker.disk.scratch", "value": 0.0}, {"name": "amount.worker.gpu", "value": 0.0}, {"name": "amount.worker.gpu.memory", "value": 0.0}], "attributes": [{"name": "attr.worker.os.family", "values": ["linux"]}, {"name": "attr.worker.cpu.arch", "values": ["x86_64"]}]}, "hostProperties": {"ipAddresses": {"ipV4Addresses": ["127.0.0.1", "0.0.0.0", "0.0.0.0"], "ipV6Addresses": ["0000:0000:0000:0000:0000:0000:00000:0000", "0000:0000:0000:0000:0000:0000:0000:0001", "0000:0000:0000:0000:0000:0000:0000:0000"]}, "hostName": "host.us-west-2.amazon.com"}}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.UpdateWorker', 'params': {'status': 'STARTED'}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000'}",
            id="UpdateWorkerBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.DeleteWorker",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000",
                "query_string": {},
                "method": "DELETE",
                "headers": {
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12 deadline_worker_agent/0.9.0.post2+g64a93a0.d20230726"
                },
                "body": b"",
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.DeleteWorker', 'params': {}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000'}",
            id="DeleteWorkerBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.BatchGetJobEntity",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity",
                "query_string": {},
                "method": "POST",
                "headers": {
                    "Content-Type": "application/json",
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12 deadline_worker_agent/0.9.0.post4+g43d00ae.d20230726",
                },
                "body": b'{"identifiers": [{"jobDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82"}}]}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.BatchGetJobEntity', 'params': {}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity'}",
            id="BatchGetJobEntityBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.AssumeQueueRoleForWorker",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/queue-role",
                "query_string": {"queueId": "queue-0000000000000000000000000000000"},
                "method": "GET",
                "headers": {
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12 deadline_worker_agent/0.9.0.post4+g43d00ae.d20230726"
                },
                "body": b"",
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/queue-role?queueId=queue-0000000000000000000000000000000",
            },
            "{'log_type': 'boto_request', 'operation': 'deadline.AssumeQueueRoleForWorker', 'params': {}, 'request_url': 'https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/queue-role?queueId=queue-0000000000000000000000000000000'}",
            id="AssumeQueueRoleForWorkerBeforeCallTest",
        ),
    ),
)
def test_log_before_call(
    event_name: str, params: Dict[str, Any], expected_result: str, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(0)
    log_before_call(event_name, params)
    assert caplog.messages == [expected_result]


@pytest.mark.parametrize(
    argnames=("event_name", "params", "expected_result"),
    argvalues=(
        pytest.param(
            "after-call.deadline.CreateWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "workerId": "worker-38aab2f87d2b45298b39875639580970",
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.CreateWorker', 'status_code': 200, 'params': {'workerId': 'worker-38aab2f87d2b45298b39875639580970'}}",
            id="CreateWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.AssumeFleetRoleForWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                }
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.AssumeFleetRoleForWorker', 'status_code': 200, 'params': {}}",
            id="AssumeFleetRoleAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.AssumeQueueRoleForWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                }
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.AssumeQueueRoleForWorker', 'status_code': 200, 'params': {}}",
            id="AssumeQueueRoleForWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.UpdateWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "status": "STOPPED",
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.UpdateWorker', 'status_code': 200, 'params': {'log': None}}",
            id="UpdateWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.BatchGetJobEntity",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                }
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.BatchGetJobEntity', 'status_code': 200, 'params': {}}",
            id="BatchGetJobEntityAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.DeleteWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                }
            },
            "{'log_type': 'boto_response', 'operation': 'deadline.DeleteWorker', 'status_code': 200, 'params': {}}",
            id="DeleteWorkerAfterCallTest",
        ),
    ),
)
def test_log_after_call(
    event_name: str, params: Dict[str, Any], expected_result: str, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(0)
    log_after_call(event_name, params)
    assert caplog.messages == [expected_result]
