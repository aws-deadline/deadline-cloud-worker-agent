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
import datetime


def test_logging_allow_list():
    # This test exists to ensure that no ACCIDENTAL changes are made to the logging allow-list

    test_allow_list: Dict[str, LoggingAllowList] = {
        "deadline.CreateWorker": {
            "log_request_url": True,
            "req_log_body": {"hostProperties": True},
            "res_log_body": {"workerId": True},
        },
        "deadline.AssumeFleetRoleForWorker": {
            "log_request_url": True,
            "res_log_body": {"credentials": {"accessKeyId": True, "expiration": True}},
        },
        "deadline.AssumeQueueRoleForWorker": {
            "log_request_url": True,
            "res_log_body": {"credentials": {"accessKeyId": True, "expiration": True}},
        },
        "deadline.UpdateWorker": {
            "log_request_url": True,
            "req_log_body": {
                "status": True,
                "capabilities": True,
                "hostProperties": True,
            },
            "res_log_body": {"log": True},
        },
        "deadline.UpdateWorkerSchedule": {
            "log_request_url": True,
            "req_log_body": {
                "updatedSessionActions": {
                    "*": {
                        "completedStatus": True,
                        "processExitCode": True,
                        "startedAt": True,
                        "endedAt": True,
                        "updatedAt": True,
                        "progressPercent": True,
                    },
                }
            },
            "res_log_body": {
                "assignedSessions": {
                    "*": {
                        "queueId": True,
                        "jobId": True,
                        "sessionActions": {
                            "sessionActionId": True,
                            "definition": {
                                "envEnter": {"environmentId": True},
                                "envExit": {"environmentId": True},
                                "taskRun": {
                                    "taskId": True,
                                    "stepId": True,
                                },
                                "syncInputJobAttachments": {"stepId": True},
                            },
                        },
                        "logConfiguration": True,
                    },
                },
                "cancelSessionActions": True,
                "desiredWorkerStatus": True,
                "updateIntervalSeconds": True,
            },
        },
        "deadline.BatchGetJobEntity": {
            "log_request_url": True,
            "req_log_body": {
                "identifiers": True,
            },
            "res_log_body": {
                "entities": {
                    "jobDetails": {
                        "jobId": True,
                        "jobAttachmentSettings": {"s3BucketName": True, "rootPrefix": True},
                        "jobRunAsUser": True,
                        "logGroupName": True,
                        "queueRoleArn": True,
                        "schemaVersion": True,
                        "pathMappingRules": False,
                    },
                    "jobAttachmentDetails": {
                        "jobId": True,
                        "attachments": {
                            "manifests": {
                                "fileSystemLocationName": True,
                                "rootPath": False,
                                "rootPathFormat": True,
                                "outputRelativeDirectories": False,
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
                    },
                    "stepDetails": {
                        "jobId": True,
                        "stepId": True,
                        "schemaVersion": True,
                        "dependencies": True,
                    },
                },
                "errors": True,
            },
        },
        "deadline.DeleteWorker": {
            "log_request_url": True,
        },
        "secretsmanager.GetSecretValue": {
            "log_request_url": True,
            "req_log_body": {"SecretId": True, "VersionId": True, "VersionStage": True},
            "res_log_body": {"ARN": True, "CreatedDate": True, "Name": True, "VersionId": True},
        },
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
            '{"log_type": "boto_request", "operation": "deadline.CreateWorker", "params": {"hostProperties": {"ipAddresses": {"ipV4Addresses": ["0.0.0.0", "127.0.0.1", "0.0.0.0"], "ipV6Addresses": ["0000:0000:0000:0000:0000:0000:0000:0000", "0000:0000:0000:0000:0000:0000:0000:0000", "0000:0000:0000:0000:0000:0000:0000:0000"]}, "hostName": "host.us-west-2.amazon.com"}}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers"}',
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
            '{"log_type": "boto_request", "operation": "deadline.AssumeFleetRoleForWorker", "params": {}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/fleet-role"}',
            id="AssumeFleetRoleForWorkerBeforeCallTest",
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
            '{"log_type": "boto_request", "operation": "deadline.AssumeQueueRoleForWorker", "params": {}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/queue-role?queueId=queue-0000000000000000000000000000000"}',
            id="AssumeQueueRoleForWorkerBeforeCallTest",
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
            '{"log_type": "boto_request", "operation": "deadline.UpdateWorker", "params": {"status": "STARTED", "capabilities": {"amounts": [{"name": "amount.worker.vcpu", "value": 8.0}, {"name": "amount.worker.memory", "value": 14987.5234375}, {"name": "amount.worker.disk.scratch", "value": 0.0}, {"name": "amount.worker.gpu", "value": 0.0}, {"name": "amount.worker.gpu.memory", "value": 0.0}], "attributes": [{"name": "attr.worker.os.family", "values": ["linux"]}, {"name": "attr.worker.cpu.arch", "values": ["x86_64"]}]}, "hostProperties": {"ipAddresses": {"ipV4Addresses": ["127.0.0.1", "0.0.0.0", "0.0.0.0"], "ipV6Addresses": ["0000:0000:0000:0000:0000:0000:00000:0000", "0000:0000:0000:0000:0000:0000:0000:0001", "0000:0000:0000:0000:0000:0000:0000:0000"]}, "hostName": "host.us-west-2.amazon.com"}}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000"}',
            id="UpdateWorkerBeforeCallTest",
        ),
        pytest.param(
            "before-call.deadline.UpdateWorkerSchedule",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000",
                "query_string": {},
                "method": "PATCH",
                "headers": {
                    "Content-Type": "application/json",
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12",
                },
                "body": b'{"updatedSessionActions": {"sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-0": {"startedAt": "2024-03-15T22:58:02.574480Z", "completedStatus": "FAILED", "processExitCode": 126, "endedAt": "2024-03-15T22:58:02.589172Z"}, "sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-1": {"completedStatus": "NEVER_ATTEMPTED", "progressMessage": "We dun failed"}, "sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-2": {"completedStatus": "NEVER_ATTEMPTED", "progressMessage": "We dun failed"}}}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/schedule",
            },
            '{"log_type": "boto_request", "operation": "deadline.UpdateWorkerSchedule", "params": {"updatedSessionActions": {"sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-0": {"startedAt": "2024-03-15T22:58:02.574480Z", "completedStatus": "FAILED", "processExitCode": 126, "endedAt": "2024-03-15T22:58:02.589172Z"}, "sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-1": {"completedStatus": "NEVER_ATTEMPTED", "progressMessage": "*REDACTED*"}, "sessionaction-45044d1fbc4f4d6388f5ef694ed0c298-2": {"completedStatus": "NEVER_ATTEMPTED", "progressMessage": "*REDACTED*"}}}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/schedule"}',
            id="UpdateWorkerScheduleBeforeCallTest",
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
            '{"log_type": "boto_request", "operation": "deadline.BatchGetJobEntity", "params": {"identifiers": [{"jobDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82"}}]}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity"}',
            id="BatchGetJobEntityBeforeCallTest-JobDetails",
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
                "body": b'{"identifiers": [{"jobAttachmentDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82"}}]}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity",
            },
            '{"log_type": "boto_request", "operation": "deadline.BatchGetJobEntity", "params": {"identifiers": [{"jobAttachmentDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82"}}]}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity"}',
            id="BatchGetJobEntityBeforeCallTest-JobAttachmentDetails",
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
                "body": b'{"identifiers": [{"stepDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82", "stepId": "step-0771968389a54c26adf4afd80bac1b82"}}]}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity",
            },
            '{"log_type": "boto_request", "operation": "deadline.BatchGetJobEntity", "params": {"identifiers": [{"stepDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82", "stepId": "step-0771968389a54c26adf4afd80bac1b82"}}]}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity"}',
            id="BatchGetJobEntityBeforeCallTest-StepDetails",
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
                "body": b'{"identifiers": [{"environmentDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82", "environmentId": "STEP:step-0771968389a54c26adf4afd80bac1b82:Identifier"}}]}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity",
            },
            '{"log_type": "boto_request", "operation": "deadline.BatchGetJobEntity", "params": {"identifiers": [{"environmentDetails": {"jobId": "job-0771968389a54c26adf4afd80bac1b82", "environmentId": "STEP:step-0771968389a54c26adf4afd80bac1b82:Identifier"}}]}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000/batchGetJobEntity"}',
            id="BatchGetJobEntityBeforeCallTest-EnvironmentDetails",
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
            '{"log_type": "boto_request", "operation": "deadline.DeleteWorker", "params": {}, "request_url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/workers/worker-0000000000000000000000000000000"}',
            id="DeleteWorkerBeforeCallTest",
        ),
        # ====================
        pytest.param(
            "before-call.secretsmanager.GetSecretValue",
            {
                "url_path": "/",
                "query_string": {},
                "method": "POST",
                "headers": {
                    "User-Agent": "Boto3/1.28.12 md/Botocore#1.31.12 ua/2.0 os/linux#5.4.247-169.350.amzn2int.x86_64 md/arch#x86_64 lang/python#3.9.17 md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.31.12 deadline_worker_agent/0.9.0.post2+g64a93a0.d20230726"
                },
                "body": b'{"SecretId": "secret-id", "VersionId": "6fb9f17a-f9a9-4729-af0f-df67e976484c", "VersionStage": "AWSCURRENT"}',
                "url": "https://secretsmanager.us-west-2.amazonaws.com/",
            },
            '{"log_type": "boto_request", "operation": "secretsmanager.GetSecretValue", "params": {"SecretId": "secret-id", "VersionId": "6fb9f17a-f9a9-4729-af0f-df67e976484c", "VersionStage": "AWSCURRENT"}, "request_url": "https://secretsmanager.us-west-2.amazonaws.com/"}',
            id="GetSecretValueBeforeCallTest",
        ),
        # ====================
        # Unknown API tests -- make sure that we redact the params, but still report the API
        pytest.param(
            "before-call.deadline.NotAnAPI",
            {
                "url_path": "/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/newthing",
                "body": '{"requestParam": "requestValue"}',
                "url": "https://**********.execute-api.us-west-2.amazonaws.com/2020-08-21/farms/farm-0000000000000000000000000000000/fleets/fleet-0000000000000000000000000000000/newthing",
            },
            '{"log_type": "boto_request", "operation": "deadline.NotAnAPI", "params": "*REDACTED*", "request_url": "*REDACTED*"}',
            id="NotAnAPIBeforeCallTest",
        ),
    ),
)
def test_log_before_call(
    event_name: str, params: Dict[str, Any], expected_result: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(0)
    log_before_call(event_name, params)
    assert caplog.messages == [expected_result]


@pytest.mark.parametrize(
    argnames=("api_name", "params"),
    argvalues=(
        pytest.param(
            "cloudwatch-logs.PutLogEvent",
            {
                "url_path": "/",
                "body": '{"requestParam": "requestValue"}',
                "url": "https://cloudwatch-logs.us-west-2.amazonaws.com/",
            },
            id="IgnoreCloudWatchBeforeCall",
        ),
        pytest.param(
            "s3.GetObject",
            {
                "url_path": "/",
                "body": '{"requestParam": "requestValue"}',
                "url": "https://s3.us-west-2.amazonaws.com/",
            },
            id="IgnoreS3BeforeCall",
        ),
    ),
)
def test_log_before_ignore_list(
    api_name: str, params: Dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(0)
    log_before_call("before-call." + api_name, params)
    assert all(api_name not in msg for msg in caplog.messages)


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
            '{"log_type": "boto_response", "operation": "deadline.CreateWorker", "status_code": 200, "params": {"workerId": "worker-38aab2f87d2b45298b39875639580970"}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="CreateWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.AssumeFleetRoleForWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "credentials": {
                    "accessKeyId": "accesskey",
                    "secretAccessKey": "secretkey",
                    "sessionToken": "token",
                    "expiration": "some date",
                },
            },
            '{"log_type": "boto_response", "operation": "deadline.AssumeFleetRoleForWorker", "status_code": 200, "params": {"credentials": {"accessKeyId": "accesskey", "secretAccessKey": "*REDACTED*", "sessionToken": "*REDACTED*", "expiration": "some date"}}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="AssumeFleetRoleForWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.AssumeQueueRoleForWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "credentials": {
                    "accessKeyId": "accesskey",
                    "secretAccessKey": "secretkey",
                    "sessionToken": "token",
                    "expiration": "some date",
                },
            },
            '{"log_type": "boto_response", "operation": "deadline.AssumeQueueRoleForWorker", "status_code": 200, "params": {"credentials": {"accessKeyId": "accesskey", "secretAccessKey": "*REDACTED*", "sessionToken": "*REDACTED*", "expiration": "some date"}}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="AssumeQueueRoleForWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.UpdateWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "log": {
                    "logDriver": "AWS",
                    "options": {"option1": "foo"},
                    "parameters": {"param1": "bar"},
                    "error": "AccessDeniedException",
                },
            },
            '{"log_type": "boto_response", "operation": "deadline.UpdateWorker", "status_code": 200, "params": {"log": {"logDriver": "AWS", "options": {"option1": "foo"}, "parameters": {"param1": "bar"}, "error": "AccessDeniedException"}}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="UpdateWorkerAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.UpdateWorkerSchedule",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "assignedSessions": {
                    "session-e4ffe548f48d456ca11b5337bdc7a175": {
                        "queueId": "queue-f9848b67822d49299296e3e47d8cc523",
                        "jobId": "job-ac95524e7128498b9082375f8e3d7665",
                        "sessionActions": [
                            {
                                "sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-0",
                                "definition": {
                                    "envEnter": {
                                        "environmentId": "JOB:job-ac95524e7128498b9082375f8e3d7665:TestEnvironment"
                                    }
                                },
                            },
                            {
                                "sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-1",
                                "definition": {
                                    "envEnter": {
                                        "environmentId": "STEP:step-ff15e0f18561495399cb07b64342b538:myenv"
                                    }
                                },
                            },
                            {
                                "sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-2",
                                "definition": {
                                    "taskRun": {
                                        "taskId": "task-ff15e0f18561495399cb07b64342b538-0",
                                        "stepId": "step-ff15e0f18561495399cb07b64342b538",
                                        "parameters": {"Foo": "FooValue"},
                                    }
                                },
                            },
                            {
                                "sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-3",
                                "definition": {
                                    "syncInputJobAttachments": {
                                        "stepId": "step-ff15e0f18561495399cb07b64342b500",
                                    }
                                },
                            },
                        ],
                        "logConfiguration": {
                            "logDriver": "AWS",
                            "options": {"option1": "foo"},
                            "parameters": {"param1": "bar"},
                            "error": "AccessDeniedException",
                        },
                    },
                },
            },
            '{"log_type": "boto_response", "operation": "deadline.UpdateWorkerSchedule", "status_code": 200, "params": {"assignedSessions": {"session-e4ffe548f48d456ca11b5337bdc7a175": {"queueId": "queue-f9848b67822d49299296e3e47d8cc523", "jobId": "job-ac95524e7128498b9082375f8e3d7665", "sessionActions": [{"sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-0", "definition": {"envEnter": {"environmentId": "JOB:job-ac95524e7128498b9082375f8e3d7665:TestEnvironment"}}}, {"sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-1", "definition": {"envEnter": {"environmentId": "STEP:step-ff15e0f18561495399cb07b64342b538:myenv"}}}, {"sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-2", "definition": {"taskRun": {"taskId": "task-ff15e0f18561495399cb07b64342b538-0", "stepId": "step-ff15e0f18561495399cb07b64342b538", "parameters": "*REDACTED*"}}}, {"sessionActionId": "sessionaction-e4ffe548f48d456ca11b5337bdc7a175-3", "definition": {"syncInputJobAttachments": {"stepId": "step-ff15e0f18561495399cb07b64342b500"}}}], "logConfiguration": {"logDriver": "AWS", "options": {"option1": "foo"}, "parameters": {"param1": "bar"}, "error": "AccessDeniedException"}}}}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="UpdateWorkerScheduleAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.BatchGetJobEntity",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "entities": [
                    {
                        "jobDetails": {
                            "jobId": "job-ac95524e7128498b9082375f8e3d7665",
                            "jobAttachmentSettings": {
                                "s3BucketName": "bucketname",
                                "rootPrefix": "assets/",
                            },
                            "jobRunAsUser": {
                                "posix": {"user": "jobuser", "group": "jobuser"},
                                "windows": {
                                    "user": "jobuser",
                                    "passwordArn": "arn:aws:secretsmanager:us-west-2:000000000000:secret:PasswordSecret-qsrF9d",
                                },
                                "runAs": "QUEUE_CONFIGURED_USER",
                            },
                            "logGroupName": "/aws/deadline/farm-1b84ca8d938d47d99a00675ff4eedd41/queue-f9848b67822d49299296e3e47d8cc523",
                            "queueRoleArn": "arn:aws:iam::000000000000:role/QueueRole",
                            "parameters": {"Foo": "FooValue"},
                            "schemaVersion": "jobtemplate-2023-09",
                        },
                        "stepDetails": {
                            "jobId": "job-ac95524e7128498b9082375f8e3d7665",
                            "stepId": "step-ff15e0f18561495399cb07b64342b538",
                            "schemaVersion": "jobtemplate-2023-09",
                            "template": {
                                "name": "StepName",
                                "script": {
                                    "actions": {"onRun": {"command": "echo", "args": ["Hi"]}}
                                },
                            },
                            "dependencies": ["step-ff15e0f18561495399cb07b64342b500"],
                        },
                        "environmentDetails": {
                            "jobId": "job-ac95524e7128498b9082375f8e3d7665",
                            "environmentId": "JOB:job-ac95524e7128498b9082375f8e3d7665:TestEnvironment",
                            "schemaVersion": "jobtemplate-2023-09",
                            "template": {
                                "name": "TestEnvironment",
                                "script": {
                                    "actions": {"onEnter": {"command": "echo", "args": ["hi"]}}
                                },
                            },
                        },
                        "jobAttachmentDetails": {
                            "jobId": "job-ac95524e7128498b9082375f8e3d7665",
                            "attachments": {
                                "manifests": [
                                    {
                                        "fileSystemLocationName": "Filesystem",
                                        "rootPath": "/mnt/shared",
                                        "rootPathFormat": "posix",
                                        "outputRelativeDirectories": ["../output"],
                                        "inputManifestPath": "manifest_file",
                                        "inputManifestHash": "1234",
                                    }
                                ],
                                "fileSystem": "COPIED",
                            },
                        },
                    }
                ],
            },
            '{"log_type": "boto_response", "operation": "deadline.BatchGetJobEntity", "status_code": 200, "params": {"entities": [{"jobDetails": {"jobId": "job-ac95524e7128498b9082375f8e3d7665", "jobAttachmentSettings": {"s3BucketName": "bucketname", "rootPrefix": "assets/"}, "jobRunAsUser": {"posix": {"user": "jobuser", "group": "jobuser"}, "windows": {"user": "jobuser", "passwordArn": "arn:aws:secretsmanager:us-west-2:000000000000:secret:PasswordSecret-qsrF9d"}, "runAs": "QUEUE_CONFIGURED_USER"}, "logGroupName": "/aws/deadline/farm-1b84ca8d938d47d99a00675ff4eedd41/queue-f9848b67822d49299296e3e47d8cc523", "queueRoleArn": "arn:aws:iam::000000000000:role/QueueRole", "parameters": "*REDACTED*", "schemaVersion": "jobtemplate-2023-09"}, "stepDetails": {"jobId": "job-ac95524e7128498b9082375f8e3d7665", "stepId": "step-ff15e0f18561495399cb07b64342b538", "schemaVersion": "jobtemplate-2023-09", "template": "*REDACTED*", "dependencies": ["step-ff15e0f18561495399cb07b64342b500"]}, "environmentDetails": {"jobId": "job-ac95524e7128498b9082375f8e3d7665", "environmentId": "JOB:job-ac95524e7128498b9082375f8e3d7665:TestEnvironment", "schemaVersion": "jobtemplate-2023-09", "template": "*REDACTED*"}, "jobAttachmentDetails": {"jobId": "job-ac95524e7128498b9082375f8e3d7665", "attachments": {"manifests": [{"fileSystemLocationName": "Filesystem", "rootPath": "*REDACTED*", "rootPathFormat": "posix", "outputRelativeDirectories": "*REDACTED*", "inputManifestPath": "manifest_file", "inputManifestHash": "1234"}], "fileSystem": "COPIED"}}}]}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="BatchGetJobEntityAfterCallTest",
        ),
        pytest.param(
            "after-call.deadline.DeleteWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
            },
            '{"log_type": "boto_response", "operation": "deadline.DeleteWorker", "status_code": 200, "params": {}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="DeleteWorkerAfterCallTest",
        ),
        # ====================
        # Unknown API tests -- make sure that we redact the params, but still report the API
        pytest.param(
            "before-call.deadline.NotAnAPI",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "ResponseParam": "ResponseValue",
            },
            '{"log_type": "boto_response", "operation": "deadline.NotAnAPI", "status_code": 200, "params": "*REDACTED*", "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="NotAnAPIBeforeCallTest",
        ),
        # ====================
        pytest.param(
            "after-call.secretsmanager.GetSecretValue",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "ARN": "arn:aws:secretsmanager:us-west-2:000000000000:secret:Secret-qsrF9d",
                "Name": "Secret",
                "VersionId": "6fb9f17a-f9a9-4729-af0f-df67e976484c",
                "SecretString": '{"username": "fakeuser", "password": "fakepassword"}',
                "SecretBinary": b"abdc",
                "VersionStages": ["AWSCURRENT"],
                "CreatedDate": datetime.datetime(
                    2021, 4, 27, 14, 8, 44, 337000, tzinfo=datetime.timezone.utc
                ),
            },
            '{"log_type": "boto_response", "operation": "secretsmanager.GetSecretValue", "status_code": 200, "params": {"ARN": "arn:aws:secretsmanager:us-west-2:000000000000:secret:Secret-qsrF9d", "Name": "Secret", "VersionId": "6fb9f17a-f9a9-4729-af0f-df67e976484c", "SecretString": "*REDACTED*", "SecretBinary": "*REDACTED*", "VersionStages": "*REDACTED*", "CreatedDate": "2021-04-27 14:08:44.337000+00:00"}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="GetSecretValueAfterCallTest",
        ),
        # ====================
        # Test that the error is logged
        pytest.param(
            "after-call.deadline.CreateWorker",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 500,
                },
                "Error": {"Message": "This is a test", "Code": "InternalServerException"},
                "reason": "CONFLICT_EXCEPTION",
            },
            '{"log_type": "boto_response", "operation": "deadline.CreateWorker", "status_code": 500, "params": {"reason": "CONFLICT_EXCEPTION"}, "error": {"Message": "This is a test", "Code": "InternalServerException"}, "request_id": "abc878ee-32b5-44d4-885f-29071648328c"}',
            id="ErrorCase",
        ),
    ),
)
def test_log_after_call(
    event_name: str, params: Dict[str, Any], expected_result: str, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(0)
    log_after_call(event_name, params)
    assert caplog.messages == [expected_result]


@pytest.mark.parametrize(
    argnames=("api_name", "params"),
    argvalues=(
        pytest.param(
            "cloudwatch-logs.PutLogEvent",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "SomeResponseParam": "ResponseValue",
            },
            id="IgnoreCloudWatchAfterCall",
        ),
        pytest.param(
            "s3.GetObject",
            {
                "ResponseMetadata": {
                    "RequestId": "abc878ee-32b5-44d4-885f-29071648328c",
                    "HTTPStatusCode": 200,
                },
                "SomeResponseParam": "ResponseValue",
            },
            id="IgnoreS3AfterCall",
        ),
    ),
)
def test_log_after_ignore_list(
    api_name: str, params: Dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(0)
    log_before_call("after-call." + api_name, params)

    assert all(api_name not in msg for msg in caplog.messages)
