# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

from typing import Any, Dict
import boto3
import botocore.client
import botocore.config
import botocore.exceptions
import pytest
import logging

from deadline_test_fixtures import Job, TaskStatus, PosixSessionUser, DeadlineClient

LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("worker")
@pytest.mark.parametrize("operating_system", ["linux"], indirect=True)
class TestJobSubmission:
    def test_success(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "Sleep Job",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {"actions": {"onRun": {"command": "/bin/sleep", "args": ["5"]}}},
                    },
                ],
            },
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_job_run_as_user(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        job_run_as_user: PosixSessionUser,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "whoami",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {
                            "embeddedFiles": [
                                {
                                    "name": "whoami",
                                    "type": "TEXT",
                                    "runnable": True,
                                    "data": "\n".join(
                                        [
                                            "#!/bin/bash",
                                            'echo "I am: $(whoami)"',
                                        ]
                                    ),
                                },
                            ],
                            "actions": {
                                "onRun": {
                                    "command": "{{ Task.File.whoami }}",
                                },
                            },
                        },
                    },
                ],
            },
        )

        # THEN
        job.wait_until_complete(client=deadline_client)

        # Retrieve job output and verify whoami printed the queue's jobsRunAsUser
        job_logs = job.get_logs(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
        )
        full_log = "\n".join(
            [le.message for _, log_events in job_logs.logs.items() for le in log_events]
        )
        assert (
            f"I am: {job_run_as_user.user}" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_failed_job_reports_failed_onrun(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
    ) -> None:

        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "syncInputfail",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": "/bin/false ",
                                },
                            },
                        },
                    },
                ],
            },
        )
        # THEN
        job.wait_until_complete(client=deadline_client)

        # Retrieve job output and verify that the taskRun session action has failed

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        found_failed_session_action: bool = False

        for session in sessions:
            session_actions = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")
            for session_action in session_actions:
                # Session action should be failed IFF it's taskRun
                if "taskRun" in session_action["definition"]:
                    found_failed_session_action = True
                    assert session_action["status"] == "FAILED"
                else:
                    assert session_action["status"] != "FAILED"
        assert found_failed_session_action

    @pytest.mark.parametrize(
        "environment_actions, expected_failed_action",
        [
            (
                {
                    "onEnter": {
                        "command": "/bin/false",
                    },
                },
                "envEnter",
            ),
            (
                {
                    "onEnter": {
                        "command": "/bin/true",
                    },
                    "onExit": {
                        "command": "/bin/false",
                    },
                },
                "envExit",
            ),
        ],
    )
    def test_failed_job_reports_failed_environment_action(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        environment_actions: Dict[str, Any],
        expected_failed_action: str,
    ) -> None:

        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "environmentactionfail",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": "/bin/sleep",
                                    "args": ["1"],
                                },
                            }
                        },
                    },
                ],
                "jobEnvironments": [
                    {"name": "badenvironment", "script": {"actions": environment_actions}}
                ],
            },
        )
        # THEN
        job.wait_until_complete(client=deadline_client)

        # Retrieve job output and verify that the taskRun session action has failed

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        found_failed_session_action: bool = False
        for session in sessions:
            session_actions = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")

            for session_action in session_actions:

                # Session action should be failed IFF it's the expected action to fail
                if expected_failed_action in session_action["definition"]:
                    found_failed_session_action = True
                    assert session_action["status"] == "FAILED"
                else:
                    assert session_action["status"] != "FAILED"
        assert found_failed_session_action
