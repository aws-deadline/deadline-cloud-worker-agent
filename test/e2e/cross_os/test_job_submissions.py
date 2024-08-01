# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""
from typing import Any, Dict, List
import pytest
import logging
from deadline_test_fixtures import Job, DeadlineClient, TaskStatus
from utils import get_operating_system_name
import backoff
import boto3
import botocore.client
import botocore.config
import botocore.exceptions

LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("session_worker")
@pytest.mark.parametrize("operating_system", [get_operating_system_name()], indirect=True)
class TestJobSubmission:
    @pytest.mark.parametrize(
        "run_actions,environment_actions, expected_failed_action",
        [
            (
                {
                    "onRun": {
                        "command": "noneexistentcommand",  # This will fail
                    },
                },
                {
                    "onEnter": {
                        "command": "whoami",
                    },
                },
                "taskRun",
            ),
            (
                {
                    "onRun": {
                        "command": "whoami",
                    },
                },
                {
                    "onEnter": {
                        "command": "noneexistentcommand",  # This will fail
                    },
                },
                "envEnter",
            ),
            (
                {
                    "onRun": {
                        "command": "whoami",
                    },
                },
                {
                    "onEnter": {
                        "command": "whoami",
                    },
                    "onExit": {
                        "command": "noneexistentcommand",  # This will fail
                    },
                },
                "envExit",
            ),
        ],
    )
    def test_job_reports_failed_session_action(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        run_actions: Dict[str, Any],
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
                "name": f"jobactionfail-{expected_failed_action}",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {"actions": run_actions},
                    },
                ],
                "jobEnvironments": [
                    {"name": "badenvironment", "script": {"actions": environment_actions}}
                ],
            },
        )
        # THEN
        job.wait_until_complete(client=deadline_client)

        # Retrieve job output and verify that the expected session action has failed

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

    @pytest.mark.parametrize(
        "run_actions,environment_actions,expected_canceled_action",
        [
            (
                {
                    "onRun": {
                        "command": (
                            "/bin/sleep" if get_operating_system_name() == "linux" else "timeout"
                        ),
                        "args": ["40"],
                        "cancelation": {
                            "mode": "NOTIFY_THEN_TERMINATE",
                            "notifyPeriodInSeconds": 1,
                        },
                    },
                },
                {
                    "onEnter": {
                        "command": "whoami",
                    },
                },
                "taskRun",
            ),
            (
                {
                    "onRun": {
                        "command": "whoami",
                    },
                },
                {
                    "onEnter": {
                        "command": (
                            "/bin/sleep" if get_operating_system_name() == "linux" else "timeout"
                        ),
                        "args": ["40"],
                        "cancelation": {
                            "mode": "NOTIFY_THEN_TERMINATE",
                            "notifyPeriodInSeconds": 1,
                        },
                    },
                },
                "envEnter",
            ),
        ],
    )
    def test_job_reports_canceled_session_action(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        run_actions: Dict[str, Any],
        environment_actions: Dict[str, Any],
        expected_canceled_action: str,
    ) -> None:
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": f"jobactioncancel-{expected_canceled_action}",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {
                            "actions": run_actions,
                        },
                    },
                ],
                "jobEnvironments": [
                    {
                        "name": "environment",
                        "script": {
                            "actions": environment_actions,
                        },
                    }
                ],
            },
        )

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def is_job_started(current_job: Job) -> bool:
            current_job.refresh_job_info(client=deadline_client)
            LOG.info(f"Waiting for job {current_job.id} to be created")
            return current_job.lifecycle_status != "CREATE_IN_PROGRESS"

        assert is_job_started(job)

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def sessions_exist(current_job: Job) -> bool:
            sessions = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")

            return len(sessions) > 0

        assert sessions_exist(job)

        deadline_client.update_job(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id, targetTaskRunStatus="CANCELED"
        )

        # THEN

        # Wait until the job is canceled or completed
        job.wait_until_complete(client=deadline_client)

        LOG.info(f"Job result: {job}")

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def is_expected_session_action_canceled(sessions: List[Dict[str, Any]]) -> bool:
            found_canceled_session_action: bool = False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")

                LOG.info(f"Session Actions: {session_actions}")
                for session_action in session_actions:

                    # Session action should be canceled if it's the action we expect to be canceled
                    if expected_canceled_action in session_action["definition"]:
                        if session_action["status"] == "CANCELED":
                            found_canceled_session_action = True
                    else:
                        assert (
                            session_action["status"] != "CANCELED"
                        )  # This should not happen at all, so we fast exit
            return found_canceled_session_action

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        assert is_expected_session_action_canceled(sessions)

    @pytest.mark.parametrize(
        "job_environments",
        [
            ([]),
            (
                [
                    {
                        "name": "environment_1",
                        "script": {
                            "actions": {
                                "onEnter": {"command": "echo", "args": ["Hello!"]},
                            },
                        },
                    },
                ]
            ),
            (
                [
                    {
                        "name": "environment_1",
                        "script": {
                            "actions": {
                                "onEnter": {"command": "echo", "args": ["Hello!"]},
                            }
                        },
                    },
                    {
                        "name": "environment_2",
                        "script": {
                            "actions": {
                                "onEnter": {"command": "echo", "args": ["Hello!"]},
                            }
                        },
                    },
                    {
                        "name": "environment_3",
                        "script": {
                            "actions": {
                                "onEnter": {"command": "echo", "args": ["Hello!"]},
                            }
                        },
                    },
                ]
            ),
        ],
    )
    def test_worker_run_with_number_of_environments(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        job_environments: List[Dict[str, Any]],
    ) -> None:
        job_template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": f"jobWithNumberOfEnvironments-{len(job_environments)}",
            "steps": [
                {
                    "name": "Step0",
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "whoami",
                            },
                        },
                    },
                },
            ],
        }

        if len(job_environments) > 0:
            job_template["jobEnvironments"] = job_environments
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template=job_template,
        )

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

        assert full_log.count("Hello!") == len(
            job_environments
        ), "Expected number of Hello statements not found in job logs."

        assert job.task_run_status == TaskStatus.SUCCEEDED
