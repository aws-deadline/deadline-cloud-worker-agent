# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""
import hashlib
from flaky import flaky
import json
import pathlib
from typing import Any, Dict, List, Optional
import pytest
import logging
from deadline_test_fixtures import (
    Job,
    DeadlineClient,
    PosixSessionUser,
    TaskStatus,
    EC2InstanceWorker,
)
from e2e.conftest import DeadlineResources
import backoff
import boto3
import botocore.client
import botocore.config
import botocore.exceptions
import re
import time
from deadline.client.config import set_setting
from deadline.client import api
import uuid
import os
import configparser
import tempfile
from e2e.utils import wait_for_job_output, submit_sleep_job, submit_custom_job


LOG = logging.getLogger(__name__)


class TestJobSubmission:
    def test_success(
        self,
        deadline_resources,
        session_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        # WHEN

        job = submit_sleep_job(
            "Test Success Sleep Job",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific queue crendentials test",
    )
    def test_queue_credentials_file_is_secure_from_other_users(
        self,
        deadline_resources,
        session_worker: EC2InstanceWorker,
        posix_job_user: PosixSessionUser,
        generic_non_queue_job_user: PosixSessionUser,
        deadline_client: DeadlineClient,
    ) -> None:
        # Test to verify that the queue credentials can never be accessed by a different user on the same machine

        job = submit_custom_job(
            "Test Sleep",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            """
            #!/usr/bin/env bash
            sleep 90
            """,
        )

        try:

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

            @backoff.on_predicate(backoff.constant, interval=5, max_time=60)
            def sessions_exist(current_job: Job) -> bool:
                sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                    farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
                ).get("sessions")

                return len(sessions) > 0

            assert sessions_exist(job)

            queue_credentials_directory = f"/var/lib/deadline/queues/{job.queue.id}"

            # Verify that the queue user is able to access the credentials file
            check_queue_user_can_access_credentials_result = session_worker.send_command(
                command=f"sudo -u {posix_job_user.user} [ -e '{queue_credentials_directory}/aws_credentials.json' ]"
            )
            assert check_queue_user_can_access_credentials_result.exit_code == 0

            # Verify that any other users are not able to access the credential files

            check_other_user_cannot_access_credentials_result = session_worker.send_command(
                command=f"sudo -u {generic_non_queue_job_user.user} [ -e '{queue_credentials_directory}/aws_credentials.json' ]"
            )

            assert check_other_user_cannot_access_credentials_result.exit_code != 0

        finally:
            deadline_client.update_job(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                targetTaskRunStatus="CANCELED",
            )
            job.wait_until_complete(client=deadline_client)

        return

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific queue crendentials test",
    )
    def test_queue_credentials_file_is_secure_from_other_queues(
        self,
        deadline_resources,
        session_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        # Test to verify that the queue credentials can never be accessed by a different queue's job user

        job = submit_sleep_job(
            "Test Sleep",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        try:

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

            @backoff.on_predicate(backoff.constant, interval=5, max_time=60)
            def sessions_exist(current_job: Job) -> bool:
                sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                    farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
                ).get("sessions")

                return len(sessions) > 0

            assert sessions_exist(job)

            queue_credentials_directory = f"/var/lib/deadline/queues/{job.queue.id}"

            # Verify that another queue's user cannot access the credentials file through a job
            second_queue_job = submit_custom_job(
                "Test Getting Primary Queue Credentials File",
                deadline_client,
                deadline_resources.farm,
                deadline_resources.queue_b,
                f"""
                #!/usr/bin/env bash
                cat {queue_credentials_directory}/aws_credentials.json
                """,
                max_retries_per_task=0,
            )
            try:
                second_queue_job.wait_until_complete(client=deadline_client)
                assert second_queue_job.task_run_status == TaskStatus.FAILED

            finally:
                deadline_client.update_job(
                    farmId=second_queue_job.farm.id,
                    queueId=second_queue_job.queue.id,
                    jobId=second_queue_job.id,
                    targetTaskRunStatus="CANCELED",
                )
                second_queue_job.wait_until_complete(client=deadline_client)

        finally:
            deadline_client.update_job(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                targetTaskRunStatus="CANCELED",
            )
            job.wait_until_complete(client=deadline_client)

        return

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific worker log test",
    )
    def test_worker_writes_logs_to_disk_securely(
        self,
        deadline_resources,
        session_worker: EC2InstanceWorker,
        posix_job_user: PosixSessionUser,
        deadline_client: DeadlineClient,
    ) -> None:
        # WHEN

        job = submit_sleep_job(
            "Test Success Sleep Job",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
        ).get("sessions")
        assert sessions

        worker_logs_directory: str = "/var/log/amazon/deadline"
        # Check that the session log file is accessible by the worker agent user only
        for session in sessions:
            session_id: str = session["sessionId"]
            session_logs_file_path: str = os.path.join(
                worker_logs_directory, job.queue.id, f"{session_id}.log"
            )

            check_session_log_exists_result = session_worker.send_command(
                command=f"sudo -u deadline-worker [ -e '{session_logs_file_path}' ]"
            )
            assert (
                check_session_log_exists_result.exit_code == 0
            )  # The -e command returns 0 on linux if the file does  exist

            # Check that the session log file is not accessible by the job  user
            check_session_log_exists_result = session_worker.send_command(
                command=f"sudo -u {posix_job_user.user} [ -e '{session_logs_file_path}' ]"
            )
            assert (
                check_session_log_exists_result.exit_code == 1
            )  # The job user should not have access to the file

        # Check that the worker agent log file is accessible by the worker user only

        check_worker_log_exists_result = session_worker.send_command(
            command=f"sudo -u deadline-worker [ -e '{worker_logs_directory}/worker-agent.log' ]"
        )
        assert check_worker_log_exists_result.exit_code == 0

        # Check that the worker agent log file is not accessible by the job user
        check_worker_log_accessible_by_job_user_result = session_worker.send_command(
            command=f"sudo -u {posix_job_user.user} [ -e '{worker_logs_directory}/worker-agent.log' ]"
        )
        assert check_worker_log_accessible_by_job_user_result.exit_code == 1

        # Check that the worker agent bootstrap log file is accessible by the worker user only
        check_worker_bootstrap_log_exists_result = session_worker.send_command(
            command=f"sudo -u deadline-worker [ -e '{worker_logs_directory}/worker-agent-bootstrap.log' ]"
        )
        assert check_worker_bootstrap_log_exists_result.exit_code == 0

        # Check that the worker agent bootstrap log file is not accessible by the job user
        check_worker_bootstrap_log_accessible_by_job_user_result = session_worker.send_command(
            command=f"sudo -u {posix_job_user.user} [ -e '{worker_logs_directory}/worker-agent-bootstrap.log' ]"
        )
        assert check_worker_bootstrap_log_accessible_by_job_user_result.exit_code == 1

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
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        run_actions: Dict[str, Any],
        environment_actions: Dict[str, Any],
        expected_failed_action: str,
    ) -> None:

        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            max_retries_per_task=0,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": f"jobactionfail-{expected_failed_action}",
                "steps": [
                    {
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "name": "Step0",
                        "script": {"actions": run_actions},
                    },
                ],
                "jobEnvironments": [
                    {"name": "badenvironment", "script": {"actions": environment_actions}}
                ],
            },
        )

        # Wait until the job is completed
        job.wait_until_complete(client=deadline_client)

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=60,
            interval=10,
        )
        def is_expected_session_action_failed(sessions: List[Dict[str, Any]]) -> bool:
            found_failed_session_action: bool = False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")

                LOG.info(f"Session actions: {session_actions}")
                for session_action in session_actions:
                    # Session action should be failed IFF it's the expected action to fail
                    if expected_failed_action in session_action["definition"]:
                        if session_action["status"] == "FAILED":
                            found_failed_session_action = True
                    else:
                        assert (
                            session_action["status"] != "FAILED"
                        ), f"Session action that should not have failed is in FAILED status. {session_action}"
            return found_failed_session_action

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        assert is_expected_session_action_failed(sessions)

    def test_worker_fails_session_action_timeout(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:
        # Test that if a task takes longer than the timeout defined, the session action goes to FAILED status
        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            max_retries_per_task=1,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "JobSessionActionTimeoutFail",
                "steps": [
                    {
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "name": "Step0",
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": (
                                        "/bin/sleep"
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else "powershell"
                                    ),
                                    "args": (
                                        ["40"]
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else ["ping", "localhost", "-n", "40"]
                                    ),
                                    "timeout": 1,  # Times out in 1 second
                                    "cancelation": {
                                        "mode": "NOTIFY_THEN_TERMINATE",
                                        "notifyPeriodInSeconds": 1,
                                    },
                                },
                            },
                        },
                    },
                ],
            },
        )

        # THEN

        # Wait until the job is completed
        job.wait_until_complete(client=deadline_client)

        found_task_run_action: bool = False
        sessions: List[Dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        for session in sessions:
            session_actions: List[Dict[str, Any]] = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")

            LOG.info(f"Session Actions: {session_actions}")
            for session_action in session_actions:
                # taskRun session action should be failed
                if "taskRun" in session_action["definition"]:
                    found_task_run_action = True
                    session_action_id: str = session_action["sessionActionId"]
                    get_session_action_response: Dict[str, Any] = (
                        deadline_client.get_session_action(
                            farmId=job.farm.id,
                            queueId=job.queue.id,
                            jobId=job.id,
                            sessionActionId=session_action_id,
                        )
                    )
                    assert get_session_action_response[
                        "status"
                    ] == "FAILED" and "TIMEOUT" in get_session_action_response.get(
                        "progressMessage", ""
                    ), f"taskRun action should have FAILED {get_session_action_response} with 'TIMEOUT' in the progressMessage"

        assert found_task_run_action

    @pytest.mark.parametrize(
        "run_actions,environment_actions,expected_canceled_action",
        [
            (
                {
                    "onRun": {
                        "command": (
                            "/bin/sleep"
                            if os.environ["OPERATING_SYSTEM"] == "linux"
                            else "powershell"
                        ),
                        "args": (
                            ["40"]
                            if os.environ["OPERATING_SYSTEM"] == "linux"
                            else ["ping", "localhost", "-n", "40"]
                        ),
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
                            "/bin/sleep"
                            if os.environ["OPERATING_SYSTEM"] == "linux"
                            else "powershell"
                        ),
                        "args": (
                            ["40"]
                            if os.environ["OPERATING_SYSTEM"] == "linux"
                            else ["ping", "localhost", "-n", "40"]
                        ),
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
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        run_actions: Dict[str, Any],
        environment_actions: Dict[str, Any],
        expected_canceled_action: str,
    ) -> None:
        job: Job = Job.submit(
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
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
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
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
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
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
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
                    elif "envExit" in session_action["definition"]:
                        # envExit should always run no matter what
                        if session_action["status"] != "SUCCEEDED":
                            return False
                    else:
                        if expected_canceled_action == "envEnter":
                            # If we canceled the envEnter, everything else should have been NEVER_ATTEMPTED
                            assert session_action["status"] == "NEVER_ATTEMPTED"
                        else:
                            assert session_action["status"] == "SUCCEEDED"
            return found_canceled_session_action

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        assert is_expected_session_action_canceled(sessions)

    @pytest.mark.parametrize("expected_canceled_action", ["envEnter", "taskRun"])
    def test_worker_reports_canceled_session_actions_as_canceled(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        expected_canceled_action: str,
    ) -> None:
        # Tests that when running a job session action with a trap for SIGINT, the corresponding session action is canceled almost immediately.
        action_script: str = (
            "#!/usr/bin/env bash\n trap 'exit 0' SIGINT\n bash\n\n sleep 300\n "
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else """try
                {
                    Start-Sleep -Seconds 300
                }
                finally
                {
                    Exit
                }"""
        )

        environment_exit_id = str(uuid.uuid4())
        # Submit a job that either sleeps a long time during envEnter, or taskRun, depending on the test setting
        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": f"jobactioncanceltrap-{expected_canceled_action}",
                "steps": [
                    {
                        "name": "Step0",
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "script": {
                            "actions": {
                                "onRun": (
                                    {"command": "{{ Task.File.runScript }}"}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ["{{ Task.File.runScript }}"],
                                    }
                                ),
                            },
                            "embeddedFiles": [
                                {
                                    "name": "runScript",
                                    "type": "TEXT",
                                    "runnable": True,
                                    "data": (
                                        action_script
                                        if expected_canceled_action == "taskRun"
                                        else "whoami"
                                    ),
                                    **(
                                        {"filename": "sleepscript.ps1"}
                                        if os.environ["OPERATING_SYSTEM"] == "windows"
                                        else {}
                                    ),
                                }
                            ],
                        },
                    },
                ],
                "jobEnvironments": [
                    {
                        "name": "environment",
                        "script": {
                            "actions": {
                                "onEnter": (
                                    (
                                        {"command": "{{ Env.File.runScript }}"}
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else {
                                            "command": "powershell",
                                            "args": ["{{ Env.File.runScript }}"],
                                        }
                                    )
                                    if expected_canceled_action == "envEnter"
                                    else {"command": "whoami"}
                                ),
                                "onExit": (
                                    (
                                        {
                                            "command": "echo",
                                            "args": ["Environment exit " + environment_exit_id],
                                        }
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else {
                                            "command": "powershell",
                                            "args": [
                                                '"Environment"',
                                                "+",
                                                '" exit "',
                                                "+",
                                                f'"{environment_exit_id}"',
                                            ],
                                        }
                                    )
                                ),
                            },
                            "embeddedFiles": [
                                {
                                    "name": "runScript",
                                    "type": "TEXT",
                                    "runnable": True,
                                    "data": (
                                        action_script
                                        if expected_canceled_action == "envEnter"
                                        else "whoami"
                                    ),
                                    **(
                                        {"filename": "sleepscript.ps1"}
                                        if os.environ["OPERATING_SYSTEM"] == "windows"
                                        else {}
                                    ),
                                }
                            ],
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
            logging.info(f"Waiting for job {current_job.id} to be created")
            return current_job.lifecycle_status != "CREATE_IN_PROGRESS"

        assert is_job_started(job)

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def action_to_cancel_has_started(current_job: Job) -> bool:
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")

            if len(sessions) == 0:
                return False
            for session in sessions:
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")

                logging.info(f"Session Actions: {session_actions}")
                for session_action in session_actions:

                    # Session action should be canceled if it's the action we expect to be canceled
                    if expected_canceled_action in session_action["definition"]:
                        if session_action["status"] == "RUNNING":
                            return True
            return False

        # Wait for the sleep action that we want to cancel to start, before canceling it
        assert action_to_cancel_has_started(job)

        deadline_client.update_job(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id, targetTaskRunStatus="CANCELED"
        )

        # Check that the expected actions should be canceled way before the sleep ends.

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=60,
            interval=5,
        )
        def is_expected_session_action_canceled(sessions) -> bool:
            found_canceled_session_action: bool = False
            for session in sessions:
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")

                logging.info(f"Session Actions: {session_actions}")
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

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        assert is_expected_session_action_canceled(sessions)

        # Wait until the job is completed

        job.wait_until_complete(client=deadline_client)

        # Verify that envExit was ran, if the action being canceled in question is the taskRun, not the envEnter
        if expected_canceled_action == "taskRun":
            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=boto3.client(
                    "logs",
                    config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
                ),
                expected_pattern=rf'{"Environment exit " + environment_exit_id}',
            )

        # Test that worker continues polling for work
        job = submit_sleep_job(
            "Test Worker after Job Canceled",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert (
            job.task_run_status == TaskStatus.SUCCEEDED
        ), "Worker failed to continue polling for work after job cancelation"

    @flaky(max_runs=3, min_passes=1)  # Flaky as sync input sometimes completes before expected.
    def test_worker_reports_canceled_sync_input_actions_as_canceled(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        tmp_path,
    ) -> None:
        # Test that when syncing input job attachments and the user cancels the job, the syncInputJobAttachments session actions are canceled
        # Create the template file, the job won't actually do anything substantial
        job_parameters: List[Dict[str, str]] = [
            {"name": "DataDir", "value": tmp_path},
        ]
        with open(os.path.join(tmp_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": "SyncInputsJob",
                        "parameterDefinitions": [
                            {
                                "name": "DataDir",
                                "type": "PATH",
                                "dataFlow": "INOUT",
                            },
                        ],
                        "steps": [
                            {
                                "name": "WhoamiStep",
                                "hostRequirements": {
                                    "attributes": [
                                        {
                                            "name": "attr.worker.os.family",
                                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                                        }
                                    ]
                                },
                                "script": {
                                    "actions": {"onRun": {"command": "whoami"}},
                                },
                            }
                        ],
                    }
                )
            )
        # Create the input files to make sync inputs take a relatively long time
        files_path: str = os.path.join(tmp_path, "files")
        os.mkdir(files_path)
        for i in range(6000):
            file_name: str = os.path.join(files_path, f"input_file_{i+1}.txt")
            with open(file_name, "w+") as input_file:
                if i % 1000 == 0:
                    # Create some big files (1GB each) so the syncInputAttachments don't fail due to low transfer rates
                    input_file.write("A" * 1000000000)
                else:
                    input_file.write(f"{i}")
        config = configparser.ConfigParser()

        set_setting("defaults.farm_id", deadline_resources.farm.id, config)
        set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)

        job_id: Optional[str] = api.create_job_from_job_bundle(
            tmp_path,
            job_parameters,
            priority=99,
            config=config,
            queue_parameter_definitions=[],
        )

        assert job_id is not None
        job_details: dict[str, Any] = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job: Job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=60,
            interval=2,
        )
        def sync_input_action_started(current_job: Job) -> bool:
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")
            if len(sessions) == 0:
                return False
            for session in sessions:
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                LOG.info(f"Session actions: {session_actions}")
                for session_action in session_actions:
                    if "syncInputJobAttachments" in session_action["definition"]:
                        if session_action["status"] in ["ASSIGNED", "RUNNING"]:
                            return True
            return False

        # Wait until the sync input action has started
        assert sync_input_action_started(job)

        deadline_client.update_job(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
            targetTaskRunStatus="CANCELED",
        )

        # Wait until the job is completed
        job.wait_until_complete(client=deadline_client)

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def sync_input_actions_are_canceled(sessions: List[Dict[str, Any]]) -> bool:
            found_canceled_sync_input_action: bool = False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                LOG.info(f"Session actions: {session_actions}")
                for session_action in session_actions:
                    # Session action should be canceled if it's the action we expect to be canceled
                    if "syncInputJobAttachments" in session_action["definition"]:
                        if session_action["status"] == "CANCELED":
                            found_canceled_sync_input_action = True
                    else:
                        assert (
                            session_action["status"] == "SUCCEEDED"
                            or session_action["status"] == "NEVER_ATTEMPTED"
                        )
            return found_canceled_sync_input_action

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")

        assert sync_input_actions_are_canceled(sessions)

    def test_worker_reports_never_attempted_tasks_if_task_is_canceled(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:

        # Tests that if a taskRun action is cancelled, all remaining taskRun actions that depend on it will be NEVER_ATTEMPTED

        step_one_name = "StepOneSucceeded"
        step_two_name = "StepTwoToCancel"
        step_three_name = "StepThreeNeverAttempted"
        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "TestSecondTaskRunCancelled",
                "jobEnvironments": [
                    {
                        "name": "WhoAmiJobEnvironment",
                        "script": {
                            "actions": {
                                "onEnter": ({"command": "whoami"}),
                                "onExit": ({"command": "whoami"}),
                            },
                        },
                    },
                ],
                "steps": [
                    {
                        "name": step_one_name,
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": (
                                        "/bin/sleep"
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else "powershell"
                                    ),
                                    "args": (
                                        ["1"]
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else ["ping", "localhost", "-n", "1"]
                                    ),
                                },
                            }
                        },
                    },
                    {
                        "name": step_two_name,
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "dependencies": [{"dependsOn": step_one_name}],
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": (
                                        "/bin/sleep"
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else "powershell"
                                    ),
                                    "args": (
                                        ["120"]
                                        if os.environ["OPERATING_SYSTEM"] == "linux"
                                        else ["ping", "localhost", "-n", "120"]
                                    ),
                                    "cancelation": {
                                        "mode": "NOTIFY_THEN_TERMINATE",
                                        "notifyPeriodInSeconds": 1,
                                    },
                                },
                            }
                        },
                    },
                    {
                        "name": step_three_name,
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "dependencies": [{"dependsOn": step_two_name}],
                        "script": {
                            "actions": {
                                "onRun": {"command": "whoami"},
                            }
                        },
                    },
                ],
            },
        )

        # Wait for the job to start

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def is_job_started_with_sessions(current_job: Job) -> bool:
            current_job.refresh_job_info(client=deadline_client)
            LOG.info(f"Waiting for job {current_job.id} to be created")
            if current_job.lifecycle_status == "CREATE_IN_PROGRESS":
                return False
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
            ).get("sessions")
            if sessions and len(sessions) > 0:
                return True
            return False

        assert is_job_started_with_sessions(job)

        # Find both the SUCCEEDED and RUNNING session action IDs

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=90,
            interval=5,
        )
        def find_succeeded_and_running_actions() -> tuple[str, str]:
            found_succeeded_action_id: Optional[str] = None
            found_running_action_id: Optional[str] = None

            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
            ).get("sessions")

            for session in sessions:
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                for session_action in session_actions:
                    definition: dict[str, Any] = session_action["definition"]
                    if "taskRun" in definition:
                        if session_action["status"] == "SUCCEEDED":
                            found_succeeded_action_id = session_action["sessionActionId"]
                        elif session_action["status"] == "RUNNING":
                            found_running_action_id = session_action["sessionActionId"]

            assert found_succeeded_action_id is not None
            assert found_running_action_id is not None

            return found_succeeded_action_id, found_running_action_id

        succeeded_action_id, running_action_id = find_succeeded_and_running_actions()
        deadline_client.update_job(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
            targetTaskRunStatus="CANCELED",
        )

        # Wait for the job to be canceled

        job.wait_until_complete(client=deadline_client)

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        for session in sessions:
            session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")
            for session_action in session_actions:
                definition: dict[str, Any] = session_action["definition"]
                if (
                    "envEnter" in definition
                    or "envExit" in definition
                    or (
                        "taskRun" in definition
                        and succeeded_action_id == session_action["sessionActionId"]
                    )
                ):
                    assert session_action["status"] == "SUCCEEDED"
                elif (
                    "taskRun" in definition
                    and running_action_id == session_action["sessionActionId"]
                ):
                    # The action that was running for a long time should now be CANCELED!
                    assert session_action["status"] == "CANCELED"
                else:
                    # Every other action should be in NEVER_ATTEMPTED status
                    assert session_action["status"] == "NEVER_ATTEMPTED"

    def test_worker_always_runs_env_exit_despite_failure(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:
        # Tests that whenever a envEnter on a job is attempted, the corresponding envExit is also ran despite session action failures

        successful_environment_name: str = "SuccessfulEnvironment"
        unsuccessful_environment_name: str = "UnsuccessfulEnvironment"
        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "TestEnvJobFail",
                "jobEnvironments": [
                    {
                        "name": successful_environment_name,
                        "script": {
                            "actions": {
                                "onEnter": ({"command": "whoami"}),
                                "onExit": ({"command": "whoami"}),
                            },
                        },
                    },
                    {
                        "name": unsuccessful_environment_name,
                        "script": {
                            "actions": {
                                "onEnter": ({"command": "nonexistentcommand"}),
                                "onExit": ({"command": "nonexistentcommand"}),
                            },
                        },
                    },
                ],
                "steps": [
                    {
                        "name": "Step0",
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "script": {
                            "actions": {
                                "onRun": ({"command": "whoami"}),
                            }
                        },
                    },
                ],
            },
        )
        # THEN

        # Wait until the job is completed
        job.wait_until_complete(client=deadline_client)

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")

        # Find that the both the unsuccessful and successful environment ran, with envExit and envEnter for each.
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=60,
            interval=2,
        )
        def check_environment_action_statuses_are_expected() -> None:
            found_successful_env_enter: bool = False
            found_unsuccessful_env_enter: bool = False
            found_unsuccessful_env_exit: bool = False
            found_successful_env_exit: bool = False
            for session in sessions:

                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                LOG.info(f"Session actions: {session_actions}")
                for session_action in session_actions:
                    definition = session_action["definition"]
                    if "envEnter" in definition:
                        if successful_environment_name in definition["envEnter"]["environmentId"]:
                            assert session_action["status"] == "SUCCEEDED"
                            found_successful_env_enter = True
                        elif (
                            unsuccessful_environment_name in definition["envEnter"]["environmentId"]
                        ):
                            assert session_action["status"] == "FAILED"
                            found_unsuccessful_env_enter = True
                    elif "envExit" in definition:
                        if successful_environment_name in definition["envExit"]["environmentId"]:
                            assert session_action["status"] == "SUCCEEDED"
                            found_successful_env_exit = True
                        elif (
                            unsuccessful_environment_name in definition["envExit"]["environmentId"]
                        ):
                            assert session_action["status"] == "FAILED"
                            found_unsuccessful_env_exit = True

            assert (
                found_successful_env_enter
                and found_unsuccessful_env_enter
                and found_unsuccessful_env_exit
                and found_successful_env_exit
            )

        check_environment_action_statuses_are_expected()

    @pytest.mark.parametrize(
        "job_environments",
        [
            (
                [
                    {
                        "name": "environment_1",
                        "script": {
                            "actions": {
                                "onEnter": (
                                    {"command": "echo", "args": ["Hello!"]}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ['"Hello"', "+", '"!"'],
                                    }  # Separating the string is needed to prevent the expected string appearing in output logs more times than expected, as windows worker logs print the command
                                ),
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
                                "onEnter": (
                                    {"command": "echo", "args": ["Hello!"]}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ['"Hello"', "+", '"!"'],
                                    }  # Separating the string is needed to prevent the expected string appearing in output logs more times than expected, as windows worker logs print the command
                                ),
                            }
                        },
                    },
                    {
                        "name": "environment_2",
                        "script": {
                            "actions": {
                                "onEnter": (
                                    {"command": "echo", "args": ["Hello!"]}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ['"Hello"', "+", '"!"'],
                                    }  # Separating the string is needed to prevent the expected string appearing in output logs more times than expected, as windows worker logs print the command
                                ),
                            }
                        },
                    },
                    {
                        "name": "environment_3",
                        "script": {
                            "actions": {
                                "onEnter": (
                                    {"command": "echo", "args": ["Hello!"]}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ['"Hello"', "+", '"!"'],
                                    }  # Separating the string is needed to prevent the expected string appearing in output logs more times than expected, as windows worker logs print the command
                                ),
                            }
                        },
                    },
                ]
            ),
        ],
    )
    def test_worker_run_with_number_of_environments(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        job_environments: List[Dict[str, Any]],
    ) -> None:
        job_template: dict[str, Any] = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": f"jobWithNumberOfEnvironments-{len(job_environments)}",
            "steps": [
                {
                    "name": "Step0",
                    "hostRequirements": {
                        "attributes": [
                            {
                                "name": "attr.worker.os.family",
                                "allOf": [os.environ["OPERATING_SYSTEM"]],
                            }
                        ]
                    },
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

        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template=job_template,
        )

        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.SUCCEEDED

        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )

        if len(job_environments) == 1:
            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=logs_client,
                # pass in alldot pattern
                expected_pattern=r"Hello!",
                assert_fail_msg="Expected Number of Hello statements not found in job logs.",
            )

        if len(job_environments) == 3:
            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=logs_client,
                expected_pattern=re.compile(r"Hello!.*Hello!.*Hello!", re.DOTALL),
                assert_fail_msg="Expected Number of Hello statements not found in job logs.",
            )

    def test_worker_streams_logs_to_cloudwatch(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:

        job_start_time_seconds: float = time.time()
        job: Job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "Hello World Job",
                "steps": [
                    {
                        "name": "Step0",
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": [os.environ["OPERATING_SYSTEM"]],
                                }
                            ]
                        },
                        "script": {
                            "actions": {
                                "onRun": (
                                    {"command": "echo", "args": ["HelloWorld"]}
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else {
                                        "command": "powershell",
                                        "args": ['"Hello"', "+", '"World"'],
                                    }  # Separating the string is needed to prevent the expected string appearing in output logs more times than expected, as windows worker logs print the command
                                ),
                            }
                        },
                    },
                ],
            },
        )

        job.wait_until_complete(client=deadline_client)

        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )

        # Retrieve job output and verify the echo is printed

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=r"HelloWorld",
        )

        # Retrieve worker logs and verify that it's not empty
        worker_log_group_name: str = (
            f"/aws/deadline/{deadline_resources.farm.id}/{deadline_resources.fleet.id}"
        )
        worker_id: Optional[str] = session_worker.worker_id
        assert worker_id is not None

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=2,
        )
        def check_for_worker_log_event() -> bool:
            worker_logs = logs_client.get_log_events(
                logGroupName=worker_log_group_name,
                logStreamName=worker_id,
                startTime=int(job_start_time_seconds * 1000),
            )

            return len(worker_logs["events"]) > 0

        assert check_for_worker_log_event(), f"Could not find a worker log for {worker_id}"

    @pytest.mark.parametrize(
        "append_string_script",
        [
            (
                "#!/usr/bin/env bash\n\n  echo -n $(cat {{Param.DataDir}}/files/test_input_file){{Param.StringToAppend}} > {{Param.DataDir}}/output_file\n"
                if os.environ["OPERATING_SYSTEM"] == "linux"
                else '''set /p input=<"{{Param.DataDir}}\\files\\test_input_file"\n powershell -Command "echo ($env:input+\'{{Param.StringToAppend}}\') | Out-File -encoding utf8 {{Param.DataDir}}\\output_file -NoNewLine"'''
            )
        ],
    )
    def test_worker_uses_job_attachment_configuration(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        append_string_script: str,
    ) -> None:
        # Verify that the worker uses the correct job attachment configuration, and writes the output to the correct location

        test_run_uuid: str = str(uuid.uuid4())

        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__),
            "job_attachment_bundle",
        )
        job_parameters: List[Dict[str, str]] = [
            {"name": "StringToAppend", "value": test_run_uuid},
            {"name": "DataDir", "value": job_bundle_path},
        ]
        try:
            with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": "AppendStringJob",
                            "parameterDefinitions": [
                                {
                                    "name": "DataDir",
                                    "type": "PATH",
                                    "dataFlow": "INOUT",
                                },
                                {"name": "StringToAppend", "type": "STRING"},
                            ],
                            "steps": [
                                {
                                    "name": "AppendString",
                                    "hostRequirements": {
                                        "attributes": [
                                            {
                                                "name": "attr.worker.os.family",
                                                "allOf": [os.environ["OPERATING_SYSTEM"]],
                                            }
                                        ]
                                    },
                                    "script": {
                                        "actions": {
                                            "onRun": {"command": "{{ Task.File.runScript }}"}
                                        },
                                        "embeddedFiles": [
                                            {
                                                "name": "runScript",
                                                "type": "TEXT",
                                                "runnable": True,
                                                "data": append_string_script,
                                                **(
                                                    {"filename": "stringappendscript.bat"}
                                                    if os.environ["OPERATING_SYSTEM"] == "windows"
                                                    else {}
                                                ),
                                            }
                                        ],
                                    },
                                }
                            ],
                        }
                    )
                )

            config = configparser.ConfigParser()

            set_setting("defaults.farm_id", deadline_resources.farm.id, config)
            set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)

            job_id: Optional[str] = api.create_job_from_job_bundle(
                job_bundle_path,
                job_parameters,
                priority=99,
                config=config,
                queue_parameter_definitions=[],
            )
            assert job_id is not None
        finally:
            # Clean up the template file
            os.remove(os.path.join(job_bundle_path, "template.json"))

        job_details: dict[str, Any] = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job: Job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )

        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job, deadline_client=deadline_client, deadline_resources=deadline_resources
        )

        try:
            with (
                open(os.path.join(job_bundle_path, "files", "test_input_file"), "r") as input_file,
                open(
                    os.path.join(
                        list(output_path.keys())[0],
                        "output_file",
                    ),
                    "r",
                    encoding="utf-8-sig",
                ) as output_file,
            ):
                input_file_content: str = input_file.read()
                output_file_content = output_file.read()

                # Verify that the output file content is the input file content plus the uuid we appended in the job
                assert output_file_content == (input_file_content + test_run_uuid)
        finally:
            os.remove(os.path.join(list(output_path.keys())[0], "output_file"))

    def test_worker_job_attachments_no_outputs_does_not_fail_job(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:
        # Tests that if a job has no job output files in the output directory, the job does not fail. This tests prevents regressions in the output code

        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__),
            "job_attachment_bundle",
        )

        try:
            with (
                open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file,
                tempfile.TemporaryDirectory() as temporary_output_directory,
            ):

                job_parameters: List[Dict[str, str]] = [
                    {
                        "name": "OutputFilePath",
                        "value": temporary_output_directory,
                    },
                ]

                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": "NoOutputJob",
                            "parameterDefinitions": [
                                {
                                    "name": "OutputFilePath",
                                    "type": "PATH",
                                    "objectType": "DIRECTORY",
                                    "dataFlow": "OUT",
                                },
                            ],
                            "steps": [
                                {
                                    "name": "MainStep",
                                    "hostRequirements": {
                                        "attributes": [
                                            {
                                                "name": "attr.worker.os.family",
                                                "allOf": [os.environ["OPERATING_SYSTEM"]],
                                            }
                                        ]
                                    },
                                    "script": {
                                        "actions": {"onRun": {"command": "whoami"}},
                                    },
                                }
                            ],
                        }
                    )
                )
                config = configparser.ConfigParser()

            set_setting("defaults.farm_id", deadline_resources.farm.id, config)
            set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)
            job_id: Optional[str] = api.create_job_from_job_bundle(
                job_bundle_path,
                job_parameters,
                priority=99,
                config=config,
                queue_parameter_definitions=[],
                require_paths_exist=True,
            )
            assert job_id is not None
        finally:
            # Clean up the template file
            os.remove(os.path.join(job_bundle_path, "template.json"))

        job_details = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )
        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.SUCCEEDED

    @pytest.mark.skip(reason="Queue role permissions are failing the test during E2E test runs")
    def test_worker_fails_job_attachment_sync_when_non_valid_queue_role(
        self,
        deadline_resources: DeadlineResources,
        session_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        # Test that when submitting a job with job attachments to a queue with a role that cannot read the S3 bucket, the worker will fail the job attachments sync

        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__),
            "job_attachment_bundle",
        )
        job_parameters: List[Dict[str, str]] = [
            {"name": "DataDir", "value": job_bundle_path},
        ]
        append_string_script = (
            "#!/usr/bin/env bash\n\n  echo -n $(cat {{Param.DataDir}}/files/test_input_file)hi > {{Param.DataDir}}/output_file\n"
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else '''set /p input=<"{{Param.DataDir}}\\files\\test_input_file"\n powershell -Command "echo ($env:input+\'hi\') | Out-File -encoding utf8 {{Param.DataDir}}\\output_file -NoNewLine"'''
        )

        try:
            with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": "JobAttachmentToNonValidRoleQueue",
                            "parameterDefinitions": [
                                {
                                    "name": "DataDir",
                                    "type": "PATH",
                                    "dataFlow": "INOUT",
                                },
                            ],
                            "steps": [
                                {
                                    "name": "Step0",
                                    "hostRequirements": {
                                        "attributes": [
                                            {
                                                "name": "attr.worker.os.family",
                                                "allOf": [os.environ["OPERATING_SYSTEM"]],
                                            }
                                        ]
                                    },
                                    "script": {
                                        "actions": {
                                            "onRun": {"command": "{{ Task.File.runScript }}"}
                                        },
                                        "embeddedFiles": [
                                            {
                                                "name": "runScript",
                                                "type": "TEXT",
                                                "runnable": True,
                                                "data": append_string_script,
                                                **(
                                                    {"filename": "stringappendscript.bat"}
                                                    if os.environ["OPERATING_SYSTEM"] == "windows"
                                                    else {}
                                                ),
                                            }
                                        ],
                                    },
                                }
                            ],
                        }
                    )
                )

            config = configparser.ConfigParser()

            set_setting("defaults.farm_id", deadline_resources.farm.id, config)
            set_setting("defaults.queue_id", deadline_resources.non_valid_role_queue.id, config)

            job_id: Optional[str] = api.create_job_from_job_bundle(
                job_bundle_path,
                job_parameters,
                priority=99,
                config=config,
                queue_parameter_definitions=[],
            )
            assert job_id is not None
        finally:
            # Clean up the template file
            os.remove(os.path.join(job_bundle_path, "template.json"))

        job_details = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.non_valid_role_queue,
            job_id=job_id,
        )
        job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.non_valid_role_queue,
            template={},
            **job_details,
        )

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def sync_input_job_attachments_failed(current_job: Job) -> bool:
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")

            if sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=sessions[0]["sessionId"],
                ).get("sessionActions")

                for session_action in session_actions:
                    if "syncInputJobAttachments" in session_action["definition"]:
                        return session_action["status"] == "FAILED"
            return False

        # Check that the syncInputJobAttachments action failed, since the queue does not have a queue role

        assert sync_input_job_attachments_failed(job)

        return

    @pytest.mark.parametrize(
        "hash_string_script",
        [
            (
                "#!/usr/bin/env bash\n\n"
                "folder_path={{Param.DataDir}}/files\n"
                'combined_contents=""\n'
                'for file in "$folder_path"/*; do\n'
                '   if [ -f "$file" ]; then\n'
                '   combined_contents+="$(cat "$file" | tr -d \'\\n\')"\n'
                "   fi\n"
                "done\n"
                "sha256_hash=$(echo -n \"$combined_contents\" | sha256sum | awk '{ print $1 }')\n"
                'echo -n "$sha256_hash" > {{Param.DataDir}}/output_file.txt'
                if os.environ["OPERATING_SYSTEM"] == "linux"
                else '$InputFolder = "{{Param.DataDir}}\\files"\n'
                '$OutputFile = "{{Param.DataDir}}\\output_file.txt"\n'
                '$combinedContent = ""\n'
                "$files = Get-ChildItem -Path $InputFolder -File\n"
                "foreach ($file in $files) {\n"
                "   $combinedContent += [IO.File]::ReadAllText($file.FullName)\n"
                "}\n"
                "$sha256 = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($combinedContent))\n"
                '$hashString = [System.BitConverter]::ToString($sha256).Replace("-", "").ToLower()\n'
                "Set-Content -Path $OutputFile -Value $hashString -NoNewLine"
            )
        ],
    )
    def test_worker_uses_job_attachment_sync(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        hash_string_script: str,
        tmp_path: pathlib.Path,
    ) -> None:
        # Verify that the worker sync job attachment correctly and report the progress correctly as well

        job_bundle_path: str = os.path.join(
            tmp_path,
            "job_attachment_bundle_large",
        )
        file_path: str = os.path.join(job_bundle_path, "files")

        os.mkdir(job_bundle_path)
        os.mkdir(file_path)

        # Create 2500 very small files to transfer
        for i in range(2500):
            file_name: str = os.path.join(file_path, f"file_{i+1}.txt")
            with open(file_name, "w") as file_to_write:
                file_to_write.write(str(i))

        # Calculate the hash of all the files content combine
        combined_string: str = ""
        for file_name in sorted(os.listdir(file_path)):
            file: str = os.path.join(file_path, file_name)
            # Open the file and read its contents
            with open(file, "r") as file_string:
                file_contents: str = file_string.read()

            # Concatenate the file contents to the combined string
            combined_string += file_contents

        combined_hash: str = hashlib.sha256(combined_string.encode()).hexdigest()

        # JA template to get all files and compute the hash
        job_parameters: List[Dict[str, str]] = [
            {"name": "DataDir", "value": job_bundle_path},
        ]
        with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": "AssetsSync",
                        "parameterDefinitions": [
                            {
                                "name": "DataDir",
                                "type": "PATH",
                                "dataFlow": "INOUT",
                            },
                        ],
                        "steps": [
                            {
                                "name": "HashString",
                                "hostRequirements": {
                                    "attributes": [
                                        {
                                            "name": "attr.worker.os.family",
                                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                                        }
                                    ]
                                },
                                "script": {
                                    "actions": {
                                        "onRun": (
                                            {"command": "{{ Task.File.runScript }}"}
                                            if os.environ["OPERATING_SYSTEM"] == "linux"
                                            else {
                                                "command": "powershell",
                                                "args": ["{{ Task.File.runScript }}"],
                                            }
                                        ),
                                    },
                                    "embeddedFiles": [
                                        {
                                            "name": "runScript",
                                            "type": "TEXT",
                                            "runnable": True,
                                            "data": hash_string_script,
                                            **(
                                                {"filename": "hashscript.ps1"}
                                                if os.environ["OPERATING_SYSTEM"] == "windows"
                                                else {}
                                            ),
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                )
            )

        config = configparser.ConfigParser()

        set_setting("defaults.farm_id", deadline_resources.farm.id, config)
        set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)

        job_id: Optional[str] = api.create_job_from_job_bundle(
            job_bundle_path,
            job_parameters,
            priority=99,
            max_retries_per_task=0,
            config=config,
            queue_parameter_definitions=[],
        )
        assert job_id is not None

        job_details = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )

        # Query the session to check for progress percentage
        complete_percentage: float = 0

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=2,
        )
        def check_percentage(complete_percentage) -> bool:
            sessions = deadline_client.list_sessions(
                farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
            ).get("sessions")

            if sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=sessions[0]["sessionId"],
                ).get("sessionActions")

                for session_action in session_actions:
                    if "syncInputJobAttachments" in session_action["definition"]:
                        assert complete_percentage <= session_action["progressPercent"]
                        complete_percentage = session_action["progressPercent"]
                        return complete_percentage == 100

            return False

        assert check_percentage(complete_percentage)

        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job, deadline_client=deadline_client, deadline_resources=deadline_resources
        )
        with (
            open(os.path.join(list(output_path.keys())[0], "output_file.txt"), "r") as output_file,
        ):
            output_file_content: str = output_file.read()
            # Verify that the hash is the same
            assert output_file_content == combined_hash

    def test_worker_uses_step_step_dependencies(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
        tmp_path: pathlib.Path,
    ) -> None:
        # Test that submits a job that has step step dependencies and confirm that the final output is as we expect

        job_bundle_path: str = os.path.join(
            tmp_path,
            "job_attachment_bundle_step_step_dependencies",
        )
        file_path: str = os.path.join(job_bundle_path, "files")

        os.mkdir(job_bundle_path)
        os.mkdir(file_path)

        # Create the initial input file
        input_file_name: str = os.path.join(file_path, "test_input_file")
        with open(input_file_name, "w") as input_file:
            input_file.write("Hello")

        job_parameters: List[Dict[str, str]] = [
            {"name": "DataDir", "value": job_bundle_path},
        ]

        append_string_script_step_one = (
            "#!/usr/bin/env bash\n\n  echo -n $(cat {{Param.DataDir}}/files/test_input_file)Hello > {{Param.DataDir}}/files/step_one_output\n"
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else '''set /p input=<"{{Param.DataDir}}\\files\\test_input_file"\n powershell -Command "echo ($env:input+\'Hello\') | Out-File -encoding utf8 {{Param.DataDir}}\\files\\step_one_output -NoNewLine"'''
        )

        append_string_script_step_two = (
            "#!/usr/bin/env bash\n\n  echo -n $(cat {{Param.DataDir}}/files/step_one_output)Hello > {{Param.DataDir}}/files/output_file\n"
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else '''set /p input=<"{{Param.DataDir}}\\files\\step_one_output"\n powershell -Command "echo ($env:input+\'Hello\') | Out-File -encoding utf8 {{Param.DataDir}}\\files\\output_file -NoNewLine"'''
        )
        # Create a template that uses step-step dependencies, appending the word "Hello" to the input file once in each step
        with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": "StepDependencyJob",
                        "parameterDefinitions": [
                            {
                                "name": "DataDir",
                                "type": "PATH",
                                "dataFlow": "INOUT",
                            },
                        ],
                        "steps": [
                            {
                                "name": "StepOne",
                                "hostRequirements": {
                                    "attributes": [
                                        {
                                            "name": "attr.worker.os.family",
                                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                                        }
                                    ]
                                },
                                "script": {
                                    "actions": {
                                        "onRun": ({"command": "{{ Task.File.runScript }}"}),
                                    },
                                    "embeddedFiles": [
                                        {
                                            "name": "runScript",
                                            "type": "TEXT",
                                            "runnable": True,
                                            "data": append_string_script_step_one,
                                            **(
                                                {"filename": "appendscript.bat"}
                                                if os.environ["OPERATING_SYSTEM"] == "windows"
                                                else {}
                                            ),
                                        }
                                    ],
                                },
                            },
                            {
                                "name": "StepTwo",
                                "dependencies": [{"dependsOn": "StepOne"}],
                                "hostRequirements": {
                                    "attributes": [
                                        {
                                            "name": "attr.worker.os.family",
                                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                                        }
                                    ]
                                },
                                "script": {
                                    "actions": {
                                        "onRun": ({"command": "{{ Task.File.runScript }}"}),
                                    },
                                    "embeddedFiles": [
                                        {
                                            "name": "runScript",
                                            "type": "TEXT",
                                            "runnable": True,
                                            "data": append_string_script_step_two,
                                            **(
                                                {"filename": "appendscripttwo.bat"}
                                                if os.environ["OPERATING_SYSTEM"] == "windows"
                                                else {}
                                            ),
                                        }
                                    ],
                                },
                            },
                        ],
                    }
                )
            )

        config = configparser.ConfigParser()

        set_setting("defaults.farm_id", deadline_resources.farm.id, config)
        set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)

        job_id: Optional[str] = api.create_job_from_job_bundle(
            job_bundle_path,
            job_parameters,
            priority=99,
            config=config,
            queue_parameter_definitions=[],
        )
        assert job_id is not None

        job_details: dict[str, Any] = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job: Job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )

        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job, deadline_client=deadline_client, deadline_resources=deadline_resources
        )

        try:
            with (
                open(
                    os.path.join(
                        list(output_path.keys())[0],
                        "files",
                        "output_file",
                    ),
                    "r",
                    encoding="utf-8-sig",
                ) as output_file,
            ):
                output_file_content = output_file.read()

                # Verify that the output file content has 3 Hellos in it as expected
                assert output_file_content.count("Hello") == 3
        finally:
            os.remove(os.path.join(list(output_path.keys())[0], "files", "output_file"))

    def test_worker_reports_task_progress_and_status_message(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:

        # Make sure that worker reports task progress, as well as the status message

        # Submit a job with a task that sleeps for 60 seconds , which is more than the UpdateWorkerSchedule interval of 30 seconds

        test_run_status_message: str = "Sleep job is running!"
        sleep_script: str = (
            f"""
            #!/usr/bin/env bash
            percent=0

            while [ $percent -le 100 ]
            do
                echo "openjd_progress: $percent"
                echo "openjd_status: {test_run_status_message}"
                ((percent+=10))
                sleep 6
            done
            """
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else f"""
            $percent = 0
            while ($percent -le 100) {{
                Write-Output "openjd_progress: $percent"
                Write-Output "openjd_status: {test_run_status_message}"
                $percent += 10
                Start-Sleep -Seconds 6
            }}
            """
        )
        job: Job = submit_custom_job(
            job_name="One Minute Sleep Job for Task Progress",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            run_script=sleep_script,
        )

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=2,
        )
        def is_job_started() -> bool:
            job.refresh_job_info(client=deadline_client)
            LOG.info(f"Waiting for job {job.id} to be created")
            return job.lifecycle_status != "CREATE_IN_PROGRESS"

        assert is_job_started()

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=180,
            interval=4,
        )
        def get_session_action_id() -> Optional[str]:
            sessions: list[dict[str, Any]] = deadline_client.list_sessions(
                farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
            ).get("sessions")

            if sessions:
                # There should be at most 1 session as there is only one task
                assert len(sessions) <= 1
                session: dict[str, Any] = sessions[0]
                session_actions: list[dict[str, Any]] = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")

                # There should be at most 1 sessionAction as there is only one task
                if session_actions:
                    assert len(session_actions) <= 1
                    return session_actions[0]["sessionActionId"]

            return None

        session_action_id: Optional[str] = get_session_action_id()
        assert session_action_id is not None

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=180,
            interval=4,
        )
        def session_action_has_expected_progress(session_action_id) -> bool:
            session_action: dict[str, Any] = deadline_client.get_session_action(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionActionId=session_action_id,
            )
            LOG.info(f"Session action for task progress test: {session_action}")
            progress_percent: float = session_action["progressPercent"]
            progress_message: str = session_action.get("progressMessage", "")
            assert progress_percent < 100
            assert session_action["status"] not in [
                "SUCEEDED",
                "FAILED",
                "INTERRUPTED",
                "CANCELED",
                "NEVER_ATTEMPTED",
                "RECLAIMING",
                "RECLAIMED",
            ]
            if progress_percent > 0 and progress_message == test_run_status_message:
                return True
            return False

        assert session_action_has_expected_progress(session_action_id)

        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_worker_fails_job_attachment_sync_when_file_does_not_exist_in_bucket(
        self,
        deadline_resources: DeadlineResources,
        session_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        tmp_path: pathlib.Path,
    ) -> None:
        # Submits a job with input job attachments, deleting the input files from the Job Attadchments bucket before the job starts, and verifying the job syncInputAttachments step fails
        job_bundle_path: str = os.path.join(
            tmp_path,
            "job_attachment_bundle",
        )
        os.mkdir(job_bundle_path)

        input_file_name: str = os.path.join(job_bundle_path, str(uuid.uuid4()))
        with open(input_file_name, "w+") as file_to_write:
            file_to_write.write(str(uuid.uuid4()))

        job_parameters: List[Dict[str, str]] = [
            {"name": "deadline:targetTaskRunStatus", "value": "SUSPENDED"},
            {"name": "DataDir", "value": job_bundle_path},
        ]

        queue_to_use = deadline_resources.queue_a
        with open(
            os.path.join(job_bundle_path, "parameter_values.json"), "w+"
        ) as parameter_values_file:
            # Make sure the job is submitted in SUSPENDED state so we have time to delete an input job attachment in the bucket
            parameter_values_file.write(
                json.dumps(
                    {
                        "parameterValues": [
                            {"name": "deadline:targetTaskRunStatus", "value": "SUSPENDED"},
                        ]
                    }
                )
            )
        with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": "JobAttachmentThatGetsDeleted",
                        "parameterDefinitions": [
                            {
                                "name": "DataDir",
                                "type": "PATH",
                                "dataFlow": "INOUT",
                            },
                        ],
                        "steps": [
                            {
                                "name": "Step0",
                                "hostRequirements": {
                                    "attributes": [
                                        {
                                            "name": "attr.worker.os.family",
                                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                                        }
                                    ]
                                },
                                "script": {
                                    "actions": {"onRun": {"command": "whoami"}},
                                },
                            }
                        ],
                    }
                )
            )

        config = configparser.ConfigParser()
        set_setting("defaults.farm_id", deadline_resources.farm.id, config)
        set_setting("defaults.queue_id", queue_to_use.id, config)
        job_id: Optional[str] = api.create_job_from_job_bundle(
            job_bundle_path,
            job_parameters,
            priority=99,
            config=config,
            queue_parameter_definitions=[],
        )

        assert job_id is not None

        job_details = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=queue_to_use,
            job_id=job_id,
        )

        LOG.info(f"job details: {job_details}")
        assert job_details.get("task_run_status") == "SUSPENDED"
        attachments: Optional[dict] = job_details.get("attachments")
        assert attachments is not None

        manifests: list[dict[str, Any]] = attachments["manifests"]

        assert manifests is not None
        first_manifest = manifests[0]

        input_manifest_path = first_manifest["inputManifestPath"]

        # Find the input manifest
        queue_job_attachment_settings: dict[str, Any] = deadline_client.get_queue(
            farmId=deadline_resources.farm.id,
            queueId=queue_to_use.id,
        )["jobAttachmentSettings"]

        job_attachments_bucket_name: str = queue_job_attachment_settings["s3BucketName"]
        root_prefix: str = queue_job_attachment_settings["rootPrefix"]

        s3_client = boto3.client("s3")

        get_manifest_object_result: dict[str, Any] = s3_client.get_object(
            Bucket=job_attachments_bucket_name,
            Key=root_prefix + "/Manifests/" + input_manifest_path,
        )

        get_object_result_body: dict[str, Any] = json.loads(
            get_manifest_object_result["Body"].read()
        )

        # Get the Job Attachment bucket file paths of the input files
        input_file_paths: list[dict[str, Any]] = get_object_result_body["paths"]
        first_input_file_hash = input_file_paths[0]["hash"]

        # Delete one of the input files from the Job Attachments bucket after confirming that it exists

        s3_client.get_object(
            Bucket=job_attachments_bucket_name,
            Key=root_prefix + "/Data/" + first_input_file_hash + ".xxh128",
        )
        s3_client.delete_object(
            Bucket=job_attachments_bucket_name,
            Key=root_prefix + "/Data/" + first_input_file_hash + ".xxh128",
        )

        # Start the job, it should fail since one of the input files is missing from the Job Attachments bucket

        deadline_client.update_job(
            farmId=deadline_resources.farm.id,
            jobId=job_id,
            queueId=queue_to_use.id,
            targetTaskRunStatus="READY",
        )

        job: Job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )
        job.wait_until_complete(client=deadline_client)

        # Job should have failed due to not being able to sync attachments
        assert job.task_run_status == TaskStatus.FAILED

        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")

        found_failed_session_action = False
        for session in sessions:
            session_actions = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")

            LOG.info(f"Session actions: {session_actions}")
            for session_action in session_actions:
                # Session action should be failed for a syncinputJobAttachments action
                if "syncInputJobAttachments" in session_action["definition"]:
                    assert (
                        session_action["status"] == "FAILED"
                    ), f"syncInputJobAttachments Session action that should have failed is in {session_action['status']} status. {session_action}"
                    found_failed_session_action = True
                else:
                    # Every other session action should have never been attempted, since the syncInputJobAttachments action failed
                    assert (
                        session_action["status"] == "NEVER_ATTEMPTED"
                    ), f"Session action that should not have failed is in FAILED status. {session_action}"
        assert (
            found_failed_session_action
        ), "Was not able to find any syncInputJobAttachments session actions"

        # Make sure the worker is still running and not crashed after this
        get_worker_response: dict[str, Any] = deadline_client.get_worker(
            farmId=session_worker.configuration.farm_id,
            fleetId=session_worker.configuration.fleet.id,
            workerId=session_worker.worker_id,
        )

        assert get_worker_response["status"] in ["STARTED", "RUNNING", "IDLE"]

        # Submit another job and verify that the worker still works properly and finishes the job

        sleep_job = submit_sleep_job(
            "Test Success Sleep Job after syncInputJobAttachments fail",
            deadline_client,
            deadline_resources.farm,
            queue_to_use,
        )

        sleep_job.wait_until_complete(client=deadline_client)

        assert sleep_job.task_run_status == TaskStatus.SUCCEEDED

    def test_worker_enters_stopping_state_while_draining(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        function_worker: EC2InstanceWorker,
        sleep_script: str = (
            """
            #!/usr/bin/env bash
            sleep 600
            """
            if os.environ["OPERATING_SYSTEM"] == "linux"
            else """
            Start-Sleep -Seconds 600
            """
        ),
    ):

        job: Job = submit_custom_job(
            job_name="10 Minutes Sleep Job",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            run_script=sleep_script,
        )

        if os.environ["OPERATING_SYSTEM"] == "linux":
            cmd_result = function_worker.send_command("sudo systemctl stop deadline-worker")
        else:
            cmd_result = function_worker.send_command("sc.exe stop DeadlineWorker")

        assert cmd_result.exit_code == 0

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=10,
        )
        def worker_stop(worker: EC2InstanceWorker) -> bool:
            response = function_worker.deadline_client.get_worker(
                farmId=function_worker.configuration.farm_id,
                fleetId=function_worker.configuration.fleet.id,
                workerId=function_worker.worker_id,
            )
            LOG.info(
                f"Waiting for {function_worker.worker_id} to transition to STOPPING/STOPPED status"
            )

            return response["status"] in ["STOPPED", "STOPPING"]

        try:
            assert worker_stop(function_worker)
        finally:
            deadline_client.update_job(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                targetTaskRunStatus="CANCELED",
            )

            job.wait_until_complete(client=deadline_client)
