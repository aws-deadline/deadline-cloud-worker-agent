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
from deadline_test_fixtures import Job, DeadlineClient, TaskStatus, EC2InstanceWorker
from e2e.conftest import DeadlineResources
import backoff
import boto3
import botocore.client
import botocore.config
import botocore.exceptions
import time
from deadline.client.config import set_setting
from deadline.client import api
import uuid
import os
import configparser

from e2e.utils import wait_for_job_output

LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("session_worker")
@pytest.mark.parametrize("operating_system", [os.environ["OPERATING_SYSTEM"]], indirect=True)
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
        deadline_resources: DeadlineResources,
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

            logging.info(f"Session actions: {session_actions}")
            for session_action in session_actions:
                # Session action should be failed IFF it's the expected action to fail
                if expected_failed_action in session_action["definition"]:
                    found_failed_session_action = True
                    assert (
                        session_action["status"] == "FAILED"
                    ), f"Session action that should have failed is not in FAILED status. {session_action}"
                else:
                    assert (
                        session_action["status"] != "FAILED"
                    ), f"Session action that should not have failed is in FAILED status. {session_action}"
        assert found_failed_session_action

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

    @pytest.mark.parametrize("expected_canceled_action", ["envEnter", "taskRun"])
    def test_worker_reports_canceled_session_actions_as_canceled(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        expected_canceled_action: str,
    ) -> None:
        # Tests that when running a job session action with a trap for SIGINT, the corresponding session action is canceled almost immediately.
        action_script = (
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
        job = Job.submit(
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
            sessions = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")

            if len(sessions) == 0:
                return False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
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
                session_actions = deadline_client.list_session_actions(
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

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")
        assert is_expected_session_action_canceled(sessions)

        # Wait until the job is completed

        job.wait_until_complete(client=deadline_client)

        # Verify that envExit was ran
        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=120,
            interval=5,
        )
        def verify_env_exit_ran() -> bool:
            job_logs = job.get_logs(
                deadline_client=deadline_client,
                logs_client=boto3.client(
                    "logs",
                    config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
                ),
            )
            full_log: str = "\n".join(
                [le.message for _, log_events in job_logs.logs.items() for le in log_events]
            )
            return ("Environment exit " + environment_exit_id) in full_log

        if expected_canceled_action == "taskRun":
            # Verify that envExit was ran, if the action being canceled in question is the taskRun, not the envEnter
            assert verify_env_exit_ran()

    @flaky(max_runs=3, min_passes=1)  # Flaky as sync input sometimes completes before expected.
    def test_worker_reports_canceled_sync_input_actions_as_canceled(
        self, deadline_resources: DeadlineResources, deadline_client: DeadlineClient, tmp_path
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
        files_path = os.path.join(tmp_path, "files")
        os.mkdir(files_path)
        for i in range(2000):
            file_name: str = os.path.join(files_path, f"input_file_{i+1}.txt")
            with open(file_name, "w+") as input_file:
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

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=60,
            interval=2,
        )
        def sync_input_action_started(current_job: Job) -> bool:
            sessions = deadline_client.list_sessions(
                farmId=current_job.farm.id, queueId=current_job.queue.id, jobId=current_job.id
            ).get("sessions")
            if len(sessions) == 0:
                return False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                logging.info(f"Session actions: {session_actions}")
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
            found_canceled_sync_input_action = False
            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions")
                logging.info(f"Session actions: {session_actions}")
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

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")

        assert sync_input_actions_are_canceled(sessions)

    def test_worker_always_runs_env_exit_despite_failure(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
    ) -> None:
        # Tests that whenever a envEnter on a job is attempted, the corresponding envExit is also ran despite session action failures

        successful_environment_name = "SuccessfulEnvironment"
        unsuccessful_environment_name = "UnsuccessfulEnvironment"
        job = Job.submit(
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

        sessions = deadline_client.list_sessions(
            farmId=job.farm.id, queueId=job.queue.id, jobId=job.id
        ).get("sessions")

        found_successful_env_enter = False
        found_unsuccessful_env_enter = False
        found_unsuccessful_env_exit = False
        found_successful_env_exit = False

        # Find that the both the unsuccessful and successful environment ran, with envExit and envEnter for each.
        for session in sessions:

            session_actions = deadline_client.list_session_actions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                sessionId=session["sessionId"],
            ).get("sessionActions")
            logging.info(f"Session actions: {session_actions}")
            for session_action in session_actions:
                definition = session_action["definition"]
                if "envEnter" in definition:
                    if successful_environment_name in definition["envEnter"]["environmentId"]:
                        found_successful_env_enter = session_action["status"] == "SUCCEEDED"
                    elif unsuccessful_environment_name in definition["envEnter"]["environmentId"]:
                        found_unsuccessful_env_enter = session_action["status"] == "FAILED"
                elif "envExit" in definition:
                    if successful_environment_name in definition["envExit"]["environmentId"]:
                        found_successful_env_exit = session_action["status"] == "SUCCEEDED"
                    elif unsuccessful_environment_name in definition["envExit"]["environmentId"]:
                        found_unsuccessful_env_exit = session_action["status"] == "FAILED"

        assert (
            found_successful_env_enter
            and found_unsuccessful_env_enter
            and found_unsuccessful_env_exit
            and found_successful_env_exit
        )

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
        job_environments: List[Dict[str, Any]],
    ) -> None:
        job_template = {
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

    def test_worker_streams_logs_to_cloudwatch(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        session_worker: EC2InstanceWorker,
    ) -> None:

        job_start_time_seconds: float = time.time()
        job = Job.submit(
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
        logs_client: boto3.client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )

        # Retrieve job output and verify the echo is printed
        job_logs = job.get_logs(deadline_client=deadline_client, logs_client=logs_client)
        full_log: str = "\n".join(
            [le.message for _, log_events in job_logs.logs.items() for le in log_events]
        )

        assert (
            full_log.count("HelloWorld") == 1
        ), "Expected number of HelloWorld statements not found in job logs."

        # Retrieve worker logs and verify that it's not empty
        worker_log_group_name: str = (
            f"/aws/deadline/{deadline_resources.farm.id}/{deadline_resources.fleet.id}"
        )
        worker_id = session_worker.worker_id

        worker_logs = logs_client.get_log_events(
            logGroupName=worker_log_group_name,
            logStreamName=worker_id,
            startTime=int(job_start_time_seconds * 1000),
        )

        assert len(worker_logs["events"]) > 0

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
            max_time=60,
            interval=2,
        )
        def check_percentage(complete_percentage):
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

            else:
                return False

        assert check_percentage(complete_percentage)

        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job, deadline_client=deadline_client, deadline_resources=deadline_resources
        )
        with (
            open(os.path.join(list(output_path.keys())[0], "output_file.txt"), "r") as output_file,
        ):
            output_file_content = output_file.read()
            # Verify that the hash is the same
            assert output_file_content == combined_hash
