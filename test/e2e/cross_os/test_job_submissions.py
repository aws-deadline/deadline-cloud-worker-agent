# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""
import hashlib
import json
import shutil
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
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments import download
from e2e.conftest import DeadlineResources
from deadline.client.config import set_setting
from deadline.client import api
import uuid
import os
import tempfile
import configparser

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
                                    "userInterface": {
                                        "label": "Input/Output Directory",
                                        "control": "CHOOSE_DIRECTORY",
                                    },
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
        job.wait_until_complete(client=deadline_client, max_retries=20)

        job_attachment_settings = get_queue(
            farm_id=deadline_resources.farm.id,
            queue_id=deadline_resources.queue_a.id,
        ).jobAttachmentSettings

        assert job_attachment_settings is not None

        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=deadline_resources.farm.id,
            queue_id=deadline_resources.queue_a.id,
            job_id=job.id,
            step_id=None,
            task_id=None,
        )

        output_paths_by_root = job_output_downloader.get_output_paths_by_root()
        with tempfile.TemporaryDirectory() as tmp_dir_name:

            # Set root path output will be downloaded to to output_root_path. Assumes there is only one root path.
            job_output_downloader.set_root_path(
                list(output_paths_by_root.keys())[0],
                tmp_dir_name,
            )
            job_output_downloader.download_job_output()

            with (
                open(os.path.join(job_bundle_path, "files", "test_input_file"), "r") as input_file,
                open(
                    os.path.join(
                        tmp_dir_name,
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

    @pytest.mark.parametrize(
        "hash_string_script",
        [
            (
                '#!/usr/bin/env bash\n\nfolder_path={{Param.DataDir}}/files\ncombined_contents=""\nfor file in "$folder_path"/*; do\n   if [ -f "$file" ]; then\n      combined_contents+="$(cat "$file" | tr -d \'\\n\')"\n   fi\ndone\nsha256_hash=$(echo -n "$combined_contents" | sha256sum | awk \'{ print $1 }\')\necho -n "$sha256_hash" > {{Param.DataDir}}/output_file.txt'
                if os.environ["OPERATING_SYSTEM"] == "linux"
                else '$InputFolder = "{{Param.DataDir}}\\files"\n$OutputFile = "{{Param.DataDir}}\\output_file.txt"\n$combinedContent = ""\n$files = Get-ChildItem -Path $InputFolder -File\nforeach ($file in $files) {\n    $combinedContent += [IO.File]::ReadAllText($file.FullName)\n}\n$sha256 = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($combinedContent))\n$hashString = [System.BitConverter]::ToString($sha256).Replace("-", "").ToLower()\nSet-Content -Path $OutputFile -Value $hashString -NoNewLine'
            )
        ],
    )
    def test_worker_uses_job_attachment_sync(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        hash_string_script: str,
    ) -> None:
        # Verify that the worker sync job attachment correctly and report the progress correctly as well

        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__),
            "job_attachment_bundle_large",
        )
        file_path: str = os.path.join(job_bundle_path, "files")

        os.mkdir(job_bundle_path)
        os.mkdir(file_path)

        # Create 2500 very small files to transfer
        for i in range(2500):
            file_name: str = os.path.join(job_bundle_path, "files", f"file_{i+1}.txt")
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
                                "userInterface": {
                                    "label": "Input/Output Directory",
                                    "control": "CHOOSE_DIRECTORY",
                                },
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
        while complete_percentage < 100:
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

        job.wait_until_complete(client=deadline_client, max_retries=20)

        job_attachment_settings = get_queue(
            farm_id=deadline_resources.farm.id,
            queue_id=deadline_resources.queue_a.id,
        ).jobAttachmentSettings

        assert job_attachment_settings is not None

        job_output_downloader = download.OutputDownloader(
            s3_settings=job_attachment_settings,
            farm_id=deadline_resources.farm.id,
            queue_id=deadline_resources.queue_a.id,
            job_id=job.id,
            step_id=None,
            task_id=None,
        )

        output_paths_by_root = job_output_downloader.get_output_paths_by_root()
        with tempfile.TemporaryDirectory() as tmp_dir_name:

            # Set root path output will be downloaded to to output_root_path. Assumes there is only one root path.
            job_output_downloader.set_root_path(
                list(output_paths_by_root.keys())[0],
                tmp_dir_name,
            )
            job_output_downloader.download_job_output()

            with (open(os.path.join(tmp_dir_name, "output_file.txt"), "r") as output_file,):
                output_file_content = output_file.read()
                # Verify that the hash is the same
                assert output_file_content == combined_hash

        shutil.rmtree(job_bundle_path)
