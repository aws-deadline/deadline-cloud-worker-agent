# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import configparser
import dataclasses
import hashlib
import json
import logging
import os
import pathlib
import tempfile
import time
import uuid
from typing import Any, Dict, List, Optional

import backoff
import boto3
import botocore.config
import pytest
import xxhash
from deadline.client import api
from deadline.client.config import set_setting
from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    Job,
    TaskStatus,
)

from e2e.conftest import DeadlineResources
from e2e.s3_validation_utils import validate_s3_job_output_manifest
from e2e.utils import (
    wait_for_job_output,
    submit_sleep_job,
    submit_job_from_bundle,
    verify_output_dir_matches,
    submit_job_from_create_job_API,
)

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Asset:
    path: str
    content: str = dataclasses.field(default_factory=lambda: uuid.uuid4().hex)
    mtime: int = 0

    @property
    def hash(self) -> str:
        return xxhash.xxh128(self.content).hexdigest()

    @property
    def size(self) -> int:
        return len(self.content)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "hash": self.hash,
            "size": self.size,
            "mtime": self.mtime,
        }


class TestJobAttachments:
    JOB_OUTPUT_PATH = os.path.join(os.getcwd(), "job_output")
    ASSET_SYNC_JOB_USER_FEATURE = "ASSET_SYNC_JOB_USER_FEATURE"

    @pytest.mark.usefixtures("asset_sync_class_worker")
    @pytest.mark.parametrize(
        "submission_os",
        [
            "linux",
            "windows",
        ],
    )
    def test_worker_job_attachment_storage_profile_path_mapping(
        self,
        submission_os: str,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
        tmp_path: pathlib.Path,
    ) -> None:
        # Test that submits a job that has storage profile and confirm that the final output path and content is as we expect

        if submission_os == "linux":
            queue_storage_profile_id = deadline_resources.queue_a_job_storage_profile_id
        else:
            queue_storage_profile_id = deadline_resources.windows_job_storage_profile_id

        worker_os = os.getenv("OPERATING_SYSTEM")
        assert worker_os is not None, (
            "OPERATING_SYSTEM environment variable is required but was not provided"
        )
        if worker_os == "linux":
            fleet_storage_profile_id = deadline_resources.fleet_storage_profile_id
        else:
            fleet_storage_profile_id = deadline_resources.windows_fleet_storage_profile_id

        queue_storage_profile_res = deadline_client.get_storage_profile(
            farmId=deadline_resources.farm.id,
            storageProfileId=queue_storage_profile_id,
        )
        assert len(queue_storage_profile_res["fileSystemLocations"]) == 1, (
            f"Expected exactly one file system location for the queue storage profile, but got: {queue_storage_profile_res}"
        )
        queue_file_system_location = queue_storage_profile_res["fileSystemLocations"][0]
        LOG.info(f"Queue file system location is: {queue_file_system_location}")

        fleet_storage_profile_res = deadline_client.get_storage_profile(
            farmId=deadline_resources.farm.id,
            storageProfileId=fleet_storage_profile_id,
        )
        assert len(fleet_storage_profile_res["fileSystemLocations"]) == 1, (
            f"Expected exactly one file system location for the fleet storage profile, but got: {fleet_storage_profile_res}"
        )
        fleet_file_system_location = fleet_storage_profile_res["fileSystemLocations"][0]
        LOG.info(f"Fleet file system location is: {fleet_file_system_location}")

        # Get S3 settings from queue using boto3 directly
        queue_info = deadline_client._real_client.get_queue(
            farmId=deadline_resources.farm.id,
            queueId=deadline_resources.queue_a.id,
        )
        s3_bucket = queue_info["jobAttachmentSettings"]["s3BucketName"]
        s3_prefix = queue_info["jobAttachmentSettings"]["rootPrefix"]

        # Create a unique root directory to not conflict with other test runs
        test_disambiguator = uuid.uuid4().hex

        # Create the initial input file
        input_asset = Asset(path=os.path.join(test_disambiguator, "files", "test_input_file"))

        asset_s3_key = f"{s3_prefix}/Data/{input_asset.hash}"
        LOG.info(f"Uploading input file to s3://{s3_bucket}/{asset_s3_key}: {input_asset}")
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=s3_bucket,
            Key=asset_s3_key,
            Body=input_asset.content,
        )

        # Create asset manifest
        manifest = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                input_asset.to_dict(),
            ],
            "totalSize": input_asset.size,
        }

        manifest_s3_key = f"{s3_prefix}/Manifests/{test_disambiguator}"
        LOG.info(f"Uploading manifest file to s3://{s3_bucket}/{manifest_s3_key}: {manifest}")
        s3.put_object(
            Bucket=s3_bucket,
            Key=manifest_s3_key,
            Body=json.dumps(manifest),
        )

        manifest_properties = {
            "fileSystemLocationName": queue_file_system_location["name"],
            "rootPath": queue_file_system_location["path"],
            "rootPathFormat": "posix" if submission_os == "linux" else "windows",
            "outputRelativeDirectories": [test_disambiguator],
            "inputManifestPath": test_disambiguator,
            "inputManifestHash": test_disambiguator,
        }

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"StorageProfilePathMappingJob[asset_sync_feature={asset_sync_feature}]"

        # Create and submit job using boto3 directly
        template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": job_name,
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
                                "allOf": [worker_os],
                            }
                        ]
                    },
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "python3" if worker_os == "linux" else "python",
                                "args": ["{{ Task.File.runScript }}"],
                            },
                        },
                        "embeddedFiles": [
                            {
                                "name": "runScript",
                                "type": "TEXT",
                                "runnable": True,
                                "data": rf"""
import json
import os

def main():
    print("Session.HasPathMappingRules = {{{{ Session.HasPathMappingRules }}}}")
    print("Path Mapping Rules File Content (Pretty Print)")
    with open(r"{{{{ Session.PathMappingRulesFile }}}}") as f:
        path_mapping_rules = json.load(f)
    print(json.dumps(path_mapping_rules, indent=2))

    expected_path_mapping_rule = [pmr for pmr in path_mapping_rules["path_mapping_rules"] if pmr["destination_path"] == r"{fleet_file_system_location["path"]}" and pmr["source_path"] == r"{queue_file_system_location["path"]}"]
    if len(expected_path_mapping_rule) != 1:
        raise Exception(rf"Expected exactly one matching path mapping rule for source={queue_file_system_location["path"]} <-> dest={fleet_file_system_location["path"]}, but got {{path_mapping_rules['path_mapping_rules']}}")
    expected_path_mapping_rule = expected_path_mapping_rule[0]
    print(f"Found matching path mapping rule: {{expected_path_mapping_rule}}")

    input_file_path = os.path.join(r"{{{{Param.DataDir}}}}", "{test_disambiguator}", "files", "test_input_file")
    with open(input_file_path) as f:
        input_contents = f.read()
    print(f"Read input file contents from '{{input_file_path}}': {{input_contents}}")

    output_file_path = os.path.join(r"{{{{Param.DataDir}}}}", "{test_disambiguator}", "files", "output_file")
    output_content = f"{{input_contents}}Hello"
    with open(output_file_path, mode="w") as f:
        f.write(output_content)
    print(f"Wrote output file contents to '{{output_file_path}}': {{output_content}}")

if __name__ == "__main__":
    main()
""",
                            }
                        ],
                    },
                },
            ],
        }

        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template=template,
            priority=50,
            attachments={
                "manifests": [
                    manifest_properties,
                ],
            },
            parameters={
                "DataDir": {"path": queue_file_system_location["path"]},
            },
            raw_kwargs={
                "storageProfileId": queue_storage_profile_id,
            },
        )
        job.wait_until_complete(client=deadline_client)

        output_root_to_file_mappings: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=str(tmp_path),
        )
        LOG.info(f"Root to output files mapping is: {output_root_to_file_mappings}")

        LOG.info(f"contents of {tmp_path}: {os.listdir(tmp_path)}")
        assert len(output_root_to_file_mappings) == 1, (
            f"Expected exactly one output root, but got: {output_root_to_file_mappings}"
        )
        output_root_path, output_file_rel_paths = list(output_root_to_file_mappings.items())[0]
        output_file_paths = [p for p in output_file_rel_paths if p.endswith("output_file")]
        assert len(output_file_paths) == 1, (
            f"Expected exactly one output file, but got: {output_file_paths}"
        )
        output_file_path = os.path.join(output_root_path, output_file_paths[0])
        try:
            with open(output_file_path, "r", encoding="utf-8-sig") as output_file:
                output_file_content = output_file.read()

                # Verify that the output file content is as expected
                assert output_file_content == f"{input_asset.content}Hello"
        finally:
            os.remove(output_file_path)

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
        asset_sync_worker_config: DeadlineWorkerConfiguration,
        asset_sync_class_worker: EC2InstanceWorker,
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

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"AppendStringJob[asset_sync_feature={asset_sync_feature}]"

        try:
            with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": job_name,
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

        # Validate S3 setup and manifest integrity after job completion
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

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
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
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

                asset_sync_feature = (
                    asset_sync_worker_config.worker_env_var.get(
                        self.ASSET_SYNC_JOB_USER_FEATURE, "False"
                    )
                    if asset_sync_worker_config.worker_env_var
                    else "False"
                )
                job_name = f"NoOutputJob[asset_sync_feature={asset_sync_feature}]"

                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": job_name,
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

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific job bundle to test job attachments dependency data flow",
    )
    @pytest.mark.parametrize(
        "file_system",
        [
            "COPIED",
            # Worker E2E test doesn't run VFS, but this helps verify VIRTUAL fallback and job run successfully
            "VIRTUAL",
        ],
    )
    def test_worker_job_attachments_dep_data_flow_linux(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
        file_system: str,
    ) -> None:
        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__), "job_attachment_bundle", "dep_data_flow", "linux_bundle"
        )

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )

        job_parameters: List[Dict[str, str]] = [
            {
                "name": "JobName",
                "value": f"Step-Step Dataflow Linux_{file_system}[asset_sync_feature={asset_sync_feature}]",
            },
            {
                "name": "AssetSync",
                "value": asset_sync_feature,
            },
        ]

        job = submit_job_from_bundle(
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            bundle_path=job_bundle_path,
            job_attachments_file_system=file_system,
            max_retries_per_task=0,
            job_parameters=job_parameters,
        )

        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Validate S3 setup and manifest integrity
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

        # Get job output path
        os.makedirs(name=self.JOB_OUTPUT_PATH, exist_ok=True)
        output_root_path = tempfile.mkdtemp(
            dir=self.JOB_OUTPUT_PATH, prefix=f"dep_data_flow_linux-{file_system}"
        )
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        LOG.info(f"output_path dict is: {output_path}")

        # Verify the final output file exists and contains expected content
        verify_output_dir_matches(
            reference_dir_path=f"{os.path.dirname(__file__)}/job_attachment_bundle/dep_data_flow/linux_bundle/correct_output",
            output_dir_path=output_root_path,
        )

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "linux",
        reason="Windows specific job bundle to test job attachments dependency data flow",
    )
    def test_worker_job_attachments_dep_data_flow_windows(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
    ) -> None:
        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__), "job_attachment_bundle", "dep_data_flow", "windows_bundle"
        )

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )

        job_parameters: List[Dict[str, str]] = [
            {
                "name": "JobName",
                "value": f"Step-Step Dataflow Win[asset_sync_feature={asset_sync_feature}]",
            },
        ]

        submit_start_time = time.perf_counter()
        job = submit_job_from_bundle(
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            bundle_path=job_bundle_path,
            job_parameters=job_parameters,
        )
        LOG.info(f"Job {job.id} submitted in {time.perf_counter() - submit_start_time:.2f} seconds")

        job.wait_until_complete(client=deadline_client, max_retries=30)
        LOG.info(f"Job {job.id} total in {time.perf_counter() - submit_start_time:.2f} seconds")

        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Validate S3 setup and manifest integrity
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

        # Get job output path
        os.makedirs(name=self.JOB_OUTPUT_PATH, exist_ok=True)
        output_root_path = tempfile.mkdtemp(
            dir=self.JOB_OUTPUT_PATH, prefix="dep_data_flow_windows"
        )
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        LOG.info(f"output_path dict is: {output_path}")

        # Verify the final output file exists and contains expected content
        verify_output_dir_matches(
            reference_dir_path=f"{os.path.dirname(__file__)}/job_attachment_bundle/dep_data_flow/windows_bundle/correct_output",
            output_dir_path=output_root_path,
        )

    @pytest.mark.skip(reason="Queue role permissions are failing the test during E2E test runs")
    def test_worker_fails_job_attachment_sync_when_non_valid_queue_role(
        self,
        deadline_resources: DeadlineResources,
        asset_sync_class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
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

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"JobAttachmentToNonValidRoleQueue[asset_sync_feature={asset_sync_feature}]"

        try:
            with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
                template_file.write(
                    json.dumps(
                        {
                            "specificationVersion": "jobtemplate-2023-09",
                            "name": job_name,
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
            pytest.param(
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
                "Set-Content -Path $OutputFile -Value $hashString -NoNewLine",
                id="hash_script",
            )
        ],
    )
    def test_worker_uses_job_attachment_sync(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
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
            file_name: str = os.path.join(file_path, f"file_{i + 1}.txt")
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

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"AssetsSync[asset_sync_feature={asset_sync_feature}]"

        with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": job_name,
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
            max_retries_per_task=2,
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
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
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

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"StepDependencyJob[asset_sync_feature={asset_sync_feature}]"

        # Create a template that uses step-step dependencies, appending the word "Hello" to the input file once in each step
        with open(os.path.join(job_bundle_path, "template.json"), "w+") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": job_name,
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

    def test_worker_fails_job_attachment_sync_when_file_does_not_exist_in_bucket(
        self,
        deadline_resources: DeadlineResources,
        asset_sync_class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
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

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"JobAttachmentThatGetsDeleted[asset_sync_feature={asset_sync_feature}]"

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
                        "name": job_name,
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
                    assert session_action["status"] == "FAILED", (
                        f"syncInputJobAttachments Session action that should have failed is in {session_action['status']} status. {session_action}"
                    )
                    found_failed_session_action = True
                else:
                    # Every other session action should have never been attempted, since the syncInputJobAttachments action failed
                    assert session_action["status"] == "NEVER_ATTEMPTED", (
                        f"Session action that should not have failed is in FAILED status. {session_action}"
                    )
        assert found_failed_session_action, (
            "Was not able to find any syncInputJobAttachments session actions"
        )

        # Make sure the worker is still running and not crashed after this
        get_worker_response: dict[str, Any] = deadline_client.get_worker(
            farmId=asset_sync_class_worker.configuration.farm_id,
            fleetId=asset_sync_class_worker.configuration.fleet.id,
            workerId=asset_sync_class_worker.worker_id,
        )

        assert get_worker_response["status"] in ["STARTED", "RUNNING", "IDLE"]

        # Submit another job and verify that the worker still works properly and finishes the job

        sleep_job = submit_sleep_job(
            f"Success Sleep Job after syncInputJobAttachments fail[asset_sync_feature={asset_sync_feature}]",
            deadline_client,
            deadline_resources.farm,
            queue_to_use,
        )

        sleep_job.wait_until_complete(client=deadline_client)

        assert sleep_job.task_run_status == TaskStatus.SUCCEEDED

    def test_job_submission_asset_sync_behaviour_expected_without_errors(
        self,
        deadline_resources,
        asset_sync_class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
        tmp_path: pathlib.Path,
    ) -> None:
        """
        Verify that asset sync as job user works as expected,
        with manifest cleanup, job attachments, and embedded files working properly.
        """
        upload_py_content = """#!/usr/bin/env python3
print("Upload script executed successfully")"""

        download_py_content = """#!/usr/bin/env python3
import os
print("Download script executed successfully")
output_path = os.path.join(r"{{ Param.DataDir }}", "output.txt")
with open(output_path, "w") as f:
    f.write("Job attachments working")"""

        # Create job bundle with attachments
        job_bundle_path = os.path.join(tmp_path, "job_bundle")

        os.makedirs(job_bundle_path, exist_ok=True)

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"JA and Embedded Files[asset_sync_feature={asset_sync_feature}]"

        # Create input file for job attachments
        input_file = os.path.join(job_bundle_path, "input.txt")
        with open(input_file, "w") as f:
            f.write("Test input data")

        job_parameters = [
            {"name": "DataDir", "value": job_bundle_path},
        ]

        with open(os.path.join(job_bundle_path, "template.json"), "w") as template_file:
            template_file.write(
                json.dumps(
                    {
                        "specificationVersion": "jobtemplate-2023-09",
                        "name": job_name,
                        "parameterDefinitions": [
                            {
                                "name": "DataDir",
                                "type": "PATH",
                                "dataFlow": "INOUT",
                            },
                        ],
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
                                        "onRun": (
                                            {
                                                "command": "bash",
                                                "args": [
                                                    "-c",
                                                    "python3 {{ Task.File.upload }} && python3 {{ Task.File.download }}",
                                                ],
                                            }
                                            if os.environ["OPERATING_SYSTEM"] == "linux"
                                            else {
                                                "command": "powershell",
                                                "args": [
                                                    "-Command",
                                                    "python {{ Task.File.upload }}; python {{ Task.File.download }}",
                                                ],
                                            }
                                        ),
                                    },
                                    "embeddedFiles": [
                                        {
                                            "name": "upload",
                                            "type": "TEXT",
                                            "runnable": True,
                                            "filename": "upload.py",
                                            "data": upload_py_content,
                                        },
                                        {
                                            "name": "download",
                                            "type": "TEXT",
                                            "runnable": True,
                                            "filename": "download.py",
                                            "data": download_py_content,
                                        },
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

        job_id = api.create_job_from_job_bundle(
            job_bundle_path,
            job_parameters,
            priority=98,
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

        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Validate S3 setup and manifest integrity
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=r"Upload script executed successfully",
        )

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=r"Download script executed successfully",
        )

        # Verify job attachments output
        output_path = wait_for_job_output(
            job=job, deadline_client=deadline_client, deadline_resources=deadline_resources
        )
        output_file = os.path.join(list(output_path.keys())[0], "output.txt")
        with open(output_file, "r") as f:
            assert f.read() == "Job attachments working"

        ## TODO: add verification that manifest cleanup completes successfully

    def test_worker_job_attachments_output_only(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
    ) -> None:
        """Test output-only job attachments with small files on both Windows and Linux"""
        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__), "job_attachment_bundle", "output_only"
        )

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )

        # Use smaller parameters for E2E testing to avoid long-running test
        # python is only available on windows, python3 is only available on linux
        command_runner = "python" if os.environ["OPERATING_SYSTEM"] == "windows" else "python3"
        job = submit_job_from_bundle(
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            bundle_path=job_bundle_path,
            max_retries_per_task=0,
            job_parameters=[
                {
                    "name": "JobName",
                    "value": f"Output Sync No Input Job[asset_sync_feature={asset_sync_feature}]",
                },
                {"name": "FilesPerTask", "value": "1"},
                {"name": "Tasks", "value": "1-5"},
                {"name": "FileSize", "value": "50"},
                {"name": "CommandRunner", "value": command_runner},
            ],
        )

        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Get job output path
        os.makedirs(name=self.JOB_OUTPUT_PATH, exist_ok=True)
        output_root_path = tempfile.mkdtemp(dir=self.JOB_OUTPUT_PATH, prefix="output_only_job")
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        LOG.info(f"output_path dict is: {output_path}")

        # Verify the final output file exists and contains expected content
        verify_output_dir_matches(
            reference_dir_path=f"{os.path.dirname(__file__)}/job_attachment_bundle/output_only_job/correct_output",
            output_dir_path=output_root_path + "/output",
        )

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific job bundle to test create job API call",
    )
    def test_worker_create_job_API_call_linux(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
    ) -> None:
        """
        Test job submission using the Create Job API with a complex job attachment bundle.

        This test uses a "complex" bundle that contains various edge cases to thoroughly
        test job attachment handling, including:
        - Have more than one manifests and root path
        - Have step-step dependency
        - Have output
        - Have storage profile
        """
        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__), "job_attachment_bundle", "complex_bundle", "linux"
        )
        os.makedirs(name=self.JOB_OUTPUT_PATH, exist_ok=True)

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )

        # Create Job API
        job = submit_job_from_create_job_API(
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            debug_snapshot_dir=job_bundle_path,
            storage_profile=True,
            job_name=f"Complex Manifest Test[asset_sync_feature={asset_sync_feature}]",
        )

        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Validate S3 setup and manifest integrity
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

        # Get job output path
        output_root_path = tempfile.mkdtemp(dir=self.JOB_OUTPUT_PATH, prefix="linux_complex_job")
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        LOG.info(f"output_path dict is: {output_path}")

        # Verify the final output file exists and contains expected content
        verify_output_dir_matches(
            reference_dir_path=f"{os.path.dirname(__file__)}/job_attachment_bundle/complex_bundle/linux/correct_output",
            output_dir_path=output_root_path,
        )

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "linux",
        reason="Windows specific job bundle to test create job API call",
    )
    def test_worker_create_job_API_call_windows(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
    ) -> None:
        """
        Test job submission using the Create Job API with a complex job attachment bundle.

        This test uses a "complex" bundle that contains various edge cases to thoroughly
        test job attachment handling, including:
        - Have more than one manifests and root path
        - Have step-step dependency
        - Have output
        """
        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__), "job_attachment_bundle", "complex_bundle", "windows"
        )
        os.makedirs(name=self.JOB_OUTPUT_PATH, exist_ok=True)

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )

        # Create Job API
        submit_start_time = time.perf_counter()
        job = submit_job_from_create_job_API(
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            debug_snapshot_dir=job_bundle_path,
            storage_profile=False,
            job_name=f"Complex Manifest Test[asset_sync_feature={asset_sync_feature}]",
        )

        LOG.info(f"Job {job.id} submitted in {time.perf_counter() - submit_start_time:.2f} seconds")
        job.wait_until_complete(client=deadline_client, max_retries=30)
        LOG.info(f"Job {job.id} total in {time.perf_counter() - submit_start_time:.2f} seconds")

        assert job.task_run_status == TaskStatus.SUCCEEDED

        # Validate S3 setup and manifest integrity
        LOG.info(f"Validating S3 setup for job {job.id}")
        validate_s3_job_output_manifest(
            job=job,
            deadline_client=deadline_client,
        )
        LOG.info("S3 validation completed successfully")

        # Get job output path
        output_root_path = tempfile.mkdtemp(dir=self.JOB_OUTPUT_PATH, prefix="windows_complex_job")
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        LOG.info(f"output_path dict is: {output_path}")

        # Verify the final output file exists and contains expected content
        verify_output_dir_matches(
            reference_dir_path=f"{os.path.dirname(__file__)}/job_attachment_bundle/complex_bundle/windows/correct_output",
            output_dir_path=output_root_path,
        )

    def test_job_attachments_no_output_relative_directories(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        asset_sync_class_worker: EC2InstanceWorker,
        asset_sync_worker_config: DeadlineWorkerConfiguration,
        test_runner_identity: dict[str, str],
    ) -> None:
        """
        Tests that a job created with multiple manifests that have the same rootPath and both manifests have a file with the same path
        results in the file from the second manifest taking precedence over the first manifest
        """
        # Get S3 settings from queue using boto3 directly
        queue_info = deadline_client._real_client.get_queue(
            farmId=deadline_resources.farm.id, queueId=deadline_resources.queue_a.id
        )
        s3_bucket = queue_info["jobAttachmentSettings"]["s3BucketName"]
        s3_prefix = queue_info["jobAttachmentSettings"]["rootPrefix"]

        # Create test files and calculate hashes
        file1_content = str(time.time())
        from xxhash import xxh3_128

        file1_hash = xxh3_128(file1_content).hexdigest()

        # Upload asset files to S3
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=s3_bucket,
            Key=f"{s3_prefix}/Data/{file1_hash}",
            Body=file1_content,
            ExpectedBucketOwner=test_runner_identity["Account"],
        )

        # Create manifests with collision (same path, different hashes)
        manifest = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "hash": file1_hash,
                    "mtime": 1679079744833848,
                    "path": "file.txt",
                    "size": len(file1_content),
                },
            ],
            "totalSize": len(file1_content),
        }

        asset_sync_feature = (
            asset_sync_worker_config.worker_env_var.get(self.ASSET_SYNC_JOB_USER_FEATURE, "False")
            if asset_sync_worker_config.worker_env_var
            else "False"
        )
        job_name = f"empty-outputRelativeDirectories-test-{os.environ['OPERATING_SYSTEM']}[asset_sync_feature={asset_sync_feature}]"

        # Upload manifests
        s3.put_object(
            Bucket=s3_bucket,
            Key=f"{s3_prefix}/Manifests/manifest1hash",
            Body=json.dumps(manifest),
            ExpectedBucketOwner=test_runner_identity["Account"],
        )

        # Create and submit job using boto3 directly
        template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": job_name,
            "parameterDefinitions": [
                {
                    "name": "AssetPath",
                    "type": "PATH",
                    "objectType": "DIRECTORY",
                    "dataFlow": "INOUT",
                    "description": "Path to input assets",
                }
            ],
            "steps": [
                {
                    "name": "step1",
                    "script": {
                        "actions": {
                            "onRun": (
                                {
                                    "command": "/bin/bash",
                                    "args": [
                                        "-c",
                                        "; ".join(
                                            [
                                                "ls -lR",
                                                "echo $(cat {{Param.AssetPath}}/file.txt)",
                                                "echo 'Modifying existing file' >> {{Param.AssetPath}}/file.txt",
                                                "echo $(cat {{Param.AssetPath}}/file.txt)",
                                                "echo 'Create a new file' > {{Param.AssetPath}}/newfile.txt",
                                                "ls -lR",
                                                "echo {{Session.PathMappingRulesFile}}",
                                                "cat {{Session.PathMappingRulesFile}}",
                                            ]
                                        ),
                                    ],
                                }
                                if os.environ["OPERATING_SYSTEM"] == "linux"
                                else {
                                    "command": "cmd",
                                    "args": [
                                        "/c",
                                        (
                                            "dir /s & "
                                            "type {{Param.AssetPath}}\\file.txt & "
                                            "echo Modifying existing file >> {{Param.AssetPath}}\\file.txt & "
                                            "type {{Param.AssetPath}}\\file.txt & "
                                            "echo Create a new file > {{Param.AssetPath}}\\newfile.txt & "
                                            "dir /s & "
                                            "echo {{Session.PathMappingRulesFile}} & "
                                            "type {{Session.PathMappingRulesFile}}"
                                        ),
                                    ],
                                }
                            ),
                        }
                    },
                }
            ],
        }
        attachments = {
            "manifests": [
                {
                    # no outputRelativeDirectories
                    "rootPath": "/test/assets1",
                    "rootPathFormat": "posix",
                    "inputManifestPath": "manifest1hash",
                    "inputManifestHash": "manifest1hash",
                },
                {
                    # empty outputRelativeDirectories
                    "outputRelativeDirectories": [],
                    "rootPath": "/test/assets2",
                    "rootPathFormat": "posix",
                    "inputManifestPath": "manifest1hash",
                    "inputManifestHash": "manifest1hash",
                },
            ]
        }

        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template=template,
            priority=50,
            attachments=attachments,
            parameters={
                "AssetPath": {"path": "/test/assets"},
            },
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED

        output_root_path = tempfile.mkdtemp(
            dir=self.JOB_OUTPUT_PATH, prefix="no_output_relative_directories_empty_output"
        )
        output_path: dict[str, list[str]] = wait_for_job_output(
            job=job,
            deadline_client=deadline_client,
            deadline_resources=deadline_resources,
            output_root_path=output_root_path,
        )
        assert len(output_path) == 0
