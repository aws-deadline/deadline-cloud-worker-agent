# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import dataclasses
import json
import logging
import os
import pathlib
import uuid
from typing import Any

import boto3
import pytest
import xxhash
from deadline_test_fixtures import (
    Job,
    DeadlineClient,
)
from e2e.conftest import DeadlineResources
from e2e.utils import (
    wait_for_job_output,
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
    @pytest.mark.usefixtures("asset_sync_session_worker")
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

        manifest_s3_key = f"{s3_prefix}/Manifests/manifesthash"
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
            "inputManifestPath": "manifesthash",
            "inputManifestHash": "manifesthash",
        }

        # Create and submit job using boto3 directly
        template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": "StorageProfilePathMappingJob",
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
