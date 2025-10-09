# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify Job Attachment's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import re
import uuid
from typing import Any, Optional

import boto3
import botocore.config
import pytest
import xxhash
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.download import get_output_manifests_by_asset_root
from deadline_test_fixtures import (
    Job,
    DeadlineClient,
)
from e2e.conftest import DeadlineResources
from e2e.utils import wait_for_job_output


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


@pytest.mark.usefixtures("session_worker")
class TestJobAttachmentsManifestRootPathCollision:
    @dataclasses.dataclass
    class JobSubmissionInfo:
        job: Job
        root_path: str
        new_output_file_path: str
        conflict_path: str
        conflict_file_m1: Asset
        conflict_file_m2: Asset
        m1_only_file: Asset
        m2_only_file: Asset
        manifest_properties_1: dict[str, Any]
        manifest_properties_2: dict[str, Any]
        job_attachment_settings: dict[str, Any]

    @pytest.fixture(
        scope="session",
        params=[
            (None, None),
            ("fs1", None),
            (None, "fs2"),
            ("fs1", "fs2"),
            ("commonfs", "commonfs"),
        ],
        ids=[
            "both None",
            "only first",
            "only second",
            "both different",
            "both same",
        ],
    )
    def file_system_location_names(self, request: pytest.FixtureRequest) -> tuple[Optional[str], Optional[str]]:
        return request.param

    @pytest.fixture(scope="session")
    def job_submission(
        self,
        file_system_location_names: tuple[Optional[str], Optional[str]],
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
    ) -> JobSubmissionInfo:
        # Get S3 settings from queue using boto3 directly
        queue_info = deadline_client._real_client.get_queue(
            farmId=deadline_resources.farm.id,
            queueId=deadline_resources.queue_a.id,
        )
        s3_bucket = queue_info["jobAttachmentSettings"]["s3BucketName"]
        s3_prefix = queue_info["jobAttachmentSettings"]["rootPrefix"]

        # Create asset files and upload to S3
        conflict_path = "output/file.txt"
        conflict_file_m1 = Asset(path=conflict_path)
        conflict_file_m2 = Asset(path=conflict_path)
        m1_only_file = Asset(path="output/m1_subdir/m1_only_file.txt")
        m2_only_file = Asset(path="output/m2_subdir/m2_only_file.txt")
        all_assets = [
            conflict_file_m1,
            conflict_file_m2,
            m1_only_file,
            m2_only_file,
        ]

        s3 = boto3.client("s3")
        for asset in all_assets:
            s3.put_object(
                Bucket=s3_bucket, Key=f"{s3_prefix}/Data/{asset.hash}", Body=asset.content
            )

        # Create manifests with collision (same path, different hashes)
        manifest1 = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                conflict_file_m1.to_dict(),
                m1_only_file.to_dict(),
            ],
            "totalSize": m1_only_file.size + conflict_file_m1.size,
        }
        manifest2 = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                conflict_file_m2.to_dict(),
                m2_only_file.to_dict(),
            ],
            "totalSize": conflict_file_m2.size + m2_only_file.size,
        }

        # Upload manifests
        s3.put_object(
            Bucket=s3_bucket, Key=f"{s3_prefix}/Manifests/manifest1hash", Body=json.dumps(manifest1)
        )
        s3.put_object(
            Bucket=s3_bucket, Key=f"{s3_prefix}/Manifests/manifest2hash", Body=json.dumps(manifest2)
        )

        root_path = "/test/assets"
        manifest_properties_1 = {
            "rootPath": root_path,
            "rootPathFormat": "posix",
            "outputRelativeDirectories": ["output/m1_subdir"],
            "inputManifestPath": "manifest1hash",
            "inputManifestHash": "manifest1hash",
        }
        if file_system_location_names[0]:
            manifest_properties_1["fileSystemLocationName"] = file_system_location_names[0]
        manifest_properties_2 = {
            "rootPath": root_path,
            "rootPathFormat": "posix",
            "outputRelativeDirectories": ["output"],
            "inputManifestPath": "manifest2hash",
            "inputManifestHash": "manifest2hash",
        }
        if file_system_location_names[1]:
            manifest_properties_2["fileSystemLocationName"] = file_system_location_names[1]

        # Create and submit job using boto3 directly
        new_output_file_path = "output/newfile.txt"
        template = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": "collision-test",
            "parameterDefinitions": [
                {
                    "name": "AssetPath",
                    "type": "PATH",
                    "objectType": "DIRECTORY",
                    "dataFlow": "INOUT",
                    "description": "Path to assets",
                }
            ],
            "steps": [
                {
                    "name": "step1",
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "/bin/bash",
                                "args": [
                                    "-c",
                                    "; ".join(
                                        [
                                            "set -x",
                                            "ls -lR",
                                            # Modify all input attachments, all should be uploaded
                                            *[
                                                f"echo '____test' >> {{{{Param.AssetPath}}}}/{asset.path}; echo $(cat {{{{Param.AssetPath}}}}/{asset.path})"
                                                for asset in all_assets
                                            ],
                                            # Create a new output attachment that should be uploaded
                                            f"echo 'new file' > {{{{Param.AssetPath}}}}/{new_output_file_path}",
                                            "ls -lR",
                                            "echo {{Session.PathMappingRulesFile}}",
                                            "cat {{Session.PathMappingRulesFile}}",
                                        ]
                                    ),
                                ],
                            }
                        }
                    },
                }
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
                    manifest_properties_1,
                    manifest_properties_2,
                ],
            },
            parameters={
                "AssetPath": {"path": root_path},
            },
        )
        job.wait_until_complete(client=deadline_client)
        return TestJobAttachmentsManifestRootPathCollision.JobSubmissionInfo(
            job=job,
            root_path=root_path,
            new_output_file_path=new_output_file_path,
            conflict_path=conflict_path,
            conflict_file_m1=conflict_file_m1,
            conflict_file_m2=conflict_file_m2,
            m1_only_file=m1_only_file,
            m2_only_file=m2_only_file,
            manifest_properties_1=manifest_properties_1,
            manifest_properties_2=manifest_properties_2,
            job_attachment_settings=queue_info["jobAttachmentSettings"],
        )

    def test_asset_with_path_collision_is_overwritten_by_second_manifest_asset(
        self,
        job_submission: JobSubmissionInfo,
        deadline_client: DeadlineClient,
    ) -> None:
        """
        Tests that a job created with multiple manifests that have the same rootPath and both manifests have a file with the same path
        results in the file from the second manifest taking precedence over the first manifest
        """

        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )
        job_submission.job.assert_single_task_log_does_not_contain(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=re.escape(job_submission.conflict_file_m1.content),
        )
        job_submission.job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=re.escape(job_submission.conflict_file_m2.content),
        )

    def test_output_manifests_are_correct(
        self,
        file_system_location_names: tuple[Optional[str], Optional[str]],
        job_submission: JobSubmissionInfo,
        deadline_client: DeadlineClient,
        tmp_path: pathlib.Path,
    ) -> None:
        """
        Tests that a job created with multiple manifests that have the same rootPath are correct. There are two expected outcomes based on the input manifest properties:
        1. Only one output manifest will exist in S3 (the second one) since the first manifest was overwritten
            - No fileSystemLocationName on either manifest
            - Both manifests have the same fileSystemLocationName
        2. Two manifests will exist in S3
            - One of the manifests has fileSystemLocationName but the other does not
            - Both manifests have fileSystemLocationName with different values
        """

        # Verify the session logs indicate two output manifests being created
        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )
        job_submission.job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=re.compile(
                f"Found [0-9]+ files? totaling .* in output directory: /sessions/session-.*/assetroot-.*/{job_submission.manifest_properties_1['outputRelativeDirectories'][0]}"
                ".*"
                f"Found [0-9]+ files? totaling .* in output directory: /sessions/session-.*/assetroot-.*/{job_submission.manifest_properties_2['outputRelativeDirectories'][0]}",
                re.DOTALL,
            ),
        )

        # Download output manifests, verify there is only one root path
        asset_root_to_manifests = get_output_manifests_by_asset_root(
            s3_settings=JobAttachmentS3Settings(**job_submission.job_attachment_settings),
            farm_id=job_submission.job.farm.id,
            queue_id=job_submission.job.queue.id,
            job_id=job_submission.job.id,
        )
        assert len(asset_root_to_manifests) == 1, (
            f"Expected exactly one root path but got: {asset_root_to_manifests}"
        )
        root_path, manifests = list(asset_root_to_manifests.items())[0]
        assert root_path == job_submission.root_path

        # Download the output, verify there is only output for one root path
        output_paths_by_root = wait_for_job_output(
            job=job_submission.job,
            deadline_client=deadline_client,
            output_root_path=str(tmp_path),
        )
        assert len(output_paths_by_root) == 1, (
            f"Expected exactly one output root path but got: {output_paths_by_root}"
        )
        root_path, output_paths = list(output_paths_by_root.items())[0]

        fs_locn_name_1, fs_locn_name_2 = file_system_location_names

        # Outcome 1 - only one manifest output should exist
        if fs_locn_name_1 == fs_locn_name_2:
            # All files except for the M1 only file should be included
            # M1 only file is excluded due to the manifest file in S3 being overwritten by the second asset manifest pass
            expected_output_files = {
                job_submission.conflict_path,
                job_submission.m2_only_file.path,
                job_submission.new_output_file_path,
            }
            assert len(manifests) == 1, f"Expected exactly one manifest path but got: {manifests}"
            output_manifest = manifests[0]
            output_manifest_paths = {path.path for path in output_manifest.paths}
            assert output_manifest_paths == expected_output_files

            # Verify the downloaded files match up too
            assert set(output_paths) == expected_output_files

        # Outcome 2 - two output manifests should exist with different contents
        else:
            # All files except for the M1 only file should be included
            # M1 only file is excluded due to the manifest file in S3 being overwritten by the second asset manifest pass
            m1_expected_output_files = {
                job_submission.conflict_path,
                job_submission.m1_only_file.path,
            }
            m2_expected_output_files = {
                job_submission.conflict_path,
                job_submission.m2_only_file.path,
                job_submission.new_output_file_path,
            }
            assert len(manifests) == 2, f"Expected exactly two manifest paths but got: {manifests}"
            m1_output_manifest, m2_output_manifest = manifests

            m1_output_manifest_paths = {path.path for path in m1_output_manifest.paths}
            m2_output_manifest_paths = {path.path for path in m2_output_manifest.paths}

            assert m1_output_manifest_paths == m1_expected_output_files
            assert m2_output_manifest_paths == m2_expected_output_files

            # Verify the downloaded files match up too
            all_expected_output_files = m1_expected_output_files.union(m2_expected_output_files)
            assert set(output_paths) == all_expected_output_files
