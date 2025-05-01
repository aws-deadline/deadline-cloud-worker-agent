# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import json
from typing import TYPE_CHECKING, Generator
from unittest.mock import MagicMock, Mock, patch, mock_open

import pytest

import deadline_worker_agent.sessions.actions as actions_module
from deadline_worker_agent.sessions.job_entities.job_details import JobDetails
from deadline_worker_agent.feature_flag import MANIFEST_REPORTING_FEATURE
from openjd.sessions import SessionUser, PathMappingRule, PathFormat
from openjd.model import ParameterValue
from pathlib import PurePosixPath
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
    EmbeddedFileText as EmbeddedFileText_2023_09,
    Action as Action_2023_09,
    StepScript as StepScript_2023_09,
    StepActions as StepActions_2023_09,
)

import deadline_worker_agent.sessions.session as session_mod
from deadline.job_attachments.models import JobAttachmentS3Settings

if TYPE_CHECKING:
    from deadline_worker_agent.sessions.job_entities import JobAttachmentDetails


@pytest.fixture
def executor() -> Mock:
    return Mock()


@pytest.fixture
def session_id() -> str:
    return "session_id"


@pytest.fixture
def python_path() -> str:
    executable_path = Path(sys.executable)
    return str(
        executable_path.parent
        / executable_path.name.lower().replace("pythonservice.exe", "python.exe")
    )


@pytest.fixture
def session_dir(session_id: str):
    with tempfile.TemporaryDirectory() as tmpdir_path:
        session_dir: str = os.path.join(tmpdir_path, session_id)
        os.makedirs(session_dir)
        yield session_dir


@pytest.fixture
def diff_dir(session_dir: str):
    return os.path.join(session_dir, "diff")


@pytest.fixture
def mock_openjd_session_cls(session_dir: str) -> Generator[MagicMock, None, None]:
    """Mocks the Worker Agent Session module's import of the Open Job Description Session class"""
    with patch.object(session_mod, "OPENJDSession") as mock_openjd_session:
        mock_openjd_session.working_directory = session_dir
        yield mock_openjd_session


@pytest.fixture
def action_id() -> str:
    return "sessionaction-abc123"


@pytest.fixture
def action(
    action_id: str,
    step_id: str,
    task_id: str,
) -> actions_module.AttachmentUploadAction:
    return actions_module.AttachmentUploadAction(
        id=action_id, session_id="session-1234", step_id=step_id, task_id=task_id
    )


class TestStart:
    """Tests for AttachmentUploadAction.start()"""

    QUEUE_ID = "queue-test"
    JOB_ID = "job-test"

    @pytest.fixture
    def session(
        self,
        session_id: str,
        job_details: JobDetails,
        job_user: SessionUser,
        job_attachment_details: JobAttachmentDetails,
        mock_openjd_session_cls: Mock,
    ) -> Mock:
        session = Mock()
        session.id = session_id
        session._job_details = job_details
        session._job_attachment_details = job_attachment_details
        session._os_user = job_user
        session.openjd_session = mock_openjd_session_cls
        session._queue_id = TestStart.QUEUE_ID
        session._queue._job_id = TestStart.JOB_ID
        session.manifest_paths_by_root = {
            "root1": "manifest1.json",
            "root2": "manifest2.json",
        }

        return session

    def test_attachment_upload_action_start_base(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentUploadAction,
        job_details: JobDetails,
        python_path: str,
        step_id: str,
        task_id: str,
        action_id: str,
        session_dir: str,
    ) -> None:
        """
        Tests that AttachmentUploadAction.start() calls AssetSync functions to prepare input
        for constructing step script to run openjd action
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_details.job_attachment_settings.s3_bucket_name,
            rootPrefix=job_details.job_attachment_settings.root_prefix,
        )
        session.manifest_out_rel_dirs_by_source = {}

        session.working_directory = Path(session_dir)

        # Mock file operations to avoid actual file access
        with patch("os.path.exists", return_value=False):
            # WHEN
            action.start(session=session, executor=executor)

        with open(
            Path(os.path.dirname(actions_module.__file__)) / "scripts" / "attachment_upload.py",
            "r",
        ) as f:
            assert action._step_script == StepScript_2023_09(
                actions=StepActions_2023_09(
                    onRun=Action_2023_09(
                        command=python_path,
                        args=[
                            "{{ Task.File.AttachmentUpload }}",
                            "-pm",
                            "{{ Session.PathMappingRulesFile }}",
                            "-s3",
                            s3_settings.to_s3_root_uri(),
                            "-mp",
                            json.dumps(session.manifest_paths_by_root),
                            "-od",
                            json.dumps({}),
                        ],
                    )
                ),
                embeddedFiles=[
                    EmbeddedFileText_2023_09(
                        name="AttachmentUpload",
                        type=EmbeddedFileTypes_2023_09.TEXT,
                        filename="upload.py",
                        data=f.read(),
                    )
                ],
            )

        session.run_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars={
                "DEADLINE_SESSIONACTION_ID": action_id,
                "DEADLINE_STEP_ID": step_id,
                "DEADLINE_TASK_ID": task_id,
                "DEADLINE_SESSION_DIR": str(session.working_directory),
                "MANIFEST_REPORTING_FEATURE": str(MANIFEST_REPORTING_FEATURE),
            },
            log_task_banner=False,
        )

    def test_attachment_upload_action_start_with_include_dirs(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentUploadAction,
        job_details: JobDetails,
        python_path: str,
        step_id: str,
        task_id: str,
        action_id: str,
    ) -> None:
        """
        Tests that AttachmentUploadAction.start() correctly handles include directories
        and passes them to the attachment_upload script
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_details.job_attachment_settings.s3_bucket_name,
            rootPrefix=job_details.job_attachment_settings.root_prefix,
        )

        # Setup session with output directories
        session.manifest_out_rel_dirs_by_source = {
            "/source/path1": ["output_dir1", "output_dir2"],
            "/source/path2": ["output_dir3"],
        }

        # Setup path mapping rules
        session.openjd_session._path_mapping_rules = [
            PathMappingRule(
                source_path_format=PathFormat.POSIX,
                source_path=PurePosixPath("/source/path1"),
                destination_path=PurePosixPath("/dest/path1"),
            ),
            PathMappingRule(
                source_path_format=PathFormat.POSIX,
                source_path=PurePosixPath("/source/path2"),
                destination_path=PurePosixPath("/dest/path2"),
            ),
            PathMappingRule(
                source_path_format=PathFormat.POSIX,
                source_path=PurePosixPath("/source/path3"),
                destination_path=PurePosixPath("/dest/path3"),
            ),
        ]

        # Expected include directories map after mapping
        expected_out_rel_dirs_map = {
            "/dest/path1": ["output_dir1", "output_dir2"],
            "/dest/path2": ["output_dir3"],
        }

        # WHEN
        action.start(session=session, executor=executor)

        # THEN
        with open(
            Path(os.path.dirname(actions_module.__file__)) / "scripts" / "attachment_upload.py",
            "r",
        ) as f:
            assert action._step_script == StepScript_2023_09(
                actions=StepActions_2023_09(
                    onRun=Action_2023_09(
                        command=python_path,
                        args=[
                            "{{ Task.File.AttachmentUpload }}",
                            "-pm",
                            "{{ Session.PathMappingRulesFile }}",
                            "-s3",
                            s3_settings.to_s3_root_uri(),
                            "-mp",
                            json.dumps(session.manifest_paths_by_root),
                            "-od",
                            json.dumps(expected_out_rel_dirs_map),
                        ],
                    )
                ),
                embeddedFiles=[
                    EmbeddedFileText_2023_09(
                        name="AttachmentUpload",
                        type=EmbeddedFileTypes_2023_09.TEXT,
                        filename="upload.py",
                        data=f.read(),
                    )
                ],
            )

        session.run_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars={
                "DEADLINE_SESSIONACTION_ID": action_id,
                "DEADLINE_STEP_ID": step_id,
                "DEADLINE_TASK_ID": task_id,
                "DEADLINE_SESSION_DIR": str(session.working_directory),
                "MANIFEST_REPORTING_FEATURE": str(MANIFEST_REPORTING_FEATURE),
            },
            log_task_banner=False,
        )

    @pytest.mark.skipif(
        not MANIFEST_REPORTING_FEATURE,
        reason="Only relevant when MANIFEST_REPORTING_FEATURE is enabled",
    )
    def test_reads_manifest_information_from_file(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentUploadAction,
        job_details: JobDetails,
        session_dir: str,
        action_id: str,
    ) -> None:
        """
        Tests that AttachmentUploadAction.start() correctly reads manifest information
        from a file and updates the session with the manifest data
        """
        # GIVEN
        # Setup test manifest info
        manifest_info = {
            "/asset/root1": {
                "outputManifestPath": "s3://bucket/Manifests/key1",
                "outputManifestHash": "hash1",
            },
            "/asset/root2": {
                "outputManifestPath": "s3://bucket/Manifests/key2",
                "outputManifestHash": "hash2",
            },
        }

        # Setup job attachment details with manifests
        job_attachment_details = MagicMock()
        job_attachment_details.manifests = [
            MagicMock(root_path="/asset/root1"),
            MagicMock(root_path="/asset/root2"),
            MagicMock(root_path="/asset/root3"),  # Root with no changes
        ]
        session._queue._job_entities.job_attachment_details.return_value = job_attachment_details

        # Set up session working directory and create a Path object for it
        session.working_directory = Path(session_dir)

        # Create expected manifest file path
        manifest_file_path = os.path.join(session_dir, f"manifest_info_{action_id}.json")

        # Mock all necessary file operations
        with patch(
            "deadline_worker_agent.sessions.actions.run_attachment_upload.open",
            mock_open(read_data="script content"),
        ) as mock_file:
            # Mock the specific open call for the manifest file
            def side_effect_open(file_path, mode, *args, **kwargs):
                if str(file_path).endswith("attachment_upload.py"):
                    return mock_open(read_data="script content")(file_path, mode, *args, **kwargs)
                elif str(file_path) == manifest_file_path or str(file_path).endswith(
                    f"manifest_info_{action_id}.json"
                ):
                    return mock_open(read_data=json.dumps(manifest_info))(
                        file_path, mode, *args, **kwargs
                    )
                return mock_open()(file_path, mode, *args, **kwargs)

            mock_file.side_effect = side_effect_open

            with patch("os.path.exists", return_value=True):
                with patch("os.remove") as mock_remove:
                    with patch("json.load", return_value=manifest_info):
                        # WHEN
                        action.start(session=session, executor=executor)

        # THEN
        # Verify manifest information was stored correctly
        expected_manifests = [
            {"outputManifestPath": "s3://bucket/Manifests/key1", "outputManifestHash": "hash1"},
            {"outputManifestPath": "s3://bucket/Manifests/key2", "outputManifestHash": "hash2"},
            {},  # Empty object for root3 with no changes
        ]
        assert len(session._manifests_for_output_sync_target_action) == len(expected_manifests)
        for i, manifest in enumerate(session._manifests_for_output_sync_target_action):
            for key, value in expected_manifests[i].items():
                assert manifest.get(key) == value

        # Verify os.remove was called once (without checking the exact path argument)
        mock_remove.assert_called_once()
