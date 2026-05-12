# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
from typing import TYPE_CHECKING, Generator
from unittest.mock import MagicMock, Mock, patch

import pytest

import deadline_worker_agent.sessions.actions as actions_module
from deadline_worker_agent.sessions.job_entities.job_details import JobDetails
from openjd.sessions import SessionUser
from openjd.model import ParameterValue
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
    CommandString,
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
        id=action_id,
        session_id="session-1234",
        step_id=step_id,
        task_id=task_id,
        start_time=1234567890.0,
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
        session.get_worker_manifest_properties_list = Mock(return_value=[])

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
        session.working_directory = Path(session_dir)

        # Mock file operations to avoid actual file access
        with patch("os.path.exists", return_value=False):
            # WHEN
            action.start(session=session, executor=executor)

        # THEN - Verify the step script is created with new format
        assert action._step_script is not None
        assert action._step_script.actions.onRun.command == CommandString(python_path)

        # Check that the arguments use the new format (no embedded file, direct script path)
        args = action._step_script.actions.onRun.args
        assert args is not None
        assert len(args) == 5  # script_path, -s3, s3_uri, -wp, worker_properties_file
        assert str(args[1]) == "-s3"
        assert str(args[2]) == s3_settings.to_s3_root_uri()
        assert str(args[3]) == "-wp"
        assert str(args[4]) == "{{ Task.File.WorkerManifestProperties }}"

        # Check embedded files contain WorkerManifestProperties
        embedded_files = action._step_script.embeddedFiles
        assert embedded_files is not None
        assert len(embedded_files) == 1
        embedded_file = embedded_files[0]
        assert embedded_file.name == "WorkerManifestProperties"
        assert embedded_file.type == EmbeddedFileTypes_2023_09.TEXT

        session._run_attachment_sync_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars={
                "DEADLINE_SESSIONACTION_ID": action_id,
                "DEADLINE_STEP_ID": step_id,
                "DEADLINE_TASK_ID": task_id,
                "DEADLINE_SESSIONACTION_START_TIME": "1234567890.0",
                "PYTHONIOENCODING": "utf-8",
            },
            log_task_banner=False,
        )

    def test_attachment_upload_action_start_with_worker_manifest_properties(
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
        Tests that AttachmentUploadAction.start() correctly handles worker manifest properties
        and passes them to the attachment_upload script
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        # Setup session with worker manifest properties
        from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties

        mock_worker_props = Mock(spec=WorkerManifestProperties)
        mock_worker_props.to_dict.return_value = {
            "root_path": "/test/path",
            "local_root_path": "/local/test/path",
            "local_manifest_paths": ["/path/to/manifest.json"],
        }

        session.get_worker_manifest_properties_list.return_value = [mock_worker_props]

        # Mock file operations to avoid actual file access
        with patch("os.path.exists", return_value=False):
            # WHEN
            action.start(session=session, executor=executor)

        # THEN - Verify the step script is created with worker manifest properties
        assert action._step_script is not None

        # Check embedded files contain WorkerManifestProperties with expected data
        embedded_files = action._step_script.embeddedFiles
        assert embedded_files is not None
        assert len(embedded_files) == 1
        embedded_file = embedded_files[0]
        assert embedded_file.name == "WorkerManifestProperties"

        # Verify the embedded data contains the worker properties
        import json

        embedded_data = json.loads(str(embedded_file.data))
        assert len(embedded_data) == 1
        assert embedded_data[0]["root_path"] == "/test/path"

        session._run_attachment_sync_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars={
                "DEADLINE_SESSIONACTION_ID": action_id,
                "DEADLINE_STEP_ID": step_id,
                "DEADLINE_TASK_ID": task_id,
                "DEADLINE_SESSIONACTION_START_TIME": "1234567890.0",
                "PYTHONIOENCODING": "utf-8",
            },
            log_task_banner=False,
        )
