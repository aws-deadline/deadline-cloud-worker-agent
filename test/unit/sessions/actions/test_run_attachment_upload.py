# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import json
from typing import TYPE_CHECKING, Generator
from unittest.mock import MagicMock, Mock, patch

import pytest

import deadline_worker_agent.sessions.actions as actions_module
from deadline_worker_agent.sessions.job_entities.job_details import JobDetails
from openjd.sessions import SessionUser
from openjd.model import ParameterValue
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

    def test_attachment_upload_action_start(
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
                            "-mm",
                            json.dumps(session.manifest_paths_by_root),
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
            },
        )
