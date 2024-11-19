# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
import os
import sys
import sysconfig
import tempfile
from typing import TYPE_CHECKING, Generator
from unittest.mock import MagicMock, Mock, patch, ANY

import pytest

from deadline_worker_agent.sessions.actions import AttachmentDownloadAction
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
def asset_sync() -> MagicMock:
    """A fixture returning a Mock to be passed in place of a deadline.job_attachments. AssetSync
    instance when creating the Worker Agent Session instance"""
    return MagicMock()


@pytest.fixture
def session_dir(session_id: str):
    with tempfile.TemporaryDirectory() as tmpdir_path:
        session_dir: str = os.path.join(tmpdir_path, session_id)
        os.makedirs(session_dir)
        yield session_dir


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
    job_attachment_details: JobAttachmentDetails,
) -> AttachmentDownloadAction:
    return AttachmentDownloadAction(
        id=action_id,
        session_id="session-1234",
        job_attachment_details=job_attachment_details,
    )


class TestStart:
    """Tests for AttachmentDownloadAction.start()"""

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
        session._session = mock_openjd_session_cls
        session._queue_id = TestStart.QUEUE_ID
        session._queue._job_id = TestStart.JOB_ID
        return session

    @pytest.fixture(autouse=True)
    def mock_asset_sync(self, session: Mock) -> Generator[MagicMock, None, None]:
        with patch.object(session, "_asset_sync") as mock_asset_sync:
            yield mock_asset_sync

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Failed in windows due to embeddedFiles.data quotation mark, which will be replaced by python embedded file soon.",
    )
    def test_attachment_download_action_start(
        self,
        executor: Mock,
        session: Mock,
        action: AttachmentDownloadAction,
        session_dir: str,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
    ) -> None:
        """
        Tests that AttachmentDownloadAction.start() calls AssetSync functions to prepare input
        for constructing step script to run openjd action
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        # WHEN
        action.start(session=session, executor=executor)

        mock_asset_sync._aggregate_asset_root_manifests.assert_called_once_with(
            session_dir=session_dir,
            s3_settings=JobAttachmentS3Settings(
                s3BucketName=job_details.job_attachment_settings.s3_bucket_name,
                rootPrefix=job_details.job_attachment_settings.root_prefix,
            ),
            queue_id=TestStart.QUEUE_ID,
            job_id=TestStart.JOB_ID,
            attachments=ANY,
            step_dependencies=[],
            dynamic_mapping_rules=ANY,
            storage_profiles_path_mapping_rules={},
        )
        mock_asset_sync.generate_dynamic_path_mapping.assert_called_once_with(
            session_dir=session_dir,
            attachments=ANY,
        )
        mock_asset_sync._check_and_write_local_manifests.assert_called_once_with(
            merged_manifests_by_root=ANY,
            manifest_write_dir=session_dir,
        )

        assert action._step_script == StepScript_2023_09(
            actions=StepActions_2023_09(
                onRun=Action_2023_09(command="{{ Task.File.AttachmentDownload }}")
            ),
            embeddedFiles=[
                EmbeddedFileText_2023_09(
                    name="AttachmentDownload",
                    type=EmbeddedFileTypes_2023_09.TEXT,
                    runnable=True,
                    data="#!/usr/bin/env bash\n\n{} attachment download -m {} --path-mapping-rules {} --s3-root-uri {}  --profile {}".format(
                        os.path.join(Path(sysconfig.get_path("scripts")), "deadline"),
                        " -m ".join([]),
                        os.path.join(session_dir, "path_mapping"),
                        "s3://job_attachments_bucket/job_attachments",
                        None,
                    ),
                )
            ],
        )
        session.run_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
        )
