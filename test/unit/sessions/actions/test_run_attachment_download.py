# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
import json
import os
import sys
import tempfile
from typing import TYPE_CHECKING, Generator
from unittest.mock import MagicMock, Mock, patch, ANY

import pytest

import deadline_worker_agent.sessions.actions as actions_module
from deadline_worker_agent.sessions.job_entities.job_details import JobDetails
from openjd.sessions import SessionUser, PosixSessionUser
from openjd.model import ParameterValue
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
    EmbeddedFileText as EmbeddedFileText_2023_09,
    Action as Action_2023_09,
    StepScript as StepScript_2023_09,
    StepActions as StepActions_2023_09,
    ArgString,
    CommandString,
    DataString,
)

import deadline_worker_agent.sessions.session as session_mod
from deadline.job_attachments.models import (
    Attachments,
    PathFormat,
    ManifestProperties,
    JobAttachmentS3Settings,
    JobAttachmentsFileSystem,
)
from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties

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
        yield Path(session_dir)


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
) -> actions_module.AttachmentDownloadAction:
    return actions_module.AttachmentDownloadAction(
        id=action_id,
        session_id="session-1234",
        job_attachment_details=job_attachment_details,
    )


@pytest.fixture
def session(
    session_id: str,
    session_dir: Path,
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
    session.working_directory = session_dir
    session._queue_id = TestStart.QUEUE_ID
    session._queue._job_id = TestStart.JOB_ID
    return session


@pytest.fixture(autouse=True)
def mock_asset_sync(session: Mock) -> Generator[MagicMock, None, None]:
    with patch.object(session, "_asset_sync") as mock_asset_sync:
        yield mock_asset_sync


class TestStart:
    """Tests for AttachmentDownloadAction.start()"""

    QUEUE_ID = "queue-test"
    JOB_ID = "job-test"
    DIR_NAME = "unique_dir_name"

    @pytest.fixture
    def mock_get_unique_dest_dir_name(self):
        with patch(
            "deadline_worker_agent.sessions.actions.run_attachment_download._get_unique_dest_dir_name"
        ) as mock:
            mock.return_value = TestStart.DIR_NAME
            yield mock

    def test_attachment_download_action_start(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
        python_path: str,
    ) -> None:
        """
        Tests that AttachmentDownloadAction.start() calls AssetSync functions to prepare input
        for constructing step script to run openjd action
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        # Create a mock WorkerManifestProperties object
        mock_manifest_properties = ManifestProperties(
            rootPath="/foo/bar",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/path/to/input/manifest.json",
            inputManifestHash="inputmanifesthash",
            outputRelativeDirectories=["/asset/output"],
        )
        mock_worker_manifest_props = WorkerManifestProperties(
            manifest_properties=mock_manifest_properties, local_root_path=str(session_dir)
        )

        session.set_worker_manifest_properties = Mock()
        session.add_local_manifest_path = Mock(return_value=mock_worker_manifest_props)

        # Mock asset sync methods
        mock_asset_sync.get_local_destination.return_value = str(session_dir)
        mock_asset_sync._check_and_write_local_manifests.return_value = {
            str(session_dir): "/path/to/manifest.json"
        }

        # WHEN
        action.start(session=session, executor=executor)
        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_details.job_attachment_settings.s3_bucket_name,
            rootPrefix=job_details.job_attachment_settings.root_prefix,
        )

        # THEN
        mock_asset_sync._aggregate_asset_root_manifests.assert_called_once_with(
            session_dir=session_dir,
            s3_settings=s3_settings,
            queue_id=TestStart.QUEUE_ID,
            job_id=TestStart.JOB_ID,
            attachments=ANY,
            step_dependencies=[],
            dynamic_mapping_rules=ANY,
            storage_profiles_path_mapping_rules=ANY,
        )
        mock_asset_sync.generate_dynamic_path_mapping.assert_called_once_with(
            session_dir=session_dir,
            attachments=ANY,
        )
        mock_asset_sync._check_and_write_local_manifests.assert_called_once_with(
            merged_manifests_by_root=ANY,
            manifest_write_dir=str(session_dir),
            manifest_name_suffix="job",
        )

        # Verify the step script structure with new arguments
        download_script_path = (
            Path(os.path.dirname(actions_module.__file__)) / "scripts" / "attachment_download.py"
        )
        expected_script = StepScript_2023_09(
            actions=StepActions_2023_09(
                onRun=Action_2023_09(
                    command=CommandString(python_path),
                    args=[
                        ArgString(str(download_script_path)),
                        ArgString("-s3"),
                        ArgString(s3_settings.to_s3_root_uri()),
                        ArgString("-wp"),
                        ArgString("{{ Task.File.WorkerManifestProperties }}"),
                    ],
                )
            ),
            embeddedFiles=[
                EmbeddedFileText_2023_09(
                    name="WorkerManifestProperties",
                    type=EmbeddedFileTypes_2023_09.TEXT,
                    data=DataString(ANY),  # JSON data will vary
                ),
            ],
        )

        # Check the basic structure
        assert action._step_script is not None
        assert action._step_script.actions.onRun.command == expected_script.actions.onRun.command
        assert action._step_script.actions.onRun.args == expected_script.actions.onRun.args
        assert action._step_script.embeddedFiles is not None
        assert len(action._step_script.embeddedFiles) == 1
        assert action._step_script.embeddedFiles[0].name == "WorkerManifestProperties"

        session._run_attachment_sync_task.assert_called_once_with(
            step_script=action._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars={
                "DEADLINE_QUEUE_ID": TestStart.QUEUE_ID,
            },
            log_task_banner=False,
        )

    def test_attachment_download_action_start_path_mapping(
        self,
        executor: Mock,
        session: Mock,
        mock_get_unique_dest_dir_name: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
        python_path: str,
    ) -> None:
        """
        Tests that AttachmentDownloadAction.start() prepare path mapping rules
        and pass to AssetSync
        """
        # GIVEN
        assert job_details.job_attachment_settings is not None
        assert job_details.job_attachment_settings.s3_bucket_name is not None
        assert job_details.job_attachment_settings.root_prefix is not None

        assert not job_details.path_mapping_rules

        # Create a mock WorkerManifestProperties object
        mock_manifest_properties = ManifestProperties(
            rootPath="/foo/bar",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/path/to/input/manifest.json",
            inputManifestHash="inputmanifesthash",
            outputRelativeDirectories=["/asset/output"],
        )
        mock_worker_manifest_props = WorkerManifestProperties(
            manifest_properties=mock_manifest_properties, local_root_path=str(session_dir)
        )

        session.set_worker_manifest_properties = Mock()
        session.add_local_manifest_path = Mock(return_value=mock_worker_manifest_props)

        # Mock asset sync methods
        mock_asset_sync.get_local_destination.return_value = str(session_dir)
        mock_asset_sync._check_and_write_local_manifests.return_value = {
            str(session_dir): "/path/to/manifest.json"
        }

        # WHEN
        action.start(session=session, executor=executor)
        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_details.job_attachment_settings.s3_bucket_name,
            rootPrefix=job_details.job_attachment_settings.root_prefix,
        )

        # THEN
        # Verify _get_unique_dest_dir_name was called with the root path
        mock_get_unique_dest_dir_name.assert_called_once_with("/foo/bar")

        # Check that the method was called with correct arguments
        mock_asset_sync._aggregate_asset_root_manifests.assert_called_once_with(
            session_dir=Path(session_dir),
            s3_settings=s3_settings,
            queue_id=TestStart.QUEUE_ID,
            job_id=TestStart.JOB_ID,
            attachments=ANY,
            step_dependencies=[],
            dynamic_mapping_rules=ANY,
            storage_profiles_path_mapping_rules={
                "/foo/bar": str(session.working_directory.joinpath(TestStart.DIR_NAME))
            },
        )

        mock_asset_sync.generate_dynamic_path_mapping.assert_called_once_with(
            session_dir=Path(session_dir),
            attachments=ANY,
        )
        mock_asset_sync._check_and_write_local_manifests.assert_called_once_with(
            merged_manifests_by_root=ANY,
            manifest_write_dir=str(session_dir),
            manifest_name_suffix="job",
        )

        # Verify WorkerManifestProperties was created and set
        session.set_worker_manifest_properties.assert_called_once()
        call_args = session.set_worker_manifest_properties.call_args[0][0]
        assert isinstance(call_args, WorkerManifestProperties)
        assert call_args.local_root_path == str(session_dir)


class TestSetStepScript:
    """Tests for AttachmentDownloadAction.set_step_script()"""

    def test_set_step_script_with_worker_manifest_properties(
        self,
        action: actions_module.AttachmentDownloadAction,
        job_attachment_details: JobAttachmentDetails,
        python_path: str,
    ) -> None:
        """
        Tests that set_step_script creates the correct step script with worker manifest properties
        """
        # GIVEN
        from deadline.job_attachments.models import ManifestProperties, PathFormat

        manifest_properties = ManifestProperties(
            rootPath="/test/root",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/test/manifest.json",
            inputManifestHash="test-hash",
            outputRelativeDirectories=["/output"],
        )

        worker_manifest_properties = WorkerManifestProperties(
            manifest_properties=manifest_properties,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        s3_settings = JobAttachmentS3Settings(
            s3BucketName="test-bucket",
            rootPrefix="test-prefix",
        )

        # WHEN
        action.set_step_script(
            worker_manifest_properties_list=[worker_manifest_properties],
            s3_settings=s3_settings,
        )

        # THEN
        assert action._step_script is not None

        # Check command and args
        assert action._step_script.actions.onRun.command == CommandString(python_path)
        # The actual path is calculated from the actions module location
        download_script_path = (
            Path(actions_module.__file__).parent / "scripts" / "attachment_download.py"
        )
        expected_args = [
            ArgString(str(download_script_path)),
            ArgString("-s3"),
            ArgString(s3_settings.to_s3_root_uri()),
            ArgString("-wp"),
            ArgString("{{ Task.File.WorkerManifestProperties }}"),
        ]
        assert action._step_script.actions.onRun.args == expected_args

        # Check embedded files
        assert action._step_script.embeddedFiles is not None
        assert len(action._step_script.embeddedFiles) == 1

        # Check WorkerManifestProperties file
        worker_props_file = action._step_script.embeddedFiles[0]
        assert worker_props_file.name == "WorkerManifestProperties"
        assert worker_props_file.type == EmbeddedFileTypes_2023_09.TEXT

        # Verify the worker properties JSON contains expected data
        worker_props_data = json.loads(worker_props_file.data)
        assert len(worker_props_data) == 1
        assert worker_props_data[0]["localRootPath"] == "/local/root"
        assert worker_props_data[0]["localManifestPaths"] == ["/local/manifest.json"]

    def test_set_step_script_with_empty_worker_manifest_properties(
        self,
        action: actions_module.AttachmentDownloadAction,
        python_path: str,
    ) -> None:
        """
        Tests that set_step_script works with empty worker manifest properties list
        """
        # GIVEN
        s3_settings = JobAttachmentS3Settings(
            s3BucketName="test-bucket",
            rootPrefix="test-prefix",
        )

        # WHEN
        action.set_step_script(
            worker_manifest_properties_list=[],
            s3_settings=s3_settings,
        )

        # THEN
        assert action._step_script is not None

        # Check that embedded files still exist but WorkerManifestProperties is empty
        assert action._step_script.embeddedFiles is not None
        assert len(action._step_script.embeddedFiles) == 1

        worker_props_file = action._step_script.embeddedFiles[0]
        assert worker_props_file.name == "WorkerManifestProperties"

        worker_props_data = json.loads(worker_props_file.data)
        assert worker_props_data == []


class TestVFS:
    @pytest.mark.skipif(sys.platform == "win32", reason="Test not supported on Windows")
    @pytest.mark.parametrize("launch_vfs_return_value", [True, False])
    def test_start_vfs_calling_asset_sync(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
        launch_vfs_return_value: bool,
    ) -> None:
        """
        Tests that _start_vfs returns the correct value based on _launch_vfs result when all conditions are met
        """

        # Mock platform to be non-Windows
        with patch("sys.platform", "linux"):
            # Set up session with required attributes for VFS
            session._os_user = PosixSessionUser(user="test-user", group="test-group")
            session._env = {"AWS_PROFILE": "test-profile"}

            # Configure the mock to return the parameterized value when _launch_vfs is called
            mock_asset_sync._launch_vfs.return_value = launch_vfs_return_value

            # Create attachments with VIRTUAL file system
            attachments = Attachments(
                manifests=[], fileSystem=JobAttachmentsFileSystem.VIRTUAL.value
            )

            # Mock merged_manifests_by_root
            merged_manifests_by_root: dict[str, BaseAssetManifest] = dict()

            # Create S3 settings
            s3_settings = JobAttachmentS3Settings(
                s3BucketName="test-bucket", rootPrefix="test-prefix"
            )

            # WHEN
            result = action._start_vfs(
                session=session,
                attachments=attachments,
                merged_manifests_by_root=merged_manifests_by_root,
                s3_settings=s3_settings,
            )

            # THEN
            assert result is launch_vfs_return_value
            mock_asset_sync._launch_vfs.assert_called_once_with(
                s3_settings=s3_settings,
                session_dir=session_dir,
                fs_permission_settings=ANY,
                merged_manifests_by_root=merged_manifests_by_root,
                os_env_vars=session._env,
                on_mount_complete=ANY,
            )

    def test_start_vfs_windows_platform(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
    ) -> None:
        """
        Tests that _start_vfs returns False on Windows platform
        """
        # Mock platform to be Windows
        with patch("sys.platform", "win32"):
            # Set up session with required attributes
            session._env = {"AWS_PROFILE": "test-profile"}

            # Create attachments with VIRTUAL file system
            attachments = Attachments(
                manifests=[], fileSystem=JobAttachmentsFileSystem.VIRTUAL.value
            )

            # Mock merged_manifests_by_root
            merged_manifests_by_root: dict[str, BaseAssetManifest] = dict()

            # Create S3 settings
            s3_settings = JobAttachmentS3Settings(
                s3BucketName="test-bucket", rootPrefix="test-prefix"
            )

            # WHEN
            result = action._start_vfs(
                session=session,
                attachments=attachments,
                merged_manifests_by_root=merged_manifests_by_root,
                s3_settings=s3_settings,
            )

            # THEN
            assert result is False
            mock_asset_sync._launch_vfs.assert_not_called()

    @pytest.mark.skipif(sys.platform == "win32", reason="Test not supported on Windows")
    def test_start_vfs_non_virtual_filesystem(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
    ) -> None:
        """
        Tests that _start_vfs returns False when file system is not VIRTUAL
        """
        # Mock platform to be non-Windows
        with patch("sys.platform", "linux"):
            # Set up session with required attributes
            session._os_user = PosixSessionUser(user="test-user", group="test-group")
            session._env = {"AWS_PROFILE": "test-profile"}

            # Create attachments with COPIED file system
            attachments = Attachments(
                manifests=[], fileSystem=JobAttachmentsFileSystem.COPIED.value
            )

            # Mock merged_manifests_by_root
            merged_manifests_by_root: dict[str, BaseAssetManifest] = dict()

            # Create S3 settings
            s3_settings = JobAttachmentS3Settings(
                s3BucketName="test-bucket", rootPrefix="test-prefix"
            )

            # WHEN
            result = action._start_vfs(
                session=session,
                attachments=attachments,
                merged_manifests_by_root=merged_manifests_by_root,
                s3_settings=s3_settings,
            )

            # THEN
            assert result is False
            mock_asset_sync._launch_vfs.assert_not_called()

    @pytest.mark.skipif(sys.platform == "win32", reason="Test not supported on Windows")
    def test_start_vfs_missing_aws_profile(
        self,
        executor: Mock,
        session: Mock,
        action: actions_module.AttachmentDownloadAction,
        session_dir: Path,
        mock_asset_sync: MagicMock,
        job_details: JobDetails,
    ) -> None:
        """
        Tests that _start_vfs returns False when AWS_PROFILE is missing
        """
        # Mock platform to be non-Windows
        with patch("sys.platform", "linux"):
            # Set up session with required attributes but missing AWS_PROFILE
            session._os_user = PosixSessionUser(user="test-user", group="test-group")
            session._env = {}  # No AWS_PROFILE

            # Create attachments with VIRTUAL file system
            attachments = Attachments(
                manifests=[], fileSystem=JobAttachmentsFileSystem.VIRTUAL.value
            )

            # Mock merged_manifests_by_root
            merged_manifests_by_root: dict[str, BaseAssetManifest] = dict()

            # Create S3 settings
            s3_settings = JobAttachmentS3Settings(
                s3BucketName="test-bucket", rootPrefix="test-prefix"
            )

            # WHEN
            result = action._start_vfs(
                session=session,
                attachments=attachments,
                merged_manifests_by_root=merged_manifests_by_root,
                s3_settings=s3_settings,
            )

            # THEN
            assert result is False
            mock_asset_sync._launch_vfs.assert_not_called()
