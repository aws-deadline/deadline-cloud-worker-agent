# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import (
    Executor,
)
import os
import sys
import json

from pathlib import Path
from logging import LoggerAdapter
from typing import Any, TYPE_CHECKING, Optional
from dataclasses import asdict

from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline.job_attachments.models import (
    Attachments,
    PathFormat,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathMappingRule,
    JobAttachmentsFileSystem,
)
from deadline.job_attachments.os_file_permission import (
    FileSystemPermissionSettings,
    PosixFileSystemPermissionSettings,
    WindowsFileSystemPermissionSettings,
    WindowsPermissionEnum,
)
from deadline.job_attachments._utils import _get_unique_dest_dir_name

from openjd.sessions import (
    LOG as OPENJD_LOG,
    LogContent,
    PathMappingRule as OpenjdPathMapping,
    PosixSessionUser,
    WindowsSessionUser,
)
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
    EmbeddedFileText as EmbeddedFileText_2023_09,
    Action as Action_2023_09,
    StepScript as StepScript_2023_09,
    StepActions as StepActions_2023_09,
    CommandString,
    ArgString,
    DataString,
)
from openjd.model import ParameterValue

from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction
from ..attachment_models import WorkerManifestProperties

if TYPE_CHECKING:
    from ..session import Session
    from ..job_entities import JobAttachmentDetails, StepDetails


class AttachmentDownloadAction(OpenjdAction):
    """Action to synchronize input job attachments for a AWS Deadline Cloud Session

    Parameters
    ----------
    id : str
        The unique action identifier
    """

    _job_attachment_details: Optional[JobAttachmentDetails]
    _step_details: Optional[StepDetails]
    _step_script: Optional[StepScript_2023_09]

    def __init__(
        self,
        *,
        id: str,
        session_id: str,
        job_attachment_details: Optional[JobAttachmentDetails] = None,
        step_details: Optional[StepDetails] = None,
    ) -> None:
        super(AttachmentDownloadAction, self).__init__(
            id=id,
            action_log_kind=(
                SessionActionLogKind.JA_SYNC_INPUT
                if step_details is None
                else SessionActionLogKind.JA_DEP_SYNC
            ),
            step_id=step_details.step_id if step_details is not None else None,
        )
        self._job_attachment_details = job_attachment_details
        self._step_details = step_details
        self._logger = LoggerAdapter(OPENJD_LOG, extra={"session_id": session_id})

    def set_step_script(
        self,
        worker_manifest_properties_list: list[WorkerManifestProperties],
        s3_settings: JobAttachmentS3Settings,
    ) -> None:
        """Sets the step script for the action

        Parameters
        ----------
        worker_manifest_properties_list : list[WorkerManifestProperties]
            The worker manifest properties list containing manifest data
        s3_settings : JobAttachmentS3Settings
            The job attachment S3 settings
        """
        # Create embedded files for each manifest and collect temporary paths
        embedded_files = []

        # Create embedded file for worker manifest properties
        worker_props_data = []
        for worker_props in worker_manifest_properties_list:
            worker_props_data.append(worker_props.to_dict())

        worker_props_json = json.dumps(worker_props_data, indent=2)
        embedded_files.append(
            EmbeddedFileText_2023_09(
                name="WorkerManifestProperties",
                type=EmbeddedFileTypes_2023_09.TEXT,
                data=DataString(worker_props_json),
            )
        )

        # Build the command arguments
        download_script_path = Path(__file__).parent / "scripts" / "attachment_download.py"
        args = [
            ArgString(str(download_script_path)),
            ArgString("-s3"),
            ArgString(s3_settings.to_s3_root_uri()),
            ArgString("-wp"),
            ArgString("{{ Task.File.WorkerManifestProperties }}"),
        ]

        executable_path = Path(sys.executable)
        python_path = executable_path.parent / executable_path.name.lower().replace(
            "pythonservice.exe", "python.exe"
        )

        self._step_script = StepScript_2023_09(
            actions=StepActions_2023_09(
                onRun=Action_2023_09(
                    command=CommandString(str(python_path)),
                    args=args,
                )
            ),
            embeddedFiles=embedded_files,
        )

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._job_attachment_details == other._job_attachment_details
            and self._step_details == other._step_details
        )

    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Initiates the synchronization of the input job attachments

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """

        if self._step_details:
            section_title = "Job Attachments Download for Step"
        else:
            section_title = "Job Attachments Download for Job"

        # Banner mimicing the one printed by the openjd-sessions runtime
        # TODO - Consider a better approach to manage the banner title
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            f"--------- {section_title}",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )

        if not (job_attachment_settings := session._job_details.job_attachment_settings):
            raise RuntimeError("Job attachment settings were not contained in JOB_DETAILS entity")

        if self._job_attachment_details:
            session._job_attachment_details = self._job_attachment_details

        # Validate that job attachment details have been provided before syncing step dependencies.
        if session._job_attachment_details is None:
            raise RuntimeError(
                "Job attachments must be synchronized before downloading Step dependencies."
            )

        step_dependencies = self._step_details.dependencies if self._step_details else []

        assert job_attachment_settings.s3_bucket_name is not None
        assert job_attachment_settings.root_prefix is not None
        assert session._asset_sync is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_attachment_settings.s3_bucket_name,
            rootPrefix=job_attachment_settings.root_prefix,
        )

        manifest_properties_list: list[ManifestProperties] = []
        if not step_dependencies:
            for ja_manifest_properties in session._job_attachment_details.manifests:
                manifest_properties: ManifestProperties = ManifestProperties(
                    rootPath=ja_manifest_properties.root_path,
                    fileSystemLocationName=ja_manifest_properties.file_system_location_name,
                    rootPathFormat=PathFormat(ja_manifest_properties.root_path_format),
                    inputManifestPath=ja_manifest_properties.input_manifest_path,
                    inputManifestHash=ja_manifest_properties.input_manifest_hash,
                    outputRelativeDirectories=ja_manifest_properties.output_relative_directories,
                )
                manifest_properties_list.append(manifest_properties)

        attachments = Attachments(
            manifests=manifest_properties_list,
            fileSystem=session._job_attachment_details.job_attachments_file_system,
        )

        storage_profiles_path_mapping_rules_dict: dict[str, str] = {
            str(rule.source_path): str(rule.destination_path)
            for rule in session._job_details.path_mapping_rules
        }

        # Generate absolute Path Mapping to local session (no storage profile)
        # returns root path to PathMappingRule mapping
        dynamic_mapping_rules: dict[str, PathMappingRule] = (
            session._asset_sync.generate_dynamic_path_mapping(
                session_dir=session.working_directory,
                attachments=attachments,
            )
        )

        # TODO - update and formalize path mapping logic
        # Temporary patch to generate path mapping for manifests with fileSystemLocationName
        generated_path_mapping: dict[str, PathMappingRule] = dict()
        if not storage_profiles_path_mapping_rules_dict:
            # if the given path mapping rules from job detail does not exist and fileSystemLocationName exists
            # map that to local session directory and make sure the mapping is persisted
            for manifest_properties in attachments.manifests:
                if manifest_properties.fileSystemLocationName:
                    dir_name: str = _get_unique_dest_dir_name(manifest_properties.rootPath)
                    local_root = str(session.working_directory.joinpath(dir_name))
                    generated_path_mapping[manifest_properties.rootPath] = PathMappingRule(
                        source_path_format=manifest_properties.rootPathFormat.value,
                        source_path=manifest_properties.rootPath,
                        destination_path=local_root,
                    )
            # Add to existing storage_profiles_path_mapping_rules_dict for job attachment to map path
            for path_mapping in generated_path_mapping.values():
                storage_profiles_path_mapping_rules_dict.update(
                    {path_mapping.source_path: path_mapping.destination_path}
                )

        # Aggregate manifests (with step step dependency handling)
        merged_manifests_by_root: dict[str, BaseAssetManifest] = (
            session._asset_sync._aggregate_asset_root_manifests(
                session_dir=session.working_directory,
                s3_settings=s3_settings,
                queue_id=session._queue_id,
                job_id=session._queue._job_id,
                attachments=attachments,
                step_dependencies=step_dependencies,
                dynamic_mapping_rules=dynamic_mapping_rules,
                storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules_dict,
            )
        )

        dynamic_mapping_rules.update(generated_path_mapping)
        job_attachment_path_mappings = list([asdict(r) for r in dynamic_mapping_rules.values()])

        # Open Job Description session implementation details -- path mappings are sorted.
        # bisect.insort only supports the 'key' arg in 3.10 or later, so
        # we first extend the list and sort it afterwards.
        if session.openjd_session._path_mapping_rules:
            session.openjd_session._path_mapping_rules.extend(
                OpenjdPathMapping.from_dict(r) for r in job_attachment_path_mappings
            )
        else:
            session.openjd_session._path_mapping_rules = [
                OpenjdPathMapping.from_dict(r) for r in job_attachment_path_mappings
            ]

        # Open Job Description Sessions sort the path mapping rules based on length of the parts make
        # rules that are subsets of each other behave in a predictable manner. We must
        # sort here since we're modifying that internal list appending to the list.
        session.openjd_session._path_mapping_rules.sort(
            key=lambda rule: -len(rule.source_path.parts)
        )

        manifest_paths_by_root = session._asset_sync._check_and_write_local_manifests(
            merged_manifests_by_root=merged_manifests_by_root,
            manifest_write_dir=str(session.working_directory),
            manifest_name_suffix="step" if self._step_details else "job",
        )

        # Create WorkerManifestProperties list for enhanced worker agent processing
        # Populate the manifest properties data from sync job input step
        # The data is avavilable for subsequent actions such as sync step step
        if not step_dependencies:
            for manifest_properties in manifest_properties_list:
                local_root_path: str = session._asset_sync.get_local_destination(
                    manifest_properties=manifest_properties,
                    dynamic_mapping_rules=dynamic_mapping_rules,
                    storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules_dict,
                )

                # Create worker manifest property and add to session
                worker_manifest_props = WorkerManifestProperties(
                    manifest_properties=manifest_properties,
                    local_root_path=local_root_path,
                )
                session.set_worker_manifest_properties(worker_manifest_props)

        # Prepare input manifest for download task run
        download_manifest_properties_list: list[WorkerManifestProperties] = list()
        for local_root_path, manifest_path in manifest_paths_by_root.items():
            download_manifest = session.add_local_manifest_path(
                local_root_path=local_root_path, manifest_path=manifest_path
            )
            # Set the input file path for download
            download_manifest.local_input_manifest_path = manifest_path
            # Add to list for passing to step scripts
            download_manifest_properties_list.append(download_manifest)

        #  Try to launch VFS if needed once all files are prepared
        if self._start_vfs(
            session=session,
            attachments=attachments,
            merged_manifests_by_root=merged_manifests_by_root,
            s3_settings=s3_settings,
        ):
            # Successfully launched VFS, running a echo step with openjd
            # for the session to proceed to the next action
            # LINUX and VIRTUAL only
            session._run_attachment_sync_task(
                step_script=StepScript_2023_09(
                    actions=StepActions_2023_09(
                        onRun=Action_2023_09(
                            command=CommandString("echo"),
                            args=[ArgString("Job Attachments mode VIRTUAL, VFS launched")],
                        )
                    ),
                ),
                task_parameter_values=dict[str, ParameterValue](),
                log_task_banner=False,
            )
        else:
            self.set_step_script(
                s3_settings=s3_settings,
                worker_manifest_properties_list=download_manifest_properties_list,
            )
            assert self._step_script is not None
            session._run_attachment_sync_task(
                step_script=self._step_script,
                task_parameter_values=dict[str, ParameterValue](),
                os_env_vars={
                    "DEADLINE_QUEUE_ID": session._queue_id,
                },
                log_task_banner=False,
            )

    def _start_vfs(
        self,
        session: Session,
        attachments: Attachments,
        merged_manifests_by_root: dict[str, BaseAssetManifest],
        s3_settings: JobAttachmentS3Settings,
    ) -> bool:
        fs_permission_settings: Optional[FileSystemPermissionSettings] = None
        if session._os_user is not None:
            if os.name == "posix":
                if not isinstance(session._os_user, PosixSessionUser):
                    raise ValueError(f"The user must be a posix-user. Got {type(session._os_user)}")
                fs_permission_settings = PosixFileSystemPermissionSettings(
                    os_user=session._os_user.user,
                    os_group=session._os_user.group,
                    dir_mode=0o20,
                    file_mode=0o20,
                )
            else:
                if not isinstance(session._os_user, WindowsSessionUser):
                    raise ValueError(
                        f"The user must be a windows-user. Got {type(session._os_user)}"
                    )
                if session._os_user is not None:
                    fs_permission_settings = WindowsFileSystemPermissionSettings(
                        os_user=session._os_user.user,
                        dir_mode=WindowsPermissionEnum.WRITE,
                        file_mode=WindowsPermissionEnum.WRITE,
                    )

        if (
            attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value
            and sys.platform != "win32"
            and fs_permission_settings is not None
            and session._env is not None
            and "AWS_PROFILE" in session._env
            and isinstance(fs_permission_settings, PosixFileSystemPermissionSettings)
        ):
            assert session._asset_sync is not None
            return session._asset_sync._launch_vfs(
                s3_settings=s3_settings,
                session_dir=session.working_directory,
                fs_permission_settings=fs_permission_settings,
                merged_manifests_by_root=merged_manifests_by_root,
                os_env_vars=dict(session._env),  # type: ignore
            )
        else:
            return False
