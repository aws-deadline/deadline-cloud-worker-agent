# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import (
    Executor,
)
import json
import sys
from logging import LoggerAdapter
from typing import Any, TYPE_CHECKING, Optional
from pathlib import Path
from deadline.job_attachments.models import (
    JobAttachmentS3Settings,
)
from openjd.sessions import LOG as OPENJD_LOG, LogContent
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
from openjd.model import ParameterValue

from ...feature_flag import MANIFEST_REPORTING_FEATURE
from ...log_messages import SessionActionLogKind
from ..attachment_models import WorkerManifestProperties
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ..session import Session


class AttachmentUploadAction(OpenjdAction):
    """Action to upload output job attachments for a AWS Deadline Cloud Session

    Parameters
    ----------
    id : str
        The unique action identifier
    session_id : str
        The session identifier that the action belongs to
    step_id : str
        The step identifier that the action belongs to
    task_id : str
        The task identifier that the action belongs to
    """

    _step_script: Optional[StepScript_2023_09]
    _step_id: str
    _task_id: Optional[str]
    _start_time: float

    def __init__(
        self,
        *,
        id: str,
        session_id: str,
        step_id: str,
        task_id: Optional[str] = None,
        start_time: float,
    ) -> None:
        super(AttachmentUploadAction, self).__init__(
            id=id,
            action_log_kind=(SessionActionLogKind.JA_SYNC_OUTPUT),
        )
        self._step_id = step_id
        self._task_id = task_id
        self._start_time = start_time

        self._logger = LoggerAdapter(OPENJD_LOG, extra={"session_id": session_id})

    def set_step_script(
        self,
        s3_settings: JobAttachmentS3Settings,
        worker_manifest_properties_list: list[WorkerManifestProperties],
    ) -> None:
        """Sets the step script for the action

        Parameters
        ----------
        s3_settings : JobAttachmentS3Settings
            The S3 settings for the job attachment
        worker_manifest_properties_list : list
            List of worker manifest properties for enhanced processing
        """

        # Create embedded file for worker manifest properties
        worker_props_data = [
            worker_props.to_dict() for worker_props in worker_manifest_properties_list
        ]

        worker_props_json = json.dumps(worker_props_data, indent=2)

        # Build the command arguments
        upload_script_path = Path(__file__).parent / "scripts" / "attachment_upload.py"
        args = [
            ArgString(str(upload_script_path)),
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
            embeddedFiles=[
                EmbeddedFileText_2023_09(
                    name="WorkerManifestProperties",
                    type=EmbeddedFileTypes_2023_09.TEXT,
                    data=DataString(worker_props_json),
                ),
            ],
        )

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._step_id == other._step_id
            and self._task_id == other._task_id
            and self._start_time == other._start_time
            and self._step_script == other._step_script
        )

    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Initiates job attachments output upload

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """

        # Banner mimicing the one printed by the openjd-sessions runtime
        # TODO - Consider a better approach to manage the banner title
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            "--------- Job Attachments Upload for Task",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )

        if not (job_attachment_settings := session._job_details.job_attachment_settings):
            raise RuntimeError("Job attachment settings were not contained in JOB_DETAILS entity")

        assert job_attachment_settings.s3_bucket_name is not None
        assert job_attachment_settings.root_prefix is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_attachment_settings.s3_bucket_name,
            rootPrefix=job_attachment_settings.root_prefix,
        )

        self.set_step_script(
            s3_settings=s3_settings,
            worker_manifest_properties_list=session.get_worker_manifest_properties_list(),
        )

        assert self._step_script is not None

        env_vars = {
            "DEADLINE_SESSIONACTION_ID": self._id,
            "DEADLINE_SESSIONACTION_START_TIME": str(self._start_time),
            "DEADLINE_STEP_ID": self._step_id,
            "MANIFEST_REPORTING_FEATURE": str(MANIFEST_REPORTING_FEATURE),
        }

        if self._task_id is not None:
            env_vars["DEADLINE_TASK_ID"] = self._task_id

        session._run_attachment_sync_task(
            step_script=self._step_script,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars=env_vars,
            log_task_banner=False,
        )
