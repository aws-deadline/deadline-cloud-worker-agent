# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch
from collections import OrderedDict

from deadline.job_attachments.models import JobAttachmentsFileSystem
from openjd.model import (
    ParameterValue,
    ParameterValueType,
    TemplateSpecificationVersion,
    UnsupportedSchema,
)
from openjd.model.v2023_09 import (
    Environment,
    EnvironmentScript,
    EnvironmentActions,
    Action,
    StepScript,
    StepActions,
    StepTemplate,
)
import pytest

from deadline_worker_agent.scheduler.session_queue import (
    EnvironmentQueueEntry,
    TaskRunQueueEntry,
    SessionActionQueue,
    SyncInputJobAttachmentsQueueEntry,
    SyncInputJobAttachmentsStepDependenciesQueueEntry,
    AttachmentDownloadActionQueueEntry,
    AttachmentUploadActionQueueEntry,
)

from deadline_worker_agent.sessions.actions import (
    EnterEnvironmentAction,
    ExitEnvironmentAction,
    RunStepTaskAction,
    SessionActionDefinition,
    SyncInputJobAttachmentsAction,
    AttachmentDownloadAction,
    AttachmentUploadAction,
)
from deadline_worker_agent.sessions.errors import (
    EnvironmentDetailsError,
    JobAttachmentDetailsError,
    JobEntityUnsupportedSchemaError,
    StepDetailsError,
)
from deadline_worker_agent.sessions.job_entities import (
    EnvironmentDetails,
    JobAttachmentDetails,
    StepDetails,
)
from deadline_worker_agent.api_models import (
    EnvironmentDetailsIdentifier,
    EnvironmentDetailsIdentifierFields,
    JobAttachmentDetailsIdentifier,
    JobAttachmentDetailsIdentifierFields,
    StepDetailsIdentifier,
    StepDetailsIdentifierFields,
    EntityIdentifier,
    EnvironmentAction,
    TaskRunAction,
    SyncInputJobAttachmentsAction as SyncInputJobAttachmentsActionBoto,
    AttachmentDownloadAction as AttachmentDownloadActionBoto,
    AttachmentUploadAction as AttachmentUploadActionBoto,
)
from deadline_worker_agent.feature_flag import ASSET_SYNC_JOB_USER_FEATURE


_TEST_ENVIRONMENT_SCRIPT = EnvironmentScript(
    actions=EnvironmentActions(onEnter=Action(command="test"))
)
_TEST_STEP_TEMPLATE = StepTemplate(
    name="TestStep", script=StepScript(actions=StepActions(onRun=Action(command="test.exe")))
)


@pytest.fixture
def job_id() -> str:
    return "job-12ca328a79904b28ad708aeac7dbb2a8"


@pytest.fixture
def job_entities() -> MagicMock:
    return MagicMock()


@pytest.fixture
def session_queue(
    job_id: str,
    job_entities: MagicMock,
) -> SessionActionQueue:
    return SessionActionQueue(
        queue_id="queue-1234",
        job_id=job_id,
        session_id="session-abcd",
        job_entities=job_entities,
        action_update_callback=Mock(),
    )


class TestSessionActionQueueDequeue:
    """Tests for the dequeue method of the SessionActionQueue"""

    @pytest.mark.parametrize(
        "action, expected",
        [
            pytest.param(
                EnvironmentQueueEntry(
                    Mock(),  # cancel event
                    EnvironmentAction(
                        sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                    ),
                ),
                EnterEnvironmentAction(
                    id="id",
                    job_env_id="envid",
                    details=EnvironmentDetails(
                        environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT)
                    ),
                ),
                id="env enter",
            ),
            pytest.param(
                EnvironmentQueueEntry(
                    Mock(),  # cancel event
                    EnvironmentAction(
                        sessionActionId="id", actionType="ENV_EXIT", environmentId="envid"
                    ),
                ),
                ExitEnvironmentAction(
                    id="id",
                    environment_id="envid",
                ),
                id="env exit",
            ),
            pytest.param(
                TaskRunQueueEntry(
                    Mock(),  # cancel event
                    TaskRunAction(
                        sessionActionId="id",
                        actionType="TASK_RUN",
                        taskId="taskId",
                        stepId="stepId",
                        # ordered so that the list order is predictable on output
                        parameters=OrderedDict(
                            strP={"string": "stringValue"},
                            pathP={"path": "/tmp"},
                            intP={"int": "12"},
                            floatP={"float": "1.2"},
                        ),
                    ),
                ),
                RunStepTaskAction(
                    id="id",
                    task_id="taskId",
                    details=StepDetails(step_template=_TEST_STEP_TEMPLATE, step_id="stepId"),
                    task_parameter_values={
                        "strP": ParameterValue(type=ParameterValueType.STRING, value="stringValue"),
                        "pathP": ParameterValue(type=ParameterValueType.PATH, value="/tmp"),
                        "intP": ParameterValue(type=ParameterValueType.INT, value="12"),
                        "floatP": ParameterValue(type=ParameterValueType.FLOAT, value="1.2"),
                    },
                ),
                id="task run",
            ),
        ],
    )
    def test(
        self,
        action: EnvironmentQueueEntry | TaskRunQueueEntry,
        expected: SessionActionDefinition,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [action]
        session_queue._actions_by_id[action.definition["sessionActionId"]] = action

        # WHEN
        result = session_queue.dequeue()

        # THEN
        assert type(result) is type(expected)
        assert result.id == expected.id  # type: ignore
        assert len(session_queue._actions) == 0
        assert len(session_queue._actions_by_id) == 0

    @pytest.mark.skipif(
        ASSET_SYNC_JOB_USER_FEATURE,
        reason="This test will be removed after releasing the asset sync job user feature",
    )
    @pytest.mark.parametrize(
        "action, expected",
        [
            pytest.param(
                SyncInputJobAttachmentsQueueEntry(
                    Mock(),  # cancel event
                    SyncInputJobAttachmentsActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                    ),
                ),
                SyncInputJobAttachmentsAction(
                    id="id",
                    session_id="session-1234",
                    job_attachment_details=JobAttachmentDetails(
                        job_attachments_file_system=JobAttachmentsFileSystem.COPIED,
                        manifests=[],
                    ),
                ),
                id="sync input job attachments",
            ),
            pytest.param(
                SyncInputJobAttachmentsStepDependenciesQueueEntry(
                    Mock(),  # cancel event
                    SyncInputJobAttachmentsActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                        stepId="step-2",
                    ),
                ),
                SyncInputJobAttachmentsAction(
                    id="id",
                    session_id="session-1234",
                    step_details=StepDetails(
                        step_template=_TEST_STEP_TEMPLATE,
                        dependencies=["step-1"],
                        step_id="step-1234",
                    ),
                ),
                id="sync input job attachments with step Id",
            ),
        ],
    )
    def test_sync_input_job_attachments_actions(
        self,
        action: (
            SyncInputJobAttachmentsQueueEntry | SyncInputJobAttachmentsStepDependenciesQueueEntry
        ),
        expected: SyncInputJobAttachmentsAction,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [action]
        session_queue._actions_by_id[action.definition["sessionActionId"]] = action

        # WHEN
        result = session_queue.dequeue()

        # THEN
        assert type(result) is type(expected)
        assert result.id == expected.id  # type: ignore
        assert len(session_queue._actions) == 0
        assert len(session_queue._actions_by_id) == 0

    @pytest.mark.skipif(
        not ASSET_SYNC_JOB_USER_FEATURE,
        reason="This test will be run unconditionally after releasing the asset sync job user featuer",
    )
    @pytest.mark.parametrize(
        "action, expected",
        [
            pytest.param(
                AttachmentDownloadActionQueueEntry(
                    Mock(),  # cancel event
                    AttachmentDownloadActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                    ),
                ),
                AttachmentDownloadAction(
                    id="id",
                    session_id="session-1234",
                    job_attachment_details=JobAttachmentDetails(
                        job_attachments_file_system=JobAttachmentsFileSystem.COPIED,
                        manifests=[],
                    ),
                ),
                id="attachment download job input",
            ),
            pytest.param(
                AttachmentDownloadActionQueueEntry(
                    Mock(),  # cancel event
                    AttachmentDownloadActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                        stepId="step-2",
                    ),
                ),
                AttachmentDownloadAction(
                    id="id",
                    session_id="session-1234",
                    step_details=StepDetails(
                        step_template=_TEST_STEP_TEMPLATE,
                        dependencies=["step-1"],
                        step_id="step-1234",
                    ),
                ),
                id="attachment download step dependency",
            ),
            pytest.param(
                AttachmentUploadActionQueueEntry(
                    Mock(),  # cancel event
                    AttachmentUploadActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ),
                AttachmentUploadAction(
                    id="id",
                    session_id="session-1234",
                    step_id="step-1",
                    task_id="task-1",
                ),
                id="attachment upload action",
            ),
        ],
    )
    def test_attachments_transfer_actions(
        self,
        action: AttachmentDownloadActionQueueEntry | AttachmentUploadActionQueueEntry,
        expected: AttachmentDownloadAction | AttachmentUploadAction,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [action]
        session_queue._actions_by_id[action.definition["sessionActionId"]] = action

        # WHEN
        result = session_queue.dequeue()

        # THEN
        assert type(result) is type(expected)
        assert result.id == expected.id  # type: ignore
        assert len(session_queue._actions) == 0
        assert len(session_queue._actions_by_id) == 0

    def test_attachment_upload_insert_dequeue(
        self,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        action = EnvironmentQueueEntry(
            Mock(),  # cancel event
            EnvironmentAction(
                sessionActionId="id-env", actionType="ENV_ENTER", environmentId="envid"
            ),
        )
        session_queue._actions = [action]
        session_queue._actions_by_id[action.definition["sessionActionId"]] = action

        upload_action = AttachmentUploadActionBoto(
            sessionActionId="id-upload",
            actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
            stepId="step-1",
            taskId="task-1",
        )

        # WHEN
        session_queue.insert_front(action=upload_action)

        # THEN
        assert len(session_queue._actions) == 2
        assert "id-upload" in session_queue._actions_by_id

        # WHEN
        next_action = session_queue.dequeue()

        # THEN
        assert type(next_action) is AttachmentUploadAction

    @pytest.mark.parametrize(
        argnames=("queue_entry", "error_type"),
        argvalues=(
            pytest.param(
                EnvironmentQueueEntry(
                    Mock(),  # cancel event
                    EnvironmentAction(
                        sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                    ),
                ),
                EnvironmentDetailsError,
                id="Environment Details Error",
            ),
            pytest.param(
                TaskRunQueueEntry(
                    Mock(),  # cancel event
                    TaskRunAction(
                        sessionActionId="id",
                        actionType="TASK_RUN",
                        taskId="taskId",
                        stepId="stepId",
                        parameters={},
                    ),
                ),
                StepDetailsError,
                id="Step Details Error",
            ),
            pytest.param(
                SyncInputJobAttachmentsQueueEntry(
                    Mock(),  # cancel event
                    SyncInputJobAttachmentsActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                    ),
                ),
                JobAttachmentDetailsError,
                id="Job Attachments Details Error",
            ),
            pytest.param(
                SyncInputJobAttachmentsStepDependenciesQueueEntry(
                    Mock(),  # cancel event
                    SyncInputJobAttachmentsActionBoto(
                        sessionActionId="id",
                        actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                        stepId="step-2",
                    ),
                ),
                StepDetailsError,
                id="Job Attachments Step Details Error",
            ),
        ),
    )
    def test_handle_job_entity_error_on_dequeue(
        self,
        queue_entry: (
            EnvironmentQueueEntry
            | TaskRunQueueEntry
            | SyncInputJobAttachmentsQueueEntry
            | SyncInputJobAttachmentsStepDependenciesQueueEntry
        ),
        error_type: type[Exception],
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id[queue_entry.definition["sessionActionId"]] = queue_entry

        inner_error = ValueError("validation failed for job entity details")
        job_entity_mock = MagicMock()
        job_entity_mock.environment_details.side_effect = inner_error
        job_entity_mock.step_details.side_effect = inner_error
        job_entity_mock.job_attachment_details.side_effect = inner_error
        session_queue._job_entities = job_entity_mock

        # WHEN / THEN
        with pytest.raises(error_type):
            session_queue.dequeue()

    @pytest.mark.parametrize(
        argnames=("queue_entry"),
        argvalues=(
            pytest.param(
                EnvironmentQueueEntry(
                    Mock(),  # cancel event
                    EnvironmentAction(
                        sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                    ),
                ),
                id="Environment Details",
            ),
            pytest.param(
                TaskRunQueueEntry(
                    Mock(),  # cancel event
                    TaskRunAction(
                        sessionActionId="id",
                        actionType="TASK_RUN",
                        taskId="taskId",
                        stepId="stepId",
                        parameters={},
                    ),
                ),
                id="Step Details",
            ),
        ),
    )
    def test_handle_unsupported_schema_on_dequeue(
        self,
        queue_entry: (
            EnvironmentQueueEntry
            | TaskRunQueueEntry
            | SyncInputJobAttachmentsQueueEntry
            | SyncInputJobAttachmentsStepDependenciesQueueEntry
        ),
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id[queue_entry.definition["sessionActionId"]] = queue_entry

        inner_error = UnsupportedSchema(TemplateSpecificationVersion.UNDEFINED.value)
        job_entity_mock = MagicMock()
        job_entity_mock.environment_details.side_effect = inner_error
        job_entity_mock.step_details.side_effect = inner_error
        job_entity_mock.job_attachment_details.side_effect = inner_error
        session_queue._job_entities = job_entity_mock

        # WHEN / THEN
        with pytest.raises(JobEntityUnsupportedSchemaError):
            session_queue.dequeue()


class TestCancelAll:
    """Tests for SessionQueue.cancel_all()"""

    @pytest.mark.parametrize(
        argnames="message",
        argvalues=("msg1", "msg2", None),
        ids=("msg1", "msg2", "no-msg"),
    )
    @pytest.mark.parametrize(
        argnames="ignore_env_exits",
        argvalues=(False, True),
        ids=("dont-ignore", "ignore"),
    )
    def test_ignore_env_exits(
        self,
        message: str | None,
        ignore_env_exits: bool,
        session_queue: SessionActionQueue,
    ) -> None:
        """Tests that when SessionActionQueue.cancel_all(..., ignore_env_exits=...) is called that
        ENV_EXIT actions are only canceled if ignore_env_exits is False"""

        # GIVEN
        session_queue._actions = [
            TaskRunQueueEntry(
                Mock(),  # cancel event
                TaskRunAction(
                    sessionActionId="task-run",
                    actionType="TASK_RUN",
                    taskId="taskId",
                    stepId="stepId",
                    # ordered so that the list order is predictable on output
                    parameters=OrderedDict(
                        strP={"string": "stringValue"},
                        pathP={"path": "/tmp"},
                        intP={"int": "12"},
                        floatP={"float": "1.2"},
                    ),
                ),
            ),
            EnvironmentQueueEntry(
                cancel=Mock(),
                definition=EnvironmentAction(
                    sessionActionId="env-exit", actionType="ENV_EXIT", environmentId="envid"
                ),
            ),
        ]
        session_queue._actions_by_id = {"task-run": dict(), "env-exit": dict()}  # type: ignore
        with patch.object(session_queue, "_cancel") as cancel_mock:
            # WHEN
            session_queue.cancel_all(
                message=message,
                ignore_env_exits=ignore_env_exits,
            )

        # THEN
        cancel_outcome = "NEVER_ATTEMPTED"
        if ignore_env_exits:
            cancel_mock.assert_called_once()
            cancel_mock.assert_any_call(
                id="task-run", message=message, cancel_outcome=cancel_outcome
            )
        else:
            assert cancel_mock.call_count == 2
            cancel_mock.assert_any_call(
                id="task-run", message=message, cancel_outcome=cancel_outcome
            )
            cancel_mock.assert_any_call(
                id="env-exit", message=message, cancel_outcome=cancel_outcome
            )


class TestIdentifiers:
    @pytest.mark.parametrize(
        argnames=("queue_entries", "expected_identifiers"),
        argvalues=(
            pytest.param([], [], id="Empty queue"),
            pytest.param(
                [
                    EnvironmentQueueEntry(
                        Mock(),  # cancel event
                        EnvironmentAction(
                            sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                        ),
                    ),
                ],
                [
                    EnvironmentDetailsIdentifier(
                        environmentDetails=EnvironmentDetailsIdentifierFields(
                            environmentId="envid",
                            jobId="job-12ca328a79904b28ad708aeac7dbb2a8",
                        )
                    ),
                ],
                id="One Entity",
            ),
            pytest.param(
                [
                    EnvironmentQueueEntry(
                        Mock(),  # cancel event
                        EnvironmentAction(
                            sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                        ),
                    ),
                    TaskRunQueueEntry(
                        Mock(),  # cancel event
                        TaskRunAction(
                            sessionActionId="id",
                            actionType="TASK_RUN",
                            taskId="taskId",
                            stepId="stepId",
                            parameters={},
                        ),
                    ),
                    SyncInputJobAttachmentsQueueEntry(
                        Mock(),  # cancel event
                        SyncInputJobAttachmentsActionBoto(
                            sessionActionId="id",
                            actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                        ),
                    ),
                    SyncInputJobAttachmentsStepDependenciesQueueEntry(
                        Mock(),  # cancel event
                        SyncInputJobAttachmentsActionBoto(
                            sessionActionId="id",
                            actionType="SYNC_INPUT_JOB_ATTACHMENTS",
                            stepId="step-2",
                        ),
                    ),
                ],
                [
                    EnvironmentDetailsIdentifier(
                        environmentDetails=EnvironmentDetailsIdentifierFields(
                            jobId="job-12ca328a79904b28ad708aeac7dbb2a8", environmentId="envid"
                        )
                    ),
                    StepDetailsIdentifier(
                        stepDetails=StepDetailsIdentifierFields(
                            jobId="job-12ca328a79904b28ad708aeac7dbb2a8",
                            stepId="stepId",
                        ),
                    ),
                    JobAttachmentDetailsIdentifier(
                        jobAttachmentDetails=JobAttachmentDetailsIdentifierFields(
                            jobId="job-12ca328a79904b28ad708aeac7dbb2a8",
                        )
                    ),
                    StepDetailsIdentifier(
                        stepDetails=StepDetailsIdentifierFields(
                            jobId="job-12ca328a79904b28ad708aeac7dbb2a8",
                            stepId="step-2",
                        ),
                    ),
                ],
                id="Multiple Entities",
            ),
        ),
    )
    def test_list_all_action_identifiers(
        self,
        session_queue: SessionActionQueue,
        queue_entries: list[
            EnvironmentQueueEntry
            | TaskRunQueueEntry
            | SyncInputJobAttachmentsQueueEntry
            | SyncInputJobAttachmentsStepDependenciesQueueEntry
            | AttachmentDownloadActionQueueEntry
            | AttachmentUploadActionQueueEntry
        ],
        expected_identifiers: list[EntityIdentifier] | None,
    ):
        # GIVEN
        session_queue._actions = queue_entries
        for queue_entry in queue_entries:
            session_queue._actions_by_id[queue_entry.definition["sessionActionId"]] = queue_entry

        # WHEN
        identifiers: list[EntityIdentifier] = session_queue.list_all_action_identifiers()

        # THEN
        assert identifiers == expected_identifiers
