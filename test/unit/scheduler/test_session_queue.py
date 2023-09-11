# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch
from collections import OrderedDict

from deadline.job_attachments.models import JobAttachmentsFileSystem
from openjd.model import SchemaVersion, UnsupportedSchema
from openjd.model.v2023_09 import (
    Environment,
    EnvironmentScript,
    EnvironmentActions,
    Action,
    StepScript,
    StepActions,
)
from openjd.sessions import Parameter, ParameterType
import pytest

from deadline_worker_agent.scheduler.session_queue import (
    CancelOutcome,
    EnvironmentQueueEntry,
    TaskRunQueueEntry,
    SessionActionQueue,
    SyncInputJobAttachmentsQueueEntry,
    SyncInputJobAttachmentsStepDependenciesQueueEntry,
)

from deadline_worker_agent.sessions.actions import (
    EnterEnvironmentAction,
    ExitEnvironmentAction,
    RunStepTaskAction,
    SessionActionDefinition,
    SyncInputJobAttachmentsAction,
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
)


_TEST_ENVIRONMENT_SCRIPT = EnvironmentScript(
    actions=EnvironmentActions(onEnter=Action(command="test"))
)
_TEST_ENVIRONMENT = Environment(
    name="TestEnv",
    script=_TEST_ENVIRONMENT_SCRIPT,
)
_TEST_STEP_SCRIPT = StepScript(actions=StepActions(onRun=Action(command="test.exe")))


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
        job_id=job_id,
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
                            oldstrP="stringValue",
                            strP={"string": "stringValue"},
                            pathP={"path": "/tmp"},
                            intP={"int": "12"},
                            floatP={"float": "1.2"},
                        ),
                    ),
                ),
                RunStepTaskAction(
                    id="id",
                    step_id="stepId",
                    task_id="taskId",
                    details=StepDetails(script=_TEST_STEP_SCRIPT),
                    task_parameter_values=[
                        Parameter(ParameterType.STRING, "oldstrP", "stringValue"),
                        Parameter(ParameterType.STRING, "strP", "stringValue"),
                        Parameter(ParameterType.PATH, "pathP", "/tmp"),
                        Parameter(ParameterType.INT, "intP", "12"),
                        Parameter(ParameterType.FLOAT, "floatP", "1.2"),
                    ],
                ),
                id="task run",
            ),
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
                    step_details=StepDetails(script=_TEST_STEP_SCRIPT, dependencies=["step-1"]),
                ),
                id="sync input job attachments with step Id",
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
        assert type(result) == type(expected)
        assert result.id == expected.id  # type: ignore
        assert result.human_readable() == expected.human_readable()  # type: ignore
        assert len(session_queue._actions) == 0
        assert len(session_queue._actions_by_id) == 0

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
        queue_entry: EnvironmentQueueEntry
        | TaskRunQueueEntry
        | SyncInputJobAttachmentsQueueEntry
        | SyncInputJobAttachmentsStepDependenciesQueueEntry,
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
        queue_entry: EnvironmentQueueEntry
        | TaskRunQueueEntry
        | SyncInputJobAttachmentsQueueEntry
        | SyncInputJobAttachmentsStepDependenciesQueueEntry,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id[queue_entry.definition["sessionActionId"]] = queue_entry

        inner_error = UnsupportedSchema(SchemaVersion.UNDEFINED.value)
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
        argnames="cancel_outcome",
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
        cancel_outcome: CancelOutcome,
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
                        oldstrP="stringValue",
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
        with patch.object(session_queue, "cancel") as cancel_mock:
            # WHEN
            session_queue.cancel_all(
                message=message,
                cancel_outcome=cancel_outcome,
                ignore_env_exits=ignore_env_exits,
            )

        # THEN
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
            | SyncInputJobAttachmentsStepDependenciesQueueEntry,
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
