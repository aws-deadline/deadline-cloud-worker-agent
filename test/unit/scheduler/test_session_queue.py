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
    CommandString,
)
import pytest

from deadline_worker_agent.scheduler.session_queue import (
    EnvironmentQueueEntry,
    TaskRunQueueEntry,
    SessionActionQueue,
    AttachmentDownloadActionQueueEntry,
    AttachmentUploadActionQueueEntry,
)
import deadline_worker_agent.scheduler.session_queue as session_queue_mod

from deadline_worker_agent.sessions.actions import (
    EnterEnvironmentAction,
    ExitEnvironmentAction,
    RunStepTaskAction,
    SessionActionDefinition,
    AttachmentDownloadAction,
    AttachmentUploadAction,
)
from deadline_worker_agent.sessions.errors import (
    EnvironmentDetailsError,
    JobEntityUnsupportedSchemaError,
    SessionActionError,
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
    AttachmentDownloadAction as AttachmentDownloadActionBoto,
    AttachmentUploadAction as AttachmentUploadActionBoto,
)


_TEST_ENVIRONMENT_SCRIPT = EnvironmentScript(
    actions=EnvironmentActions(onEnter=Action(command=CommandString("test")))
)
_TEST_STEP_TEMPLATE = StepTemplate(
    name="TestStep",
    script=StepScript(actions=StepActions(onRun=Action(command=CommandString("test.exe")))),
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
                    details=EnvironmentDetails(
                        environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT)
                    ),
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
                        startTime=1234567890.0,
                    ),
                ),
                AttachmentUploadAction(
                    id="id",
                    session_id="session-1234",
                    step_id="step-1",
                    task_id="task-1",
                    start_time=1234567890.0,
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
            startTime=1234567890.0,
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
        argnames=("queue_entry", "error_type", "expected_step_id", "expected_task_id"),
        argvalues=(
            pytest.param(
                EnvironmentQueueEntry(
                    Mock(),  # cancel event
                    EnvironmentAction(
                        sessionActionId="id", actionType="ENV_ENTER", environmentId="envid"
                    ),
                ),
                EnvironmentDetailsError,
                None,
                None,
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
                "stepId",
                "taskId",
                id="Step Details Error",
            ),
        ),
    )
    def test_handle_job_entity_error_on_dequeue(
        self,
        queue_entry: (EnvironmentQueueEntry | TaskRunQueueEntry),
        error_type: type[SessionActionError],
        expected_step_id: str | None,
        expected_task_id: str | None,
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

        # WHEN
        with pytest.raises(error_type) as excinfo:
            session_queue.dequeue()

        # THEN
        # The Session error handler reads e.step_id/e.task_id when reporting the
        # failure. They must be populated (not raise AttributeError) so that the
        # action fails cleanly instead of triggering an unexpected worker error
        # and reschedule loop.
        assert excinfo.value.step_id == expected_step_id
        assert excinfo.value.task_id == expected_task_id
        # The failed action must be removed from the queue. If it were left
        # queued, cancel_all() would re-report it as NEVER_ATTEMPTED and clobber
        # the FAILED status the Session reports -- which the service rejects for
        # the first session action, crashing the worker scheduler.
        action_id = queue_entry.definition["sessionActionId"]
        assert session_queue._actions == []
        assert action_id not in session_queue._actions_by_id

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
        queue_entry: (EnvironmentQueueEntry | TaskRunQueueEntry),
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
                    AttachmentDownloadActionQueueEntry(
                        Mock(),  # cancel event
                        AttachmentDownloadActionBoto(
                            sessionActionId="id",
                            actionType="SYNC_INPUT_JOB_ATTACHMENTS",
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


class TestPeekResolvedSymbolTableJson:
    """Tests for SessionActionQueue.peek_resolved_symbol_table_json"""

    def test_returns_none_for_empty_queue(
        self,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN
        assert session_queue._actions == []

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result is None

    def test_returns_environment_table_when_first_action_is_env_enter(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        table_json = '[{"name":"Job.Name","type":"string","value":"Example Job"}]'
        job_entities.environment_details.return_value = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="action-1", actionType="ENV_ENTER", environmentId="env-1"
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result == table_json
        job_entities.environment_details.assert_called_once_with(environment_id="env-1")

    def test_returns_environment_table_when_first_action_is_env_exit(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        table_json = '[{"name":"Job.Name","type":"string","value":"Example Job"}]'
        job_entities.environment_details.return_value = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="action-1", actionType="ENV_EXIT", environmentId="env-1"
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result == table_json
        job_entities.environment_details.assert_called_once_with(environment_id="env-1")

    def test_returns_step_table_when_first_action_is_task_run(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        table_json = '[{"name":"Job.Name","type":"string","value":"Example Job"}]'
        job_entities.step_details.return_value = StepDetails(
            step_template=_TEST_STEP_TEMPLATE,
            step_id="step-1",
            resolved_symbol_table_json=table_json,
        )
        entry = TaskRunQueueEntry(
            Mock(),
            TaskRunAction(
                sessionActionId="action-1",
                actionType="TASK_RUN",
                stepId="step-1",
                taskId="task-1",
                parameters={},
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result == table_json
        job_entities.step_details.assert_called_once_with(step_id="step-1")

    def test_returns_none_and_does_not_raise_when_entity_resolution_raises(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        job_entities.environment_details.side_effect = RuntimeError("service unavailable")
        entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="action-1", actionType="ENV_ENTER", environmentId="env-1"
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        with patch.object(session_queue_mod, "logger") as mock_logger:
            result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result is None
        mock_logger.warning.assert_called_once()

    def test_does_not_consume_queue(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        table_json = '[{"name":"Job.Name","type":"string","value":"Example Job"}]'
        job_entities.environment_details.return_value = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="action-1", actionType="ENV_ENTER", environmentId="env-1"
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        peek_result = session_queue.peek_resolved_symbol_table_json()

        # THEN — queue is unmodified
        assert len(session_queue._actions) == 1
        assert "action-1" in session_queue._actions_by_id

        # AND — subsequent dequeue yields the same action
        dequeue_result = session_queue.dequeue()
        assert dequeue_result is not None
        assert dequeue_result.id == "action-1"
        assert peek_result == table_json


class TestPeekResolvedSymbolTableJsonScansPastSync:
    """Tests that peek scans past non-ENV/non-TASK actions to find the symbol table.

    These cover the gap-22 scenario: when SYNC_INPUT_JOB_ATTACHMENTS is the
    first queued action, Job.Name must still be resolved from a subsequent
    ENV_* or TASK_RUN action.
    """

    def test_returns_env_table_when_sync_input_precedes_env_enter(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN — sync action is first, env action is second
        table_json = '[{"name":"Job.Name","type":"string","value":"MyJob"}]'
        job_entities.environment_details.return_value = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        sync_entry = AttachmentDownloadActionQueueEntry(
            Mock(),
            AttachmentDownloadActionBoto(
                sessionActionId="sync-1", actionType="SYNC_INPUT_JOB_ATTACHMENTS"
            ),
        )
        env_entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="env-1", actionType="ENV_ENTER", environmentId="env-1"
            ),
        )
        session_queue._actions = [sync_entry, env_entry]
        session_queue._actions_by_id["sync-1"] = sync_entry
        session_queue._actions_by_id["env-1"] = env_entry

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN
        assert result == table_json
        job_entities.environment_details.assert_called_once_with(environment_id="env-1")

    def test_returns_none_when_queue_has_only_sync_actions(
        self,
        session_queue: SessionActionQueue,
    ) -> None:
        # GIVEN — queue contains only attachment sync actions (no ENV/TASK)
        sync_entry = AttachmentDownloadActionQueueEntry(
            Mock(),
            AttachmentDownloadActionBoto(
                sessionActionId="sync-1", actionType="SYNC_INPUT_JOB_ATTACHMENTS"
            ),
        )
        upload_entry = AttachmentUploadActionQueueEntry(
            Mock(),
            AttachmentUploadActionBoto(
                sessionActionId="upload-1",
                actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
                stepId="step-1",
                startTime=0.0,
            ),
        )
        session_queue._actions = [sync_entry, upload_entry]
        session_queue._actions_by_id["sync-1"] = sync_entry
        session_queue._actions_by_id["upload-1"] = upload_entry

        # WHEN
        result = session_queue.peek_resolved_symbol_table_json()

        # THEN — no ENV/TASK action exists, so None is correct
        assert result is None

    def test_skips_action_whose_entity_resolution_fails_and_returns_next(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN — first ENV action's entity resolution fails, second succeeds
        table_json = '[{"name":"Job.Name","type":"string","value":"MyJob"}]'
        env_details_good = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        job_entities.environment_details.side_effect = [
            RuntimeError("entity fetch failed"),
            env_details_good,
        ]
        entry_bad = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="env-bad", actionType="ENV_ENTER", environmentId="env-bad"
            ),
        )
        entry_good = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="env-good", actionType="ENV_ENTER", environmentId="env-good"
            ),
        )
        session_queue._actions = [entry_bad, entry_good]
        session_queue._actions_by_id["env-bad"] = entry_bad
        session_queue._actions_by_id["env-good"] = entry_good

        # WHEN
        with patch.object(session_queue_mod, "logger") as mock_logger:
            result = session_queue.peek_resolved_symbol_table_json()

        # THEN — skipped the failed entity, returned from the good one
        assert result == table_json
        mock_logger.warning.assert_called_once()


class TestDequeueEnvExitDetails:
    """Tests that ENV_EXIT dequeue produces an ExitEnvironmentAction carrying fetched details"""

    def test_env_exit_dequeue_includes_details(
        self,
        session_queue: SessionActionQueue,
        job_entities: MagicMock,
    ) -> None:
        # GIVEN
        table_json = '[{"name":"Job.Name","type":"string","value":"Example Job"}]'
        env_details = EnvironmentDetails(
            environment=Environment(name="TestEnv", script=_TEST_ENVIRONMENT_SCRIPT),
            resolved_symbol_table_json=table_json,
        )
        job_entities.environment_details.return_value = env_details
        entry = EnvironmentQueueEntry(
            Mock(),
            EnvironmentAction(
                sessionActionId="action-1", actionType="ENV_EXIT", environmentId="env-1"
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["action-1"] = entry

        # WHEN
        result = session_queue.dequeue()

        # THEN
        assert isinstance(result, ExitEnvironmentAction)
        assert result.id == "action-1"
        # The details kwarg is passed to ExitEnvironmentAction; verify the
        # attribute is set (concurrent agent adds _details field).
        assert result._details is env_details  # type: ignore[attr-defined]
