# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import pytest

from deadline_worker_agent.log_messages import SessionActionLogKind
from deadline_worker_agent.sessions.errors import (
    EnvironmentDetailsError,
    JobAttachmentDetailsError,
    JobEntityUnsupportedSchemaError,
    SessionActionError,
    StepDetailsError,
)


# All SessionActionError subclasses that share the base (action_id, action_log_kind,
# message, *, step_id, task_id) constructor signature.
MESSAGE_ERROR_TYPES: tuple[type[SessionActionError], ...] = (
    SessionActionError,
    EnvironmentDetailsError,
    JobAttachmentDetailsError,
    StepDetailsError,
)


class TestSessionActionError:
    """Contract tests for SessionActionError and its subclasses.

    Session._start_action catches SessionActionError raised while dequeueing an
    action and reports the failure by reading ``e.step_id``/``e.task_id``. If the
    base ``__init__`` fails to store those attributes, that handler raises an
    AttributeError; the action is then never attempted and the task
    reschedule-loops instead of failing cleanly. These tests pin the attribute
    contract for every subclass so that regression cannot reappear.
    """

    @pytest.mark.parametrize("error_type", MESSAGE_ERROR_TYPES)
    def test_stores_step_and_task_id(self, error_type: type[SessionActionError]) -> None:
        # WHEN
        err = error_type(
            "action-id",
            SessionActionLogKind.TASK_RUN,
            "something went wrong",
            step_id="step-123",
            task_id="task-456",
        )

        # THEN
        assert err.step_id == "step-123"
        assert err.task_id == "task-456"
        assert err.action_id == "action-id"
        assert err.action_log_kind == SessionActionLogKind.TASK_RUN
        assert str(err) == "something went wrong"

    @pytest.mark.parametrize("error_type", MESSAGE_ERROR_TYPES)
    def test_step_and_task_id_default_to_none(self, error_type: type[SessionActionError]) -> None:
        # WHEN
        err = error_type("action-id", SessionActionLogKind.ENV_ENTER, "boom")

        # THEN
        assert err.step_id is None
        assert err.task_id is None

    def test_unsupported_schema_error_stores_step_and_task_id(self) -> None:
        # JobEntityUnsupportedSchemaError has a distinct signature (schema_version
        # instead of message) but must honor the same step_id/task_id contract.
        # WHEN
        err = JobEntityUnsupportedSchemaError(
            "action-id",
            SessionActionLogKind.TASK_RUN,
            "9999-99",
            step_id="step-123",
            task_id="task-456",
        )

        # THEN
        assert err.step_id == "step-123"
        assert err.task_id == "task-456"
        assert err.schema_version == "9999-99"

    def test_unsupported_schema_error_defaults_to_none(self) -> None:
        # WHEN
        err = JobEntityUnsupportedSchemaError(
            "action-id", SessionActionLogKind.ENV_ENTER, "9999-99"
        )

        # THEN
        assert err.step_id is None
        assert err.task_id is None
