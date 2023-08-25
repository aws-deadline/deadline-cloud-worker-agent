# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from datetime import timedelta

from ..session import Session
from .action_definition import SessionActionDefinition
from ..errors import CancelationError


class OpenjdAction(SessionActionDefinition):
    """Common base class for Open Job Description session actions"""

    _current_action_cancel_sent = False

    def cancel(self, *, session: Session, time_limit: timedelta | None = None) -> None:
        if self._current_action_cancel_sent:
            return

        try:
            session._session.cancel_action(time_limit=time_limit)
        except RuntimeError:
            if action_status := session._session.action_status:
                action_state = action_status.state
                raise CancelationError(
                    f"Could not cancel {self.id}. It completed as {action_state.name}"
                ) from None
            else:
                raise CancelationError(f"Could not cancel {self.id}. No action was run") from None
        finally:
            self._current_action_cancel_sent = True

    cancel.__doc__ = SessionActionDefinition.cancel.__doc__
