# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import (
    CancelledError as FutureCancelledError,
    Executor,
    TimeoutError as FutureTimeoutError,
)
from functools import partial
from datetime import timedelta
from logging import getLogger, LoggerAdapter
from threading import Event
from typing import Any, TYPE_CHECKING, Optional

from deadline.job_attachments.exceptions import AssetSyncCancelledError
from openjd.sessions import ActionState, ActionStatus, LOG as OPENJD_LOG

from ..session import Session
from ...log_messages import SessionActionLogKind

from .action_definition import SessionActionDefinition

if TYPE_CHECKING:
    from concurrent.futures import Future
    from ..session import Session
    from ..job_entities import JobAttachmentDetails, StepDetails


logger = getLogger(__name__)


class SyncCanceled(Exception):
    """Exception indicating the synchronization was canceled"""

    pass


class SyncInputJobAttachmentsAction(SessionActionDefinition):
    """Action to synchronize input job attachments for a AWS Deadline Cloud job

    Parameters
    ----------
    id : str
        The unique action identifier
    """

    _cancel: Event
    _future: Future[None]
    _job_attachment_details: Optional[JobAttachmentDetails]
    _step_details: Optional[StepDetails]

    def __init__(
        self,
        *,
        id: str,
        session_id: str,
        job_attachment_details: Optional[JobAttachmentDetails] = None,
        step_details: Optional[StepDetails] = None,
    ) -> None:
        super(SyncInputJobAttachmentsAction, self).__init__(
            id=id,
            action_log_kind=(
                SessionActionLogKind.JA_SYNC_INPUT
                if step_details is None
                else SessionActionLogKind.JA_DEP_SYNC
            ),
            step_id=step_details.step_id if step_details is not None else None,
        )
        self._cancel = Event()
        self._job_attachment_details = job_attachment_details
        self._step_details = step_details
        self._logger = LoggerAdapter(OPENJD_LOG, extra={"session_id": session_id})

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
        self._logger.info("")
        self._logger.info("==============================================")
        self._logger.info(f"--------- {section_title}")
        self._logger.info("==============================================")

        sync_asset_inputs_kwargs: dict[str, Any] = {
            "cancel": self._cancel,
            "job_attachment_details": self._job_attachment_details,
        }
        if self._step_details:
            sync_asset_inputs_kwargs["step_dependencies"] = self._step_details.dependencies

        self._future = executor.submit(
            session.sync_asset_inputs,
            **sync_asset_inputs_kwargs,
        )
        session.update_action(
            action_status=ActionStatus(state=ActionState.RUNNING),
        )
        done_with_session = partial(self._on_done, session=session)
        self._future.add_done_callback(done_with_session)

    def _on_done(
        self,
        future: Future[None],
        session: Session,
    ) -> None:
        """Callback called when the future completes. Reports the action result depending on the
        outcome.

        Parameters
        ----------
        future : Future[None]
            The future tracking the asset synchronization.
        session : Session
            The session that the action is running in
        """
        action_status: ActionStatus = ActionStatus(state=ActionState.SUCCESS)
        try:
            future.result()
        except (FutureCancelledError, AssetSyncCancelledError):
            action_status = ActionStatus(
                state=ActionState.CANCELED,
                fail_message="Canceled",
            )
        except Exception as e:
            session.logger.exception(e)
            action_status = ActionStatus(
                state=ActionState.FAILED,
                fail_message=str(e),
            )
        finally:
            # We need to directly complete the action. Other actions rely on the Open Job Description session's
            # callback to complete the action
            session.update_action(action_status=action_status)

    def cancel(self, *, session: Session, time_limit: timedelta | None = None) -> None:
        if self._future.cancel():
            session.update_action(
                action_status=ActionStatus(
                    state=ActionState.CANCELED,
                    fail_message="Canceled",
                )
            )
            return

        if time_limit:
            assert time_limit > timedelta(), f"time_limit must be positive, but got {time_limit}"
            try:
                self._future.result(timeout=time_limit.total_seconds())
            except FutureTimeoutError:
                self._cancel.set()
            except Exception:
                # Other exceptions are handled
                pass
        else:
            self._cancel.set()
