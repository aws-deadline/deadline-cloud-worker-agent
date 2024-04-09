# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from datetime import timedelta
from typing import Optional

from abc import ABC, abstractmethod

from ..session import Session
from ...log_messages import SessionActionLogKind


class SessionActionDefinition(ABC):
    """Abstract base class for an action that must be performed by a Worker


    Parameters
    ----------
    id : str
        The unique session action identifier
    """

    _id: str
    _action_log_kind: SessionActionLogKind
    _step_id: Optional[str]

    def __init__(
        self, *, id: str, action_log_kind: SessionActionLogKind, step_id: Optional[str] = None
    ) -> None:
        self._id = id
        self._action_log_kind = action_log_kind
        self._step_id = step_id

    @property
    def id(self) -> str:
        """The unique identifier of the SessionAction"""
        return self._id

    @property
    def action_log_kind(self) -> SessionActionLogKind:
        return self._action_log_kind

    @property
    def step_id(self) -> Optional[str]:
        return self._step_id

    @abstractmethod
    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Starts running the action.

        The action should be initiated but ran asynchronously on a different thread and not block
        the calling thread.

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """
        ...

    @abstractmethod
    def cancel(
        self,
        *,
        session: Session,
        time_limit: timedelta | None = None,
    ) -> None:
        """Cancels the running action.

        Parameters
        ----------
        session : Session
            The session that the action is running within
        time_limit : timedelta | None
            If specified and the action uses the NOTIFY_THEN_TERMINATE cancelation method, overrides
            the grace time allowed before force-terminating the action.

        Raises
        ------
        deadline_worker_agent.sessions.actions.errors.CancelationError
            Raised if there was an issue initiating the canceling of the running action
        """
        ...
