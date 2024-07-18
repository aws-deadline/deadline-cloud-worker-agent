# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ..session import Session


class ExitEnvironmentAction(OpenjdAction):
    """Action to exit an environment within a Worker session

    Parameters
    ----------
    id : str
        A unique identifier for the session action
    environment_id : str
        The job environment identifier
    """

    _environment_id: str

    def __init__(
        self,
        *,
        id: str,
        environment_id: str,
    ) -> None:
        super(ExitEnvironmentAction, self).__init__(
            id=id, action_log_kind=SessionActionLogKind.ENV_EXIT
        )
        self._environment_id = environment_id

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._environment_id == other._environment_id
        )

    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Initiates the exiting of an environment in the session

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """
        session.exit_environment(
            job_env_id=self._environment_id, os_env_vars={"DEADLINE_SESSIONACTION_ID": self._id}
        )
