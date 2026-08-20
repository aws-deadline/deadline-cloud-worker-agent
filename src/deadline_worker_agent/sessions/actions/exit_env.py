# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ..job_entities import EnvironmentDetails
    from ..session import Session


class ExitEnvironmentAction(OpenjdAction):
    """Action to exit an environment within a Worker session

    Parameters
    ----------
    id : str
        A unique identifier for the session action
    environment_id : str
        The job environment identifier
    details : EnvironmentDetails | None
        Optional environment details carrying the pre-resolved symbol table
    """

    _environment_id: str
    _details: EnvironmentDetails | None

    def __init__(
        self,
        *,
        id: str,
        environment_id: str,
        details: EnvironmentDetails | None = None,
    ) -> None:
        super(ExitEnvironmentAction, self).__init__(
            id=id, action_log_kind=SessionActionLogKind.ENV_EXIT
        )
        self._environment_id = environment_id
        self._details = details

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._environment_id == other._environment_id
            and self._details == other._details
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
            job_env_id=self._environment_id,
            os_env_vars={"DEADLINE_SESSIONACTION_ID": self._id},
            resolved_symbol_table_json=self._details.resolved_symbol_table_json
            if self._details is not None
            else None,
        )
