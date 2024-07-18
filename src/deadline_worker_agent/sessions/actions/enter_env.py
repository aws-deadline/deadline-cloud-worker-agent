# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from typing import Any, TYPE_CHECKING

from openjd.sessions import EnvironmentIdentifier

from ..job_entities import EnvironmentDetails
from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ...api_models import EnvironmentAction
    from ..session import Session
    from .action_definition import SessionActionDefinition


class EnterEnvironmentAction(OpenjdAction):
    """Action to enter an environment within a Worker session

    Parameters
    ----------
    id : str
        A unique identifier for the session action
    job_env_id : str
        A unique identifier for the environment within the Open Job Description job
    environment_details : EnvironmentDetails
        The environment details
    """

    _job_env_id: str
    _details: EnvironmentDetails
    _session_env_id: EnvironmentIdentifier | None = None

    def __init__(
        self,
        *,
        id: str,
        job_env_id: str,
        details: EnvironmentDetails,
    ) -> None:
        super(EnterEnvironmentAction, self).__init__(
            id=id, action_log_kind=SessionActionLogKind.ENV_ENTER
        )
        self._job_env_id = job_env_id
        self._details = details

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._job_env_id == other._job_env_id
            and self._session_env_id == other._session_env_id
            and self._details == other._details
        )

    @classmethod
    def from_boto(
        cls,
        data: EnvironmentAction,
        *,
        details: EnvironmentDetails,
    ) -> SessionActionDefinition:
        if not isinstance(data, dict):
            raise TypeError(f"data must be a dict, but got {type(data)}")

        if (action_type := data.get("actionType", None)) is None:
            raise ValueError("type is not specified")
        elif action_type != "ENV_ENTER":
            raise ValueError(f"type must be ENV_ENTER but got {action_type}")

        if (environment_id := data.get("environmentId", None)) is None:
            raise ValueError("environmentId is not specified")
        elif not isinstance(environment_id, str):
            raise TypeError(f"expected environmentId to be str but got {type(environment_id)}")

        if (action_id := data.get("sessionActionId", None)) is None:
            raise ValueError("sessionActionId is not specified")
        elif not isinstance(action_id, str):
            raise TypeError(f"expected sessionActionId to be str but got {type(action_id)}")

        return EnterEnvironmentAction(
            id=action_id,
            job_env_id=environment_id,
            details=details,
        )

    @property
    def session_env_id(self) -> EnvironmentIdentifier | None:
        return self._session_env_id

    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Initiates the entering an environment in the session

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """
        session.enter_environment(
            job_env_id=self._job_env_id,
            environment=self._details.environment,
            os_env_vars={"DEADLINE_SESSIONACTION_ID": self._id},
        )
