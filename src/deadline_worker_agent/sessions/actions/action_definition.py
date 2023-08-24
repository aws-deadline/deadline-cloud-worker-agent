# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from datetime import timedelta

from abc import ABC, abstractmethod

from ..session import Session


class SessionActionDefinition(ABC):
    """Abstract base class for an action that must be performed by a Worker


    Parameters
    ----------
    id : str
        The unique session action identifier
    """

    _id: str

    def __init__(
        self,
        *,
        id: str,
    ) -> None:
        self._id = id

    @property
    def id(self) -> str:
        """The unique identifier of the SessionAction"""
        return self._id

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

    @abstractmethod
    def human_readable(self) -> str:
        """Returns a structured human-readable string that represents the subject and verb of the
        action. The structural convention for the action ID is:

            SUBJECT.VERB[ (PARAM_NAME=PARAM_VALUE, ...) ]

        SUBJECT describes that subject of the action and may contain letters (a-z, A-Z),
        numbers (0-9), and hyphens (-), square brackets ([]), parentheses (()).

        VERB describes the verb of the action being performed and may contain letters(a-z, A-Z),
        and hyphens.

        PARAM_NAME and PARAM_VALUE describe the parameters to the action. The allowable values for
        are inherited from OpenJobIO step parameter spaces.

        For example:

        environment.activate()
        environment.deactivate()
        environment[maya].activate()
        environment[scene-airship].activate()
        step[render].run(frame=1)
        environment[scene-airshop].deactivate()
        environment[maya].deactivate()
        step[animate-ffmpeg].run()

        The above sequence of action IDs depict a worker session that first opens up AutoDesk Maya,
        loads a scene file, renders a frame from the scene, unloads the scene, closes the
        application and then animates the results using ffmpeg."""
        ...
