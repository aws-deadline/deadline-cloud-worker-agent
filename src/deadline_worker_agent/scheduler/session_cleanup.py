# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import subprocess
from threading import Lock

from openjobio.sessions import SessionUser, PosixSessionUser

from .log import LOGGER
from ..sessions import Session

logger = LOGGER


class SessionUserCleanupManager:
    """
    Class that manages cleaning up sessions with users. Note that the Session class already has its own
    cleanup, this class implements extra cleanup steps that the Session class cannot do.

    This class keeps track of all sessions registered with it and performs cleanup steps
    when certain criteria are met. For example, when all sessions with a specific session user are
    cleaned up, this class will stop any remaining processes running as that session user.
    """

    _user_session_map_lock: Lock
    _user_session_map: dict[str, dict[str, Session]]
    """Map of session user to a map of session IDs and sessions using that user"""

    _cleanup_session_user_processes: bool

    def __init__(
        self,
        cleanup_session_user_processes: bool = True,
    ) -> None:
        self._user_session_map_lock = Lock()
        self._user_session_map = {}
        self._cleanup_session_user_processes = cleanup_session_user_processes

    def register(self, session: Session):
        if session.os_user is None:
            return
        if not isinstance(session.os_user, PosixSessionUser):
            # TODO: Windows support, or modify the SessionUser class to either:
            # 1. Require subclasses to provide a default str representation
            # 2. Require subclasses to implement __eq__
            raise NotImplementedError("Windows not supported")

        with self._user_session_map_lock:
            session_dict = self._user_session_map.get(session.os_user.user, None)
            if session_dict is None:
                session_dict = {}
                self._user_session_map[session.os_user.user] = session_dict

            session_dict[session.id] = session

    def deregister(self, session: Session):
        if session.os_user is None:
            return
        if not isinstance(session.os_user, PosixSessionUser):
            # TODO: Windows support, or modify the SessionUser class to either:
            # 1. Require subclasses to implement __repr__ so its output can be used as a dict key
            # 2. Require subclasses to implement __eq__ and __hash__ so the class can be used as
            #    a dict key
            raise NotImplementedError("Windows not supported")

        with self._user_session_map_lock:
            session_dict = self._user_session_map.get(session.os_user.user, None)
            if session_dict is None:
                return

            registered_session = session_dict.pop(session.id, None)
            if registered_session is None:
                return

            if len(session_dict) == 0:
                self._cleanup_session_user(session.os_user)
                self._user_session_map.pop(session.os_user.user, None)

    @property
    def registered_sessions(self):
        return self._user_session_map.items()

    def _cleanup_session_user(self, user: SessionUser):
        if self._cleanup_session_user_processes:
            try:
                # Clean up any remaining job user processes
                SessionUserCleanupManager.cleanup_session_user_processes(user)
            except Exception as e:
                logger.warn(f"Failed to stop session user processes: {e}")

    @staticmethod
    def cleanup_session_user_processes(user: SessionUser):
        if not isinstance(user, PosixSessionUser):
            # TODO: Windows support
            logger.warning(
                "Stopping session user processes will be skipped because this feature is only supported on POSIX systems"
            )
            raise NotImplementedError("Windows not supported")

        # Check that the session user isn't the current user (agent user)
        current_user = subprocess.check_output(["/usr/bin/whoami"], text=True)
        if current_user == user.user:
            logger.info(
                f"Skipping cleaning up processes because the session user matches the agent user '{current_user}'"
            )
            return

        logger.info(f"Cleaning up remaining session user processes for '{user.user}'")
        try:
            pkill_result = subprocess.run(
                args=["sudo", "-u", user.user, "/usr/bin/pkill", "-eU", user.user],
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                logger.info(
                    f"No processes stopped because none were found running as '{user.user}'"
                )
                return
            else:
                logger.warning(f"Failed to stop processes running as '{user.user}': {e}")
                raise
        else:
            # pkill stdout will look like:
            #  killed (pid 1111)
            #  killed (pid 2222)
            #  etc.
            pkill_output = "\n".join([pkill_result.stdout, pkill_result.stderr]).rstrip()
            logger.info(f"Stopped processes:\n{pkill_output}")
