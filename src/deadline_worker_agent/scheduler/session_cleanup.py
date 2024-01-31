# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import subprocess
import os
from threading import Lock

from openjd.sessions import SessionUser, PosixSessionUser, WindowsSessionUser
from typing import cast
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

    def _get_user(self, session: Session):
        if os.name == "posix":
            posix_user: PosixSessionUser = cast(PosixSessionUser, session.os_user)
            return posix_user.user
        else:
            windows_user: WindowsSessionUser = cast(WindowsSessionUser, session._os_user)
            return windows_user.user

    def register(self, session: Session):
        if session.os_user is None:
            return

        with self._user_session_map_lock:
            user_name = self._get_user(session)
            session_dict = self._user_session_map.get(user_name, None)
            if session_dict is None:
                session_dict = {}
                self._user_session_map[user_name] = session_dict

            session_dict[session.id] = session

    def deregister(self, session: Session):
        if session.os_user is None:
            return

        with self._user_session_map_lock:
            user_name = self._get_user(session)
            session_dict = self._user_session_map.get(user_name, None)
            if session_dict is None:
                return

            registered_session = session_dict.pop(session.id, None)
            if registered_session is None:
                return

            if len(session_dict) == 0:
                self._cleanup_session_user(session.os_user)
                self._user_session_map.pop(user_name, None)

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
