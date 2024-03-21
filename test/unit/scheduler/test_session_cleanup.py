# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock, patch
import subprocess

from openjd.sessions import SessionUser, PosixSessionUser, WindowsSessionUser
import pytest
import os

from deadline_worker_agent.scheduler.session_cleanup import (
    SessionUserCleanupManager,
)
import deadline_worker_agent.scheduler.session_cleanup as session_cleanup_mod


class FakeSessionUser(SessionUser):
    def __init__(self, user: str):
        self.user = user

    @staticmethod
    def _get_process_user() -> str:
        return ""


class TestSessionUserCleanupManager:
    @pytest.fixture
    def manager(self) -> SessionUserCleanupManager:
        return SessionUserCleanupManager()

    @pytest.fixture
    def user_session_map_lock_mock(
        self,
        manager: SessionUserCleanupManager,
    ) -> Generator[MagicMock, None, None]:
        with patch.object(manager, "_user_session_map_lock") as mock:
            yield mock

    @pytest.fixture
    def os_user(self) -> SessionUser:
        if os.name == "posix":
            return PosixSessionUser(user="user", group="group")
        else:
            return WindowsSessionUser(user="user", password="fakepassword")

    @pytest.fixture
    def session(self, os_user: SessionUser) -> MagicMock:
        session_stub = MagicMock()
        session_stub.os_user = os_user
        session_stub.id = "session-123"
        return session_stub

    class TestRegister:
        def test_registers_session(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            os_user: SessionUser,
            user_session_map_lock_mock: MagicMock,
        ):
            # WHEN
            manager.register(session)

            # THEN
            registered_sessions = dict(manager.registered_sessions)
            assert os_user.user in registered_sessions
            assert session.id in registered_sessions[os_user.user]
            user_session_map_lock_mock.__enter__.assert_called_once()
            user_session_map_lock_mock.__exit__.assert_called_once()

        def test_register_skipped_no_user(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            user_session_map_lock_mock: MagicMock,
        ):
            # GIVEN
            session.os_user = None

            # WHEN
            manager.register(session)

            # THEN
            assert len(manager.registered_sessions) == 0
            user_session_map_lock_mock.__enter__.assert_not_called()
            user_session_map_lock_mock.__exit__.assert_not_called()

    class TestDeregister:
        def test_deregisters_session(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            os_user: PosixSessionUser,
            user_session_map_lock_mock: MagicMock,
        ):
            # GIVEN
            manager.register(session)
            user_session_map_lock_mock.reset_mock()

            # WHEN
            manager.deregister(session)

            # THEN
            registered_sessions = dict(manager.registered_sessions)
            assert os_user.user not in registered_sessions
            user_session_map_lock_mock.__enter__.assert_called_once()
            user_session_map_lock_mock.__exit__.assert_called_once()

        def test_deregister_skipped_no_user(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            os_user: PosixSessionUser,
            user_session_map_lock_mock: MagicMock,
        ):
            # GIVEN
            manager.register(session)
            user_session_map_lock_mock.reset_mock()
            second_session = MagicMock()
            second_session.os_user = None

            # WHEN
            manager.deregister(second_session)

            # THEN
            registered_sessions = dict(manager.registered_sessions)
            assert os_user.user in registered_sessions
            user_session_map_lock_mock.__enter__.assert_not_called()
            user_session_map_lock_mock.__exit__.assert_not_called()

    class TestCleanupSessionUser:
        @pytest.fixture
        def cleanup_session_user_processes_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(SessionUserCleanupManager, "cleanup_session_user_processes") as mock:
                yield mock

        def test_calls_cleanup_session_user_processes(
            self,
            os_user: SessionUser,
            manager: SessionUserCleanupManager,
            cleanup_session_user_processes_mock: MagicMock,
        ):
            # WHEN
            manager._cleanup_session_user(os_user)

            # THEN
            cleanup_session_user_processes_mock.assert_called_once_with(os_user)

        def test_skips_cleanup_when_configured_to(
            self,
            os_user: SessionUser,
            cleanup_session_user_processes_mock: MagicMock,
        ):
            # GIVEN
            manager = SessionUserCleanupManager(cleanup_session_user_processes=False)

            # WHEN
            manager._cleanup_session_user(os_user)

            # THEN
            cleanup_session_user_processes_mock.assert_not_called()

    class TestCleanupSessionUserProcesses:
        @pytest.fixture
        def agent_user(
            self,
        ) -> SessionUser:
            if os.name == "posix":
                return PosixSessionUser(user="agent_user", group="agent_group")
            else:
                return WindowsSessionUser(user="user", password="fakepassword")

        @pytest.fixture(autouse=True)
        def subprocess_check_output_mock(
            self,
            agent_user: SessionUser,
        ) -> Generator[MagicMock, None, None]:
            with patch.object(
                session_cleanup_mod.subprocess,
                "check_output",
                return_value=f"{agent_user.user}\n",
            ) as mock:
                yield mock

        @pytest.fixture(autouse=True)
        def subprocess_run_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(session_cleanup_mod.subprocess, "run") as mock:
                yield mock

        @pytest.mark.skipif(os.name != "posix", reason="Posix-only test.")
        def test_cleans_up_posix_processes(
            self,
            os_user: SessionUser,
            subprocess_run_mock: MagicMock,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)
            subprocess_run_mock.return_value.stdout = "stdout"
            subprocess_run_mock.return_value.stderr = "stderr"

            # WHEN
            SessionUserCleanupManager.cleanup_session_user_processes(os_user)

            # THEN
            assert (
                f"Cleaning up remaining session user processes for '{os_user.user}'" in caplog.text
            )
            if os.name == "posix":
                subprocess_run_mock.assert_called_once_with(
                    args=["sudo", "-u", os_user.user, "/usr/bin/pkill", "-eU", os_user.user],
                    capture_output=True,
                    check=True,
                    text=True,
                )
            assert "Stopped processes:\n" in caplog.text

        @pytest.mark.skipif(os.name != "nt", reason="Windows-only test.")
        def test_cleans_up_windows_processes(
            self,
            os_user: SessionUser,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)
            pid = 123
            bad_pid = 567
            mock_process_iter = MagicMock()
            mock_process_iter.return_value = [
                MagicMock(
                    info={"pid": pid, "name": "example.exe", "username": f"host\\{os_user.user}"}
                ),
                MagicMock(
                    info={
                        "pid": bad_pid,
                        "name": "another_example.exe",
                        "username": "host\\another_user",
                    }
                ),
            ]
            mock_open_process = MagicMock()
            mock_terminate_process = MagicMock()
            mock_close_handle = MagicMock()

            with (
                patch("psutil.process_iter", mock_process_iter),
                patch("win32api.OpenProcess", mock_open_process),
                patch("win32api.TerminateProcess", mock_terminate_process),
                patch("win32api.CloseHandle", mock_close_handle),
            ):
                # WHEN
                SessionUserCleanupManager.cleanup_session_user_processes(os_user)

            # THEN
            assert (
                f"Cleaning up remaining session user processes for '{os_user.user}'" in caplog.text
            )
            assert f"Stopped process PID: {pid}" in caplog.text
            assert f"Stopped process PID: {bad_pid}" not in caplog.text
            mock_terminate_process.assert_called_once()

        @pytest.mark.skipif(os.name != "nt", reason="Windows-only test.")
        def test_no_windows_processes_to_clean_up(
            self,
            os_user: SessionUser,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)
            mock_process_iter = MagicMock()
            mock_process_iter.return_value = [
                MagicMock(
                    info={
                        "pid": 123,
                        "name": "example.exe",
                        "username": "host\\another_user",
                    }
                ),
            ]
            mock_open_process = MagicMock()
            mock_terminate_process = MagicMock()
            mock_close_handle = MagicMock()

            with (
                patch("psutil.process_iter", mock_process_iter),
                patch("win32api.OpenProcess", mock_open_process),
                patch("win32api.TerminateProcess", mock_terminate_process),
                patch("win32api.CloseHandle", mock_close_handle),
            ):
                # WHEN
                SessionUserCleanupManager.cleanup_session_user_processes(os_user)

            # THEN
            assert (
                f"No processes stopped because none were found running as '{os_user.user}'"
                in caplog.text
            )
            mock_terminate_process.assert_not_called()

        @pytest.mark.skipif(os.name != "posix", reason="Posix-only test.")
        def test_no_posix_processes_to_clean_up(
            self,
            os_user: PosixSessionUser,
            subprocess_run_mock: MagicMock,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)
            subprocess_run_mock.side_effect = subprocess.CalledProcessError(returncode=1, cmd="")

            # WHEN
            SessionUserCleanupManager.cleanup_session_user_processes(os_user)

            # THEN
            subprocess_run_mock.assert_called_once()
            assert (
                f"No processes stopped because none were found running as '{os_user.user}'"
                in caplog.text
            )

        @pytest.mark.skipif(os.name != "posix", reason="Posix-only test.")
        def test_fails_to_clean_up_posix_processes(
            self,
            os_user: PosixSessionUser,
            subprocess_run_mock: MagicMock,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)
            err = subprocess.CalledProcessError(returncode=123, cmd="")
            subprocess_run_mock.side_effect = err

            # WHEN
            with pytest.raises(subprocess.CalledProcessError) as raised_err:
                SessionUserCleanupManager.cleanup_session_user_processes(os_user)

            # THEN
            subprocess_run_mock.assert_called_once()
            assert f"Failed to stop processes running as '{os_user.user}': {err}" in caplog.text
            assert raised_err.value is err

        def test_skips_if_session_user_is_agent_user(
            self,
            subprocess_run_mock: MagicMock,
            agent_user: PosixSessionUser,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)

            # WHEN
            with patch(
                "getpass.getuser",
                return_value=agent_user.user,  # type: ignore
            ):
                SessionUserCleanupManager.cleanup_session_user_processes(agent_user)

            # THEN
            subprocess_run_mock.assert_not_called()
            assert (
                f"Skipping cleaning up processes because the session user matches the agent user '{agent_user.user}'"
                in caplog.text
            )
