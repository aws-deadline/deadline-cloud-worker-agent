# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock, patch
import subprocess

from openjd.sessions import SessionUser, PosixSessionUser
import pytest

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
    def os_user(self) -> PosixSessionUser:
        return PosixSessionUser(user="user", group="group")

    @pytest.fixture
    def session(self, os_user: PosixSessionUser) -> MagicMock:
        session_stub = MagicMock()
        session_stub.os_user = os_user
        session_stub.id = "session-123"
        return session_stub

    class TestRegister:
        def test_registers_session(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            os_user: PosixSessionUser,
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

        def test_register_raises_windows_not_supported(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            user_session_map_lock_mock: MagicMock,
        ):
            # GIVEN
            session.os_user = FakeSessionUser("user-123")

            # WHEN
            with pytest.raises(NotImplementedError) as raised_err:
                manager.register(session)

            # THEN
            assert str(raised_err.value) == "Windows not supported"
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

        def test_deregister_raises_windows_not_supported(
            self,
            manager: SessionUserCleanupManager,
            session: MagicMock,
            user_session_map_lock_mock: MagicMock,
        ):
            # GIVEN
            session.os_user = FakeSessionUser("user-123")

            # WHEN
            with pytest.raises(NotImplementedError) as raised_err:
                manager.deregister(session)

            # THEN
            assert str(raised_err.value) == "Windows not supported"
            assert len(manager.registered_sessions) == 0
            user_session_map_lock_mock.__enter__.assert_not_called()
            user_session_map_lock_mock.__exit__.assert_not_called()

    class TestCleanupSessionUser:
        @pytest.fixture
        def cleanup_session_user_processes_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(SessionUserCleanupManager, "cleanup_session_user_processes") as mock:
                yield mock

        def test_calls_cleanup_session_user_processes(
            self,
            os_user: PosixSessionUser,
            manager: SessionUserCleanupManager,
            cleanup_session_user_processes_mock: MagicMock,
        ):
            # WHEN
            manager._cleanup_session_user(os_user)

            # THEN
            cleanup_session_user_processes_mock.assert_called_once_with(os_user)

        def test_skips_cleanup_when_configured_to(
            self,
            os_user: PosixSessionUser,
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
            os_user: PosixSessionUser,
        ) -> PosixSessionUser:
            return PosixSessionUser(user=f"agent_{os_user.user}", group=f"agent_{os_user.group}")

        @pytest.fixture(autouse=True)
        def subprocess_check_output_mock(
            self,
            agent_user: PosixSessionUser,
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

        def test_cleans_up_processes(
            self,
            os_user: PosixSessionUser,
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
            subprocess_run_mock.assert_called_once_with(
                args=["sudo", "-u", os_user.user, "/usr/bin/pkill", "-eU", os_user.user],
                capture_output=True,
                check=True,
                text=True,
            )
            assert "Stopped processes:\n" in caplog.text

        def test_not_posix_user(
            self,
            subprocess_run_mock: MagicMock,
        ):
            # GIVEN
            fake_user = FakeSessionUser("user-123")

            # WHEN
            with pytest.raises(NotImplementedError) as raised_err:
                SessionUserCleanupManager.cleanup_session_user_processes(fake_user)

            # THEN
            assert str(raised_err.value) == "Windows not supported"
            subprocess_run_mock.assert_not_called()

        def test_no_processes_to_clean_up(
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

        def test_fails_to_clean_up_processes(
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
            subprocess_check_output_mock: MagicMock,
            agent_user: PosixSessionUser,
            caplog: pytest.LogCaptureFixture,
        ):
            # GIVEN
            caplog.set_level(0)

            # WHEN
            SessionUserCleanupManager.cleanup_session_user_processes(agent_user)

            # THEN
            subprocess_check_output_mock.assert_called_once_with(["/usr/bin/whoami"], text=True)
            subprocess_run_mock.assert_not_called()
            assert (
                f"Skipping cleaning up processes because the session user matches the agent user '{agent_user.user}'"
                in caplog.text
            )
