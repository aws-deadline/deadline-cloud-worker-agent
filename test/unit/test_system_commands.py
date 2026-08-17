# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the trusted-path command resolver.

These pin the security properties of ``_system_commands``. Each has been
mutation-checked: reverting the corresponding production behaviour makes a named
test here fail. See the module docstring of ``_system_commands`` for why each
property matters.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from deadline_worker_agent._system_commands import (
    SystemCommandNotFoundError,
    find_system_command,
    system_command_path,
    trusted_directories,
)


@pytest.fixture
def executable_dir(tmp_path: Path) -> Path:
    """A directory containing an executable file named ``target-cmd``."""
    target = tmp_path / "target-cmd"
    target.write_text("#!/bin/sh\ntrue\n")
    target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return tmp_path


class TestOnlySearchesTrustedDirectories:
    def test_resolves_a_command_in_a_searched_directory(self, executable_dir: Path) -> None:
        """The negative control. Without this, "nothing was found" results below
        would be indistinguishable from a resolver that never finds anything."""
        # GIVEN
        with patch(
            "deadline_worker_agent._system_commands.trusted_directories",
            return_value=(str(executable_dir),),
        ):
            # WHEN
            result = find_system_command("target-cmd")

        # THEN
        assert result == str(executable_dir / "target-cmd")

    def test_does_not_resolve_a_command_outside_searched_directories(
        self, executable_dir: Path
    ) -> None:
        # GIVEN
        with patch(
            "deadline_worker_agent._system_commands.trusted_directories",
            return_value=("/usr/bin", "/bin"),
        ):
            # WHEN
            result = find_system_command("target-cmd")

        # THEN
        assert result is None

    def test_ignores_path_even_when_it_contains_a_matching_command(
        self, executable_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pins "PATH is never read".

        This is the property a ``shutil.which`` implementation would silently
        violate, and the one a PATH fallback for missing commands would undo.
        """
        # GIVEN
        monkeypatch.setenv("PATH", str(executable_dir))
        with patch(
            "deadline_worker_agent._system_commands.trusted_directories",
            return_value=("/usr/bin", "/bin"),
        ):
            # WHEN
            result = find_system_command("target-cmd")

        # THEN
        assert result is None, "PATH was consulted"

    def test_returns_the_first_matching_directory(self, tmp_path: Path) -> None:
        """Order is load-bearing: on NixOS the setuid sudo wrapper must win over
        a non-setuid /usr/bin copy."""
        # GIVEN
        first, second = tmp_path / "first", tmp_path / "second"
        for directory in (first, second):
            directory.mkdir()
            target = directory / "target-cmd"
            target.write_text("#!/bin/sh\ntrue\n")
            target.chmod(target.stat().st_mode | stat.S_IXUSR)

        with patch(
            "deadline_worker_agent._system_commands.trusted_directories",
            return_value=(str(first), str(second)),
        ):
            # WHEN
            result = find_system_command("target-cmd")

        # THEN
        assert result == str(first / "target-cmd")


class TestRejectsNonBareNames:
    @pytest.mark.parametrize(
        "name",
        [
            pytest.param("a/b", id="forward-slash"),
            pytest.param("a\\b", id="backslash"),
            pytest.param("", id="empty"),
            pytest.param(".", id="curdir"),
            pytest.param("..", id="pardir"),
        ],
    )
    def test_rejects_name_with_a_path_component(self, name: str) -> None:
        with pytest.raises(ValueError):
            find_system_command(name)

    def test_rejects_traversal_even_though_the_target_is_reachable(self, tmp_path: Path) -> None:
        """The guard must be about the name, not about whether the join happens to
        land on a real file -- so prove the target IS reachable by that join
        before asserting the name is refused.

        Without this, a test using a directory whose parent holds nothing would
        pass even with the guard deleted, pinning nothing.
        """
        # GIVEN
        target = tmp_path / "target-cmd"
        target.write_text("#!/bin/sh\ntrue\n")
        target.chmod(target.stat().st_mode | stat.S_IXUSR)
        nested = tmp_path / "nested"
        nested.mkdir()
        assert os.path.isfile(os.path.join(str(nested), "../target-cmd")), (
            "precondition: the traversal target is reachable by this join"
        )

        # WHEN / THEN
        with patch(
            "deadline_worker_agent._system_commands.trusted_directories",
            return_value=(str(nested),),
        ):
            with pytest.raises(ValueError, match="path separator"):
                find_system_command("../target-cmd")

    def test_rejection_is_valueerror_not_notfound(self) -> None:
        """A bad name is a caller bug; a missing command is an environment
        problem. Conflating them would let a caller mistake one for the other."""
        with pytest.raises(ValueError):
            system_command_path("a/b")


class TestMissingCommandRaises:
    def test_find_returns_none(self) -> None:
        assert find_system_command("deadline-definitely-not-installed") is None

    def test_system_command_path_raises_rather_than_returning_the_bare_name(self) -> None:
        """The silent-fallback failure mode: returning "sudo" here would look
        fixed and behave exactly as the vulnerability did."""
        with pytest.raises(SystemCommandNotFoundError) as excinfo:
            system_command_path("deadline-definitely-not-installed")

        message = str(excinfo.value)
        assert "deadline-definitely-not-installed" in message
        assert "PATH is deliberately not searched" in message

    def test_is_not_a_filenotfounderror(self) -> None:
        """Callers around subprocess treat FileNotFoundError as "optional tool
        absent, carry on". This must not be absorbed by that handling."""
        assert not issubclass(SystemCommandNotFoundError, FileNotFoundError)


class TestTrustedDirectories:
    def test_all_entries_are_absolute(self) -> None:
        """A relative entry would resolve against the process working directory."""
        for directory in trusted_directories():
            assert os.path.isabs(directory), f"{directory} is not absolute"

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX directory layout")
    def test_posix_searches_the_setuid_wrapper_directory_before_usr_bin(self) -> None:
        directories = trusted_directories()

        assert directories.index("/run/wrappers/bin") < directories.index("/usr/bin")

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX directory layout")
    def test_posix_searches_both_sbin_locations(self) -> None:
        """shutdown is at /usr/sbin/shutdown on usr-merged distributions but only
        at /sbin/shutdown on some Debian releases. Hardcoding either one broke a
        host; both must be searched."""
        directories = trusted_directories()

        assert "/usr/sbin" in directories
        assert "/sbin" in directories

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only layout")
    def test_windows_searches_system32_under_systemroot(self) -> None:
        directories = trusted_directories()

        assert len(directories) == 1
        assert directories[0].lower().endswith("system32")
