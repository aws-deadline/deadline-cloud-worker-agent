# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the trusted-path command resolver.

These pin the security properties of ``_system_commands``. Each has been
mutation-checked: reverting the corresponding production behaviour makes a named
test here fail. See the module docstring of ``_system_commands`` for why each
property matters.
"""

from __future__ import annotations

import os
import posixpath
import re
import stat
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from deadline_worker_agent.startup import entrypoint as entrypoint_mod
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
            # This resolver is the one with a Windows branch, so the drive-relative
            # case is directly exploitable here:
            # ntpath.join(r"C:\Windows\System32", "D:evil") == "D:evil", which
            # escapes every trusted directory while containing no separator.
            pytest.param("D:evil", id="drive-relative"),
            pytest.param("C:evil", id="drive-relative-same-drive"),
            pytest.param("a:b", id="colon"),
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

    def test_is_a_filenotfounderror(self) -> None:
        """The inverse of what an earlier revision asserted, and the reversal is the
        point.

        That revision made this a plain Exception so "carry on degraded" handlers
        could not absorb it -- a theory about surrounding code that was never
        checked against it. The semantics this condition has are
        FileNotFoundError's: the thing we meant to launch is not there, which is
        exactly how capabilities.py and metrics.py already treat a missing
        nvidia-smi.
        """
        assert issubclass(SystemCommandNotFoundError, FileNotFoundError)
        assert SystemCommandNotFoundError is not FileNotFoundError


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
    def test_posix_has_the_two_nixos_entries_as_a_pair(self) -> None:
        """/run/wrappers/bin alone supports no complete code path: it holds only the
        setuid wrappers, so on NixOS it resolves sudo and nothing else. pkill lives
        in the sw/bin symlink farm, so without that entry the ordering would resolve
        sudo and then fail on pkill."""
        directories = trusted_directories()

        assert "/run/wrappers/bin" in directories
        assert "/run/current-system/sw/bin" in directories

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX directory layout")
    def test_posix_searches_both_sbin_locations(self) -> None:
        """Both sbin directories are searched, for commands that live only there.

        The original justification for these two entries was `shutdown`, which is
        no longer resolved at all (its path is a sudoers contract, see
        TestShutdownPathIsASudoersContract). So today no production caller resolves
        an sbin command: sudo and pkill are both /usr/bin, and shutdown.exe is
        System32.

        They are kept rather than deleted because the resolver is general and the
        split is real -- on non-usr-merged distributions a system command can exist
        only under /sbin -- but the honest status is "no current caller", not "the
        shutdown case requires it".
        """
        directories = trusted_directories()

        assert "/usr/sbin" in directories
        assert "/sbin" in directories

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only layout")
    def test_windows_searches_system32_under_systemroot(self) -> None:
        directories = trusted_directories()

        assert len(directories) == 1
        assert directories[0].lower().endswith("system32")


class TestShutdownPathIsASudoersContract:
    """`shutdown`'s path is granted by a sudoers rule, so it must not be resolved.

    ``installer/install.sh`` writes ``/etc/sudoers.d/deadline-worker-shutdown``
    containing a ``NOPASSWD:`` rule for one *exact* path. sudoers matches that path
    literally, so if the agent invokes a different absolute path for the same binary
    -- which a trusted-directory search can legitimately produce, since
    ``/usr/bin`` is searched before ``/usr/sbin`` and both exist on a usr-merged
    distribution -- the rule stops matching, sudo prompts for a password, and
    shutdown-on-stop fails.

    These tests read install.sh rather than restating its path, so the pairing
    cannot drift apart silently. That is the whole point: a constant asserted
    against a copy of itself would pin nothing.
    """

    @staticmethod
    def _granted_shutdown_argv(installer: str) -> list[str]:
        """The full argv the given installer's NOPASSWD rule grants.

        The whole argv, not just the path. sudo matches the entire command line, so
        pinning only the path would let two silent drifts through: the macOS rule
        losing its ``-h`` while the agent still sends it, or the Linux rule gaining
        one while the agent sends a bare ``now``. Either produces a password prompt
        at shutdown time, which is exactly the failure this test exists to catch.
        """
        script = (
            Path(__file__).parents[2] / "src" / "deadline_worker_agent" / "installer" / installer
        )
        content = script.read_text()
        match = re.search(r"NOPASSWD:\s*(\S.*?)\s*$", content, re.MULTILINE)
        assert match is not None, (
            f"Could not find the NOPASSWD shutdown rule in {installer}. If the rule "
            "moved or changed shape, this test needs updating -- do not delete it, "
            "the contract it guards is still real."
        )
        return match.group(1).split()

    @pytest.mark.parametrize(
        "installer,platform,expected_flags",
        [
            pytest.param("install.sh", "linux", ["now"], id="linux"),
            pytest.param("install_macos.sh", "darwin", ["-h", "now"], id="macOS"),
        ],
    )
    def test_shutdown_argv_matches_the_installer_sudoers_rule(
        self, installer: str, platform: str, expected_flags: list
    ) -> None:
        """Both platforms have their own rule, and both must be pinned.

        install_macos.sh grants `/sbin/shutdown -h now` and says in its own comment
        that "the sudoers command MUST continue to match that argv exactly". The
        macOS constant agreed with it only by coincidence until this test existed.
        """
        # GIVEN the argv the agent builds for this platform, minus the sudo prefix
        granted = self._granted_shutdown_argv(installer)
        configuration = MagicMock()
        configuration.no_shutdown = False
        with (
            patch.object(sys, "platform", platform),
            patch.object(
                entrypoint_mod, "system_command_path", side_effect=lambda n: f"/trusted/{n}"
            ),
            patch.object(entrypoint_mod.subprocess, "Popen") as popen,
        ):
            popen.return_value.communicate.return_value = (b"", b"")
            popen.return_value.returncode = 0
            entrypoint_mod._host_shutdown(config=configuration)

        # WHEN the sudo prefix is dropped
        invoked = list(popen.call_args.args[0])
        assert invoked[0] == "/trusted/sudo"
        invoked_command = invoked[1:]

        # THEN
        assert invoked_command == granted, (
            f"The agent would run {invoked_command!r} but {installer} grants "
            f"{granted!r}. sudo matches the whole command line, so these must agree."
        )
        assert invoked_command[1:] == expected_flags

    @pytest.mark.parametrize("installer", ["install.sh", "install_macos.sh"])
    def test_the_granted_path_is_absolute(self, installer: str) -> None:
        """A relative path in a sudoers rule would not be a contract at all.

        posixpath rather than os.path: the granted path is a POSIX path read out of
        a shell script, and from Python 3.13 ntpath.isabs() treats a single-slash
        path as drive-relative rather than absolute. Using os.path made this a
        statement about the host running the tests, and it failed the
        windows-latest 3.13 leg for that reason.
        """
        assert posixpath.isabs(self._granted_shutdown_argv(installer)[0])

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="trusted_directories() returns the Windows System32 layout there, "
        "which has no /usr/bin or /usr/sbin to order",
    )
    def test_shutdown_is_not_looked_up_in_the_trusted_directories(self) -> None:
        """The regression this guards: if `shutdown` were resolved, the search order
        could return a path outside the sudoers grant.

        Asserted structurally -- `/usr/bin` really does precede `/usr/sbin`, so a
        resolved `shutdown` on a usr-merged host really can differ from the granted
        path. That makes "do not resolve it" the load-bearing decision rather than a
        stylistic one.
        """
        directories = trusted_directories()

        assert directories.index("/usr/bin") < directories.index("/usr/sbin")
        assert entrypoint_mod.LINUX_SHUTDOWN_PATH.startswith("/usr/sbin/")

    @pytest.mark.skipif(sys.platform != "darwin", reason="macOS layout")
    def test_macos_shutdown_path_exists_on_this_host(self) -> None:
        """macOS has no /usr/sbin/shutdown, so the two platforms need different
        constants. Positive control that the macOS one is right."""
        assert os.path.isfile(entrypoint_mod.MACOS_SHUTDOWN_PATH)
