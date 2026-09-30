# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for _WindowsScriptRunner's output tailing and failure handling."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# This if is required for two purposes:
# 1.  It short-circuits mypy from type checking this module on platforms other than Windows
#     https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
# 2.  It causes the tests to not be discovered/ran on non-Windows platforms
if sys.platform == "win32":
    import win32con

    from deadline_worker_agent.windows.win_admin_runner import (
        _MAX_PENDING_BYTES,
        _WindowsScriptRunner,
    )

    @pytest.fixture
    def logger() -> MagicMock:
        return MagicMock(spec=logging.Logger)

    @pytest.fixture
    def runner(tmp_path: Path, logger: MagicMock) -> _WindowsScriptRunner:
        script = tmp_path / "host_configuration.ps1"
        script.write_text("Write-Output hi\n", encoding="utf-8")
        return _WindowsScriptRunner(
            working_directory=tmp_path,
            script_path=str(script),
            logger=logger,
        )

    class TestEmitAvailableLines:
        def test_drains_every_available_line_in_one_call(
            self, runner: _WindowsScriptRunner, tmp_path: Path, logger: MagicMock
        ) -> None:
            # The whole point of the drain loop: one call must not stop after one line,
            # or throughput is capped by the caller's 100ms process-handle poll.
            log = tmp_path / "out.log"
            log.write_text("a\nb\nc\n", encoding="utf-8")

            pending = bytearray()
            with log.open("rb") as f:
                assert runner._emit_available_lines(f, pending) is True

            assert [c.args[0] for c in logger.info.call_args_list] == ["a", "b", "c"]
            assert pending == bytearray()

        def test_holds_a_trailing_partial_line(
            self, runner: _WindowsScriptRunner, tmp_path: Path, logger: MagicMock
        ) -> None:
            # The file is appended to concurrently, so a fragment whose newline has not
            # been written yet must not be emitted as its own event.
            log = tmp_path / "out.log"
            log.write_text("complete\npartial-no-newline", encoding="utf-8")

            pending = bytearray()
            with log.open("rb") as f:
                runner._emit_available_lines(f, pending)

            assert [c.args[0] for c in logger.info.call_args_list] == ["complete"]
            assert pending == b"partial-no-newline"

        def test_joins_a_partial_line_with_its_continuation(
            self, runner: _WindowsScriptRunner, tmp_path: Path, logger: MagicMock
        ) -> None:
            # The fragment held above is completed by the next read, producing one event.
            log = tmp_path / "out.log"
            log.write_text("frag", encoding="utf-8")

            pending = bytearray()
            with log.open("rb") as f:
                runner._emit_available_lines(f, pending)
                assert pending == b"frag"
                with log.open("a", encoding="utf-8") as w:
                    w.write("ment\n")
                runner._emit_available_lines(f, pending)

            assert [c.args[0] for c in logger.info.call_args_list] == ["fragment"]

        def test_reports_nothing_emitted_on_an_empty_file(
            self, runner: _WindowsScriptRunner, tmp_path: Path
        ) -> None:
            # Drives the caller's sleep, so it must be False rather than None.
            log = tmp_path / "out.log"
            log.write_text("", encoding="utf-8")

            pending = bytearray()
            with log.open("rb") as f:
                assert runner._emit_available_lines(f, pending) is False

        def test_a_runaway_line_is_released_rather_than_buffered_without_bound(
            self, runner: _WindowsScriptRunner, tmp_path: Path, logger: MagicMock
        ) -> None:
            # Holding a partial line keeps it whole, but the content is arbitrary script
            # output. Without a cap, a Write-Host -NoNewline loop or a single huge blob is
            # retained in the agent's memory in full until a newline arrives. The previous
            # one-line-per-poll code split it but kept memory flat, so the cap restores
            # that bound while still keeping normal lines intact.
            log = tmp_path / "out.log"
            log.write_bytes(b"x" * (_MAX_PENDING_BYTES + 100))

            pending = bytearray()
            with log.open("rb") as f:
                assert runner._emit_available_lines(f, pending) is True

            assert logger.info.call_count == 1, "the runaway line was not released"
            assert len(pending) <= _MAX_PENDING_BYTES, (
                f"buffer exceeded the cap: {len(pending)} bytes held"
            )

    class TestRunDecoding:
        def test_undecodable_bytes_do_not_abort_the_tail(
            self, runner: _WindowsScriptRunner, logger: MagicMock
        ) -> None:
            # Goes through _run so the production open() is the one under test.
            #
            # The log file holds verbatim script output, and console tools emitting in
            # the legacy ANSI codepage produce bytes that are not valid UTF-8. Decoded
            # strictly, readline() raises UnicodeDecodeError, which subclasses ValueError
            # rather than OSError, so it escaped the salvage handler and propagated out of
            # _run. Losing a whole run over one bad byte is worse than replacing it.
            def create_logfile(self_: _WindowsScriptRunner) -> None:
                # 0xE9 is "e-acute" in cp1252 and invalid as standalone UTF-8.
                Path(self_._logfile).write_bytes(b"caf\xe9 started\nsecond line\n")

            with (
                patch.object(
                    _WindowsScriptRunner,
                    "_prepare_file_permissions",
                    autospec=True,
                    side_effect=create_logfile,
                ),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.shell.ShellExecuteEx",
                    return_value={"hProcess": 1234},
                ),
                # One polling pass that reads, then the process is reported as exited.
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32event.WaitForSingleObject",
                    side_effect=[win32con.WAIT_TIMEOUT, 0],
                ),
                patch("deadline_worker_agent.windows.win_admin_runner.time.sleep"),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32process.GetExitCodeProcess",
                    return_value=0,
                ),
            ):
                result = runner._run(executable="powershell.exe", command="-NoProfile")

            assert result == 0
            emitted = [str(c.args[0]) for c in logger.info.call_args_list]
            assert any(line.endswith(" started") for line in emitted), (
                f"the line with the undecodable byte was lost: {emitted}"
            )
            assert "second line" in emitted, f"the tail stopped early: {emitted}"

        def test_a_multibyte_character_split_across_polls_is_not_corrupted(
            self, runner: _WindowsScriptRunner, logger: MagicMock
        ) -> None:
            # Goes through _run so the production read mode is the one under test.
            #
            # The writer can flush mid-character, so a poll can see only the first byte of
            # a two-byte character. A text-mode read finalizes its incremental decoder at
            # end-of-file and replaces the held byte, so the character arrived as two
            # replacement characters. Buffering bytes and decoding per complete line means
            # errors="replace" only fires on bytes that are genuinely invalid rather than
            # on bytes that merely have not arrived yet.
            def create_logfile(self_: _WindowsScriptRunner) -> None:
                Path(self_._logfile).write_bytes(b"caf\xc3")  # first byte of 'é'

            polls = {"n": 0}

            def poll(handle: int, timeout: int) -> int:
                # Poll 1: the loop reads the partial character.
                # Poll 2: the continuation byte has landed, so the loop reads the rest.
                # Poll 3: report the process as exited.
                polls["n"] += 1
                if polls["n"] == 2:
                    with open(runner._logfile, "ab") as w:
                        w.write(b"\xa9 done\n")
                return win32con.WAIT_TIMEOUT if polls["n"] <= 2 else 0

            with (
                patch.object(
                    _WindowsScriptRunner,
                    "_prepare_file_permissions",
                    autospec=True,
                    side_effect=create_logfile,
                ),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.shell.ShellExecuteEx",
                    return_value={"hProcess": 1234},
                ),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32event.WaitForSingleObject",
                    side_effect=poll,
                ),
                patch("deadline_worker_agent.windows.win_admin_runner.time.sleep"),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32process.GetExitCodeProcess",
                    return_value=0,
                ),
            ):
                runner._run(executable="powershell.exe", command="-NoProfile")

            emitted = [str(c.args[0]) for c in logger.info.call_args_list]
            assert "café done" in emitted, f"the split character was corrupted: {emitted}"

    class TestRunFailureHandling:
        def test_returns_nonzero_when_the_process_never_started(
            self, runner: _WindowsScriptRunner, logger: MagicMock
        ) -> None:
            # If ShellExecuteEx raises, there is no process handle to read an exit code
            # from. Reading one anyway raised UnboundLocalError, masking the real error.
            # The script's log file does not exist either, since PowerShell never ran,
            # so _run also has to tolerate the salvage read finding nothing.
            with (
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.shell.ShellExecuteEx",
                    side_effect=OSError("elevation refused"),
                ),
                patch.object(_WindowsScriptRunner, "_prepare_file_permissions"),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32process.GetExitCodeProcess"
                ) as mock_exit_code,
            ):
                result = runner._run(executable="powershell.exe", command="-NoProfile")

            assert result != 0
            mock_exit_code.assert_not_called()
            # The original failure is what the operator needs, so it must be logged.
            assert any("elevation refused" in str(c.args[0]) for c in logger.info.call_args_list)

        def test_a_failing_salvage_read_does_not_escape(
            self, runner: _WindowsScriptRunner, logger: MagicMock
        ) -> None:
            # Whatever goes wrong while salvaging output, _run must still return a code
            # rather than raise, or the original failure is buried. UnicodeDecodeError is
            # the realistic case: it subclasses ValueError, so an OSError-only guard
            # missed it.
            def create_logfile(self_: _WindowsScriptRunner) -> None:
                Path(self_._logfile).write_text("some output\n", encoding="utf-8")

            with (
                patch.object(
                    _WindowsScriptRunner,
                    "_prepare_file_permissions",
                    autospec=True,
                    side_effect=create_logfile,
                ),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.shell.ShellExecuteEx",
                    side_effect=OSError("elevation refused"),
                ),
                patch.object(
                    _WindowsScriptRunner,
                    "_emit_available_lines",
                    side_effect=UnicodeDecodeError("utf-8", b"\xe9", 0, 1, "invalid start byte"),
                ),
            ):
                result = runner._run(executable="powershell.exe", command="-NoProfile")

            assert result != 0
            emitted = [str(c.args[0]) for c in logger.info.call_args_list]
            assert any("elevation refused" in line for line in emitted), (
                f"the original failure was lost: {emitted}"
            )
            assert any("Could not read host configuration log" in line for line in emitted)

        def test_a_fragment_is_not_spliced_onto_the_salvaged_output(
            self, runner: _WindowsScriptRunner, logger: MagicMock
        ) -> None:
            # The except path re-opens the log from offset 0, so a partial line left in
            # `pending` by a failure mid-tail must not be prepended to the first line
            # read there: that would emit "Step 3 of 5 in progStep 3 of 5 in prog".
            #
            # A line with no trailing newline is exactly what the tail loop buffers,
            # and is what a script that dies mid-write leaves behind.
            fragment = "Step 3 of 5 in prog"

            def create_logfile(self_: _WindowsScriptRunner) -> None:
                # Stands in for the real _prepare_file_permissions, which creates the
                # log file via touch_file(). FileContext unlinks it on entry, so the
                # content has to be written from here rather than before _run.
                Path(self_._logfile).write_text(fragment, encoding="utf-8")

            with (
                patch.object(
                    _WindowsScriptRunner,
                    "_prepare_file_permissions",
                    autospec=True,
                    side_effect=create_logfile,
                ),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.shell.ShellExecuteEx",
                    return_value={"hProcess": 1234},
                ),
                # First poll times out so the loop reads and buffers the fragment; the
                # second raises, sending control to the salvage path.
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32event.WaitForSingleObject",
                    side_effect=[win32con.WAIT_TIMEOUT, OSError("handle went away mid-tail")],
                ),
                patch("deadline_worker_agent.windows.win_admin_runner.time.sleep"),
                patch(
                    "deadline_worker_agent.windows.win_admin_runner.win32process.GetExitCodeProcess",
                    return_value=0,
                ),
            ):
                runner._run(executable="powershell.exe", command="-NoProfile")

            emitted = [str(c.args[0]) for c in logger.info.call_args_list]
            assert fragment in emitted, f"the salvaged fragment was not logged: {emitted}"
            assert fragment * 2 not in emitted, (
                f"a stale fragment was spliced onto salvaged output: {emitted}"
            )
