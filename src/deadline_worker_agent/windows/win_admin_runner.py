# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"
from logging import Logger, LoggerAdapter
from pathlib import Path, PureWindowsPath
from typing import IO, Optional, Union
import subprocess
import win32com.shell.shell as shell
import win32con
from win32com.shell import shellcon
import win32event
import win32process
import os
import time

from deadline_worker_agent.file_system_operations import FileSystemPermissionEnum, touch_file
from deadline_worker_agent.utils import FileContext


_MAX_PENDING_BYTES = 64 * 1024
"""Cap on a buffered partial line before it is released as its own log event.

`pending` exists to keep a line whole across polls, but the content is arbitrary script
output: a `Write-Host -NoNewline` loop, or one line carrying a large base64 blob, would
otherwise be retained in full until a newline arrives. Splitting a pathologically long
line is the lesser evil against unbounded growth, and the limit is far above any
plausible real line.
"""


class _WindowsScriptRunner:
    def __init__(
        self,
        working_directory: Path,
        script_path: str,
        # A LoggerAdapter is accepted so the caller can hand over one that types the
        # tailed output lines; see _HostConfigurationOutputLogAdapter.
        logger: Union[Logger, LoggerAdapter],
    ):
        """
        working_directory: Process working directory.
        script_path: Full path where to run the script.
        environment_variables: Environment variables to set for the process.
        admin: Run the process as admin.
        """
        self._working_directory = working_directory
        self._script_path = script_path
        self._logfile = os.path.join(os.path.dirname(script_path), "host_configuration.log")
        self._logger = logger

    def _emit_line(self, pending: bytearray) -> None:
        """Decode and log the buffered bytes as one line, then clear the buffer.

        Decoded per complete line rather than incrementally by a TextIOWrapper. The file
        is read while it is still being appended to, so a text-mode read that lands
        mid-character finalizes its decoder at end-of-file and replaces the held bytes:
        a two-byte character flushed across a poll boundary arrived as two replacement
        characters. Decoding a whole line at once means errors="replace" only fires on
        bytes that are genuinely invalid rather than on bytes that have not arrived yet.

        utf-8-sig strips the BOM that PowerShell's Out-File writes. It only strips one at
        the start of its input, so it is a no-op for every line after the first.
        """
        line = bytes(pending).decode("utf-8-sig", errors="replace").rstrip("\r\n")
        pending.clear()
        self._logger.info(line)

    def _emit_available_lines(self, f: IO[bytes], pending: bytearray) -> bool:
        """Emit every complete line currently available from `f`, and return whether any
        were emitted.

        Drains in a loop rather than taking one line per caller iteration: the caller
        polls the process handle with a 100ms timeout, so reading a single line per poll
        capped output at roughly ten lines per second no matter how much was already
        buffered. A script that logs verbosely, which is the usual reason to configure a
        host configuration script at all, then lagged far behind its own execution.

        `pending` carries a trailing partial line between calls. The file is being
        appended to concurrently, so a read can return a fragment whose newline has not
        been written yet. Emitting that fragment would split a line across two log
        events; holding it until the newline arrives keeps each event whole, up to
        _MAX_PENDING_BYTES.
        """
        emitted = False
        while True:
            chunk = f.readline()
            if not chunk:
                return emitted
            pending += chunk
            if chunk.endswith(b"\n") or len(pending) > _MAX_PENDING_BYTES:
                self._emit_line(pending)
                emitted = True

    def _flush_pending(self, pending: bytearray) -> None:
        """Emit a trailing partial line, if the script's final line had no newline."""
        if pending:
            line = bytes(pending).decode("utf-8-sig", errors="replace").rstrip("\r\n")
            pending.clear()
            if line:
                self._logger.info(line)

    def _run(self, executable: str, command: str) -> int:
        """Run the executable with command. Tails the prescribed log file until process exit.
        Returns the process exit code.
        """
        with FileContext(file_path=self._logfile, delete_existing=True) as _:
            self._prepare_file_permissions()

            # Bound before the try: if ShellExecuteEx raises, the handle is never
            # assigned, and the GetExitCodeProcess call below would then fail with an
            # UnboundLocalError that replaces the error actually worth reporting.
            process_handle = None
            pending = bytearray()

            # Run the command, allow the window to show.
            # https://learn.microsoft.com/en-us/windows/win32/api/shellapi/ns-shellapi-shellexecuteinfoa
            try:
                result = shell.ShellExecuteEx(
                    nShow=win32con.SW_SHOW,
                    fMask=shellcon.SEE_MASK_NOCLOSEPROCESS,
                    lpVerb="runas",
                    lpFile=executable,
                    lpParameters=command,
                    lpDirectory=str(self._working_directory.resolve()),
                )
                process_handle = result["hProcess"]

                # Tail the output while the process is running
                with open(self._logfile, "rb") as f:
                    while (
                        win32event.WaitForSingleObject(process_handle, 100) == win32con.WAIT_TIMEOUT
                    ):
                        if not self._emit_available_lines(f, pending):
                            time.sleep(0.1)  # Small delay to reduce CPU usage

                    # Read any remaining output at the end.
                    self._emit_available_lines(f, pending)
                    self._flush_pending(pending)
            except Exception as e:
                self._logger.info(f"Powershell execute error with {e}")
                # Dropped rather than carried over: this re-opens the log file from the
                # beginning, so a fragment left in `pending` by a failure mid-tail would
                # be prepended to the first line read here, splicing unrelated text onto
                # it. The fragment is re-read from the file anyway.
                pending.clear()
                try:
                    with open(self._logfile, "rb") as f:
                        # Read any remaining output at the end.
                        self._emit_available_lines(f, pending)
                        self._flush_pending(pending)
                except Exception as read_error:
                    # Deliberately broad. The log file may not exist at all, since
                    # ShellExecuteEx can fail before PowerShell ever writes to it. The
                    # salvage read must never be the thing that raises out of _run:
                    # that would hand the caller an exception instead of a non-zero exit
                    # code and bury the original failure logged just above.
                    self._logger.info(f"Could not read host configuration log: {read_error}")

            if process_handle is None:
                # The process never started, so there is no exit code to read. Report a
                # non-zero code so the caller fails host configuration; the cause was
                # logged above.
                self._logger.info("Powershell did not start; reporting exit code 1")
                return 1

            # Get the exit code
            return_code = win32process.GetExitCodeProcess(process_handle)
            self._logger.info(f"Powershell exited with {return_code}")
            return return_code

    def run_powershell(self, env_vars: Optional[dict[str, Optional[str]]]) -> int:
        """Run powershell with the specified script file
        Args:
            env_vars: Dictionary of environment variables where values can be None
        Returns:
            The process exit code.
        """
        executable = "powershell.exe"
        wrapper_script = PureWindowsPath(__file__).parent / "scripts" / "admin_script.ps1"

        # Convert environment variables to list of strings
        env_var_list = []
        if env_vars:
            for key, value in env_vars.items():
                if value is None:
                    env_var_list.append(f"{key}=NULL")
                else:
                    env_var_list.append(f"{key}={value}")

        ps_command = [
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper_script),
            "-ScriptToRun",
            self._script_path,
            "-LogFile",
            self._logfile,
        ] + env_var_list

        command_line = subprocess.list2cmdline(ps_command)
        return self._run(executable=executable, command=command_line)

    def _prepare_file_permissions(self):
        """Ensure the script and log is executable and readable by the current user."""
        touch_file(
            file_path=Path(self._logfile),
            agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
            group="Administrators",
            group_permission=FileSystemPermissionEnum.FULL_CONTROL,
            disable_permission_inheritance=True,
        )
        touch_file(
            file_path=Path(self._script_path),
            agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
            group="Administrators",
            group_permission=FileSystemPermissionEnum.FULL_CONTROL,
            disable_permission_inheritance=True,
        )
