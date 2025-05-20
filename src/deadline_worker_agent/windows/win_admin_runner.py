# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"
from logging import Logger
from pathlib import Path, PureWindowsPath
from typing import Optional
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


class _WindowsScriptRunner:
    def __init__(
        self,
        working_directory: Path,
        script_path: str,
        logger: Logger,
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

    def _run(self, executable: str, command: str) -> int:
        """Run the executable with command. Tails the prescribed log file until process exit.
        Returns the process exit code.
        """
        with FileContext(file_path=self._logfile, delete_existing=True) as _:
            self._prepare_file_permissions()

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
                with open(self._logfile, "r", encoding="utf-8-sig") as f:
                    while (
                        win32event.WaitForSingleObject(process_handle, 100) == win32con.WAIT_TIMEOUT
                    ):
                        line = f.readline()
                        if line:
                            self._logger.info(line.rstrip("\r\n"))
                        else:
                            time.sleep(0.1)  # Small delay to reduce CPU usage

                    # Read any remaining output at the end.
                    for line in f:
                        self._logger.info(line.rstrip("\r\n"))
            except Exception as e:
                self._logger.info(f"Powershell execute error with {e}")
                with open(self._logfile, "r", encoding="utf-8-sig") as f:
                    # Read any remaining output at the end.
                    for line in f:
                        self._logger.info(line.rstrip("\r\n"))

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
