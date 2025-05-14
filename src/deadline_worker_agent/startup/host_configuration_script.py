# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import timedelta
from logging import Logger
from pathlib import Path
from threading import Event
from openjd.model import SymbolTable
from typing import Optional
import sys

from deadline_worker_agent.utils import FileContext


from ..config.config import Configuration
from openjd.sessions._runner_base import ScriptRunnerBase, TerminateCancelMethod
from openjd.sessions._embedded_files import EmbeddedFilesScope
from openjd.sessions._session_user import PosixSessionUser
from openjd.model.v2023_09 import (
    EmbeddedFileText as EmbeddedFileText_2023_09,
)
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
)
from openjd.model.v2023_09 import DataString as DataString_2023_09
from ..aws_credentials.worker_boto3_session import WorkerBoto3Session
from openjd.sessions._types import ActionState
from openjd.sessions._logging import LoggerAdapter
from ..log_messages import WorkerHostConfigurationLogEvent, WorkerHostConfigurationStatus

if sys.platform == "win32":
    from ..windows.win_admin_runner import _WindowsScriptRunner


class HostConfigurationScriptRunner(ScriptRunnerBase):
    """Host Configuration Script Runner. Borrows from OpenJD Script Runner Base, similar to Session Actions"""

    _host_configuration_script: str
    """
    The host configuiration script to run once the worker agent reaches STARTED state.
    """

    _host_configuration_timeout_seconds: int
    """
    The amount of time to allow the host configuration script to run before timing out.
    """

    _session_directory: Path
    """The location in the filesystem where embedded files will be materialized.
    """

    _worker_boto3_session: WorkerBoto3Session
    """Boto3 Fleet Session credentials."""

    _configuration: Configuration
    """The configuration for the worker agent."""

    def __init__(
        self,
        logger: Logger,
        configuration: Configuration,
        worker_id: str,
        session_directory: Path,
        worker_boto3_session: WorkerBoto3Session,
        host_configuration_script: str,
        host_configuration_timeout_seconds: int = 300,
        runas_user=PosixSessionUser(user="root") if sys.platform != "win32" else None,
    ) -> None:
        self._configuration = configuration
        self._worker_id = worker_id
        self._worker_boto3_session = worker_boto3_session
        self._host_configuration_script = host_configuration_script
        self._host_configuration_timeout_seconds = host_configuration_timeout_seconds
        self._log = logger
        self._logger_adapter = LoggerAdapter(logger=logger, extra={"worker_id": self._worker_id})
        self._session_files_directory = session_directory
        # Internal flag to turn off Windows RunAs during unit testing.
        self._windows_run_as_admin = True
        self._runas_user = runas_user

        # Async processing event.
        self._action_event = Event()
        self._action_state: Optional[ActionState] = None

        # Super init is after setting members for computing env vars.
        super().__init__(
            logger=self._logger_adapter,
            user=self._runas_user,
            os_env_vars=self._host_configuration_env_vars(),
            session_working_directory=session_directory,
            startup_directory=session_directory,
            callback=self._action_callback,
        )
        self._print_section_banner = False

    def _script_file_name(self) -> str:
        return "host_configuration.ps1" if sys.platform == "win32" else "host_configuration.sh"

    def _write_script_file(self) -> str:
        """Returns the full path with file name after writing the script to disk."""
        # Materialize the input script to the session directory.
        script_file_name = self._script_file_name()
        host_config_script = EmbeddedFileText_2023_09(
            name="WorkerHostConfigurationScript",
            type=EmbeddedFileTypes_2023_09.TEXT,
            filename=script_file_name,
            data=DataString_2023_09(self._host_configuration_script),
            runnable=True,  # chmod +x
        )
        self._materialize_files(
            scope=EmbeddedFilesScope.ENV,  # env files are runnable.
            files=[host_config_script],
            dest_directory=self._session_files_directory,
            symtab=SymbolTable(),
        )

        script_file_path = str(self._session_files_directory / script_file_name)
        return script_file_path

    def run(self) -> int:
        """
        Run the host configuration script
        returns The exit code 0 for success, number otherwise.
        """
        if self._host_configuration_script is None:
            self._log.info(
                WorkerHostConfigurationLogEvent(
                    farm_id=self._configuration.farm_id,
                    fleet_id=self._configuration.fleet_id,
                    worker_id=self._worker_id,
                    message="No host configuration script provided.",
                    status=WorkerHostConfigurationStatus.SKIPPED,
                )
            )
            return 0

        script_file_path = self._write_script_file()

        self._log_section_banner(
            logger=self._logger_adapter, section_title="Running Host Configuration Script"
        )

        with FileContext(script_file_path) as _:
            if sys.platform == "win32":
                exit_code = self._run_win32(script_file_path)
            else:
                exit_code = self._run_posix()

        self._log_section_banner(
            logger=self._logger_adapter,
            section_title=f"Finished running Host Configuration Script, exit code: {exit_code}",
        )
        return exit_code

    def _run_posix(self) -> int:
        """
        Run the host configuration script on posix.
        returns the exit code.
        """
        if sys.platform != "win32":
            # Now that we have a script, run it.
            command = ["./host_configuration.sh"]

            self._action_event.clear()
            self._run(command)

            # Wait for the completion event.
            # Async callback prints out a message based on run state.
            self._action_event.wait()

            if self._action_state is ActionState.SUCCESS and self.exit_code == 0:
                return self.exit_code
            else:
                return self.exit_code if self.exit_code is not None else -1

        assert False, "This method should never be run in Win32"

    def _run_win32(self, script_file_path: str) -> int:
        """
        Run the host configuration script on Windows.
        returns the exit code.
        """
        if sys.platform == "win32":
            win32_runner = _WindowsScriptRunner(
                script_path=script_file_path,
                working_directory=self._session_files_directory,
                logger=self._log,
            )
            exit_code = win32_runner.run_powershell(self._host_configuration_env_vars())
            return exit_code

        assert False, "This method should never be run outside of Win32."

    def _host_configuration_env_vars(self) -> Optional[dict[str, Optional[str]]]:
        credentials = self._worker_boto3_session.get_credentials()
        env = {
            "DEADLINE_FARM_ID": self._configuration.farm_id,
            "DEADLINE_FLEET_ID": self._configuration.fleet_id,
            "DEADLINE_WORKER_ID": self._worker_id,
            "HOST_CONFIG_TIMEOUT_SECONDS": str(self._host_configuration_timeout_seconds),
            "AWS_ACCESS_KEY_ID": credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
            "AWS_SESSION_TOKEN": credentials.token,
        }
        return env

    def _action_callback(self, state: ActionState) -> None:
        """This method is inherited from the base class and only used for posix"""
        self._action_state = state

        if state in ActionState.RUNNING:
            return

        # Unblock to exit.
        self._action_event.set()

    def cancel(
        self, *, time_limit: Optional[timedelta] = None, mark_action_failed: bool = False
    ) -> None:
        """This method is inherited from the base class and only used for posix."""
        # Action cancellation. In this case, we terminate the child.
        self._cancel(TerminateCancelMethod(), time_limit, mark_action_failed)

    def _log_section_banner(self, logger: LoggerAdapter, section_title: str) -> None:
        logger.info("")
        logger.info(
            "============================================================================================"
        )
        logger.info(f"--------- {section_title} ---------")
        logger.info(
            "============================================================================================"
        )
