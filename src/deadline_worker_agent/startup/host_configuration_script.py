# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import time
from datetime import timedelta
from logging import Logger
from pathlib import Path
from threading import Event, Thread
from collections.abc import Mapping
from openjd.model import SymbolTable
from typing import Any, MutableMapping, Optional
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
from ..log_messages import (
    WorkerHostConfigurationLogEvent,
    WorkerHostConfigurationOutputLogEvent,
    WorkerHostConfigurationStatus,
)

if sys.platform == "win32":
    from ..windows.win_admin_runner import _WindowsScriptRunner


class _HostConfigurationOutputLogAdapter(LoggerAdapter):
    """Wraps plain-string log messages in a WorkerHostConfigurationOutputLogEvent.

    Both output paths log command output as plain strings: on POSIX, OpenJD's
    ScriptRunnerBase/LoggingSubprocess calls logger.info(line) for each line read from
    the subprocess, and on Windows _WindowsScriptRunner does the same while tailing the
    script's log file. Because that logger is the Agent's rather than
    openjd.sessions', LogRecordStringTranslationFilter does not recognise it as an
    OpenJD record and falls through to wrapping the line in an untyped StringLogEvent.

    Converting here, at the one logger both paths share, types every such line without
    reaching into OpenJD or duplicating the conversion per platform.

    Inherits OpenJD's LoggerAdapter to keep its merge-rather-than-replace handling of
    the `extra` kwarg, which callers below rely on for `worker_id`.
    """

    def __init__(
        self,
        *,
        logger: Logger,
        farm_id: str,
        fleet_id: str,
        worker_id: Optional[str],
    ) -> None:
        super().__init__(logger=logger, extra={"worker_id": worker_id})
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        # %-style arguments have to be applied before process() wraps the message.
        # LoggerAdapter.log() forwards args to the logger separately from msg, so they
        # never reach process(); the wrapped event would then carry the unformatted
        # template while the args sat unused on the record. The filter's
        # BaseLogEvent branch calls the event's own getMessage(), which does no %
        # substitution, so the record would render literally as "Running command %s"
        # and silently drop the command. OpenJD's LoggingSubprocess logs exactly that
        # way (_subprocess.py: `self._logger.info("Running command %s", cmd_line)`).
        if not self.isEnabledFor(level):
            # Checked before formatting rather than left to super().log(), since paying
            # the substitution cost for a suppressed level defeats the laziness that
            # %-style logging exists to provide.
            return
        if args and isinstance(msg, str):
            # Mirrors logging.LogRecord.__init__'s normalization of a single non-empty
            # mapping argument, which is how a "%(name)s"-style call arrives. Without
            # it, `msg % (mapping,)` raises "format requires a mapping".
            fmt_args: Any = (
                args[0] if len(args) == 1 and isinstance(args[0], Mapping) and args[0] else args
            )
            try:
                msg = msg % fmt_args
            except (TypeError, ValueError, KeyError):
                # A malformed template. Leaving the args attached would drop them
                # silently: process() below wraps any str in an event, and the filter's
                # BaseLogEvent branch neither applies args nor reports the mismatch, so
                # nothing would substitute them and nothing would complain. Append them
                # instead, so the values still reach the log.
                msg = f"{msg} {args!r}"
            args = ()
        super().log(level, msg, *args, **kwargs)

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, Any]:
        msg, kwargs = super().process(msg, kwargs)
        if isinstance(msg, str):
            msg = WorkerHostConfigurationOutputLogEvent(
                farm_id=self._farm_id,
                fleet_id=self._fleet_id,
                worker_id=self._worker_id,
                message=msg,
            )
        return msg, kwargs


class _HostConfigTimer:
    """Periodically logs elapsed and remaining time while the host configuration script runs.

    The host configuration timeout is enforced server-side — the service backend kills the worker
    when the timeout is reached. This timer provides client-side visibility into the countdown
    so operators can see progress in the logs before the worker is terminated.

    Logs every 30s normally, accelerating to every 10s when ≤60s remain.
    """

    _NORMAL_INTERVAL_S: int = 30
    _ACCELERATED_INTERVAL_S: int = 10
    _ACCELERATE_THRESHOLD_S: int = 60

    def __init__(
        self,
        *,
        timeout_seconds: int,
        logger: Logger,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._logger = logger
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._stop_event = Event()
        self._thread: Optional[Thread] = None

    def start(self) -> None:
        self._thread = Thread(target=self._run, daemon=True, name="host-config-timer")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        start_time = time.monotonic()

        while not self._stop_event.is_set():
            elapsed = time.monotonic() - start_time
            remaining = max(0, self._timeout_seconds - elapsed)

            interval = (
                self._ACCELERATED_INTERVAL_S
                if remaining <= self._ACCELERATE_THRESHOLD_S
                else self._NORMAL_INTERVAL_S
            )

            self._stop_event.wait(timeout=interval)
            if self._stop_event.is_set():
                break

            elapsed = time.monotonic() - start_time
            remaining = max(0, self._timeout_seconds - elapsed)

            self._logger.info(
                WorkerHostConfigurationLogEvent(
                    farm_id=self._farm_id,
                    fleet_id=self._fleet_id,
                    worker_id=self._worker_id,
                    message=(
                        f"Host Config Time — Elapsed: {int(elapsed)}s, Remaining: {int(remaining)}s"
                    ),
                    status=WorkerHostConfigurationStatus.RUNNING,
                )
            )

            if remaining <= 0:
                self._logger.warning(
                    WorkerHostConfigurationLogEvent(
                        farm_id=self._farm_id,
                        fleet_id=self._fleet_id,
                        worker_id=self._worker_id,
                        message=(
                            f"Host config timeout expired ({self._timeout_seconds}s). "
                            f"Worker may be terminated by the service."
                        ),
                        status=WorkerHostConfigurationStatus.FAILED,
                    )
                )
                break


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
        self._logger_adapter = _HostConfigurationOutputLogAdapter(
            logger=logger,
            farm_id=configuration.farm_id,
            fleet_id=configuration.fleet_id,
            worker_id=self._worker_id,
        )
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

        timer = _HostConfigTimer(
            timeout_seconds=self._host_configuration_timeout_seconds,
            logger=self._log,
            farm_id=self._configuration.farm_id,
            fleet_id=self._configuration.fleet_id,
            worker_id=self._worker_id,
        )
        timer.start()
        try:
            with FileContext(script_file_path) as _:
                if sys.platform == "win32":
                    exit_code = self._run_win32(script_file_path)
                else:
                    exit_code = self._run_posix()
        finally:
            timer.stop()

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
                # The adapter rather than the raw logger, so the lines it tails out of
                # the script's log file are typed the same way the POSIX path's are.
                logger=self._logger_adapter,
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
