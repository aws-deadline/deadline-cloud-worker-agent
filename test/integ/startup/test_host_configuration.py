# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Host Configuration Script Runner"""

import os
import sys
from enum import Enum
from logging import INFO, Logger, getLogger
from botocore.credentials import Credentials
from pathlib import Path
import random
import string
from typing import Any, List
import pytest

from typing import Generator
from unittest.mock import MagicMock, patch


from logging.handlers import QueueHandler
from queue import Empty, SimpleQueue

from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments
from deadline_worker_agent.config.config import Configuration
from deadline_worker_agent.startup.host_configuration_script import HostConfigurationScriptRunner
from deadline_worker_agent.log_messages import (
    LogRecordStringTranslationFilter,
    WorkerHostConfigurationLogEvent,
)


@pytest.fixture(scope="function")
def message_queue() -> SimpleQueue:
    return SimpleQueue()


@pytest.fixture(scope="function")
def queue_handler(message_queue: SimpleQueue) -> QueueHandler:
    return QueueHandler(message_queue)


@pytest.fixture(scope="function", autouse=True)
def config_file_mock() -> Generator[MagicMock, None, None]:
    """Fixture that mocks deadline_worker_agent.config.config_file.ConfigFile.load() to raise a
    FileNotFound error.

    This can be used to avoid tests being impacted by the contents of a worker agent config file
    present in the development environment.
    """
    with patch(
        "deadline_worker_agent.config.config_file.ConfigFile.load",
        side_effect=FileNotFoundError(),
    ) as mock_config_file_load:
        yield mock_config_file_load


def _config(
    tmp_dir: Path,
) -> Configuration:
    cli_args = ParsedCommandLineArguments()
    cli_args.farm_id = "farm-00000000000000000000000000000000"
    cli_args.fleet_id = "fleet-00000000000000000000000000000000"
    cli_args.run_jobs_as_agent_user = False
    cli_args.posix_job_user = "some-user:some-group"
    cli_args.no_shutdown = True
    cli_args.profile = None
    cli_args.verbose = None
    cli_args.disallow_instance_profile = False
    # Direct the logs and persistence state into a temporary directory
    cli_args.logs_dir = tmp_dir / "temp-logs-dir"
    cli_args.persistence_dir = tmp_dir / "temp-persist-dir"
    config = Configuration(parsed_cli_args=cli_args)

    # These directories need to exist
    cli_args.logs_dir.mkdir()
    cli_args.persistence_dir.mkdir()

    return config


def build_logger(handler: QueueHandler) -> Logger:
    charset = string.ascii_letters + string.digits + string.punctuation
    name_suffix = "".join(random.choices(charset, k=32))
    log = getLogger(".".join((__name__, name_suffix)))
    log.setLevel(INFO)
    log.addHandler(handler)

    string_translation_filter = LogRecordStringTranslationFilter()
    log.addFilter(string_translation_filter)
    return log


def collect_queue_messages(queue: SimpleQueue) -> list[str]:
    """Extract the text of messages from a SimpleQueue containing LogRecords"""
    messages: list[Any] = []
    try:
        while True:
            messages.append(queue.get_nowait().getMessage())
    except Empty:
        pass

    str_messages = [message for message in messages if isinstance(message, str)]
    structure_messages = [
        structured_message.msg
        for structured_message in messages
        if isinstance(structured_message, WorkerHostConfigurationLogEvent)
    ]
    all_messages = str_messages + structure_messages
    # On linux, ' is escaped, this is just a way to make the logs consistent with OSX.
    return [msg.replace("'", "") for msg in all_messages]


class TestOS(Enum):
    LINUX = 1
    WIN32 = 2
    ALL = 3


class TestHostConfigurationScriptRunner:
    """Tests for the Host Configuration Script Runner"""

    def _create_host_configuration_script_runner(
        self,
        script: str,
        timeout: int,
        tmp: Path,
        queue_handler: QueueHandler,
    ) -> HostConfigurationScriptRunner:
        """Create a Host Configuration Script Runner"""

        configuration = _config(tmp)

        mock_boto = MagicMock()
        mock_boto.get_credentials.return_value = Credentials(
            access_key="access_key_id",
            secret_key="secret_access_key",
            token="session_token",
        )

        runner = HostConfigurationScriptRunner(
            logger=build_logger(queue_handler),
            configuration=configuration,
            worker_id="worker-00000000000000000000000000000000",
            session_directory=tmp,
            worker_boto3_session=mock_boto,
            host_configuration_script=script,
            host_configuration_timeout_seconds=timeout,
            runas_user=None,  # We cannot use root for tests.
        )
        return runner

    @pytest.mark.parametrize(
        ("script", "timeout", "success", "expected_logs", "test_os"),
        (
            pytest.param(
                None,
                300,
                True,
                [],
                TestOS.ALL,
                id="None Config Script. Should be no-op",
            ),
            pytest.param(
                "echo Hello\nexit 0",
                300,
                True,
                ["Hello"],
                TestOS.LINUX,
                id="Hello successful test case",
            ),
            pytest.param(
                "echo Sleep\nsleep 2\nexit 0",
                5,
                True,
                ["Sleep"],
                TestOS.LINUX,
                id="Timed out test case",
            ),
            pytest.param(
                "echo Error\nexit 1",
                300,
                False,
                ["Error"],
                TestOS.LINUX,
                id="Script exit non-zero test case",
            ),
            pytest.param(
                "set\nexit 0",
                300,
                True,
                [
                    "DEADLINE_FARM_ID=farm-00000000000000000000000000000000",
                    "DEADLINE_FLEET_ID=fleet-00000000000000000000000000000000",
                    "DEADLINE_WORKER_ID=worker-00000000000000000000000000000000",
                    "HOST_CONFIG_TIMEOUT_SECONDS=300",
                    "AWS_ACCESS_KEY_ID=access_key_id",
                    "AWS_SECRET_ACCESS_KEY=secret_access_key",
                    "AWS_SESSION_TOKEN=session_token",
                ],
                TestOS.LINUX,
                id="Environment Variables are set test case",
            ),
            pytest.param(
                """echo Hello
                exit 0""",
                300,
                True,
                ["Hello"],
                TestOS.WIN32,
                id="Hello successful windows test case",
            ),
            pytest.param(
                "echo Sleep\nsleep 2\necho DoneSleeping\nexit 0",
                5,
                True,
                ["Sleep", "DoneSleeping"],
                TestOS.WIN32,
                id="Timed out windows test case",
            ),
            pytest.param(
                "echo Error\nexit 1",
                300,
                False,
                ["Error"],
                TestOS.WIN32,
                id="Script exit non-zero windows test case",
            ),
            pytest.param(
                r"""
ls env:
Get-ChildItem env: | ForEach-Object { "$($_.Name)=$($_.Value)" }
                """,
                300,
                True,
                [
                    "DEADLINE_FARM_ID=farm-00000000000000000000000000000000",
                    "DEADLINE_FLEET_ID=fleet-00000000000000000000000000000000",
                    "DEADLINE_WORKER_ID=worker-00000000000000000000000000000000",
                    "HOST_CONFIG_TIMEOUT_SECONDS=300",
                    "AWS_ACCESS_KEY_ID=access_key_id",
                    "AWS_SECRET_ACCESS_KEY=secret_access_key",
                    "AWS_SESSION_TOKEN=session_token",
                ],
                TestOS.WIN32,
                id="Environment Variables are set test case",
            ),
        ),
    )
    def test_host_configuration_script_runner(
        self,
        script: str,
        timeout: int,
        success: bool,
        expected_logs: List[str],
        test_os: TestOS,
        message_queue: SimpleQueue,
        queue_handler: QueueHandler,
        tmp_path: Path,
    ):
        """Test the Host Configuration Script Runner basic happy path"""

        if sys.platform == "win32" and test_os == TestOS.LINUX:
            # Skip when a linux test case runs on windows.
            return
        elif sys.platform != "win32" and test_os == TestOS.WIN32:
            # Skip when a windows test case runs on linux.
            return

        # Given
        runner = self._create_host_configuration_script_runner(
            script=script,
            timeout=timeout,
            tmp=tmp_path,
            queue_handler=queue_handler,
        )

        # When
        exit_code = runner.run()
        run_success = exit_code == 0

        # Then
        assert success == run_success
        messages = collect_queue_messages(message_queue)

        for log in expected_logs:
            assert any(log in m for m in messages)

    def test_script_and_log_file_access(self, queue_handler: QueueHandler, tmp_path: Path):
        """
        Tests the script file is only accessible by worker agent user.
        On Windows, also check the log file is only accessible by worker agent user.
        """

        # Given
        runner = self._create_host_configuration_script_runner(
            script="echo Hello",
            timeout=300,
            tmp=tmp_path,
            queue_handler=queue_handler,
        )

        # When
        script_file = runner._write_script_file()

        # Then
        if sys.platform != "win32":
            assert os.path.exists(script_file)

            # Verify no read/write/execute access by other users.
            mode = os.stat(script_file).st_mode
            assert not mode & 0o004
            assert not mode & 0o002
            assert not mode & 0o001
        else:
            from deadline_worker_agent.windows.win_admin_runner import _WindowsScriptRunner

            # Need to prepare the file permissions on Windows
            win32_runner = _WindowsScriptRunner(
                script_path=script_file,
                working_directory=runner._session_files_directory,
                logger=runner._log,
            )
            win32_runner._prepare_file_permissions()

            # Test the script file
            _windows_file_permissions_test(script_file)
            # Test the log file
            _windows_file_permissions_test(win32_runner._logfile)


def _windows_file_permissions_test(file_path: str) -> None:
    assert sys.platform == "win32"
    import getpass
    import win32con
    import win32security
    import ntsecuritycon

    # Get relevant SIDs
    current_user_sid, _, _ = win32security.LookupAccountName(None, getpass.getuser())
    administrators_sid, _, _ = win32security.LookupAccountName(None, "Administrators")
    users_group_sid, _, _ = win32security.LookupAccountName(None, "Users")

    # Get security descriptor
    sd = win32security.GetFileSecurity(
        str(file_path),
        win32con.DACL_SECURITY_INFORMATION | win32con.OWNER_SECURITY_INFORMATION,
    )
    dacl = sd.GetSecurityDescriptorDacl()

    if dacl is None:
        assert False, "No DACL found - all users have access to the file."

    # Track permissions for allowed entities
    current_user_permissions = 0
    admin_permissions = 0
    other_sids_found = []

    # Explicit check that Users group has no permissions at all
    for i in range(dacl.GetAceCount()):
        ace = dacl.GetAce(i)
        (ace_type, ace_flags), ace_mask, sid = ace

        assert ace_type == ntsecuritycon.ACCESS_ALLOWED_ACE_TYPE, (
            f"Unexpected ace type found for sid {sid}"
        )

        if sid == users_group_sid:
            assert False, (
                f"Users group should not have any permissions, but found ACE with mask: {ace_mask}"
            )

        if sid == current_user_sid:
            current_user_permissions |= ace_mask
        elif sid == administrators_sid:
            admin_permissions |= ace_mask
        else:  # We already checked Users group
            # Keep track of any other SIDs that have access
            other_sids_found.append((win32security.LookupAccountSid(None, sid)[0], ace_mask))

    # Check that no other SIDs have access
    assert not other_sids_found, f"Found unexpected SIDs with access: {other_sids_found}"

    # Define required permissions for current user and admin.
    # This is the scoped down set from checking the underlying application.
    required_permissions = (
        ntsecuritycon.FILE_READ_DATA  # 0x1
        | ntsecuritycon.FILE_WRITE_DATA  # 0x2
        | ntsecuritycon.FILE_APPEND_DATA  # 0x4
        | ntsecuritycon.FILE_READ_EA  # 0x8
        | ntsecuritycon.FILE_WRITE_EA  # 0x10
        | ntsecuritycon.FILE_EXECUTE  # 0x20
        | ntsecuritycon.FILE_DELETE_CHILD  # 0x40
        | ntsecuritycon.FILE_READ_ATTRIBUTES  # 0x80
        | ntsecuritycon.FILE_WRITE_ATTRIBUTES  # 0x100
        | ntsecuritycon.DELETE  # 0x10000
        | ntsecuritycon.READ_CONTROL  # 0x20000
        | ntsecuritycon.WRITE_DAC  # 0x40000
        | ntsecuritycon.WRITE_OWNER  # 0x80000
        | ntsecuritycon.SYNCHRONIZE  # 0x100000
    )

    # Check current user permissions
    assert current_user_permissions == required_permissions, (
        f"Current user does not have correct permissions. Has: {current_user_permissions}, "
        f"Needs: {required_permissions}"
    )

    # Check Administrator group has required permissions.
    assert admin_permissions == required_permissions, (
        f"Administrator does not have correct permissions. Has: {current_user_permissions}, "
        f"Needs: {required_permissions}"
    )

    # Check that inheritance is disabled
    control = sd.GetSecurityDescriptorControl()
    assert control[0] & win32security.SE_DACL_PROTECTED, "DACL should be protected from inheritance"
