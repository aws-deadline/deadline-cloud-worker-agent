# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import stat
from typing import Optional, Generator
from unittest.mock import ANY, MagicMock, patch
from datetime import datetime, timezone, timedelta
from threading import Event
from pathlib import Path
import tempfile
import pytest

from deadline_worker_agent.aws.deadline import (
    DeadlineRequestInterrupted,
    DeadlineRequestWorkerOfflineError,
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestError,
)
import deadline_worker_agent.aws_credentials.queue_boto3_session as queue_boto3_session_mod
from deadline_worker_agent.aws_credentials.queue_boto3_session import QueueBoto3Session
from openjd.sessions import PosixSessionUser, WindowsSessionUser, SessionUser
from deadline_worker_agent.file_system_operations import FileSystemPermissionEnum


@pytest.fixture(autouse=True)
def file_cache_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(queue_boto3_session_mod, "JSONFileCache") as mock:
        yield mock


@pytest.fixture(autouse=True)
def temporary_credentials_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(queue_boto3_session_mod, "TemporaryCredentials") as mock:
        yield mock


@pytest.fixture(autouse=True)
def aws_config_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(queue_boto3_session_mod, "AWSConfig") as mock:
        yield mock


@pytest.fixture(autouse=True)
def aws_credentials_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(queue_boto3_session_mod, "AWSCredentials") as mock:
        yield mock


@pytest.fixture
def deadline_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def os_user() -> Optional[SessionUser]:
    if os.name == "posix":
        return PosixSessionUser(user="user", group="group")
    else:
        return WindowsSessionUser(user="user", group="group", password="fakepassword")


class TestInit:
    def test_construction(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser],
    ) -> None:
        # Just testing basic construction.
        # Make sure that the required instance methods are called and that we
        # store the init params in the instance.

        # GIVEN
        event = Event()

        with (
            patch.object(QueueBoto3Session, "_create_credentials_directory") as mock_create_dir,
            patch.object(QueueBoto3Session, "_install_credential_process") as mock_install,
            patch.object(QueueBoto3Session, "refresh_credentials") as mock_refresh,
            patch.object(QueueBoto3Session, "cleanup") as mock_cleanup,
        ):
            # WHEN
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=os_user,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )

            # THEN
            assert session._deadline_client is deadline_client
            assert session._farm_id == farm_id
            assert session._fleet_id == fleet_id
            assert session._queue_id == queue_id
            assert session._worker_id == worker_id
            assert session._os_user is os_user
            assert session._interrupt_event is event

            mock_create_dir.assert_called_once()
            mock_install.assert_called_once()
            mock_refresh.assert_called_once()
            mock_cleanup.assert_not_called()

    def test_refresh_raises(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser],
    ) -> None:
        # Make sure that we cleanup when the refresh_credentials() method raises an exception

        # GIVEN
        event = Event()

        with (
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials") as mock_refresh,
            patch.object(QueueBoto3Session, "cleanup") as mock_cleanup,
        ):
            mock_refresh.side_effect = Exception("Uh-oh")

            # THEN
            with pytest.raises(Exception) as exc_context:
                QueueBoto3Session(
                    deadline_client=deadline_client,
                    farm_id=farm_id,
                    fleet_id=fleet_id,
                    worker_id=worker_id,
                    queue_id=queue_id,
                    os_user=os_user,
                    interrupt_event=event,
                    worker_persistence_dir=Path("/var/lib/deadline"),
                )

            # THEN
            mock_refresh.assert_called_once()
            mock_cleanup.assert_called_once()
            assert exc_context.value is mock_refresh.side_effect


class TestCleanup:
    def test(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
    ) -> None:
        # Regression test to make sure that cleanup always:
        # 1. Uninstalls the credential process from the user's AWS configuration
        # 2. Deletes the credentials directory that was created.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
            # Relevant mocks for the test
            patch.object(QueueBoto3Session, "_uninstall_credential_process") as mock_uninstall,
            patch.object(QueueBoto3Session, "_delete_credentials_directory") as mock_delete_dir,
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )

            # WHEN
            session.cleanup()

            # THEN
            mock_uninstall.assert_called_once()
            mock_delete_dir.assert_called_once()


class TestHasCredentials:
    @pytest.mark.parametrize("expired", [True, False])
    def test(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        expired: bool,
    ) -> None:
        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
            # Relevant mocks for the test
            patch.object(QueueBoto3Session, "get_credentials") as mock_get_credentials,
        ):
            mock_credentials_object = MagicMock()
            mock_credentials_object.are_expired.return_value = expired
            mock_get_credentials.return_value = mock_credentials_object

            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )

            # WHEN
            result = session.has_credentials

            # THEN
            assert result == (not expired)


SAMPLE_DEADLINE_CREDENTIALS = {
    "accessKeyId": "access-key",
    "secretAccessKey": "secret-key",
    "sessionToken": "token",
    "expiration": datetime.now(timezone.utc) + timedelta(hours=1),
}
SAMPLE_ASSUME_ROLE_RESPONSE = {"credentials": SAMPLE_DEADLINE_CREDENTIALS}


class TestRefreshCredentials:
    def test_uses_bootstrap_credentials(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that if the Session contains credentials that ARE expired,
        # then it will use the given bootstrap_session credentials to do the refresh.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )
        with (
            # Relevant mocks for the test
            patch.object(
                queue_boto3_session_mod, "assume_queue_role_for_worker"
            ) as assume_role_mock,
            patch.object(QueueBoto3Session, "get_credentials") as mock_get_credentials,
        ):
            assume_role_mock.return_value = SAMPLE_ASSUME_ROLE_RESPONSE
            mock_temporary_creds = MagicMock()
            temporary_credentials_cls_mock.from_deadline_assume_role_response.return_value = (
                mock_temporary_creds
            )
            mock_credentials_object = MagicMock()
            mock_get_credentials.return_value = mock_credentials_object

            # WHEN
            session.refresh_credentials()

            # THEN
            assume_role_mock.assert_called_once_with(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                interrupt_event=event,
            )
            temporary_credentials_cls_mock.from_deadline_assume_role_response.assert_called_once_with(
                response=SAMPLE_ASSUME_ROLE_RESPONSE,
                credentials_required=False,
                api_name="AssumeQueueRoleForWorker",
            )
            mock_temporary_creds.cache.assert_called_once_with(
                cache=file_cache_cls_mock.return_value, cache_key=session._credentials_filename
            )
            mock_credentials_object.set_credentials.assert_called_once_with(
                mock_temporary_creds.to_deadline.return_value
            )

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(DeadlineRequestInterrupted(Exception("inner")), id="interrupt"),
            pytest.param(DeadlineRequestWorkerOfflineError(Exception("inner")), id="offline"),
            pytest.param(
                DeadlineRequestConditionallyRecoverableError(Exception("inner")), id="conditionally"
            ),
            pytest.param(
                DeadlineRequestUnrecoverableError(Exception("inner")), id="unconditionally"
            ),
            pytest.param(Exception("Surprise!"), id="exception"),
        ],
    )
    def test_reraises_from_assume(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        exception: Exception,
    ) -> None:
        # Test that if the assume-role raises an exception that we re-raise it..

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )
        with (
            # Relevant mocks for the test
            patch.object(
                queue_boto3_session_mod, "assume_queue_role_for_worker"
            ) as assume_role_mock,
        ):
            assume_role_mock.side_effect = exception

            # WHEN
            with pytest.raises((DeadlineRequestError, DeadlineRequestInterrupted)) as exc_context:
                session.refresh_credentials()

            # THEN
            if isinstance(exception, (DeadlineRequestError, DeadlineRequestInterrupted)):
                assert exc_context.value is exception
            else:
                # Raw exceptions are wrapped in an unrecoverable exception
                assert isinstance(exc_context.value, DeadlineRequestUnrecoverableError)
                assert exc_context.value.inner_exc is exception

    @pytest.mark.parametrize("exception", [KeyError("key"), TypeError("type"), ValueError("value")])
    def test_reraises_from_parse(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        temporary_credentials_cls_mock: MagicMock,
        exception: Exception,
    ) -> None:
        # Test that if the parsing of the API response fails then we raise an exception.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )
        with (
            # Relevant mocks for the test
            patch.object(
                queue_boto3_session_mod, "assume_queue_role_for_worker"
            ) as assume_role_mock,
            patch.object(QueueBoto3Session, "get_credentials"),
        ):
            assume_role_mock.return_value = SAMPLE_ASSUME_ROLE_RESPONSE

            temporary_credentials_cls_mock.from_deadline_assume_role_response.side_effect = (
                exception
            )

            # THEN
            with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
                session.refresh_credentials()

            assert exc_context.value.inner_exc is exception


class TestCreateCredentialsDirectory:
    def test_success(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser],
    ) -> None:
        # Test that the directory is created securely.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
        ):
            mock_path = MagicMock()
            mock_path.__truediv__.return_value = mock_path

            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=os_user,
                interrupt_event=event,
                worker_persistence_dir=mock_path,
            )

        with (
            patch.object(queue_boto3_session_mod, "make_directory") as mock_make_directory,
            patch.object(queue_boto3_session_mod.shutil, "chown") as mock_chown,
        ):
            # WHEN
            session._create_credentials_directory()

        # THEN

        if isinstance(os_user, PosixSessionUser):
            mock_path.mkdir.assert_called_once_with(
                exist_ok=True,
                parents=True,
                mode=0o750,
            )
            mock_chown.assert_called_once_with(mock_path, group=os_user.group)
        else:
            mock_make_directory.assert_called_once_with(
                dir_path=mock_path,
                exist_ok=True,
                parents=True,
                permitted_user=os_user,
                agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                group_permission=FileSystemPermissionEnum.READ,
            )

    def test_reraises_oserror(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
    ) -> None:
        # Test that a failure to create the directory is re-raised.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
        ):
            mock_path = MagicMock()
            mock_path.__truediv__.return_value = mock_path

            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=mock_path,
            )

        mock_path.mkdir.side_effect = OSError("Boom!")

        # THEN
        with pytest.raises(OSError) as exc_context:
            session._create_credentials_directory()

        assert exc_context.value is mock_path.mkdir.side_effect


class TestDeleteCredentialsDirectory:
    @pytest.mark.parametrize("exists", [True, False])
    def test_success(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        exists: bool,
    ) -> None:
        # Test that the directory is created securely.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
            # For the actual test
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=None,
                interrupt_event=event,
                worker_persistence_dir=Path(tmpdir),
            )

            if exists:
                os.makedirs(Path(tmpdir) / "queues" / queue_id, exist_ok=True)

            with patch.object(queue_boto3_session_mod.shutil, "rmtree") as mock_rmtree:
                # WHEN
                session._delete_credentials_directory()

            # THEN
            if exists:
                mock_rmtree.assert_called_once_with(Path(tmpdir) / "queues" / queue_id, onerror=ANY)
            else:
                mock_rmtree.assert_not_called()


class TestInstallCredentialProcess:
    def test_success(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser],
        aws_config_cls_mock: MagicMock,
        aws_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that the directory is created securely.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
            # For the actual test
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=os_user,
                interrupt_event=event,
                worker_persistence_dir=Path(tmpdir),
            )

        aws_config_mock = aws_config_cls_mock.return_value
        aws_credentials_mock = aws_credentials_cls_mock.return_value

        with (
            patch.object(queue_boto3_session_mod.os, "open") as mock_os_open,
            patch.object(queue_boto3_session_mod.shutil, "chown") as mock_chown,
            patch("builtins.open", spec=True) as mock_builtins_open,
            patch.object(
                QueueBoto3Session, "_generate_credential_process_script"
            ) as mock_generate_script,
            patch.object(queue_boto3_session_mod, "set_permissions") as mock_set_permissions,
        ):
            # WHEN
            session._install_credential_process()

        # THEN
        if os.name == "posix":
            credentials_process_script_path = (
                Path(tmpdir) / "queues" / queue_id / "get_aws_credentials.sh"
            )
        else:
            credentials_process_script_path = (
                Path(tmpdir) / "queues" / queue_id / "get_aws_credentials.cmd"
            )
        mock_os_open.assert_called_once_with(
            path=str(credentials_process_script_path),
            flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            mode=(stat.S_IRWXU)
            if os_user is None
            else (stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP),
        )
        mock_builtins_open.assert_called_once_with(
            mock_os_open.return_value, mode="w", encoding="utf-8"
        )
        mock_builtins_open.return_value.__enter__.assert_called_once()
        mock_builtins_open.return_value.__exit__.assert_called_once()
        mock_builtins_open.return_value.__enter__.return_value.write.assert_called_once_with(
            mock_generate_script.return_value
        )
        if os_user is None:
            mock_chown.assert_not_called()
        else:
            # This assert for type checking. Expand the if-else chain when adding new user kinds.
            if os.name == "posix":
                assert isinstance(os_user, PosixSessionUser)
                mock_chown.assert_called_once_with(
                    credentials_process_script_path, group=os_user.group
                )
            else:
                assert isinstance(os_user, WindowsSessionUser)
                mock_set_permissions.assert_called_once_with(
                    file_path=credentials_process_script_path,
                    permitted_user=os_user,
                    agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                    group_permission=FileSystemPermissionEnum.EXECUTE,
                )

        aws_config_mock.install_credential_process.assert_called_once_with(
            session._profile_name, credentials_process_script_path
        )
        aws_credentials_mock.install_credential_process.assert_called_once_with(
            session._profile_name, credentials_process_script_path
        )


class TestUninstallCredentialProcess:
    def test_success(
        self,
        deadline_client: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser],
        aws_config_cls_mock: MagicMock,
        aws_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that we call the correct methods to see the credentials process removed from
        # the user's AWS configuration.

        # GIVEN
        event = Event()
        with (
            # To get through __init__
            patch.object(QueueBoto3Session, "_create_credentials_directory"),
            patch.object(QueueBoto3Session, "_install_credential_process"),
            patch.object(QueueBoto3Session, "refresh_credentials"),
            # For the actual test
            patch.object(queue_boto3_session_mod, "Path") as mock_path_cls,
        ):
            mock_path = MagicMock()
            mock_path_cls.return_value = mock_path

            session = QueueBoto3Session(
                deadline_client=deadline_client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
                os_user=os_user,
                interrupt_event=event,
                worker_persistence_dir=Path("/var/lib/deadline"),
            )

        aws_config_mock = aws_config_cls_mock.return_value
        aws_credentials_mock = aws_credentials_cls_mock.return_value

        # WHEN
        session._uninstall_credential_process()

        # THEN
        aws_config_mock.uninstall_credential_process.assert_called_once_with(session._profile_name)
        aws_credentials_mock.uninstall_credential_process.assert_called_once_with(
            session._profile_name
        )
