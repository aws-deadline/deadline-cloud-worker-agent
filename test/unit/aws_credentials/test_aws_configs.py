# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from pathlib import Path
from typing import Type, Generator, Optional

import deadline_worker_agent.aws_credentials.aws_configs as aws_configs_mod
from deadline_worker_agent.aws_credentials.aws_configs import (
    AWSConfig,
    AWSCredentials,
    _AWSConfigBase,
    _setup_file,
)
from openjd.sessions import PosixSessionUser, WindowsSessionUser, SessionUser
from deadline_worker_agent.file_system_operations import FileSystemPermissionEnum
import os


@pytest.fixture
def profile_name() -> str:
    return "queue-1234567890abcdef"


@pytest.fixture(autouse=True)
def mock_chown() -> Generator[MagicMock, None, None]:
    with patch.object(aws_configs_mod, "chown") as mock_chown:
        yield mock_chown


@pytest.fixture(autouse=True)
def mock_touch_file() -> Generator[MagicMock, None, None]:
    with patch.object(aws_configs_mod, "touch_file") as mock_touch_file:
        yield mock_touch_file


@pytest.fixture(params=[True, False])
def os_user(request: pytest.FixtureRequest) -> Optional[SessionUser]:
    if request.param:
        if os.name == "posix":
            return PosixSessionUser(user="user", group="group")
        else:
            return WindowsSessionUser(user="user", password="fakepassword")
    else:
        return None


class TestSetupFile:
    """Tests for the _setup_file() function"""

    @pytest.fixture
    def file_path(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def exists(self) -> bool:
        return False

    @pytest.fixture(autouse=True)
    def mock_exists(self, file_path: MagicMock, exists: bool) -> None:
        file_path.exists.return_value = exists

    @pytest.mark.parametrize(
        argnames=("exists",),
        argvalues=(
            pytest.param(True, id="exists"),
            pytest.param(False, id="doesnt-exist"),
        ),
    )
    def test_creates_file_if_needed(
        self,
        file_path: MagicMock,
        os_user: Optional[SessionUser],
        exists: bool,
        mock_touch_file: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN

        # WHEN
        _setup_file(
            file_path=file_path,
            owner=os_user,
        )

        # THEN
        if os_user:
            if os.name == "posix":
                assert isinstance(os_user, PosixSessionUser)
                file_path.touch.assert_called_once_with(mode=0o640)
            else:
                assert isinstance(os_user, WindowsSessionUser)
                mock_touch_file.assert_called_once_with(
                    file_path=file_path,
                    permitted_user=os_user,
                    user_permission=FileSystemPermissionEnum.READ,
                    agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                )
        else:
            if os.name == "posix":
                file_path.touch.assert_called_once_with(mode=0o600)
                file_path.chmod.assert_called_once_with(mode=0o600)
            else:
                mock_touch_file(
                    file_path=file_path,
                    agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                )

    def test_changes_permissions(
        self,
        file_path: MagicMock,
        os_user: Optional[SessionUser],
        mock_touch_file: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN
        chmod: MagicMock = file_path.chmod

        # WHEN
        _setup_file(file_path=file_path, owner=os_user)

        # THEN
        if os_user:
            if os.name == "posix":
                assert isinstance(os_user, PosixSessionUser)
                file_path.chmod.assert_called_once_with(mode=0o640)
            else:
                assert isinstance(os_user, WindowsSessionUser)
                mock_touch_file.assert_called_once_with(
                    file_path=file_path,
                    permitted_user=os_user,
                    user_permission=FileSystemPermissionEnum.READ,
                    agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                )
        elif os.name == "posix":
            chmod.assert_called_once_with(mode=0o600)
        else:
            mock_touch_file.assert_called_once_with(
                file_path=file_path,
                agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
            )

    def test_changes_group_ownership(
        self,
        file_path: MagicMock,
        os_user: Optional[SessionUser],
        mock_touch_file: MagicMock,
        mock_chown: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN

        # WHEN
        _setup_file(
            file_path=file_path,
            owner=os_user,
        )

        # THEN
        if os_user:
            if os.name == "posix":
                assert isinstance(os_user, PosixSessionUser)
                file_path.touch.assert_called_once_with(mode=0o640)
                mock_chown.assert_called_once_with(file_path, group=os_user.group)
            else:
                assert isinstance(os_user, WindowsSessionUser)
                mock_touch_file.assert_called_once_with(
                    file_path=file_path,
                    permitted_user=os_user,
                    user_permission=FileSystemPermissionEnum.READ,
                    agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                )
        elif os.name == "posix":
            file_path.touch.assert_called_once_with(mode=0o600)
            mock_chown.assert_not_called()
        else:
            mock_touch_file.assert_called_once_with(
                file_path=file_path,
                agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
            )


class AWSConfigTestBase:
    """Base class for common testing logic of AWSConfig and AWSCredentials classes"""

    @pytest.fixture(autouse=True)
    def mock_config_parser_cls(self) -> Generator[MagicMock, None, None]:
        with patch.object(aws_configs_mod, "ConfigParser") as mock_config_parser:
            yield mock_config_parser

    @pytest.fixture
    def mock_config_parser(self, mock_config_parser_cls: MagicMock) -> MagicMock:
        return mock_config_parser_cls.return_value

    @pytest.fixture(autouse=True)
    def mock_open(self) -> Generator[MagicMock, None, None]:
        with patch("builtins.open") as mock_open:
            yield mock_open

    @pytest.fixture(autouse=True)
    def mock_expanduser(self) -> Generator[MagicMock, None, None]:
        with patch.object(aws_configs_mod.Path, "expanduser") as mock_expanduser:
            yield mock_expanduser

    @pytest.fixture(autouse=True)
    def mock_chmod(self) -> Generator[MagicMock, None, None]:
        with patch.object(aws_configs_mod.Path, "chmod") as mock_chmod:
            yield mock_chmod

    @pytest.fixture(
        params=(True, False),
        ids=("file-exists", "file-not-exists"),
    )
    def exists(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    @pytest.fixture(autouse=True)
    def mock_exists(self, mock_expanduser: MagicMock, exists: bool) -> MagicMock:
        mock_exists: MagicMock = mock_expanduser.return_value.exists

        def side_effect() -> bool:
            return exists

        mock_exists.side_effect = side_effect
        # mock_exists.return_value = exists
        return mock_exists

    @pytest.fixture
    def parent_dir(self) -> MagicMock:
        return MagicMock()

    def test_init(
        self,
        config_class: Type[_AWSConfigBase],
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
        parent_dir: MagicMock,
    ) -> None:
        # GIVEN
        if os.name == "posix":
            assert isinstance(os_user, PosixSessionUser) or os_user is None
        else:
            assert isinstance(os_user, WindowsSessionUser) or os_user is None
        config_parser_read: MagicMock = mock_config_parser.read

        with patch.object(aws_configs_mod, "_setup_file") as setup_file_mock:
            # WHEN
            config = config_class(
                os_user=os_user,
                parent_dir=parent_dir,
            )

        # THEN
        setup_file_mock.assert_called_once_with(
            file_path=config.path,
            owner=os_user,
        )
        config_parser_read.assert_called_once_with(config.path)

    def test_path(
        self,
        config_class: Type[_AWSConfigBase],
        expected_path: Path,
        os_user: Optional[SessionUser],
        parent_dir: MagicMock,
    ) -> None:
        # WHEN
        if os.name == "posix":
            assert isinstance(os_user, PosixSessionUser) or os_user is None
        else:
            assert isinstance(os_user, WindowsSessionUser) or os_user is None

        config = config_class(
            os_user=os_user,
            parent_dir=parent_dir,
        )
        result = config.path

        # THEN
        assert result == expected_path

    @patch.object(aws_configs_mod.Path, "absolute")
    def test_install_credential_process(
        self,
        mock_absolute: MagicMock,
        config_class: Type[_AWSConfigBase],
        profile_name: str,
        expected_profile_name_section: str,
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
        parent_dir: MagicMock,
    ) -> None:
        # GIVEN
        if os.name == "posix":
            assert isinstance(os_user, PosixSessionUser) or os_user is None
        else:
            assert isinstance(os_user, WindowsSessionUser) or os_user is None
        config = config_class(os_user=os_user, parent_dir=parent_dir)
        script_path = Path("/path/to/installdir/echo_them_credentials.sh")
        with patch.object(config, "_write") as write_mock:
            # WHEN
            config.install_credential_process(profile_name=profile_name, script_path=script_path)

        # THEN
        mock_config_parser.__setitem__.assert_called_once_with(
            expected_profile_name_section,
            {
                "credential_process": mock_absolute.return_value.__str__.return_value,
            },
        )
        write_mock.assert_called_once_with()

    @patch.object(aws_configs_mod.Path, "absolute")
    def test_uninstall_credential_process(
        self,
        mock_absolute: MagicMock,
        config_class: Type[_AWSConfigBase],
        profile_name: str,
        expected_profile_name_section: str,
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
        parent_dir: MagicMock,
    ) -> None:
        # GIVEN
        if os.name == "posix":
            assert isinstance(os_user, PosixSessionUser) or os_user is None
        else:
            assert isinstance(os_user, WindowsSessionUser) or os_user is None
        config = config_class(
            os_user=os_user,
            parent_dir=parent_dir,
        )
        script_path = Path("/path/to/installdir/echo_them_credentials.sh")
        with patch.object(config, "_write") as write_mock:
            config.install_credential_process(profile_name=profile_name, script_path=script_path)
            mock_config_parser.__setitem__.assert_called_once_with(
                expected_profile_name_section,
                {
                    "credential_process": mock_absolute.return_value.__str__.return_value,
                },
            )
            write_mock.assert_called_once_with()
            write_mock.reset_mock()
            mock_config_parser.__contains__.return_value = True

            # WHEN
            config.uninstall_credential_process(profile_name=profile_name)

        # THEN
        mock_config_parser.__delitem__.assert_any_call(expected_profile_name_section)
        write_mock.assert_called_once_with()

    def test_write(
        self,
        config_class: Type[_AWSConfigBase],
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
        parent_dir: MagicMock,
    ) -> None:
        # GIVEN
        if os.name == "posix":
            assert isinstance(os_user, PosixSessionUser) or os_user is None
        else:
            assert isinstance(os_user, WindowsSessionUser) or os_user is None
        with (
            patch.object(aws_configs_mod, "_logger") as logger_mock,
            patch.object(config_class, "path", new_callable=PropertyMock) as path_prop_mock,
        ):
            path: MagicMock = path_prop_mock.return_value
            config = config_class(
                os_user=os_user,
                parent_dir=parent_dir,
            )
            info_mock: MagicMock = logger_mock.info

            # WHEN
            config._write()

        # THEN
        info_mock.assert_called_once_with(f"Writing updated {path} to disk.")
        path.open.assert_called_once_with(mode="w")
        mock_config_parser.write.assert_called_once_with(
            fp=path.open.return_value.__enter__.return_value,
            space_around_delimiters=False,
        )


class TestAWSConfig(AWSConfigTestBase):
    """
    Test class derived from AWSConfigTestBase for AWSConfig.

    All tests are defined in the base class. This class defines the fixtures that feed into those tests.
    """

    @pytest.fixture
    def config_class(self) -> Type[_AWSConfigBase]:
        return AWSConfig

    @pytest.fixture
    def expected_profile_name_section(self, profile_name: str) -> str:
        return f"profile {profile_name}"

    @pytest.fixture
    def expected_path(self, parent_dir: MagicMock) -> str:
        return parent_dir / "config"


class TestAWSCredentials(AWSConfigTestBase):
    """
    Test class derived from AWSConfigTestBase for AWSCredentials.

    All tests are defined in the base class. This class defines the fixtures that feed into those tests.
    """

    @pytest.fixture
    def config_class(self) -> Type[_AWSConfigBase]:
        return AWSCredentials

    @pytest.fixture
    def expected_profile_name_section(self, profile_name: str) -> str:
        return f"{profile_name}"

    @pytest.fixture
    def expected_path(self, parent_dir: MagicMock) -> str:
        return parent_dir / "credentials"
