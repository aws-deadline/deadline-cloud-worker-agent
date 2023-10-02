# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from typing import Type, Generator, Optional

import deadline_worker_agent.aws_credentials.aws_configs as aws_configs_mod
from deadline_worker_agent.aws_credentials.aws_configs import (
    AWSConfig,
    AWSCredentials,
    _AWSConfigBase,
    _setup_file,
    _setup_parent_dir,
)
import os

if os.name == "posix":
    from openjd.sessions import PosixSessionUser
from openjd.sessions import SessionUser


@pytest.fixture
def profile_name() -> str:
    return "queue-1234567890abcdef"


@pytest.fixture(autouse=True)
def mock_run_cmd_as() -> Generator[MagicMock, None, None]:
    with patch.object(aws_configs_mod, "_run_cmd_as") as mock_run_cmd_as:
        yield mock_run_cmd_as


@pytest.fixture()
def os_user() -> Optional[SessionUser]:
    if os.name == "posix":
        return PosixSessionUser(user="some-user", group="some-group")
    else:
        return None


@pytest.mark.skipif(os.name == "nt", reason="Windows is not yet supported.")
class TestSetupParentDir:
    """Tests for the _setup_parent_dir() function"""

    @pytest.fixture
    def dir_path(self) -> MagicMock:
        return MagicMock()

    def test_creates_dir(
        self,
        dir_path: MagicMock,
        os_user: Optional[str],
        mock_run_cmd_as: MagicMock,
    ) -> None:
        """Tests that the directory is created if necessary with the expected permissions"""
        # GIVEN
        mkdir: MagicMock = dir_path.mkdir
        assert isinstance(os_user, PosixSessionUser) or os_user is None

        # WHEN
        _setup_parent_dir(dir_path=dir_path, owner=os_user)

        # THEN
        if os_user:
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["mkdir", "-p", str(dir_path)])
            mock_run_cmd_as.assert_any_call(
                user=os_user,
                cmd=["chown", f"{os_user.user}:{os_user.group}", str(dir_path)],
            )
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["chmod", "770", str(dir_path)])
        else:
            mkdir.assert_called_once_with(
                mode=0o700,
                exist_ok=True,
            )

    def test_sets_group_ownership(
        self,
        dir_path: MagicMock,
        os_user: str,
        mock_run_cmd_as: MagicMock,
    ) -> None:
        """Tests that the directory group ownership is set as specified"""
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None

        # WHEN
        _setup_parent_dir(dir_path=dir_path, owner=os_user)

        # THEN
        if os_user:
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["mkdir", "-p", str(dir_path)])
            mock_run_cmd_as.assert_any_call(
                user=os_user,
                cmd=["chown", f"{os_user.user}:{os_user.group}", str(dir_path)],
            )
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["chmod", "770", str(dir_path)])
        else:
            mock_run_cmd_as.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="Windows is not yet supported.")
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
        mock_run_cmd_as: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None

        # WHEN
        _setup_file(
            file_path=file_path,
            owner=os_user,
        )

        # THEN
        if os_user:
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["touch", str(file_path)])
            mock_run_cmd_as.assert_any_call(
                user=os_user,
                cmd=["chown", f"{os_user.user}:{os_user.group}", str(file_path)],
            )
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["chmod", "660", str(file_path)])
        else:
            file_path.exists.assert_called_once_with()
            if exists:
                file_path.touch.assert_not_called()
            else:
                file_path.touch.assert_called_once_with()

    def test_changes_permissions(
        self,
        file_path: MagicMock,
        os_user: Optional[SessionUser],
        mock_run_cmd_as: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN
        chmod: MagicMock = file_path.chmod
        assert isinstance(os_user, PosixSessionUser) or os_user is None

        # WHEN
        _setup_file(file_path=file_path, owner=os_user)

        # THEN
        if os_user:
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["touch", str(file_path)])
            mock_run_cmd_as.assert_any_call(
                user=os_user,
                cmd=["chown", f"{os_user.user}:{os_user.group}", str(file_path)],
            )
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["chmod", "660", str(file_path)])
        else:
            chmod.assert_called_once_with(mode=0o640 if os_user is not None else 0o600)

    def test_changes_group_ownership(
        self,
        file_path: MagicMock,
        os_user: Optional[SessionUser],
        mock_run_cmd_as: MagicMock,
    ) -> None:
        """Tests the config/credentials file is created if necessary"""
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None

        # WHEN
        _setup_file(
            file_path=file_path,
            owner=os_user,
        )

        # THEN
        if os_user:
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["touch", str(file_path)])
            mock_run_cmd_as.assert_any_call(
                user=os_user,
                cmd=["chown", f"{os_user.user}:{os_user.group}", str(file_path)],
            )
            mock_run_cmd_as.assert_any_call(user=os_user, cmd=["chmod", "660", str(file_path)])
        else:
            mock_run_cmd_as.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="Windows is not yet supported.")
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

    def test_init(
        self,
        config_class: Type[_AWSConfigBase],
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
    ) -> None:
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        config_parser_read: MagicMock = mock_config_parser.read

        with (
            patch.object(config_class, "_get_path") as get_path_mock,
            patch.object(aws_configs_mod, "_setup_parent_dir") as setup_parent_dir_mock,
            patch.object(aws_configs_mod, "_setup_file") as setup_file_mock,
        ):
            config_path: MagicMock = get_path_mock.return_value

            # WHEN
            config = config_class(os_user=os_user)

        # THEN
        expected_user = os_user.user if os_user is not None else ""
        get_path_mock.assert_called_once_with(os_user=expected_user)
        assert config._config_path == config_path
        setup_parent_dir_mock.assert_called_once_with(
            dir_path=get_path_mock.return_value.parent,
            owner=os_user,
        )
        setup_file_mock.assert_called_once_with(
            file_path=get_path_mock.return_value,
            owner=os_user,
        )
        config_parser_read.assert_called_once_with(config._config_path)

    def test_get_path(
        self,
        config_class: Type[_AWSConfigBase],
        expected_path: str,
        os_user: Optional[SessionUser],
    ) -> None:
        # WHEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        result = config_class._get_path(os_user=os_user.user if os_user is not None else "")

        # THEN
        expected_path = expected_path.format(user=os_user.user if os_user is not None else "")
        assert result == Path(expected_path).expanduser()

    @patch.object(aws_configs_mod.Path, "absolute")
    def test_install_credential_process(
        self,
        mock_absolute: MagicMock,
        config_class: Type[_AWSConfigBase],
        profile_name: str,
        expected_profile_name_section: str,
        os_user: Optional[SessionUser],
        mock_config_parser: MagicMock,
    ) -> None:
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        config = config_class(os_user=os_user)
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
    ) -> None:
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        config = config_class(os_user=os_user)
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
    ) -> None:
        # GIVEN
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        with (
            patch.object(aws_configs_mod, "_logger") as logger_mock,
            patch.object(config_class, "_get_path") as get_path_mock,
        ):
            path: MagicMock = get_path_mock.return_value
            config = config_class(os_user)
            info_mock: MagicMock = logger_mock.info

            # WHEN
            config._write()

        # THEN
        info_mock.assert_called_once_with(f"Writing updated {config._config_path} to disk.")
        path.open.assert_called_once_with(mode="w")
        mock_config_parser.write.assert_called_once_with(
            fp=path.open.return_value.__enter__.return_value,
            space_around_delimiters=False,
        )


@pytest.mark.skipif(os.name == "nt", reason="Windows is not yet supported.")
class TestAWSConfig(AWSConfigTestBase):
    """
    Test class derrived from AWSConfigTestBase for AWSConfig.

    All tests are defined in the base class. This class defines the fixtures that feed into those tests.
    """

    @pytest.fixture
    def config_class(self) -> Type[_AWSConfigBase]:
        return AWSConfig

    @pytest.fixture
    def expected_profile_name_section(self, profile_name: str) -> str:
        return f"profile {profile_name}"

    @pytest.fixture
    def expected_path(self, os_user: Optional[SessionUser]) -> str:
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        return f"~{os_user.user if os_user is not None else ''}/.aws/config"


@pytest.mark.skipif(os.name == "nt", reason="Windows is not yet supported.")
class TestAWSCredentials(AWSConfigTestBase):
    """
    Test class derrived from AWSConfigTestBase for AWSCredentials.

    All tests are defined in the base class. This class defines the fixtures that feed into those tests.
    """

    @pytest.fixture
    def config_class(self) -> Type[_AWSConfigBase]:
        return AWSCredentials

    @pytest.fixture
    def expected_profile_name_section(self, profile_name: str) -> str:
        return f"{profile_name}"

    @pytest.fixture
    def expected_path(self, os_user: Optional[SessionUser]) -> str:
        assert isinstance(os_user, PosixSessionUser) or os_user is None
        return f"~{os_user.user if os_user is not None else ''}/.aws/credentials"
