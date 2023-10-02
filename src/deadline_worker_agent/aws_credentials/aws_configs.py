# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import stat
import os
import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from pathlib import Path
from typing import Optional
from openjd.sessions import PosixSessionUser, SessionUser
from subprocess import run, DEVNULL, PIPE, STDOUT
from ..set_windows_permissions import set_user_restricted_path_permissions

__all__ = [
    "AWSConfig",
    "AWSCredentials",
]

_logger = logging.getLogger(__name__)


def _run_cmd_as(*, user: PosixSessionUser, cmd: list[str]) -> None:
    sudo = ["sudo", "-u", user.user, "-i"]
    # Raises: CalledProcessError
    run(sudo + cmd, stdin=DEVNULL, stderr=STDOUT, stdout=PIPE, check=True)


def _setup_parent_dir(*, dir_path: Path, owner: SessionUser | None = None) -> None:
    if owner is None:
        if os.name == "posix":
            create_perms: int = stat.S_IRWXU
            dir_path.mkdir(mode=create_perms, exist_ok=True)
        else:
            dir_path.mkdir(exist_ok=True)
            set_user_restricted_path_permissions(dir_path.name)
    else:
        assert isinstance(owner, PosixSessionUser)
        _run_cmd_as(user=owner, cmd=["mkdir", "-p", str(dir_path)])
        _run_cmd_as(user=owner, cmd=["chown", f"{owner.user}:{owner.group}", str(dir_path)])
        _run_cmd_as(user=owner, cmd=["chmod", "770", str(dir_path)])


def _setup_file(*, file_path: Path, owner: SessionUser | None = None) -> None:
    if owner is None:
        if not file_path.exists():
            file_path.touch()
        mode = stat.S_IRUSR | stat.S_IWUSR
        file_path.chmod(mode=mode)
    else:
        assert isinstance(owner, PosixSessionUser)
        _run_cmd_as(user=owner, cmd=["touch", str(file_path)])
        _run_cmd_as(user=owner, cmd=["chown", f"{owner.user}:{owner.group}", str(file_path)])
        _run_cmd_as(user=owner, cmd=["chmod", "660", str(file_path)])


class _AWSConfigBase(ABC):
    """
    Abstract Base class which represents an AWS Config/Credentials file.

    Implementers must implement _get_profile_name() and __class__._get_path().

    Defines functions for reading the config from a given config path, as well as installing and
    uninstalling the config.
    """

    _config_path: Path
    _config_parser: ConfigParser
    _os_user: Optional[SessionUser]

    def __init__(self, os_user: Optional[SessionUser]) -> None:
        """
        Constructor for the AWSConfigBase class

        Args:
            os_user (Optional[SessionUser]): If non-None, then this is the os user to add read
                permissions for. If None, then the only the process user will be able to read
                the credentials files.
        """
        super().__init__()

        if os_user is not None and not isinstance(os_user, PosixSessionUser):
            raise NotImplementedError("Only posix user impersonation is currently implemented.")

        self._config_path = self._get_path(os_user=os_user.user if os_user is not None else "")
        self._config_parser = ConfigParser()

        # setup the containing directory permissions and ownership
        config_dir = self._config_path.parent
        _setup_parent_dir(
            dir_path=config_dir,
            owner=os_user,
        )

        # ensure the file exists and has correct permissions and ownership
        _setup_file(
            file_path=self._config_path,
            owner=os_user,
        )

        # finally, read the config
        self._config_parser.read(self._config_path)

    def install_credential_process(self, profile_name: str, script_path: Path) -> None:
        """
        Installs a credential process given the profile name and script path

        Args:
            profile_name (str): The profile name to install under
            script_path (Path): The script to call in the process
        """
        self._config_parser[self._get_profile_name(profile_name)] = {
            "credential_process": str(script_path.absolute())
        }
        self._write()

    def uninstall_credential_process(self, profile_name: str) -> None:
        """
        Uninstalls a credential process given the profile name

        Args:
            profile_name (str): The profile name to uninstall
        """
        modified = False
        if self._get_profile_name(profile_name) in self._config_parser:
            del self._config_parser[self._get_profile_name(profile_name)]
            modified = True

        if modified:
            self._write()

    def _write(self) -> None:
        """
        Writes the config to the config path given in the constructor
        """
        _logger.info(f"Writing updated {self._config_path} to disk.")
        with self._config_path.open(mode="w") as fp:
            self._config_parser.write(fp=fp, space_around_delimiters=False)

    @abstractmethod
    def _get_profile_name(self, profile_name: str) -> str:  # pragma: no cover
        """
        Returns the profile name in the format required by the config file

        Returns:
            str: The formatted profile name
        """
        raise NotImplementedError("_get_profile_name is not implemented by _AWSConfigBase")

    @staticmethod
    @abstractmethod
    def _get_path(os_user: str) -> Path:  # pragma: no cover
        raise NotImplementedError("_get_path is not implemented by _AWSConfigBase")


class AWSConfig(_AWSConfigBase):
    """
    Implementation of _AWSConfigBase to represent the ~/.aws/config file
    """

    def _get_profile_name(self, profile_name: str) -> str:
        return f"profile {profile_name}"

    @staticmethod
    def _get_path(os_user: str) -> Path:
        return Path(f"~{os_user}/.aws/config").expanduser()


class AWSCredentials(_AWSConfigBase):
    """
    Implementation of _AWSConfigBase to represent the ~/.aws/credentials file
    """

    def _get_profile_name(self, profile_name: str) -> str:
        return profile_name

    @staticmethod
    def _get_path(os_user: str) -> Path:
        return Path(f"~{os_user}/.aws/credentials").expanduser()
