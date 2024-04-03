# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from abc import ABC, abstractmethod
from configparser import ConfigParser
from pathlib import Path
from shutil import chown
from typing import Optional
import logging
import os
import stat

from openjd.sessions import PosixSessionUser, SessionUser

from ..file_system_operations import (
    FileSystemPermissionEnum,
    touch_file,
)
from ..log_messages import FilesystemLogEvent, FilesystemLogEventOp

__all__ = [
    "AWSConfig",
    "AWSCredentials",
]

_logger = logging.getLogger(__name__)


def _setup_file(*, file_path: Path, owner: SessionUser | None = None) -> None:
    if os.name == "posix":
        if owner is None:
            # Read-write for owner user
            mode = stat.S_IRUSR | stat.S_IWUSR
            file_path.touch(mode=mode)
            file_path.chmod(mode=mode)
        else:
            assert isinstance(owner, PosixSessionUser)
            mode = (
                # Read/write for owner user
                stat.S_IRUSR
                | stat.S_IWUSR
                |
                # Read for owner group
                stat.S_IRGRP
            )
            file_path.touch(mode=mode)
            file_path.chmod(mode=mode)
            chown(file_path, group=owner.group)
    else:
        if owner is None:
            touch_file(
                file_path=file_path,
                agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
            )
        else:
            touch_file(
                file_path=file_path,
                permitted_user=owner,
                user_permission=FileSystemPermissionEnum.READ,
                agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
            )


class _AWSConfigBase(ABC):
    """
    Abstract Base class which represents an AWS Config/Credentials file.

    Implementers must implement _get_profile_name() and __class__._get_path().

    Defines functions for reading the config from a given config path, as well as installing and
    uninstalling the config.
    """

    _config_parser: ConfigParser
    _os_user: Optional[SessionUser]
    _parent_dir: Path

    def __init__(
        self,
        *,
        os_user: Optional[SessionUser],
        parent_dir: Path,
    ) -> None:
        """
        Constructor for the AWSConfigBase class

        Args:
            os_user (Optional[SessionUser]): If non-None, then this is the os user to add read
                permissions for. If None, then the only the process user will be able to read
                the credentials files.
            parent_dir (Path): The directory where the AWS config and credentials files will be
                written to.
        """
        super().__init__()

        self._parent_dir = parent_dir

        self._config_parser = ConfigParser()

        # ensure the file exists and has correct permissions and ownership
        _setup_file(
            file_path=self.path,
            owner=os_user,
        )

        # finally, read the config
        self._config_parser.read(self.path)

    def install_credential_process(
        self,
        profile_name: str,
        script_path: Path,
    ) -> None:
        """
        Installs a credential process given the profile name and script path

        Args:
            profile_name (str): The profile name to install under
            script_path (Path): The script to call in the process
        """
        self._config_parser[self._get_profile_name(profile_name)] = {
            "credential_process": str(script_path.absolute()),
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
        _logger.info(
            FilesystemLogEvent(
                op=FilesystemLogEventOp.WRITE,
                filepath=str(self.path),
                message="Saving profile updates.",
            )
        )
        with self.path.open(mode="w") as fp:
            self._config_parser.write(fp=fp, space_around_delimiters=False)

    @abstractmethod
    def _get_profile_name(self, profile_name: str) -> str:  # pragma: no cover
        """
        Returns the profile name in the format required by the config file

        Returns:
            str: The formatted profile name
        """
        raise NotImplementedError("_get_profile_name is not implemented by _AWSConfigBase")

    @property
    @abstractmethod
    def path(self) -> Path:  # pragma: no cover
        typ = type(self)
        raise NotImplementedError(
            f"path property is not implemented by {typ.__module__}.{typ.__name__}"
        )


class AWSConfig(_AWSConfigBase):
    """
    Implementation of _AWSConfigBase to represent the ~/.aws/config file
    """

    _region: str

    def __init__(
        self,
        *,
        os_user: Optional[SessionUser],
        parent_dir: Path,
        region: str,
    ) -> None:
        """
        Constructor for the AWSConfigBase class

        Args:
            os_user (Optional[SessionUser]): If non-None, then this is the os user to add read
                permissions for. If None, then the only the process user will be able to read
                the credentials files.
            parent_dir (Path): The directory where the AWS config and credentials files will be
                written to.
            region (str): The target region where the credentials are for
        """
        super(AWSConfig, self).__init__(
            os_user=os_user,
            parent_dir=parent_dir,
        )
        self._region = region

    def _get_profile_name(self, profile_name: str) -> str:
        return f"profile {profile_name}"

    @property
    def path(self) -> Path:
        return self._parent_dir / "config"

    def install_credential_process(
        self,
        profile_name: str,
        script_path: Path,
    ) -> None:
        """
        Installs a credential process given the profile name and script path

        Args:
            profile_name (str): The profile name to install under
            script_path (Path): The script to call in the process
        """
        self._config_parser[self._get_profile_name(profile_name)] = {
            "credential_process": str(script_path.absolute()),
            "region": self._region,
        }
        self._write()


class AWSCredentials(_AWSConfigBase):
    """
    Implementation of _AWSConfigBase to represent the ~/.aws/credentials file
    """

    def _get_profile_name(self, profile_name: str) -> str:
        return profile_name

    @property
    def path(self) -> Path:
        return self._parent_dir / "credentials"
