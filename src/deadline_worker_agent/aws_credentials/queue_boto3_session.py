# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
import logging
from typing import Any, Optional, cast
from pathlib import Path
import shutil
import stat
from threading import Event

from botocore.utils import JSONFileCache
from openjd.sessions import PosixSessionUser, WindowsSessionUser, SessionUser

from ..boto import DeadlineClient
from ..file_system_operations import (
    make_directory,
    set_permissions,
    FileSystemPermissionEnum,
)

from .temporary_credentials import TemporaryCredentials
from ..aws.deadline import (
    DeadlineRequestUnrecoverableError,
    DeadlineRequestInterrupted,
    DeadlineRequestWorkerOfflineError,
    DeadlineRequestConditionallyRecoverableError,
    assume_queue_role_for_worker,
)
from .aws_configs import AWSConfig, AWSCredentials
from .boto3_sessions import BaseBoto3Session, SettableCredentials

_logger = logging.getLogger(__name__)


class QueueBoto3Session(BaseBoto3Session):
    """A Boto3 Session that contains Queue Role AWS Credentials for use by:
    1. Any service Session Action run within an Open Job Description Session; and
    2. The Worker when performing actions on behalf of a service Session Action for
       an Open Job Description Session.

    When created, this Session:
    1. Installs an AWS Credentials Process in the ~/.aws of the given os_user, or the current user if
       not provided.
    2. Creates a directory in which to put: a/ a file containing this session's credentials; and b/ a
       script file to use as an AWS Credentials Process for the Queue's job user.

    **If you create an instance of this class, then you must ensure that a code path will always
    result in the cleanup() method of that instance being called when done with the instance object.**

    Calling QueueBoto3Session.refresh_credentials() will cause a service call to AssumeQueueRoleForWorker.
    When successful, a refresh will:
    1. Update the AWS Credentials stored & used by this Boto3 Session;
    2. Persist the obtained AWS Credentials to disk for use in the credential's process; and
    3. Update this Boto3 Session's AWS Credentials will be updated with the result.
    """

    _deadline_client: DeadlineClient
    _farm_id: str
    _fleet_id: str
    _queue_id: str
    _worker_id: str
    _os_user: Optional[SessionUser]
    _interrupt_event: Event

    # Name of the profile written to the user's AWS configuration for the
    # credentials process
    _profile_name: str

    # Directory where the credentials file & credentials process script are
    # written.
    _credential_dir: Path

    # Serializes the credentials to disk
    _file_cache: JSONFileCache

    # Basename of the filename (minus extension) of the file that credentials are written to
    _credentials_filename: str

    # Location of the credentials process script written to disk
    _credentials_process_script_path: Path

    _aws_config: AWSConfig
    _aws_credentials: AWSCredentials

    def __init__(
        self,
        *,
        deadline_client: DeadlineClient,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        queue_id: str,
        os_user: Optional[SessionUser] = None,
        interrupt_event: Event,
        worker_persistence_dir: Path,
    ) -> None:
        super().__init__()

        self._deadline_client = deadline_client
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._queue_id = queue_id
        self._os_user = os_user
        self._interrupt_event = interrupt_event

        self._profile_name = f"deadline-{self._queue_id}"

        self._credential_dir = worker_persistence_dir / "queues" / self._queue_id
        self._file_cache = JSONFileCache(working_dir=self._credential_dir)
        self._credentials_filename = (
            "aws_credentials"  # note: .json extension added by JSONFileCache
        )

        if os.name == "posix":
            self._credentials_process_script_path = self._credential_dir / "get_aws_credentials.sh"
        else:
            self._credentials_process_script_path = self._credential_dir / "get_aws_credentials.cmd"

        self._aws_config = AWSConfig(self._os_user)
        self._aws_credentials = AWSCredentials(self._os_user)

        self._create_credentials_directory()
        self._install_credential_process()

        try:
            self.refresh_credentials()
        except:
            self.cleanup()
            raise

    def cleanup(self) -> None:
        """This must be called when you are done with the constructed object.
        It deletes any files that were written to disk, and undoes changes to the
        AWS configuration of the user.
        """
        self._uninstall_credential_process()
        self._delete_credentials_directory()

    @property
    def credential_process_profile_name(self) -> str:
        """The name of the profile that the Credentials Process is being installed
        under.
        """
        return self._profile_name

    @property
    def has_credentials(self) -> bool:
        """Query whether or not the Session has AWS Credentials.
        The IAM Role on a Queue is optional, so it is possible that there are
        no AWS Credentials available for the Queue; this returns False in this case.

        """
        credentials_object = cast(SettableCredentials, self.get_credentials())
        return not credentials_object.are_expired()

    def refresh_credentials(self) -> None:
        """Attempt a refresh of the AWS Credentials stored in this Session by
        calling the AssumeQueueRoleForUser API.

        If successful, then:
        1. Update the credentials stored in this Session; and
        2. Persist the retrieved credentials to disk.

        Raises:
           DeadlineRequestUnrecoverableError -- When we could not obtain new credentials
              for any reason.
           DeadlineRequestInterrupted -- If the interrupt event was set.
           DeadlineRequestWorkerOfflineError -- If the request determined that the Worker no longer
              has an online status on the service.
           DeadlineRequestConditionallyRecoverableError -- If we experience an exception making the
              request that the caller may be able to recover from.
        """
        _logger.info(
            "Requesting AWS Credentials for Queue %s on behalf of Worker %s via AssumeQueueRoleForWorker",
            self._queue_id,
            self._worker_id,
        )

        try:
            response = assume_queue_role_for_worker(
                deadline_client=self._deadline_client,
                farm_id=self._farm_id,
                fleet_id=self._fleet_id,
                worker_id=self._worker_id,
                queue_id=self._queue_id,
                interrupt_event=self._interrupt_event,
            )
        except DeadlineRequestInterrupted:
            # We were interrupted. Let our caller know so that they can respond accordingly.
            raise
        except DeadlineRequestWorkerOfflineError:
            _logger.warning(
                "Response to AssumeQueueRoleForWorker for Queue %s indicates that the Worker is not online.",
                self._queue_id,
            )
            # We just return. Other code paths within the Worker Agent's main scheduler event loop will respond,
            # more appropriately, to this situation.
            raise
        except DeadlineRequestConditionallyRecoverableError as e:
            _logger.warning(
                "Response to AssumeQueueRoleForWorker for Queue %s is a recoverable error: %s",
                self._queue_id,
                e.inner_exc,
            )
            raise
        except DeadlineRequestUnrecoverableError as e:
            _logger.error(
                "Response to AssumeQueueRoleForWorker for Queue %s is an unrecoverable error: %s",
                self._queue_id,
                e.inner_exc,
            )
            raise
        except Exception as e:
            # This should never happen since assume_queue_role_for_worker only raises
            # DeadlineRequest*Errors, but let's be paranoid.
            _logger.critical(
                "Unexpected exception from AssumeQueueRoleForWorker for Queue %s. Please report this to the service team. -- %s",
                self._queue_id,
                e,
            )
            raise DeadlineRequestUnrecoverableError(e)

        try:
            temporary_creds = TemporaryCredentials.from_deadline_assume_role_response(
                response=cast(dict[str, Any], response),
                credentials_required=False,
                api_name="AssumeQueueRoleForWorker",
            )
        except (KeyError, TypeError, ValueError) as e:
            # Something was bad with the response. That's unrecoverable.
            raise DeadlineRequestUnrecoverableError(e)

        if temporary_creds:
            temporary_creds.cache(cache=self._file_cache, cache_key=self._credentials_filename)
            if self._os_user is not None:
                if os.name == "posix":
                    assert isinstance(self._os_user, PosixSessionUser)
                    (self._credential_dir / self._credentials_filename).with_suffix(".json").chmod(
                        stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP
                    )
                    shutil.chown(
                        (self._credential_dir / self._credentials_filename).with_suffix(".json"),
                        group=self._os_user.group,
                    )
                else:
                    assert isinstance(self._os_user, WindowsSessionUser)
                    set_permissions(
                        file_path=(self._credential_dir / self._credentials_filename).with_suffix(
                            ".json"
                        ),
                        permitted_user=self._os_user,
                        user_permission=FileSystemPermissionEnum.READ_WRITE,
                        group_permission=FileSystemPermissionEnum.READ_WRITE,
                    )
            credentials_object = cast(SettableCredentials, self.get_credentials())
            credentials_object.set_credentials(temporary_creds.to_deadline())

            _logger.info(
                "New temporary Queue AWS Credentials obtained for Queue %s. They expire at %s.",
                self._queue_id,
                temporary_creds.expiry_time,
            )
        else:
            _logger.info("No AWS Credentials received for Queue %s.", self._queue_id)

    def _create_credentials_directory(self) -> None:
        """Creates the directory that we're going to write the credentials file to"""

        # make the <worker_persistence_dir>/queues/<queue-id> dir and set permissions
        if os.name == "posix":
            try:
                self._credential_dir.mkdir(
                    exist_ok=True, parents=True, mode=(stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP)
                )
            except OSError:
                _logger.error(
                    "Please check user permissions. Could not create directory: %s",
                    str(self._credential_dir),
                )
                raise
            if self._os_user is not None:
                if isinstance(self._os_user, PosixSessionUser):
                    shutil.chown(self._credential_dir, group=self._os_user.group)
        else:
            if self._os_user is None:
                make_directory(
                    dir_path=self._credential_dir,
                    exist_ok=True,
                    parents=True,
                    user_permission=FileSystemPermissionEnum.READ_WRITE,
                )
            else:
                make_directory(
                    dir_path=self._credential_dir,
                    exist_ok=True,
                    parents=True,
                    permitted_user=self._os_user,
                    user_permission=FileSystemPermissionEnum.READ_WRITE,
                    group_permission=FileSystemPermissionEnum.READ,
                )

    def _delete_credentials_directory(self) -> None:
        # delete the <worker_persistence_dir>/queues/<queue-id> dir
        if self._credential_dir.exists():
            not_deleted = list[str]()

            def onerror(function, path, excinfo):
                nonlocal not_deleted
                not_deleted.append(path)

            shutil.rmtree(self._credential_dir, onerror=onerror)
            for path in not_deleted:
                _logger.warning("Failed to delete %s", path)

    def _install_credential_process(self) -> None:
        """
        Installs the credential process and sets permissions on the files interacted with.
        """

        _logger.info(
            "Installing Credential Process for Queue %s as profile %s.",
            self._queue_id,
            self._profile_name,
        )

        # write the credential process script and set permissions
        mode: int = stat.S_IRWXU
        if self._os_user is not None:
            mode |= stat.S_IRGRP | stat.S_IXGRP
        descriptor = os.open(
            path=str(self._credentials_process_script_path),
            flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            mode=mode,
        )
        with open(descriptor, mode="w", encoding="utf-8") as f:
            f.write(self._generate_credential_process_script())
            if self._os_user is not None:
                if os.name == "posix":
                    assert isinstance(self._os_user, PosixSessionUser)
                    shutil.chown(self._credentials_process_script_path, group=self._os_user.group)
                else:
                    assert isinstance(self._os_user, WindowsSessionUser)
                    set_permissions(
                        file_path=self._credentials_process_script_path,
                        permitted_user=self._os_user,
                        user_permission=FileSystemPermissionEnum.EXECUTE,
                        group_permission=FileSystemPermissionEnum.READ_WRITE,
                    )

        # install credential process to ~<job-user>/.aws/config and
        # ~<job-user>/.aws/credentials
        for aws_cred_file in (self._aws_config, self._aws_credentials):
            aws_cred_file.install_credential_process(
                self._profile_name, self._credentials_process_script_path
            )

    def _generate_credential_process_script(self) -> str:
        """
        Generates the bash script which generates the credentials as JSON output on STDOUT.
        This script will be used by the installed credential process.
        """
        if os.name == "posix":
            return ("#!/bin/bash\nset -eu\ncat {0}\n").format(
                (self._credential_dir / self._credentials_filename).with_suffix(".json")
            )
        else:
            return ('@echo off\ntype "{0}"\n').format(
                (self._credential_dir / self._credentials_filename).with_suffix(".json")
            )

    def _uninstall_credential_process(self) -> None:
        """
        Uninstalls the credential process
        """
        # uninstall the credential process from /home/<job-user>/.aws/config and
        # /home/<job-user>/.aws/credentials
        _logger.info("Uninstalling Credential Process for Queue %s", self._queue_id)
        for aws_cred_file in (self._aws_config, self._aws_credentials):
            aws_cred_file.uninstall_credential_process(self._profile_name)
