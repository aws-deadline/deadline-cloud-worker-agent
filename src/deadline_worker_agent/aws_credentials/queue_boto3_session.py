# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

# Built-in
from pathlib import Path
import shlex
from threading import Event
from typing import Any, Optional, cast
import logging
import os
import shutil
import stat
import subprocess

# Third-party
from botocore.utils import JSONFileCache
from openjd.sessions import PosixSessionUser, WindowsSessionUser, SessionUser

# First-party
from ..boto import DeadlineClient
from ..file_system_operations import (
    make_directory,
    set_permissions,
    FileSystemPermissionEnum,
)
from .aws_configs import AWSConfig, AWSCredentials
from ..aws.deadline import (
    DeadlineRequestUnrecoverableError,
    DeadlineRequestInterrupted,
    DeadlineRequestWorkerOfflineError,
    DeadlineRequestConditionallyRecoverableError,
    assume_queue_role_for_worker,
)
from .boto3_sessions import BaseBoto3Session, SettableCredentials
from .temporary_credentials import TemporaryCredentials

from ..log_messages import (
    FilesystemLogEvent,
    FilesystemLogEventOp,
    AwsCredentialsLogEvent,
    AwsCredentialsLogEventOp,
)

_logger = logging.getLogger(__name__)


class QueueBoto3Session(BaseBoto3Session):
    """A Boto3 Session that contains Queue Role AWS Credentials for use by:

    1.  Any session action run within an Open Job Description session; and
    2.  The Worker when performing actions on behalf of a session [action] for an Open Job
        Description session.

    When created, this Session:

    1.  Creates a queue-specific directory under the worker agent's persistence directory.
        The directory ownership and permissions are configured such that the OS user that the worker
        agent runs as is able to write to it and the job user is able to read from it. No other
        OS users are granted access to these files

        The directory contains:

        *   an AWS config file
        *   an AWS credentials file
        *   a script file to use as an AWS Credentials Process for the Queue's job user
        *   a file containing a JSON representation of the session's credentials

    2.  Creates an aws profile with a "credential_process" within the AWS config and credentials
        files. This looks like:

            ```ini
            [profile queue-897585318504478c9bc7eeeae7785dbb]
            credential_process=/var/lib/deadline/queues/queue-897585318504478c9bc7eeeae7785dbb/get_aws_credentials
            ```

        This feature is supported by official AWS SDKs and the CLI to provide IAM credentials.
        See https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html

    3.  Calls AssumeQueueRoleForWorker and writes the resulting credentials in the format expected
        by credential_process to the JSON file.

    ****************************************** IMPORTANT *******************************************
    If you successfully create an instance of this class, then you must ensure that a code path will
    alwaysresult in the cleanup() method of that instance being called when done with the instance
    object.
    ****************************************** IMPORTANT *******************************************

    Calling QueueBoto3Session.refresh_credentials() will cause a service call to
    AssumeQueueRoleForWorker. When successful, a refresh will:

    1.  Update the AWS Credentials stored & used by this Boto3 Session;
    2.  Persist the obtained AWS Credentials to disk for use in the credential's process; and
    3.  Update this Boto3 Session's AWS Credentials will be updated with the result.
    """

    _deadline_client: DeadlineClient
    _farm_id: str
    _fleet_id: str
    _queue_id: str
    _worker_id: str
    _role_arn: str
    _os_user: Optional[SessionUser]
    _interrupt_event: Event
    _region: str

    # Name of the profile written to the user's AWS configuration for the
    # credentials process
    _profile_name: str

    # Directory where the credentials file & credentials process script are
    # written.
    _credential_dir: Path

    # Serializes the credentials to disk
    _file_cache: JSONFileCache

    # Basename of the filename (minus extension) of the file that credentials are written to
    _credentials_filename_no_ext: str

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
        role_arn: str,
        os_user: Optional[SessionUser] = None,
        interrupt_event: Event,
        worker_persistence_dir: Path,
        region: str,
    ) -> None:
        super().__init__()

        self._deadline_client = deadline_client
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._queue_id = queue_id
        self._role_arn = role_arn
        self._os_user = os_user
        self._interrupt_event = interrupt_event
        self._region = region

        self._profile_name = f"deadline-{self._queue_id}"

        self._credential_dir = self._get_credentials_dir(worker_persistence_dir, queue_id)
        self._file_cache = JSONFileCache(working_dir=self._credential_dir)
        self._credentials_filename_no_ext = (
            "aws_credentials"  # note: .json extension added by JSONFileCache
        )

        if os.name == "posix":
            self._credentials_process_script_path = self._credential_dir / "get_aws_credentials.sh"
        else:
            self._credentials_process_script_path = self._credential_dir / "get_aws_credentials.cmd"

        self._create_credentials_directory(os_user)

        self._aws_config = AWSConfig(
            os_user=self._os_user,
            parent_dir=self._credential_dir,
            region=self._region,
        )
        self._aws_credentials = AWSCredentials(
            os_user=self._os_user,
            parent_dir=self._credential_dir,
        )

        self._install_credential_process()

        # Output at debug level queue credential file ownership and permissions
        self._debug_path_permissions(self._credential_dir)
        self._debug_path_permissions(self._aws_config.path)
        self._debug_path_permissions(self._aws_credentials.path)
        self._debug_path_permissions(self._credentials_process_script_path)

        try:
            self.refresh_credentials()
        except:
            self.cleanup()
            raise

    def _get_credentials_dir(self, worker_persistence_dir: Path, queue_id: str) -> Path:
        return worker_persistence_dir / "queues" / queue_id

    def _debug_path_permissions(self, path: Path, level: int = logging.DEBUG) -> None:
        """Outputs information about the ownership and permissions of a path.

        The output format is:

            <PATH> | user = <USER> | group = <GROUP> | mode = <MODE>

        Argument
        --------
            path (Path): The path
            level (int): The logging level. Defaults to DEBUG.
        """

        # This is a performance optimization since production workers will log at INFO or higher
        if os.name == "posix" and logging.root.isEnabledFor(level) and _logger.isEnabledFor(level):
            # These imports are not at the top because otherwise mypy complains with:
            #
            # src\deadline_worker_agent\aws_credentials\queue_boto3_session.py:203: error:
            # Module has no attribute "getpwuid"  [attr-defined]
            import grp
            import pwd

            if not path.exists():
                _logger.log(level, "path does not exist: %s", path)
                return
            st = path.stat()
            _logger.log(
                level,
                "%s | user = %s | group = %s | mode = %s",
                path,
                pwd.getpwuid(st.st_uid).pw_name,  # type: ignore[attr-defined]
                grp.getgrgid(st.st_gid).gr_name,  # type: ignore[attr-defined]
                oct(st.st_mode),
            )

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
    def aws_config(self) -> AWSConfig:
        """The path to the AWS configuration file"""
        return self._aws_config

    @property
    def aws_credentials(self) -> AWSCredentials:
        """The path to the AWS credentials file"""
        return self._aws_credentials

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
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.QUERY,
                resource=self._queue_id,
                role_arn=self._role_arn,
                message="Requesting AWS Credentials",
            )
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
            _logger.error(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="Worker is not online.",
                )
            )
            # We just return. Other code paths within the Worker Agent's main scheduler event loop will respond,
            # more appropriately, to this situation.
            raise
        except DeadlineRequestConditionallyRecoverableError as e:
            _logger.warning(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="Recoverable errror %s." % e.inner_exc,
                )
            )
            raise
        except DeadlineRequestUnrecoverableError as e:
            _logger.error(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="Unrecoverable errror %s." % e.inner_exc,
                )
            )
            raise
        except Exception as e:
            # This should never happen since assume_queue_role_for_worker only raises
            # DeadlineRequest*Errors, but let's be paranoid.
            _logger.exception(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="Unexpected exception. Please report this to the service team.",
                )
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

        credentials_file_path = self._credentials_file_path()

        if temporary_creds:
            temporary_creds.cache(
                cache=self._file_cache, cache_key=self._credentials_filename_no_ext
            )
            self._debug_path_permissions(credentials_file_path)
            if self._os_user is not None:
                if os.name == "posix":
                    assert isinstance(self._os_user, PosixSessionUser)
                    credentials_file_path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)
                    shutil.chown(
                        credentials_file_path,
                        group=self._os_user.group,
                    )
                    self._debug_path_permissions(credentials_file_path)
                else:
                    assert isinstance(self._os_user, WindowsSessionUser)
                    set_permissions(
                        file_path=credentials_file_path,
                        permitted_user=self._os_user,
                        agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                        user_permission=FileSystemPermissionEnum.READ,
                    )
            elif os.name == "posix":
                credentials_file_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            else:
                set_permissions(
                    file_path=credentials_file_path,
                    agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                )
            credentials_object = cast(SettableCredentials, self.get_credentials())
            credentials_object.set_credentials(temporary_creds.to_deadline())

            _logger.info(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="Obtained temporary Queue AWS Credentials.",
                    expiry=str(temporary_creds.expiry_time),
                )
            )
        else:
            _logger.info(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.QUERY,
                    resource=self._queue_id,
                    role_arn=self._role_arn,
                    message="No AWS Credentials received.",
                )
            )

    def _create_credentials_directory(self, os_user: Optional[SessionUser] = None) -> None:
        """Creates the directory that we're going to write the credentials file to"""

        # make the <worker_persistence_dir>/queues/<queue-id> dir and set permissions
        if os.name == "posix":
            mode: int = stat.S_IRWXU
            if os_user:
                mode = mode | stat.S_IXGRP | stat.S_IRGRP
            try:
                self._credential_dir.mkdir(exist_ok=True, parents=True, mode=mode)
                self._credential_dir.chmod(mode)
                _logger.info(
                    FilesystemLogEvent(
                        op=FilesystemLogEventOp.CREATE,
                        filepath=str(self._credential_dir),
                        message="Credentials directory.",
                    )
                )
            except OSError:
                _logger.error(
                    FilesystemLogEvent(
                        op=FilesystemLogEventOp.CREATE,
                        filepath=str(self._credential_dir),
                        message="Could not create directory. Please check user permissions.",
                    )
                )
                raise
            if os_user is not None:
                assert isinstance(os_user, PosixSessionUser)
                _logger.debug(
                    "Changing group ownership of %s to %s",
                    self._credential_dir,
                    os_user.group,
                )
                shutil.chown(self._credential_dir, group=os_user.group)
        else:
            if self._os_user is None:
                make_directory(
                    dir_path=self._credential_dir,
                    exist_ok=True,
                    parents=True,
                    agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                )
            else:
                make_directory(
                    dir_path=self._credential_dir,
                    exist_ok=True,
                    parents=True,
                    permitted_user=self._os_user,
                    agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                    user_permission=FileSystemPermissionEnum.READ,
                )
            _logger.info(
                FilesystemLogEvent(
                    op=FilesystemLogEventOp.CREATE,
                    filepath=str(self._credential_dir),
                    message="Credentials directory.",
                )
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
                _logger.warning(
                    FilesystemLogEvent(
                        op=FilesystemLogEventOp.DELETE, filepath=path, message="Failed to delete."
                    )
                )

    def _install_credential_process(self) -> None:
        """
        Installs the credential process and sets permissions on the files interacted with.
        """

        _logger.info(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.INSTALL,
                resource=self._queue_id,
                role_arn=self._role_arn,
                message="Installing Credential Process as profile %s." % self._profile_name,
            )
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
            if os.name == "posix":
                # If the file pre-existed, the mode argument in os.open(..., mode=...) will
                # not be used.
                os.chmod(
                    descriptor,
                    mode=mode,
                )

            # Change permissions
            if self._os_user is not None:
                if os.name == "posix":
                    assert isinstance(self._os_user, PosixSessionUser)
                    shutil.chown(descriptor, group=self._os_user.group)
                else:
                    assert isinstance(self._os_user, WindowsSessionUser)
                    set_permissions(
                        file_path=self._credentials_process_script_path,
                        permitted_user=self._os_user,
                        agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                        user_permission=FileSystemPermissionEnum.EXECUTE,
                    )
            elif os.name == "nt":
                # If the file pre-existed, the mode argument in os.open(..., mode=...) will
                # not be used.
                set_permissions(
                    file_path=self._credentials_process_script_path,
                    agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                )

            f.write(self._generate_credential_process_script())

        _logger.info(
            FilesystemLogEvent(
                op=FilesystemLogEventOp.WRITE,
                filepath=str(self._credentials_process_script_path),
                message="Credential Process script.",
            )
        )

        # install credential process to the AWS config and credentials files
        for aws_cred_file in (self._aws_config, self._aws_credentials):
            aws_cred_file.install_credential_process(
                self._profile_name, self._credentials_process_script_path
            )

    def _credentials_file_path(self) -> Path:
        return (self._credential_dir / self._credentials_filename_no_ext).with_suffix(".json")

    def _generate_credential_process_script(self) -> str:
        """
        Generates the bash script which generates the credentials as JSON output on STDOUT.
        This script will be used by the installed credential process.
        """
        credential_files_path = self._credentials_file_path()
        if os.name == "posix":
            return ("#!/bin/bash\nset -eu\n{0}").format(
                " ".join(shlex.quote(arg) for arg in ["cat", str(credential_files_path)])
            )
        else:
            return ("@echo off\n{0}\n").format(
                subprocess.list2cmdline(["type", str(credential_files_path)])
            )

    def _uninstall_credential_process(self) -> None:
        """
        Uninstalls the credential process
        """
        # uninstall the credential process from /home/<job-user>/.aws/config and
        # /home/<job-user>/.aws/credentials
        _logger.info(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.DELETE,
                resource=self._queue_id,
                role_arn=self._role_arn,
                message="Uninstalling Credential Process.",
            )
        )
        for aws_cred_file in (self._aws_config, self._aws_credentials):
            aws_cred_file.uninstall_credential_process(self._profile_name)
