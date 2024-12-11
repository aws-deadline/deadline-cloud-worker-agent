# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from unittest.mock import MagicMock, patch
import os
import secrets
import string
from typing import Optional

import pytest
from pytest import FixtureRequest
from typing import Generator, Optional

from deadline.job_attachments.models import (
    JobAttachmentsFileSystem,
    Attachments,
    ManifestProperties,
    PathFormat,
)
from openjd.model import (
    JobParameterValues,
    ParameterValue,
    SpecificationRevision,
    TemplateSpecificationVersion,
)
from openjd.sessions import (
    PathMappingRule,
    SessionUser,
    PosixSessionUser,
    WindowsSessionUser,
)

from deadline_worker_agent.api_models import HostProperties, IpAddresses
from deadline_worker_agent.installer import (
    ParsedCommandLineArguments,
)
from deadline_worker_agent.sessions.job_entities.job_details import (
    JobAttachmentSettings,
    JobDetails,
    JobRunAsUser,
)
from deadline_worker_agent.sessions.job_entities.job_attachment_details import (
    JobAttachmentDetails,
    JobAttachmentManifestProperties,
)
from deadline_worker_agent.config import JobsRunAsUserOverride

VFS_DEFAULT_INSTALL_PATH = "/opt/deadline_vfs"


@pytest.fixture
def region() -> str:
    return "us-west-2"


@pytest.fixture
def user() -> str:
    return "wa_user"


@pytest.fixture
def password() -> str:
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return "".join(secrets.choice(alphabet) for _ in range(12))


@pytest.fixture(params=("wa_group",))
def group(request: pytest.FixtureRequest) -> Optional[str]:
    return request.param


@pytest.fixture
def service_start() -> bool:
    return False


@pytest.fixture
def confirmed() -> bool:
    return True


@pytest.fixture
def allow_shutdown() -> bool:
    return False


@pytest.fixture
def telemetry_opt_out() -> bool:
    return True


@pytest.fixture
def install_service() -> bool:
    return True


@pytest.fixture
def vfs_install_path() -> str:
    return VFS_DEFAULT_INSTALL_PATH


@pytest.fixture
def grant_required_access() -> bool:
    return True


@pytest.fixture
def disallow_instance_profile() -> bool:
    return True


@pytest.fixture
def windows_job_user() -> str:
    return "job-user"


@pytest.fixture
def parsed_args(
    farm_id: str,
    fleet_id: str,
    region: str,
    user: str,
    password: Optional[str],
    group: Optional[str],
    service_start: bool,
    confirmed: bool,
    allow_shutdown: bool,
    install_service: bool,
    telemetry_opt_out: bool,
    vfs_install_path: str,
    grant_required_access: bool,
    disallow_instance_profile: bool,
    windows_job_user: str,
) -> ParsedCommandLineArguments:
    parsed_args = ParsedCommandLineArguments()
    parsed_args.farm_id = farm_id
    parsed_args.fleet_id = fleet_id
    parsed_args.user = user
    parsed_args.password = password
    parsed_args.group = group
    parsed_args.region = region
    parsed_args.service_start = service_start
    parsed_args.confirmed = confirmed
    parsed_args.allow_shutdown = allow_shutdown
    parsed_args.install_service = install_service
    parsed_args.telemetry_opt_out = telemetry_opt_out
    parsed_args.vfs_install_path = vfs_install_path
    parsed_args.grant_required_access = grant_required_access
    parsed_args.disallow_instance_profile = disallow_instance_profile
    parsed_args.windows_job_user = windows_job_user
    return parsed_args


@pytest.fixture(
    params=("linux",),
)
def platform(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def s3_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def logs_client() -> MagicMock:
    return MagicMock()


@pytest.fixture(autouse=True)
def patch_windows_session_user_validate():
    with patch.object(WindowsSessionUser, "_validate_username_password"):
        yield


@pytest.fixture()
def job_user() -> SessionUser:
    if os.name == "posix":
        return PosixSessionUser(user="some-user", group="some-group")
    else:
        return WindowsSessionUser(user="some-user", password="some-password")


@pytest.fixture()
def job_run_as_user_overrides(job_user: SessionUser) -> JobsRunAsUserOverride:
    return JobsRunAsUserOverride(run_as_agent=False, job_user=job_user)


@pytest.fixture
def command():
    return "echo"


@pytest.fixture
def on_run_args():
    return ["on run"]


@pytest.fixture
def timeout():
    return 10


@pytest.fixture
def job_id() -> str:
    return "job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def step_id() -> str:
    return "step-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def queue_id() -> str:
    return "queue-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def farm_id() -> str:
    return "farm-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def fleet_id() -> str:
    return "fleet-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def worker_id() -> str:
    return "worker-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def log_s3_bucket() -> str:
    return "mybucket"


@pytest.fixture
def log_s3_key() -> str:
    return "my/s3/key"


@pytest.fixture
def log_cw_group_name() -> str:
    return "log_cw_group_name"


@pytest.fixture
def log_cw_stream_name() -> str:
    return "log_cw_stream_name"


@pytest.fixture
def task_id() -> str:
    return "task-123"


@pytest.fixture
def action_id() -> str:
    return "action-111"


@pytest.fixture
def job_env_id() -> str:
    return "job_env-111"


@pytest.fixture(
    params=(
        Attachments(
            manifests=[
                ManifestProperties(
                    rootPath="/tmp",
                    rootPathFormat=PathFormat.POSIX,
                    inputManifestPath="rootPrefix/Manifests/farm-1/queue-1/Inputs/0000/0123_input.xxh128",
                    inputManifestHash="inputmanifesthash",
                    outputRelativeDirectories=["test_outputs"],
                )
            ],
        ),
        None,
    ),
    ids=("with-job-settings", "no-attachments"),
)
def attachments(request: FixtureRequest) -> Attachments | None:
    """A Job Settings object"""
    return request.param  # type: ignore


@pytest.fixture
def asset_sync(attachments: Attachments | None) -> MagicMock:
    assetsync = MagicMock()
    assetsync.get_attachments.return_value = attachments
    return assetsync


@pytest.fixture
def session_id() -> str:
    return "session-526ce00917ac4497b3e7a742e9710b41"


@pytest.fixture
def log_group_name() -> str:
    """The name of the log group for the session"""
    return "log-group-name"


@pytest.fixture
def job_template_version() -> TemplateSpecificationVersion:
    """The Open Job Description schema version"""
    return TemplateSpecificationVersion.JOBTEMPLATE_v2023_09


@pytest.fixture
def specification_revision() -> SpecificationRevision:
    return SpecificationRevision.v2023_09


@pytest.fixture
def queue_job_attachment_settings() -> JobAttachmentSettings:
    return JobAttachmentSettings(
        root_prefix="job_attachments",
        s3_bucket_name="job_attachments_bucket",
    )


@pytest.fixture
def job_attachment_output_directory() -> str:
    return "/asset/output"


@pytest.fixture
def job_attachment_manifest_properties(
    queue_job_attachment_settings: JobAttachmentSettings,
    job_attachment_output_directory: str,
    farm_id: str,
    queue_id: str,
) -> JobAttachmentManifestProperties:
    return JobAttachmentManifestProperties(
        root_path="/foo/bar",
        root_path_format="posix",
        file_system_location_name="",
        input_manifest_path=f"{queue_job_attachment_settings.root_prefix}/Manifests/{farm_id}/{queue_id}/Inputs/0000/0123_input.xxh128",
        input_manifest_hash="inputmanifesthash",
        output_relative_directories=[job_attachment_output_directory],
    )


@pytest.fixture
def job_attachments_file_system() -> JobAttachmentsFileSystem:
    return JobAttachmentsFileSystem.COPIED


@pytest.fixture
def job_attachment_details(
    job_attachment_manifest_properties: JobAttachmentManifestProperties,
    job_attachments_file_system: JobAttachmentsFileSystem,
) -> JobAttachmentDetails | None:
    """Job attachment settings for the job"""
    return JobAttachmentDetails(
        manifests=[job_attachment_manifest_properties],
        job_attachments_file_system=job_attachments_file_system,
    )


@pytest.fixture
def job_parameters() -> JobParameterValues:
    """The job's parameters"""
    return dict[str, ParameterValue]()


@pytest.fixture
def job_run_as_user() -> JobRunAsUser | None:
    """The OS user/group associated with the job's queue"""
    # TODO: windows support
    if os.name != "posix":
        return None
    return JobRunAsUser(posix=PosixSessionUser(user="job-user", group="job-user"))


@pytest.fixture
def path_mapping_rules() -> list[PathMappingRule] | None:
    """The path mapping rules to pass to Open Job Description"""
    return []


@pytest.fixture
def job_details(
    queue_job_attachment_settings: JobAttachmentSettings,
    job_parameters: JobParameterValues,
    log_group_name: str,
    job_run_as_user: JobRunAsUser,
    path_mapping_rules: list[PathMappingRule],
    specification_revision: SpecificationRevision,
) -> JobDetails:
    return JobDetails(
        job_attachment_settings=queue_job_attachment_settings,
        parameters=job_parameters,
        job_run_as_user=job_run_as_user,
        path_mapping_rules=path_mapping_rules,
        log_group_name=log_group_name,
        schema_version=specification_revision,
    )


@pytest.fixture
def step_script() -> MagicMock:
    return MagicMock()


@pytest.fixture
def step_template() -> MagicMock:
    return MagicMock()


@pytest.fixture
def hostname() -> str:
    return "workerhostname"


@pytest.fixture
def host_properties(hostname: str) -> HostProperties:
    return HostProperties(
        hostName=hostname,
        ipAddresses=IpAddresses(
            ipV4Addresses=["127.0.0.1", "192.168.1.100"],
            ipV6Addresses=["::1", "fe80:0000:0000:0000:c685:08ff:fe45:0641"],
        ),
    )


@pytest.fixture
def mock_config_file_not_found() -> Generator[MagicMock, None, None]:
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
