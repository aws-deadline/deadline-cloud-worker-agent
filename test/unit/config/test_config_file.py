# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Any
from unittest.mock import MagicMock, patch
import tempfile
from pathlib import Path
from deadline_worker_agent.capabilities import Capabilities

from pydantic import ValidationError, BaseSettings
import pytest

try:
    from tomllib import TOMLDecodeError
except ModuleNotFoundError:
    from tomli import TOMLDecodeError

from deadline_worker_agent.config.errors import ConfigurationError
from deadline_worker_agent.config.config_file import (
    WorkerConfigSection,
    AwsConfigSection,
    LoggingConfigSection,
    OsConfigSection,
    ConfigFile,
)
from deadline_worker_agent.config import config_file as config_file_mod


@pytest.fixture
def farm_id() -> str:
    return "farm-8bfcf5df8a93404396768811e8442506"


@pytest.fixture
def fleet_id() -> str:
    return "fleet-caada3f1ca944b3cbfa85e399de4a4a3"


@pytest.fixture
def worker_config_section_data(
    farm_id: str | None,
    fleet_id: str | None,
) -> dict[str, Any]:
    return {
        "farm_id": farm_id,
        "fleet_id": fleet_id,
    }


@pytest.fixture(
    params=("my_profile", "anotherprofile"),
)
def profile(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(
    params=(
        True,
        False,
    ),
)
def allow_ec2_instance_profile(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture
def aws_config_section_data(
    profile: str | None,
    allow_ec2_instance_profile: bool | None,
) -> dict[str, Any]:
    return {
        "profile": profile,
        "allow_ec2_instance_profile": allow_ec2_instance_profile,
    }


@pytest.fixture(
    params=(
        True,
        False,
    ),
)
def verbose(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture
def worker_logs_dir(tmp_path: Path) -> str | None:
    return str(tmp_path)


@pytest.fixture(
    params=(
        True,
        False,
        None,
    ),
)
def local_session_logs(request: pytest.FixtureRequest) -> bool | None:
    return request.param


@pytest.fixture
def logging_config_section_data(
    verbose: bool | None,
    worker_logs_dir: str | None,
    local_session_logs: bool | None,
) -> dict[str, Any]:
    return {
        "verbose": verbose,
        "worker_logs_dir": worker_logs_dir,
        "local_session_logs": local_session_logs,
    }


@pytest.fixture(
    params=(True, False),
)
def run_jobs_as_agent_user(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(params=("some-user:some-group", None))
def posix_job_user(request: pytest.FixtureRequest) -> str | None:
    return request.param


@pytest.fixture(params=("a-job-user", None))
def windows_job_user(request: pytest.FixtureRequest) -> str | None:
    return request.param


@pytest.fixture(
    params=(True, False),
)
def shutdown_on_stop(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(
    params=(True, False),
)
def retain_session_dir(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture
def os_config_section_data(
    run_jobs_as_agent_user: bool,
    posix_job_user: str,
    windows_job_user: str,
    shutdown_on_stop: bool | None,
    retain_session_dir: bool | None,
) -> dict[str, Any]:
    return {
        "run_jobs_as_agent_user": run_jobs_as_agent_user,
        "posix_job_user": posix_job_user,
        "windows_job_user": windows_job_user,
        "shutdown_on_stop": shutdown_on_stop,
        "retain_session_dir": retain_session_dir,
    }


class TestWorkerConfigSection:
    def test_valid_inputs(
        self,
        worker_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that WorkerConfigSection accepts valid field values"""
        # WHEN
        worker_config = WorkerConfigSection.parse_obj(worker_config_section_data)

        # THEN
        assert worker_config.farm_id == worker_config_section_data["farm_id"]
        assert worker_config.fleet_id == worker_config_section_data["fleet_id"]

    @pytest.mark.parametrize(
        argnames="farm_id",
        argvalues=(
            pytest.param("not-valid-farm-id", id="bad-format"),
            pytest.param("", id="empty"),
        ),
    )
    def test_nonvalid_farm_id(
        self,
        worker_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that WorkerConfigSections raises ValidationErrors for non-valid farm_id values"""

        # WHEN
        def when() -> WorkerConfigSection:
            return WorkerConfigSection.parse_obj(worker_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_farm_id(
        self,
        worker_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that an absent "farm_id" key does not raise a ValidationError"""
        # GIVEN
        del worker_config_section_data["farm_id"]

        # THEN
        WorkerConfigSection.parse_obj(worker_config_section_data)

    @pytest.mark.parametrize(
        argnames="fleet_id",
        argvalues=(
            pytest.param("non-valid-fleet-id", id="bad-format"),
            pytest.param("", id="empty"),
        ),
    )
    def test_nonvalid_fleet_id(
        self,
        worker_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that WorkerConfigSections raises ValidationErrors for non-valid fleet_id values"""

        # WHEN
        def when() -> WorkerConfigSection:
            return WorkerConfigSection.parse_obj(worker_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_fleet_id(
        self,
        worker_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that an absent "fleet_id" key does not raise a ValidationError"""
        # GIVEN
        del worker_config_section_data["fleet_id"]

        # THEN
        WorkerConfigSection.parse_obj(worker_config_section_data)


class TestAwsConfigSection:
    def test_valid_inputs(
        self,
        aws_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that AwsConfigSection accepts valid inputs for its available fields"""
        # WHEN
        aws_config = AwsConfigSection.parse_obj(aws_config_section_data)

        # THEN
        assert aws_config.profile == aws_config_section_data["profile"]

    @pytest.mark.parametrize(
        argnames="profile",
        argvalues=(
            pytest.param("", id="empty"),
            pytest.param("a" * 65, id="longer-than-max-length"),
        ),
    )
    def test_not_valid_profile(
        self,
        aws_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for non-valid AWS profile values"""

        # WHEN
        def when() -> AwsConfigSection:
            return AwsConfigSection.parse_obj(aws_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    @pytest.mark.parametrize(
        argnames="allow_ec2_instance_profile",
        argvalues=(
            pytest.param("string value", id="bad-type-str"),
            pytest.param([1], id="bad-type-array"),
        ),
    )
    def test_not_valid_allow_ec2_instance_profile(
        self,
        aws_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for non-valid
        allow_ec2_instance_profile values"""

        # WHEN
        def when() -> AwsConfigSection:
            print(aws_config_section_data)
            return AwsConfigSection.parse_obj(aws_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()


class TestLoggingConfigSection:
    def test_valid_inputs(
        self,
        logging_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that LoggingConfigSection accepts valid inputs for its available fields"""
        # WHEN
        logging_config = LoggingConfigSection.parse_obj(logging_config_section_data)

        # THEN
        assert logging_config.verbose == logging_config_section_data["verbose"]
        assert (
            logging_config.local_session_logs == logging_config_section_data["local_session_logs"]
        )
        assert logging_config.worker_logs_dir == Path(
            logging_config_section_data["worker_logs_dir"]
        )

    @pytest.mark.parametrize(
        argnames="verbose",
        argvalues=(
            pytest.param("str", id="bad-type-str"),
            pytest.param([1], id="bad-type-list"),
        ),
    )
    def test_not_valid_verbose(
        self,
        logging_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for non-valid AWS profile values"""

        # WHEN
        def when() -> LoggingConfigSection:
            return LoggingConfigSection.parse_obj(logging_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_verbose(
        self,
        logging_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that absent a "verbose" value in the input to LoggingConfigSection, it should
        have a corresponding attribute value of None"""
        # GIVEN
        del logging_config_section_data["verbose"]

        # WHEN
        logging_config = LoggingConfigSection.parse_obj(logging_config_section_data)

        # THEN
        assert logging_config.verbose is None

    @pytest.mark.parametrize(
        argnames="worker_logs_dir",
        argvalues=(
            pytest.param(1, id="int"),
            pytest.param(1.5, id="float"),
            pytest.param(True, id="bool"),
            pytest.param(["a"], id="list"),
            pytest.param({"a": 1}, id="dict"),
        ),
    )
    def test_non_valid_worker_logs_dir(
        self,
        logging_config_section_data: dict[str, Any],
    ) -> None:
        # WHEN
        def when() -> LoggingConfigSection:
            return LoggingConfigSection.parse_obj(logging_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    @pytest.mark.parametrize(
        argnames="local_session_logs",
        argvalues=(
            pytest.param("abc", id="str"),
            pytest.param([1, 2, 3], id="list"),
            pytest.param({"a": 1}, id="dict"),
        ),
    )
    def test_non_valid_local_session_logs(
        self,
        logging_config_section_data: dict[str, Any],
    ) -> None:
        # GIVEN
        # WHEN
        def when() -> LoggingConfigSection:
            return LoggingConfigSection.parse_obj(logging_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()


class TestOsConfigSection:
    def test_valid_inputs(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that OsConfigSection accepts valid inputs for its available fields"""
        # WHEN
        os_config = OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        assert os_config.run_jobs_as_agent_user == os_config_section_data["run_jobs_as_agent_user"]
        assert os_config.posix_job_user == os_config_section_data["posix_job_user"]
        assert os_config.windows_job_user == os_config_section_data["windows_job_user"]
        assert os_config.shutdown_on_stop == os_config_section_data["shutdown_on_stop"]
        assert os_config.retain_session_dir == os_config_section_data["retain_session_dir"]

    @pytest.mark.parametrize(
        argnames="run_jobs_as_agent_user",
        argvalues=(
            pytest.param("str", id="bad-type-str"),
            pytest.param([1], id="bad-type-list"),
        ),
    )
    def test_nonvalid_run_jobs_as_agent_user(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for non-valid run_jobs_as_agent_user values"""

        # WHEN
        def when() -> OsConfigSection:
            return OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_run_jobs_as_agent_user(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that absent a "run_jobs_as_agent_user" value in the input to OsConfigSection, it should
        have a corresponding attribute value of None"""
        # GIVEN
        del os_config_section_data["run_jobs_as_agent_user"]

        # WHEN
        os_config = OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        assert os_config.run_jobs_as_agent_user is None

    @pytest.mark.parametrize(
        argnames="posix_job_user",
        argvalues=(
            pytest.param(True, id="bad-type-bool"),
            pytest.param([1], id="bad-type-list"),
            pytest.param("just-a-user", id="str no colon"),
            pytest.param("just-a-user:", id="str no group"),
            pytest.param(":just-a-group", id="str no user"),
        ),
    )
    def test_nonvalid_posix_job_user(self, os_config_section_data: dict[str, Any]) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for not valid posix job values"""

        # WHEN
        def when() -> OsConfigSection:
            return OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_posix_job_user(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that absent a "posix_job_user" value in the input to OsConfigSection, it should
        have a corresponding attribute value of None"""
        # GIVEN
        del os_config_section_data["posix_job_user"]

        # WHEN
        os_config = OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        assert os_config.posix_job_user is None

    @pytest.mark.parametrize(
        argnames="windows_job_user",
        argvalues=[
            pytest.param(True, id="bad-type-bool"),
            pytest.param([1], id="bad-type-list"),
            pytest.param("a" * 513, id="too long"),
        ],
    )
    def test_nonvalid_windows_job_user(self, os_config_section_data: dict[str, Any]) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for not valid windows job user values"""

        # WHEN
        def when() -> OsConfigSection:
            return OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_windows_job_user(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that absent a "windows_job_user" value in the input to OsConfigSection, it should
        have a corresponding attribute value of None"""
        # GIVEN
        del os_config_section_data["windows_job_user"]

        # WHEN
        os_config = OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        assert os_config.windows_job_user is None

    @pytest.mark.parametrize(
        argnames="retain_session_dir",
        argvalues=(
            pytest.param("str", id="bad-type-str"),
            pytest.param([1], id="bad-type-list"),
        ),
    )
    def test_nonvalid_retain_session_dir(self, os_config_section_data: dict[str, Any]) -> None:
        """Asserts that AwsConfigSections raises ValidationErrors for not valid retain_session_dir values"""

        # WHEN
        def when() -> OsConfigSection:
            return OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        with pytest.raises(ValidationError):
            when()

    def test_absent_retain_session_dir(
        self,
        os_config_section_data: dict[str, Any],
    ) -> None:
        """Asserts that absent a "retain_session_dir" value in the input to OsConfigSection, it should
        have a corresponding attribute value of None"""
        # GIVEN
        del os_config_section_data["retain_session_dir"]

        # WHEN
        os_config = OsConfigSection.parse_obj(os_config_section_data)

        # THEN
        assert os_config.retain_session_dir is None


FULL_CONFIG_FILE_DATA = {
    "worker": {
        "farm_id": "farm-1f0ece77172c441ebe295491a51cf6d5",
        "fleet_id": "fleet-c4a9481caa88404fa878a7fb98f8a4dd",
        "cleanup_session_user_processes": False,
        "worker_persistence_dir": "/my/worker/persistence",
    },
    "aws": {
        "profile": "my_aws_profile_name",
        "allow_ec2_instance_profile": True,
    },
    "logging": {
        "verbose": True,
        "worker_logs_dir": "/var/log/amazon/deadline",
        "local_session_logs": False,
    },
    "os": {
        "run_jobs_as_agent_user": False,
        "posix_job_user": "user:group",
        "shutdown_on_stop": False,
        "retain_session_dir": False,
    },
    "capabilities": {
        "amounts": {
            "amount.slots": 20,
            "deadline:amount.pets": 99,
        },
        "attributes": {
            "attr.groups": ["simulation"],
            "acmewidgetsco:attr.admins": ["bob", "alice"],
        },
    },
}


class TestConfigFileValidation:
    @pytest.mark.parametrize(
        "config_file_data",
        [
            pytest.param(
                {
                    "worker": {
                        "cleanup_session_user_processes": False,
                    },
                    "aws": {},
                    "logging": {},
                    "os": {},
                    "capabilities": {
                        "amounts": {},
                        "attributes": {},
                    },
                },
                id="only required fields",
            ),
            pytest.param(
                FULL_CONFIG_FILE_DATA,
                id="all fields",
            ),
        ],
    )
    def test_input_validation_success(self, config_file_data: dict[str, Any]):
        """Asserts that a valid input dictionary passes ConfigFile model validation"""
        # WHEN
        config_file = ConfigFile.parse_obj(config_file_data)

        # THEN
        assert config_file.worker == WorkerConfigSection.parse_obj(config_file_data["worker"])
        assert config_file.aws == AwsConfigSection.parse_obj(config_file_data["aws"])
        assert config_file.logging == LoggingConfigSection.parse_obj(config_file_data["logging"])
        assert config_file.os == OsConfigSection.parse_obj(config_file_data["os"])
        assert config_file.capabilities == Capabilities.parse_obj(config_file_data["capabilities"])

    @pytest.mark.parametrize(
        ("section_to_modify", "nonvalid_section_data"),
        [
            pytest.param("worker", None, id="missing worker config section"),
            pytest.param(
                "worker",
                {"farm_id": "farm-x"},
                id="nonvalid worker config section - nonvalid farm_id (bad format)",
            ),
            pytest.param("aws", None, id="missing aws config section"),
            pytest.param(
                "aws",
                {"profile": ""},
                id="nonvalid aws config section - profile is an empty string",
            ),
            pytest.param("logging", None, id="missing logging config section"),
            pytest.param(
                "logging",
                {"verbose": "verbose"},
                id="nonvalid logging config section - verbose is not bool",
            ),
            pytest.param("os", None, id="missing os config section"),
            pytest.param(
                "os",
                {"posix_job_user": " user : group "},
                id="nonvalid os config section - nonvalid posix_job_user value (whitespace)",
            ),
            pytest.param("capabilities", None, id="missing capabilities section"),
            pytest.param(
                "capabilities",
                {"amounts": {}},
                id="nonvalid capabilities config section - missing attributes",
            ),
        ],
    )
    def test_input_validation_failure(
        self, section_to_modify: str, nonvalid_section_data: dict[str, Any] | None
    ):
        """Tests that an nonvalid iniput dictionary fails ConfigFile model validation"""
        config_file_data = FULL_CONFIG_FILE_DATA.copy()

        if nonvalid_section_data is None:
            del config_file_data[section_to_modify]
        else:
            config_file_data[section_to_modify] = nonvalid_section_data

        # WHEN
        def when() -> ConfigFile:
            return ConfigFile.parse_obj(config_file_data)

        with pytest.raises(ValidationError) as excinfo:
            when()

        # THEN
        assert len(excinfo.value.errors()) > 0


FULL_CONFIG_FILE = """
[worker]
farm_id = "farm-1f0ece77172c441ebe295491a51cf6d5"
fleet_id = "fleet-c4a9481caa88404fa878a7fb98f8a4dd"
worker_persistence_dir = "/my/worker/persistence"

[aws]
profile = "my_aws_profile_name"
allow_ec2_instance_profile = true

[logging]
verbose = true
worker_logs_dir = "/var/log/amazon/deadline"
local_session_logs = false
host_metrics_logging = true
host_metrics_logging_interval_seconds = 1

[os]
run_jobs_as_agent_user = false
posix_job_user = "user:group"
shutdown_on_stop = false
retain_session_dir = false

[capabilities.amounts]
"amount.slots" = 20
"deadline:amount.pets" = 99

[capabilities.attributes]
"attr.groups" = [
  "simulation",
  "maya",
  "nuke"
]
"acmewidgetsco:attr.admins" = [
  "bob",
  "alice"
]
"""


class TestConfigFileLoad:
    def test_config_load_normal(
        self,
    ) -> None:
        # GIVEN
        with (
            patch.object(ConfigFile, "get_config_path") as mock_get_config_path,
            patch.object(config_file_mod, "load_toml") as mock_load_toml,
            patch.object(ConfigFile, "parse_obj") as mock_parse_obj,
        ):
            config_path: MagicMock = mock_get_config_path.return_value
            config_path_open: MagicMock = config_path.open
            config_path_fh: MagicMock = config_path_open.return_value
            config_path_fh_ctx: MagicMock = config_path_fh.__enter__.return_value

            # WHEN
            config_file = ConfigFile.load()

        # THEN
        mock_get_config_path.assert_called_once_with()
        config_path_open.assert_called_once_with(mode="rb")
        mock_load_toml.assert_called_once_with(config_path_fh_ctx)
        mock_parse_obj.assert_called_once_with(mock_load_toml.return_value)
        assert config_file is mock_parse_obj.return_value

    def test_config_load_full_toml(
        self,
    ) -> None:
        # GIVEN
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "config_file.toml"
            with config_file.open("w", encoding="utf-8") as fh:
                fh.write(FULL_CONFIG_FILE)

            # WHEN
            config = ConfigFile.load(config_file)

        # THEN
        assert config.worker.farm_id == "farm-1f0ece77172c441ebe295491a51cf6d5"
        assert config.worker.fleet_id == "fleet-c4a9481caa88404fa878a7fb98f8a4dd"
        assert config.worker.worker_persistence_dir == Path("/my/worker/persistence")

        assert config.aws.profile == "my_aws_profile_name"
        assert config.aws.allow_ec2_instance_profile is True

        assert config.logging.verbose is True
        assert config.logging.worker_logs_dir == Path("/var/log/amazon/deadline")
        assert config.logging.local_session_logs is False
        assert config.logging.host_metrics_logging is True
        assert config.logging.host_metrics_logging_interval_seconds == 1

        assert config.os.run_jobs_as_agent_user is False
        assert config.os.posix_job_user == "user:group"
        assert config.os.shutdown_on_stop is False
        assert config.os.retain_session_dir is False

        assert config.capabilities.amounts == {"amount.slots": 20, "deadline:amount.pets": 99}
        assert config.capabilities.attributes == {
            "attr.groups": ["simulation", "maya", "nuke"],
            "acmewidgetsco:attr.admins": ["bob", "alice"],
        }

    def test_config_load_toml_decode_error(
        self,
    ) -> None:
        """Tests that if the TOML parser raises a decode error, that the exception"""
        # GIVEN
        with (
            patch.object(ConfigFile, "get_config_path") as mock_get_config_path,
            patch.object(config_file_mod, "load_toml") as mock_load_toml,
            patch.object(ConfigFile, "parse_obj") as mock_parse_obj,
        ):
            config_path: MagicMock = mock_get_config_path.return_value
            config_path_open: MagicMock = config_path.open
            config_path_fh: MagicMock = config_path_open.return_value
            config_path_fh_ctx: MagicMock = config_path_fh.__enter__.return_value
            error_msg = "an error msg"
            toml_decode_error = TOMLDecodeError(error_msg)
            mock_load_toml.side_effect = toml_decode_error

            # THEN
            with pytest.raises(
                ConfigurationError, match=f"Configuration file (.*) is not valid TOML: {error_msg}"
            ) as raise_ctx:
                # WHEN
                ConfigFile.load()

        # THEN
        # assert that the raise exception chains the original TOMLDecodeError exception
        assert raise_ctx.value.__cause__ is toml_decode_error
        mock_get_config_path.assert_called_once_with()
        config_path_open.assert_called_once_with(mode="rb")
        mock_load_toml.assert_called_once_with(config_path_fh_ctx)
        mock_parse_obj.assert_not_called()

    def test_config_full_toml_as_settings(
        self,
    ) -> None:
        # GIVEN
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "config_file.toml"
            with config_file.open("w", encoding="utf-8") as fh:
                fh.write(FULL_CONFIG_FILE)
            config = ConfigFile.load(config_file)

        # WHEN
        settings = config.as_settings(BaseSettings())

        # THEN
        expected = {
            # worker
            "cleanup_session_user_processes": True,
            "farm_id": "farm-1f0ece77172c441ebe295491a51cf6d5",
            "fleet_id": "fleet-c4a9481caa88404fa878a7fb98f8a4dd",
            "worker_persistence_dir": Path("/my/worker/persistence"),
            # aws
            "profile": "my_aws_profile_name",
            "allow_instance_profile": True,
            # logging
            "verbose": True,
            "worker_logs_dir": Path("/var/log/amazon/deadline"),
            "local_session_logs": False,
            "host_metrics_logging": True,
            "host_metrics_logging_interval_seconds": 1,
            # os
            "run_jobs_as_agent_user": False,
            "posix_job_user": "user:group",
            "no_shutdown": True,  # opposite of 'shutdown_on_stop'
            "retain_session_dir": False,
            # capabilities
            "capabilities": Capabilities(
                amounts={"amount.slots": 20, "deadline:amount.pets": 99},
                attributes={
                    "attr.groups": ["simulation", "maya", "nuke"],
                    "acmewidgetsco:attr.admins": ["bob", "alice"],
                },
            ),
        }

        assert settings == expected
