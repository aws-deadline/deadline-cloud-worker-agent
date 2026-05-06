# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import platform
import pytest
import uuid
import time

from unittest.mock import patch, MagicMock
from urllib import request

from deadline_worker_agent.telemetry import (
    TelemetryClient,
    TelemetryEvent,
    _swallow_exceptions,
)


@pytest.fixture(scope="function", name="mock_telemetry_client")
def fixture_telemetry_client():
    with (
        patch.object(TelemetryClient, "_start_threads"),
        patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto_client,
    ):
        mock_boto_client.return_value.meta.endpoint_url = "https://fake-endpoint-url"
        client = TelemetryClient(
            package_name="deadline-cloud-worker-agent",
            package_ver="1.2.3.4567",
        )
        assert client.is_initialized
        return client


def test_opt_out_config():
    """Ensures the telemetry client doesn't fully initialize if the opt out config setting is set"""
    with (
        patch.object(TelemetryClient, "_start_threads"),
        patch(
            "deadline_worker_agent.telemetry.TelemetryClient._read_opt_out_from_config",
            return_value=True,
        ),
    ):
        client = TelemetryClient("deadline-cloud-worker-agent", "1.0.0")
    assert not client.is_initialized
    assert not hasattr(client, "endpoint")
    assert not hasattr(client, "event_queue")
    assert not hasattr(client, "processing_thread")
    # Ensure nothing blows up if we try recording telemetry after we've opted out
    client.record_error({}, str(type(Exception)))


@pytest.mark.parametrize(
    "env_var_value",
    [
        pytest.param("true"),
        pytest.param("1"),
        pytest.param("yes"),
        pytest.param("on"),
    ],
)
def test_opt_out_env_var(monkeypatch, env_var_value):
    """Ensures the telemetry client doesn't fully initialize if the opt out env var is set"""
    monkeypatch.setenv("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", env_var_value)
    with patch.object(TelemetryClient, "_start_threads"):
        client = TelemetryClient("deadline-cloud-worker-agent", "1.0.0")
    assert not client.is_initialized
    assert not hasattr(client, "endpoint")
    assert not hasattr(client, "event_queue")
    assert not hasattr(client, "processing_thread")
    # Ensure nothing blows up if we try recording telemetry after we've opted out
    client.record_error({}, str(type(Exception)))


def test_initialize_failure_then_success():
    """
    Tests that a failure in initializing keeps the property as false, but trying again
    without an exception initializes everything successfully.
    """
    with (
        patch.object(TelemetryClient, "_start_threads"),
        patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto_client,
    ):
        mock_boto_client.side_effect = Exception("Boto3 blew up!")
        client = TelemetryClient(
            package_name="deadline-cloud-worker-agent",
            package_ver="1.2.3.4567",
        )

        assert not client.is_initialized
        assert not hasattr(client, "endpoint")
        assert not hasattr(client, "event_queue")
        assert not hasattr(client, "processing_thread")

        mock_boto_client.side_effect = None
        mock_boto_client.return_value.meta.endpoint_url = "https://fake-endpoint-url"
        client.initialize()
        assert client.is_initialized
        assert client.endpoint == "https://management.fake-endpoint-url/2023-10-12/telemetry"


def test_get_telemetry_identifier():
    """Ensures that getting the telemetry identifier handles empty/malformed strings"""
    with (
        patch.object(TelemetryClient, "_start_threads"),
        patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto_client,
        patch(
            "deadline_worker_agent.config.config_file.ConfigFile.load",
            side_effect=FileNotFoundError,
        ),
        patch("deadline_worker_agent.telemetry._get_setting", return_value=""),
    ):
        mock_boto_client.return_value.meta.endpoint_url = "https://fake"
        client = TelemetryClient("deadline-cloud-worker-agent", "1.0.0")

    # Should have generated a valid UUID
    uuid.UUID(client.telemetry_id, version=4)


def test_get_telemetry_identifier_uses_existing():
    """Uses existing identifier from worker config if valid."""
    test_id = str(uuid.uuid4())
    mock_config = MagicMock()
    mock_config.telemetry.identifier = test_id

    with (
        patch.object(TelemetryClient, "_start_threads"),
        patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto_client,
        patch(
            "deadline_worker_agent.config.config_file.ConfigFile.load",
            return_value=mock_config,
        ),
    ):
        mock_boto_client.return_value.meta.endpoint_url = "https://fake"
        client = TelemetryClient("deadline-cloud-worker-agent", "1.0.0")

    assert client.telemetry_id == test_id


class TestFallbackMechanism:
    """Tests for the legacy config fallback and persistence to worker.toml"""

    def test_opt_out_from_worker_toml(self):
        """opt_out in worker.toml is used directly without fallback."""
        mock_config = MagicMock()
        mock_config.telemetry.opt_out = True

        with patch(
            "deadline_worker_agent.config.config_file.ConfigFile.load",
            return_value=mock_config,
        ):
            result = TelemetryClient._read_opt_out_from_config()

        assert result is True

    def test_opt_out_falls_back_to_legacy_config(self):
        """When worker.toml has no opt_out, reads from ~/.deadline/config."""
        mock_config = MagicMock()
        mock_config.telemetry.opt_out = None

        with (
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                return_value=mock_config,
            ),
            patch(
                "deadline_worker_agent.telemetry._get_setting", return_value="true"
            ) as mock_get_setting,
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.modify_config_file_settings",
            ) as mock_modify,
        ):
            result = TelemetryClient._read_opt_out_from_config()

        assert result is True
        mock_get_setting.assert_called_once_with("telemetry.opt_out")
        mock_modify.assert_called_once()

    def test_opt_out_legacy_false_does_not_persist(self):
        """When legacy config has opt_out=false, does not write to worker.toml."""
        mock_config = MagicMock()
        mock_config.telemetry.opt_out = None

        with (
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                return_value=mock_config,
            ),
            patch("deadline_worker_agent.telemetry._get_setting", return_value="false"),
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.modify_config_file_settings",
            ) as mock_modify,
        ):
            result = TelemetryClient._read_opt_out_from_config()

        assert result is False
        mock_modify.assert_not_called()

    def test_opt_out_no_worker_toml_falls_back(self):
        """When worker.toml doesn't exist, falls back to legacy config."""
        with (
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                side_effect=FileNotFoundError,
            ),
            patch("deadline_worker_agent.telemetry._get_setting", return_value="true"),
        ):
            result = TelemetryClient._read_opt_out_from_config()

        assert result is True

    def test_identifier_from_legacy_config_persists_to_worker_toml(self):
        """Identifier from legacy config is persisted to worker.toml."""
        test_id = str(uuid.uuid4())

        with (
            patch.object(TelemetryClient, "_start_threads"),
            patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto,
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                side_effect=FileNotFoundError,
            ),
            patch("deadline_worker_agent.telemetry._get_setting", return_value=test_id),
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.modify_config_file_settings",
            ) as mock_modify,
        ):
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient("test", "1.0.0")

        assert client.telemetry_id == test_id
        mock_modify.assert_called_once()

    def test_identifier_generated_when_legacy_invalid(self):
        """A new UUID is generated when legacy config has invalid identifier."""
        with (
            patch.object(TelemetryClient, "_start_threads"),
            patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto,
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                side_effect=FileNotFoundError,
            ),
            patch("deadline_worker_agent.telemetry._get_setting", return_value="not-a-uuid"),
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.modify_config_file_settings",
            ) as mock_modify,
        ):
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient("test", "1.0.0")

        assert client.telemetry_id != "not-a-uuid"
        uuid.UUID(client.telemetry_id, version=4)
        mock_modify.assert_called_once()

    def test_identifier_from_worker_toml_does_not_persist(self):
        """When identifier exists in worker.toml, no write occurs."""
        test_id = str(uuid.uuid4())
        mock_config = MagicMock()
        mock_config.telemetry.identifier = test_id

        with (
            patch.object(TelemetryClient, "_start_threads"),
            patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto,
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                return_value=mock_config,
            ),
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.modify_config_file_settings",
            ) as mock_modify,
        ):
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient("test", "1.0.0")

        assert client.telemetry_id == test_id
        mock_modify.assert_not_called()


@pytest.mark.timeout(5)  # Timeout in case we don't exit the while loop
def test_process_event_queue_thread(mock_telemetry_client):
    """Test that the queue processing thread function exits cleanly after getting None"""
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    with (
        patch.object(request, "urlopen"),
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch("deadline_worker_agent.telemetry.boto3.Session"),
    ):
        mock_telemetry_client._process_event_queue_thread()
    assert queue_mock.get.call_count == 2


@pytest.mark.parametrize(
    "http_code,attempt_count",
    [
        (400, 1),
        (429, TelemetryClient.MAX_RETRY_ATTEMPTS),
        (500, TelemetryClient.MAX_RETRY_ATTEMPTS),
    ],
)
@pytest.mark.timeout(5)
def test_process_event_queue_thread_retries_and_exits(
    mock_telemetry_client, http_code, attempt_count
):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    http_error = request.HTTPError("http://test.com", http_code, "Http Error", {}, None)  # type: ignore
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    with (
        patch.object(request, "urlopen", side_effect=http_error),
        patch.object(time, "sleep"),
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch("deadline_worker_agent.telemetry.boto3.Session"),
    ):
        mock_telemetry_client._process_event_queue_thread()
    assert queue_mock.get.call_count == 1


@pytest.mark.timeout(5)
def test_process_event_queue_thread_handles_unexpected_error(mock_telemetry_client):
    """Test that the thread exits cleanly after getting an unexpected exception"""
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [TelemetryEvent(), None]
    mock_telemetry_client.event_queue = queue_mock
    with (
        patch.object(request, "urlopen", side_effect=Exception("Some error")),
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch("deadline_worker_agent.telemetry.boto3.Session"),
    ):
        mock_telemetry_client._process_event_queue_thread()
    assert queue_mock.get.call_count == 1


def test_record_error(mock_telemetry_client):
    """Test that recording an error sends the expected TelemetryEvent to the thread queue"""
    queue_mock = MagicMock()
    test_error_details = {"some_field": "some_value"}
    test_exc = Exception("some exception")
    expected_event = TelemetryEvent(
        event_type="com.amazon.rum.deadline.error",
        event_details={
            "some_field": "some_value",
            "exception_type": str(type(test_exc)),
        },
    )
    mock_telemetry_client.event_queue = queue_mock
    mock_telemetry_client.record_error(test_error_details, str(type(test_exc)))
    queue_mock.put_nowait.assert_called_once_with(expected_event)


@pytest.mark.parametrize(
    "endpoint,prefix,expected_result",
    [
        pytest.param(
            "test.endpoint.url",
            "",
            "test.endpoint.url",
            id="The endpoint is not prefixed if the prefix is empty.",
        ),
        pytest.param(
            "test.endpoint.url",
            "management.",
            "test.endpoint.url",
            id="The endpoint is not prefixed if the endpoint does not start with 'https://'.",
        ),
        pytest.param(
            "https://test.endpoint.url",
            "management.",
            "https://management.test.endpoint.url",
            id="The prefix is inserted right after 'https://'.",
        ),
    ],
)
def test_get_prefixed_endpoint(
    mock_telemetry_client: TelemetryClient,
    endpoint: str,
    prefix: str,
    expected_result: str,
):
    """Test that the _get_prefixed_endpoint function returns the expected prefixed endpoint"""
    assert mock_telemetry_client._get_prefixed_endpoint(endpoint, prefix) == expected_result


@pytest.mark.timeout(5)
def test_process_event_queue_thread_merges_common_details_into_payload(mock_telemetry_client):
    """Common details are merged into the event payload at send time."""
    mock_telemetry_client.update_common_details({"common_key": "common_value"})
    queue_mock = MagicMock()
    queue_mock.get.side_effect = [
        TelemetryEvent(
            event_type="com.amazon.rum.deadline.test",
            event_details={"probe": 1},
        ),
        None,
    ]
    mock_telemetry_client.event_queue = queue_mock

    with (
        patch.object(request, "urlopen") as urlopen_mock,
        patch.object(TelemetryClient, "get_account_id", return_value=None),
        patch("deadline_worker_agent.telemetry.boto3.Session"),
    ):
        mock_telemetry_client._process_event_queue_thread()

    assert urlopen_mock.call_count == 1
    sent_request = urlopen_mock.call_args[0][0]
    body = json.loads(sent_request.data.decode("utf-8"))
    details = json.loads(body["RumEvents"][0]["details"])
    assert details["common_key"] == "common_value"
    assert details["probe"] == 1


class TestSwallowExceptionsDecorator:
    """Tests for the _swallow_exceptions decorator"""

    def test_returns_value_on_success(self):
        @_swallow_exceptions
        def succeeds():
            return 42

        assert succeeds() == 42

    def test_returns_none_on_exception(self):
        @_swallow_exceptions
        def fails():
            raise RuntimeError("boom")

        assert fails() is None

    def test_logs_exception(self):
        @_swallow_exceptions
        def fails():
            raise RuntimeError("boom")

        with patch("deadline_worker_agent.telemetry.logger") as mock_logger:
            fails()
            mock_logger.debug.assert_called_once()
            assert "fails" in mock_logger.debug.call_args[0][1]

    def test_preserves_function_name(self):
        @_swallow_exceptions
        def my_func():
            pass

        assert my_func.__name__ == "my_func"


class TestTelemetryClientSwallowExceptions:
    """Tests that decorated TelemetryClient methods don't propagate exceptions"""

    def test_set_opt_out_swallows_exception(self, mock_telemetry_client):
        with patch(
            "deadline_worker_agent.telemetry.os.environ.get", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client.set_opt_out()

    def test_initialize_swallows_exception(self, mock_telemetry_client):
        mock_telemetry_client._initialized = False
        mock_telemetry_client.telemetry_opted_out = False
        with patch(
            "deadline_worker_agent.telemetry.boto3.client", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client.initialize()
        assert not mock_telemetry_client.is_initialized

    def test_record_event_swallows_exception(self, mock_telemetry_client):
        with patch.object(
            mock_telemetry_client, "_put_telemetry_record", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client.record_event(
                event_type="com.amazon.rum.deadline.test",
                event_details={},
            )

    def test_exit_cleanly_swallows_exception(self, mock_telemetry_client):
        mock_telemetry_client.event_queue = MagicMock()
        mock_telemetry_client.event_queue.put_nowait.side_effect = RuntimeError("boom")
        mock_telemetry_client._exit_cleanly()

    def test_init_swallows_get_telemetry_identifier_exception(self):
        with (
            patch.object(TelemetryClient, "_start_threads"),
            patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto,
            patch(
                "deadline_worker_agent.config.config_file.ConfigFile.load",
                side_effect=RuntimeError("boom"),
            ),
            patch("deadline_worker_agent.telemetry._get_setting", side_effect=RuntimeError("boom")),
        ):
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient(
                package_name="deadline-cloud-worker-agent",
                package_ver="1.0.0",
            )
            assert client.telemetry_id is not None

    def test_init_swallows_get_system_metadata_exception(self):
        with (
            patch.object(TelemetryClient, "_start_threads"),
            patch("deadline_worker_agent.telemetry.boto3.client") as mock_boto,
            patch.object(platform, "uname", side_effect=RuntimeError("boom")),
        ):
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient(
                package_name="deadline-cloud-worker-agent",
                package_ver="1.0.0",
            )
            assert "version" not in client._system_metadata


class TestGetAccountId:
    """Tests for the background-thread account ID resolution."""

    def test_prefers_credential_account_id(self, mock_telemetry_client):
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = "111122223333"
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) == "111122223333"
        session_mock.client.assert_not_called()

    def test_falls_back_to_sts_when_credentials_lack_account_id(self, mock_telemetry_client):
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = None
        session_mock.client.return_value.get_caller_identity.return_value = {
            "Account": "444455556666"
        }
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) == "444455556666"

    def test_returns_none_when_sts_unreachable(self, mock_telemetry_client):
        session_mock = MagicMock()
        session_mock.get_credentials.return_value.account_id = None
        session_mock.client.return_value.get_caller_identity.side_effect = Exception(
            "STS unreachable"
        )
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) is None

    def test_returns_none_when_no_credentials(self, mock_telemetry_client):
        session_mock = MagicMock()
        session_mock.get_credentials.return_value = None
        session_mock.client.return_value.get_caller_identity.side_effect = Exception(
            "no creds, no STS"
        )
        mock_telemetry_client.get_account_id.cache_clear()
        assert mock_telemetry_client.get_account_id(session_mock) is None
