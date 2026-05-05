# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import platform
import time
import uuid
from queue import Full
from unittest.mock import MagicMock, patch
from urllib import request

import pytest

from deadline_worker_agent.telemetry import (
    TelemetryClient,
    TelemetryEvent,
    _swallow_exceptions,
)


@pytest.fixture
def mock_telemetry_client():
    """Creates a TelemetryClient with threads and network mocked out."""
    with patch.object(TelemetryClient, "_start_threads"), patch(
        "deadline_worker_agent.telemetry.boto3.client"
    ) as mock_boto_client:
        mock_boto_client.return_value.meta.endpoint_url = "https://fake-endpoint-url"
        client = TelemetryClient(
            package_name="deadline-cloud-worker-agent",
            package_ver="1.2.3.4567",
        )
        assert client._initialized
        return client


class TestSwallowExceptions:
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


class TestOptOut:
    def test_opt_out_env_var(self, monkeypatch):
        """Telemetry client doesn't initialize if env var is set."""
        monkeypatch.setenv("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", "true")
        with patch.object(TelemetryClient, "_start_threads"):
            client = TelemetryClient("test-package", "1.0.0")
        assert not client._initialized
        assert client.telemetry_opted_out

    @pytest.mark.parametrize("env_var_value", ["true", "1", "yes", "on"])
    def test_opt_out_env_var_values(self, monkeypatch, env_var_value):
        """Various truthy env var values all opt out."""
        monkeypatch.setenv("DEADLINE_CLOUD_TELEMETRY_OPT_OUT", env_var_value)
        with patch.object(TelemetryClient, "_start_threads"):
            client = TelemetryClient("test-package", "1.0.0")
        assert client.telemetry_opted_out

    def test_opt_out_worker_config(self):
        """Telemetry client doesn't initialize if worker config has opt_out=true."""
        mock_config_file = MagicMock()
        mock_config_file.telemetry.opt_out = True

        with patch.object(TelemetryClient, "_start_threads"), patch(
            "deadline_worker_agent.telemetry.os.environ.get", return_value=""
        ), patch(
            "deadline_worker_agent.telemetry.TelemetryClient._read_opt_out_from_config",
            return_value=True,
        ):
            client = TelemetryClient("test-package", "1.0.0")
        assert client.telemetry_opted_out
        assert not client._initialized

    def test_not_opted_out_records_events(self, mock_telemetry_client):
        """When not opted out, events are queued."""
        queue_mock = MagicMock()
        mock_telemetry_client.event_queue = queue_mock

        mock_telemetry_client.record_event(
            event_type="com.amazon.rum.deadline.test", event_details={"key": "value"}
        )

        queue_mock.put_nowait.assert_called_once()

    def test_opted_out_does_not_record(self, mock_telemetry_client):
        """When opted out, events are not queued."""
        mock_telemetry_client.telemetry_opted_out = True
        queue_mock = MagicMock()
        mock_telemetry_client.event_queue = queue_mock

        mock_telemetry_client.record_event(
            event_type="com.amazon.rum.deadline.test", event_details={}
        )

        queue_mock.put_nowait.assert_not_called()


class TestInitialize:
    def test_initialize_failure_then_success(self):
        """A failure in initialize keeps _initialized False; retrying succeeds."""
        with patch.object(TelemetryClient, "_start_threads"), patch(
            "deadline_worker_agent.telemetry.boto3.client"
        ) as mock_boto_client:
            mock_boto_client.side_effect = [
                Exception("Boto3 blew up!"),
                MagicMock(meta=MagicMock(endpoint_url="https://fake-endpoint-url")),
            ]
            client = TelemetryClient(
                package_name="test-package",
                package_ver="1.0.0",
            )
            assert not client._initialized

            # Retry
            mock_boto_client.side_effect = None
            mock_boto_client.return_value.meta.endpoint_url = "https://fake-endpoint-url"
            client._initialize()
            assert client._initialized
            assert client.endpoint == "https://management.fake-endpoint-url/2023-10-12/telemetry"

    def test_initialize_swallows_exception(self, mock_telemetry_client):
        """_initialize doesn't propagate exceptions."""
        mock_telemetry_client._initialized = False
        mock_telemetry_client.telemetry_opted_out = False
        with patch(
            "deadline_worker_agent.telemetry.boto3.client", side_effect=RuntimeError("boom")
        ):
            mock_telemetry_client._initialize()
        assert not mock_telemetry_client._initialized


class TestGetPrefixedEndpoint:
    @pytest.mark.parametrize(
        "endpoint,prefix,expected",
        [
            ("test.endpoint.url", "", "test.endpoint.url"),
            ("test.endpoint.url", "management.", "test.endpoint.url"),
            (
                "https://test.endpoint.url",
                "management.",
                "https://management.test.endpoint.url",
            ),
        ],
    )
    def test_get_prefixed_endpoint(self, mock_telemetry_client, endpoint, prefix, expected):
        assert mock_telemetry_client._get_prefixed_endpoint(endpoint, prefix) == expected


class TestProcessEventQueueThread:
    @pytest.mark.timeout(5)
    def test_exits_on_none(self, mock_telemetry_client):
        """Queue processing thread exits cleanly after getting None."""
        queue_mock = MagicMock()
        queue_mock.get.side_effect = [TelemetryEvent(), None]
        mock_telemetry_client.event_queue = queue_mock

        with patch.object(request, "urlopen"):
            mock_telemetry_client._process_event_queue_thread()

        assert queue_mock.get.call_count == 2

    @pytest.mark.parametrize(
        "http_code,expected_attempts",
        [
            (400, 1),
            (429, TelemetryClient.MAX_RETRY_ATTEMPTS),
            (500, TelemetryClient.MAX_RETRY_ATTEMPTS),
        ],
    )
    @pytest.mark.timeout(5)
    def test_retries_and_exits(self, mock_telemetry_client, http_code, expected_attempts):
        """Thread retries on 429/500 and exits on other errors."""
        http_error = request.HTTPError(
            "http://test.com", http_code, "Http Error", {}, None  # type: ignore
        )
        queue_mock = MagicMock()
        queue_mock.get.side_effect = [TelemetryEvent(), None]
        mock_telemetry_client.event_queue = queue_mock

        with patch.object(request, "urlopen", side_effect=http_error), patch.object(
            time, "sleep"
        ):
            mock_telemetry_client._process_event_queue_thread()

        # On non-retryable errors, only 1 event is dequeued (thread exits)
        assert queue_mock.get.call_count == 1

    @pytest.mark.timeout(5)
    def test_handles_unexpected_error(self, mock_telemetry_client):
        """Thread exits cleanly on unexpected exceptions."""
        queue_mock = MagicMock()
        queue_mock.get.side_effect = [TelemetryEvent(), None]
        mock_telemetry_client.event_queue = queue_mock

        with patch.object(request, "urlopen", side_effect=Exception("Some error")):
            mock_telemetry_client._process_event_queue_thread()

        assert queue_mock.get.call_count == 1


class TestRecordEvent:
    def test_record_error(self, mock_telemetry_client):
        """Recording an error sends the expected event."""
        queue_mock = MagicMock()
        mock_telemetry_client.event_queue = queue_mock

        mock_telemetry_client.record_error({"some_field": "value"}, "RuntimeError")

        queue_mock.put_nowait.assert_called_once()
        event = queue_mock.put_nowait.call_args[0][0]
        assert event.event_type == "com.amazon.rum.deadline.error"
        assert event.event_details["exception_type"] == "RuntimeError"
        assert event.event_details["some_field"] == "value"

    def test_record_event_includes_common_details(self, mock_telemetry_client):
        """Common details are merged into event details."""
        queue_mock = MagicMock()
        mock_telemetry_client.event_queue = queue_mock
        mock_telemetry_client.update_common_details({"common_key": "common_value"})

        mock_telemetry_client.record_event(
            event_type="com.amazon.rum.deadline.test",
            event_details={"event_key": "event_value"},
        )

        event = queue_mock.put_nowait.call_args[0][0]
        assert event.event_details["common_key"] == "common_value"
        assert event.event_details["event_key"] == "event_value"

    def test_record_event_swallows_exception(self, mock_telemetry_client):
        """record_event doesn't propagate exceptions."""
        with patch.object(
            mock_telemetry_client, "_put_telemetry_record", side_effect=RuntimeError("boom")
        ):
            # Should not raise
            mock_telemetry_client.record_event(
                event_type="com.amazon.rum.deadline.test", event_details={}
            )


class TestExitCleanly:
    def test_exit_cleanly_swallows_exception(self, mock_telemetry_client):
        """_exit_cleanly doesn't propagate exceptions from a full queue."""
        mock_telemetry_client.event_queue = MagicMock()
        mock_telemetry_client.event_queue.put_nowait.side_effect = Full()
        mock_telemetry_client.processing_thread = MagicMock()

        # Should not raise
        mock_telemetry_client._exit_cleanly()


class TestSystemMetadata:
    def test_get_system_metadata(self, mock_telemetry_client):
        """System metadata contains expected fields."""
        metadata = mock_telemetry_client._get_system_metadata()
        assert metadata["service"] == "deadline-cloud-worker-agent"
        assert metadata["version"] == "1.2.3"
        assert "python_version" in metadata
        assert "osName" in metadata
        assert "osVersion" in metadata

    def test_get_system_metadata_macos(self, mock_telemetry_client):
        """Darwin is reported as macOS."""
        with patch.object(
            platform, "uname", return_value=MagicMock(system="Darwin", release="23.0.0")
        ):
            metadata = mock_telemetry_client._get_system_metadata()
        assert metadata["osName"] == "macOS"


class TestGetTelemetryIdentifier:
    def test_uses_existing_valid_uuid(self):
        """Uses existing identifier if it's a valid UUID4."""
        test_id = str(uuid.uuid4())
        with patch(
            "deadline_worker_agent.telemetry._get_setting", return_value=test_id
        ), patch.object(TelemetryClient, "_start_threads"), patch(
            "deadline_worker_agent.telemetry.boto3.client"
        ) as mock_boto:
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient("test", "1.0.0")
        assert client.telemetry_id == test_id

    def test_generates_new_uuid_if_invalid(self):
        """Generates a new UUID if the stored one is invalid."""
        with patch(
            "deadline_worker_agent.telemetry._get_setting", return_value="not-a-uuid"
        ), patch.object(TelemetryClient, "_start_threads"), patch(
            "deadline_worker_agent.telemetry.boto3.client"
        ) as mock_boto:
            mock_boto.return_value.meta.endpoint_url = "https://fake"
            client = TelemetryClient("test", "1.0.0")
        # Should be a valid UUID4
        uuid.UUID(client.telemetry_id, version=4)
        assert client.telemetry_id != "not-a-uuid"
