# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


from unittest.mock import patch, MagicMock
import pytest

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.aws.deadline import (
    record_success_fail_telemetry_event,
    record_worker_start_telemetry_event,
    record_sync_inputs_telemetry_event,
    record_sync_outputs_telemetry_event,
    record_uncaught_exception_telemetry_event,
)
from deadline_worker_agent.capabilities import Capabilities
from deadline.job_attachments.progress_tracker import SummaryStatistics


def test_record_worker_start_telemetry_event():
    """
    Tests that when record_worker_start_telemetry_event() is called, the correct
    event type and details are passed to the telemetry client's record_event() method.
    """
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client

        # GIVEN
        caps = Capabilities(
            amounts={"amount.a": 2, "amount.b": 99},
            attributes={"attr.a": ["z"], "attr.b": ["c"]},
        )
        # WHEN
        record_worker_start_telemetry_event(caps)

    # THEN
    mock_telemetry_client.record_event.assert_called_with(
        event_type="com.amazon.rum.deadline.worker_agent.start",
        event_details={
            "amounts": {"amount.a": 2, "amount.b": 99},
            "attributes": {"attr.a": ["z"], "attr.b": ["c"]},
        },
    )


def test_record_sync_inputs_telemetry_event():
    """
    Tests that when record_sync_inputs_telemetry_event() is called, the correct
    event type and details are passed to the telemetry client's record_event() method.
    """
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client
        # GIVEN
        summary_stats = SummaryStatistics(
            total_time=8,
            total_files=10,
            total_bytes=1000,
            processed_files=8,
            processed_bytes=800,
            skipped_files=2,
            skipped_bytes=200,
            transfer_rate=100,
        )
        # WHEN
        record_sync_inputs_telemetry_event(
            queue_id="queue-test",
            summary=summary_stats,
        )

    # THEN
    mock_telemetry_client.record_event.assert_called_with(
        event_type="com.amazon.rum.deadline.worker_agent.sync_inputs_summary",
        event_details={
            "total_time": 8,
            "total_files": 10,
            "total_bytes": 1000,
            "processed_files": 8,
            "processed_bytes": 800,
            "skipped_files": 2,
            "skipped_bytes": 200,
            "transfer_rate": 100.0,
            "queue_id": "queue-test",
        },
    )


def test_record_sync_outputs_telemetry_event():
    """
    Tests that when record_sync_outputs_telemetry_event() is called, the correct
    event type and details are passed to the telemetry client's record_event() method.
    """
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client
        # GIVEN
        summary_stats = SummaryStatistics(
            total_time=8,
            total_files=10,
            total_bytes=1000,
            processed_files=8,
            processed_bytes=800,
            skipped_files=2,
            skipped_bytes=200,
            transfer_rate=100,
        )
        # WHEN
        record_sync_outputs_telemetry_event(
            queue_id="queue-test",
            summary=summary_stats,
        )

    # THEN
    mock_telemetry_client.record_event.assert_called_with(
        event_type="com.amazon.rum.deadline.worker_agent.sync_outputs_summary",
        event_details={
            "total_time": 8,
            "total_files": 10,
            "total_bytes": 1000,
            "processed_files": 8,
            "processed_bytes": 800,
            "skipped_files": 2,
            "skipped_bytes": 200,
            "transfer_rate": 100.0,
            "queue_id": "queue-test",
        },
    )


def test_record_uncaught_exception_telemetry_event():
    """
    Tests that when record_uncaught_exception_telemetry_event() is called, the correct
    event type and details are passed to the telemetry client's record_event() method.
    """
    # GIVEN
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client

        # WHEN
        error = ValueError()
        record_uncaught_exception_telemetry_event(str(type(error)))

    # THEN
    mock_telemetry_client.record_error.assert_called_with(
        exception_type="<class 'ValueError'>",
        event_details={"exception_scope": "uncaught"},
    )


def test_record_decorator_success():
    """Tests that recording a decorator successful metric"""
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client

        # GIVEN
        @record_success_fail_telemetry_event()
        def successful():
            return

        # WHEN
        successful()  # type: ignore

        # THEN
        mock_telemetry_client.record_event.assert_called_with(
            event_type="com.amazon.rum.deadline.worker_agent.successful",
            event_details={
                "is_success": True,
            },
        )


def test_record_decorator_fails():
    """Tests that recording a decorator failed metric"""
    mock_telemetry_client = MagicMock()

    with patch.object(deadline_mod, "_get_deadline_telemetry_client") as mock_get_telemetry_client:
        mock_get_telemetry_client.return_value = mock_telemetry_client

        # GIVEN
        @record_success_fail_telemetry_event()
        def fails():
            raise RuntimeError("foobar")

        # WHEN
        with pytest.raises(RuntimeError):
            fails()  # type: ignore

        # THEN
        mock_telemetry_client.record_event.assert_called_with(
            event_type="com.amazon.rum.deadline.worker_agent.fails",
            event_details={
                "is_success": False,
            },
        )
