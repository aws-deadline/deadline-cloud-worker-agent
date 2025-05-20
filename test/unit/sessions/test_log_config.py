# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch
import logging

import pytest
import json

from deadline_worker_agent.api_models import (
    LogConfiguration as BotoLogConfiguration,
)
from deadline_worker_agent.sessions.log_config import (
    LogConfiguration,
    LogProvisioningError,
    ActionOutputCaptureFilter,
)
import deadline_worker_agent.sessions.log_config as log_config_mod


class TestLogProvisioningError:
    """Tests for the LogProvisioningError class"""

    @pytest.mark.parametrize(
        argnames="message",
        argvalues=("msg1", "msg2"),
    )
    def test_str(self, message: str):
        """Test that the LogProvisioningError.__str__ returns the format:

        Log provisioning error: {message}
        """

        # GIVEN
        log_provisioning_error = LogProvisioningError(message=message)

        # WHEN
        str_rep = str(log_provisioning_error)

        # THEN
        assert str_rep == f"Log provisioning error: {message}"


class TestLogConfiguration:
    """Tests for the LogConfiguration class"""

    def test_from_boto_local_log_setup(
        self,
        tmp_path: Path,
    ) -> None:
        """Tests that when using the LogConfiguration.log_session() return value as a
        context-manager that...

        On enter:

            -   A logging.FileHandler is created corresponding to the passed-in session_log_file
                Path
            -   The handler is attached to the supplied loggers
            -   A formatter is attached to the handler to output timestamp, level, and message

        On exit:

            -   The created logging.FileHandler is removed from the supplied loggers
            -   The formatter is removed from the handler
        """

        # GIVEN
        session_log_file = tmp_path / "session-log.txt"
        loggers: list[logging.Logger] = [MagicMock(), MagicMock()]
        boto_log_configuration = BotoLogConfiguration(
            logDriver="awslogs",
            options={
                "logGroupName": "lg",
                "logStreamName": "ls",
            },
            parameters={
                "interval": "15",
            },
        )
        log_config = LogConfiguration.from_boto(
            loggers=loggers,
            log_configuration=boto_log_configuration,
            session_log_file=session_log_file,
        )

        with (
            patch.object(log_config_mod.logging, "FileHandler") as mock_file_handler_cls,
            patch.object(log_config_mod.logging, "Formatter") as mock_formatter_cls,
        ):
            # WHEN
            with log_config.log_session(
                queue_id="queue-1234",
                job_id="job-1234",
                session_id="some-session",
                boto_session=MagicMock(),
            ):
                # THEN
                mock_file_handler_cls.assert_called_once_with(filename=session_log_file)
                local_file_handler: MagicMock = mock_file_handler_cls.return_value

                # Formatter
                mock_formatter_cls.assert_any_call("%(asctime)s %(levelname)s %(message)s")
                formatter: MagicMock = mock_formatter_cls("%(asctime)s %(levelname)s %(message)s")

                for logger in loggers:
                    add_handler_mock: MagicMock = cast(MagicMock, logger).addHandler
                    add_handler_mock.assert_any_call(local_file_handler)

                # WHEN (exiting context manager)
            # THEN
            for logger in loggers:
                logger_mock = cast(MagicMock, logger)

                remove_handler_mock: MagicMock = logger_mock.removeHandler
                remove_handler_mock.assert_any_call(local_file_handler)

                set_formatter_mock: MagicMock = local_file_handler.setFormatter
                set_formatter_mock.assert_called_once_with(formatter)

    @pytest.mark.parametrize(
        argnames="log_provision_error_msg",
        argvalues=(
            "msg1",
            "msg2",
        ),
    )
    def test_from_boto_log_provision_error(
        self,
        log_provision_error_msg: str,
        tmp_path: Path,
    ) -> None:
        """Tests that if the passed in BotoSessionLogConfiguration instance contains an error
        message in the "error" field, that a LogProvisioningError is raised with the corresponding
        error message"""

        # GIVEN
        loggers: list[logging.Logger] = []
        boto_log_configuration = BotoLogConfiguration(
            error=log_provision_error_msg,
            logDriver="awslogs",
            options={},
            parameters={
                "interval": "15",
            },
        )

        # THEN
        with pytest.raises(LogProvisioningError) as raise_ctx:
            # WHEN
            LogConfiguration.from_boto(
                loggers=loggers,
                log_configuration=boto_log_configuration,
                session_log_file=tmp_path / "session-log.txt",
            )

        # THEN
        assert raise_ctx.value.message == log_provision_error_msg


class TestActionOutputCaptureFilter:
    """Tests for the ActionOutputCaptureFilter class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.logger = logging.getLogger("test_logger")
        self.logger.setLevel(logging.INFO)
        # Clear any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        # Create a handler that will capture log records
        self.log_handler = logging.StreamHandler()
        self.logger.addHandler(self.log_handler)
        # Default session ID for tests
        self.session_id = "test-session-id"
        self.snapshot_result = {"root": "test-root", "manifest": "test-manifest"}

    def test_filter_with_session_id_mismatch(self):
        """Test that the filter ignores records with non-matching session_id"""
        # Create a mock callback
        mock_callback = MagicMock()

        # Create the filter with the callback
        action_output_capture_filter = ActionOutputCaptureFilter(
            session_id=self.session_id, callback=mock_callback
        )
        self.log_handler.addFilter(action_output_capture_filter)

        # Create a log record with non-matching session_id
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"ja_snapshot: {json.dumps(self.snapshot_result)}",
            args=(),
            exc_info=None,
        )
        setattr(record, "session_id", "different-session-id")

        # Filter the record
        result = action_output_capture_filter.filter(record)

        # Verify the filter returns True but doesn't process the message
        assert result is True
        mock_callback.assert_not_called()

    def test_filter_with_non_string_message(self):
        """Test that the filter handles non-string messages correctly"""
        # Create a mock callback
        mock_callback = MagicMock()

        # Create the filter with the callback
        action_output_capture_filter = ActionOutputCaptureFilter(
            session_id=self.session_id, callback=mock_callback
        )
        self.log_handler.addFilter(action_output_capture_filter)

        # Create a log record with a non-string message
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg={"key": "value"},  # Non-string message
            args=(),
            exc_info=None,
        )
        setattr(record, "session_id", self.session_id)

        # Filter the record
        result = action_output_capture_filter.filter(record)

        # Verify the filter returns True but doesn't process the message
        assert result is True
        mock_callback.assert_not_called()

    def test_filter_regex_pattern_matching(self):
        """Test that the filter correctly matches the regex pattern for ActionOutputMessageKind"""
        # Create a mock callback
        mock_callback = MagicMock()

        # Create the filter with the callback
        action_output_capture_filter = ActionOutputCaptureFilter(
            session_id=self.session_id, callback=mock_callback
        )

        # Create log records with the ActionOutputMessageKind values as prefixes
        ja_snapshot_record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"ja_snapshot: {json.dumps(self.snapshot_result)}",
            args=(),
            exc_info=None,
        )
        setattr(ja_snapshot_record, "session_id", self.session_id)

        ja_upload_record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="ja_upload: This is an upload message",
            args=(),
            exc_info=None,
        )
        setattr(ja_upload_record, "session_id", self.session_id)

        # Test that the filter correctly matches the patterns
        assert action_output_capture_filter.filter(ja_snapshot_record) is True
        assert action_output_capture_filter.filter(ja_upload_record) is True

    def test_filter_regex_pattern_matching_with_handler(self):
        """Test that the filter correctly processes messages with the regex pattern and calls the handler"""
        # Create a mock callback
        mock_callback = MagicMock()

        # Create the filter with the callback
        action_output_capture_filter = ActionOutputCaptureFilter(
            session_id=self.session_id, callback=mock_callback
        )

        # Create a log record with the ja_snapshot prefix
        ja_snapshot_record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"ja_snapshot: {json.dumps(self.snapshot_result)}",
            args=(),
            exc_info=None,
        )
        setattr(ja_snapshot_record, "session_id", self.session_id)

        # Filter the record
        action_output_capture_filter.filter(ja_snapshot_record)

        # Verify the callback was called with the correct arguments
        # The third parameter is a boolean that we're not testing here
        mock_callback.assert_called_once_with(
            log_config_mod.ActionOutputMessageKind.JA_SNAPSHOT,
            self.snapshot_result,
        )

    def test_handler_error_handling(self):
        """Test that errors in handlers are properly handled"""
        # Create a mock callback that raises an exception
        mock_callback = MagicMock(side_effect=ValueError("Test error"))

        # Create the filter with the callback
        action_output_capture_filter = ActionOutputCaptureFilter(
            session_id=self.session_id, callback=mock_callback
        )

        # Create a log record with the ja_snapshot prefix
        ja_snapshot_record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"ja_snapshot: {json.dumps(self.snapshot_result)}",
            args=(),
            exc_info=None,
        )
        setattr(ja_snapshot_record, "session_id", self.session_id)

        # Filter the record - this should not raise an exception
        result = action_output_capture_filter.filter(ja_snapshot_record)

        # Verify the filter returns True and the error is appended to the message
        assert result is True
        assert "ERROR:" in ja_snapshot_record.msg

    def test_multiple_matched_groups_handling(self):
        """Test handling of multiple matched groups in the regex pattern"""
        # Create a mock callback
        mock_callback = MagicMock()

        # Create the filter with the callback and a patched _FILTER_MATCHER that would match multiple groups
        with patch.object(ActionOutputCaptureFilter, "_FILTER_MATCHER") as mock_matcher:
            # Configure the mock to simulate multiple matched groups
            mock_match = MagicMock()
            mock_match.lastindex = 2
            mock_match.group.return_value = "Test message"
            mock_match.groupdict.return_value = {
                "ja_snapshot": "ja_snapshot",
                "ja_upload": "ja_upload",
            }
            mock_matcher.match.return_value = mock_match

            action_output_capture_filter = ActionOutputCaptureFilter(
                session_id=self.session_id, callback=mock_callback
            )

            # Create a log record
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="ja_snapshot: ja_upload: Test message",
                args=(),
                exc_info=None,
            )
            setattr(record, "session_id", self.session_id)

            # Filter the record
            result = action_output_capture_filter.filter(record)

            # Verify the filter returns True but doesn't process the message due to multiple matches
            assert result is True
            mock_callback.assert_not_called()
