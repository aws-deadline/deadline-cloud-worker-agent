# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch
import logging

import pytest

from deadline_worker_agent.api_models import (
    LogConfiguration as BotoLogConfiguration,
)
from deadline_worker_agent.sessions.log_config import LogConfiguration, LogProvisioningError
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
