# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import MagicMock, patch
from datetime import timedelta, datetime, timezone

from deadline_worker_agent.aws_credentials.boto3_sessions import SettableCredentials
from deadline_worker_agent.aws_credentials.credentials_refresher import AwsCredentialsRefresher
import deadline_worker_agent.aws_credentials.credentials_refresher as refresher_mod
from deadline_worker_agent.aws.deadline import (
    DeadlineRequestInterrupted,
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestError,
)


class TestAwsCredentialRefresherInit:
    def test_success(self) -> None:
        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        # intentionally different from defaults
        advisory = timedelta(minutes=60)
        mandatory = timedelta(minutes=30)

        # WHEN
        refresher = AwsCredentialsRefresher(
            resource=resource,
            session=session,
            failure_callback=callback,
            advisory_refresh_timeout=advisory,
            mandatory_refresh_timeout=mandatory,
        )

        # THEN
        assert refresher._session is session
        assert refresher._resource == resource
        assert refresher._advisory_refresh_timeout == advisory
        assert refresher._mandatory_refresh_timeout == mandatory
        assert refresher._timer is None

    def test_time_to_expiry(self) -> None:
        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        refresher = AwsCredentialsRefresher(
            resource=resource,
            session=session,
            failure_callback=callback,
        )
        credentials = SettableCredentials()
        now_start = datetime.now(timezone.utc)
        credentials.expiry = now_start + timedelta(minutes=10)
        session.get_credentials.return_value = credentials

        # WHEN
        time_now, time_expired = refresher._credentials_time_to_expiry()

        # THEN
        assert time_now >= now_start
        # generous rounding for accuracy in this test...
        assert time_now <= (now_start + timedelta(minutes=1))
        assert time_expired == (credentials.expiry - time_now)

    @pytest.mark.parametrize(
        "time_remaining, expected_refresh_seconds",
        [
            # Note: advisory is 15 minutes by default
            pytest.param(timedelta(minutes=20), 5 * 60, id="outside advisory period"),
            pytest.param(timedelta(minutes=5), 60, id="inside advisory period"),
        ],
    )
    def test_set_timer(self, time_remaining: timedelta, expected_refresh_seconds: int) -> None:
        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(resource="queue-1234")
        refresher = AwsCredentialsRefresher(
            resource=resource,
            session=session,
            failure_callback=callback,
        )
        time_now = datetime.now(timezone.utc)
        mock_timer_initial = MagicMock()
        refresher._timer = mock_timer_initial

        with patch.object(refresher_mod, "Timer", MagicMock()) as mock_timer_cls:
            mock_timer = mock_timer_cls.return_value
            # WHEN
            refresher._set_timer(time_now, time_remaining)

            # THEN
            mock_timer_initial.cancel.assert_called_once()
            mock_timer_cls.assert_called_once_with(expected_refresh_seconds, refresher._refresh)
            mock_timer.start.assert_called_once()

    def test_enter(self) -> None:
        # Entering the context manager should setup a timer.

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        with patch.object(AwsCredentialsRefresher, "_set_timer") as mock_set_timer:
            refresher = AwsCredentialsRefresher(
                resource=resource,
                session=session,
                failure_callback=callback,
            )

            # WHEN
            refresher.__enter__()

            # THEN
            mock_set_timer.assert_called_once()

    def test_exit(self) -> None:
        # Exiting the context manager should cancel any existing timer

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        refresher = AwsCredentialsRefresher(
            resource=resource,
            session=session,
            failure_callback=callback,
        )
        mock_timer = MagicMock()
        refresher._timer = mock_timer
        refresher._context_count = 1

        # WHEN
        refresher.__exit__(MagicMock(), MagicMock(), MagicMock())

        # THEN
        mock_timer.cancel.assert_called_once()
        assert refresher._timer is None

    def test_refresh_success(self) -> None:
        # Test the happy-path of refresh -- we call refresh on the session, and
        # then set a new timer.

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        time_now = datetime.now(timezone.utc)
        time_remaining = timedelta(minutes=60)

        with (
            patch.object(
                AwsCredentialsRefresher, "_credentials_time_to_expiry"
            ) as mock_time_to_expiry,
            patch.object(AwsCredentialsRefresher, "_set_timer") as mock_set_timer,
        ):
            mock_time_to_expiry.return_value = (time_now, time_remaining)

            refresher = AwsCredentialsRefresher(
                resource=resource,
                session=session,
                failure_callback=callback,
            )

            # WHEN
            refresher._refresh()

            # THEN
            mock_set_timer.assert_called_once_with(time_now, time_remaining)

    def test_refresh_remaining_less_than_mandatory(self) -> None:
        # Test that if we have less time remaining than the mandatory timeout period
        # that we invoke the callback with a TimeoutError

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        time_now = datetime.now(timezone.utc)
        time_remaining = timedelta(minutes=5)  # Less than 10mins => mandatory timeout
        credentials = SettableCredentials()
        credentials.expiry = time_now + time_remaining
        session.get_credentials.return_value = credentials

        with (
            patch.object(
                AwsCredentialsRefresher, "_credentials_time_to_expiry"
            ) as mock_time_to_expiry,
            patch.object(AwsCredentialsRefresher, "_set_timer"),
        ):
            mock_time_to_expiry.return_value = (time_now, time_remaining)

            refresher = AwsCredentialsRefresher(
                resource=resource,
                session=session,
                failure_callback=callback,
            )

            # WHEN
            refresher._refresh()

            # THEN
            callback.assert_called_once()
            assert len(callback.call_args.args) == 1
            callback_first_call_arg = callback.call_args.args[0]
            assert isinstance(callback_first_call_arg, TimeoutError)
            assert callback_first_call_arg.args[0] == credentials.expiry

    @pytest.mark.parametrize(
        "exception, invokes_callback",
        [
            pytest.param(DeadlineRequestInterrupted(Exception("inner")), False, id="interrupted"),
            pytest.param(
                DeadlineRequestUnrecoverableError(Exception("inner")), True, id="unrecoverable"
            ),
            pytest.param(DeadlineRequestError(Exception("inner")), True, id="generic error"),
        ],
    )
    def test_terminal_exception(self, exception: Exception, invokes_callback: bool) -> None:
        # Test that when the session's refresh_credentials raises certain exceptions then
        # we cease trying to refresh, and sometimes invoke the callback

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        time_now = datetime.now(timezone.utc)
        time_remaining = timedelta(minutes=60)

        with (
            patch.object(
                AwsCredentialsRefresher, "_credentials_time_to_expiry"
            ) as mock_time_to_expiry,
            patch.object(AwsCredentialsRefresher, "_set_timer") as mock_set_timer,
        ):
            mock_time_to_expiry.return_value = (time_now, time_remaining)

            refresher = AwsCredentialsRefresher(
                resource=resource,
                session=session,
                failure_callback=callback,
            )
            session.refresh_credentials.side_effect = exception

            # WHEN
            refresher._refresh()

            # THEN
            mock_time_to_expiry.assert_not_called()
            mock_set_timer.assert_not_called()
            if invokes_callback:
                callback.assert_called_once_with(exception)
            else:
                callback.assert_not_called()

    @pytest.mark.parametrize(
        "exception, invokes_callback",
        [
            pytest.param(
                DeadlineRequestConditionallyRecoverableError(Exception("inner")),
                False,
                id="conditional",
            ),
        ],
    )
    def test_conditional_exception(self, exception: Exception, invokes_callback: bool) -> None:
        # Test that when the session's refresh_credentials raises a conditionally recoverable error
        # that we invoke the callback, but still do the regular flow to retry continuing.

        # GIVEN
        session = MagicMock()
        callback = MagicMock()
        resource = dict(queue_id="queue-1234")
        time_now = datetime.now(timezone.utc)
        time_remaining = timedelta(minutes=60)

        with (
            patch.object(
                AwsCredentialsRefresher, "_credentials_time_to_expiry"
            ) as mock_time_to_expiry,
            patch.object(AwsCredentialsRefresher, "_set_timer") as mock_set_timer,
        ):
            mock_time_to_expiry.return_value = (time_now, time_remaining)

            refresher = AwsCredentialsRefresher(
                resource=resource,
                session=session,
                failure_callback=callback,
            )
            session.refresh_credentials.side_effect = exception

            # WHEN
            refresher._refresh()

            # THEN
            callback.assert_called_once_with(exception)

            mock_time_to_expiry.assert_called_once()
            mock_set_timer.assert_called_once()
