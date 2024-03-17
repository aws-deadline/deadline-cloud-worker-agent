# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import logging
from typing import Callable, Optional, TypeVar, Any, cast
from types import TracebackType
from datetime import datetime, timedelta, timezone
from threading import Lock, Timer

from .boto3_sessions import BaseBoto3Session, SettableCredentials
from ..aws.deadline import (
    DeadlineRequestInterrupted,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestError,
)
from ..log_messages import AwsCredentialsLogEvent, AwsCredentialsLogEventOp


_logger = logging.getLogger(__name__)


class AwsCredentialsRefresher:
    """A context manager that continually refreshes the AWS Credentials stored within a
    BaseBoto3Session (i.e. WorkerBoto3Session or QueueBoto3Session) as long as the context manager
    is entered.

    **This context manager can be entered multiple times, from different threads. It will fully
    exit only once it has been exited as many times as it has been entered.**

    We provide for an advisory refresh timeout and a mandatory refresh timeout (borrowing
    the terms from botocore). If we fail to refresh AWS Credentials:
    1. During the advisory timeout, but not the mandatory timeout then we log a warning and
       then retry in a minute.
    2. During the mandatory timeout (including expired) is considered fatal. We invoke the provided
       callback and then cease trying to refresh credentials. The expectation is that the Agent
       will fail or exit as appropriate.

    On the callback. The interface is: Callable[[Exception], None]
    1. If attempting to refresh credentials raises an exception, then the callback will be invoked
       with that Exception object. We will then cease trying to refresh credentials.
    2. If we find that we need to stop trying to refresh credentials due to being within the
       mandatory timeout period (or expired) then the callback will be called with a TimeoutError exception.
       That TimeoutError exception's arg[0] will be a datetime instance that provides the time at which
       the credentials expire (or did expire).
    """

    # Note: These timeout minimums must be defined in minutes, and
    # they should differ by at least 5 minutes.
    MIN_ADVISORY_REFRESH_TIMEOUT = timedelta(minutes=15)
    MIN_MANDATORY_REFRESH_TIMEOUT = timedelta(minutes=10)
    assert MIN_MANDATORY_REFRESH_TIMEOUT + timedelta(minutes=5) <= MIN_ADVISORY_REFRESH_TIMEOUT

    _session: BaseBoto3Session
    _resource: dict[str, str]
    _advisory_refresh_timeout: timedelta
    _mandatory_refresh_timeout: timedelta
    _failure_callback: Callable[[Exception], None]
    _timer: Optional[Timer]
    _retries_attempted_after_expired: int

    # How many times this context manager has been entered, and
    # the lock to take when updating the count.
    _context_count: int
    _context_lock: Lock

    def __init__(
        self,
        *,
        resource: dict[str, str],
        session: BaseBoto3Session,
        failure_callback: Callable[[Exception], None],
        advisory_refresh_timeout: timedelta = timedelta(minutes=15),
        mandatory_refresh_timeout: timedelta = timedelta(minutes=10),
    ) -> None:
        """
        Args:
            identifier (str): An identifier for the Session. Will be printed in logs.
            session (BaseBoto3Session): The Session that will have its AWS Credentials
                periodically refreshed by this instance.
            advisory_refresh_timeout (timedelta): When the Session's credentials are within
                this delta of expiring, then the refresh interval is increased to once per
                minute until successful or the mandatory threshold has been reached.
            mandatory_refresh_timeout (timedelta): If the Session's credentials are ever
                within this delta of expiring after attempting to refresh them, then the
                failure_callback is invoked with a TimeoutError and this instance ceases
                to attempt to refresh the credentials. The expectation is that the application
                will fail/exit the associated subsystem.

            failure_callback (Callable[[Exception], None]): A callback that is invoked if
                an unrecoverable error is encountered when trying to refresh credentials, or
                if the mandatory refresh timeout has been breached.
        """
        if advisory_refresh_timeout < AwsCredentialsRefresher.MIN_ADVISORY_REFRESH_TIMEOUT:
            raise RuntimeError("advisory refresh too small")
        if mandatory_refresh_timeout < AwsCredentialsRefresher.MIN_MANDATORY_REFRESH_TIMEOUT:
            raise RuntimeError("mandatory refresh too small")

        self._session = session
        self._resource = resource
        self._advisory_refresh_timeout = advisory_refresh_timeout
        self._mandatory_refresh_timeout = mandatory_refresh_timeout
        self._failure_callback = failure_callback
        self._timer = None

        self._context_count = 0
        self._context_lock = Lock()

    def __enter__(self) -> AwsCredentialsRefresher:
        with self._context_lock:
            if self._context_count == 0:
                # Set the timer only if this is our first time entering the context manager.
                # If another thread is already in the context then that will have started the timer
                # going.
                self._set_timer(*self._credentials_time_to_expiry())
            self._context_count += 1
        return self

    def __exit__(self, type: TypeVar, value: Any, traceback: TracebackType) -> None:
        with self._context_lock:
            self._context_count -= 1
            if self._context_count == 0:
                # Stop the timer only once there are no threads within the context.
                if self._timer:
                    self._timer.cancel()
                    self._timer = None

    def _credentials_time_to_expiry(self) -> tuple[datetime, timedelta]:
        credentials = cast(SettableCredentials, self._session.get_credentials())
        time_now = datetime.now(timezone.utc)
        return time_now, credentials.expiry - time_now

    def _set_timer(self, time_now: datetime, time_remaining: timedelta) -> None:
        if self._timer:
            self._timer.cancel()
            self._timer = None
        if self._advisory_refresh_timeout < time_remaining:
            # If we're not in the advisory refresh period, then
            # try to refresh at the start of the advisory period.
            refresh_in = time_remaining - self._advisory_refresh_timeout
        else:
            # If we're within the advisory period, then retry every minute as long as we
            # are allowed to retry.
            refresh_in = timedelta(minutes=1)

        self._timer = Timer(refresh_in.total_seconds(), self._refresh)
        self._timer.start()
        _logger.info(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.REFRESH,
                **self._resource,
                message="Refresh scheduled.",
                scheduled_time=str((time_now + refresh_in).isoformat()),
            )
        )

    def _refresh(self) -> None:
        # Invoked by the Timer() thread when it's time to attempt to refresh the
        # stored AWS Credentials.

        try:
            self._session.refresh_credentials()
        except DeadlineRequestInterrupted:
            # This is raised if the Session has an interrupt Event and that
            # event was set by some external actor.
            # Action for us is to just stop trying to refresh, and let the
            # external actor wind things down as needed.
            return
        except DeadlineRequestConditionallyRecoverableError as e:
            # Log & invoke callback, but still continue trying to
            # refresh if possible. i.e. Let the owner of this refresher
            # stop it as they see fit.
            self._failure_callback(e)
            pass
        except (DeadlineRequestUnrecoverableError, DeadlineRequestError) as e:
            # The error is unrecoverable. Log it, let the owner know, and
            # stop trying to refresh.
            self._failure_callback(e)
            return

        time_now, time_remaining = self._credentials_time_to_expiry()
        if time_remaining < self._mandatory_refresh_timeout:
            # We attempted to refresh and still have credentials
            # that expire within the mandatory period (or have already
            # expired).
            # Invoke the callback to let the owner know, and cease refreshing.
            credentials = cast(SettableCredentials, self._session.get_credentials())
            self._failure_callback(TimeoutError(credentials.expiry))
        else:
            self._set_timer(time_now, time_remaining)
