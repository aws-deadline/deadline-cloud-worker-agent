# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Module for log synchronization to CloudWatch Logs"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial
from logging import Formatter, Handler, Logger, LogRecord
from threading import Event, Thread
from time import monotonic, sleep
from types import TracebackType
from typing import Any, Deque, Generator, NamedTuple, Type

from typing_extensions import TypedDict

from .loggers import logger as _logger

__all__ = [
    "CloudWatchHandler",
    "FormattedLogEntry",
    "stream_cloudwatch_logs",
]

LOG_CONFIG_OPTION_GROUP_NAME_KEY = "logGroupName"
LOG_CONFIG_OPTION_STREAM_NAME_KEY = "logStreamName"


_DEFAULT_FMT_STRING = "%(message)s"


class PutLogEventsConstraints(NamedTuple):
    max_batch_size_bytes: int
    max_events_per_batch: int
    max_log_event_size: int
    max_future_time_delta: timedelta
    max_past_time_delta: timedelta
    max_time_span_in_batch: timedelta


PUT_LOG_EVENTS_CONSTRAINTS = PutLogEventsConstraints(
    max_batch_size_bytes=1048576,
    max_events_per_batch=10000,
    max_log_event_size=256 * 1000,  # 256KB
    max_future_time_delta=timedelta(hours=2),
    # The Worker agent is more strict in the oldest log event it will accept so that we do not need
    # to check the CloudWatch log group retention, which allows 1 day as its shortest value
    max_past_time_delta=timedelta(days=1),
    max_time_span_in_batch=timedelta(hours=24),
)
PUT_LOG_EVENTS_EVENT_PADDING = 26


class FormattedLogEntry(NamedTuple):
    timestamp: int
    message: str


class CloudWatchLogEvent(TypedDict):
    timestamp: int
    message: str


class PartitionedCloudWatchLogEvent(NamedTuple):
    """A log event that has been processed by the CloudWatchLogEventPartitioner"""

    log_event: CloudWatchLogEvent
    size: int


class CloudWatchLogEventRejectedException(Exception):
    """Exception raised when a log event is rejected by a PutLogEvents batch"""

    batch_full: bool
    """
    Whether the event was rejected because the current batch is full.
    The event should be kept to send in the next batch.
    """
    reason: str | None
    """
    Reason message for why the event was rejected. If this is set, it should be prepended to the
    session log so that it is emitted immediately in the session log.
    """

    def __init__(self, batch_full: bool, reason: str | None = None) -> None:
        super().__init__(reason)
        self.batch_full = batch_full
        self.reason = reason


class CloudWatchLogEventBatch:
    log_events: list[PartitionedCloudWatchLogEvent]

    min_timestamp_ms: int | None
    max_timestamp_ms: int | None

    def __init__(self) -> None:
        self.log_events = []
        self.min_timestamp_ms = None
        self.max_timestamp_ms = None

    def add(self, processed_log_event: PartitionedCloudWatchLogEvent) -> None:
        """
        Adds a log event to this batch. If the event cannot be added, returns False and a
        CloudWatchLogEventRejectionInfo object with details on why it could not be added.

        Args:
            processed_log_event (ProcessedCloudWatchLogEvent): The log event to add.

        Raises:
            CloudWatchLogEventRejectedException - raised when the event cannot be added
        """
        self._validate_log_event_can_be_added(processed_log_event)

        if (
            self.min_timestamp_ms is None
            or processed_log_event.log_event["timestamp"] < self.min_timestamp_ms
        ):
            self.min_timestamp_ms = processed_log_event.log_event["timestamp"]
        if (
            self.max_timestamp_ms is None
            or processed_log_event.log_event["timestamp"] > self.max_timestamp_ms
        ):
            self.max_timestamp_ms = processed_log_event.log_event["timestamp"]

        self.log_events.append(processed_log_event)

    def _validate_log_event_can_be_added(self, event: PartitionedCloudWatchLogEvent) -> None:
        """
        Checks if a log event can be added to a batch for the CloudWatch PutLogEvents API.

        NOTE: There is a risk of an infinite loop if the logs generated by this function do not meet
        the PutLogEvents criteria enforced by this function. Ensure that all log messages generated
        by this function adhere to the restrictions enforced by this function.

        Args:
            event (ProcessedCloudWatchLogEvent): The log event to check.

        Raises:
            CloudWatchLogEventRejectedException - raised when the log event cannot be added
        """
        # Verify the event fits into the max batch size
        if self.count + 1 > PUT_LOG_EVENTS_CONSTRAINTS.max_events_per_batch:
            raise CloudWatchLogEventRejectedException(batch_full=True, reason=None)
        if (
            self.size + event.size + PUT_LOG_EVENTS_EVENT_PADDING
            > PUT_LOG_EVENTS_CONSTRAINTS.max_batch_size_bytes
        ):
            raise CloudWatchLogEventRejectedException(batch_full=True, reason=None)

        now = datetime.now()
        # datetime expects timestamp in seconds, convert log event timestamp which is in milliseconds
        log_event_time = datetime.fromtimestamp(event.log_event["timestamp"] / 1000)

        def _log_event_preview() -> CloudWatchLogEvent:
            return CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"],
                # Truncate preview message to 100 chars (size chosen arbitrarily)
                message=f"{event.log_event['message'][0:100]} (truncated)",
            )

        # Verify log event is not too far in the future
        if now + PUT_LOG_EVENTS_CONSTRAINTS.max_future_time_delta < log_event_time:
            raise CloudWatchLogEventRejectedException(
                batch_full=False,
                reason=f"Ignoring log event that is too far in the future (max {PUT_LOG_EVENTS_CONSTRAINTS.max_future_time_delta.total_seconds()}s): {_log_event_preview()}",
            )

        # Verify events in batch do not span more than allowed time span
        min_datetime = log_event_time
        if self.min_datetime and self.min_datetime < min_datetime:
            min_datetime = self.min_datetime

        max_datetime = log_event_time
        if self.max_datetime and self.max_datetime > max_datetime:
            max_datetime = self.max_datetime

        batch_timespan = max_datetime - min_datetime
        if batch_timespan > PUT_LOG_EVENTS_CONSTRAINTS.max_time_span_in_batch:
            raise CloudWatchLogEventRejectedException(
                batch_full=False,
                reason=f"Ignoring log event that would exceed the max allowed time span in a batch of {PUT_LOG_EVENTS_CONSTRAINTS.max_time_span_in_batch.total_seconds()}s: {_log_event_preview()}",
            )

        # Verify log message is not older than 1 day
        max_past_timedelta = PUT_LOG_EVENTS_CONSTRAINTS.max_past_time_delta
        if log_event_time < now - max_past_timedelta:
            raise CloudWatchLogEventRejectedException(
                batch_full=False,
                reason=f"Ignoring log event that is older than {max_past_timedelta.days} days: {_log_event_preview()}",
            )

    @property
    def size(self) -> int:
        return sum(log_event.size + PUT_LOG_EVENTS_EVENT_PADDING for log_event in self.log_events)

    @property
    def count(self) -> int:
        return len(self.log_events)

    @property
    def min_datetime(self) -> datetime | None:
        return (
            # datetime expects timestamp in seconds, convert log event timestamp which is in milliseconds
            datetime.fromtimestamp(self.min_timestamp_ms / 1000)
            if self.min_timestamp_ms is not None
            else None
        )

    @property
    def max_datetime(self) -> datetime | None:
        return (
            # datetime expects timestamp in seconds, convert log event timestamp which is in milliseconds
            datetime.fromtimestamp(self.max_timestamp_ms / 1000)
            if self.max_timestamp_ms is not None
            else None
        )


class CloudWatchLogEventPartitionException(Exception):
    log_event: FormattedLogEntry

    def __init__(self, *args, log_event: FormattedLogEntry) -> None:
        super().__init__(*args)
        self.log_event = log_event


class CloudWatchLogEventPartitioner:
    """
    Class that returns partitioned CloudWatch log event records that meet CloudWatch service criteria
    for the maximum size of log events.
    See: https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    """

    _partitioned_event_deque: Deque[PartitionedCloudWatchLogEvent]
    _raw_event_deque: Deque[FormattedLogEntry]

    def __init__(self, raw_deque: Deque[FormattedLogEntry]) -> None:
        self._raw_event_deque = raw_deque
        self._partitioned_event_deque = deque()

    def next(self) -> PartitionedCloudWatchLogEvent:
        try:
            return self._partitioned_event_deque.popleft()
        # deque.popleft raises IndexError if there are no elements available
        except IndexError:
            while True:
                try:
                    events = self._partition_raw_event()
                except CloudWatchLogEventPartitionException as e:
                    # Something went wrong with chunking a raw log event. Send a warning message
                    # to the session log and skip it.
                    err_msg = f"Failed to process raw log event: {e}\n\nSkipping event..."
                    self._partitioned_event_deque.appendleft(
                        PartitionedCloudWatchLogEvent(
                            log_event=CloudWatchLogEvent(
                                timestamp=e.log_event.timestamp,
                                message=err_msg,
                            ),
                            size=len(err_msg.encode("utf-8")),
                        )
                    )
                else:
                    self._partitioned_event_deque.extend(events[1:])
                    return events[0]

    def appendleft(self, log_event: PartitionedCloudWatchLogEvent) -> None:
        self._partitioned_event_deque.appendleft(log_event)

    def _partition_raw_event(self) -> list[PartitionedCloudWatchLogEvent]:
        """
        Processes a raw log event to ensure it fits in the max size per log event enforced by CloudWatch.
        If the event is too large, it will be chunked into multiple smaller events that do fit.

        Returns:
            list[ProcessedCloudWatchLogEvent]: The processed CloudWatch event(s)

        Raises:
            IndexError - raised when there are no more raw log events
            CloudWatchLogEventPartitionException - raised when an error occurs while chunking a UTF-8
                string, most likely due to a log event having nonvalid UTF-8 encoding.
        """
        # raises: IndexError - we let this propagate up to indicate that there are no more events
        raw_event = self._raw_event_deque.popleft()

        # Chunk the string in case we're over the max size of a log event in CloudWatch so we can
        # split up the log event into multiple chunks that fit into that max size.
        #
        # If we're within the max size, _chunk_string will return the original contents.
        try:
            message_chunks = CloudWatchLogEventPartitioner._chunk_string(
                raw_event.message,
                PUT_LOG_EVENTS_CONSTRAINTS.max_log_event_size,
            )
        except ValueError as e:
            raise CloudWatchLogEventPartitionException(
                f"Failed to chunk raw log event: {raw_event}",
                log_event=raw_event,
            ) from e

        return [
            PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(timestamp=raw_event.timestamp, message=msg),
                size=size,
            )
            for msg, size in message_chunks
        ]

    @property
    def has_items(self) -> bool:
        return len(self._partitioned_event_deque) > 0 or len(self._raw_event_deque) > 0

    @staticmethod
    def _chunk_string(s: str, size: int) -> list[tuple[str, int]]:
        """
        Chunks a string into multiple strings with a specific max size.

        We'll just split the string by the number of unicode code points, like the CloudWatch agent does.
        This means that grapheme clusters (i.e. a single, user-perceived character) will not be preserved.
        See: https://github.com/aws/amazon-cloudwatch-agent/blob/5fe8f1a376d25bac3632c1953bb300a9cf95d0f5/plugins/inputs/logfile/tail/tail.go#L608-L628

        Args:
            s (str): The string to split
            size (int): The max size of each chunk

        Returns:
            list[tuple[str, int]]: List of tuples of the chunked string and its size in bytes when encoded to UTF-8

        Raises:
            ValueError - raised when the input string could not be chunked into valid UTF-8 strings. This is likely due
            to a malformed string input that encodes to a nonvalid UTF-8 byte sequence.
        """
        assert (
            size >= 4
        ), f"Chunk size too small ({size}). Must be at least 4 bytes to handle all UTF-8 characters."

        start = 0
        chunks: list[tuple[str, int]] = []
        s_utf8 = s.encode("utf-8")
        len_s_utf8 = len(s_utf8)

        while start < len_s_utf8:
            end = start + size
            if end >= len_s_utf8:
                # We're at the end of the string, treat the rest of it as our chunk.
                end = len_s_utf8
            else:
                # Backtrack until we hit a non-continuation byte in UTF-8, which always starts with a bit sequence of: 10
                while end > start and s_utf8[end] & 0xC0 == 0x80:
                    end -= 1
                if end <= start:
                    raise ValueError(
                        f"Cannot chunk UTF-8 string: could not find first byte of code point between index {start} and {start + size}"
                    )

            chunk = s_utf8[start:end].decode("utf-8")
            chunks.append((chunk, end - start))

            start = end

        return chunks


class CloudWatchLogStreamThread(Thread):
    """
    A thread that is responsible for reading log events from a queue and publishing them to a
    specified CloudWatch log stream.

    This abstracts CloudWatch API service limits from the producer:

    1.  The maximum batch size (10000 at the present) of log events that can be included in a
        PutLogEvents API request
    2.  The maximum PutLogEvents API request frequency per CloudWatch log stream (5 requests /sec at
        the present)

    See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    """

    MAX_PUT_LOG_EVENTS_PER_STREAM_SEC = 5
    PUT_LOG_EVENTS_ERROR_DELAY_SECONDS = 1
    PUT_LOG_EVENTS_ERROR_STOPPED_RETRIES = 5

    _logs_client: Any
    _log_event_partitioner: CloudWatchLogEventPartitioner
    _log_group_name: str
    _log_stream_name: str
    _prev_request_times: Deque[float]
    _stop_event: Event

    def __init__(
        self,
        *args: Any,
        logs_client: Any,
        log_event_queue: Deque[FormattedLogEntry],
        log_group_name: str,
        log_stream_name: str,
        stop_event: Event,
        **kwargs: Any,
    ) -> None:
        """
        Constructs a CloudWatchLogStreamThread

        Arguments:
            logs_client (boto3.client):
                The boto3 client for the CloudWatch logs service.
            log_event_queue (Deque[FormattedLogEntry]):
                The queue of log events to be published to CloudWatch
            log_group_name (str):
                The name of the CloudWatch log group name to publish the log events to
            log_stream_name (str):
                The name of the CloudWatch log stream name to publish the log events to
            stop_event (threading.Event):
                An event to signal that the thread should flush the remaining logs in the queue
                and exit
        """
        self._logs_client = logs_client
        self._log_event_partitioner = CloudWatchLogEventPartitioner(raw_deque=log_event_queue)
        self._log_group_name = log_group_name
        self._log_stream_name = log_stream_name
        self._stop_event = stop_event
        self._prev_request_times: Deque[float] = deque()

        super().__init__(*args, **kwargs)

    def _is_running(self) -> bool:
        """
        The thread should continue running until a stop event is received and the log event queue
        is empty.
        """
        return not self._stop_event.is_set() or self._log_event_partitioner.has_items

    def run(self) -> None:
        """
        The run loop of the thread. This loops until the stop_event instance is signalled and the
        log events in the queue are flushed.
        """
        log_events: list[CloudWatchLogEvent] = []

        while self._is_running():
            log_events = self._collect_logs()

            if log_events:
                self._upload_logs(log_events=log_events)
            else:
                # No log events. Sleep to avoid a tight loop and explicitly release the GIL for
                # other threads.
                sleep(0.2)

    def _collect_logs(self) -> list[CloudWatchLogEvent]:
        """
        Collect and return as many logs as are available in the deque - up to the maximum allowed
        per CWL PutLogEvents request.
        """
        batch = CloudWatchLogEventBatch()

        # Collect the maximum number of records that satisfy the PutLogEvents constraints.
        # See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        while True:
            try:
                log_event = self._log_event_partitioner.next()
            # CloudWatchLogEventPartitioner.next() raises IndexError if there are no log events available
            except IndexError:
                break
            else:
                try:
                    batch.add(log_event)
                except CloudWatchLogEventRejectedException as e:
                    if e.reason:
                        # Send the rejection reason to the session log
                        self._log_event_partitioner.appendleft(
                            PartitionedCloudWatchLogEvent(
                                log_event=CloudWatchLogEvent(
                                    timestamp=log_event.log_event["timestamp"],
                                    message=e.reason,
                                ),
                                size=len(e.reason.encode("utf-8")),
                            )
                        )
                    if e.batch_full:
                        # Keep the message for the next batch and stop processing events
                        self._log_event_partitioner.appendleft(log_event)
                        break

        # PutLogEvents requires that log events are chronological within a single API request.
        # Different threads can log concurrently and the log events in the resulting list ordering
        # can become non-chronological. We must sort the list by timestamp.
        #
        # Python's sorted() is a stable sort, so the ordering of log events from the same ordering
        # will be preserved. See:
        # https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts
        log_events = [processed_log_event.log_event for processed_log_event in batch.log_events]
        log_events = sorted(log_events, key=lambda log_event: log_event["timestamp"])

        return log_events

    def _upload_logs(
        self,
        *,
        log_events: list[CloudWatchLogEvent],
    ) -> None:
        """
        Uploads a batch of logs to the specified CloudWatch log group/stream
        """
        success = False
        stop_attempts = CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_STOPPED_RETRIES
        put_log_events = partial(
            self._logs_client.put_log_events,
            logGroupName=self._log_group_name,
            logStreamName=self._log_stream_name,
            logEvents=log_events,
        )
        self._throttle_put_log_events()
        _logger.debug("Calling PutLogEvents with %d log events", len(log_events))
        while stop_attempts > 0:
            try:
                put_log_events()
            except Exception as e:
                error_args: list[Any] = []

                if self._stop_event.is_set():
                    stop_attempts -= 1
                    error_args = [
                        "Error uploading CloudWatch logs (sleeping %ds, %d attempts remaining): %s",
                        CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS,
                        stop_attempts,
                        e,
                    ]
                else:
                    error_args = [
                        "Error uploading CloudWatch logs (sleeping %ds): %s",
                        CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS,
                        e,
                    ]
                _logger.error(*error_args, stack_info=True)
                sleep(CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS)
            else:
                success = True
                break

        if not success:
            assert self._stop_event.is_set()
            _logger.error("Unable to upload logs due to task ending")

    def _throttle_put_log_events(self) -> None:
        """
        Guarantees that only 5 CWL requests are made per second. This method should be called
        immediately before calling the boto3 put_log_events call.
        """

        # We use time.monotonic to be robust to system clock changes
        now = monotonic()

        if (
            len(self._prev_request_times)
            >= CloudWatchLogStreamThread.MAX_PUT_LOG_EVENTS_PER_STREAM_SEC
        ):
            # Remove timestamps older than one second
            while True:
                try:
                    earliest = self._prev_request_times.popleft()
                except IndexError:
                    break

                if now - earliest < 1:
                    # If we reach a timestamp that is less than a second ago, we add it back to the
                    # LHS of the deque and break from the loop
                    self._prev_request_times.appendleft(earliest)
                    break

            # If we still have more than the maximum number of PutLogEvents requests in the last
            # second, then sleep until the oldest API request is one second old
            if (
                len(self._prev_request_times)
                >= CloudWatchLogStreamThread.MAX_PUT_LOG_EVENTS_PER_STREAM_SEC
            ):
                sleep_duration = 1 - (now - earliest)
                _logger.debug(
                    "PutLogEvents limit reached (%d per second), sleeping %.2f seconds",
                    CloudWatchLogStreamThread.MAX_PUT_LOG_EVENTS_PER_STREAM_SEC,
                    sleep_duration,
                )
                sleep(sleep_duration)

        # Add the timestamp to the deque for the next iteration
        self._prev_request_times.append(now)


class CloudWatchHandler(Handler):
    _log_event_queue: Deque[FormattedLogEntry]
    _log_stream_thread: CloudWatchLogStreamThread
    _stop_event: Event

    def __init__(
        self,
        *,
        logs_client: Any,
        log_group_name: str,
        log_stream_name: str,
    ) -> None:
        self._log_event_queue = deque()
        self._stop_event = Event()
        self._log_stream_thread = CloudWatchLogStreamThread(
            logs_client=logs_client,
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            log_event_queue=self._log_event_queue,
            stop_event=self._stop_event,
            daemon=True,
        )
        self._log_stream_thread.start()

        super(CloudWatchHandler, self).__init__()

    def __enter__(self) -> CloudWatchHandler:
        return self

    def __exit__(
        self,
        type: Type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def emit(self, record: LogRecord) -> None:
        # Queue the record for streaming to CloudWatch
        try:
            message = self.format(record)
            # CloudWatch requires a message with minimum length of 1. Blank lines may format into
            # empty strings, so we pad them with a single space character
            # See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_InputLogEvent.html
            if not message:
                message = " "
            # record.created is expressed in seconds (floating-point) since
            # January 1, 1970, 00:00:00 (UTC) but CloudWatch expects an integer value expressed in
            # microseconds since utc-epoch.
            # Our service uses a SessionAction's startedAt/endedAt times to determine which part of a
            # log belongs to that particular SessionAction. So, we need to take some care to ensure that
            # the time that we report to CloudWatch is rounded in the same way that the service will round
            # the startedAt/endedAt time that it receives. Our service truncates, rather than rounds, times
            # to microseconds so we do the same here.
            timestamp = int(record.created * 1000)
            self._log_event_queue.append(
                FormattedLogEntry(
                    timestamp=timestamp,
                    message=message,
                )
            )
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        super().close()
        self._stop_event.set()
        self._log_stream_thread.join()


@contextmanager
def stream_cloudwatch_logs(
    *,
    logs_client: Any,
    log_group_name: str,
    log_stream_name: str,
    logger: Logger,
    log_fmt: str = _DEFAULT_FMT_STRING,
) -> Generator[CloudWatchHandler, None, None]:
    with CloudWatchHandler(
        logs_client=logs_client,
        log_group_name=log_group_name,
        log_stream_name=log_stream_name,
    ) as handler:
        handler.setFormatter(Formatter(log_fmt))
        logger.addHandler(handler)
        try:
            _logger.info(f"logs streamed to CWL target: {log_group_name}/{log_stream_name}")
            yield handler
        finally:
            logger.removeHandler(handler)
