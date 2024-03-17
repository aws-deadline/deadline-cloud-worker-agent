# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import itertools
import os
from collections import deque
from datetime import datetime, timedelta
from logging import INFO, Formatter, LogRecord
from threading import Event
from typing import Any, Generator, Optional
from unittest.mock import MagicMock, PropertyMock, call, patch

from pytest import fixture, mark, param, raises

import deadline_worker_agent.log_sync.cloudwatch as module
import deadline_worker_agent.log_sync.loggers as logger_mod
from deadline_worker_agent.log_sync.cloudwatch import (
    CloudWatchHandler,
    CloudWatchLogEvent,
    CloudWatchLogEventBatch,
    CloudWatchLogEventPartitioner,
    CloudWatchLogEventRejectedException,
    CloudWatchLogStreamThread,
    FormattedLogEntry,
    PartitionedCloudWatchLogEvent,
    stream_cloudwatch_logs,
)
from deadline_worker_agent.log_messages import LogRecordStringTranslationFilter


@fixture
def mock_module_logger() -> Generator[MagicMock, None, None]:
    """Patches deadline_worker_agent.log_sync.cloudwatch.logger.logger with a mock object"""
    with patch.object(module, "_logger") as mock_module_logger:
        yield mock_module_logger


class TestCloudWatchLogEventBatch:
    @fixture(autouse=True)
    def now(self) -> datetime:
        return datetime(2000, 1, 1)

    @fixture(autouse=True)
    def datetime_mock(self, now: datetime) -> Generator[MagicMock, None, None]:
        with patch.object(module, "datetime") as mock:
            mock.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            mock.fromtimestamp.side_effect = lambda *args, **kwargs: datetime.fromtimestamp(
                *args, **kwargs
            )
            mock.now.return_value = now
            yield mock

    @fixture
    def event(self, now: datetime) -> PartitionedCloudWatchLogEvent:
        return PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(timestamp=int(now.timestamp() * 1000), message="abc"),
            size=len("abc".encode("utf-8")),
        )

    @fixture
    def batch(self) -> CloudWatchLogEventBatch:
        return CloudWatchLogEventBatch()

    def test_add_first_event(
        self, event: PartitionedCloudWatchLogEvent, batch: CloudWatchLogEventBatch
    ):
        # WHEN
        batch.add(event)

        # THEN
        assert batch.min_timestamp_ms == event.log_event["timestamp"]
        assert batch.max_timestamp_ms == event.log_event["timestamp"]
        assert len(batch.log_events) == 1
        assert event in batch.log_events

    def test_add_oldest(self, event: PartitionedCloudWatchLogEvent, batch: CloudWatchLogEventBatch):
        # GIVEN
        older_event = PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"] - 1,
                message="older",
            ),
            size=5,
        )
        batch.add(event)

        # WHEN
        batch.add(older_event)

        # THEN
        assert batch.min_timestamp_ms == older_event.log_event["timestamp"]
        assert batch.max_timestamp_ms == event.log_event["timestamp"]
        assert older_event in batch.log_events

    def test_add_newest(self, event: PartitionedCloudWatchLogEvent, batch: CloudWatchLogEventBatch):
        # GIVEN
        newer_event = PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"] + 1,
                message="newer",
            ),
            size=5,
        )
        batch.add(event)

        # WHEN
        batch.add(newer_event)

        # THEN
        assert batch.min_timestamp_ms == event.log_event["timestamp"]
        assert batch.max_timestamp_ms == newer_event.log_event["timestamp"]
        assert newer_event in batch.log_events

    def test_add_in_between(
        self, event: PartitionedCloudWatchLogEvent, batch: CloudWatchLogEventBatch
    ):
        # GIVEN
        newer_event = PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"] + 10,
                message="newer",
            ),
            size=5,
        )
        middle_event = PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"] + 5,
                message="middle",
            ),
            size=6,
        )
        batch.add(event)
        batch.add(newer_event)

        # WHEN
        batch.add(middle_event)

        # THEN
        assert batch.min_timestamp_ms == event.log_event["timestamp"]
        assert batch.max_timestamp_ms == newer_event.log_event["timestamp"]
        assert middle_event in batch.log_events

    def test_size_includes_padding(
        self, event: PartitionedCloudWatchLogEvent, batch: CloudWatchLogEventBatch
    ):
        # GIVEN
        expected_padding = 26
        message = "this is a message"
        other_event = PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                timestamp=event.log_event["timestamp"],
                message=message,
            ),
            size=len(message.encode("utf-8")),
        )
        batch.add(event)
        batch.add(other_event)
        expected_size = (event.size + expected_padding) + (other_event.size + expected_padding)

        # WHEN
        actual_size = batch.size

        # THEN
        assert actual_size == expected_size

    class TestValidateLogEventCanBeAdded:
        @patch.object(module, "datetime", wraps=datetime)
        def test_valid_log_event(
            self,
            datetime_mock: MagicMock,
        ):
            # GIVEN
            now = datetime(2000, 1, 1)
            datetime_mock.now.return_value = now
            event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(
                    message="abc", timestamp=(int(now.timestamp()) * 1000)
                ),
                size=3,
            )
            batch = CloudWatchLogEventBatch()

            # WHEN
            batch._validate_log_event_can_be_added(event)

            # THEN
            # The function did not throw, test passed

        def test_too_many_events(self):
            # GIVEN
            with patch.object(
                CloudWatchLogEventBatch,
                "count",
                new_callable=PropertyMock(
                    return_value=module.PUT_LOG_EVENTS_CONSTRAINTS.max_events_per_batch
                ),
            ):
                batch = CloudWatchLogEventBatch()

                # WHEN
                with raises(CloudWatchLogEventRejectedException) as raised_exc:
                    batch._validate_log_event_can_be_added(MagicMock())

            # THEN
            assert raised_exc.value.batch_full is True
            assert raised_exc.value.reason is None

        @mark.parametrize(
            ("batch_size", "event_size"),
            (
                (0, module.PUT_LOG_EVENTS_CONSTRAINTS.max_batch_size_bytes),
                (module.PUT_LOG_EVENTS_CONSTRAINTS.max_batch_size_bytes, 0),
                (
                    module.PUT_LOG_EVENTS_CONSTRAINTS.max_batch_size_bytes / 2,
                    module.PUT_LOG_EVENTS_CONSTRAINTS.max_batch_size_bytes / 2,
                ),
            ),
        )
        def test_over_batch_size(
            self,
            batch_size: int,
            event_size: int,
        ):
            # GIVEN
            event = PartitionedCloudWatchLogEvent(
                log_event=MagicMock(),
                size=event_size,
            )
            with patch.object(
                CloudWatchLogEventBatch, "size", new_callable=PropertyMock(return_value=batch_size)
            ):
                batch = CloudWatchLogEventBatch()

                # WHEN
                with raises(CloudWatchLogEventRejectedException) as raised_exc:
                    batch._validate_log_event_can_be_added(event)

            # THEN
            assert raised_exc.value.batch_full is True
            assert raised_exc.value.reason is None

        def test_too_far_future(self, now: datetime):
            # GIVEN
            event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(
                    message="abc",
                    timestamp=int(
                        (
                            now
                            + module.PUT_LOG_EVENTS_CONSTRAINTS.max_future_time_delta
                            + timedelta(seconds=1)
                        ).timestamp()
                        * 1000  # Multiply by 1000 since CW expects milliseconds
                    ),
                ),
                size=3,
            )
            batch = CloudWatchLogEventBatch()

            # WHEN
            with raises(CloudWatchLogEventRejectedException) as raised_exc:
                batch._validate_log_event_can_be_added(event)

            # THEN
            assert raised_exc.value.batch_full is False
            assert raised_exc.value.reason is not None
            assert (
                f"Ignoring log event that is too far in the future (max {module.PUT_LOG_EVENTS_CONSTRAINTS.max_future_time_delta.total_seconds()}s"
                in raised_exc.value.reason
            )

        @mark.parametrize(
            ("event_time", "batch_min_datetime", "batch_max_datetime"),
            (
                param(
                    datetime(2000, 1, 1) - timedelta(seconds=1),
                    datetime(2000, 1, 1),
                    datetime(2000, 1, 1) + module.PUT_LOG_EVENTS_CONSTRAINTS.max_time_span_in_batch,
                    id="too far in past",
                ),
                param(
                    datetime(2000, 1, 1) + timedelta(seconds=1),
                    datetime(2000, 1, 1) - module.PUT_LOG_EVENTS_CONSTRAINTS.max_time_span_in_batch,
                    datetime(2000, 1, 1),
                    id="too far in future",
                ),
            ),
        )
        def test_exceed_batch_timespan(
            self,
            event_time: datetime,
            batch_min_datetime: datetime,
            batch_max_datetime: datetime,
            datetime_mock: MagicMock,
        ):
            # GIVEN
            batch = CloudWatchLogEventBatch()
            batch.min_timestamp_ms = int(batch_min_datetime.timestamp())
            batch.max_timestamp_ms = int(batch_max_datetime.timestamp())
            event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(
                    message="abc",
                    # Multiply by 1000 since CW expects milliseconds
                    timestamp=int(event_time.timestamp()) * 1000,
                ),
                size=3,
            )
            datetime_mock.now.return_value = event_time

            # WHEN
            with raises(CloudWatchLogEventRejectedException) as raised_exc:
                batch._validate_log_event_can_be_added(event)

            # THEN
            assert raised_exc.value.batch_full is False
            assert raised_exc.value.reason is not None
            assert (
                f"Ignoring log event that would exceed the max allowed time span in a batch of {module.PUT_LOG_EVENTS_CONSTRAINTS.max_time_span_in_batch.total_seconds()}s"
                in raised_exc.value.reason
            )

        def test_too_far_past_max_past_time_delta(self, now: datetime):
            # GIVEN
            batch = CloudWatchLogEventBatch()
            event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(
                    message="abc",
                    timestamp=int(
                        (
                            now
                            - module.PUT_LOG_EVENTS_CONSTRAINTS.max_past_time_delta
                            - timedelta(seconds=1)
                        ).timestamp()
                        * 1000  # Multiply by 1000 since CW expects milliseconds
                    ),
                ),
                size=3,
            )

            # WHEN
            with raises(CloudWatchLogEventRejectedException) as raised_exc:
                batch._validate_log_event_can_be_added(event)

            # THEN
            assert raised_exc.value.batch_full is False
            assert raised_exc.value.reason is not None
            assert (
                f"Ignoring log event that is older than {module.PUT_LOG_EVENTS_CONSTRAINTS.max_past_time_delta.days} days"
                in raised_exc.value.reason
            )


class TestCloudWatchLogEventPartitioner:
    @fixture
    def event(self) -> PartitionedCloudWatchLogEvent:
        return PartitionedCloudWatchLogEvent(
            log_event=CloudWatchLogEvent(
                message="abc",
                timestamp=123,
            ),
            size=3,
        )

    @fixture
    def deque_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(module, "deque") as cls_mock:
            yield cls_mock.return_value

    class TestNext:
        @fixture(autouse=True)
        def process_raw_event_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(CloudWatchLogEventPartitioner, "_partition_raw_event") as mock:
                yield mock

        def test_pops_processed_queue(
            self,
            event: PartitionedCloudWatchLogEvent,
            process_raw_event_mock: MagicMock,
            deque_mock: MagicMock,
        ):
            # GIVEN
            event_processor = CloudWatchLogEventPartitioner(raw_deque=deque())
            popleft_mock: MagicMock = deque_mock.popleft
            popleft_mock.return_value = event

            # WHEN
            result = event_processor.next()

            # THEN
            assert result == event
            popleft_mock.assert_called_once()
            process_raw_event_mock.assert_not_called()

        def test_processes_raw_event(
            self,
            event: PartitionedCloudWatchLogEvent,
            process_raw_event_mock: MagicMock,
            deque_mock: MagicMock,
        ):
            # GIVEN
            event_processor = CloudWatchLogEventPartitioner(raw_deque=deque())
            second_event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(message="def", timestamp=345),
                size=3,
            )
            process_raw_event_mock.return_value = [
                event,
                second_event,
            ]
            deque_mock.popleft.side_effect = IndexError()

            # WHEN
            result = event_processor.next()

            # THEN
            assert result == event
            deque_mock.popleft.assert_called_once()
            process_raw_event_mock.assert_called_once()
            deque_mock.extend.assert_called_once_with([second_event])

    class TestProcessRawEvent:
        @fixture
        def chunk_string_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(CloudWatchLogEventPartitioner, "_chunk_string") as mock:
                yield mock

        def test_process_raw_event_within_size_constraint(
            self,
            deque_mock: MagicMock,
            chunk_string_mock: MagicMock,
        ):
            # GIVEN
            raw_event = FormattedLogEntry(111, "aaa")
            deque_mock.popleft.return_value = raw_event
            chunk_string_mock.return_value = [(raw_event.message, 3)]
            event_processor = CloudWatchLogEventPartitioner(raw_deque=deque_mock)

            # WHEN
            result = event_processor._partition_raw_event()

            # THEN
            assert result == [
                PartitionedCloudWatchLogEvent(
                    log_event=CloudWatchLogEvent(
                        timestamp=raw_event.timestamp,
                        message=raw_event.message,
                    ),
                    size=3,
                )
            ]
            deque_mock.popleft.assert_called_once()
            chunk_string_mock.assert_called_once_with(
                raw_event.message,
                module.PUT_LOG_EVENTS_CONSTRAINTS.max_log_event_size,
            )

        def test_process_raw_event_over_size_constraint(
            self,
            deque_mock: MagicMock,
            chunk_string_mock: MagicMock,
        ):
            # GIVEN
            chunks = [
                (
                    "a" * module.PUT_LOG_EVENTS_CONSTRAINTS.max_log_event_size,
                    module.PUT_LOG_EVENTS_CONSTRAINTS.max_log_event_size,
                ),
                (
                    "a",
                    1,
                ),
            ]
            chunk_string_mock.return_value = chunks
            raw_event = FormattedLogEntry(111, "".join(msg for msg, _ in chunks))
            deque_mock.popleft.return_value = raw_event
            event_processor = CloudWatchLogEventPartitioner(raw_deque=deque_mock)

            # WHEN
            result = event_processor._partition_raw_event()

            # THEN
            assert result == [
                PartitionedCloudWatchLogEvent(
                    log_event=CloudWatchLogEvent(timestamp=raw_event.timestamp, message=msg),
                    size=size,
                )
                for msg, size in chunks
            ]
            deque_mock.popleft.assert_called_once()
            chunk_string_mock.assert_called_once_with(
                raw_event.message,
                module.PUT_LOG_EVENTS_CONSTRAINTS.max_log_event_size,
            )

    class TestChunkString:
        def test_throws_when_chunk_size_too_small(self):
            # WHEN
            with raises(AssertionError) as raised_err:
                CloudWatchLogEventPartitioner._chunk_string("test", 1)

            # THEN
            assert (
                "Chunk size too small (1). Must be at least 4 bytes to handle all UTF-8 characters."
                == str(raised_err.value)
            )

        def test_throws_when_cannot_find_utf8_code_point_start_byte(self):
            # GIVEN
            fake_string = MagicMock()
            # Create a "string" that just has a bunch of continuation bytes in UTF-8 (i.e. they all start with bit sequence: 10)
            fake_string.encode.return_value = bytes([0x80] * 10)

            # WHEN
            with raises(ValueError) as raised_err:
                CloudWatchLogEventPartitioner._chunk_string(fake_string, 5)

            # THEN
            assert (
                "Cannot chunk UTF-8 string: could not find first byte of code point between index 0 and 5"
                == str(raised_err.value)
            )

        @mark.parametrize(
            ("chunk_size", "message", "expected"),
            (
                param(
                    10,
                    "hello world",
                    [
                        ("hello worl", 10),
                        ("d", 1),
                    ],
                    id="ascii",
                ),
                param(
                    22,
                    "😀😃😄😁😆😅🤣😂🙂🙃😉😊😇🥰😍🤪",
                    [
                        ("😀😃😄😁😆", 20),
                        ("😅🤣😂🙂🙃", 20),
                        ("😉😊😇🥰😍", 20),
                        ("🤪", 4),
                    ],
                    id="emoji",
                ),
                param(
                    64,
                    "هذا اختبار يتم استخدامه للتحقق من أن الكود الذي يتعامل مع تقسيم النص يولد الأجزاء التي لا تزال نصًا صالحًا. لا يتحقق هذا الاختبار من أن القطع تحافظ على مجموعات حروف الحروف الأصلية.",
                    [
                        ("هذا اختبار يتم استخدامه للتحقق من أ", 64),
                        ("ن الكود الذي يتعامل مع تقسيم النص ي", 63),
                        ("ولد الأجزاء التي لا تزال نصًا صالحً", 64),
                        ("ا. لا يتحقق هذا الاختبار من أن القطع", 64),
                        (" تحافظ على مجموعات حروف الحروف الأص", 64),
                        ("لية.", 7),
                    ],
                    id="unicode-arabic",
                ),
                param(
                    32,
                    "이것은 텍스트 청킹을 처리하는 코드가 여전히 유효한 텍스트인 청크를 생성하는지 확인하는 데 사용되는 테스트입니다.",
                    [
                        ("이것은 텍스트 청킹을 ", 30),
                        ("처리하는 코드가 여전히", 32),
                        (" 유효한 텍스트인 청크", 30),
                        ("를 생성하는지 확인하는", 32),
                        (" 데 사용되는 테스트입", 30),
                        ("니다.", 7),
                    ],
                    id="unicode-korean",
                ),
            ),
        )
        def test_chunks_string_correctly(
            self, chunk_size: int, message: str, expected: list[tuple[str, int]]
        ):
            # WHEN
            actual = CloudWatchLogEventPartitioner._chunk_string(message, chunk_size)

            # THEN
            assert actual == expected


class TestCloudWatchLogStreamThread:
    @fixture
    def log_event_queue(self) -> MagicMock:
        return MagicMock()

    @fixture
    def log_group_name(self) -> str:
        return "log_group"

    @fixture
    def log_stream_name(self) -> str:
        return "log_stream"

    @fixture
    def stop_event(self) -> MagicMock:
        return MagicMock()

    @fixture
    def log_group_retention_in_days(self) -> int:
        return 10

    @fixture
    def describe_log_groups_response(
        self,
        log_group_name: str,
        log_group_retention_in_days: int,
    ) -> dict:
        return {
            "logGroups": [
                {
                    "logGroupName": log_group_name,
                    "retentionInDays": log_group_retention_in_days,
                },
            ],
        }

    @fixture
    def mock_logs_client(
        self,
        logs_client: MagicMock,
        describe_log_groups_response: dict,
    ) -> Generator[MagicMock, None, None]:
        with patch.object(logs_client, "describe_log_groups") as mock_describe_log_groups:
            mock_describe_log_groups.return_value = describe_log_groups_response
            yield logs_client

    @fixture
    def cloud_watch_log_stream_thread(
        self,
        mock_logs_client: MagicMock,
        log_event_queue: MagicMock,
        log_group_name: str,
        log_stream_name: str,
        stop_event: MagicMock,
    ) -> CloudWatchLogStreamThread:
        return CloudWatchLogStreamThread(
            logs_client=mock_logs_client,
            log_event_queue=log_event_queue,
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            stop_event=stop_event,
        )

    @fixture(autouse=True)
    def sleep_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(module, "sleep") as sleep_mock:
            yield sleep_mock

    def test_max_log_events_per_request(
        self,
    ) -> None:
        """
        Asserts that the constant used as an upper-bound for the number of log events per
        PutLogEvents request is correct.
        """
        assert module.PUT_LOG_EVENTS_CONSTRAINTS.max_events_per_batch == 10000

    def test_max_put_log_events_per_stream_sec(
        self,
    ) -> None:
        """
        Asserts that the constant used as an upper-bound for the number of PutLogEvents API requests
        per CloudWatch log stream per second is correct.
        """
        assert CloudWatchLogStreamThread.MAX_PUT_LOG_EVENTS_PER_STREAM_SEC == 5

    @mark.parametrize(
        argnames=("stop_event_is_set", "log_event_queue_len", "expected_return_value"),
        argvalues=(
            # Queue is empty, but stop event is not set so we should still be running
            (False, 0, True),
            # Stop event is not set and queue is non-empty, so we should still be running
            (False, 1, True),
            # Stop event is set and queue is empty, so we should no longer be running
            (True, 0, False),
            # Stop event is set, but there is still one log event, so we should loop again
            (True, 1, True),
        ),
    )
    def test_is_running(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        stop_event: MagicMock,
        log_event_queue: MagicMock,
        stop_event_is_set: bool,
        log_event_queue_len: int,
        expected_return_value: bool,
    ) -> None:
        """
        Asserts that CloudWatchLogStreamThread._is_running() returns true iff
        the stop event has not been set or the log event queue is non-empty.
        """
        # GIVEN
        stop_event_is_set_mock: MagicMock = stop_event.is_set
        # Simulate a single loop iteration
        stop_event_is_set_mock.return_value = stop_event_is_set
        # Mock len(log_event_queue) to return 0 so the loop exits
        log_event_queue_len_mock: MagicMock = log_event_queue.__len__
        log_event_queue_len_mock.return_value = log_event_queue_len

        # WHEN
        result = cloud_watch_log_stream_thread._is_running()

        # THEN
        assert result == expected_return_value

    def test_run_with_log_events(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        sleep_mock: MagicMock,
    ) -> None:
        """
        Asserts that the CloudWatchLogStreamThread.run() calls
        CloudWatchLogStreamThread._upload_logs() with logs returned by
        CloudWatchLogStreamThread._collect_logs() and does not sleep.
        """
        # GIVEN
        collected_log_events: list[CloudWatchLogEvent] = [
            CloudWatchLogEvent(
                message="a msg",
                timestamp=123,
            )
        ]

        with (
            patch.object(cloud_watch_log_stream_thread, "_is_running") as is_running_mock,
            patch.object(cloud_watch_log_stream_thread, "_collect_logs") as collect_logs_mock,
            patch.object(cloud_watch_log_stream_thread, "_upload_logs") as upload_logs_mock,
        ):
            # Make the loop run once
            is_running_mock.side_effect = [True, False]
            # Mock the collected log events
            collect_logs_mock.return_value = collected_log_events

            # WHEN
            cloud_watch_log_stream_thread.run()

        # THEN
        collect_logs_mock.assert_called_once_with()
        upload_logs_mock.assert_called_once_with(log_events=collected_log_events)
        sleep_mock.assert_not_called()
        # CloudWatchLogStreamThread._is_running() should be called twice - once for the first loop
        # iteration, and again to see if the loop should be run again.
        assert is_running_mock.call_count == 2

    def test_run_without_log_events(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        sleep_mock: MagicMock,
    ) -> None:
        """
        Asserts that the CloudWatchLogStreamThread.run() does not call
        CloudWatchLogStreamThread._upload_logs() if no logs are returned by
        CloudWatchLogStreamThread._collect_logs() and instead sleeps for 0.2s.
        """
        # GIVEN
        collected_log_events: list[CloudWatchLogEvent] = []

        with (
            patch.object(cloud_watch_log_stream_thread, "_is_running") as is_running_mock,
            patch.object(cloud_watch_log_stream_thread, "_collect_logs") as collect_logs_mock,
            patch.object(cloud_watch_log_stream_thread, "_upload_logs") as upload_logs_mock,
        ):
            # Make the loop run once
            is_running_mock.side_effect = [True, False]
            # Mock the collected log events
            collect_logs_mock.return_value = collected_log_events

            # WHEN
            cloud_watch_log_stream_thread.run()

        # THEN
        collect_logs_mock.assert_called_once_with()
        upload_logs_mock.assert_not_called()
        sleep_mock.assert_called_once_with(0.2)
        # CloudWatchLogStreamThread._is_running() should be called twice - once for the first loop
        # iteration, and again to see if the loop should be run again.
        assert is_running_mock.call_count == 2

    def test_upload_logs_success(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        logs_client: MagicMock,
        stop_event: MagicMock,
        log_group_name: str,
        log_stream_name: str,
    ) -> None:
        """
        Asserts that CloudWatchLogStreamThread._upload_logs():

        1.  Call CloudWatchLogStreamThread._throttle_put_log_events() to ensure that the CloudWatch
            PutLogEvents API request limit per-log-stream is not exceeded
        2.  Calls the boto3 logs client's "put_log_events()" method to make a PutLogEvents API
            request containing the supplied log events
        3.  Creates a PutLogEvents CloudWatch API request that puts the log events to the log group
            and log stream supplied in the constructor.
        """
        # GIVEN
        logs_client_put_log_events: MagicMock = logs_client.put_log_events
        stop_event_is_set: MagicMock = stop_event.is_set
        stop_event_is_set.return_value = False
        log_events: list[CloudWatchLogEvent] = [
            CloudWatchLogEvent(
                message="msg",
                timestamp=1,
            ),
        ]

        with patch.object(
            cloud_watch_log_stream_thread,
            "_throttle_put_log_events",
        ) as throttle_mock:
            # WHEN
            cloud_watch_log_stream_thread._upload_logs(log_events=log_events)

        # THEN
        throttle_mock.assert_called_once_with()
        logs_client_put_log_events.assert_called_once_with(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=log_events,
        )

    def test_upload_logs_boto_exception_before_stop(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        logs_client: MagicMock,
        stop_event: MagicMock,
        mock_module_logger: MagicMock,
        sleep_mock: MagicMock,
    ) -> None:
        """
        Asserts that when:

        1.  The stop event is not set
        2.  CloudWatchLogStreamThread._upload_logs() calls the boto3 logs client's
            "put_log_events()" method and an exception is raised

        Then:

        1.  The method repeats this log upload attempt indefinitely until it succeeds.
        2.  The method sleeps >= 1s to allow for transient error conditions to clear
        """
        # GIVEN
        logs_client_put_log_events: MagicMock = logs_client.put_log_events
        stop_event_is_set: MagicMock = stop_event.is_set
        stop_event_is_set.return_value = False
        log_events: list[CloudWatchLogEvent] = [
            CloudWatchLogEvent(
                message="msg",
                timestamp=1,
            ),
        ]
        mock_module_logger_error: MagicMock = mock_module_logger.error

        # Simulate 1000 boto3 put_log_events exceptions followed by success
        next_sequence_token = "next_sequence_token"
        put_log_events_success_response = {"nextSequenceToken": next_sequence_token}
        put_log_events_exception = Exception("exception msg")
        num_exceptions = 1000
        logs_client_put_log_events.side_effect = itertools.chain(
            # Simulate large number of successive errors
            itertools.repeat(object=put_log_events_exception, times=num_exceptions),
            # followed by a success response
            [put_log_events_success_response],
        )

        with (
            patch.object(
                cloud_watch_log_stream_thread, "_throttle_put_log_events"
            ) as throttle_mock,
        ):
            # WHEN
            cloud_watch_log_stream_thread._upload_logs(log_events=log_events)

            # THEN
            throttle_mock.assert_called_once_with()
            assert mock_module_logger_error.call_count == num_exceptions
            mock_module_logger_error.assert_has_calls(
                [
                    call(
                        "Error uploading CloudWatch logs (sleeping %ds): %s",
                        CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS,
                        put_log_events_exception,
                        stack_info=True,
                    )
                ]
                * num_exceptions
            )
            assert sleep_mock.call_count == num_exceptions

            class GreaterThanOne:
                def __eq__(self, other: Any) -> bool:
                    return other >= 1

            sleep_mock.assert_has_calls([call(GreaterThanOne())] * num_exceptions)

    def test_upload_logs_boto_exception_after_stop_recovery(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        logs_client: MagicMock,
        stop_event: MagicMock,
        mock_module_logger: MagicMock,
        sleep_mock: MagicMock,
    ) -> None:
        """
        Asserts that when:

        1.  The stop event is set
        2.  CloudWatchLogStreamThread._upload_logs() calls the boto3 logs client's
            "put_log_events()" method and an exception is raised

        Then, the method re-attempts the upload until it succeeds or at most 5 attempts.

        This test case covers the case where the retry succeeds before 5 attempts
        """
        # GIVEN
        logs_client_put_log_events: MagicMock = logs_client.put_log_events
        stop_event_is_set: MagicMock = stop_event.is_set
        stop_event_is_set.return_value = True
        log_events: list[CloudWatchLogEvent] = [
            CloudWatchLogEvent(
                message="msg",
                timestamp=1,
            ),
        ]
        mock_module_logger_error: MagicMock = mock_module_logger.error

        # Simulate 3 boto3 put_log_events exceptions followed by success
        next_sequence_token = "next_sequence_token"
        put_log_events_success_response = {"nextSequenceToken": next_sequence_token}
        put_log_events_exception = Exception("exception msg")
        num_exceptions = 3
        logs_client_put_log_events.side_effect = itertools.chain(
            # Simulate large number of successive errors
            itertools.repeat(object=put_log_events_exception, times=num_exceptions),
            # followed by a success response
            [put_log_events_success_response],
        )

        with (
            patch.object(
                cloud_watch_log_stream_thread, "_throttle_put_log_events"
            ) as throttle_mock,
        ):
            # WHEN
            cloud_watch_log_stream_thread._upload_logs(log_events=log_events)

            # THEN
            throttle_mock.assert_called_once_with()
            assert mock_module_logger_error.call_count == num_exceptions
            mock_module_logger_error.assert_has_calls(
                [
                    call(
                        "Error uploading CloudWatch logs (sleeping %ds, %d attempts remaining): %s",
                        CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS,
                        stop_attempts,
                        put_log_events_exception,
                        stack_info=True,
                    )
                    for stop_attempts in range(4, 4 - num_exceptions, -1)
                ]
            )
            assert sleep_mock.call_count == num_exceptions

            class GreaterThanOne:
                def __eq__(self, other: Any) -> bool:
                    return other >= 1

            sleep_mock.assert_has_calls([call(GreaterThanOne())] * num_exceptions)

    def test_upload_logs_boto_exception_after_stop_fail(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        logs_client: MagicMock,
        stop_event: MagicMock,
        mock_module_logger: MagicMock,
        sleep_mock: MagicMock,
    ) -> None:
        """
        Asserts that when:

        1.  The stop event is set
        2.  CloudWatchLogStreamThread._upload_logs() calls the boto3 logs client's
            "put_log_events()" method and an exception is raised

        Then, the method re-attempts the upload until it succeeds or at most 5 attempts.

        This test case covers the case where the retry fails after 5 attempts
        """
        # GIVEN
        logs_client_put_log_events: MagicMock = logs_client.put_log_events
        stop_event_is_set: MagicMock = stop_event.is_set
        stop_event_is_set.return_value = True
        log_events: list[CloudWatchLogEvent] = [
            CloudWatchLogEvent(
                message="msg",
                timestamp=1,
            ),
        ]
        mock_module_logger_error: MagicMock = mock_module_logger.error

        # Simulate 5 boto3 put_log_events exceptions
        put_log_events_exception = Exception("exception msg")
        num_exceptions = 5
        # Simulate large number of successive errors
        logs_client_put_log_events.side_effect = itertools.repeat(
            object=put_log_events_exception, times=num_exceptions
        )

        # WHEN
        cloud_watch_log_stream_thread._upload_logs(log_events=log_events)

        # THEN
        assert mock_module_logger_error.call_count == num_exceptions + 1
        mock_module_logger_error.assert_has_calls(
            [
                call(
                    "Error uploading CloudWatch logs (sleeping %ds, %d attempts remaining): %s",
                    CloudWatchLogStreamThread.PUT_LOG_EVENTS_ERROR_DELAY_SECONDS,
                    stop_attempts,
                    put_log_events_exception,
                    stack_info=True,
                )
                for stop_attempts in range(4, -1, -1)
            ]
        )
        mock_module_logger_error.assert_any_call("Unable to upload logs due to task ending")
        assert sleep_mock.call_count == num_exceptions

        class GreaterThanOne:
            def __eq__(self, other: Any) -> bool:
                return other >= 1

        sleep_mock.assert_has_calls([call(GreaterThanOne())] * num_exceptions)

    @mark.parametrize(
        argnames=(
            "initial_prev_request_times",
            "cur_time",
            "expected_sleep_amt",
            "expected_next_prev_request_times",
        ),
        argvalues=(
            (
                (4.1, 4.3),
                4.4,
                # Will not sleep because there are only two requests that are
                # < 1s old.
                None,
                (4.1, 4.3, 4.4),
            ),
            (
                (104.12, 105.3),
                105.8,
                # Will not sleep because there are only two requests that are
                # < 1s old.
                None,
                (104.12, 105.3, 105.8),
            ),
            (
                (31421.12, 31421.3, 31421.36, 31421.41, 31421.53, 31421.6),
                31422.15,
                # There are 5 requests < 1s old so the method should sleep by
                # 1 - (31422.15 - 31421.3) = 0.1499999999978172
                0.1499999999978172,
                # Earliest timestamp should get truncated and newest added to
                # the right
                (31421.3, 31421.36, 31421.41, 31421.53, 31421.6, 31422.15),
            ),
            (
                (31421.3, 31421.36, 31421.41, 31421.53, 31421.6),
                31422.33,
                # Will not sleep because there are only four requests that are
                # < 1s old.
                None,
                # Earliest timestamp should get truncated (older than 1s) and
                # newest added to the right
                (31421.36, 31421.41, 31421.53, 31421.6, 31422.33),
            ),
            (
                (31421.3, 31421.36, 31421.41, 31421.53, 31421.6),
                31424,
                # Will not sleep because all five requests are > 1s old.
                None,
                # All prior timestamps are purged and the current timestamp
                # is added
                (31424,),
            ),
        ),
        ids=(
            "two-within-sec",
            "one-older-than-sec-one-within-sec",
            "six-within-sec",
            "one-older-than-sec-five-within-sec",
            "five-older-than-sec",
        ),
    )
    def test_throttle_put_log_events(
        self,
        cloud_watch_log_stream_thread: CloudWatchLogStreamThread,
        initial_prev_request_times: list[float],
        cur_time: float,
        expected_sleep_amt: Optional[float],
        expected_next_prev_request_times: list[float],
        sleep_mock: MagicMock,
        mock_module_logger: MagicMock,
    ) -> None:
        # GIVEN
        mock_module_logger_debug: MagicMock = mock_module_logger.debug
        cloud_watch_log_stream_thread._prev_request_times = deque(initial_prev_request_times)

        with patch.object(module, "monotonic", return_value=cur_time) as monotonic_mock:
            # WHEN
            cloud_watch_log_stream_thread._throttle_put_log_events()

            # THEN
            monotonic_mock.assert_called_once_with()
            if expected_sleep_amt is not None:
                sleep_mock.assert_called_once_with(expected_sleep_amt)
                mock_module_logger_debug.assert_called_once_with(
                    "PutLogEvents limit reached (%d per second), sleeping %.2f seconds",
                    CloudWatchLogStreamThread.MAX_PUT_LOG_EVENTS_PER_STREAM_SEC,
                    expected_sleep_amt,
                )
            else:
                sleep_mock.assert_not_called()

            assert (
                tuple(cloud_watch_log_stream_thread._prev_request_times)
                == expected_next_prev_request_times
            )

    class TestCollectLogs:
        @fixture
        def log_event_partitioner_mock(self) -> Generator[MagicMock, None, None]:
            with patch.object(module, "CloudWatchLogEventPartitioner") as cls_mock:
                yield cls_mock.return_value

        @fixture
        def cw_thread(
            self,
            mock_logs_client: MagicMock,
            log_event_queue: MagicMock,
            log_group_name: str,
            log_stream_name: str,
            stop_event: MagicMock,
            # Explicitly request fixture to mock out the log event processor
            log_event_partitioner_mock: MagicMock,
        ) -> CloudWatchLogStreamThread:
            return CloudWatchLogStreamThread(
                logs_client=mock_logs_client,
                log_event_queue=log_event_queue,
                log_group_name=log_group_name,
                log_stream_name=log_stream_name,
                stop_event=stop_event,
            )

        def test_collects_log_event(
            self,
            log_event_partitioner_mock: MagicMock,
            cw_thread: CloudWatchLogStreamThread,
        ):
            # GIVEN
            expected_log_event = PartitionedCloudWatchLogEvent(
                log_event=CloudWatchLogEvent(timestamp=123, message="abc"),
                size=3,
            )
            log_event_partitioner_mock.next.side_effect = [
                expected_log_event,
                IndexError(),
            ]

            # WHEN
            with patch.object(
                module.CloudWatchLogEventBatch, "_validate_log_event_can_be_added"
            ) as batch_log_event_can_be_added_mock:
                result = cw_thread._collect_logs()

            # THEN
            assert result == [expected_log_event.log_event]
            assert log_event_partitioner_mock.next.call_count == 2
            batch_log_event_can_be_added_mock.assert_called_once_with(expected_log_event)

        @mark.parametrize(
            ("batch_full", "reason"),
            (
                param(
                    True,
                    None,
                    id="batch full only",
                ),
                param(
                    False,
                    "The reason",
                    id="reason only",
                ),
                param(
                    True,
                    "batch is full",
                    id="batch full with reason",
                ),
            ),
        )
        def test_rejected_log_event(
            self,
            batch_full: bool,
            reason: Optional[str],
            cw_thread: CloudWatchLogStreamThread,
            log_event_partitioner_mock: MagicMock,
        ):
            # GIVEN
            log_event_partitioner_mock.next.side_effect = [
                log_event_partitioner_mock.next.return_value,
                IndexError(),
            ]
            side_effect = CloudWatchLogEventRejectedException(batch_full=batch_full, reason=reason)

            # WHEN
            with patch.object(
                module.CloudWatchLogEventBatch,
                "_validate_log_event_can_be_added",
                side_effect=side_effect,
            ) as batch_log_event_can_be_added_mock:
                result = cw_thread._collect_logs()

            # THEN
            assert result == []

            expected_log_event = log_event_partitioner_mock.next.return_value
            if batch_full:
                log_event_partitioner_mock.next.assert_called_once()
                batch_log_event_can_be_added_mock.assert_called_once_with(expected_log_event)
                log_event_partitioner_mock.appendleft.assert_any_call(expected_log_event)
            if reason:
                log_event_partitioner_mock.appendleft.assert_any_call(
                    PartitionedCloudWatchLogEvent(
                        log_event=CloudWatchLogEvent(
                            timestamp=expected_log_event.log_event["timestamp"],
                            message=reason,
                        ),
                        size=len(reason.encode("utf-8")),
                    )
                )


class TestCloudWatchHandler:
    """Tests for the CloudWatchHandler class"""

    @fixture(autouse=True)
    def mock_cloud_watch_log_stream_thread_cls(
        self,
    ) -> Generator[MagicMock, None, None]:
        with patch.object(
            module, "CloudWatchLogStreamThread"
        ) as mock_cloud_watch_log_stream_thread:
            yield mock_cloud_watch_log_stream_thread

    @fixture
    def mock_cloud_watch_log_stream_thread(
        self,
        mock_cloud_watch_log_stream_thread_cls: MagicMock,
    ) -> MagicMock:
        return mock_cloud_watch_log_stream_thread_cls.return_value

    @fixture
    def handler(
        self,
        logs_client: MagicMock,
        log_cw_group_name: str,
        log_cw_stream_name: str,
    ) -> CloudWatchHandler:
        return CloudWatchHandler(
            logs_client=logs_client,
            log_group_name=log_cw_group_name,
            log_stream_name=log_cw_stream_name,
        )

    def test_stop_event_creation(self, handler: CloudWatchHandler) -> None:
        """
        Asserts that initializing a CloudWatchHandler instance creates a threading.Event
        and stores it in a "_stop_event" attribute.
        """
        # THEN
        assert isinstance(handler._stop_event, Event)
        assert not handler._stop_event.is_set()

    def test_thread_creation_and_start(
        self,
        logs_client: MagicMock,
        handler: CloudWatchHandler,
        mock_cloud_watch_log_stream_thread_cls: MagicMock,
        mock_cloud_watch_log_stream_thread: MagicMock,
        log_cw_group_name: str,
        log_cw_stream_name: str,
    ) -> None:
        """
        Tests that when constructing a CloudWatchHandler instance, it creates a
        CloudWatchLogStreamThread and starts it.
        """
        # GIVEN
        mock_cloud_watch_log_stream_thread_start: MagicMock = (
            mock_cloud_watch_log_stream_thread.start
        )

        # THEN
        mock_cloud_watch_log_stream_thread_cls.assert_called_once_with(
            logs_client=logs_client,
            log_group_name=log_cw_group_name,
            log_stream_name=log_cw_stream_name,
            log_event_queue=handler._log_event_queue,
            stop_event=handler._stop_event,
            daemon=True,
        )
        mock_cloud_watch_log_stream_thread_start.assert_called_once_with()

    def test_emit(
        self,
        handler: CloudWatchHandler,
    ) -> None:
        """
        Tests that when calling CloudWatchHandler.emit() it converts the LogRecord into
        a corresponding FormattedLogEntry instance and adds it to the log event queue.

        In particular, the timestamp of the LogRecord should be converted from floating point
        unix timestamp (milliseconds since epoch Jan 1, 1970 UTC) to an integer of nanoseconds.
        The log message should be copied verbatim.
        """
        # GIVEN
        record = LogRecord(
            name="someloggername",
            level=INFO,
            pathname=os.path.abspath(__file__),
            lineno=1,
            msg="a log message",
            args=tuple(),
            exc_info=None,
        )

        with patch.object(handler, "_log_event_queue") as log_queue_mock:
            log_queue_append_mock: MagicMock = log_queue_mock.append

            # WHEN
            handler.emit(record)

            # THEN
            log_queue_append_mock.assert_called_once_with(
                FormattedLogEntry(
                    timestamp=int(record.created * 1000),
                    message=record.message,
                )
            )

    def test_emit_exception(
        self,
        handler: CloudWatchHandler,
    ) -> None:
        """
        Tests that when calling CloudWatchHandler.emit() it converts the LogRecord into
        a corresponding FormattedLogEntry instance and attempts to append it to the log event queue.

        If attempting to add it to a queue raises an exception, the Handler.handleError() method of
        the parent class should be called passing in the log record.
        """
        # GIVEN
        record = LogRecord(
            name="someloggername",
            level=INFO,
            pathname=os.path.abspath(__file__),
            lineno=1,
            msg="a log message",
            args=tuple(),
            exc_info=None,
        )
        exception = Exception("error msg")

        with (
            patch.object(handler, "_log_event_queue") as log_queue_mock,
            patch.object(handler, "handleError") as handle_error_mock,
        ):
            log_queue_append_mock: MagicMock = log_queue_mock.append
            log_queue_append_mock.side_effect = exception

            # WHEN
            handler.emit(record)

            # THEN
            log_queue_append_mock.assert_called_once_with(
                FormattedLogEntry(
                    timestamp=int(record.created * 1000),
                    message=record.message,
                )
            )
            handle_error_mock.assert_called_once_with(record)

    def test_close(
        self,
        handler: CloudWatchHandler,
        mock_cloud_watch_log_stream_thread: MagicMock,
    ) -> None:
        """Asserts that CloudWatchHandler.close():

        1.  Calls the the `set()` method of the stop event to notify the CloudWatchLogStreamThread
            that it should exit once the queue is drained
        2.  Calls the `join()` method of the CloudWatchLogStreamThread to block until that thread
            has exited.
        """
        # GIVEN
        mock_cloud_watch_log_stream_thread_join: MagicMock = mock_cloud_watch_log_stream_thread.join
        with patch.object(handler._stop_event, "set") as stop_event_set_mock:
            # WHEN
            handler.close()

        # THEN
        stop_event_set_mock.assert_called_once_with()
        mock_cloud_watch_log_stream_thread_join.assert_called_once_with()

    def test_context_mgr(
        self,
        handler: CloudWatchHandler,
    ) -> None:
        """
        Asserts that using CloudWatchHandler as a context manager has the following
        behaviors

        On entering the context: nothing
        On exiting the context: calls the 'close()' method on itself
        """

        # GIVEN
        with (patch.object(handler, "close") as close_mock,):
            # WHEN
            with handler:
                # THEN
                close_mock.assert_not_called()

            # WHEN (exiting context)
            # THEN
            close_mock.assert_called_once_with()

    def test_context_mgr_exception(
        self,
        handler: CloudWatchHandler,
    ) -> None:
        """
        Asserts that when using CloudWatchHandler as a context manager and an exception is
        raised while the context is active, that CloudWatchHandler still calls its 'close()'
        method.
        """

        # GIVEN
        with (patch.object(handler, "close") as close_mock,):
            with raises(Exception):
                with handler:
                    # WHEN
                    raise Exception("my exception")

            # THEN
            close_mock.assert_called_once_with()


@mark.parametrize(
    "mock_logger",
    [
        param(MagicMock(spec=logger_mod.ROOT_LOGGER), id="ROOT_LOGGER"),
    ],
)
def test_stream_cloudwatch(
    logs_client: MagicMock,
    log_cw_group_name: str,
    log_cw_stream_name: str,
    mock_module_logger: MagicMock,
    mock_logger: MagicMock,
) -> None:
    """
    Asserts that stream_cloudwatch_logs() function that returns a context-manager. The context
    manager should have the following behaiovrs:

    Entering the context-manager:

    1.  Creates a CloudWatchHandler instance and forwards the relevant function arguments to
        the initializer
    2.  Enters the context of the CloudWatchHandler
    3.  Creates and attaches a logging.Formatter instance with the expected default format string
        ("%(message)s")
    4.  Attaches the CloudWatchHandler to the given logger
    5.  Emits an INFO level log event to the module logger to indicate the CWL destination

    Exiting the context-manager:

    1.  Exits the context of the CloudWatchHandler
    2.  Removes the CloudWatchHandler handler from the given logger
    """
    # GIVEN
    with patch.object(module, "CloudWatchHandler") as handler_mock:
        handler: MagicMock = handler_mock.return_value
        handler_enter: MagicMock = handler.__enter__
        handler_enter.return_value = handler
        handler_exit: MagicMock = handler.__exit__
        handler_set_formatter_mock: MagicMock = handler.setFormatter
        handler_add_filter_mock: MagicMock = handler.addFilter
        ctx_mgr = stream_cloudwatch_logs(
            logs_client=logs_client,
            log_group_name=log_cw_group_name,
            log_stream_name=log_cw_stream_name,
            logger=mock_logger,
        )
        action_output_logger_add_handler: MagicMock = mock_logger.addHandler
        action_output_logger_remove_handler: MagicMock = mock_logger.removeHandler
        mock_module_logger_info: MagicMock = mock_module_logger.info

        # WHEN
        with ctx_mgr as ctx:
            # THEN
            assert ctx is handler
            handler_mock.assert_called_once_with(
                logs_client=logs_client,
                log_group_name=log_cw_group_name,
                log_stream_name=log_cw_stream_name,
            )

            handler_enter.assert_called_once_with()

            # Assert the formatter was attached
            assert handler_set_formatter_mock.call_count == 1
            formatter = handler_set_formatter_mock.call_args[0][0]
            assert isinstance(formatter, Formatter)
            # Assume the default formatting only contains the log message.
            # CloudWatch maintains timestamps for each log event already.
            assert formatter._fmt == "%(json)s"

            handler_add_filter_mock.assert_called_once()
            assert isinstance(
                handler_add_filter_mock.call_args.args[0], LogRecordStringTranslationFilter
            )

            action_output_logger_add_handler.assert_called_once_with(handler)
            action_output_logger_remove_handler.assert_not_called()

            mock_module_logger_info.assert_called_once_with(
                f"logs streamed to CWL target: {log_cw_group_name}/{log_cw_stream_name}"
            )

        handler_exit.assert_called_once()
        action_output_logger_remove_handler.assert_called_once_with(handler)
