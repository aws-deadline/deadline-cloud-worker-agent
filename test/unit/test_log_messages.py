# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import logging
import pytest
import sys

from unittest.mock import patch, MagicMock
from typing import Any, Union, Generator, Optional

import deadline_worker_agent as agent_module
from deadline_worker_agent.log_messages import (
    AgentInfoLogEvent,
    ApiRequestLogEvent,
    ApiResponseLogEvent,
    AwsCredentialsLogEvent,
    AwsCredentialsLogEventOp,
    BaseLogEvent,
    FilesystemLogEvent,
    FilesystemLogEventOp,
    MetricsLogEvent,
    MetricsLogEventSubtype,
    SessionLogEvent,
    SessionLogEventSubtype,
    SessionActionLogEvent,
    SessionActionLogEventSubtype,
    SessionActionLogKind,
    StringLogEvent,
    WorkerLogEvent,
    WorkerLogEventOp,
    LogRecordStringTranslationFilter,
)

from openjd.sessions import (
    LOG,
    LogContent,
)

# List tests alphabetically by the Log class being tested.
# This will make it easier to spot whether we've missed any.
TEST_RECORDS = (
    # "message, expected_dict, expected_desc, expected_message"
    pytest.param(
        "A string to be converted",
        {"message": "A string to be converted"},
        "",
        "A string to be converted",
        id="Basic string message",
    ),
    #
    #
    pytest.param(
        ApiRequestLogEvent(
            operation="TestOp",
            request_url="http://url",
            params={"p1": "foo", "p2": {"p3": "bar"}},
        ),
        {
            "ti": "📤",
            "type": "API",
            "subtype": "Req",
            "operation": "TestOp",
            "params": {"p1": "foo", "p2": {"p3": "bar"}},
            "request_url": "http://url",
        },
        "📤 API.Req 📤 ",
        "[TestOp] params={'p1': 'foo', 'p2': {'p3': 'bar'}} request_url=http://url",
        id="API Request; no resource",
    ),
    pytest.param(
        ApiRequestLogEvent(
            operation="TestOp",
            request_url="http://url",
            params={"p1": "foo", "p2": {"p3": "bar"}},
            deadline_resource={"fleet_id": "fleet-1234"},
        ),
        {
            "ti": "📤",
            "type": "API",
            "subtype": "Req",
            "operation": "TestOp",
            "params": {"p1": "foo", "p2": {"p3": "bar"}},
            "request_url": "http://url",
            "resource": {"fleet_id": "fleet-1234"},
        },
        "📤 API.Req 📤 ",
        "[TestOp] resource={'fleet_id': 'fleet-1234'} params={'p1': 'foo', 'p2': {'p3': 'bar'}} request_url=http://url",
        id="API Request; with resource",
    ),
    #
    #
    pytest.param(
        ApiResponseLogEvent(
            operation="TestOp",
            status_code="200",
            params={"p1": "foo", "p2": {"p3": "bar"}},
            request_id="1234-abcd",
        ),
        {
            "ti": "📥",
            "type": "API",
            "subtype": "Resp",
            "operation": "TestOp",
            "status_code": "200",
            "params": {"p1": "foo", "p2": {"p3": "bar"}},
            "request_id": "1234-abcd",
        },
        "📥 API.Resp 📥 ",
        "[TestOp](200) params={'p1': 'foo', 'p2': {'p3': 'bar'}} request_id=1234-abcd",
        id="API Response; no error",
    ),
    pytest.param(
        ApiResponseLogEvent(
            operation="TestOp",
            status_code="200",
            params={"p1": "foo", "p2": {"p3": "bar"}},
            request_id="1234-abcd",
            error={"Code": "ErrCode", "Message": "It dun blew up"},
        ),
        {
            "ti": "📥",
            "type": "API",
            "subtype": "Resp",
            "operation": "TestOp",
            "status_code": "200",
            "error": {"Code": "ErrCode", "Message": "It dun blew up"},
            "params": {"p1": "foo", "p2": {"p3": "bar"}},
            "request_id": "1234-abcd",
        },
        "📥 API.Resp 📥 ",
        "[TestOp](200) error={'Code': 'ErrCode', 'Message': 'It dun blew up'} params={'p1': 'foo', 'p2': {'p3': 'bar'}} request_id=1234-abcd",
        id="API Response; with error",
    ),
    #
    #
    pytest.param(
        AwsCredentialsLogEvent(
            op=AwsCredentialsLogEventOp.LOAD, resource="queue-1234", message="A message"
        ),
        {
            "ti": "🔑",
            "type": "AWSCreds",
            "subtype": "Load",
            "message": "A message",
            "resource": "queue-1234",
        },
        "🔑 AWSCreds.Load 🔑 ",
        "A message [queue-1234]",
        id="AwsCredentials Load",
    ),
    pytest.param(
        AwsCredentialsLogEvent(
            op=AwsCredentialsLogEventOp.QUERY,
            resource="queue-1234",
            message="A message",
            role_arn="arn:aws.../Role",
        ),
        {
            "ti": "🔑",
            "type": "AWSCreds",
            "subtype": "Query",
            "message": "A message",
            "resource": "queue-1234",
            "role_arn": "arn:aws.../Role",
        },
        "🔑 AWSCreds.Query 🔑 ",
        "A message [queue-1234][arn:aws.../Role]",
        id="AwsCredentials With RoleArn",
    ),
    pytest.param(
        AwsCredentialsLogEvent(
            op=AwsCredentialsLogEventOp.REFRESH,
            resource="queue-1234",
            message="A message",
            expiry="2024-01-01 00:00:00+00:00",
        ),
        {
            "ti": "🔑",
            "type": "AWSCreds",
            "subtype": "Refresh",
            "message": "A message",
            "expiry": "2024-01-01 00:00:00+00:00",
            "resource": "queue-1234",
        },
        "🔑 AWSCreds.Refresh 🔑 ",
        "A message (Expires: 2024-01-01 00:00:00+00:00) [queue-1234]",
        id="AwsCredentials With Expiry",
    ),
    pytest.param(
        AwsCredentialsLogEvent(
            op=AwsCredentialsLogEventOp.REFRESH,
            resource="queue-1234",
            message="A message",
            scheduled_time="2024-01-01 00:00:00+00:00",
        ),
        {
            "ti": "🔑",
            "type": "AWSCreds",
            "subtype": "Refresh",
            "message": "A message",
            "scheduled_time": "2024-01-01 00:00:00+00:00",
            "resource": "queue-1234",
        },
        "🔑 AWSCreds.Refresh 🔑 ",
        "A message (ScheduledTime: 2024-01-01 00:00:00+00:00) [queue-1234]",
        id="AwsCredentials With ScheduledTime",
    ),
    #
    #
    pytest.param(
        FilesystemLogEvent(
            op=FilesystemLogEventOp.READ,
            filepath="/tmp/filename.txt",
            message="A message",
        ),
        {
            "ti": "💾",
            "type": "FileSystem",
            "subtype": "Read",
            "message": "A message",
            "filepath": "/tmp/filename.txt",
        },
        "💾 FileSystem.Read 💾 ",
        "A message [/tmp/filename.txt]",
        id="Filesystem Read",
    ),
    pytest.param(
        FilesystemLogEvent(
            op=FilesystemLogEventOp.WRITE,
            filepath="/tmp/filename.txt",
            message="A message",
        ),
        {
            "ti": "💾",
            "type": "FileSystem",
            "subtype": "Write",
            "message": "A message",
            "filepath": "/tmp/filename.txt",
        },
        "💾 FileSystem.Write 💾 ",
        "A message [/tmp/filename.txt]",
        id="Filesystem Write",
    ),
    #
    #
    pytest.param(
        MetricsLogEvent(
            subtype=MetricsLogEventSubtype.SYSTEM,
            metrics={"m1": "10", "m2": "75"},
        ),
        {"ti": "📊", "type": "Metrics", "subtype": "System", "m1": "10", "m2": "75"},
        "📊 Metrics.System 📊 ",
        "m1 10 m2 75",
        id="Metrics",
    ),
    #
    #
    pytest.param(
        SessionLogEvent(
            subtype=SessionLogEventSubtype.STARTING,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            message="A message",
        ),
        {
            "ti": "🔷",
            "type": "Session",
            "subtype": "Starting",
            "session_id": "session-1234",
            "message": "A message",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🔷 Session.Starting 🔷 ",
        "[session-1234] A message [queue-1234/job-1234]",
        id="Session; no extras",
    ),
    pytest.param(
        SessionLogEvent(
            subtype=SessionLogEventSubtype.USER,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            message="A message",
            user="jobuser",
        ),
        {
            "ti": "🔷",
            "type": "Session",
            "subtype": "User",
            "session_id": "session-1234",
            "message": "A message",
            "user": "jobuser",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🔷 Session.User 🔷 ",
        "[session-1234] A message (User: jobuser) [queue-1234/job-1234]",
        id="Session; with user",
    ),
    pytest.param(
        SessionLogEvent(
            subtype=SessionLogEventSubtype.ADD,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            message="A message",
            action_ids=["sessionaction-1234", "sessionaction-abcd"],
            queued_action_count=12,
        ),
        {
            "ti": "🔷",
            "type": "Session",
            "subtype": "Add",
            "session_id": "session-1234",
            "message": "A message",
            "action_ids": ["sessionaction-1234", "sessionaction-abcd"],
            "queued_action_count": 12,
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🔷 Session.Add 🔷 ",
        "[session-1234] A message (ActionIds: ['sessionaction-1234', 'sessionaction-abcd']) (QueuedActionCount: 12) [queue-1234/job-1234]",
        id="Session Add",
    ),
    pytest.param(
        SessionLogEvent(
            subtype=SessionLogEventSubtype.REMOVE,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            message="A message",
            action_ids=["sessionaction-1234", "sessionaction-abcd"],
            queued_action_count=2,
        ),
        {
            "ti": "🔷",
            "type": "Session",
            "subtype": "Remove",
            "session_id": "session-1234",
            "message": "A message",
            "action_ids": ["sessionaction-1234", "sessionaction-abcd"],
            "queued_action_count": 2,
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🔷 Session.Remove 🔷 ",
        "[session-1234] A message (ActionIds: ['sessionaction-1234', 'sessionaction-abcd']) (QueuedActionCount: 2) [queue-1234/job-1234]",
        id="Session Remove",
    ),
    pytest.param(
        SessionLogEvent(
            subtype=SessionLogEventSubtype.LOGS,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            message="A message",
            log_dest="/tmp/logfile.txt",
        ),
        {
            "ti": "🔷",
            "type": "Session",
            "subtype": "Logs",
            "session_id": "session-1234",
            "message": "A message",
            "log_dest": "/tmp/logfile.txt",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🔷 Session.Logs 🔷 ",
        "[session-1234] A message (LogDestination: /tmp/logfile.txt) [queue-1234/job-1234]",
        id="Session; with user",
    ),
    #
    #
    pytest.param(
        SessionActionLogEvent(
            subtype=SessionActionLogEventSubtype.START,
            queue_id="queue-1234",
            job_id="job-1234",
            step_id="step-1234",
            task_id="task-1234",
            session_id="session-1234",
            action_log_kind=SessionActionLogKind.TASK_RUN,
            action_id="sessionaction-1234",
            message="A message",
        ),
        {
            "ti": "🟢",
            "type": "Action",
            "subtype": "Start",
            "session_id": "session-1234",
            "action_id": "sessionaction-1234",
            "kind": "TaskRun",
            "message": "A message",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
            "step_id": "step-1234",
            "task_id": "task-1234",
        },
        "🟢 Action.Start 🟢 ",
        "[session-1234](sessionaction-1234) A message (Kind: TaskRun) [queue-1234/job-1234/step-1234/task-1234]",
        id="SessionAction Start",
    ),
    pytest.param(
        SessionActionLogEvent(
            subtype=SessionActionLogEventSubtype.CANCEL,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            action_log_kind=SessionActionLogKind.ENV_ENTER,
            action_id="sessionaction-1234",
            message="A message",
        ),
        {
            "ti": "🟨",
            "type": "Action",
            "subtype": "Cancel",
            "session_id": "session-1234",
            "action_id": "sessionaction-1234",
            "kind": "EnvEnter",
            "message": "A message",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🟨 Action.Cancel 🟨 ",
        "[session-1234](sessionaction-1234) A message (Kind: EnvEnter) [queue-1234/job-1234]",
        id="SessionAction Cancel",
    ),
    pytest.param(
        SessionActionLogEvent(
            subtype=SessionActionLogEventSubtype.INTERRUPT,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            action_log_kind=SessionActionLogKind.ENV_EXIT,
            action_id="sessionaction-1234",
            message="A message",
        ),
        {
            "ti": "🟨",
            "type": "Action",
            "subtype": "Interrupt",
            "session_id": "session-1234",
            "action_id": "sessionaction-1234",
            "kind": "EnvExit",
            "message": "A message",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🟨 Action.Interrupt 🟨 ",
        "[session-1234](sessionaction-1234) A message (Kind: EnvExit) [queue-1234/job-1234]",
        id="SessionAction Interrupt",
    ),
    pytest.param(
        SessionActionLogEvent(
            subtype=SessionActionLogEventSubtype.END,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
            action_log_kind=SessionActionLogKind.TASK_RUN,
            action_id="sessionaction-1234",
            message="A message",
            status="SUCCESS",
        ),
        {
            "ti": "🟣",
            "type": "Action",
            "subtype": "End",
            "session_id": "session-1234",
            "action_id": "sessionaction-1234",
            "kind": "TaskRun",
            "message": "A message",
            "status": "SUCCESS",
            "queue_id": "queue-1234",
            "job_id": "job-1234",
        },
        "🟣 Action.End 🟣 ",
        "[session-1234](sessionaction-1234) A message (Status: SUCCESS) (Kind: TaskRun) [queue-1234/job-1234]",
        id="SessionAction End",
    ),
    #
    #
    pytest.param(
        WorkerLogEvent(
            op=WorkerLogEventOp.CREATE,
            farm_id="farm-1234",
            fleet_id="fleet-1234",
            message="A message",
        ),
        {
            "ti": "💻",
            "type": "Worker",
            "subtype": "Create",
            "message": "A message",
            "farm_id": "farm-1234",
            "fleet_id": "fleet-1234",
        },
        "💻 Worker.Create 💻 ",
        "A message [farm-1234/fleet-1234]",
        id="Worker; no extras",
    ),
    pytest.param(
        WorkerLogEvent(
            op=WorkerLogEventOp.ID,
            farm_id="farm-1234",
            fleet_id="fleet-1234",
            worker_id="worker-1234",
            message="A message",
        ),
        {
            "ti": "💻",
            "type": "Worker",
            "subtype": "ID",
            "message": "A message",
            "farm_id": "farm-1234",
            "fleet_id": "fleet-1234",
            "worker_id": "worker-1234",
        },
        "💻 Worker.ID 💻 ",
        "A message [farm-1234/fleet-1234/worker-1234]",
        id="Worker; with worker id",
    ),
)

# AgentInfo is a special snowflake in that it populates its own data, so we can't test it
# using the same pattern as other messages.


@pytest.mark.parametrize("message, expected_dict, expected_desc, expected_message", TEST_RECORDS)
@pytest.mark.parametrize(
    "level",
    (
        # No need to be exhaustive. A couple is sufficient
        pytest.param(logging.ERROR, id="error"),
        pytest.param(logging.INFO, id="info"),
    ),
)
def test_messages_logged(
    level: int,
    message: Union[str, BaseLogEvent],
    expected_dict: dict[str, Any],
    expected_desc: str,
    expected_message: str,
) -> None:
    # Test all of the log events. Ensure that they:
    #  1. Generate the expected non-structured output;
    #  2. Generate json output, with keys in the expected order; and
    #  3. That the LogRecordStringTranslationFilter() translates plain text
    #     and adds the json & desc records to the LogRecord.
    #

    # GIVEN
    record = logging.LogRecord(
        name="Test", level=level, pathname="test", lineno=10, msg=message, args=None, exc_info=None
    )
    filter = LogRecordStringTranslationFilter()

    # WHEN
    result = filter.filter(record)
    result = filter.filter(record)  # Twice just to make sure the filter logic is sound
    text_result = record.getMessage()
    dict_result = json.loads(record.json)  # type: ignore

    # THEN
    assert result
    assert isinstance(record.msg, BaseLogEvent)

    # These are added by the filter
    assert hasattr(record, "json")
    assert hasattr(record, "desc")

    # Make sure that the text message is correct
    assert record.msg.desc() == expected_desc
    assert text_result == expected_message

    # Make sure that the generated json is as expected
    assert dict_result.get("level") == record.levelname
    assert list(dict_result.keys())[0] == "level"  # level is fist
    del dict_result["level"]
    assert dict_result == expected_dict
    assert list(dict_result.keys()) == list(expected_dict.keys()), "Key ordering differs"

    # Finally, some invariants about the log event class definition.
    event = record.msg
    assert (
        # If we have any field, then we must have a type
        (event.ti is None and event.type is None and event.subtype is None)
        or (event.type is not None)
    )


@pytest.mark.parametrize("message, expected_dict, expected_desc, expected_message", TEST_RECORDS)
def test_messages_logged_exception(
    message: Union[str, BaseLogEvent],
    expected_dict: dict[str, Any],
    expected_desc: str,
    expected_message: str,
) -> None:
    # Test that the LogRecordStringTranslationFilter() adds execption information to the log event,
    # and that the log event includes the exception in log output.

    # GIVEN
    level = logging.ERROR
    try:
        raise RuntimeError("Test exception!")
    except:
        record = logging.LogRecord(
            name="Test",
            level=level,
            pathname="test",
            lineno=10,
            msg=message,
            args=None,
            exc_info=sys.exc_info(),
        )
    filter = LogRecordStringTranslationFilter()

    # WHEN
    result = filter.filter(record)
    result = filter.filter(record)  # Twice just to make sure the filter logic is sound
    text_result = record.getMessage()
    dict_result = json.loads(record.json)  # type: ignore

    # THEN
    assert result

    assert "exception" in dict_result
    assert list(dict_result.keys())[-1] == "exception", "exception isn't the final key"
    assert "Traceback (most recent call last):" in dict_result["exception"]
    assert (
        'in test_messages_logged_exception\n    raise RuntimeError("Test exception!")\nRuntimeError: Test exception!'
        in dict_result["exception"]
    )
    for extra_key in ("level", "exception"):
        del dict_result[extra_key]
    assert dict_result == expected_dict

    assert "\nTraceback (most recent call last):" in text_result
    assert text_result.endswith(
        'in test_messages_logged_exception\n    raise RuntimeError("Test exception!")\nRuntimeError: Test exception!'
    )
    assert text_result.startswith(expected_message)


def test_log_agent_info() -> None:
    # Testing AgentInfo. Keeping this simple. Just make sure that it doesn't crash when serializing.
    #
    # GIVEN
    agent_info = AgentInfoLogEvent()
    record = logging.LogRecord(
        name="Test",
        level=logging.INFO,
        pathname="test",
        lineno=10,
        msg=agent_info,
        args=None,
        exc_info=None,
    )
    filter = LogRecordStringTranslationFilter()

    # WHEN
    result = filter.filter(record)
    result = filter.filter(record)  # Twice just to make sure the filter logic is sound

    # THEN
    assert result
    assert isinstance(record.msg, BaseLogEvent)
    assert record.msg.desc() == "AgentInfo "
    record.getMessage()  # Doesn't raise an exception
    assert hasattr(
        record, "json"
    )  # filter populated the json field (which tests AgentInfoLogEvent.asdict())


@pytest.fixture
def session_id() -> str:
    return "session-1234"


@pytest.fixture
def queue_id() -> str:
    return "queue-1234"


@pytest.fixture
def job_id() -> str:
    return "job-1234"


@pytest.fixture
def scheduler_session(queue_id: str, job_id: str) -> MagicMock:
    scheduler_session = MagicMock()
    scheduler_session.session = MagicMock()
    scheduler_session.session._queue_id = queue_id
    scheduler_session.session._job_id = job_id
    return scheduler_session


@pytest.fixture
def session_map(
    session_id: str, scheduler_session: MagicMock
) -> Generator[dict[str, MagicMock], None, None]:
    with patch.object(
        agent_module.scheduler.scheduler.SessionMap, "get_session_map"
    ) as mock_get_session_map:
        mock_get_session_map.return_value = {session_id: scheduler_session}
        yield mock_get_session_map


EXPECTED_LOG_CONTENT = LogContent.EXCEPTION_INFO | LogContent.PROCESS_CONTROL | LogContent.HOST_INFO


@pytest.mark.parametrize(
    "log_content, expected_result",
    [
        pytest.param(content, content in EXPECTED_LOG_CONTENT, id=content.name)
        for content in list(LogContent)
    ]
    + [
        pytest.param(LogContent(0), True, id="No Content"),
        pytest.param(~LogContent(0), False, id="All Content"),
        pytest.param(None, False, id="None"),
    ],
)
def test_log_openjd_logs(
    session_id: str,
    queue_id: str,
    job_id: str,
    session_map: dict[str, MagicMock],
    log_content: Optional[LogContent],
    expected_result: bool,
) -> None:
    # GIVEN
    message = "Test OpenJD Message"
    record = logging.makeLogRecord(
        {
            "name": LOG.name,
            "level": logging.INFO,
            "levelname": "INFO",
            "pathname": "test",
            "lineno": 10,
            "msg": message,
            "args": None,
            "exc_info": None,
            "session_id": session_id,
            "openjd_log_content": log_content,
        }
    )
    log_filter = LogRecordStringTranslationFilter()

    # WHEN
    result = log_filter.filter(record)
    assert result == expected_result
    result = log_filter.filter(record)  # Twice just to make sure the filter logic is sound

    # THEN
    assert result == expected_result
    if expected_result:
        assert isinstance(record.msg, SessionLogEvent)
        assert record.getMessage() == f"[{session_id}] {message} [{queue_id}/{job_id}]"
        assert hasattr(
            record, "json"
        )  # filter populated the json field (which tests AgentInfoLogEvent.asdict())
        assert record.json == json.dumps(
            {
                "level": "INFO",
                "ti": "🔷",
                "type": "Session",
                "subtype": "Runtime",
                "session_id": session_id,
                "message": message,
                "queue_id": queue_id,
                "job_id": job_id,
            },
            ensure_ascii=False,
        )


def test_openjd_logs_no_openjd_log_content(
    session_id: str,
    queue_id: str,
    job_id: str,
    session_map: dict[str, MagicMock],
) -> None:
    # GIVEN
    message = "Test OpenJD Message"
    record = logging.makeLogRecord(
        {
            "name": LOG.name,
            "level": logging.INFO,
            "levelname": "INFO",
            "pathname": "test",
            "lineno": 10,
            "msg": message,
            "args": None,
            "exc_info": None,
            "session_id": session_id,
        }
    )
    log_filter = LogRecordStringTranslationFilter()

    # WHEN
    result = log_filter.filter(record)

    # THEN
    assert not result


def test_openjd_logs_openjd_log_content_wrong_type(
    session_id: str,
    queue_id: str,
    job_id: str,
    session_map: dict[str, MagicMock],
) -> None:
    # GIVEN
    message = "Test OpenJD Message"
    record = logging.makeLogRecord(
        {
            "name": LOG.name,
            "level": logging.INFO,
            "levelname": "INFO",
            "pathname": "test",
            "lineno": 10,
            "msg": message,
            "args": None,
            "exc_info": None,
            "session_id": session_id,
            "openjd_log_content": True,
        }
    )
    log_filter = LogRecordStringTranslationFilter()

    # WHEN
    result = log_filter.filter(record)

    # THEN
    assert not result


def test_openjd_logs_openjd_log_content_no_session_id() -> None:
    # GIVEN
    message = "Test OpenJD Message."
    expected_message = f"{message} The Worker Agent could not determine the session ID of this log originating from OpenJD. Please report this to the service team."
    record = logging.makeLogRecord(
        {
            "name": LOG.name,
            "level": logging.INFO,
            "levelname": "INFO",
            "pathname": "test",
            "lineno": 10,
            "msg": message,
            "args": None,
            "exc_info": None,
            "openjd_log_content": LogContent.EXCEPTION_INFO,
        }
    )
    log_filter = LogRecordStringTranslationFilter()

    # WHEN
    result = log_filter.filter(record)
    assert result
    result = log_filter.filter(record)  # Twice just to make sure the filter logic is sound

    # THEN
    assert result
    assert isinstance(record.msg, StringLogEvent)
    assert record.getMessage() == expected_message
    assert hasattr(
        record, "json"
    )  # filter populated the json field (which tests AgentInfoLogEvent.asdict())
    assert record.json == json.dumps(
        {
            "level": "INFO",
            "message": expected_message,
        },
        ensure_ascii=False,
    )


def test_openjd_logs_openjd_log_content_session_not_in_map() -> None:
    # GIVEN
    message = "Test OpenJD Message."
    session_id = "not exist"
    expected_message = f"{message} The Worker Agent could not locate the job and queue ID for this log originating from session {session_id}. Please report this to the service team."
    record = logging.makeLogRecord(
        {
            "name": LOG.name,
            "level": logging.INFO,
            "levelname": "INFO",
            "pathname": "test",
            "lineno": 10,
            "msg": message,
            "args": None,
            "exc_info": None,
            "session_id": session_id,
            "openjd_log_content": LogContent.EXCEPTION_INFO,
        }
    )
    log_filter = LogRecordStringTranslationFilter()

    # WHEN
    result = log_filter.filter(record)
    assert result
    result = log_filter.filter(record)  # Twice just to make sure the filter logic is sound

    # THEN
    assert result
    assert isinstance(record.msg, StringLogEvent)
    assert record.getMessage() == expected_message
    assert hasattr(
        record, "json"
    )  # filter populated the json field (which tests AgentInfoLogEvent.asdict())
    assert record.json == json.dumps(
        {
            "level": "INFO",
            "message": expected_message,
        },
        ensure_ascii=False,
    )
