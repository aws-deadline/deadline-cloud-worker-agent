# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that host configuration script output is logged as a typed log event.

Before this, the script's output reached the logger as a plain string and
LogRecordStringTranslationFilter wrapped it in an untyped StringLogEvent, so a
consumer could neither filter host configuration output nor attribute a line to the
worker that produced it.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from deadline_worker_agent.log_messages import (
    LogRecordStringTranslationFilter,
    StringLogEvent,
    WorkerHostConfigurationLogEvent,
    WorkerHostConfigurationOutputLogEvent,
    WorkerHostConfigurationStatus,
)
from deadline_worker_agent.startup.host_configuration_script import (
    _HostConfigurationOutputLogAdapter,
)

FARM_ID = "farm-00000000000000000000000000000000"
FLEET_ID = "fleet-00000000000000000000000000000000"
WORKER_ID = "worker-00000000000000000000000000000000"


@pytest.fixture
def logger() -> MagicMock:
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def adapter(logger: MagicMock) -> _HostConfigurationOutputLogAdapter:
    return _HostConfigurationOutputLogAdapter(
        logger=logger,
        farm_id=FARM_ID,
        fleet_id=FLEET_ID,
        worker_id=WORKER_ID,
    )


class TestHostConfigurationOutputLogAdapter:
    def test_wraps_a_plain_string_in_a_typed_event(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # GIVEN a plain string, as OpenJD's LoggingSubprocess and the Windows runner
        # both emit for each line of script output
        # WHEN
        msg, _ = adapter.process("a line of script output", {})

        # THEN
        assert isinstance(msg, WorkerHostConfigurationOutputLogEvent)
        assert msg.type == "Worker"
        assert msg.subtype == "HostConfiguration"
        assert msg.asdict()["message"] == "a line of script output"

    def test_carries_the_resource_ids(self, adapter: _HostConfigurationOutputLogAdapter) -> None:
        # The whole point of typing these lines: they become attributable.
        # WHEN
        msg, _ = adapter.process("a line of script output", {})

        # THEN
        dd = msg.asdict()
        assert dd["farm_id"] == FARM_ID
        assert dd["fleet_id"] == FLEET_ID
        assert dd["worker_id"] == WORKER_ID

    def test_omits_worker_id_when_unknown(self, logger: MagicMock) -> None:
        # GIVEN host configuration can be invoked before a worker id is known
        adapter = _HostConfigurationOutputLogAdapter(
            logger=logger, farm_id=FARM_ID, fleet_id=FLEET_ID, worker_id=None
        )

        # WHEN
        msg, _ = adapter.process("a line of script output", {})

        # THEN
        assert "worker_id" not in msg.asdict()

    def test_passes_through_an_existing_log_event(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # GIVEN a message that is already a log event, as the runner's own status
        # events are. Re-wrapping it would lose its type.
        event = StringLogEvent("already an event")

        # WHEN
        msg, _ = adapter.process(event, {})

        # THEN
        assert msg is event

    def test_merges_the_extra_kwarg(self, adapter: _HostConfigurationOutputLogAdapter) -> None:
        # Inherited from OpenJD's LoggerAdapter, which merges `extra` rather than
        # replacing it. Asserted here so subclassing cannot silently drop it.
        # WHEN
        _, kwargs = adapter.process("a line", {"extra": {"caller_key": "caller_value"}})

        # THEN
        assert kwargs["extra"]["caller_key"] == "caller_value"
        assert kwargs["extra"]["worker_id"] == WORKER_ID

    def test_applies_percent_style_args(self, adapter: _HostConfigurationOutputLogAdapter) -> None:
        # LoggerAdapter.log() forwards args to the logger separately from msg, so they
        # never reach process(). Wrapping the raw template would leave the record
        # rendering as "Running command %s" with the argument dropped, because the
        # filter calls the event's getMessage() which does no % substitution. OpenJD
        # logs exactly this way.
        # WHEN
        adapter.info("Running command %s", "/bin/sh -c foo")

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert isinstance(msg, WorkerHostConfigurationOutputLogEvent)
        assert msg.asdict()["message"] == "Running command /bin/sh -c foo"

    def test_applies_multiple_percent_style_args(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # WHEN
        adapter.info("%s exited with %d", "powershell", 3)

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert msg.asdict()["message"] == "powershell exited with 3"

    def test_a_literal_percent_without_args_is_untouched(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # Script output is arbitrary text and may contain a bare %, which must not be
        # treated as a format specifier when there are no args to apply.
        # WHEN
        adapter.info("disk usage 93% complete")

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert msg.asdict()["message"] == "disk usage 93% complete"

    def test_applies_mapping_style_args(self, adapter: _HostConfigurationOutputLogAdapter) -> None:
        # logging.LogRecord normalizes a single non-empty mapping argument, which is how
        # a "%(name)s"-style call arrives. Without mirroring that, `msg % (mapping,)`
        # raises "format requires a mapping" and the template is logged unsubstituted.
        # WHEN
        adapter.info("%(cmd)s failed", {"cmd": "setup.ps1"})

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert msg.asdict()["message"] == "setup.ps1 failed"

    def test_an_empty_mapping_is_treated_as_a_positional_arg(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # The stdlib normalization requires a *non-empty* mapping, so an empty dict stays
        # positional. Matching that keeps "%s" with an empty dict rendering as "{}".
        # WHEN
        adapter.info("got %s", {})

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert msg.asdict()["message"] == "got {}"

    def test_a_malformed_template_keeps_the_args(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # The args must not vanish. process() wraps any str in an event, and the filter's
        # BaseLogEvent branch neither substitutes args nor reports the mismatch, so
        # leaving them attached would drop them with no diagnostic at all.
        # WHEN
        adapter.info("only one slot %s", "a", "b")

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        rendered = msg.asdict()["message"]
        assert "only one slot %s" in rendered
        assert "'a'" in rendered and "'b'" in rendered

    def test_a_missing_mapping_key_keeps_the_args(
        self, adapter: _HostConfigurationOutputLogAdapter
    ) -> None:
        # A missing key raises KeyError rather than TypeError/ValueError, so it needs to
        # be caught too.
        # WHEN
        adapter.info("%(missing)s here", {"present": 1})

        # THEN
        msg = adapter.logger.log.call_args.args[1]  # type: ignore[attr-defined]
        assert "%(missing)s here" in msg.asdict()["message"]

    def test_does_not_format_for_a_suppressed_level(
        self, adapter: _HostConfigurationOutputLogAdapter, logger: MagicMock
    ) -> None:
        # %-style logging exists so the substitution is not paid for when the record is
        # going to be dropped.
        logger.isEnabledFor.return_value = False

        # WHEN
        adapter.debug("expensive %s", "arg")

        # THEN
        logger.log.assert_not_called()

    def test_logging_through_the_adapter_reaches_the_logger_typed(
        self, adapter: _HostConfigurationOutputLogAdapter, logger: MagicMock
    ) -> None:
        # End to end through the public LoggerAdapter API, since that is what OpenJD
        # calls rather than process() directly.
        # WHEN
        adapter.info("a line of script output")

        # THEN
        logger.log.assert_called_once()
        level, msg = logger.log.call_args.args[:2]
        assert level == logging.INFO
        assert isinstance(msg, WorkerHostConfigurationOutputLogEvent)


class TestTypedOutputSurvivesTheLogFilter:
    """The filter is what previously flattened these lines, so cover it directly."""

    def _filtered_json(self, msg: Any) -> dict[str, Any]:
        record = logging.LogRecord(
            name="Test",
            level=logging.INFO,
            pathname="test",
            lineno=10,
            msg=msg,
            args=None,
            exc_info=None,
        )
        assert LogRecordStringTranslationFilter().filter(record)
        return json.loads(record.json)  # type: ignore[attr-defined]

    def test_a_plain_string_is_still_untyped(self) -> None:
        # GIVEN the old behavior, retained for every other plain-string log call
        # WHEN / THEN
        dd = self._filtered_json("a line of script output")
        assert "type" not in dd
        assert "subtype" not in dd

    def test_output_and_status_share_a_subtype_and_differ_by_status(self) -> None:
        # The two events are one filterable family on the wire, so a consumer selecting
        # Worker/HostConfiguration gets the script's transcript alongside the outcome that
        # explains it. `status` is the documented discriminator: only the transitions carry it.
        output = self._filtered_json(
            WorkerHostConfigurationOutputLogEvent(
                farm_id=FARM_ID, fleet_id=FLEET_ID, worker_id=WORKER_ID, message="a line"
            )
        )
        status = self._filtered_json(
            WorkerHostConfigurationLogEvent(
                farm_id=FARM_ID,
                fleet_id=FLEET_ID,
                worker_id=WORKER_ID,
                message="failed",
                status=WorkerHostConfigurationStatus.FAILED,
                exit_code=7,
            )
        )

        assert output["subtype"] == status["subtype"] == "HostConfiguration"
        assert "status" not in output
        assert status["status"] == "Failed"
        assert status["exit_code"] == 7

    def test_the_typed_output_event_keeps_its_type(self) -> None:
        # WHEN
        dd = self._filtered_json(
            WorkerHostConfigurationOutputLogEvent(
                farm_id=FARM_ID,
                fleet_id=FLEET_ID,
                worker_id=WORKER_ID,
                message="a line of script output",
            )
        )

        # THEN
        assert dd["type"] == "Worker"
        assert dd["subtype"] == "HostConfiguration"
        assert dd["worker_id"] == WORKER_ID

    def test_the_plain_text_message_is_the_bare_line(self) -> None:
        # The resource ids belong in the structured record, not appended to every line
        # of the plain-text log the way the lifecycle events do it.
        record = logging.LogRecord(
            name="Test",
            level=logging.INFO,
            pathname="test",
            lineno=10,
            msg=WorkerHostConfigurationOutputLogEvent(
                farm_id=FARM_ID,
                fleet_id=FLEET_ID,
                worker_id=WORKER_ID,
                message="a line of script output",
            ),
            args=None,
            exc_info=None,
        )
        assert LogRecordStringTranslationFilter().filter(record)

        assert record.getMessage() == "a line of script output"
