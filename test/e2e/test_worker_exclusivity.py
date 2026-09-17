"""Exercises the macOS single-worker exclusivity state machine in conftest."""

import os
from unittest.mock import MagicMock
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("OPERATING_SYSTEM") != "macos",
    reason="Exercises the macOS single-worker exclusivity registry, which is inert elsewhere",
)

from e2e import conftest  # noqa: E402


def _reset():
    conftest._installed_worker = None
    conftest._displaced_session_worker = None


def _req(failed=0):
    r = MagicMock()
    r.session.testsfailed = failed
    r.session.stash = {}
    return r


def test_second_install_stops_the_first():
    _reset()
    a, b = MagicMock(name="a"), MagicMock(name="b")
    conftest._claim_host(a, None)
    conftest._claim_host(b, None)
    a.stop.assert_called_once()
    b.stop.assert_not_called()
    assert conftest._installed_worker is b


def test_displaced_session_worker_is_reinstated_once():
    _reset()
    sess, variant = MagicMock(name="sess"), MagicMock(name="variant")
    conftest._claim_host(sess, sess)  # session worker installs
    conftest._claim_host(variant, sess)  # variant displaces it
    sess.stop.assert_called_once()
    conftest.reinstate_session_worker(sess)  # next session_worker use
    sess.start.assert_called_once()
    assert conftest._installed_worker is sess
    conftest.reinstate_session_worker(sess)  # idempotent
    sess.start.assert_called_once()


def test_no_reinstate_when_not_displaced():
    _reset()
    sess = MagicMock(name="sess")
    conftest._claim_host(sess, sess)
    conftest.reinstate_session_worker(sess)
    sess.start.assert_not_called()


def test_stop_worker_skips_an_already_displaced_worker():
    _reset()
    a, b = MagicMock(name="a"), MagicMock(name="b")
    conftest._claim_host(a, None)
    conftest._claim_host(b, None)  # a stopped here
    a.stop.reset_mock()
    conftest.stop_worker(_req(), a)  # a's fixture teardown
    a.stop.assert_not_called()  # would have raised on double DeleteWorker


def test_stop_worker_stops_the_holder_and_releases():
    _reset()
    a = MagicMock(name="a")
    conftest._claim_host(a, None)
    conftest.stop_worker(_req(), a)
    a.stop.assert_called_once()
    assert conftest._installed_worker is None


def test_claim_survives_a_failing_incumbent_stop():
    _reset()
    a, b = MagicMock(name="a"), MagicMock(name="b")
    a.stop.side_effect = RuntimeError("bootout failed")
    conftest._claim_host(a, None)
    conftest._claim_host(b, None)  # must not propagate
    assert conftest._installed_worker is b
