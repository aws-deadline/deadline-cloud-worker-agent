"""Exercises the macOS single-worker exclusivity state machine in conftest.

Runs on every platform, not only macOS. The state machine is inert on Linux and Windows because
conftest._MACOS is False there, so the module patches that flag on instead of skipping: the logic
that gates a macOS merge then gets its coverage from the Linux and Windows runs too, rather than
only from the one platform it changes.

The registry is module-level state in a conftest that is also driving real workers, so every value
this module touches is set through monkeypatch. Assigning the globals directly would leave a
MagicMock in _installed_worker for the rest of the session: the next _claim_host would stop the mock
instead of the real incumbent and install over a running agent, and any real worker's teardown would
take the "already displaced; nothing to stop" path and leak its worker record. monkeypatch restores
the previous values after each test, which keeps that impossible.
"""

from unittest.mock import MagicMock

import pytest

from e2e import conftest


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(conftest, "_MACOS", True)
    monkeypatch.setattr(conftest, "_installed_worker", None)
    monkeypatch.setattr(conftest, "_displaced_session_worker", None)
    # KEEP_WORKER_AFTER_FAILURE is read from the environment by the code under test, and a real run
    # may have it set. Cleared so these tests describe the default path unless they set it.
    monkeypatch.delenv("KEEP_WORKER_AFTER_FAILURE", raising=False)


def _req(failed: int = 0) -> MagicMock:
    r = MagicMock()
    r.session.testsfailed = failed
    r.session.stash = {}
    return r


def test_second_install_stops_the_first() -> None:
    a, b = MagicMock(name="a"), MagicMock(name="b")
    conftest._claim_host(_req(), a, None)
    conftest._claim_host(_req(), b, None)
    a.stop.assert_called_once()
    b.stop.assert_not_called()
    assert conftest._installed_worker is b


def test_displaced_session_worker_is_reinstated_once() -> None:
    sess, variant = MagicMock(name="sess"), MagicMock(name="variant")
    conftest._claim_host(_req(), sess, sess)  # session worker installs
    conftest._claim_host(_req(), variant, sess)  # variant displaces it
    sess.stop.assert_called_once()
    conftest.reinstate_session_worker(_req(), sess)  # next session_worker use
    sess.start.assert_called_once()
    assert conftest._installed_worker is sess
    conftest.reinstate_session_worker(_req(), sess)  # idempotent
    sess.start.assert_called_once()


def test_reinstating_stops_a_live_per_test_worker() -> None:
    # The reinstate path installs onto the host like any other, so it has to release it first. A
    # class-scoped worker can still be alive when a session_worker test reinstates; without this the
    # session worker would overwrite its worker.toml in place and its teardown would find nothing to
    # stop, leaking the record.
    sess, variant = MagicMock(name="sess"), MagicMock(name="variant")
    conftest._claim_host(_req(), sess, sess)
    conftest._claim_host(_req(), variant, sess)
    variant.stop.assert_not_called()

    conftest.reinstate_session_worker(_req(), sess)

    variant.stop.assert_called_once()
    assert conftest._installed_worker is sess


def test_a_failed_reinstall_stays_displaced() -> None:
    # Clearing the flag before start() would report the worker as installed while the host has no
    # agent on it, and the next use would hand back a stale worker instead of retrying.
    sess, variant = MagicMock(name="sess"), MagicMock(name="variant")
    sess.start.side_effect = RuntimeError("bootstrap failed")
    conftest._claim_host(_req(), sess, sess)
    conftest._claim_host(_req(), variant, sess)

    with pytest.raises(RuntimeError, match="bootstrap failed"):
        conftest.reinstate_session_worker(_req(), sess)

    assert conftest._displaced_session_worker is sess


def test_no_reinstate_when_not_displaced() -> None:
    sess = MagicMock(name="sess")
    conftest._claim_host(_req(), sess, sess)
    conftest.reinstate_session_worker(_req(), sess)
    sess.start.assert_not_called()


def test_stop_worker_skips_an_already_displaced_worker() -> None:
    a, b = MagicMock(name="a"), MagicMock(name="b")
    conftest._claim_host(_req(), a, None)
    conftest._claim_host(_req(), b, None)  # a stopped here
    a.stop.reset_mock()
    conftest.stop_worker(_req(), a)  # a's fixture teardown
    a.stop.assert_not_called()  # would have raised on double DeleteWorker


def test_stop_worker_stops_the_holder_and_releases() -> None:
    a = MagicMock(name="a")
    conftest._claim_host(_req(), a, None)
    conftest.stop_worker(_req(), a)
    a.stop.assert_called_once()
    assert conftest._installed_worker is None


def test_claim_survives_a_failing_incumbent_stop() -> None:
    a, b = MagicMock(name="a"), MagicMock(name="b")
    a.stop.side_effect = RuntimeError("bootout failed")
    conftest._claim_host(_req(), a, None)
    conftest._claim_host(_req(), b, None)  # must not propagate
    assert conftest._installed_worker is b


def test_a_kept_worker_still_holds_the_host(monkeypatch: pytest.MonkeyPatch) -> None:
    # KEEP_WORKER_AFTER_FAILURE leaves the agent installed and running, so releasing the slot would
    # let the next install destroy the state the flag exists to preserve.
    monkeypatch.setenv("KEEP_WORKER_AFTER_FAILURE", "true")
    a = MagicMock(name="a")
    conftest._claim_host(_req(), a, None)

    conftest.stop_worker(_req(failed=1), a)

    a.stop.assert_not_called()
    assert conftest._installed_worker is a


def test_claim_refuses_to_displace_a_kept_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEP_WORKER_AFTER_FAILURE", "true")
    a, b = MagicMock(name="a"), MagicMock(name="b")
    conftest._claim_host(_req(), a, None)
    conftest.stop_worker(_req(failed=1), a)

    with pytest.raises(RuntimeError, match="KEEP_WORKER_AFTER_FAILURE"):
        conftest._claim_host(_req(failed=1), b, None)

    a.stop.assert_not_called()
    assert conftest._installed_worker is a


def test_the_registry_is_inert_off_macos(monkeypatch: pytest.MonkeyPatch) -> None:
    # The whole mechanism is macOS-only: Linux and Windows workers are separate instances and are
    # genuinely concurrent, so nothing here may stop one of them.
    monkeypatch.setattr(conftest, "_MACOS", False)
    a = MagicMock(name="a")
    monkeypatch.setattr(conftest, "_installed_worker", a)
    conftest.stop_worker(_req(), MagicMock(name="b"))
    a.stop.assert_not_called()
    assert conftest._installed_worker is a

    sess = MagicMock(name="sess")
    monkeypatch.setattr(conftest, "_displaced_session_worker", sess)
    conftest.reinstate_session_worker(_req(), sess)
    sess.start.assert_not_called()
