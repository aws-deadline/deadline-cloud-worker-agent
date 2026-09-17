# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
End-to-end tests for the session_runtime worker configuration.

These tests validate that the worker agent correctly routes session execution
to the configured runtime (python, rust, or service-selected) and that
missing-runtime failures surface visibly rather than silently falling back.

TestRustUnavailableAndRecovery is adaptive: its fixture moves the Rust
adapter's one exclusive import aside before the test runs, then the test
switches to python and verifies recovery, all within a single test function to
guarantee phase ordering. The fixture moves the tree back in its teardown, so
the fault does not outlive the class on a host that outlives the build.
Shell commands branch per OS: Linux workers use bash, Windows workers use
PowerShell via SSM AWS-RunPowerShellScript.

Hint-dependent scenarios:
    Two classes below cannot run until the service stamps a runtimeHint on
    session specs for the test account, which requires that account to be
    allowlisted for the service-side runtime-hint feature gates. The tests
    themselves cannot create or observe that state, so they carry an
    unconditional skip mark with the unlock condition in the reason. Remove
    the mark when the account reaches the corresponding state:

      TestServiceSelectedWithRustHint       account allowlisted for the
                                            per-OS runtime gate (hint=rust)
      TestServiceSelectedWithPythonexprHint outer gate open, per-OS gate
                                            closed (hint=pythonexpr)

    Note that TestServiceSelectedDefaultsToPython asserts the absence of a
    hint. Its premise inverts in either of those worlds, so it will start
    failing and must be retired at the same time.

The Rust adapter needs no env var: openjd-model ships the Rust extension
(openjd._openjd_rs) in its platform wheels from 0.10.0 onward, and this
package pins openjd-model >= 0.11.1, so a worker installed from this wheel
always has it. The rust scenarios therefore run unconditionally.

Log assertion anchor: the scheduler logs
    "Selected session runtime: <kind> (hint=...)"
per session start (PR #1016).

Worker agent log paths:
    Linux:   /var/log/amazon/deadline/worker-agent.log
    Windows: C:\\ProgramData\\Amazon\\Deadline\\Logs\\worker-agent.log

Worker openjd package location (for making the Rust adapter unloadable):
    Resolved from the worker's own python at test time on both platforms
    (Linux: the venv at /opt/deadline/worker; Windows: the system python; see
    the rust_unavailable_worker fixture)
"""

from typing import Generator, Union

import backoff
import dataclasses
import logging
import os
import shlex

import pytest

from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineWorker,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    LocalMacWorker,
    TaskStatus,
)

from e2e.conftest import DeadlineResources, create_worker, stop_worker
from e2e.utils import (
    is_worker_started,
    job_failure_message,
    submit_sleep_job,
)

LOG = logging.getLogger(__name__)

# Agent log paths per OS
# This module drives the agent's service directly, which the DeadlineWorker base
# class does not declare. Both concrete workers that can host a macOS or Linux
# agent expose it, so accept either rather than requiring the EC2 one.
ServiceControllableWorker = Union[EC2InstanceWorker, LocalMacWorker]

_LINUX_AGENT_LOG = "/var/log/amazon/deadline/worker-agent.log"
_WINDOWS_AGENT_LOG = r"C:\ProgramData\Amazon\Deadline\Logs\worker-agent.log"

# Worker config paths per OS
_LINUX_WORKER_TOML = "/etc/amazon/deadline/worker.toml"
_WINDOWS_WORKER_TOML = r"C:\ProgramData\Amazon\Deadline\Config\worker.toml"

# Worker venv site-packages path (for making the Rust adapter unloadable).
# Linux: the worker agent runs inside a venv at this path. This is not a
# guess: it is the same literal deadline_test_fixtures uses to create the
# venv in its userdata ("python3 -m venv /opt/deadline/worker"). The openjd
# package location inside it is resolved by asking that venv's python at
# test time, so no Python minor version appears here. On Windows the
# installer puts python on the machine PATH (PrependPath=1 in the fixtures
# userdata), so the interpreter is asked directly.
_LINUX_WORKER_VENV = "/opt/deadline/worker"

# The macOS agent runs as a system LaunchDaemon; `launchctl print system/<label>` is the equivalent
# of `systemctl is-active` there. Taken from the worker class rather than repeated as a literal, so
# a rename in the fixtures package cannot leave this querying a label that no longer exists.
_MACOS_LAUNCHD_LABEL = LocalMacWorker.LAUNCHD_LABEL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _running_on_windows() -> bool:
    """Return True when the suite is running against Windows workers.

    Reads OPERATING_SYSTEM, the variable every other e2e module in this
    package branches on. Duck-typing the worker object instead (checking for
    a Windows-only class attribute) would silently fall through to the Linux
    branch if the fixture library ever renamed that attribute, sending bash
    to a Windows host.
    """
    return os.environ["OPERATING_SYSTEM"].lower() == "windows"


def _running_on_macos() -> bool:
    """Return True when the suite is running against a macOS worker.

    Same reasoning as _running_on_windows: OPERATING_SYSTEM is what every other e2e module in this
    package branches on, and duck-typing the worker object would fall through to the systemd branch
    if the fixture library renamed an attribute.
    """
    return os.environ["OPERATING_SYSTEM"].lower() == "macos"


def _agent_log_path(worker: ServiceControllableWorker) -> str:
    """Return the agent log path appropriate for the worker's OS."""
    return _WINDOWS_AGENT_LOG if _running_on_windows() else _LINUX_AGENT_LOG


def _assert_log_contains(
    worker: ServiceControllableWorker,
    pattern: str,
    description: str,
) -> None:
    """Assert that the agent log on the remote worker contains *pattern*.

    Matches *pattern* as a literal string on both platforms (grep -F on
    Linux, Select-String -SimpleMatch on Windows), so regex metacharacters
    and embedded quotes in patterns like (hint='rust') need no escaping by
    callers. Retries briefly: every
    caller greps only after awaiting the job's terminal status, and the
    asserted line is written at session start, so the line is normally on
    disk minutes before the first attempt -- the retry is defence against
    slow log flushes or a transient SSM hiccup, not an expected wait.
    """
    log_path = _agent_log_path(worker)

    if _running_on_windows():
        # PowerShell: Select-String with -Quiet returns $true/$false.
        # Single quotes inside the pattern are doubled for PS literal strings.
        ps_pattern = pattern.replace("'", "''")
        cmd = (
            f"if (Select-String -Path '{log_path}' -Pattern '{ps_pattern}' -SimpleMatch -Quiet) "
            f"{{ exit 0 }} else {{ exit 1 }}"
        )
    else:
        cmd = f"grep -qF {shlex.quote(pattern)} {log_path}"

    @backoff.on_exception(backoff.constant, AssertionError, max_time=30, interval=5)
    def _check() -> None:
        cmd_result = worker.send_command(cmd)
        assert cmd_result.exit_code == 0, (
            f"Expected agent log to contain '{pattern}' ({description}). "
            f"exit_code={cmd_result.exit_code}, stdout={cmd_result.stdout!r}"
        )

    _check()


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


@pytest.fixture(
    scope="class",
    params=[
        pytest.param("python", id="python"),
        pytest.param("rust", id="rust"),
    ],
)
def explicit_runtime_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[tuple[ServiceControllableWorker, str], None, None]:
    """Create a worker with session_runtime set to the parametrized value.

    Two values: python and rust. Both run unconditionally; the Rust
    extension ships in openjd-model's platform wheels.

    Yields (worker, runtime) so tests can access the runtime string alongside
    the worker instance.
    """
    runtime: str = request.param
    with create_worker(
        dataclasses.replace(worker_config, session_runtime=runtime),
        request,
    ) as worker:
        assert isinstance(worker, (EC2InstanceWorker, LocalMacWorker))
        yield worker, runtime
    stop_worker(request, worker)


class TestExplicitModeRouting:
    """Worker with a given session_runtime routes sessions to the expected adapter.

    Parametrized over python and rust. Each parameter gets its own worker
    with the corresponding session_runtime in worker.toml.

    We assert the exact "Selected session runtime: <kind>" log line since
    the selected runtime is deterministic for explicit python/rust modes.
    """

    def test_job_succeeds_and_log_shows_runtime_selected(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        explicit_runtime_worker: tuple[ServiceControllableWorker, str],
    ) -> None:
        worker, runtime = explicit_runtime_worker
        job = submit_sleep_job(
            f"session_runtime={runtime} routing test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        _assert_log_contains(
            worker,
            f"Selected session runtime: {runtime}",
            f"session_runtime={runtime} should log {runtime} selection",
        )


@pytest.fixture(scope="class")
def service_selected_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(
        dataclasses.replace(worker_config, session_runtime="service-selected"),
        request,
    ) as worker:
        assert isinstance(worker, (EC2InstanceWorker, LocalMacWorker))
        yield worker
    stop_worker(request, worker)


class TestServiceSelectedDefaultsToPython:
    """Worker with session_runtime=service-selected defaults to python when no hint is stamped.

    The service does not stamp runtimeHint today (the service-side feature
    gates are closed). This validates the no-hint default branch on real
    infrastructure: when session_runtime=service-selected and no runtimeHint
    is present in the session spec, the worker falls back to the python
    adapter. NOTE: this premise inverts once the runtime-hint feature gates
    open for the test account. This class will then start failing and must be
    retired, at the same time as the hint-following classes below are enabled.
    """

    def test_job_succeeds_and_log_shows_python_selected(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        service_selected_worker: ServiceControllableWorker,
    ) -> None:
        job = submit_sleep_job(
            "session_runtime=service-selected (no hint) routing test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        _assert_log_contains(
            service_selected_worker,
            "Selected session runtime: python (hint=None)",
            "service-selected with no hint should default to python",
        )


@pytest.mark.skip(
    reason="Requires the test account to be allowlisted for the service-side "
    "runtime-hint gates so the service stamps runtimeHint=rust. Remove this "
    "mark once that is true; see the module docstring."
)
class TestServiceSelectedWithRustHint:
    """Worker with session_runtime=service-selected routes to rust when DP stamps hint=rust.

    Skipped unconditionally: the account must be allowlisted for the
    service-side runtime-hint feature gates before the hint appears in session
    specs. The Rust extension itself always ships, so no other precondition is
    outstanding.
    """

    def test_job_succeeds_and_log_shows_rust_selected(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        service_selected_worker: ServiceControllableWorker,
    ) -> None:
        job = submit_sleep_job(
            "session_runtime=service-selected (hint=rust) routing test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        _assert_log_contains(
            service_selected_worker,
            "Selected session runtime: rust (hint='rust')",
            "service-selected with hint=rust should route to rust",
        )


@pytest.mark.skip(
    reason="Requires the test account to be in the intermediate allowlist state "
    "(outer runtime-hint gate open, per-OS gate closed) so the service stamps "
    "runtimeHint=pythonexpr. Remove this mark once that is true; see the "
    "module docstring."
)
class TestServiceSelectedWithPythonexprHint:
    """Worker with session_runtime=service-selected routes to python when DP stamps hint=pythonexpr.

    Unlock condition: the staged-rollout intermediate state where the test
    account is allowlisted for the outer runtime-hint feature gate but NOT
    the per-OS runtime gate, so the service stamps hint=pythonexpr. This
    class validates the hint-following path routes to python (distinct code
    path from the no-hint default).

    RETIREMENT: delete this class when the feature gates are fully
    open/removed (the intermediate state no longer exists).
    """

    def test_job_succeeds_and_log_shows_python_selected_with_pythonexpr_hint(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        service_selected_worker: ServiceControllableWorker,
    ) -> None:
        job = submit_sleep_job(
            "session_runtime=service-selected (hint=pythonexpr) routing test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        _assert_log_contains(
            service_selected_worker,
            "Selected session runtime: python (hint='pythonexpr')",
            "service-selected with hint=pythonexpr should route to python via hint path",
        )


# Suffix for the moved-aside openjd/model/_v1 tree. Not an importable module name, so the tree is
# inert where it sits, and a literal so pipeline/e2e-macos.sh can find and restore one left behind
# by a build that was killed between the move and the restore.
_V1_MOVED_ASIDE_SUFFIX = ".e2e-moved-aside"


def _resolve_openjd_model_dir(worker: ServiceControllableWorker) -> str:
    """Return the directory of the worker's installed openjd.model package.

    Resolved once, by asking the worker's own python, and reused for both the move and the move
    back. It cannot be resolved a second time: openjd.model imports from its _v1 subpackage, so
    once that is moved aside the import used to locate it no longer works.
    """
    if _running_on_windows():
        # openjd is a namespace package (no __init__), so its __file__ is None; resolve the
        # concrete openjd.model subpackage instead.
        cmd = 'python -c "import openjd.model, os; print(os.path.dirname(openjd.model.__file__))"'
    else:
        cmd = (
            f"{_LINUX_WORKER_VENV}/bin/python -c "
            "'import openjd.model, os; print(os.path.dirname(openjd.model.__file__))'"
        )
    result = worker.send_command(cmd)
    assert result.exit_code == 0, f"Failed to resolve the openjd.model location: {result}"
    # Last line, not the whole of stdout: the remote transport may prepend its own output.
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert lines, f"Resolving the openjd.model location produced no output: {result}"
    return lines[-1]


@pytest.fixture(scope="class")
def rust_unavailable_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    """A worker with session_runtime=rust whose Rust adapter cannot load.

    Fault injection: openjd/model/_v1 is the one import the Rust adapter needs that nothing else
    does. rust.py imports openjd.model._v1 (plus openjd._openjd_rs and openjd.expr), so taking it
    away makes the adapter fail to import while leaving the agent itself intact.

    Do NOT touch openjd/_openjd_rs.* or openjd/expr: openjd.sessions' pure-Python runner
    (_runner_base.py) imports openjd.expr, which is a facade over the native extension. Removing
    those stops the agent from starting at all -- a broken install, not an unavailable adapter, and
    it defeats the point of these tests.

    Moved aside and moved back rather than deleted. On EC2 either would do, because the instance is
    discarded with the class. LocalMacWorker installs into a venv at /opt/deadline/worker on a
    reserved-capacity host that outlives the build, and pipeline/e2e-macos.sh keeps that venv
    deliberately, so a delete would be permanent: every later rust assertion on that machine would
    fail, starting with TestExplicitModeRouting[rust] in the next build, which runs before this
    class and would report a missing adapter as a routing regression.

    The move is asserted in both directions -- target present before, gone after, back afterwards --
    so a change in install layout fails here rather than surfacing as a confusing job outcome.
    """
    with create_worker(
        dataclasses.replace(worker_config, session_runtime="rust"),
        request,
    ) as worker:
        assert isinstance(worker, (EC2InstanceWorker, LocalMacWorker))

        is_win = _running_on_windows()
        model_dir = _resolve_openjd_model_dir(worker)
        sep = "\\" if is_win else "/"
        v1 = f"{model_dir}{sep}_v1"
        moved = f"{v1}{_V1_MOVED_ASIDE_SUFFIX}"

        if is_win:
            move_cmd = (
                f"if (-not (Test-Path '{v1}')) {{ Write-Error 'expected {v1} to exist'; exit 1 }}; "
                f"Move-Item -Force '{v1}' '{moved}'; "
                f"if (Test-Path '{v1}') {{ Write-Error 'failed to move {v1} aside'; exit 1 }}; "
                "exit 0"
            )
            restore_cmd = (
                f"if (Test-Path '{moved}') {{ "
                f"if (Test-Path '{v1}') {{ Remove-Item -Recurse -Force '{v1}' }}; "
                f"Move-Item -Force '{moved}' '{v1}' }}; "
                f"if (-not (Test-Path '{v1}')) {{ Write-Error 'failed to restore {v1}'; exit 1 }}; "
                "exit 0"
            )
        else:
            move_cmd = (
                f'set -e; if [ ! -d "{v1}" ]; then echo "expected {v1} to exist" >&2; exit 1; fi; '
                f'mv "{v1}" "{moved}"; '
                f'if [ -d "{v1}" ]; then echo "failed to move {v1} aside" >&2; exit 1; fi'
            )
            restore_cmd = (
                f'set -e; if [ -d "{moved}" ]; then rm -rf "{v1}"; mv "{moved}" "{v1}"; fi; '
                f'if [ ! -d "{v1}" ]; then echo "failed to restore {v1}" >&2; exit 1; fi'
            )

        move_result = worker.send_command(move_cmd)
        assert move_result.exit_code == 0, f"Failed to move {v1} aside: {move_result}"

        try:
            yield worker
        finally:
            # Asserted, not best-effort. On macOS a failed restore leaves the host's venv unable to
            # load the Rust adapter for every subsequent build, so it has to be loud even though
            # raising in teardown reports as an error on a test that may have passed.
            restore_result = worker.send_command(restore_cmd)
            assert restore_result.exit_code == 0, (
                f"Failed to restore {v1}; a macOS host is now poisoned for later builds and needs "
                f"the tree moved back by hand: {restore_result}"
            )
    stop_worker(request, worker)


class TestRustUnavailableAndRecovery:
    """Worker with session_runtime=rust fails visibly when the Rust adapter cannot load,
    then recovers after switching to python via config edit + service restart.

    This is a single sequential test function rather than separate methods because
    the phases share one worker and are order-dependent: the config edit in phase 2 only means
    anything after the failure in phase 1. A single function makes the ordering immune to
    test reordering/parallelization plugins (pytest-xdist, pytest-randomly).

    The Rust adapter is already unloadable on arrival -- rust_unavailable_worker moves
    openjd/model/_v1 aside before yielding and moves it back in its teardown, so the fault does
    not outlive the class on a host that outlives the build.
    """

    def test_rust_unavailable_fails_visibly_then_recovers_after_switch_to_python(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        rust_unavailable_worker: ServiceControllableWorker,
    ) -> None:
        """Rust mode fails visibly when the adapter cannot load, then recovers after
        switching to python via worker.toml edit + service restart."""

        worker = rust_unavailable_worker
        is_win = _running_on_windows()

        # -- Phase 1: Submit job as rust mode -> expect failure --------------------
        job = submit_sleep_job(
            "session_runtime=rust (unavailable) failure test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        job.wait_until_complete(client=deadline_client)

        # The job should fail because the Rust runtime is unavailable
        assert job.task_run_status == TaskStatus.FAILED, (
            f"Expected job to FAIL when Rust runtime is unavailable, but got {job.task_run_status}"
        )

        # Verify the error message mentions the adapter being unavailable
        _assert_log_contains(
            worker,
            "RustSessionRuntime adapter is not available",
            "rust mode with missing extension should log adapter-not-available error",
        )

        # -- Phase 2: Switch to python + restart service --------------------------
        toml_path = _WINDOWS_WORKER_TOML if is_win else _LINUX_WORKER_TOML

        if is_win:
            # PowerShell: line-by-line -replace (no -Raw: '^' must anchor each
            # line, matching the fixture's injection mode; with -Raw it only
            # matches the start of the whole file and silently no-ops).
            # Select-String makes a silent no-op loud; '.' wildcards the quotes
            # to avoid another quote-escaping layer.
            switch_cmd = (
                f"$content = Get-Content -Path '{toml_path}'; "
                f"$content = $content -replace '^session_runtime = .*', "
                f"'session_runtime = \"python\"'; "
                f"Set-Content -Path '{toml_path}' -Value $content; "
                f"if (-not (Select-String -Path '{toml_path}' "
                f"-Pattern '^session_runtime = .python.$' -Quiet)) {{ exit 1 }}"
            )
        else:
            # -i.bak rather than a bare -i: BSD sed, which macOS ships, requires an
            # argument to -i and would otherwise take the script as the backup suffix.
            # The suffixed form is accepted by both BSD and GNU sed.
            switch_cmd = (
                f"sed -i.bak 's/^session_runtime = .*/session_runtime = \"python\"/' {toml_path}"
                f" && rm -f {toml_path}.bak"
                f" && grep -q '^session_runtime = .python.$' {toml_path}"
            )

        sed_result = worker.send_command(switch_cmd)
        assert sed_result.exit_code == 0, (
            f"Failed to update worker.toml session_runtime: {sed_result}"
        )

        # Restart the worker service to pick up the new config
        worker.stop_worker_service()

        # Three branches, not two. The POSIX branch names systemctl, which macOS has not got:
        # `systemctl is-active` there exits 127 with empty stdout, satisfying both assertions on
        # the first attempt. That is a gate that contributes no settle time at all on the one
        # platform where the restart actually races, so macOS asks launchd instead.
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=45,
            interval=5,
        )
        def check_worker_service_stopped() -> None:
            if is_win:
                status_result = worker.send_command("(Get-Service DeadlineWorker).Status")
                assert status_result.stdout.strip() != "Running"
            elif _running_on_macos():
                # `launchctl print` exits non-zero once the job is unloaded, which is what
                # stop_worker_service does on macOS. While it is still loaded the output carries
                # `state = running` until the process is gone, so both are checked: a job that is
                # loaded but stopped is also an acceptable stop.
                status_result = worker.send_command(
                    f"launchctl print system/{_MACOS_LAUNCHD_LABEL}"
                )
                assert status_result.exit_code != 0 or "state = running" not in status_result.stdout
            else:
                status_result = worker.send_command("systemctl is-active deadline-worker")
                assert status_result.exit_code != 0
                assert status_result.stdout.strip() != "active"

        check_worker_service_stopped()

        worker.start_worker_service()

        # Wait for the worker to come back online
        assert worker.worker_id is not None
        # Bound to a local: mypy does not carry the narrowing from the assert above into the
        # nested function, since worker.worker_id is an attribute that could change between
        # the assert and the call.
        worker_id = worker.worker_id

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=120,
            interval=10,
        )
        def wait_worker_started() -> None:
            assert worker.worker_id is not None  # narrow Optional inside the closure for mypy
            assert is_worker_started(
                deadline_client=deadline_client,
                farm_id=deadline_resources.farm.id,
                fleet_id=deadline_resources.fleet.id,
                worker_id=worker_id,
            )

        wait_worker_started()

        # -- Phase 3: Submit job -> expect success with python --------------------
        job = submit_sleep_job(
            "session_runtime switch rust->python test",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        _assert_log_contains(
            worker,
            "Selected session runtime: python",
            "after switching to python, log should show python selection",
        )
