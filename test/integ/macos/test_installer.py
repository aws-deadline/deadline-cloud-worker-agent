# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Integration tests for install_macos.sh.

These tests run the macOS installer for real (via sudo) and assert the
invariants it must establish. They mutate host state -- users, groups,
directories, a LaunchDaemon plist, and a sudoers file -- so they only run when
RUN_INSTALLER_TESTS=true (set by the macos_installer_test.yml workflow, whose
runners are throwaway VMs). The tests are ordered: installation happens in
module-scoped fixtures and later tests build on earlier runs' state.

The agent is installed from the repository checkout into a venv. The service is
never left running: the farm/fleet ids are fakes, and the one test that loads
the service with --start boots it out again.
"""

# This assertion short-circuits mypy from type checking this module on platforms other than macOS
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "darwin"

import os
import plistlib
import re
import subprocess
import time
from pathlib import Path

import pytest

try:
    from tomllib import load as load_toml
except ModuleNotFoundError:
    from tomli import load as load_toml

FARM_ID = "farm-aabbccddeeff11223344556677889900"
FLEET_ID = "fleet-00998877665544332211ffeeddccbbaa"
REGION = "us-west-2"
WA_USER = "deadline-worker"
JOB_GROUP = "deadline-job-users"
LAUNCHD_LABEL = "com.amazon.deadline.worker-agent"
PLIST_PATH = Path("/Library/LaunchDaemons") / f"{LAUNCHD_LABEL}.plist"
SUDOERS_PATH = Path("/etc/sudoers.d/deadline-worker-shutdown")

REPO_ROOT = Path(__file__).parent.parent.parent.parent
INSTALLER = REPO_ROOT / "src" / "deadline_worker_agent" / "installer" / "install_macos.sh"

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INSTALLER_TESTS", "").lower() != "true",
    reason=(
        "Skipping installer integration tests: they mutate host state (users, groups, "
        "LaunchDaemon, sudoers) and only run when RUN_INSTALLER_TESTS=true"
    ),
)


def run_installer(*extra_args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Runs install_macos.sh via sudo with the standard test arguments."""
    venv_bin = Path(os.environ["WA_VENV_BIN"])
    cmd = [
        "sudo",
        "bash",
        str(INSTALLER),
        "--farm-id",
        FARM_ID,
        "--fleet-id",
        FLEET_ID,
        "--region",
        REGION,
        "--scripts-path",
        str(venv_bin),
        "--python-interpreter-path",
        str(venv_bin / "python"),
        "-y",
        *extra_args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def sudo_output(*cmd: str) -> str:
    return subprocess.run(["sudo", *cmd], capture_output=True, text=True, check=True).stdout.strip()


def dscl_read(path: str, key: str) -> str:
    out = subprocess.run(
        ["dscl", ".", "-read", path, key], capture_output=True, text=True, check=True
    ).stdout
    # Standard attributes print as "Key: value"; native ones as
    # "dsAttrTypeNative:Key: value" -- take everything after the last colon.
    return out.rsplit(":", 1)[1].strip()


def user_exists(user: str) -> bool:
    return (
        subprocess.run(["dscl", ".", "-read", f"/Users/{user}"], capture_output=True).returncode
        == 0
    )


def service_is_registered() -> bool:
    return (
        subprocess.run(
            ["sudo", "launchctl", "print", f"system/{LAUNCHD_LABEL}"], capture_output=True
        ).returncode
        == 0
    )


def agent_process_running() -> bool:
    return (
        subprocess.run(["pgrep", "-f", "deadline-worker-agent"], capture_output=True).returncode
        == 0
    )


class TestVfsRejection:
    """--vfs-install-path is unsupported on macOS; it must fail before any mutation.

    Runs first: it asserts that the agent user does not exist yet, which is only
    meaningful before TestInstall's fixture has run the real installation.
    """

    def test_vfs_install_path_rejected_without_side_effects(self) -> None:
        # GIVEN a system that has not had the installer run on it
        assert not user_exists(WA_USER), "test ordering violated: installer already ran"

        # WHEN
        result = run_installer("--vfs-install-path", "/opt/deadline_vfs", check=False)

        # THEN
        assert result.returncode != 0
        # AND nothing was created
        assert not user_exists(WA_USER)
        assert not PLIST_PATH.exists()


@pytest.fixture(scope="module")
def installed() -> subprocess.CompletedProcess:
    """Runs the installer (with --allow-shutdown, without --start) once for this module."""
    return run_installer("--allow-shutdown")


class TestInstall:
    """Invariants established by a plain install (no --start)."""

    def test_agent_user_is_hidden_service_account(self, installed) -> None:
        assert user_exists(WA_USER)
        assert dscl_read(f"/Users/{WA_USER}", "IsHidden") == "1"
        assert "/usr/bin/false" in dscl_read(f"/Users/{WA_USER}", "UserShell")

    def test_agent_primary_group_is_not_job_group(self, installed) -> None:
        """SECURITY INVARIANT: the agent user's PRIMARY group is its dedicated
        per-user group -- never the job group, which is secondary-only. If this
        inverts, job-user processes could read agent credentials."""
        wa_gid = dscl_read(f"/Users/{WA_USER}", "PrimaryGroupID")
        job_gid = dscl_read(f"/Groups/{JOB_GROUP}", "PrimaryGroupID")
        assert wa_gid != job_gid
        # The primary gid resolves to the agent's dedicated self-named group
        assert dscl_read(f"/Groups/{WA_USER}", "PrimaryGroupID") == wa_gid
        # Job group membership is secondary
        groups = subprocess.run(
            ["id", "-Gn", WA_USER], capture_output=True, text=True, check=True
        ).stdout.split()
        assert JOB_GROUP in groups

    @pytest.mark.parametrize(
        ("path", "expected_mode", "expected_owner"),
        [
            ("/var/lib/deadline/credentials", "700", WA_USER),
            ("/etc/amazon/deadline", "750", "root"),
            ("/etc/amazon/deadline/worker.toml", "640", "root"),
            ("/var/log/amazon/deadline", "750", WA_USER),
        ],
    )
    def test_file_modes(
        self, installed, path: str, expected_mode: str, expected_owner: str
    ) -> None:
        # sudo is required to stat inside these directories: the test user is in
        # neither the agent group nor the job group, so unprivileged access is
        # denied -- which is itself the isolation working (asserted below).
        assert sudo_output("stat", "-f", "%Lp %Su", path) == f"{expected_mode} {expected_owner}"

    def test_session_root_is_under_var(self, installed) -> None:
        # /var is writable; the sealed read-only root volume is not (macOS 10.15+)
        assert (
            subprocess.run(
                ["sudo", "test", "-d", "/var/lib/deadline/sessions"], capture_output=True
            ).returncode
            == 0
        )

    def test_credentials_denied_to_unprivileged_user(self, installed) -> None:
        with pytest.raises(PermissionError):
            os.stat("/var/lib/deadline/credentials/anything")

    def test_worker_toml_contains_configuration(self, installed) -> None:
        raw = sudo_output("cat", "/etc/amazon/deadline/worker.toml")
        # Parse rather than grep so we validate the file is well-formed TOML too
        import io

        config = load_toml(io.BytesIO(raw.encode()))
        assert config["worker"]["farm_id"] == FARM_ID
        assert config["worker"]["fleet_id"] == FLEET_ID

    def test_plist_is_boot_ready_and_root_owned(self, installed) -> None:
        # launchd rejects group/other-writable daemon plists
        assert sudo_output("stat", "-f", "%Lp %Su %Sg", str(PLIST_PATH)) == "644 root wheel"
        plist = plistlib.loads(PLIST_PATH.read_bytes())
        assert plist["Label"] == LAUNCHD_LABEL
        # Runs as the agent user, not root
        assert plist["UserName"] == WA_USER
        # Boot-ready: RunAtLoad=true starts it whenever launchd loads it (every
        # boot -- the systemctl-enable analog), KeepAlive restarts it on failure
        assert plist["RunAtLoad"] is True
        assert plist["KeepAlive"] == {"SuccessfulExit": False}
        # ProgramArguments is exactly one element: the agent binary path. A
        # word-split bug would fracture a path containing spaces into multiple
        # argv elements (see test_plist_program_arguments_survive_spaced_path).
        venv_bin = Path(os.environ["WA_VENV_BIN"])
        assert plist["ProgramArguments"] == [str(venv_bin / "deadline-worker-agent")]

    def test_plist_program_arguments_survive_spaced_path(self, installed, tmp_path) -> None:
        """A scripts path containing a space (e.g. a venv under '/Users/My Name')
        must produce a single, intact ProgramArguments element."""
        spaced_dir = tmp_path / "spaced dir" / "bin"
        spaced_dir.mkdir(parents=True)
        venv_bin = Path(os.environ["WA_VENV_BIN"])
        agent_program = spaced_dir / "deadline-worker-agent"
        agent_program.symlink_to(venv_bin / "deadline-worker-agent")
        # World-traversable so the installer's checks and launchd can read it
        tmp_path.chmod(0o755)
        (tmp_path / "spaced dir").chmod(0o755)
        spaced_dir.chmod(0o755)

        result = subprocess.run(
            [
                "sudo",
                "bash",
                str(INSTALLER),
                "--farm-id",
                FARM_ID,
                "--fleet-id",
                FLEET_ID,
                "--region",
                REGION,
                "--scripts-path",
                str(spaced_dir),
                "--python-interpreter-path",
                str(venv_bin / "python"),
                "-y",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        plist = plistlib.loads(PLIST_PATH.read_bytes())
        assert plist["ProgramArguments"] == [str(agent_program)]
        # Restore the standard plist for subsequent tests
        run_installer("--allow-shutdown")

    def test_service_not_loaded_without_start(self, installed) -> None:
        """Without --start the installer must NOT load (bootstrap) the service:
        not registered with launchd, and no agent process running."""
        assert not service_is_registered()
        assert not agent_process_running()

    def test_sudoers_grants_exactly_shutdown(self, installed) -> None:
        assert sudo_output("stat", "-f", "%Lp %Su", str(SUDOERS_PATH)) == "440 root"
        # visudo validates the syntax
        subprocess.run(["sudo", "visudo", "-cf", str(SUDOERS_PATH)], check=True)
        content = sudo_output("cat", str(SUDOERS_PATH))
        assert re.search(
            rf"^{WA_USER} ALL=\(root\) NOPASSWD: /sbin/shutdown -h now$", content, re.MULTILINE
        )

    @pytest.mark.parametrize("path", [SUDOERS_PATH])
    def test_every_sudoers_file_is_root_owned_and_parses(self, installed, path: Path) -> None:
        """Every file this installer writes into /etc/sudoers.d goes through the same
        validate-then-install helper. A malformed file there breaks sudo host-wide, and a
        non-root-owned or group-writable one is ignored by sudo outright, so assert both
        properties for each file the installer creates.

        Parameterized over a single path today (the --allow-shutdown rule is the only
        one) so that adding a second sudoers file picks up these checks by listing it
        here rather than by writing a new test."""
        assert path.exists(), f"installer did not create {path}"
        assert sudo_output("stat", "-f", "%Lp %Su %Sg", str(path)) == "440 root wheel"
        subprocess.run(["sudo", "visudo", "-cf", str(path)], check=True)


class TestReinstall:
    """Behavior of running the installer again over an existing installation."""

    def test_reinstall_is_idempotent(self, installed) -> None:
        run_installer("--allow-shutdown")
        # Spot-check that the security invariants survived the re-run
        wa_gid = dscl_read(f"/Users/{WA_USER}", "PrimaryGroupID")
        job_gid = dscl_read(f"/Groups/{JOB_GROUP}", "PrimaryGroupID")
        assert wa_gid != job_gid
        assert (
            sudo_output("stat", "-f", "%Lp %Su", "/var/lib/deadline/credentials")
            == f"700 {WA_USER}"
        )
        plistlib.loads(PLIST_PATH.read_bytes())
        subprocess.run(["sudo", "visudo", "-cf", str(SUDOERS_PATH)], check=True)

    def test_reinstall_without_allow_shutdown_revokes_sudoers(self, installed) -> None:
        run_installer()
        assert not SUDOERS_PATH.exists()

    def test_install_with_start_registers_service(self, installed) -> None:
        """--start loads the service with launchd, which also proves launchd
        accepts the generated plist beyond XML well-formedness. The agent
        process itself cannot authenticate against the fake farm/fleet and may
        be mid-restart when we look, so only registration is asserted."""
        run_installer("--start")
        try:
            assert service_is_registered()
        finally:
            # The crash-looping agent must not outlive the test
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()

    def test_reinstall_with_start_survives_bootout_race(self, installed) -> None:
        """A re-install with --start over an already-loaded service must not
        abort on the asynchronous bootout/bootstrap race: launchd may still be
        unloading the old instance when the new bootstrap runs."""
        run_installer("--start")
        try:
            assert service_is_registered()
            # Immediately re-install over the loaded (crash-looping) service
            result = run_installer("--start", check=False)
            assert result.returncode == 0, result.stdout + result.stderr
            assert service_is_registered()
        finally:
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()

    def test_reinstall_without_start_restores_loaded_service(self, installed) -> None:
        """A config-only re-run (no --start) over a LOADED service must put the
        service back afterward -- matching Linux, where a re-run without
        `systemctl start` leaves a running service running. Without this, a
        config change (e.g. toggling --allow-shutdown) would silently take the
        worker offline until the next reboot."""
        run_installer("--start")
        try:
            assert service_is_registered()
            # Config-only re-run: no --start
            run_installer()
            assert service_is_registered(), (
                "re-install without --start left the previously-loaded service stopped"
            )
        finally:
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()

    def test_reinstall_without_start_over_unloaded_service_stays_unloaded(self, installed) -> None:
        """The restore logic must not overreach: a re-run without --start over
        an UNLOADED service must leave it unloaded (only boot starts it)."""
        assert not service_is_registered(), "test precondition: service must be unloaded"
        run_installer()
        assert not service_is_registered()
        assert not agent_process_running()

    def test_reinstall_over_loaded_service_warns_about_restart(self, installed) -> None:
        """launchd cannot reload a changed plist in place, so a re-install
        restarts a running agent and interrupts any session it is running --
        a divergence from Linux, where a config-only re-run leaves the running
        process untouched. The installer must say so rather than doing it
        silently."""
        run_installer("--start")
        try:
            assert service_is_registered()
            result = run_installer()
            assert "restarted" in result.stdout, (
                f"re-install over a loaded service did not warn about the restart: {result.stdout}"
            )
        finally:
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()

    def test_first_install_does_not_warn_about_restart(self, installed) -> None:
        """The restart warning must be specific to the re-install-over-loaded
        case: a first install has no running agent to interrupt."""
        assert not service_is_registered(), "test precondition: service must be unloaded"
        result = run_installer("--start")
        try:
            assert "restarted" not in result.stdout, (
                f"install over an unloaded service warned about a restart: {result.stdout}"
            )
        finally:
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()


class TestStartsOnBoot:
    """The daemon must come back by itself after a reboot.

    SCOPE: this covers the launchd half of "reboot and re-attach" only. A real
    reboot cannot run on a GitHub macOS runner, and re-registering with the
    Deadline service needs a live farm/fleet, which these tests deliberately do
    not have (the ids are fakes). What is reproduced here is the mechanism a
    reboot uses: at boot launchd loads every plist in /Library/LaunchDaemons and
    starts the ones with RunAtLoad=true. `launchctl bootstrap system <plist>` is
    that same load-and-start step, so a plist that starts under bootstrap from a
    cold (unloaded) state is a plist that starts on boot.

    End-to-end reboot + re-attach against a real fleet is tracked separately; it
    needs an EC2 Mac dedicated host and macOS support in
    deadline-cloud-test-fixtures.
    """

    def test_installed_plist_starts_the_daemon_from_cold(self, installed) -> None:
        """Install without --start, then load the plist the way boot does.

        This is the regression guard for the RunAtLoad/KeepAlive combination. If
        RunAtLoad were false (or the plist were otherwise not boot-ready), the
        install would leave a worker that never runs again after a restart --
        which is what an operator experiences as "the host rebooted and the
        worker never came back"."""
        # GIVEN a config-only install (no --start) over an unloaded service, so
        # nothing is running and only the on-disk plist can bring it back
        run_installer()
        assert not service_is_registered(), "test precondition: service must be unloaded"

        plist = plistlib.loads(PLIST_PATH.read_bytes())
        assert plist["RunAtLoad"] is True, (
            "plist is not boot-ready: RunAtLoad must be true or the daemon will "
            "not start after a reboot"
        )

        # WHEN launchd loads it, exactly as it does for /Library/LaunchDaemons at boot
        try:
            subprocess.run(
                ["sudo", "launchctl", "bootstrap", "system", str(PLIST_PATH)],
                capture_output=True,
                check=True,
            )

            # THEN the service is registered and launchd actually spawned it.
            # The agent cannot authenticate against the fake farm/fleet, so it
            # exits and KeepAlive respawns it; poll for a PID rather than
            # requiring one at an instant we might catch between restarts.
            assert service_is_registered()
            assert self._daemon_had_a_pid(), (
                "launchd loaded the plist but never spawned the process; "
                "RunAtLoad did not take effect"
            )
        finally:
            subprocess.run(
                ["sudo", "launchctl", "bootout", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
            )
        assert not service_is_registered()

    @staticmethod
    def _daemon_had_a_pid(timeout_s: float = 20.0) -> bool:
        """True if `launchctl print` ever reports a pid for the daemon.

        A crash-looping job is only briefly resident, so sample repeatedly
        instead of once. A "pid = N" line means launchd started the process,
        which is the property under test; whether that process then exits on the
        fake credentials is irrelevant here.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            out = subprocess.run(
                ["sudo", "launchctl", "print", f"system/{LAUNCHD_LABEL}"],
                capture_output=True,
                text=True,
            ).stdout
            if re.search(r"^\s*pid\s*=\s*\d+", out, re.MULTILINE):
                return True
            time.sleep(0.5)
        return False
