---
name: e2e-overnight-run
description: Run the full worker-agent E2E suite across all four runtime x OS variants (Linux/Windows x Python/Rust session runtimes) unattended against the live Deadline Cloud service. Builds the wheel under test, rebuilds the e2e env, and supervises sequential runs in tmux with automatic retry and a results summary. Use when validating a release candidate, a session-runtime change, or a dependency bump before shipping.
tags: [deadline-cloud, worker-agent, e2e, session-runtime, rust, python, linux, windows, overnight, release-validation, tmux, hatch, pytest]
---

# Comprehensive Worker Agent E2E Overnight Run

Run `test/e2e` across all four combinations — **Linux + Python**,
**Linux + Rust**, **Windows + Python**, **Windows + Rust** — in one unattended,
tmux-backed, retrying sequence. Built for release validation: kick it off, come
back later, read one summary table.

The worker agent's `session_runtime` setting selects the OpenJD session backend
(`python` = v0, `rust` = v1). E2E only pins the runtime on a fresh worker
config, so validating both backends means running the suite twice per OS. This
skill automates that matrix. It builds on the E2E setup documented in the
`worker-agent-testing` skill and `DEVELOPMENT.md`.

## When to use

- Validating a **release candidate** before shipping.
- A change to the **session runtime** (adapter code, runtime selection, the
  service `runtimeHint` path) or an **openjd-sessions / openjd-model pin bump**.
- Any change where "does the agent still run real jobs on both runtimes, on
  both OSes?" is the question.

For a scoped change (Rust-only), run just the two rust variants — see
`--variants`.

## Runtime selection reference

| Item | Value |
|------|-------|
| Config field | `session_runtime` = `python` \| `rust` \| `service-selected` (config-file / CLI only — no environment variable) |
| Test-fixtures field | `DeadlineWorkerConfiguration.session_runtime` (recent `deadline-cloud-test-fixtures`) |
| Hatch scripts | `e2e:test` (agent default = python), `e2e:test-rust` (whole suite pinned to rust) |
| Runtime selector | `--session-runtime {python,rust,service-selected}` pytest option / `WORKER_AGENT_SESSION_RUNTIME` env var |
| Canonical runtime proof | worker log line `Selected session runtime: <rt> (hint=...)` |

## Prerequisites

1. **A worker-agent git worktree** with the code under test checked out (or
   cherry-picked in). The wheel is built from here — test mainline from a
   mainline checkout, or a PR branch by checking it out first.
2. **AWS credentials** for your e2e account, valid long enough for the whole
   run (~6–8h for all four variants). Configure them however you normally do
   (`AWS_PROFILE` + `AWS_DEFAULT_REGION`, SSO, etc.).
3. **Testing infrastructure deployed** and the per-OS infra env-var scripts
   present in the worktree root. See the E2E prerequisites in the
   `worker-agent-testing` skill / `DEVELOPMENT.md`:
   ```sh
   scripts/deploy_e2e_testing_infrastructure.sh
   ./scripts/get_e2e_test_ids_from_cfn.sh --os Linux   > .e2e_linux_infra.sh
   ./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh
   ```
4. `tmux`, `hatch`, `git`, `aws` on PATH.

## Run it

```sh
# All four variants, unattended:
skills/e2e-overnight-run/scripts/overnight_e2e.sh --repo /path/to/worktree

# Rust-only re-validation after a Rust-path change:
overnight_e2e.sh --repo /path/to/worktree --variants "linux-rust windows-rust"
```

The script, in order:

1. Validates every prerequisite (worktree, credentials, infra scripts, tools)
   and **aborts before provisioning anything** if one is missing.
2. `hatch build` → exports `WORKER_AGENT_WHL_PATH` to the built wheel.
3. Rebuilds the `e2e` hatch env so it resolves the pinned test dependencies.
4. **Verifies `DeadlineWorkerConfiguration` exposes `session_runtime`** — the
   most common stale-environment failure.
5. Launches a detached tmux supervisor that runs each variant sequentially,
   retrying once on failure.

Everything lands in `<repo>/.e2e-overnight/`:

| File | Meaning |
|------|---------|
| `<variant>.log` | full pytest output |
| `<variant>.exit` | exit code (`0` = all passed) |
| `<variant>-retry.log` | retry attempt, if the first failed |
| `SUMMARY.md` | one row per attempt with the pytest banner |
| `SUPERVISOR_DONE` | present once every variant has finished |

## Read the results

```sh
cat <repo>/.e2e-overnight/SUMMARY.md
```

- **Exit 0** on a variant = all tests passed on that runtime + OS.
- **Red on attempt 1, green on retry** = infrastructure flake (EC2 capacity,
  SSM timeout, bootstrap race), not a code failure.
- **Red on both** = real failure. Read `<variant>.log`; search for `^FAILED ` /
  `^ERROR ` lines (not the substring `CREATE_FAILED`, which is benign
  pre-test-cleanup noise).

Wall time is roughly 6–8h for all four (Linux ~1.5h each, Windows ~2.5h each).

## Verifying a run was genuine (not a silent fallback)

Two traps make a green run meaningless — check both when it matters:

1. **Wrong wheel.** If `WORKER_AGENT_WHL_PATH` is unset, the harness installs
   the published package instead of your build. `overnight_e2e.sh` always sets
   it; confirm via the `wheel:` line at the top of each `<variant>.log`.
2. **Runtime actually used.** The rust variants use `e2e:test-rust` (recorded
   as the `script:` line in the log). To prove a session used rust, grep the
   worker's own log line `Selected session runtime: rust`. The
   `test/e2e/test_session_runtime.py` routing tests assert exactly this, and a
   negative control (an invalid `session_runtime` value making the worker fail
   to start) is how the injection path is validated.

## Gotchas

- **Stale e2e env.** A previously built `e2e` env can pin an old
  `deadline-cloud-test-fixtures` without the `session_runtime` field, so the
  runtime-routing tests die on a config-injection error. The script rebuilds
  the env by default; use `--keep-env` only when you know it is current.
- **Missing binary wheels on older hosts.** If the env build fails compiling a
  test-only transitive dependency from source, pass a pip constraints file via
  `--constraints` (see `scripts/e2e-constraints.txt.example`).
- **Parametrize labels are not the OS.** A test id like
  `test_...path_mapping[windows]` in the Linux suite is a *parameter label* (a
  Windows-style storage profile exercised on the Linux worker), not a Windows
  worker. Real Windows-only tests appear as `SKIPPED` in Linux runs.
- **Windows `xpassed`/`xfailed`.** Some Windows-only tests are marked
  `xfail(strict=False)` for known-unreliable environment setup and may xfail or
  xpass run-to-run without affecting the exit code.
- **Credential lifetime.** The supervisor cannot refresh credentials; ensure
  they cover the full run before launching.

## Scripts

| Script | Role |
|--------|------|
| `scripts/overnight_e2e.sh` | Entry point — validate, build, prep env, launch supervisor. |
| `scripts/_supervise.sh` | Internal — sequential runs, retry, `SUMMARY.md`. |
| `scripts/e2e-constraints.txt.example` | Template for the optional `--constraints` file. |
