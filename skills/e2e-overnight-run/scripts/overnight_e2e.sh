#!/usr/bin/env bash
#
# overnight_e2e.sh — one-command, unattended worker-agent E2E across all four
# runtime x OS variants (linux-python, linux-rust, windows-python,
# windows-rust).
#
# It validates every prerequisite up front (fail loud, never silently test the
# wrong thing), builds the wheel under test, rebuilds the e2e hatch env, then
# launches a detached tmux supervisor that runs each variant to completion with
# a single automatic retry on failure.
#
# USAGE
#   overnight_e2e.sh --repo /path/to/worktree [options]
#
# OPTIONS
#   --repo DIR         Worker-agent git worktree to test (REQUIRED). The wheel
#                      is built from HERE, so check out / cherry-pick the code
#                      under test into it first.
#   --variants "..."   Space-separated subset of:
#                        linux-python linux-rust windows-python windows-rust
#                      Default: all four.
#   --region REGION    AWS region (default: from AWS_DEFAULT_REGION, else
#                      us-west-2).
#   --constraints FILE Optional pip constraints file (exported as PIP_CONSTRAINT
#                      for the env build). Use on hosts where a test-only
#                      transitive dependency has no compatible binary wheel.
#   --keep-env         Do not remove/rebuild the e2e hatch env. Faster, but
#                      risks a stale deadline-cloud-test-fixtures without the
#                      session_runtime field.
#   --session TMUX     tmux session name (default: e2e).
#   -h | --help        Show this help.
#
# PREREQUISITES (checked; the script aborts if any are missing)
#   * Run from inside a git worktree of deadline-cloud-worker-agent.
#   * Valid AWS credentials for your e2e account (aws sts get-caller-identity).
#   * .e2e_linux_infra.sh and/or .e2e_windows_infra.sh present in --repo.
#     Generate them once per account (see the E2E prerequisites in the skill
#     README / DEVELOPMENT.md):
#       ./scripts/get_e2e_test_ids_from_cfn.sh --os Linux   > .e2e_linux_infra.sh
#       ./scripts/get_e2e_test_ids_from_cfn.sh --os Windows > .e2e_windows_infra.sh
#   * tmux, hatch, git, aws on PATH.
#
# OUTPUT (under <repo>/.e2e-overnight/)
#   <variant>.log         full pytest output for the first attempt
#   <variant>.exit        exit code sentinel (0 = all passed)
#   <variant>-retry.log   retry attempt, if the first failed
#   SUMMARY.md            one table row per attempt with the pytest banner
#   SUPERVISOR_DONE       created when every variant has finished
#
# MONITOR
#   tmux attach -t e2e
#   tail -f <repo>/.e2e-overnight/<variant>.log
#   cat <repo>/.e2e-overnight/SUMMARY.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REPO=""
VARIANTS="linux-python linux-rust windows-python windows-rust"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"
CONSTRAINTS_SRC=""
KEEP_ENV=0
TMUX_SESSION="e2e"

die() { echo "ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)        REPO="$2"; shift 2;;
        --variants)    VARIANTS="$2"; shift 2;;
        --region)      REGION="$2"; shift 2;;
        --constraints) CONSTRAINTS_SRC="$2"; shift 2;;
        --keep-env)    KEEP_ENV=1; shift;;
        --session)     TMUX_SESSION="$2"; shift 2;;
        -h|--help)     grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
        *) die "unknown argument: $1";;
    esac
done

# ---------------------------------------------------------------------------
# Validate prerequisites — fail loud before spending hours on EC2.
# ---------------------------------------------------------------------------
[[ -n "$REPO" ]] || die "--repo is required"
REPO="$(cd "$REPO" 2>/dev/null && pwd)" || die "--repo path does not exist"
cd "$REPO"

git rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || die "$REPO is not a git worktree"

for tool in tmux hatch git aws; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found on PATH"
done

export AWS_DEFAULT_REGION="$REGION"
aws sts get-caller-identity >/dev/null 2>&1 \
    || die "no valid AWS credentials for region $REGION (configure them first)"

needs_linux=0; needs_windows=0
for v in $VARIANTS; do
    case "$v" in
        linux-*)   needs_linux=1;;
        windows-*) needs_windows=1;;
        *) die "unknown variant '$v' (want: linux-python linux-rust windows-python windows-rust)";;
    esac
done
[[ $needs_linux -eq 1   && ! -f "$REPO/.e2e_linux_infra.sh"   ]] && die "missing $REPO/.e2e_linux_infra.sh"
[[ $needs_windows -eq 1 && ! -f "$REPO/.e2e_windows_infra.sh" ]] && die "missing $REPO/.e2e_windows_infra.sh"

OUTDIR="$REPO/.e2e-overnight"
mkdir -p "$OUTDIR"

CONSTRAINTS=""
if [[ -n "$CONSTRAINTS_SRC" ]]; then
    [[ -f "$CONSTRAINTS_SRC" ]] || die "--constraints file not found: $CONSTRAINTS_SRC"
    CONSTRAINTS="$OUTDIR/constraints.txt"
    cp "$CONSTRAINTS_SRC" "$CONSTRAINTS"
    export PIP_CONSTRAINT="$CONSTRAINTS"
fi

echo "==> Building the wheel under test from $REPO"
rm -f dist/*
hatch build >/dev/null
WHL="$(ls "$REPO"/dist/*.whl | head -1)"
[[ -f "$WHL" ]] || die "wheel build produced no .whl"
export WORKER_AGENT_WHL_PATH="$WHL"
echo "    wheel: $WHL"
echo "    commit: $(git log --oneline -1)"

if [[ $KEEP_ENV -eq 0 ]]; then
    echo "==> Rebuilding the e2e hatch env (avoids a stale test-fixtures pin)"
    hatch env remove e2e >/dev/null 2>&1 || true
    hatch run e2e:sync >/dev/null 2>&1 \
        || die "e2e env sync failed (see requirements-e2e.txt; a --constraints file may be needed on this host)"
fi

# The single most valuable guard: the fixtures library MUST expose the
# session_runtime field or the runtime-routing tests fail on a config-injection
# error rather than a real signal.
echo "==> Verifying deadline-cloud-test-fixtures exposes session_runtime"
hatch run e2e:python - <<'PY' || die "test-fixtures lacks session_runtime; recreate the env or bump requirements-e2e.txt"
import dataclasses, sys
from deadline_test_fixtures import DeadlineWorkerConfiguration as C
sys.exit(0 if "session_runtime" in {f.name for f in dataclasses.fields(C)} else 1)
PY

# ---------------------------------------------------------------------------
# Launch the supervisor in a detached tmux session.
# ---------------------------------------------------------------------------
rm -f "$OUTDIR/SUPERVISOR_DONE"
# env -u TMUX so this works whether or not you are already inside tmux.
env -u TMUX tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true
env -u TMUX tmux new-session -d -s "$TMUX_SESSION" -x 220 -y 50
env -u TMUX tmux send-keys -t "$TMUX_SESSION" \
    "REPO='$REPO' REGION='$REGION' VARIANTS='$VARIANTS' PIP_CONSTRAINT='${CONSTRAINTS}' WORKER_AGENT_WHL_PATH='$WHL' bash '$SCRIPT_DIR/_supervise.sh' 2>&1 | tee '$OUTDIR/supervisor.log'" \
    C-m

echo
echo "==> Launched. All requested variants will run unattended in tmux session '$TMUX_SESSION'."
echo "    Monitor : tmux attach -t $TMUX_SESSION"
echo "    Results : cat $OUTDIR/SUMMARY.md"
echo "    Done when: $OUTDIR/SUPERVISOR_DONE exists"
