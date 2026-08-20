#!/usr/bin/env bash
#
# _supervise.sh — internal helper for overnight_e2e.sh. Runs each requested
# variant to completion, one at a time (never two farms' workers concurrently),
# retrying once on failure, and records a results table in SUMMARY.md.
#
# Expects these from the caller's environment:
#   REPO                   worker-agent worktree
#   REGION                 AWS region
#   VARIANTS               space-separated variant list
#   WORKER_AGENT_WHL_PATH  wheel under test
#   PIP_CONSTRAINT         (optional) pip constraints file
#
# Not meant to be invoked directly — run overnight_e2e.sh instead.
set -uo pipefail

OUTDIR="$REPO/.e2e-overnight"
SUMMARY="$OUTDIR/SUMMARY.md"

export AWS_DEFAULT_REGION="$REGION"
export WORKER_AGENT_WHL_PATH
[[ -n "${PIP_CONSTRAINT:-}" ]] && export PIP_CONSTRAINT

cd "$REPO"

variant_infra()  { case "$1" in linux-*) echo ".e2e_linux_infra.sh";; windows-*) echo ".e2e_windows_infra.sh";; esac; }
variant_script() { case "$1" in *-python) echo "test";; *-rust) echo "test-rust";; esac; }

pytest_banner() {
    grep -aoE '=+ [0-9]+ (passed|failed)[^=]*=+' "$1" 2>/dev/null | tail -1 \
        || echo "(no pytest summary — run did not reach a verdict)"
}

{
    echo "# Worker Agent E2E — overnight run summary"
    echo
    echo "- started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- repo: $REPO"
    echo "- commit: $(git log --oneline -1)"
    echo "- wheel: $WORKER_AGENT_WHL_PATH"
    echo "- region: $REGION"
    echo
    echo "| variant | attempt | exit | pytest summary |"
    echo "|---------|---------|------|----------------|"
} > "$SUMMARY"

run_one() {
    local tag="$1" infra="$2" script="$3" log="$OUTDIR/${1}.log"
    {
        echo "=== $tag ==="
        echo "started:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "infra:    $infra"
        echo "script:   hatch run e2e:$script"
        echo "wheel:    $WORKER_AGENT_WHL_PATH"
        echo "identity: $(aws sts get-caller-identity --query Arn --output text 2>&1)"
        echo "==============================="
    } > "$log"
    # shellcheck disable=SC1090
    ( source "$REPO/$infra"; echo "OPERATING_SYSTEM=${OPERATING_SYSTEM:-?} FARM_ID=${FARM_ID:-?}" >> "$log"; \
      hatch run "e2e:$script" >> "$log" 2>&1 )
    local rc=$?
    { echo "==============================="; echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"; echo "EXIT=$rc"; } >> "$log"
    return $rc
}

for tag in $VARIANTS; do
    infra="$(variant_infra "$tag")"
    script="$(variant_script "$tag")"

    echo "[supervisor] starting $tag ($(date -u +%H:%MZ))"
    run_one "$tag" "$infra" "$script"; rc=$?
    echo "$rc" > "$OUTDIR/${tag}.exit"
    echo "| $tag | 1 | $rc | $(pytest_banner "$OUTDIR/${tag}.log") |" >> "$SUMMARY"

    # One automatic retry. Most non-zero exits here are transient infra flakes
    # (EC2 capacity, SSM timeouts, bootstrap races); pre_test_cleanup runs at
    # the start of every attempt. Keep both logs so a real failure (both red)
    # is distinguishable from a flake (retry green).
    if [[ "$rc" != "0" ]]; then
        echo "[supervisor] $tag failed (exit=$rc); retrying once"
        run_one "${tag}-retry" "$infra" "$script"; rrc=$?
        echo "$rrc" > "$OUTDIR/${tag}-retry.exit"
        echo "| $tag | 2 (retry) | $rrc | $(pytest_banner "$OUTDIR/${tag}-retry.log") |" >> "$SUMMARY"
    fi
done

{
    echo
    echo "- finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "Exit 0 = all tests passed. Non-zero = pytest reported failures or the"
    echo "run could not start; read the matching .log. A variant that is red on"
    echo "attempt 1 but green on retry was an infrastructure flake, not a"
    echo "code failure."
} >> "$SUMMARY"

touch "$OUTDIR/SUPERVISOR_DONE"
echo "[supervisor] ALL RUNS COMPLETE — see $SUMMARY"
