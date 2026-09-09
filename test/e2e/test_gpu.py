# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
GPU compute E2E tests.

Validates that a job launched through the full worker-agent session path
(launchd daemon context -> sudo -u <jobRunAsUser> -> setsid shim -> task) can
actually reach the GPU and run a compute workload. The agent itself does not
mediate GPU access -- the job process talks to the graphics API directly -- so
this test confirms the session machinery does not get in the way (no window
server / GUI login session is available to a system daemon).

macOS: uses Metal via `swift`. The probe creates the default Metal device,
compiles a compute kernel at runtime, dispatches it, and verifies the result;
it exits non-zero on any failure, so a SUCCEEDED task status is the assertion.
"""

import os

import pytest
from deadline_test_fixtures import (
    DeadlineClient,
    Job,
    TaskStatus,
)

from e2e.conftest import DeadlineResources
from e2e.utils import submit_custom_job, job_failure_message


# A self-contained Metal compute probe. Written to the session working directory
# (the task's CWD) rather than $TMPDIR, which is empty in the `sudo -u ... -i`
# login shell and would resolve to the sealed read-only root volume.
_METAL_PROBE = r"""#!/bin/zsh
set -e
echo "=== identity ==="
whoami
echo "=== GPU inventory ==="
system_profiler SPDisplaysDataType | grep -E "Chipset Model|Metal Support"
echo "=== Metal compute ==="
cat > ./metalprobe.swift <<'SWIFT'
import Metal
guard let dev = MTLCreateSystemDefaultDevice() else { print("FAIL: no Metal device"); exit(1) }
print("device:", dev.name)
guard let q = dev.makeCommandQueue() else { print("FAIL: no command queue"); exit(2) }
let src = "kernel void doub(device float* o [[buffer(0)]], uint i [[thread_position_in_grid]]) { o[i] = float(i) * 2.0; }"
let lib = try! dev.makeLibrary(source: src, options: nil)
let pipe = try! dev.makeComputePipelineState(function: lib.makeFunction(name: "doub")!)
let n = 16
let buf = dev.makeBuffer(length: n * 4, options: .storageModeShared)!
let cb = q.makeCommandBuffer()!
let enc = cb.makeComputeCommandEncoder()!
enc.setComputePipelineState(pipe)
enc.setBuffer(buf, offset: 0, index: 0)
enc.dispatchThreads(MTLSize(width: n, height: 1, depth: 1),
                    threadsPerThreadgroup: MTLSize(width: n, height: 1, depth: 1))
enc.endEncoding()
cb.commit()
cb.waitUntilCompleted()
let p = buf.contents().bindMemory(to: Float.self, capacity: n)
print("compute result[8] =", p[8], "(expect 16.0)")
guard p[8] == 16.0 else { print("FAIL: wrong compute result"); exit(3) }
print("PASS: GPU compute works from session")
SWIFT
/usr/bin/swift ./metalprobe.swift
"""


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] != "macos",
    reason="macOS (Metal) specific GPU test",
)
class TestMacGPU:
    def test_metal_compute_runs_in_session(
        self,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        session_worker,
    ) -> None:
        """A task can create a Metal device and run a compute kernel from within
        the worker-agent session (daemon context, as the jobRunAsUser). The
        probe exits non-zero on any GPU failure, so SUCCEEDED is the assertion."""
        job: Job = submit_custom_job(
            job_name="macOS: Metal GPU compute",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            run_script=_METAL_PROBE,
            description=(
                "Validates GPU compute works from a job session on macOS. "
                "Expected status: SUCCEEDED if the Metal compute kernel runs correctly."
            ),
        )
        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )
