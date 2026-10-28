# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Blender render E2E test.

Validates that a job submitted to Deadline Cloud is picked up by the worker agent and rendered
by Blender, end to end: service -> agent -> launchd daemon context -> sudo -u <jobRunAsUser> ->
Blender. The other suites exercise the agent with shell probes; this one runs a real DCC.

macOS only, for now. The worker host has Blender installed by pipeline/e2e-macos.sh, because
Deadline Cloud's conda channel has no macOS Blender package to deliver it through a queue
environment.

The job renders the factory-startup scene (a cube, a light, and a camera) with Cycles on the
CPU, so it needs no GPU and no scene file, and therefore no job attachments. Blender's
`--background` mode exits 0 even after a failed render, so the script loads the output back and
checks that it is the expected size and not blank; it exits non-zero otherwise, which makes a
SUCCEEDED task status the assertion.
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

_BLENDER_PATH = os.environ.get("E2E_BLENDER_PATH", "")

# Written to the session working directory (the task's CWD) rather than $TMPDIR, which is empty in
# the `sudo -u ... -i` login shell and would resolve to the sealed read-only root volume.
_RENDER_SCRIPT = r"""#!/bin/zsh
set -euo pipefail
BLENDER='{blender}'
echo "=== identity ==="; whoami
echo "=== blender ==="; "$BLENDER" --version | sed -n 1p
cat > ./render_probe.py <<'PY'
import os, sys, bpy
scene = bpy.context.scene
scene.render.engine = "CYCLES"
scene.cycles.device = "CPU"
scene.cycles.samples = 8
scene.cycles.use_denoising = False
scene.render.resolution_x, scene.render.resolution_y = 320, 240
scene.render.image_settings.file_format = "PNG"
# An absolute path: "//" resolves against the saved .blend file, and this scene was never saved.
out = os.path.join(os.getcwd(), "frame_0001.png")
scene.render.filepath = out
bpy.ops.render.render(write_still=True)
img = bpy.data.images.load(out)
w, h = img.size
px = img.pixels[:]
lum = [0.299 * px[i] + 0.587 * px[i + 1] + 0.114 * px[i + 2] for i in range(0, len(px), 4)]
spread = max(lum) - min(lum)
print(f"PROBE: {{w}}x{{h}} spread={{spread:.3f}}")
if (w, h) != (320, 240) or spread < 0.1:
    print("FAIL: render missing or blank")
    sys.exit(3)
print("PASS: Blender rendered a non-blank frame")
PY
# --python-exit-code turns an exception in the probe into a non-zero exit status.
"$BLENDER" --background --factory-startup --python-exit-code 1 --python ./render_probe.py 2>&1
"""


def _require_blender() -> None:
    """Skip when Blender isn't installed, unless this run requires it.

    pipeline/e2e-macos.sh sets E2E_REQUIRE_BLENDER, so on CI a missing Blender fails this test
    instead of skipping it: a skip there would report green for a render that never ran.
    """
    if _BLENDER_PATH and os.access(_BLENDER_PATH, os.X_OK):
        return
    reason = (
        "Blender is not installed on this worker host (E2E_BLENDER_PATH is "
        f"{_BLENDER_PATH or 'unset'}). pipeline/e2e-macos.sh installs it; see its log for why "
        "that failed."
    )
    if os.environ.get("E2E_REQUIRE_BLENDER"):
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] != "macos",
    reason="macOS-specific Blender test; Blender is installed only on the macOS worker host",
)
class TestMacBlender:
    def test_blender_job_renders_a_frame(
        self,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        session_worker,
    ) -> None:
        """A Blender job submitted to Deadline Cloud is picked up by the worker agent and renders
        a non-blank frame as the jobRunAsUser. The script exits non-zero if the render is
        missing or blank, so SUCCEEDED is the assertion."""
        _require_blender()
        job: Job = submit_custom_job(
            job_name="macOS: Blender render",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            run_script=_RENDER_SCRIPT.format(blender=_BLENDER_PATH),
            description=(
                "Validates the worker agent runs a Blender render end to end on macOS. "
                "Expected status: SUCCEEDED if Blender renders a non-blank frame."
            ),
        )
        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )
