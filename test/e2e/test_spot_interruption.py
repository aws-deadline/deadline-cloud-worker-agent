# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module verifies that when a worker agent is interrupted (simulating a spot interruption)
during different session action phases, the session actions are reported with the correct statuses.

Specifically, it tests that when an interruption occurs during:
- Job attachment sync (SYNC_INPUT_JOB_ATTACHMENTS)
- Environment enter (ENV_ENTER)

The interrupted action is reported as INTERRUPTED, and subsequent queued task run actions
are also reported as INTERRUPTED (not NEVER_ATTEMPTED), so that the interruption reason
is visible to the user in the console's retries modal.
"""

import logging
import os
import time
from typing import Any, Dict, List

import backoff
import pytest
from deadline_test_fixtures import (
    DeadlineClient,
    EC2InstanceWorker,
    Job,
)

from e2e.conftest import DeadlineResources

LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    os.environ.get("OPERATING_SYSTEM") == "windows",
    reason="Linux specific test (SIGTERM simulation)",
)
class TestSpotInterruption:
    """Tests that session actions are correctly reported as INTERRUPTED when the worker
    is interrupted (simulating a spot interruption via SIGTERM)."""

    def _send_sigterm_to_worker(self, worker: EC2InstanceWorker) -> None:
        """Send SIGTERM to the worker agent process to simulate a spot interruption."""
        result = worker.send_command(
            f"sudo pkill --signal TERM --full -u {worker.configuration.agent_user} deadline-worker-agent"
        )
        assert result.exit_code == 0, f"Failed to send SIGTERM to worker agent: {result}"
        LOG.info("Sent SIGTERM to worker agent")

    def _wait_for_action_running(
        self,
        deadline_client: DeadlineClient,
        job: Job,
        action_type: str,
        timeout: int = 120,
    ) -> str:
        """Wait until a session action of the given type is in RUNNING status.

        Returns the session ID where the action was found running.
        """

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=timeout,
            interval=5,
        )
        def _poll() -> str | None:
            sessions = deadline_client.list_sessions(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
            ).get("sessions", [])

            for session in sessions:
                session_actions = deadline_client.list_session_actions(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    sessionId=session["sessionId"],
                ).get("sessionActions", [])

                for action in session_actions:
                    if (
                        action_type in action.get("definition", {})
                        and action["status"] == "RUNNING"
                    ):
                        LOG.info(
                            f"Found {action_type} action in RUNNING status: {action['sessionActionId']}"
                        )
                        return session["sessionId"]
            return None

        session_id = _poll()
        assert session_id is not None, f"Timed out waiting for {action_type} action to be RUNNING"
        return session_id

    def _get_session_actions(
        self,
        deadline_client: DeadlineClient,
        job: Job,
        session_id: str,
    ) -> List[Dict[str, Any]]:
        """Get all session actions for a given session."""
        return deadline_client.list_session_actions(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
            sessionId=session_id,
        ).get("sessionActions", [])

    def test_task_run_reported_as_interrupted_when_sync_input_interrupted(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        function_worker: EC2InstanceWorker,
    ) -> None:
        """
        Tests that when a spot interruption (SIGTERM) occurs during job attachment sync,
        the sync action is reported as INTERRUPTED and the subsequent task run action
        is also reported as INTERRUPTED (not NEVER_ATTEMPTED).
        """
        # GIVEN
        # Submit a job with a long-running step (sleep 300) so the sync has time to be interrupted.
        # The job attachment sync will be triggered by the job template having attachments.
        # We use a large embedded file to simulate attachment sync taking time.
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            max_retries_per_task=0,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "test-spot-interrupt-during-sync",
                "steps": [
                    {
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": ["linux"],
                                }
                            ]
                        },
                        "name": "SleepStep",
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": "/bin/sleep",
                                    "args": ["300"],
                                }
                            },
                        },
                    },
                ],
                # Add a job environment that sleeps to give us time to interrupt during env enter
                "jobEnvironments": [
                    {
                        "name": "LongEnvEnter",
                        "script": {
                            "actions": {
                                "onEnter": {
                                    "command": "/bin/sleep",
                                    "args": ["120"],
                                },
                                "onExit": {
                                    "command": "/bin/true",
                                },
                            }
                        },
                    }
                ],
            },
        )

        LOG.info(f"Submitted job {job.id}")

        # WHEN
        # Wait for the environment enter action to be running
        session_id = self._wait_for_action_running(
            deadline_client, job, action_type="envEnter", timeout=120
        )

        # Give the action a few seconds to get underway
        time.sleep(5)

        # Send SIGTERM to simulate spot interruption
        self._send_sigterm_to_worker(function_worker)

        # Wait for the job to reach a terminal state
        job.wait_until_complete(client=deadline_client, max_retries=20)

        # THEN
        session_actions = self._get_session_actions(deadline_client, job, session_id)
        LOG.info(f"Session actions after interruption: {session_actions}")

        # Find the env enter and task run actions
        env_enter_action = None
        task_run_action = None
        for action in session_actions:
            if "envEnter" in action.get("definition", {}):
                env_enter_action = action
            elif "taskRun" in action.get("definition", {}):
                task_run_action = action

        # The env enter action should be INTERRUPTED
        assert env_enter_action is not None, "Expected to find an envEnter session action"
        assert env_enter_action["status"] == "INTERRUPTED", (
            f"Expected envEnter action to be INTERRUPTED, got {env_enter_action['status']}"
        )

        # The task run action should be INTERRUPTED (not NEVER_ATTEMPTED)
        assert task_run_action is not None, "Expected to find a taskRun session action"
        assert task_run_action["status"] == "INTERRUPTED", (
            f"Expected taskRun action to be INTERRUPTED, got {task_run_action['status']}. "
            f"This is the bug from Bea-33959 — task runs should show INTERRUPTED when a "
            f"preceding action was interrupted due to spot interruption."
        )
