# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Installer's behavior by submitting commands to the
Deadline Cloud worker and checking that the result/output of the worker agent is as we expect it.
"""
import pytest
import logging
from deadline_test_fixtures import EC2InstanceWorker
from utils import get_operating_system_name

LOG = logging.getLogger(__name__)


@pytest.mark.parametrize("operating_system", [get_operating_system_name()], indirect=True)
class TestInstaller:
    def test_installer_shutdown_permission(
        self,
        session_worker: EC2InstanceWorker,
    ) -> None:

        cmd_result = session_worker.send_command(
            "egrep \
                '^deadline-worker ALL=\(root\) NOPASSWD: /usr/sbin/shutdown now$' \
                /etc/sudoers.d/deadline-worker-shutdown"
        )

        assert cmd_result.exit_code == 0, f"Shutdown WA permission do not exist: {cmd_result}"
