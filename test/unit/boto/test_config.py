# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.config import Config

from deadline_worker_agent._version import __version__
from deadline.client import version as deadline_client_lib_version
from openjd.sessions import version as openjd_sessions_version
import deadline_worker_agent.boto.config as boto_config_mod


class TestDeadlineBotocoreConfig:
    """Tests for deadline_worker_agent.boto.config.DEADLINE_BOTOCORE_CONFIG"""

    def test_sets_user_agent(self) -> None:
        """Asserts that DEADLINE_BOTOCORE_CONFIG sets the user_agent_extra to identify the worker
        agent and its version in the format:

            deadline_worker_agent/<VERSION>
        """
        # WHEN
        DEADLINE_BOTOCORE_CONFIG = boto_config_mod.DEADLINE_BOTOCORE_CONFIG

        # THEN
        assert isinstance(DEADLINE_BOTOCORE_CONFIG, Config)
        libraries: list[str] = DEADLINE_BOTOCORE_CONFIG.user_agent_extra.split(" ")
        assert libraries[0] == f"deadline_worker_agent/{__version__}"
        assert libraries[1] == f"deadline_cloud/{deadline_client_lib_version}"
        assert libraries[2] == f"openjd_sessions/{openjd_sessions_version}"

    def test_does_not_retry(self) -> None:
        """Asserts that DEADLINE_BOTOCORE_CONFIG sets retries.max_attempts to 1.

        The worker agent handles the retry strategy.
        """
        # WHEN
        DEADLINE_BOTOCORE_CONFIG = boto_config_mod.DEADLINE_BOTOCORE_CONFIG

        # THEN
        assert isinstance(DEADLINE_BOTOCORE_CONFIG, Config)
        assert DEADLINE_BOTOCORE_CONFIG.retries == {
            "max_attempts": 1,
        }


class TestOtherBotocoreConfig:
    """Tests for deadline_worker_agent.boto.config.OTHER_BOTOCORE_CONFIG"""

    def test_uses_default_retries(self) -> None:
        """Asserts that OTHER_BOTOCORE_CONFIG does not configure retries"""
        # WHEN
        OTHER_BOTOCORE_CONFIG = boto_config_mod.OTHER_BOTOCORE_CONFIG

        # THEN
        assert isinstance(OTHER_BOTOCORE_CONFIG, Config)
        assert hasattr(OTHER_BOTOCORE_CONFIG, "retries") and OTHER_BOTOCORE_CONFIG.retries is None

    def test_sets_user_agent(self) -> None:
        """Asserts that OTHER_BOTOCORE_CONFIG sets the user_agent_extra to identify the worker
        agent and its version in the format:

            deadline_worker_agent/<VERSION>
        """
        # WHEN
        OTHER_BOTOCORE_CONFIG = boto_config_mod.OTHER_BOTOCORE_CONFIG

        # THEN
        assert isinstance(OTHER_BOTOCORE_CONFIG, Config)
        libraries: list[str] = OTHER_BOTOCORE_CONFIG.user_agent_extra.split(" ")
        assert libraries[0] == f"deadline_worker_agent/{__version__}"
        assert libraries[1] == f"deadline_cloud/{deadline_client_lib_version}"
        assert libraries[2] == f"openjd_sessions/{openjd_sessions_version}"
