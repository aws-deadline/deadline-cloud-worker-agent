# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.config import Config

from deadline_worker_agent._version import __version__
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
        assert DEADLINE_BOTOCORE_CONFIG.user_agent_extra == f"deadline_worker_agent/{__version__}"

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
        assert OTHER_BOTOCORE_CONFIG.user_agent_extra == f"deadline_worker_agent/{__version__}"
