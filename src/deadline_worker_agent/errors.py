# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations


class PlatformInterruption(Exception):
    """A shutdown warning signal was emitted by the platform"""

    pass


class ServiceShutdown(Exception):
    """The render management service is issuing a shutdown command"""

    pass
