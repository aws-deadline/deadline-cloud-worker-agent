# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"

from .win_api import (
    PROFILEINFO,
    UnloadUserProfile,
)
from .logon import (
    logon_user,
    load_user_profile,
)

__all__ = [
    "load_user_profile",
    "logon_user",
    "PROFILEINFO",
    "UnloadUserProfile",
]
