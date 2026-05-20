# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys

assert sys.platform == "win32"

import win32security
from typing import Optional, Tuple


def is_domain_user(username: str) -> bool:
    """Returns True if the username is in a domain format (DDL or UPN)."""
    return "\\" in username or "@" in username


def parse_domain_and_user(username: str) -> Tuple[Optional[str], str]:
    """
    Parses a username into (domain, user) components.

    - "DOMAIN\\user" -> ("DOMAIN", "user")
    - "user@domain.com" -> (None, "user@domain.com")  # UPN passed as-is to LogonUser with domain=None
    - "user" -> (None, "user")

    Returns:
        Tuple of (domain, username) suitable for passing to LogonUser.
    """
    if "\\" in username:
        domain, user = username.split("\\", 1)
        return (domain, user)
    # UPN and local users: pass domain=None, let Windows resolve
    return (None, username)


def resolve_to_ddl(username: str) -> str:
    """
    Resolves any username format (local, DDL, UPN) to down-level logon name (DOMAIN\\user).

    Uses TranslateName which handles all formats natively.
    For local users, returns "COMPUTERNAME\\user".

    Raises:
        OSError: If the account cannot be found.
    """
    return win32security.TranslateName(
        username, win32security.NameUnknown, win32security.NameSamCompatible
    )
