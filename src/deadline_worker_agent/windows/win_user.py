# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys

assert sys.platform == "win32"


def is_domain_user(username: str) -> bool:
    """Returns True if the username is in a domain format.

    Domain users can be specified in two formats:
    - Down-Level Logon Name (DDL): DOMAIN\\username
    - User Principal Name (UPN): username@domain.com

    See https://learn.microsoft.com/en-us/windows/win32/secauthn/user-name-formats
    """
    return "\\" in username or "@" in username
