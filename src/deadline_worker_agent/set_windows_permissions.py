# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Optional
import subprocess
import getpass


def set_user_restricted_path_permissions(path: str, username: Optional[str] = None):
    """
    Set permissions for a specified file or directory (and any child objects)
    to give full control only to the specified user.

    Args:
        path (str): The path of the file or directory for which permissions will be set.
        username (str, optional): The username for whom permissions will be granted. If none is
                        provided the current username will be used.

    Example:
        path = "C:\\example_directory_or_file"
        username = "a_username"
        set_user_restricted_path_permissions(path, username)
    """

    if not username:
        username = getpass.getuser()

    subprocess.run(
        [
            "icacls",
            path,
            # Remove any existing permissions
            "/inheritance:r",
            # OI - Contained objects will inherit
            # CI - Sub-directories will inherit
            # F  - Full control
            "/grant",
            ("{0}:(OI)(CI)(F)").format(username),
            "/T",  # Apply recursively for directories
        ],
        check=True,
    )
