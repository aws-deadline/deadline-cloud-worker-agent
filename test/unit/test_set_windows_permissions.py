# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import unittest
from unittest.mock import patch
from deadline_worker_agent.set_windows_permissions import set_user_restricted_path_permissions


class TestSetWindowsPermissions(unittest.TestCase):
    @patch("subprocess.run")
    @patch("getpass.getuser", return_value="testuser")
    def test_set_user_restricted_path_permissions_with_default_username(
        self, mock_getuser, mock_subprocess_run
    ):
        # GIVEN
        path = "C:\\example_directory_or_file"

        # WHEN
        set_user_restricted_path_permissions(path)

        # THEN
        expected_command = [
            "icacls",
            path,
            "/inheritance:r",
            "/grant",
            "{0}:(OI)(CI)(F)".format("testuser"),
            "/T",
        ]
        mock_subprocess_run.assert_called_once_with(expected_command, check=True)
        mock_getuser.assert_called_once()

    @patch("subprocess.run")
    @patch("getpass.getuser")
    def test_set_user_restricted_path_permissions_with_specific_username(
        self, mock_getuser, mock_subprocess_run
    ):
        # GIVEN
        path = "C:\\example_directory_or_file"
        custom_username = "customuser"

        # WHEN
        set_user_restricted_path_permissions(path, username=custom_username)

        # THEN
        expected_command = [
            "icacls",
            path,
            "/inheritance:r",
            "/grant",
            "{0}:(OI)(CI)(F)".format(custom_username),
            "/T",
        ]
        mock_subprocess_run.assert_called_once_with(expected_command, check=True)
        mock_getuser.assert_not_called()


if __name__ == "__main__":
    unittest.main()
