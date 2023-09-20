# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import unittest
from unittest.mock import patch
from deadline_worker_agent.set_windows_permissions import grant_full_control


class TestGrantFullControl(unittest.TestCase):
    @patch("subprocess.run")
    @patch("getpass.getuser", return_value="testuser")
    def test_grant_full_control_with_default_username(self, mock_getuser, mock_subprocess_run):
        path = "C:\\example_directory_or_file"
        grant_full_control(path)

        expected_command = [
            "icacls",
            path,
            "/inheritance:r",
            "/grant",
            "{0}:(OI)(CI)(F)".format("testuser"),
            "/T",
        ]

        mock_subprocess_run.assert_called_once_with(expected_command)

        mock_getuser.assert_called_once()

    @patch("subprocess.run")
    @patch("getpass.getuser")
    def test_grant_full_control_with_custom_username(self, mock_getuser, mock_subprocess_run):
        path = "C:\\example_directory_or_file"
        custom_username = "customuser"
        grant_full_control(path, username=custom_username)

        expected_command = [
            "icacls",
            path,
            "/inheritance:r",
            "/grant",
            "{0}:(OI)(CI)(F)".format(custom_username),
            "/T",
        ]

        mock_subprocess_run.assert_called_once_with(expected_command)

        mock_getuser.assert_not_called()


if __name__ == "__main__":
    unittest.main()
