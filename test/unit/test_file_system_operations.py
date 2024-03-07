# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from pathlib import Path
from unittest import mock
from unittest.mock import patch
from openjd.sessions import WindowsSessionUser
import pytest
import deadline_worker_agent.file_system_operations as file_system_operations


@pytest.mark.skipif(os.name != "nt", reason="Windows-only test.")
class TestFileSystemOperations:
    @patch("getpass.getuser", return_value="testuser")
    def test_set_permissions_invalid_session_user_with_user_permissions(self, mock_getuser):
        with pytest.raises(ValueError):
            # WHEN
            file_system_operations.set_permissions(
                file_path=Path(),
                user_permission=file_system_operations.FileSystemPermissionEnum.WRITE,
                permitted_user=None,
            )

    @patch("win32security.SetFileSecurity", return_value=mock.Mock)
    @patch("win32security.GetFileSecurity")
    @patch("win32security.ACL")
    @patch("win32security.LookupAccountName", return_value=tuple([mock.Mock, "mock_str", 1]))
    @patch("getpass.getuser", return_value="testuser")
    def test_set_permissions_valid_session_user_with_permissions(
        self,
        mock_getuser,
        mock_usersid,
        mock_dacl,
        mock_get_securitydescriptor,
        mock_set_securitydescriptor,
    ):
        # GIVEN
        valid_user = WindowsSessionUser(user="valid_user", password="fake_password")
        agent_user_permission_param = file_system_operations.FileSystemPermissionEnum.WRITE

        dacl_mocked_obj = mock_dacl.return_value
        sd_mocked_obj = mock_get_securitydescriptor.return_value

        # WHEN
        file_system_operations.set_permissions(
            file_path=Path(),
            permitted_user=valid_user,
            agent_user_permission=agent_user_permission_param,
        )

        # THEN
        dacl_mocked_obj.AddAccessAllowedAceEx.assert_called_once_with(2, 3, 1179926, mock.Mock)
        sd_mocked_obj.SetSecurityDescriptorDacl.assert_called_once_with(1, dacl_mocked_obj, 0)

    @patch("win32security.SetFileSecurity", return_value=mock.Mock)
    @patch("win32security.GetFileSecurity")
    @patch("win32security.ACL")
    @patch("win32security.LookupAccountName", return_value=tuple([mock.Mock, "mock_str", 1]))
    @patch("getpass.getuser", return_value="testuser")
    def test_set_permissions_valid_session_user_multiple_permissions(
        self,
        mock_getuser,
        mock_usersid,
        mock_dacl,
        mock_get_securitydescriptor,
        mock_set_securitydescriptor,
    ):
        # GIVEN
        valid_user = WindowsSessionUser(user="valid_user", password="fake_password")

        dacl_mocked_obj = mock_dacl.return_value
        sd_mocked_obj = mock_get_securitydescriptor.return_value

        # WHEN
        file_system_operations.set_permissions(
            file_path=Path(),
            permitted_user=valid_user,
            agent_user_permission=file_system_operations.FileSystemPermissionEnum.WRITE,
        )

        # THEN
        dacl_mocked_obj.AddAccessAllowedAceEx.assert_called_once_with(2, 3, 1179926, mock.Mock)
        sd_mocked_obj.SetSecurityDescriptorDacl.assert_called_once_with(1, dacl_mocked_obj, 0)

    @patch("win32security.SetFileSecurity", return_value=mock.Mock)
    @patch("win32security.GetFileSecurity")
    @patch("win32security.ACL")
    @patch("win32security.LookupAccountName", return_value=tuple([mock.Mock, "mock_str", 1]))
    @patch("getpass.getuser", return_value="testuser")
    @patch.object(Path, "exists")
    def test_touch_file_not_exists(
        self,
        mock_pathlib,
        mock_getuser,
        mock_usersid,
        mock_dacl,
        mock_get_securitydescriptor,
        mock_set_securitydescriptor,
    ):
        # GIVEN
        valid_user = WindowsSessionUser(user="valid_user", password="fake_password")

        file_path_mock = mock_pathlib.return_value
        file_path_mock.exists.return_value = False

        # WHEN
        file_system_operations.touch_file(
            file_path=file_path_mock,
            user_permission=file_system_operations.FileSystemPermissionEnum.WRITE,
            permitted_user=valid_user,
        )

        # THEN
        file_path_mock.touch.assert_called_once()

    @patch("win32security.SetFileSecurity", return_value=mock.Mock)
    @patch("win32security.GetFileSecurity")
    @patch("win32security.ACL")
    @patch("win32security.LookupAccountName", return_value=tuple([mock.Mock, "mock_str", 1]))
    @patch("getpass.getuser", return_value="testuser")
    @patch.object(Path, "mkdir")
    def test_make_directory(
        self,
        mock_pathlib,
        mock_getuser,
        mock_usersid,
        mock_dacl,
        mock_get_securitydescriptor,
        mock_set_securitydescriptor,
    ):
        # GIVEN
        valid_user = WindowsSessionUser(user="valid_user", password="fake_password")

        file_path_mock = mock_pathlib.return_value

        # WHEN
        file_system_operations.make_directory(
            dir_path=file_path_mock,
            user_permission=file_system_operations.FileSystemPermissionEnum.WRITE,
            permitted_user=valid_user,
        )

        # THEN
        file_path_mock.mkdir.assert_called_once_with(exist_ok=True, parents=False)
