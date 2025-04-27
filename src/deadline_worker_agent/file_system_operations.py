# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Optional
from openjd.sessions import WindowsSessionUser, SessionUser
import getpass
from pathlib import Path
import os
from typing import cast
from enum import Enum


class FileSystemPermissionEnum(Enum):
    READ = "READ"
    WRITE = "WRITE"
    EXECUTE = "EXECUTE"
    READ_WRITE = "READ_WRITE"
    FULL_CONTROL = "FULL_CONTROL"
    LIST_DIRECTORY_AND_READ = "LIST_DIRECTORY_AND_READ"


def set_permissions(
    file_path: Path,
    user_permission: Optional[FileSystemPermissionEnum] = None,
    permitted_user: Optional[SessionUser] = None,
    agent_user_permission: Optional[FileSystemPermissionEnum] = None,
):
    if os.name == "nt":
        permitted_user = cast(WindowsSessionUser, permitted_user)

        _set_windows_permissions(
            path=file_path,
            user=permitted_user.user if permitted_user else None,
            user_permission=user_permission,
            agent_user_permission=agent_user_permission,
        )


def touch_file(
    file_path: Path,
    user_permission: Optional[FileSystemPermissionEnum] = None,
    permitted_user: Optional[SessionUser] = None,
    agent_user_permission: Optional[FileSystemPermissionEnum] = None,
    group: Optional[str] = None,
    group_permission: Optional[FileSystemPermissionEnum] = None,
    disable_permission_inheritance: bool = False,
):
    if os.name == "nt":
        permitted_user = cast(WindowsSessionUser, permitted_user)

        if not file_path.exists():
            file_path.touch()

        _set_windows_permissions(
            path=file_path,
            user=permitted_user.user if permitted_user else None,
            user_permission=user_permission,
            agent_user_permission=agent_user_permission,
            group=group,
            group_permission=group_permission,
            disable_permission_inheritance=disable_permission_inheritance,
        )


def make_directory(
    dir_path: Path,
    user_permission: Optional[FileSystemPermissionEnum] = None,
    permitted_user: Optional[SessionUser] = None,
    agent_user_permission: Optional[FileSystemPermissionEnum] = None,
    exist_ok: bool = True,
    parents: bool = False,
):
    if os.name == "nt":
        permitted_user = cast(WindowsSessionUser, permitted_user)

        dir_path.mkdir(exist_ok=exist_ok, parents=parents)

        _set_windows_permissions(
            path=dir_path,
            user=permitted_user.user if permitted_user else None,
            user_permission=user_permission,
            agent_user_permission=agent_user_permission,
        )


def _set_windows_permissions(
    path: Path,
    user: Optional[str] = None,
    user_permission: Optional[FileSystemPermissionEnum] = None,
    group: Optional[str] = None,
    group_permission: Optional[FileSystemPermissionEnum] = None,
    agent_user_permission: Optional[FileSystemPermissionEnum] = None,
    users_group_permission: Optional[FileSystemPermissionEnum] = None,
    disable_permission_inheritance: bool = False,
):
    import win32security
    import ntsecuritycon

    agent_username = getpass.getuser()
    full_path = str(path.resolve())

    if user_permission is not None and user is None:
        raise ValueError("A user must be specified to set user permissions")

    if group_permission is not None and group is None:
        raise ValueError("A group must be specified to set group permissions")

    # We don't want to propagate existing permissions, so create a new DACL
    dacl = win32security.ACL()

    # Add an ACE to the DACL giving the agent user the required access and inheritance of the ACE
    if agent_user_permission is not None:
        user_sid, _, _ = win32security.LookupAccountName(None, agent_username)
        dacl.AddAccessAllowedAceEx(
            win32security.ACL_REVISION,
            ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
            _get_ntsecuritycon_mode(agent_user_permission),
            user_sid,
        )

    # Add an ACE to the DACL giving the additional user the required access and inheritance of the ACE
    if user_permission is not None and user is not None:
        user_sid, _, _ = win32security.LookupAccountName(None, user)
        dacl.AddAccessAllowedAceEx(
            win32security.ACL_REVISION,
            ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
            _get_ntsecuritycon_mode(user_permission),
            user_sid,
        )

    # Add an ACE to the DACL giving the group the required access and inheritance of the ACE
    if group_permission is not None and group is not None:
        # Note that despite the name LookupAccountName returns SIDs for groups too
        group_sid, _, _ = win32security.LookupAccountName(None, group)
        dacl.AddAccessAllowedAceEx(
            win32security.ACL_REVISION,
            ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
            _get_ntsecuritycon_mode(group_permission),
            group_sid,
        )

    # Add an ACE to the DACL giving the User group the required access and inheritance of the ACE
    if users_group_permission is not None:
        users_group_sid, _, _ = win32security.LookupAccountName(None, "Users")
        dacl.AddAccessAllowedAceEx(
            win32security.ACL_REVISION,
            ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE,
            _get_ntsecuritycon_mode(users_group_permission),
            users_group_sid,
        )

    # Get the security descriptor of the object
    sd = win32security.GetFileSecurity(str(path.resolve()), win32security.DACL_SECURITY_INFORMATION)

    # Disable inheritance and convert inherited ACEs to explicit ACEs
    if disable_permission_inheritance:
        sd.SetSecurityDescriptorControl(
            win32security.SE_DACL_PROTECTED, win32security.SE_DACL_PROTECTED
        )

    # Set the security descriptor's DACL to the newly-created DACL
    # Arguments:
    # 1. bDaclPresent = 1: Indicates that the DACL is present in the security descriptor.
    #    If set to 0, this method ignores the provided DACL and allows access to all principals.
    # 2. dacl: The discretionary access control list (DACL) to be set in the security descriptor.
    # 3. bDaclDefaulted = 0: Indicates the DACL was provided and not defaulted.
    #    If set to 1, indicates the DACL was defaulted, as in the case of permissions inherited from a parent directory.
    sd.SetSecurityDescriptorDacl(1, dacl, 0)

    # Set the security descriptor to the object
    win32security.SetFileSecurity(full_path, win32security.DACL_SECURITY_INFORMATION, sd)


def _get_ntsecuritycon_mode(mode: FileSystemPermissionEnum) -> int:
    import ntsecuritycon

    permission_mapping = {
        FileSystemPermissionEnum.READ.value: ntsecuritycon.FILE_GENERIC_READ,
        FileSystemPermissionEnum.WRITE.value: ntsecuritycon.FILE_GENERIC_WRITE,
        FileSystemPermissionEnum.READ_WRITE.value: ntsecuritycon.FILE_GENERIC_READ
        | ntsecuritycon.FILE_GENERIC_WRITE
        | ntsecuritycon.FILE_DELETE_CHILD,
        FileSystemPermissionEnum.EXECUTE.value: ntsecuritycon.FILE_GENERIC_EXECUTE
        | ntsecuritycon.FILE_GENERIC_READ,
        FileSystemPermissionEnum.FULL_CONTROL.value: ntsecuritycon.GENERIC_ALL,
        FileSystemPermissionEnum.LIST_DIRECTORY_AND_READ.value: ntsecuritycon.FILE_LIST_DIRECTORY
        | ntsecuritycon.FILE_TRAVERSE
        | ntsecuritycon.FILE_GENERIC_READ,
    }
    return permission_mapping[mode.value]
