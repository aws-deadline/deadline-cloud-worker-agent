# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import ctypes
import sys
from ctypes.wintypes import (
    BOOL,
    DWORD,
    HANDLE,
    LONG,
    LPCWSTR,
    LPWSTR,
    PDWORD,
    PHANDLE,
    ULONG,
)
from ctypes import POINTER
from typing import Sequence


# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
assert sys.platform == "win32"


# =======================
# Structures
# =======================


# https://learn.microsoft.com/en-us/windows/win32/api/ntdef/ns-ntdef-luid
class LUID(ctypes.Structure):
    _fields_ = [("LowPart", ULONG), ("HighPart", LONG)]


class LUID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("Luid", LUID), ("Attributes", DWORD)]


# https://learn.microsoft.com/en-us/windows/win32/api/profinfo/ns-profinfo-profileinfoa
class PROFILEINFO(ctypes.Structure):
    _fields_ = [
        ("dwSize", DWORD),
        ("dwFlags", DWORD),
        ("lpUserName", LPWSTR),
        ("lpProfilePath", LPWSTR),
        ("lpDefaultPath", LPWSTR),
        ("lpServerName", LPWSTR),
        ("lpPolicyPath", LPWSTR),
        ("hProfile", HANDLE),
    ]


# https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-token_privileges
class TOKEN_PRIVILEGES(ctypes.Structure):
    _fields_ = [
        ("PrivilegeCount", DWORD),
        # Note: To use
        #   ctypes.cast(ctypes.byref(self.Privileges), ctypes.POINTER(LUID_AND_ATTRIBUTES * self.PrivilegeCount)).contents
        ("Privileges", LUID_AND_ATTRIBUTES * 0),
    ]

    @staticmethod
    def allocate(length: int) -> "TOKEN_PRIVILEGES":
        malloc_size_in_bytes = ctypes.sizeof(TOKEN_PRIVILEGES) + 2 * ctypes.sizeof(
            LUID_AND_ATTRIBUTES
        )
        malloc_buffer = (ctypes.c_byte * malloc_size_in_bytes)()
        token_privs = ctypes.cast(malloc_buffer, POINTER(TOKEN_PRIVILEGES))[0]
        token_privs.PrivilegeCount = length
        return token_privs

    def privileges_array(self) -> Sequence[LUID_AND_ATTRIBUTES]:
        return ctypes.cast(
            ctypes.byref(self.Privileges), ctypes.POINTER(LUID_AND_ATTRIBUTES * self.PrivilegeCount)
        ).contents


# ---------
# From: advapi32.dll
# ---------
advapi32 = ctypes.WinDLL("advapi32")

# https://learn.microsoft.com/en-us/windows/win32/api/securitybaseapi/nf-securitybaseapi-adjusttokenprivileges
advapi32.AdjustTokenPrivileges.restype = BOOL
advapi32.AdjustTokenPrivileges.argtypes = [
    HANDLE,  # [in] TokenHandle
    BOOL,  # [in] DisableAllPrivileges
    POINTER(TOKEN_PRIVILEGES),  # [in, optional] NewState
    DWORD,  # [in] BufferLength
    POINTER(TOKEN_PRIVILEGES),  # [out, optional] PreviousState
    PDWORD,  # [out, optional] ReturnLength
]

# https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-logonuserw
advapi32.LogonUserW.restype = BOOL
advapi32.LogonUserW.argtypes = [
    LPCWSTR,  # [in] lpszUsername
    LPCWSTR,  # [in, optional] lpszDomain
    LPCWSTR,  # [in, optional] lpszPassword
    DWORD,  # [in] dwLogonType
    DWORD,  # [in] dwLogonProvider
    PHANDLE,  # [out] phToken
]

# https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-lookupprivilegevaluew
advapi32.LookupPrivilegeValueW.restype = BOOL
advapi32.LookupPrivilegeValueW.argtypes = [
    LPCWSTR,  # [in, optional] lpSystemName
    LPCWSTR,  # [in] lpName
    POINTER(LUID),  # [out] lpLuid
]

# https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocesstoken
advapi32.OpenProcessToken.restype = BOOL
advapi32.OpenProcessToken.argtypes = [
    HANDLE,  # [in] ProcessHandle,
    DWORD,  # [in] DesiredAccess
    PHANDLE,  # [out] TokenHandle
]

# exports:
AdjustTokenPrivileges = advapi32.AdjustTokenPrivileges
LogonUserW = advapi32.LogonUserW
LookupPrivilegeValueW = advapi32.LookupPrivilegeValueW
OpenProcessToken = advapi32.OpenProcessToken

# ---------
# From: kernel32.dll
# ---------
kernel32 = ctypes.WinDLL("kernel32")

# https://learn.microsoft.com/en-us/windows/win32/api/handleapi/nf-handleapi-closehandle
kernel32.CloseHandle.restype = BOOL
kernel32.CloseHandle.argtypes = [HANDLE]  # [in] hObject

# https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getcurrentprocess
kernel32.GetCurrentProcess.restype = HANDLE
kernel32.GetCurrentProcess.argtypes = []

# exports:
CloseHandle = kernel32.CloseHandle
GetCurrentProcess = kernel32.GetCurrentProcess


# ---------
# From: userenv.dll
# ---------
userenv = ctypes.WinDLL("userenv")

# https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-loaduserprofilew
userenv.LoadUserProfileW.restype = BOOL
userenv.LoadUserProfileW.argtypes = [
    HANDLE,  # [in] hToken
    POINTER(PROFILEINFO),  # [in, out] lpProfileInfo
]

# https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-unloaduserprofile
userenv.UnloadUserProfile.restype = BOOL
userenv.UnloadUserProfile.argtypes = [
    HANDLE,  # [in] hToken
    HANDLE,  # [in] hProfile
]

# exports:
LoadUserProfileW = userenv.LoadUserProfileW
UnloadUserProfile = userenv.UnloadUserProfile
