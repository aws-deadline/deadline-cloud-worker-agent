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
    PHANDLE,
    ULONG,
)
from ctypes import POINTER


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


# ---------
# From: advapi32.dll
# ---------
advapi32 = ctypes.WinDLL("advapi32")

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

# exports:
LogonUserW = advapi32.LogonUserW

# ---------
# From: kernel32.dll
# ---------
kernel32 = ctypes.WinDLL("kernel32")

# https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getcurrentprocess
kernel32.GetCurrentProcess.restype = HANDLE
kernel32.GetCurrentProcess.argtypes = []

# exports:
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
