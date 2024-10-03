# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""This module contains code for interacting with Linux capabilities.

See https://man7.org/linux/man-pages/man7/capabilities.7.html for details on this Linux kernel
feature.
"""

import ctypes
import os
import sys
from logging import getLogger
from ctypes.util import find_library
from functools import cache
from typing import Any, Optional, Tuple, TYPE_CHECKING


logger = getLogger(__name__)


# Capability sets
# See https://ddnet.org/codebrowser/include/sys/capability.h.html#cap_flag_t
CAP_EFFECTIVE = 0
CAP_PERMITTED = 1
CAP_INHERITABLE = 2

# Capability bit numbers
# See https://github.com/torvalds/linux/blob/28eb75e178d389d325f1666e422bc13bbbb9804c/include/uapi/linux/capability.h#L147
CAP_KILL = 5

# Values for cap_flag_value_t arguments
# See https://ddnet.org/codebrowser/include/sys/capability.h.html#cap_flag_value_t
CAP_CLEAR = 0
CAP_SET = 1

cap_flag_t = ctypes.c_int
cap_flag_value_t = ctypes.c_int
cap_value_t = ctypes.c_int


class UserCapHeader(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_uint32),
        ("pid", ctypes.c_int),
    ]


class UserCapData(ctypes.Structure):
    _fields_ = [
        ("effective", ctypes.c_uint32),
        ("permitted", ctypes.c_uint32),
        ("inheritable", ctypes.c_uint32),
    ]


class Cap(ctypes.Structure):
    _fields_ = [
        ("head", UserCapHeader),
        ("data", UserCapData),
    ]


if TYPE_CHECKING:
    cap_t = ctypes._Pointer[Cap]
    cap_flag_value_ptr = ctypes._Pointer[cap_flag_value_t]
    cap_value_ptr = ctypes._Pointer[cap_value_t]
    ssize_ptr_t = ctypes._Pointer[ctypes.c_ssize_t]
else:
    cap_t = ctypes.POINTER(Cap)
    cap_flag_value_ptr = ctypes.POINTER(cap_flag_value_t)
    cap_value_ptr = ctypes.POINTER(cap_value_t)
    ssize_ptr_t = ctypes.POINTER(ctypes.c_ssize_t)


def _cap_set_proc_err_check(
    result: ctypes.c_int,
    func: Any,
    args: Tuple[Any, ...],
) -> ctypes.c_int:  # pragma: nocover
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return result


def _cap_get_proc_err_check(
    result: cap_t,
    func: Any,
    args: Tuple[cap_t, cap_flag_t, ctypes.c_int, cap_value_ptr, cap_flag_value_t],
) -> cap_t:  # pragma: nocover
    if not result:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return result


def _cap_to_text_errcheck(
    result: ctypes.c_char_p,
    func: Any,
    args: Tuple[cap_t, ssize_ptr_t],
) -> ctypes.c_char_p:  # pragma: nocover
    if not result:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return result


def _cap_get_flag_errcheck(
    result: ctypes.c_int, func: Any, args: Tuple[cap_t, cap_value_t, cap_flag_t, cap_flag_value_ptr]
) -> ctypes.c_int:  # pragma: nocover
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return result


@cache
def _get_libcap() -> Optional[ctypes.CDLL]:  # pragma: nocover
    if not sys.platform.startswith("linux"):
        raise OSError(f"libcap is only available on Linux, but found platform: {sys.platform}")

    libcap_path = find_library("cap")
    if not libcap_path:
        return None

    libcap = ctypes.CDLL(libcap_path, use_errno=True)

    # https://man7.org/linux/man-pages/man3/cap_set_proc.3.html
    libcap.cap_set_proc.restype = ctypes.c_int
    libcap.cap_set_proc.argtypes = [
        ctypes.POINTER(Cap),
    ]
    libcap.cap_set_proc.errcheck = _cap_set_proc_err_check  # type: ignore

    # https://man7.org/linux/man-pages/man3/cap_get_proc.3.html
    libcap.cap_get_proc.restype = cap_t
    libcap.cap_get_proc.argtypes = []
    libcap.cap_get_proc.errcheck = _cap_get_proc_err_check  # type: ignore

    # https://man7.org/linux/man-pages/man3/cap_set_flag.3.html
    libcap.cap_set_flag.restype = ctypes.c_int
    libcap.cap_set_flag.argtypes = [
        cap_t,
        cap_flag_t,
        ctypes.c_int,
        cap_value_ptr,
        cap_flag_value_t,
    ]

    # https://man7.org/linux/man-pages/man3/cap_get_flag.3.html
    libcap.cap_get_flag.restype = ctypes.c_int
    libcap.cap_get_flag.argtypes = (
        cap_t,
        cap_value_t,
        cap_flag_t,
        cap_flag_value_ptr,
    )
    libcap.cap_get_flag.errcheck = _cap_get_flag_errcheck  # type: ignore

    # https://man7.org/linux/man-pages/man3/cap_to_text.3.html
    libcap.cap_to_text.restype = ctypes.c_char_p
    libcap.cap_to_text.argtypes = [
        cap_t,
        ssize_ptr_t,
    ]
    libcap.cap_to_text.errcheck = _cap_to_text_errcheck  # type: ignore

    return libcap


def _get_caps_str(
    *,
    libcap: ctypes.CDLL,
    caps: cap_t,
) -> str:
    cap_text = libcap.cap_to_text(caps, None).decode()
    return cap_text


def _has_cap_kill_inheritable(
    *,
    libcap: ctypes.CDLL,
    caps: cap_t,
) -> bool:
    flag_value = cap_flag_value_t()
    libcap.cap_get_flag(caps, CAP_KILL, CAP_INHERITABLE, ctypes.byref(flag_value))
    return flag_value.value == CAP_SET


def drop_kill_cap_from_inheritable() -> None:
    if not sys.platform.startswith("linux"):
        return
    libcap = _get_libcap()
    if not libcap:
        logger.warning(
            "Unable to locate libcap. The worker agent will run without Linux capability awareness."
        )
        return

    caps = libcap.cap_get_proc()
    caps_str = _get_caps_str(libcap=libcap, caps=caps)
    if _has_cap_kill_inheritable(libcap=libcap, caps=caps):
        logger.info(
            "CAP_KILL was found in the thread's inheritable capability set (%s). Dropping CAP_KILL from the thread's inheritable capability set",
            caps_str,
        )
        cap_value_arr_t = cap_value_t * 1
        cap_value_arr = cap_value_arr_t()
        cap_value_arr[0] = CAP_KILL
        libcap.cap_set_flag(
            caps,
            CAP_INHERITABLE,
            len(cap_value_arr),
            cap_value_arr,
            CAP_CLEAR,
        )
        libcap.cap_set_proc(caps)
        caps_str_after = _get_caps_str(libcap=libcap, caps=caps)
        logger.info("Capabilites are: %s", caps_str_after)
    else:
        logger.info(
            "CAP_KILL was not found in the thread's inheritable capability set (%s)", caps_str
        )


def main() -> None:
    libcap = _get_libcap()
    if not libcap:
        print("ERROR: libcap not found")
        sys.exit(1)
    caps = libcap.cap_get_proc()
    print(_get_caps_str(libcap=libcap, caps=caps))


if __name__ == "__main__":
    main()
