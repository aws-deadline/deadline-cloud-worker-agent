# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Resolution of system command names to absolute paths, without consulting PATH.

Invoking a privileged helper by bare name (``sudo``, ``shutdown``, ``pkill``)
resolves it through ``PATH``. Where any part of that search path is influenced by
less-trusted input, the resolution itself becomes the vulnerability: CWE-426,
Untrusted Search Path. This module removes ``PATH`` from the picture by scanning a
fixed list of trusted absolute directories instead.

Three properties are load-bearing, and each is pinned by a test in
``test/unit/test_system_commands.py``:

* **``PATH`` is never read.** Not directly, and not indirectly via
  :func:`shutil.which`, which resolves through ``PATH`` and so would reintroduce
  the problem while appearing to fix it.
* **Only paths under the trusted directories are returned**, and a name
  containing a path separator is rejected -- otherwise joining ``/usr/bin`` with
  ``../../tmp/evil`` would make this module the injection point it exists to
  remove.
* **A missing command raises.** Falling back to the bare name would restore the
  vulnerability while looking fixed, which is the worst available failure mode
  for this class of fix.

Why a resolver rather than absolute-path literals: the locations are not
universal. NixOS keeps the setuid ``sudo`` wrapper at ``/run/wrappers/bin/sudo``,
and on non-usr-merged Debian ``shutdown`` exists only at ``/sbin/shutdown``. A
hardcoded literal turns a security bug into an availability bug on those hosts.
"""

from __future__ import annotations

import os
import sys
from typing import Optional, Tuple

__all__ = [
    "SystemCommandNotFoundError",
    "find_system_command",
    "system_command_path",
    "trusted_directories",
]


_POSIX_TRUSTED_DIRECTORIES: Tuple[str, ...] = (
    # Ordered, deliberately. On NixOS the setuid `sudo` wrapper lives here and
    # the /usr/bin copy is absent or not setuid, so this must be searched first.
    "/run/wrappers/bin",
    # ...and these two NixOS entries are a pair. /run/wrappers/bin holds only the
    # setuid/setcap wrappers, so on NixOS it resolves `sudo` and nothing else:
    # /usr/bin holds just `env`, /bin just `sh`, and the sbin directories are
    # absent. `pkill` lives in this symlink farm, which nixos-rebuild manages and
    # root owns, so it is trust-equivalent to /usr/bin there. Without it the
    # ordering above would resolve `sudo` and then fail on `pkill`.
    "/run/current-system/sw/bin",
    "/usr/bin",
    "/bin",
    # sbin last. Note this list is no longer used to locate `shutdown` -- that path
    # is a sudoers contract, see entrypoint.LINUX_SHUTDOWN_PATH -- but other
    # commands can still live only under /sbin on non-usr-merged distributions.
    "/usr/sbin",
    "/sbin",
)

_WINDOWS_FALLBACK_SYSTEM_ROOT = r"C:\Windows"


class SystemCommandNotFoundError(FileNotFoundError):
    """A required system command was not present in any trusted directory.

    A :class:`FileNotFoundError`, and therefore an :class:`OSError`, on purpose.

    An earlier revision made this a plain ``Exception``, reasoning that an
    unavailable privileged helper must not be absorbed by handlers that catch
    ``FileNotFoundError`` to mean "carry on degraded". That reasoning assumed rather
    than checked what surrounding code does with it, and the semantics this
    condition has are ``FileNotFoundError``'s: the thing we meant to launch is not
    there. ``capabilities.py`` and ``metrics.py`` already treat that as "feature
    unavailable" for ``nvidia-smi``, which is the right shape here too.

    Remaining a distinct type still lets a caller tell "not in any trusted
    directory" apart from "``exec`` failed", and the message says which.
    """


def trusted_directories() -> Tuple[str, ...]:
    """The absolute directories searched for system commands, in order."""
    if sys.platform == "win32":
        # SystemRoot is set by the operating system for every process. It is not
        # job-controlled the way a session subprocess's environment is, and an
        # attacker able to set it in the agent's own environment already has
        # agent-level control, so reading it does not weaken the boundary this
        # module defends.
        system_root = os.environ.get("SystemRoot") or _WINDOWS_FALLBACK_SYSTEM_ROOT
        return (os.path.join(system_root, "System32"),)
    return _POSIX_TRUSTED_DIRECTORIES


def _validate_command_name(name: str) -> None:
    """Reject anything that is not a bare command name."""
    if not name:
        raise ValueError("A system command name must not be empty.")
    if name in (os.curdir, os.pardir):
        raise ValueError(f"{name!r} is not a system command name.")
    # Both separators are checked on both platforms. A backslash is a legal POSIX
    # filename character, but no command resolved here contains one, and treating
    # it as suspect keeps the check identical rather than subtly weaker on POSIX.
    # The colon is rejected too, and on this module it is not hypothetical -- this
    # is the one resolver here with a Windows branch:
    #
    #     ntpath.join(r"C:\Windows\System32", "D:evil") == "D:evil"
    #
    # A drive-relative name discards the trusted prefix entirely while containing no
    # separator at all, resolving against that drive's own current directory. A
    # separator-only guard lets it straight through, which would make this module
    # the injection point it exists to remove.
    if "/" in name or "\\" in name or ":" in name:
        raise ValueError(
            f"A system command name must not contain a path separator or drive "
            f"specifier, but got {name!r}."
        )


def _is_executable_file(path: str) -> bool:
    return os.path.isfile(path) and os.access(path, os.X_OK)


def find_system_command(name: str) -> Optional[str]:
    """Return the absolute path to ``name``, or ``None`` if it is not installed.

    ``PATH`` is not consulted. Use this when the command's absence is tolerable;
    use :func:`system_command_path` when it is required.

    Raises:
        ValueError: if ``name`` is not a bare command name.
    """
    _validate_command_name(name)
    for directory in trusted_directories():
        candidate = os.path.join(directory, name)
        if _is_executable_file(candidate):
            return candidate
    return None


def system_command_path(name: str) -> str:
    """Return the absolute path to ``name``.

    Raises:
        ValueError: if ``name`` is not a bare command name.
        SystemCommandNotFoundError: if ``name`` is in no trusted directory.
    """
    path = find_system_command(name)
    if path is None:
        raise SystemCommandNotFoundError(
            f"Could not find the system command {name!r} in any trusted directory "
            f"({', '.join(trusted_directories())}). PATH is deliberately not searched."
        )
    return path
