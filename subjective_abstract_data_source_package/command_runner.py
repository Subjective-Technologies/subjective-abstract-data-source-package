"""Where a datasource's child process runs — here, or on another machine.

A CLI datasource is two things wound together: knowing how to *invoke* an agent CLI
and parse what it says, and knowing how to *run a process*. The first is the whole
value of the datasource and is specific to Claude or Codex or Grok. The second is
`subprocess`, is identical in all of them, and is the only part that has to change
for the CLI to run somewhere else.

So it is separated out. A datasource asks its runner to start a process, look for a
binary, read a file; the runner decides whether that happens locally or over SSH. A
datasource that has been converted needs no knowledge of SSH at all, and the same
code answers a turn whether the agent is installed on this box or on a machine
across the world.

Only the interface and the local implementation live here, in the package every
datasource already depends on. The SSH implementation lives with the SSH
datasource, where the host-key handling, the askpass machinery and the connection
multiplexing already are — so nothing in this package, and nothing in a CLI
datasource, has to depend on any of that.

The surface is deliberately small: it is exactly what the CLI datasources were
found to do to their environment, and nothing else.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from abc import ABC, abstractmethod


class CommandRunner(ABC):
    """How and where a datasource runs things."""

    #: True when the process runs on another machine. Datasources use this to skip
    #: work that only makes sense locally, not to decide *how* to do anything.
    is_remote: bool = False

    #: Shown to the operator when a turn fails, so "command not found" says which
    #: machine it was not found on.
    location: str = "this machine"

    @abstractmethod
    def popen(self, argv: list[str], *, env: dict | None = None, cwd: str = "") -> subprocess.Popen:
        """Start a process and return it. Must be killable as a group."""

    @abstractmethod
    def run(
        self, argv: list[str], *, timeout: float, env: dict | None = None, cwd: str = ""
    ) -> subprocess.CompletedProcess:
        """Run to completion and capture the output."""

    @abstractmethod
    def which(self, name: str, candidates: tuple[str, ...] = ()) -> str | None:
        """Find an executable, trying `candidates` as absolute paths first.

        Agent CLIs install in places no PATH guarantees — nvm, ~/.local/bin,
        ~/.npm-global — so every datasource carries a list of the places to look.
        """

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """Whether a path exists as a file. Used to check for credential files."""

    @abstractmethod
    def upload(self, local_path: str) -> str:
        """Make a local file available to the process, returning the path to use.

        Attachments live on the machine running the service; a remote CLI cannot
        open them. Locally this is the identity function.
        """

    def cleanup(self) -> None:
        """Release anything the runner holds. Safe to call more than once."""


class LocalRunner(CommandRunner):
    """The behaviour every CLI datasource had before runners existed."""

    is_remote = False
    location = "this machine"

    def popen(self, argv: list[str], *, env: dict | None = None, cwd: str = "") -> subprocess.Popen:
        return subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=cwd or None,
            # Its own process group, so a cancel can take the CLI and everything it
            # spawned down together rather than orphaning them.
            start_new_session=True,
        )

    def run(
        self, argv: list[str], *, timeout: float, env: dict | None = None, cwd: str = ""
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, env=env, cwd=cwd or None
        )

    def which(self, name: str, candidates: tuple[str, ...] = ()) -> str | None:
        for candidate in candidates:
            expanded = os.path.expanduser(candidate)
            if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
                return expanded
        return shutil.which(name)

    def is_file(self, path: str) -> bool:
        return os.path.isfile(os.path.expanduser(path))

    def upload(self, local_path: str) -> str:
        return local_path


def runner_from(connection: dict | None) -> CommandRunner:
    """The runner a connection asks for.

    A connection may carry a ready-made runner under `command_runner` — that is how
    the service injects an SSH one without any datasource importing SSH code. With
    nothing specified, everything runs here, exactly as it always did.
    """
    supplied = (connection or {}).get("command_runner")
    if isinstance(supplied, CommandRunner):
        return supplied
    return LocalRunner()
