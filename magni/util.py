"""Process helpers: logging, fatal errors, command execution, tool checks."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import config


def log(msg: str) -> None:
    print(msg, flush=True)


def die(msg: str, code: int = 1) -> sys.NoReturn:
    """Log an error and exit. Replaces the log(...); sys.exit(1) pattern."""
    log(msg)
    sys.exit(code)


def run(
    cmd: list[str], check: bool = True, env: dict | None = None, cwd: Path | None = None
) -> subprocess.CompletedProcess:
    """Run a command with logging. Raises on failure if check=True."""
    log(f"  $ {' '.join(str(c) for c in cmd)}")
    e = os.environ.copy()
    if env:
        e.update(env)
    result = subprocess.run(cmd, cwd=str(cwd or config.ROOT), env=e)  # noqa: PLW1510 - check=False callers inspect returncode themselves
    if check and result.returncode != 0:
        sys.exit(result.returncode)
    return result


def require_tool(name: str) -> None:
    """Exit with a clear error if a tool is missing from PATH."""
    if shutil.which(name) is None:
        die(f"Error: '{name}' not found on PATH")


def clean_extra(extra: list[str] | None) -> list[str]:
    """Strip argparse REMAINDER artifacts: a leading `--` separator (kept
    literally by nargs=REMAINDER) and Make KEY=VALUE leaks."""
    if not extra:
        return []
    if extra[0] == "--":
        extra = extra[1:]
    return [a for a in extra if "=" not in a]
