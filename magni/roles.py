"""Worker role table for AFL++ campaigns.

One Role per worker kind. Previously this was spread over five structures
(WORKER_ROLES 4-tuple, ROLE_CORPUS, ROLE_TIMEOUT_MS, GRAMMAR_ROLES,
ROLE_BINARIES) that all had to be updated in lockstep; field names now make
each role self-describing.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from . import config
from .util import die


@dataclass(frozen=True)
class Role:
    """A campaign worker kind.

    binary:  which fuzz target to run ("cov", "asan", "laf", "exec").
    corpus:  seed pool (defaults to the parser corpus).
    timeout_ms: per-exec timeout (exec scripts need longer).
    afl_flags: extra afl-fuzz flags ("cmplog_bin" expands to the cmplog path).
    sched:    power-schedule flags.
    env:      extra worker environment.
    python_mutator: layer the SQL-aware custom mutator on top of havoc.
    """

    binary: str
    corpus: Path = config.CORPUS_DIR
    timeout_ms: str = config.DEFAULT_TIMEOUT_MS
    afl_flags: tuple = ()
    sched: tuple = ()
    env: dict = field(default_factory=dict)
    python_mutator: bool = False

    def target(self) -> Path:
        return TARGETS[self.binary]

    def worker_env(self) -> dict[str, str]:
        """Role env plus grammar-mutator injection (PYTHONPATH is resolved
        fresh here so it can never go stale)."""
        e = dict(self.env)
        if self.python_mutator:
            if not config.GRAMMAR_MUTATOR.is_file():
                die(f"Error: grammar mutator missing: {config.GRAMMAR_MUTATOR}")
            e["AFL_PYTHON_MODULE"] = "grammar_mutator"
            base = os.environ.get("PYTHONPATH", "")
            e["PYTHONPATH"] = str(config.FUZZ_DIR) + (os.pathsep + base if base else "")
        return e


TARGETS = {
    "cov": config.FUZZ_TARGET_COV,
    "asan": config.FUZZ_TARGET,
    "laf": config.FUZZ_TARGET_LAF,
    "exec": config.FUZZ_EXEC_TARGET,
}

EXEC_ENV = {"ASAN_OPTIONS": config.ASAN_FUZZ_OPTIONS}

ROLES: dict[str, Role] = {
    "master": Role("cov", sched=("-p", "exploit")),
    "explore": Role("cov", sched=("-p", "explore")),
    "fast": Role("cov", sched=("-p", "fast")),
    "coe": Role("cov", sched=("-p", "coe")),
    "seek": Role("cov", sched=("-p", "seek")),
    "cmplog": Role("cov", afl_flags=("-c", "cmplog_bin", "-l", "2AT")),
    "asan": Role("asan", env=dict(EXEC_ENV)),
    "laf": Role("laf"),
    "mopt": Role("cov", afl_flags=("-L", "0")),
    "oldq": Role("cov", afl_flags=("-Z",)),
    "exec": Role(
        "exec", corpus=config.EXEC_CORPUS_DIR, timeout_ms=config.EXEC_TIMEOUT_MS, env=dict(EXEC_ENV)
    ),
    "grammar": Role("cov", sched=("-p", "explore"), python_mutator=True),
    "exec_grammar": Role(
        "exec",
        corpus=config.EXEC_CORPUS_DIR,
        timeout_ms=config.EXEC_TIMEOUT_MS,
        env=dict(EXEC_ENV),
        python_mutator=True,
    ),
}

# Default secondary rotation when roles aren't specified.
DEFAULT_SECONDARIES = ["cmplog", "asan", "fast", "explore", "coe", "laf", "mopt"]


def default_roles(workers: int) -> list[str]:
    """master + rotation through DEFAULT_SECONDARIES."""
    roles = ["master"]
    for i in range(1, workers):
        roles.append(DEFAULT_SECONDARIES[(i - 1) % len(DEFAULT_SECONDARIES)])
    return roles


def check_roles(roles: list[str]) -> None:
    unknown = [r for r in roles if r not in ROLES]
    if unknown:
        die(f"Error: unknown worker roles: {unknown} (see magni.py help)")


def role_names() -> str:
    """Comma-separated role list for help text (single-sourced)."""
    return ",".join(ROLES)
