"""Promote AFL++ worker-queue finds into the exec seed corpus.

Thin wrappers over the shared pool flow in magni.promote (EXEC_POOL):
the exec target binary + 5000 ms timeout for minimization, the
EXEC_SEEDS/EXEC_PROMOTED tables as diff basis, and
corpus_exec/promoted_seeds.py as the append target. The promoted_NNNN
counter is scoped to this pool (independent from the parser sequence).
"""

from pathlib import Path

from . import config
from .corpus import parse_seed_tables
from .promote import (
    append_to_promoted_pool,
    corpus_promote_pool,
    load_existing_pool,
    minimize_merged_pool,
)

GEN_EXEC_CORPUS = config.GEN_EXEC_CORPUS
EXEC_PROMOTED = config.EXEC_PROMOTED


def parse_exec_tables() -> dict[str, tuple[str, bytes]]:
    """Load EXEC_SEEDS + EXEC_PROMOTED via exec. Returns {md5: (name, bytes)}."""
    return parse_seed_tables(GEN_EXEC_CORPUS, *config.EXEC_POOL.seed_tables)


def minimize_merged_exec(seen: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """Reduce merged queue items with afl-cmin against the exec target."""
    return minimize_merged_pool(config.EXEC_POOL, seen)


def load_existing_exec() -> dict[str, tuple[str, bytes]]:
    """Existing scripts: tables plus on-disk files. Returns {md5: (name, bytes)}."""
    return load_existing_pool(config.EXEC_POOL)


def append_to_promoted(
    new_seeds: list, existing_names: set[str], promoted_path: Path = EXEC_PROMOTED
) -> None:
    """Append new (content, metadata) scripts as promoted_NNNN entries."""
    append_to_promoted_pool(config.EXEC_POOL, new_seeds, existing_names, promoted_path)


def corpus_promote_exec(minimize: bool, dry_run: bool, test_after: bool = False) -> None:
    """Merge exec worker queues, dedup, diff, and append new scripts."""
    corpus_promote_pool(config.EXEC_POOL, minimize, dry_run, test_after)
