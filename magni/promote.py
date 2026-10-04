"""Promote AFL++ worker-queue finds into the parser seed corpus."""

import tempfile
from pathlib import Path

from . import config
from .build import ensure_cov_target, ensure_exec_target
from .cmin import afl_cmin
from .corpus import (
    corpus_generate,
    corpus_test,
    md5,
    parse_afl_metadata,
    parse_seed_tables,
)
from .fuzz import fuzz_out_dir
from .util import die, log


def merge_queues(out: Path) -> dict[str, tuple[bytes, str]]:
    """Dedup all worker queues under out by content. Returns {md5: (bytes, meta)}."""
    seen: dict[str, tuple[bytes, str]] = {}
    for queue_dir in sorted(out.glob("*/queue")):
        for f in sorted(queue_dir.iterdir()):
            if f.is_file() and not f.name.endswith(".py"):
                content = f.read_bytes()
                h = md5(content)
                if h not in seen:
                    seen[h] = (content, parse_afl_metadata(f.name))
    return seen


def ensure_for_pool(pool: config.CorpusPool) -> None:
    """Build the cmin target for a pool (cached by freshness checks)."""
    if pool.is_exec:
        ensure_exec_target()
    else:
        ensure_cov_target()


def minimize_merged_pool(
    pool: config.CorpusPool, seen: dict[str, tuple[bytes, str]]
) -> dict[str, tuple[bytes, str]]:
    """Reduce merged queue items with afl-cmin against the pool's target."""
    ensure_for_pool(pool)
    with tempfile.TemporaryDirectory(prefix="magni_merge_") as tmpdir:
        merge_path = Path(tmpdir) / "merged"
        merge_path.mkdir()
        for h, (content, _) in seen.items():
            (merge_path / h).write_bytes(content)
        min_path = Path(tmpdir) / "minimized"
        kept = afl_cmin(
            merge_path, min_path, timeout_ms=pool.cmin_timeout_ms, target=pool.cmin_target
        )
        log(f"  {len(kept)} seeds after minimization")
        out = {}
        for f in sorted(min_path.iterdir()):
            content = f.read_bytes()
            out[md5(content)] = (content, parse_afl_metadata(f.name))
        return out


def minimize_merged(seen: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """Reduce merged queue items with afl-cmin. Returns {md5: (bytes, meta)}."""
    return minimize_merged_pool(config.PARSER_POOL, seen)


def load_existing_pool(pool: config.CorpusPool) -> dict[str, tuple[str, bytes]]:
    """Existing entries: gen-script tables plus on-disk files. Returns {md5: (name, bytes)}.

    The file scan uses is_staged_seed for both pools. (The parser scan
    previously also picked up promoted_seeds.py itself as a phantom entry;
    its md5 can never equal a real seed, so the diff is unaffected.)
    """
    existing = parse_seed_tables(pool.gen_script, *pool.seed_tables)
    for f in pool.corpus_dir.iterdir():
        if config.is_staged_seed(f):
            content = f.read_bytes()
            h = md5(content)
            if h not in existing:
                existing[h] = (f.name, content)
    return existing


def load_existing() -> dict[str, tuple[str, bytes]]:
    """Existing seeds: SEEDS table plus on-disk files. Returns {md5: (name, bytes)}."""
    return load_existing_pool(config.PARSER_POOL)


def next_promoted_entries(new_seeds: list, existing_names: set[str]) -> list[str]:
    """Render new (content, metadata) seeds as promoted_NNNN table entries.

    The counter is scoped to the pool (parser and exec sequences are
    independent); collisions with existing names are skipped, never reused.
    """
    max_index = 0
    for name in existing_names:
        if name.startswith("promoted_"):
            try:
                max_index = max(max_index, int(name.split("_")[1]))
            except IndexError, ValueError:
                pass
    entries = []
    for content, metadata in new_seeds:
        max_index += 1
        name = f"promoted_{max_index:04d}"
        while name in existing_names:
            max_index += 1
            name = f"promoted_{max_index:04d}"
        existing_names.add(name)
        comment = f"  # {metadata}" if metadata else ""
        entries.append(f'    ("{name}", {content!r}),{comment}')
    return entries


def append_to_gen_corpus(new_seeds: list, existing_names: set[str]) -> None:
    """Append new (content, metadata) seeds as promoted_NNNN entries."""
    new_entries = next_promoted_entries(new_seeds, existing_names)

    text = config.GEN_CORPUS.read_text()
    lines = text.split("\n")
    insert_pos = len(lines) - 1
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped.startswith("(") and stripped.endswith(("),", "),")):
            insert_pos = i + 1
            break
    for entry in reversed(new_entries):
        lines.insert(insert_pos, entry)
    config.GEN_CORPUS.write_text("\n".join(lines))


def corpus_promote_pool(
    pool: config.CorpusPool, minimize: bool, dry_run: bool, test_after: bool = False
) -> None:
    """Shared 5-step promote flow: merge queues, optionally minimize, diff
    against the pool's tables + on-disk files, and append new entries."""
    out = fuzz_out_dir()
    if not out.exists():
        die(f"Error: {out} not found")

    log("[1/5] Merging worker queues...")
    seen = merge_queues(out)
    log(f"  {len(seen)} unique items across workers")
    if not seen:
        log("No queue items found. Nothing to promote.")
        return

    if minimize:
        log("[2/5] Minimizing with afl-cmin...")
        seen = minimize_merged_pool(pool, seen)
    else:
        log("[2/5] Skipping minimization (use --minimize to enable)")

    log(f"[3/5] Loading existing {pool.label}s...")
    existing = load_existing_pool(pool)
    log(f"  {len(existing)} existing {pool.label}s (tables + on-disk)")

    log("[4/5] Diffing against existing corpus...")
    new_seeds = [(content, meta) for h, (content, meta) in seen.items() if h not in existing]
    if not new_seeds:
        log("  No new seeds found. Corpus is up to date.")
        return
    log(f"  {len(new_seeds)} new {pool.label}s to promote")

    if dry_run:
        log(f"\n[DRY RUN] Would add {len(new_seeds)} {pool.label}s to {pool.promoted_path}")
        return

    log(f"[5/5] Appending {len(new_seeds)} {pool.label}s to {pool.promoted_path}...")
    if pool.is_exec:
        append_to_promoted_pool(pool, new_seeds, {name for name, _ in existing.values()})
    else:
        append_to_gen_corpus(new_seeds, {name for name, _ in existing.values()})
    log(f"  Done. Total {pool.label}s: {len(existing) + len(new_seeds)}")

    if test_after:
        corpus_generate(exec_scripts=pool.is_exec)
        corpus_test(exec_scripts=pool.is_exec)


def append_to_promoted_pool(
    pool: config.CorpusPool,
    new_seeds: list,
    existing_names: set[str],
    promoted_path: Path | None = None,
) -> None:
    """Append new entries to a trailing-list promoted file (exec pool layout).

    Insert position is the close of the trailing [...] block — unlike the
    parser GEN_CORPUS, where entries land after the last tuple mid-file.
    promoted_path defaults to the pool's file (tests inject a temp file).
    """
    new_entries = next_promoted_entries(new_seeds, existing_names)
    target = promoted_path or pool.promoted_path
    text = target.read_text(encoding="utf-8")
    lines = text.split("\n")
    insert_pos = len(lines) - 1
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip() == "]":
            insert_pos = i
            break
    for entry in reversed(new_entries):
        lines.insert(insert_pos, entry)
    target.write_text("\n".join(lines), encoding="utf-8")


def corpus_promote(minimize: bool, dry_run: bool, test_after: bool = False) -> None:
    """Merge AFL++ worker queues, dedup, diff, and append new seeds to gen_corpus.py."""
    corpus_promote_pool(config.PARSER_POOL, minimize, dry_run, test_after)
