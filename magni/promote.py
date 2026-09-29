"""Promote AFL++ worker-queue finds into the parser seed corpus."""

import tempfile
from pathlib import Path

from . import config
from .build import ensure_cov_target
from .cmin import afl_cmin
from .corpus import corpus_generate, corpus_test, md5, parse_afl_metadata, parse_seeds
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


def minimize_merged(seen: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """Reduce merged queue items with afl-cmin. Returns {md5: (bytes, meta)}."""
    ensure_cov_target()
    with tempfile.TemporaryDirectory(prefix="magni_merge_") as tmpdir:
        merge_path = Path(tmpdir) / "merged"
        merge_path.mkdir()
        for h, (content, _) in seen.items():
            (merge_path / h).write_bytes(content)
        min_path = Path(tmpdir) / "minimized"
        kept = afl_cmin(merge_path, min_path)
        log(f"  {len(kept)} seeds after minimization")
        minimized = sorted(min_path.iterdir())
        out = {}
        for f in minimized:
            content = f.read_bytes()
            out[md5(content)] = (content, parse_afl_metadata(f.name))
        return out


def load_existing() -> dict[str, tuple[str, bytes]]:
    """Existing seeds: SEEDS table plus on-disk files. Returns {md5: (name, bytes)}."""
    existing = parse_seeds()
    for f in config.CORPUS_DIR.iterdir():
        if f.is_file() and f.name != "gen_corpus.py" and not f.name.startswith("__"):
            h = md5(f.read_bytes())
            if h not in existing:
                existing[h] = (f.name, f.read_bytes())
    return existing


def append_to_gen_corpus(new_seeds: list, existing_names: set[str]) -> None:
    """Append new (content, metadata) seeds as promoted_NNNN entries."""
    max_index = 0
    for name in existing_names:
        if name.startswith("promoted_"):
            try:
                max_index = max(max_index, int(name.split("_")[1]))
            except (IndexError, ValueError):
                pass
    new_entries = []
    for content, metadata in new_seeds:
        max_index += 1
        name = f"promoted_{max_index:04d}"
        while name in existing_names:
            max_index += 1
            name = f"promoted_{max_index:04d}"
        existing_names.add(name)
        comment = f"  # {metadata}" if metadata else ""
        new_entries.append(f'    ("{name}", {content!r}),{comment}')

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


def corpus_promote(minimize: bool, dry_run: bool, test_after: bool = False) -> None:
    """Merge AFL++ worker queues, dedup, diff, and append new seeds to gen_corpus.py."""
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
        seen = minimize_merged(seen)
    else:
        log("[2/5] Skipping minimization (use --minimize to enable)")

    log("[3/5] Loading existing seeds...")
    existing = load_existing()
    log(f"  {len(existing)} existing seeds (SEEDS + on-disk)")

    log("[4/5] Diffing against existing corpus...")
    new_seeds = [(content, meta) for h, (content, meta) in seen.items()
                 if h not in existing]
    if not new_seeds:
        log("  No new seeds found. Corpus is up to date.")
        return
    log(f"  {len(new_seeds)} new seeds to promote")

    if dry_run:
        log(f"\n[DRY RUN] Would add {len(new_seeds)} seeds to {config.GEN_CORPUS}")
        return

    log(f"[5/5] Appending {len(new_seeds)} seeds to {config.GEN_CORPUS}...")
    append_to_gen_corpus(new_seeds, {name for name, _ in existing.values()})
    log(f"  Done. Total seeds: {len(existing) + len(new_seeds)}")

    if test_after:
        corpus_generate()
        corpus_test()
