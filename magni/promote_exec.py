"""Promote AFL++ worker-queue finds into the exec seed corpus.

Mirrors magni.promote (parser flow) with three deltas: the exec target
binary + 5000 ms timeout for minimization, the EXEC_SEEDS/EXEC_PROMOTED
tables as diff basis, and corpus_exec/promoted_seeds.py as the append
target. The promoted_NNNN counter is scoped to this pool (independent
from the parser sequence).
"""

import tempfile
from pathlib import Path

from . import config
from .build import ensure_exec_target
from .cmin import afl_cmin
from .corpus import corpus_generate, corpus_test, md5, parse_afl_metadata
from .fuzz import fuzz_out_dir
from .promote import merge_queues
from .util import die, log

GEN_EXEC_CORPUS = config.GEN_EXEC_CORPUS
EXEC_PROMOTED = config.EXEC_CORPUS_DIR / "promoted_seeds.py"


def parse_exec_tables() -> dict[str, tuple[str, bytes]]:
    """Load EXEC_SEEDS + EXEC_PROMOTED via exec. Returns {md5: (name, bytes)}."""
    ns = {"__name__": "gen_exec_corpus_parsing", "__file__": str(GEN_EXEC_CORPUS)}
    try:
        exec(compile(GEN_EXEC_CORPUS.read_text(), str(GEN_EXEC_CORPUS), "exec"), ns)
    except SystemExit:
        pass  # gen script calls sys.exit(main()) — ignore it
    except Exception:
        pass
    seeds = {}
    for table in ("EXEC_SEEDS", "EXEC_PROMOTED"):
        for entry in ns.get(table, []):
            if not isinstance(entry, (tuple, list)) or len(entry) != 2:
                continue
            name, content = entry
            if isinstance(content, str):
                content = content.encode("utf-8")
            if isinstance(content, bytes):
                seeds[md5(content)] = (name, content)
    return seeds


def minimize_merged_exec(seen: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """Reduce merged queue items with afl-cmin against the exec target."""
    ensure_exec_target()
    with tempfile.TemporaryDirectory(prefix="magni_exec_merge_") as tmpdir:
        merge_path = Path(tmpdir) / "merged"
        merge_path.mkdir()
        for h, (content, _) in seen.items():
            (merge_path / h).write_bytes(content)
        min_path = Path(tmpdir) / "minimized"
        kept = afl_cmin(merge_path, min_path, timeout_ms=config.EXEC_TIMEOUT_MS,
                        target=config.FUZZ_EXEC_TARGET)
        log(f"  {len(kept)} seeds after minimization")
        out = {}
        for f in sorted(min_path.iterdir()):
            content = f.read_bytes()
            out[md5(content)] = (content, parse_afl_metadata(f.name))
        return out


def load_existing_exec() -> dict[str, tuple[str, bytes]]:
    """Existing scripts: tables plus on-disk files. Returns {md5: (name, bytes)}."""
    existing = parse_exec_tables()
    for f in config.EXEC_CORPUS_DIR.iterdir():
        if f.is_file() and f.suffix != ".py" and f.name != "__pycache__":
            content = f.read_bytes()
            h = md5(content)
            if h not in existing:
                existing[h] = (f.name, content)
    return existing


def append_to_promoted(new_seeds: list, existing_names: set[str],
                       promoted_path: Path = EXEC_PROMOTED) -> None:
    """Append new (content, metadata) scripts as promoted_NNNN entries."""
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

    text = promoted_path.read_text(encoding="utf-8")
    lines = text.split("\n")
    # The promoted list is the trailing [...] block; insert before its close.
    insert_pos = len(lines) - 1
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip() == "]":
            insert_pos = i
            break
    for entry in reversed(new_entries):
        lines.insert(insert_pos, entry)
    promoted_path.write_text("\n".join(lines), encoding="utf-8")


def corpus_promote_exec(minimize: bool, dry_run: bool, test_after: bool = False) -> None:
    """Merge exec worker queues, dedup, diff, and append new scripts."""
    out = fuzz_out_dir()
    if not out.exists():
        die(f"Error: {out} not found (set MAGNI_FUZZ_OUT to the exec campaign dir)")

    log("[1/5] Merging worker queues...")
    seen = merge_queues(out)
    log(f"  {len(seen)} unique items across workers")
    if not seen:
        log("No queue items found. Nothing to promote.")
        return

    if minimize:
        log("[2/5] Minimizing with afl-cmin (exec target)...")
        seen = minimize_merged_exec(seen)
    else:
        log("[2/5] Skipping minimization (use --minimize to enable)")

    log("[3/5] Loading existing scripts...")
    existing = load_existing_exec()
    log(f"  {len(existing)} existing scripts (tables + on-disk)")

    log("[4/5] Diffing against existing corpus...")
    new_seeds = [(content, meta) for h, (content, meta) in seen.items()
                 if h not in existing]
    if not new_seeds:
        log("  No new seeds found. Corpus is up to date.")
        return
    log(f"  {len(new_seeds)} new seeds to promote")

    if dry_run:
        log(f"\n[DRY RUN] Would add {len(new_seeds)} seeds to {EXEC_PROMOTED}")
        return

    log(f"[5/5] Appending {len(new_seeds)} seeds to {EXEC_PROMOTED}...")
    append_to_promoted(new_seeds, {name for name, _ in existing.values()})
    log(f"  Done. Total seeds: {len(existing) + len(new_seeds)}")

    if test_after:
        corpus_generate(exec_scripts=True)
        corpus_test(exec_scripts=True)
