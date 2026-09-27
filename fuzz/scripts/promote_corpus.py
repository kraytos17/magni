#!/usr/bin/env python3
"""Promote AFL++ grown queue seeds into the canonical corpus (gen_corpus.py).

Merges all worker queues, deduplicates by content hash, optionally minimizes
with afl-cmin, diffs against existing SEEDS, and appends new entries to
gen_corpus.py. Idempotent — safe to run multiple times.

Usage:
    python3 fuzz/scripts/promote_corpus.py [OPTIONS]

Options:
    --output DIR     AFL++ output dir with worker queues (default: fuzz/afl-output)
    --corpus DIR     Canonical corpus dir containing gen_corpus.py (default: fuzz/corpus)
    --minimize       Run afl-cmin to minimize before diffing (slow, ~5min)
    --dry-run        Show what would be added without writing
    --test           Run make fuzz-corpus && make fuzz-test after promoting
    --verbose        Print per-file details
"""

import argparse
import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GEN_CORPUS = ROOT / "fuzz" / "corpus" / "gen_corpus.py"
FUZZ_TARGET_COV = ROOT / "fuzz" / "fuzz_target_cov"


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def parse_seeds(gen_corpus_path: Path) -> dict[str, bytes]:
    """Parse existing SEEDS from gen_corpus.py, return {md5: (name, content)}.

    Uses exec() to load the SEEDS variable directly — avoids regex parsing
    issues with multiline bytes literals and escaped characters.
    """
    ns = {"__name__": "gen_corpus_parsing", "__file__": str(gen_corpus_path)}
    try:
        exec(compile(gen_corpus_path.read_text(), str(gen_corpus_path), "exec"), ns)
    except SystemExit:
        pass  # gen_corpus.py calls sys.exit(main()) — ignore it
    except Exception:
        pass

    seeds = {}
    for entry in ns.get("SEEDS", []):
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        name, content = entry
        if isinstance(content, str):
            content = content.encode("utf-8")
        if isinstance(content, bytes):
            seeds[md5(content)] = (name, content)
    return seeds


def parse_afl_metadata(filename: str) -> str:
    """Extract useful AFL metadata from queue filename for a comment."""
    parts = filename.split(",")
    meta = []
    for p in parts:
        if p.startswith("orig:"):
            meta.append(p[5:])
        elif p.startswith("+cov"):
            meta.append("+cov")
        elif p.startswith("sync:"):
            meta.append(f"sync:{p.split(':')[1]}")
    return ",".join(meta) if meta else filename


def merge_queues(output_dir: Path) -> dict[str, tuple[bytes, str]]:
    """Merge all worker queues, dedup by content hash.

    Returns {md5: (content_bytes, afl_metadata)}.
    Skips non-seed files (like gen_corpus.py).
    """
    seen = {}
    for queue_dir in sorted(output_dir.glob("*/queue")):
        for f in sorted(queue_dir.iterdir()):
            if f.is_file() and not f.name.endswith(".py"):
                content = f.read_bytes()
                h = md5(content)
                if h not in seen:
                    seen[h] = (content, parse_afl_metadata(f.name))
    return seen


def run_cmin(merged_dir: Path, output_dir: Path) -> list[Path]:
    """Run afl-cmin to minimize the merged corpus. Returns list of minimized files."""
    cmd = [
        "afl-cmin",
        "-i", str(merged_dir),
        "-o", str(output_dir),
        "-m", "none",
        "-t", "1000",
        "--", str(FUZZ_TARGET_COV), "@@",
    ]

    env = os.environ.copy()
    env["AFL_MAP_SIZE"] = "10000000"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"afl-cmin failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return sorted(output_dir.iterdir())


def generate_seed_name(existing_names: set[str], index: int) -> str:
    """Generate a unique promoted_NNNN name."""
    while True:
        name = f"promoted_{index:04d}"
        if name not in existing_names:
            return name
        index += 1


def main():
    parser = argparse.ArgumentParser(description="Promote AFL++ queue to canonical corpus")
    parser.add_argument("--output", default="fuzz/afl-output", help="AFL++ output dir")
    parser.add_argument("--corpus", default="fuzz/corpus", help="Canonical corpus dir")
    parser.add_argument("--minimize", action="store_true", help="Run afl-cmin before diffing")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be added")
    parser.add_argument("--test", action="store_true", help="Run make fuzz-corpus && make fuzz-test")
    parser.add_argument("--verbose", action="store_true", help="Print details")
    args = parser.parse_args()

    output_dir = ROOT / args.output
    corpus_dir = ROOT / args.corpus
    gen_corpus = corpus_dir / "gen_corpus.py"

    if not output_dir.exists():
        print(f"Error: {output_dir} not found", file=sys.stderr)
        sys.exit(1)
    if not gen_corpus.exists():
        print(f"Error: {gen_corpus} not found", file=sys.stderr)
        sys.exit(1)

    # Step 1: Merge queues
    print("[1/5] Merging worker queues...")
    seen = merge_queues(output_dir)
    print(f"  {len(seen)} unique items across workers")

    if len(seen) == 0:
        print("No queue items found. Nothing to promote.")
        return

    # Step 2: Optionally minimize with afl-cmin
    if args.minimize:
        print("[2/5] Minimizing with afl-cmin...")
        with tempfile.TemporaryDirectory(prefix="magni_merge_") as tmpdir:
            merge_path = Path(tmpdir) / "merged"
            merge_path.mkdir()
            for h, (content, _) in seen.items():
                # Use md5 as filename to avoid collisions
                dest = merge_path / h
                dest.write_bytes(content)

            min_path = Path(tmpdir) / "minimized"
            min_path.mkdir()
            minimized = run_cmin(merge_path, min_path)
            print(f"  {len(minimized)} seeds after minimization")

            # Re-build seen from minimized set
            seen = {}
            for f in minimized:
                content = f.read_bytes()
                h = md5(content)
                seen[h] = (content, parse_afl_metadata(f.name))
    else:
        print("[2/5] Skipping minimization (use --minimize to enable)")

    # Step 3: Load existing seeds
    print("[3/5] Loading existing seeds from gen_corpus.py...")
    existing = parse_seeds(gen_corpus)
    # Also load all on-disk corpus files (catches dynamic seeds like deep_*)
    for f in corpus_dir.iterdir():
        if f.is_file() and f.name != "gen_corpus.py" and not f.name.startswith("__"):
            content = f.read_bytes()
            h = md5(content)
            if h not in existing:
                existing[h] = (f.name, content)
    print(f"  {len(existing)} existing seeds (SEEDS + on-disk)")

    # Step 4: Diff — find new seeds
    print("[4/5] Diffing against existing corpus...")
    new_seeds = []
    for h, (content, metadata) in seen.items():
        if h not in existing:
            new_seeds.append((content, metadata))

    if not new_seeds:
        print("  No new seeds found. Corpus is up to date.")
        return

    print(f"  {len(new_seeds)} new seeds to promote")

    if args.verbose:
        for content, metadata in new_seeds[:20]:
            preview = content[:80].decode("utf-8", errors="replace")
            print(f"    [{metadata}]: {preview}...")
        if len(new_seeds) > 20:
            print(f"    ... and {len(new_seeds) - 20} more")

    # Step 5: Append to gen_corpus.py
    if args.dry_run:
        print(f"\n[DRY RUN] Would add {len(new_seeds)} seeds to {gen_corpus}")
        return

    print(f"[5/5] Appending {len(new_seeds)} seeds to {gen_corpus}...")

    # Find highest existing promoted_NNNN index from parsed seeds
    existing_names = set()
    for name, _ in existing.values():
        existing_names.add(name)

    max_index = 0
    for name in existing_names:
        if name.startswith("promoted_"):
            try:
                idx = int(name.split("_")[1])
                max_index = max(max_index, idx)
            except (IndexError, ValueError):
                pass

    # Build new entries
    new_entries = []
    for content, metadata in new_seeds:
        max_index += 1
        name = generate_seed_name(existing_names, max_index)
        existing_names.add(name)

        # Format as Python bytes literal
        py_repr = repr(content)
        comment = f"  # {metadata}" if metadata else ""
        entry = f'    ("{name}", {py_repr}),{comment}'
        new_entries.append(entry)

    # Insert before the closing ] of SEEDS
    # Find the last entry line (before the closing bracket)
    text = gen_corpus.read_text()
    lines = text.split("\n")

    # Find the position right after the last seed entry
    insert_pos = len(lines) - 1
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped.startswith("(") and (stripped.endswith(("),", "),"))):
            insert_pos = i + 1
            break

    # Insert new entries
    for entry in reversed(new_entries):
        lines.insert(insert_pos, entry)

    gen_corpus.write_text("\n".join(lines))
    print(f"  Done. Total seeds: {len(existing) + len(new_seeds)}")

    # Optionally run tests
    if args.test:
        print("\n[6/6] Running make fuzz-corpus && make fuzz-test...")
        for cmd in [["make", "fuzz-corpus"], ["make", "fuzz-test"]]:
            print(f"  $ {' '.join(cmd)}")
            result = subprocess.run(cmd, cwd=str(ROOT))
            if result.returncode != 0:
                print(f"  Command failed: {' '.join(cmd)}", file=sys.stderr)
                sys.exit(1)
        print("  All tests passed.")


if __name__ == "__main__":
    main()
