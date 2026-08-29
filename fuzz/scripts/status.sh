#!/usr/bin/env bash
# Print per-worker progress of the running AFL++ campaign.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

for f in fuzz/afl-output/*/fuzzer_stats; do
  [ -f "$f" ] || continue
  echo "== $(basename "$(dirname "$f")") =="
  rg "execs_done|execs_per_sec|corpus_count|saved_crashes|saved_hangs|stability|cycles_done" "$f"
done
