#!/usr/bin/env bash
# Launch a parallel AFL++ campaign: 1 master + (N-1) secondary workers sharing
# one output dir (seeds sync via the master's queue). Each worker gets its own
# core (pinned by AFL++); leave at least 1-2 cores for the OS/build.
#   fuzz/scripts/run-afl-parallel.sh N [afl-fuzz extra args...]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

N="${1:-4}"
shift || true

OUT="${AFL_OUT:-fuzz/afl-output}"
rm -rf "$OUT"

pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' INT TERM EXIT

echo "Starting $N AFL++ workers (output: $OUT)..."
AFL_OUT="$OUT" bash fuzz/scripts/run-afl.sh -M master "$@" &
pids+=($!)

for i in $(seq 1 $((N - 1))); do
  AFL_OUT="$OUT" bash fuzz/scripts/run-afl.sh -S "s$i" "$@" &
  pids+=($!)
done

wait "${pids[@]}"
