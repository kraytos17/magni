#!/usr/bin/env bash
# Run every fuzz corpus seed under the ASan fuzz target.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [ ! -x fuzz/fuzz_target ]; then
  bash fuzz/scripts/build.sh
fi

for f in fuzz/corpus/*; do
  ASAN_OPTIONS=detect_leaks=0 fuzz/fuzz_target "$f" || { echo "FAILED on seed: $f"; exit 1; }
done
echo "All fuzz seeds passed under ASan."
