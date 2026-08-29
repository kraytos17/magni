#!/usr/bin/env bash
# Launch an AFL++ campaign against the coverage-instrumented fuzz target.
#   fuzz/scripts/run-afl.sh [afl-fuzz extra args...]
#
# The target is built from Odin LLVM IR with AFL++'s LLVM pass (build-cov.sh),
# so coverage feedback and the forkserver are active. Extra args pass through to
# afl-fuzz: e.g. `-V 300` for a 5-minute run, or `-M master` / `-S s1` for
# parallel campaigns (see run-afl-parallel.sh).
#
# Env overrides:
#   AFL_OUT=<dir>   output directory (default fuzz/afl-output)
#   AFL_NO_UI=1     headless (CI)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [ ! -x fuzz/fuzz_target_cov ]; then
  bash fuzz/scripts/build-cov.sh
fi

OUT="${AFL_OUT:-fuzz/afl-output}"
rm -rf "$OUT"
mkdir -p fuzz/corpus

# A secondary worker (-S) syncs seeds from the master's queue, so it must NOT
# supply -i. Everyone else seeds from the corpus.
seed_args=(-i fuzz/corpus)
if [[ " $* " == *" -S "* ]]; then
  seed_args=()
fi

AFL_SKIP_CPUFREQ=1 \
AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 \
afl-fuzz \
    "${seed_args[@]}" \
    -o "$OUT" \
    -x fuzz/sql.dict \
    -t 1000 \
    -m none \
    "$@" \
    -- fuzz/fuzz_target_cov @@
