#!/usr/bin/env bash
# Launch an AFL++ campaign against the ASan fuzz target.
#   fuzz/scripts/run-afl.sh [afl-fuzz extra args...]
#
# Currently runs in dumb mode (-n): the target is not yet AFL-instrumented
# (Phase 4 = Odin LLVM IR -> AFL++ LLVM pass), so coverage feedback is 0 but
# crash/hang detection works. Remove -n once instrumentation lands.
#
# Environment flags needed on dev workstations:
#   AFL_SKIP_CPUFREQ=1                          - skip the CPU-governor check
#   AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1     - core_pattern sends dumps to a pipe
#   AFL_NO_UI=1 (add when running headless/CI)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [ ! -x fuzz/fuzz_target ]; then
    bash fuzz/scripts/build.sh
fi

rm -rf fuzz/afl-output
mkdir -p fuzz/corpus

AFL_SKIP_CPUFREQ=1 \
AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 \
afl-fuzz \
    -n \
    -i fuzz/corpus \
    -o fuzz/afl-output \
    -t 1000 \
    -m none \
    "$@" \
    -- fuzz/fuzz_target @@
