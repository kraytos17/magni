#!/usr/bin/env bash
# Build the coverage-instrumented fuzz target for AFL++.
#
# Odin emits per-package LLVM IR (including the runtime) via
# -build-mode:llvm-ir; afl-clang-fast compiles it with the AFL++ LLVM pass and
# links the executable. This is the FAST build for campaigns — no ASan. Confirm
# any crash under the ASan target via fuzz/scripts/run-one.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

rm -rf fuzz/build
mkdir -p fuzz/build
odin build fuzz -build-mode:llvm-ir -collection:src=src -o:speed -out:fuzz/build

# -o:speed emits a single merged module named ".ll"; the default emits one
# .ll per package. Compile whichever shape was produced.
if [ -f fuzz/build/.ll ]; then
  afl-clang-fast fuzz/build/.ll -o fuzz/fuzz_target_cov
else
  afl-clang-fast fuzz/build/*.ll -o fuzz/fuzz_target_cov
fi
echo "Built fuzz/fuzz_target_cov (AFL++ coverage-instrumented)"
