#!/usr/bin/env bash
# Reproduce a single testcase against the ASan fuzz target.
#   fuzz/scripts/run-one.sh <testcase-file>
# Exits with the fuzz target's exit status.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [ $# -ne 1 ]; then
    echo "usage: $0 <testcase-file>" >&2
    exit 2
fi

if [ ! -x fuzz/fuzz_target ]; then
    bash fuzz/scripts/build.sh
fi

ASAN_OPTIONS=detect_leaks=0:abort_on_error=1 fuzz/fuzz_target "$1"
