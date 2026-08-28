#!/usr/bin/env bash
# Build the fuzz target with AddressSanitizer. Run every AFL++ campaign against
# this binary so memory bugs (UAF/OOB/double-free) surface as sanitizer reports.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

odin build fuzz \
    -collection:src=src \
    -o:none \
    -sanitize:address \
    -out:fuzz/fuzz_target

echo "Built fuzz/fuzz_target (AddressSanitizer)"
