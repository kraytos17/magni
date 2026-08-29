#!/usr/bin/env bash
# Vet dispatcher.
#   vet.sh                        fast vet (odin check, full vet flags)
#   vet.sh <shadowing|unused|style|cast>   single-flag vet over src + tests
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

case "${1:-}" in
  "")
    odin check src -collection:src=src -vet -vet-shadowing -warnings-as-errors -strict-style
    exit 0 ;;
  shadowing) flags=(-vet-shadowing) ;;
  unused)    flags=(-vet-unused) ;;
  style)     flags=(-vet-style -vet-semicolon) ;;
  cast)      flags=(-vet-cast) ;;
  *) echo "usage: $0 [shadowing|unused|style|cast]" >&2; exit 2 ;;
esac

odin build src -collection:src=src "${flags[@]}" -warnings-as-errors -out:/dev/null
odin test  tests -collection:src=src "${flags[@]}" -warnings-as-errors -define:ODIN_TEST_THREADS=1
