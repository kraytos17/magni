#!/usr/bin/env bash
# MagniDB fuzz command dispatcher.
#   fuzz.sh <cmd> [args...]
#   cmds: corpus | build | cov | test | run | campaign | status | stop
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

cmd="${1:-help}"
shift || true

case "$cmd" in
  corpus)   python3 fuzz/corpus/gen_corpus.py ;;
  build)    bash fuzz/scripts/build.sh ;;
  cov)      bash fuzz/scripts/build-cov.sh ;;
  test)     bash fuzz/scripts/test-corpus.sh ;;
  run)      bash fuzz/scripts/run-afl.sh "$@" ;;
  campaign) AFL_NO_UI=1 bash fuzz/scripts/run-afl-parallel.sh "${FUZZ_WORKERS:-4}" -V "${FUZZ_SECONDS:-3600}" "$@" ;;
  status)   bash fuzz/scripts/status.sh ;;
  stop)     pkill afl-fuzz || true ;;
  *)
    echo "usage: fuzz.sh <corpus|build|cov|test|run|campaign|status|stop>" >&2
    exit 2 ;;
esac
