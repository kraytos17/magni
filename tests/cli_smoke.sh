#!/usr/bin/env bash

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN=./build/magni
TMP=$(mktemp /tmp/magni_cli_test.XXXXXX.db)
TMP2=$(mktemp /tmp/magni_cli_test.XXXXXX)
TMP3=$(mktemp /tmp/magni_cli_test.XXXXXX.db)
export BIN TMP TMP2 TMP3
trap 'rm -f "$TMP" "$TMP2" "$TMP3" test.db' EXIT

fails=0
t() { # t <name> <cmd...>
  local name="$1"; shift
  if "$@"; then
    echo "  [test] $name PASS"
  else
    echo "  [test] $name FAIL"
    fails=$((fails + 1))
  fi
}

t "--help succeeds" "$BIN" --help
t "-h succeeds" "$BIN" -h
t "default database path creates test.db" bash -c 'rm -f test.db; "$BIN" --eval "CREATE TABLE t (x INT);" >/dev/null 2>&1 && [ -f test.db ]'
t "positional database path works" bash -c '"$BIN" --eval "CREATE TABLE t (x INT);" "$TMP" >/dev/null 2>&1 && [ -f "$TMP" ]'
printf 'CREATE TABLE t (x INT);\nINSERT INTO t VALUES (42);\nSELECT * FROM t;\n' > "$TMP2"
t "--eval executes SQL" bash -c '"$BIN" --file "$TMP2" "$TMP" | grep -q 42'
t "pipe mode reads SQL from stdin" bash -c 'printf "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (99);\nSELECT * FROM t;\n" | "$BIN" "$TMP3" | grep -q 99'

echo "CLI smoke tests complete ($fails failures)."
exit "$fails"
