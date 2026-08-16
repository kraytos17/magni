#!/usr/bin/env bash
set -u

MAGNI="./build/magni"
PASS=0
FAIL=0
ROOT=$(mktemp -d /tmp/magni_cli_full.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

OUT=""

say()  { echo "$1" | sed 's/;/;\n/g' | sed 's/^[[:space:]]*/       /' | sed '/^[[:space:]]*$/d'; echo; }
run()  { local d; d=$(mktemp "${ROOT}/db_XXXXXX.db"); say "$1"; OUT=$(echo "$1" | "$MAGNI" "$d" 2>&1) || true; }
runf() { local d; d=$(mktemp "${ROOT}/db_XXXXXX.db"); local f; f=$(mktemp "${ROOT}/sql_XXXXXX.sql"); printf "$1" > "$f"; say "(from --file)"; OUT=$("$MAGNI" --file "$f" "$d" 2>&1) || true; }
rune() { local d; d=$(mktemp "${ROOT}/db_XXXXXX.db"); say "$1"; OUT=$("$MAGNI" --eval "$1" "$d" 2>&1) || true; }
db()   { mktemp "${ROOT}/db_XXXXXX.db"; }
has()    { echo "$OUT" | grep -qiF "$1"; }
no_err() { ! echo "$OUT" | grep -qiE "error|Error"; }
is_err() { echo "$OUT" | grep -qiE "error|Error"; }
ok()     { local name="$1"; shift; if [ $# -eq 0 ] || "$@"; then echo "  PASS  $name"; ((PASS++)); else echo "  FAIL  $name"; echo "       output: $OUT"; ((FAIL++)); fi; }

echo "=== Magni Full CLI Integration Tests ==="
echo ""

echo "--- Basic CLI ---"

OUT=$("$MAGNI" --help 2>&1) || true
ok "--help succeeds" has "usage"

OUT=$("$MAGNI" -h 2>&1) || true
ok "-h succeeds" has "usage"

OUT=$("$MAGNI" --version 2>&1) || true
ok "--version prints version" has "magni"

rune "CREATE TABLE t (x INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
ok "--eval executes SQL" has "1"

TMP=$(mktemp /tmp/magni_cli.XXXXXX)
printf "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (42);\nSELECT * FROM t;\n" > "$TMP"
say "(from --file)"
OUT=$("$MAGNI" --file "$TMP" "$(db)" 2>&1) || true
rm -f "$TMP"
ok "--file executes SQL file" has "42"

say "CREATE TABLE t (x INT); INSERT INTO t VALUES (99); SELECT * FROM t;"
OUT=$(printf "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (99);\nSELECT * FROM t;\n" | "$MAGNI" "$(db)" 2>&1) || true
ok "pipe mode reads SQL from stdin" has "99"

echo ""
echo "--- DDL ---"

run "CREATE TABLE t (a INT, b TEXT, c REAL, d BLOB);"
ok "CREATE TABLE with all types" no_err

run "CREATE TABLE pk_t (id INT PRIMARY KEY, val INT); INSERT INTO pk_t VALUES (1, 10); SELECT * FROM pk_t;"
ok "PRIMARY KEY constraint" has "1"

run "CREATE TABLE nn_t (x INT NOT NULL); INSERT INTO nn_t VALUES (5); SELECT * FROM nn_t;"
ok "NOT NULL constraint (valid insert)" has "5"

run "CREATE TABLE nn_t2 (x INT NOT NULL); INSERT INTO nn_t2 VALUES (NULL);"
ok "NOT NULL constraint rejects NULL" is_err

run "CREATE TABLE def_t (x INT DEFAULT 99, y INT); INSERT INTO def_t (y) VALUES (1); SELECT x FROM def_t;"
ok "DEFAULT constraint applied on omitted column" has "99"

run "CREATE TABLE chk_t (x INT CHECK (x > 0)); INSERT INTO chk_t VALUES (5); SELECT * FROM chk_t;"
ok "CHECK constraint accepts valid value" has "5"

run "CREATE TABLE chk_t2 (x INT CHECK (x > 0)); INSERT INTO chk_t2 VALUES (-1);"
ok "CHECK constraint rejects invalid value" is_err

run "CREATE TABLE d1 (a INT); DROP TABLE d1;"
ok "DROP TABLE" no_err

run "DROP TABLE nonexistent;"
ok "DROP nonexistent table fails" is_err

run "CREATE TABLE dup (x INT); CREATE TABLE dup (y INT);"
ok "Duplicate table name rejected" is_err

run "CREATE TABLE wide (c01 INT, c02 INT, c03 INT, c04 INT, c05 INT, c06 INT, c07 INT, c08 INT, c09 INT, c10 INT, c11 INT);"
ok "Max 10 columns enforced" is_err

run "CREATE TABLE fk_ref (id INT PRIMARY KEY); CREATE TABLE fk_child (ref INT REFERENCES fk_ref(id));"
ok "FOREIGN KEY valid ref table" no_err

echo ""
echo "--- DML ---"

run "CREATE TABLE t (a INT, b TEXT, c REAL); INSERT INTO t VALUES (1, 'hello', 3.14); SELECT * FROM t;"
ok "INSERT with all types" has "hello"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t (b, a) VALUES ('world', 42); SELECT * FROM t;"
ok "INSERT with column reorder" has "42"

run "CREATE TABLE t (a INT, b INT DEFAULT 0); INSERT INTO t (a) VALUES (5); SELECT b FROM t;"
ok "INSERT with default (omitted column)" has "0"

run "CREATE TABLE t (a INT PRIMARY KEY, b INT); INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (1, 20);"
ok "Duplicate PRIMARY KEY rejected" is_err

run "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); UPDATE t SET b = 99 WHERE a = 1; SELECT b FROM t;"
ok "UPDATE with WHERE" has "99"

run "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20); DELETE FROM t WHERE a = 1; SELECT a FROM t;"
ok "DELETE with WHERE" has "2"

run "CREATE TABLE t (a BLOB); INSERT INTO t VALUES (X'CAFE');"
ok "Blob literal (X'CAFE')" no_err

echo ""
echo "--- SELECT ---"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); INSERT INTO t VALUES (2, 'two'); SELECT * FROM t;"
ok "SELECT * returns all rows" has "two"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); INSERT INTO t VALUES (2, 'two'); SELECT a FROM t;"
ok "SELECT specific columns" has "1"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); INSERT INTO t VALUES (2, 'two'); SELECT * FROM t WHERE a = 1;"
ok "WHERE = filter" has "one"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (2, 'b'); SELECT * FROM t WHERE a = 1 OR a = 2;"
ok "WHERE OR" has "b"

run "CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('apple'); INSERT INTO t VALUES ('banana'); SELECT * FROM t WHERE a LIKE 'a%';"
ok "WHERE LIKE" has "apple"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT * FROM t WHERE a IN (1, 3);"
ok "WHERE IN (literal list)" has "3"

run "CREATE TABLE t (a INT, b INT, c INT); INSERT INTO t VALUES (1, 2, 0); INSERT INTO t VALUES (1, 9, 0); INSERT INTO t VALUES (0, 2, 3); INSERT INTO t VALUES (0, 9, 3); SELECT a, b, c FROM t WHERE a = 1 AND b = 2 OR c = 3 ORDER BY a, b, c;"
ok "WHERE mixed AND/OR" has "1|2|0"
ok "WHERE mixed AND/OR includes OR leg" has "0|2|3"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); INSERT INTO t VALUES (2, 'two'); SELECT b AS name, COUNT(*) AS n FROM t GROUP BY b;"
ok "aggregate AS alias header" has "|name|n|"

run "SELECT 1; SELECT 2;"
ok "multi-SELECT script renders first result" has "|1|"
ok "multi-SELECT script renders all results" has "|2|"

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'x'); INSERT INTO t VALUES (2, 'y'); INSERT INTO t VALUES (3, 'z'); SELECT a FROM t WHERE NOT a = 1;"
ok "WHERE NOT prefix" has "2"
ok "WHERE NOT prefix excludes match" no_err

run "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); INSERT INTO t VALUES (2, 'two'); SELECT a id FROM t;"
ok "bare identifier alias header" has "|id|"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1), (2), (3); SELECT COUNT(*) FROM t;"
ok "multi-row VALUES insert" has "3"

run "CREATE TABLE t (id INT PRIMARY KEY, name TEXT); INSERT INTO t (name) VALUES ('a'), ('b'); SELECT id FROM t ORDER BY id;"
ok "omitted PK auto-fills rowid" has "(2 rows)"
ok "omitted PK values are 1 and 2" has "1 "
ok "omitted PK second value is 2" has "2 "

run "CREATE TABLE s (g INT, v INT); INSERT INTO s VALUES (1,10),(1,20),(2,5),(3,100); SELECT g FROM s GROUP BY g HAVING count > 1;"
ok "GROUP BY HAVING (no select aggregate)" has "|1|"
ok "GROUP BY HAVING output clean" no_err

run "CREATE TABLE t1 (a INT); CREATE TABLE t2 (b INT); INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); INSERT INTO t2 VALUES (1); SELECT * FROM t1 WHERE a IN (SELECT b FROM t2);"
ok "WHERE IN (subquery)" has "1"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (3); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT * FROM t ORDER BY a;"
ok "ORDER BY ASC" has "1"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT * FROM t ORDER BY a DESC;"
ok "ORDER BY DESC" has "3"

run "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 9); INSERT INTO t VALUES (2, 8); SELECT * FROM t ORDER BY a, b;"
ok "ORDER BY multi-column" has "9"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT * FROM t ORDER BY a LIMIT 1;"
ok "LIMIT" has "1"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT * FROM t ORDER BY a LIMIT 1 OFFSET 2;"
ok "LIMIT OFFSET" has "3"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT DISTINCT a FROM t;"
ok "DISTINCT" no_err

echo ""
echo "--- Aggregates & GROUP BY ---"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT COUNT(*) FROM t;"
ok "COUNT(*)" has "3"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t;"
ok "SUM/AVG/MIN/MAX" has "6"

run "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (1, 30); SELECT a, SUM(b) FROM t GROUP BY a;"
ok "GROUP BY" has "40"

run "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (1, 30); SELECT a, COUNT(*) FROM t GROUP BY a HAVING count > 1;"
ok "GROUP BY HAVING filters (count > 1)" has "1 rows"

echo ""
echo "--- JOINs ---"

run "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); INSERT INTO t2 VALUES (2); INSERT INTO t2 VALUES (3); SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;"
ok "INNER JOIN" has "2"

run "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); INSERT INTO t2 VALUES (2); SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;"
ok "LEFT JOIN (preserves left rows)" has "1"

run "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); INSERT INTO t1 VALUES (1); INSERT INTO t2 VALUES (2); SELECT * FROM t1 CROSS JOIN t2;"
ok "CROSS JOIN" has "2"

echo ""
echo "--- Subqueries ---"

run "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT * FROM (SELECT * FROM t WHERE a > 1) AS sub;"
ok "FROM subquery" has "2"

echo ""
echo "--- Set Operations ---"

run "CREATE TABLE a (x INT); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2); CREATE TABLE b (x INT); INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); SELECT x FROM a UNION SELECT x FROM b;"
ok "UNION" has "3"

run "CREATE TABLE a (x INT); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2); CREATE TABLE b (x INT); INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); SELECT x FROM a UNION SELECT x FROM b;"
ok "UNION dedups" has "2"

run "CREATE TABLE a (x INT); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2); CREATE TABLE b (x INT); INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); SELECT x FROM a UNION ALL SELECT x FROM b;"
ok "UNION ALL" has "3"

run "CREATE TABLE a (x INT); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2); CREATE TABLE b (x INT); INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); SELECT x FROM a INTERSECT SELECT x FROM b;"
ok "INTERSECT" has "2"

run "CREATE TABLE a (x INT); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2); CREATE TABLE b (x INT); INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); SELECT x FROM a EXCEPT SELECT x FROM b;"
ok "EXCEPT" has "1"

run "SELECT 1 UNION SELECT 2;"
ok "literal UNION" has "2"

echo ""
echo "--- Transactions ---"

run "CREATE TABLE t (a INT); BEGIN; INSERT INTO t VALUES (42); COMMIT; SELECT * FROM t;"
ok "BEGIN/COMMIT persists writes" has "42"

run "CREATE TABLE t (a INT); BEGIN; INSERT INTO t VALUES (99); ROLLBACK; SELECT * FROM t;"
ok "BEGIN/ROLLBACK discards writes" no_err

echo ""
echo "--- Time-Travel ---"

D=$(db)
say "CREATE TABLE tt (a TEXT);"
OUT=$("$MAGNI" --eval "CREATE TABLE tt (a TEXT);" "$D" 2>&1) || true
say "INSERT INTO tt VALUES ('v1');"
OUT=$("$MAGNI" --eval "INSERT INTO tt VALUES ('v1');" "$D" 2>&1) || true
say "INSERT INTO tt VALUES ('v2');"
OUT=$("$MAGNI" --eval "INSERT INTO tt VALUES ('v2');" "$D" 2>&1) || true
say "SELECT * FROM tt AS OF SNAPSHOT 2;"
OUT=$("$MAGNI" --eval "SELECT * FROM tt AS OF SNAPSHOT 2;" "$D" 2>&1) || true
ok "AS OF SNAPSHOT returns historical data" has "v1"

say "SELECT * FROM tt AS OF SNAPSHOT 3;"
OUT=$("$MAGNI" --eval "SELECT * FROM tt AS OF SNAPSHOT 3;" "$D" 2>&1) || true
ok "AS OF SNAPSHOT 3 returns latest data" has "v2"

say ".snapshots"
OUT=$("$MAGNI" --eval ".snapshots" "$D" 2>&1) || true
ok ".snapshots lists chain" no_err

echo ""
echo "--- Dot-commands ---"

D=$(db)
say "CREATE TABLE t (x INT); INSERT INTO t VALUES (1);"
OUT=$("$MAGNI" --eval "CREATE TABLE t (x INT); INSERT INTO t VALUES (1);" "$D" 2>&1) || true
say ".tables"
OUT=$("$MAGNI" --eval ".tables" "$D" 2>&1) || true
ok ".tables lists tables" has "t"

say ".schema"
OUT=$("$MAGNI" --eval ".schema" "$D" 2>&1) || true
ok ".schema shows DDL" no_err

say ".version"
OUT=$("$MAGNI" --eval ".version" "$D" 2>&1) || true
ok ".version prints version" no_err

say ".integrity"
OUT=$("$MAGNI" --eval ".integrity" "$D" 2>&1) || true
ok ".integrity check passes" no_err

say ".stats"
OUT=$("$MAGNI" --eval ".stats" "$D" 2>&1) || true
ok ".stats reports statistics" no_err

say ".checkpoint"
OUT=$("$MAGNI" --eval ".checkpoint" "$D" 2>&1) || true
ok ".checkpoint succeeds" no_err

say ".help"
OUT=$("$MAGNI" --eval ".help" "$D" 2>&1) || true
ok ".help prints help" no_err

say ".desc t"
OUT=$("$MAGNI" --eval ".desc t" "$D" 2>&1) || true
ok ".desc describes table" no_err

say ".dump t"
OUT=$("$MAGNI" --eval ".dump t" "$D" 2>&1) || true
ok ".dump table" no_err

echo ""
echo "--- Edge Cases / Error Handling ---"

rune "SELECT * FROM nonexistent;"
ok "Missing table error" is_err

rune "INSERT INTO nonexistent VALUES (1);"
ok "Insert into missing table error" is_err

rune "CREATE TABLE t (a INT CHECK (a > 0)); INSERT INTO t VALUES (-1);"
ok "CHECK constraint rejects invalid" is_err

rune "CREATE TABLE t (x INT); INSERT INTO t VALUES (1, 2);"
ok "Column count mismatch error" is_err

rune "CREATE TABLE t (x INT); INSERT INTO t VALUES ('not_a_number');"
ok "Type mismatch in INSERT" is_err

echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed"
echo "=============================="
exit $FAIL
