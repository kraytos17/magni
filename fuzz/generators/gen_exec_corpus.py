"""Generate the exec seed corpus (multi-statement SQL scripts).

Run from the repo root: python3 magni.py corpus generate --exec

EXEC_SEEDS are hand-written scripts covering DDL/DML/queries/txns/snapshots.
EXEC_PROMOTED (in promoted_seeds.py) holds fuzzer-grown finds appended by
`corpus promote --exec`.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_HERE, "..")))  # fuzz/: seedgen
CORPUS = os.path.normpath(os.path.join(_HERE, "..", "corpus_exec"))
sys.path.insert(0, CORPUS)  # corpus dir: promoted_seeds
import seedgen

EXEC_SEEDS = [
    ("script_ddl", """\
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0.0);
CREATE TABLE products (price INT CHECK (price > 0), cat INT, FOREIGN KEY (cat) REFERENCES c(id));
DROP TABLE products;
"""),
    ("script_dml", """\
CREATE TABLE t (a INT, b TEXT);
INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, NULL);
SELECT * FROM t WHERE b IS NULL;
SELECT * FROM t WHERE b IS NOT NULL;
UPDATE t SET b = 'X' WHERE a > 1;
DELETE FROM t WHERE a = 3;
SELECT * FROM t;
"""),
    ("script_joins", """\
CREATE TABLE a (id INT, v TEXT);
CREATE TABLE b (id INT, w TEXT);
INSERT INTO a VALUES (1, 'x'), (2, 'y');
INSERT INTO b VALUES (1, 'p'), (3, 'q');
SELECT * FROM a JOIN b ON a.id = b.id;
SELECT * FROM a LEFT JOIN b ON a.id = b.id;
SELECT * FROM a JOIN b USING (id);
"""),
    ("script_aggregates", """\
CREATE TABLE g (k TEXT, v INT);
INSERT INTO g VALUES ('a', 1), ('a', 2), ('b', 3);
SELECT k, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM g GROUP BY k HAVING COUNT(*) > 1;
SELECT DISTINCT k FROM g ORDER BY k LIMIT 2;
"""),
    ("script_subquery", """\
CREATE TABLE t (x INT);
INSERT INTO t VALUES (1), (2), (3);
SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;
SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x < 3);
SELECT x FROM t UNION SELECT x FROM t UNION ALL SELECT x FROM t;
"""),
    ("script_txn", """\
CREATE TABLE t (x INT);
BEGIN;
INSERT INTO t VALUES (1);
COMMIT;
BEGIN;
INSERT INTO t VALUES (2);
ROLLBACK;
SELECT * FROM t;
"""),
    ("script_snapshots", """\
CREATE TABLE t (x INT);
INSERT INTO t VALUES (1);
SELECT * FROM t AS OF SNAPSHOT 1;
"""),
    ("script_between", """\
CREATE TABLE t (a INT);
INSERT INTO t VALUES (1), (5), (10), (15);
SELECT * FROM t WHERE a BETWEEN 1 AND 10;
SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10;
"""),
    ("script_types", """\
CREATE TABLE t (i INT, r REAL, s TEXT, b BLOB);
INSERT INTO t VALUES (0xCAFE, 1.5e3, 'O''Reilly', X'DEADBEEF');
INSERT INTO t VALUES (-2147483648, .5, '', X'');
SELECT * FROM t WHERE s LIKE 'O%';
"""),
    ("script_malformed", """\
SELECT FROM WHERE;
INSERT INTO t VALUES ();
CREATE TABLE;
SELECT 'unterminated;
SELECT * FROM (SELECT * FROM t;
"""),
    ("script_empty", """\
;
;;
"""),
    ("script_explain", """\
CREATE TABLE t (x INT);
EXPLAIN SELECT * FROM t WHERE x = 1;
EXPLAIN INSERT INTO t VALUES (1);
"""),
    ("script_asof", """\
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1);
SELECT id FROM t AS OF TIMESTAMP 1719000000000000;
"""),
    ("script_nested", """\
CREATE TABLE t (a INT, b INT, c INT, d INT);
SELECT * FROM t WHERE (a = 1 AND (b = 2 OR (c = 3 AND (d = 4))));
"""),
    ("script_right_join", """\
CREATE TABLE a (id INT);
CREATE TABLE b (id INT);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT * FROM a RIGHT JOIN b ON a.id = b.id;
SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id;
"""),
    ("script_bare_select", """\
SELECT 1;
SELECT k;
SELECT a, b;
SELECT *;
"""),
    ("script_aggregate_errors", """\
CREATE TABLE g (u INT);
INSERT INTO g VALUES (5);
SELECT 0, COUNT(*) FROM g;
SELECT MAX(nosuchcol) FROM g;
SELECT MIN(nosuchcol) FROM g;
SELECT COUNT(*) FROM g;
"""),
    ("script_text_index", """\
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);
CREATE INDEX i_body ON docs (body);
INSERT INTO docs VALUES (1, 'alpha', 10), (2, 'bb', 20), (3, 'alpha', 30), (4, '', 40), (5, NULL, 50);
SELECT rowid FROM docs WHERE body = 'alpha';
SELECT rowid FROM docs WHERE body LIKE 'al%';
SELECT rowid FROM docs WHERE body IN ('alpha', 'bb');
SELECT id, body, v FROM docs WHERE body = 'alpha';
SELECT id FROM docs WHERE body LIKE 'b%' ORDER BY id DESC LIMIT 2;
EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';
EXPLAIN SELECT id FROM docs WHERE body LIKE 'al%';
UPDATE docs SET v = 99 WHERE body = 'alpha';
DELETE FROM docs WHERE body = 'bb';
SELECT rowid FROM docs WHERE body = 'bb';
"""),
    ("script_drop_index", """\
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);
CREATE INDEX i_body ON docs (body);
INSERT INTO docs VALUES (1, 'alpha', 10), (2, 'bb', 20), (3, NULL, 30);
SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;
DROP INDEX i_body;
SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;
EXPLAIN SELECT id FROM docs WHERE body = 'alpha';
DROP INDEX i_body;
DROP INDEX nope;
CREATE INDEX i_body ON docs (body);
SELECT id FROM docs WHERE body = 'bb' ORDER BY id;
"""),
    ("script_multi_index", """\
CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT);
CREATE INDEX i_title ON docs (title);
CREATE INDEX i_body ON docs (body);
INSERT INTO docs VALUES (1, 't1', 'alpha'), (2, 't2', 'beta'), (3, 't1', 'beta');
SELECT id FROM docs WHERE title = 't1' AND body = 'beta' ORDER BY id;
SELECT id FROM docs WHERE title = 't2' OR body = 'alpha' ORDER BY id;
SELECT body FROM docs WHERE body = 'beta' ORDER BY id;
EXPLAIN SELECT id FROM docs WHERE title = 't1' AND body = 'beta';
DROP INDEX i_body;
SELECT id FROM docs WHERE title = 't1' ORDER BY id;
DROP INDEX i_title ON docs;
SELECT id FROM docs WHERE title = 't1' ORDER BY id;
"""),
    ("script_admin", """\
CREATE TABLE t (id INT PRIMARY KEY, body TEXT);
CREATE INDEX i_body ON t (body);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, NULL);
.checkpoint
SELECT id FROM t WHERE body = 'a';
DELETE FROM t WHERE id = 2;
.vacuum
SELECT rowid FROM t WHERE body = 'a';
SELECT rowid FROM t WHERE body = 'b';
.expire
.expire 0
.expire abc
.frobnicate
SELECT * FROM t;
"""),
]

from promoted_seeds import EXEC_PROMOTED


def main():
    names = seedgen.managed_names(EXEC_SEEDS) | seedgen.managed_names(EXEC_PROMOTED)
    seedgen.clear_managed(CORPUS, names)

    written = seedgen.write_entries(CORPUS, EXEC_SEEDS + EXEC_PROMOTED)

    print(f"wrote {written} exec seeds to {CORPUS}")


if __name__ == "__main__":
    sys.exit(main())
