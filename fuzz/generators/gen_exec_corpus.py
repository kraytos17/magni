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
    ("script_index_backfill", """\
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);
INSERT INTO docs VALUES (1, 'alpha', 10), (2, 'bb', 20), (3, 'alpha', 30), (4, '', 40), (5, NULL, 50);
CREATE INDEX i_body ON docs (body);
SELECT rowid FROM docs WHERE body = 'alpha' ORDER BY rowid;
SELECT rowid FROM docs WHERE body LIKE 'al%' ORDER BY rowid;
SELECT rowid FROM docs WHERE body IN ('alpha', 'bb', 'missing') ORDER BY rowid;
EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';
UPDATE docs SET body = 'gamma' WHERE id = 1;
SELECT rowid FROM docs WHERE body = 'alpha' ORDER BY rowid;
SELECT rowid FROM docs WHERE body = 'gamma' ORDER BY rowid;
DELETE FROM docs WHERE body = 'alpha';
SELECT rowid FROM docs WHERE body = 'alpha' ORDER BY rowid;
DROP INDEX i_body;
SELECT id FROM docs WHERE body = 'gamma' ORDER BY id;
CREATE INDEX i_body ON docs (body);
SELECT rowid FROM docs WHERE body LIKE '%mm%' ORDER BY rowid;
SELECT rowid FROM docs WHERE body LIKE 'b_b' ORDER BY rowid;
SELECT rowid FROM docs WHERE body LIKE '%' ORDER BY rowid;
"""),
    ("script_joins_multi", """\
CREATE TABLE a (id INT, v TEXT);
CREATE TABLE b (id INT, w TEXT);
CREATE TABLE c (id INT, z INT);
INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'w');
INSERT INTO b VALUES (1, 'p'), (2, 'q');
INSERT INTO c VALUES (1, 10), (3, 30);
SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id ORDER BY a.id;
SELECT * FROM a CROSS JOIN b ORDER BY a.id LIMIT 3;
SELECT * FROM a RIGHT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id ORDER BY a.id;
SELECT * FROM (SELECT id FROM a WHERE id > 1) AS s JOIN b ON s.id = b.id ORDER BY s.id;
SELECT * FROM a AS x JOIN a AS y ON x.id = y.id ORDER BY x.id LIMIT 2;
SELECT b.id, COUNT(*) FROM a JOIN b ON a.id = b.id GROUP BY b.id HAVING COUNT(*) > 0 ORDER BY b.id;
SELECT * FROM a JOIN b ON a.id = b.id WHERE c.id > 0;
"""),
    ("script_constraints", """\
CREATE TABLE t (id INT PRIMARY KEY, v INT DEFAULT 7, s TEXT NOT NULL, c INT CHECK (c > 0));
INSERT INTO t (id, s, c) VALUES (1, 'a', 5);
INSERT INTO t VALUES (2, 99, 'b', 3);
INSERT INTO t VALUES (1, 0, 'dup', 1);
INSERT INTO t VALUES (3, 0, NULL, 1);
INSERT INTO t VALUES (4, 0, 'neg', -2);
SELECT * FROM t ORDER BY id;
CREATE TABLE p (id INT PRIMARY KEY);
CREATE TABLE k (id INT, pid INT, FOREIGN KEY (pid) REFERENCES p(id));
INSERT INTO p VALUES (1), (2);
INSERT INTO k VALUES (10, 1), (20, 9);
SELECT * FROM k ORDER BY id;
DROP TABLE k;
DROP TABLE p;
DROP TABLE t;
SELECT * FROM t;
"""),
    ("script_snapshot_ops", """\
CREATE TABLE t (x INT, y TEXT);
INSERT INTO t VALUES (1, 'a');
INSERT INTO t VALUES (2, 'b');
.snapshots
.snapshot tag 2 second
SELECT * FROM t AS OF SNAPSHOT 1;
INSERT INTO t VALUES (3, 'c');
.snapshot restore 2
SELECT * FROM t;
INSERT INTO t VALUES (4, 'd');
.rollforward
SELECT * FROM t;
.snapshot restore 99
.snapdiff 1 2
.expire 1
.snapshots
SELECT * FROM t AS OF SNAPSHOT 1;
"""),
    ("script_agg_join", """\
CREATE TABLE o (oid INT, cid INT, amt INT);
CREATE TABLE c (cid INT, name TEXT);
INSERT INTO o VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50);
INSERT INTO c VALUES (1, 'acme'), (2, 'globex');
SELECT c.name, COUNT(*), SUM(o.amt) FROM o JOIN c ON o.cid = c.cid GROUP BY c.name HAVING SUM(o.amt) > 100 ORDER BY c.name;
SELECT cid, COUNT(*) FROM o GROUP BY cid, amt HAVING COUNT(*) >= 1 ORDER BY cid LIMIT 2;
SELECT * FROM (SELECT cid, SUM(amt) AS total FROM o GROUP BY cid HAVING SUM(amt) > 60) AS s ORDER BY total DESC;
SELECT DISTINCT cid FROM o ORDER BY cid;
"""),
    ("script_admin_introspect", """\
CREATE TABLE t (id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
.tables
.schema
.desc t
.desc missing
.dump t
.dump missing
.stats
.tree_page 1
.tree_page 999999
.tree_page abc
.integrity
SELECT * FROM t;
"""),
    ("script_txn_mixed", """\
CREATE TABLE t (id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
BEGIN;
INSERT INTO t VALUES (2, 20);
CREATE TABLE u (id INT);
INSERT INTO u VALUES (1);
SELECT * FROM t ORDER BY id;
SELECT * FROM u;
ROLLBACK;
SELECT * FROM t ORDER BY id;
SELECT * FROM u;
BEGIN;
UPDATE t SET v = 99 WHERE id = 1;
DELETE FROM t WHERE id = 99;
COMMIT;
SELECT * FROM t ORDER BY id;
BEGIN;
COMMIT;
ROLLBACK;
"""),
    ("script_asof_series", """\
CREATE TABLE t (id INT, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT * FROM t AS OF TIMESTAMP 1;
INSERT INTO t VALUES (2, 'b');
SELECT * FROM t AS OF SNAPSHOT 1;
SELECT * FROM t AS OF SNAPSHOT 2;
SELECT * FROM t AS OF SNAPSHOT 99;
DELETE FROM t WHERE id = 1;
SELECT * FROM t AS OF SNAPSHOT 2;
SELECT * FROM t;
"""),
]

from promoted_seeds import EXEC_PROMOTED


def main():
    """Rewrite the exec corpus deterministically (hand + promoted scripts)."""
    names = seedgen.managed_names(EXEC_SEEDS) | seedgen.managed_names(EXEC_PROMOTED)
    seedgen.clear_managed(CORPUS, names)

    written = seedgen.write_entries(CORPUS, EXEC_SEEDS + EXEC_PROMOTED)

    print(f"wrote {written} exec seeds to {CORPUS}")


if __name__ == "__main__":
    sys.exit(main())
