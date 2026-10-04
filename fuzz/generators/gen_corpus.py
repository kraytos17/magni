"""Generate the AFL++ seed corpus for the SQL parser fuzz target.

The corpus directory is gitignored, so this script is the single source of
truth for seeds. It is deterministic: it clears fuzz/corpus and rewrites every
seed. Run from the repo root:

    python3 fuzz/generators/gen_corpus.py

Seeds are curated by grammar production plus adversarial shapes (malformed,
unterminated, deeply nested). Deep nesting is bounded on purpose: the parser
caps SELECT nesting at MAX_PARSE_NESTING (512) and returns a clean error, so
the over-limit seed exercises that guard rather than crashing.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_HERE, "..")))  # fuzz/: seedgen
CORPUS = os.path.normpath(os.path.join(_HERE, "..", "corpus"))
sys.path.insert(0, CORPUS)  # corpus dir: promoted_seeds
import seedgen

HAND_SEEDS = [
    ("empty", ""),
    ("minimal_valid", "SELECT 1;"),
    ("simple_select", "SELECT * FROM users;"),
    ("select_where", "SELECT name, score FROM users WHERE score > 50 AND name != 'Bob';"),
    ("inner_join", "SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;"),
    ("left_join", "SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;"),
    ("cross_join", "SELECT * FROM t1 CROSS JOIN t2;"),
    ("subquery", "SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;"),
    ("group_having", "SELECT name, COUNT(*) FROM users GROUP BY name HAVING count > 1;"),
    ("union", "SELECT x FROM t1 UNION SELECT x FROM t2;"),
    ("union_all", "SELECT x FROM t1 UNION ALL SELECT x FROM t2;"),
    ("intersect", "SELECT x FROM t1 INTERSECT SELECT x FROM t2;"),
    ("except", "SELECT x FROM t1 EXCEPT SELECT x FROM t2;"),
    ("literal", "SELECT 1, 'a', NULL;"),
    ("explain", "EXPLAIN SELECT * FROM users WHERE id = 1;"),
    ("create_table",
     "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0.0);"),
    ("insert_values", "INSERT INTO users VALUES (1, 'Alice', 99.5);"),
    ("insert_cols", "INSERT INTO users (name, score, id) VALUES ('Bob', 88.0, 2);"),
    ("update", "UPDATE users SET score = 100.0 WHERE id = 1;"),
    ("delete", "DELETE FROM users WHERE id = 1;"),
    ("drop", "DROP TABLE users;"),
    ("txn", "BEGIN; INSERT INTO t VALUES (1); COMMIT;"),
    ("order_limit", "SELECT * FROM t ORDER BY name DESC LIMIT 5 OFFSET 10;"),
    ("distinct", "SELECT DISTINCT name FROM users;"),
    ("like_in",
     "SELECT * FROM t WHERE name LIKE 'A%' AND id IN (1, 3, 5) AND id NOT IN (2, 4);"),
    ("hex_literals", "SELECT 0xCAFE, 0xff;"),
    ("as_of_snapshot", "SELECT id FROM t AS OF SNAPSHOT 5;"),
    ("as_of_timestamp", "SELECT id FROM t AS OF TIMESTAMP 1719000000000000;"),
    ("constraints",
     "CREATE TABLE products (price INT CHECK (price > 0), FOREIGN KEY (cat) REFERENCES c(id));"),
    ("nested_parens", "SELECT * FROM t WHERE (a = 1 AND (b = 2 OR (c = 3 AND (d = 4))));"),
    ("right_join", "SELECT * FROM a RIGHT JOIN b ON a.id = b.id;"),
    ("right_outer_join", "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id;"),
    ("between_simple", "SELECT * FROM t WHERE a BETWEEN 1 AND 10;"),
    ("not_between", "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10;"),
    ("using_join", "SELECT * FROM a JOIN b USING (id);"),
    ("between_subquery", "SELECT * FROM t WHERE id BETWEEN (SELECT min_id FROM bounds) AND 100;"),
    ("unterminated_string", "SELECT 'abc; "),
    ("unterminated_comment", "SELECT 1 /* comment"),
    ("malformed", "SELECT FROM WHERE;"),
    ("empty_values", "INSERT INTO t VALUES ();"),
    ("numbers", "SELECT -2147483648, 2147483647, 0.5, 1e10;"),
    ("whitespace", "  \n\tSELECT   1  ;  "),
    ("unicode", "SELECT 'h\u00e9llo w\u00f6rld';".encode("utf-8")),
    ("unicode_emoji", "SELECT '😀';".encode()),
    ("long_identifier", b"SELECT " + b"x" * 500 + b";"),
    ("long_string", b"SELECT '" + b"x" * 10000 + b"';"),
    ("where_eq", 'SELECT * FROM t WHERE a = 1;'),
    ("where_lt_le_ge", 'SELECT * FROM t WHERE a < 1 AND b <= 2 AND c >= 3 AND d <> 4;'),
    ("where_not_like", "SELECT * FROM t WHERE name NOT LIKE 'A%';"),
    ("where_in_subquery", 'SELECT * FROM t WHERE id IN (SELECT id FROM u);'),
    ("where_col_col", 'SELECT * FROM t WHERE t1.a = t2.b;'),
    ("where_having_agg", 'SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) >= 2;'),
    ("where_or_flat", 'SELECT * FROM t WHERE a=1 OR b=2;'),
    ("where_not_prefix", 'SELECT * FROM t WHERE NOT (a=1 OR b=2);'),
    ("where_not_chain", 'SELECT * FROM t WHERE NOT NOT a=1;'),
    ("where_is_null", 'SELECT * FROM t WHERE b IS NULL;'),
    ("where_is_not_null", 'SELECT * FROM t WHERE b IS NOT NULL;'),
    ("where_is_null_combo", 'SELECT a FROM t WHERE b IS NULL OR a = 1 AND b IS NOT NULL;'),
    ("where_mixed_precedence", 'SELECT * FROM t WHERE a=1 AND b=2 OR c=3 AND d=4;'),
    ("where_and_3way", 'SELECT * FROM t WHERE a=1 AND b=2 AND c=3;'),
    ("join_bare", 'SELECT * FROM a JOIN b ON a.x=b.y;'),
    ("join_comma", 'SELECT * FROM a, b WHERE a.id=b.id;'),
    ("join_left_outer", 'SELECT * FROM a LEFT OUTER JOIN b ON a.x=b.y;'),
    ("join_multi", 'SELECT * FROM a JOIN b ON a.x=b.y JOIN c ON b.y=c.z;'),
    ("join_subquery", 'SELECT * FROM t1 JOIN (SELECT * FROM t2) AS s ON t1.x=s.y;'),
    ("join_complex_on", 'SELECT * FROM a JOIN b ON a.x=b.y AND a.z>1;'),
    ("alias_variants", 'SELECT t.a AS x, t.b y FROM t AS tbl JOIN u ON tbl.id=u.id;'),
    ("table_alias_plain", 'SELECT * FROM users AS u;'),
    ("table_alias_implicit", 'SELECT * FROM users u;'),
    ("qualified_select", 'SELECT t.a, t.b FROM t;'),
    ("group_multi", 'SELECT a,b,COUNT(*) FROM t GROUP BY a,b;'),
    ("having_agg", 'SELECT g, COUNT(*) FROM t GROUP BY g HAVING SUM(v) > 5;'),
    ("having_or", 'SELECT g FROM t GROUP BY g HAVING COUNT(*) >=2 OR SUM(v)>50;'),
    ("order_multi", 'SELECT * FROM t ORDER BY a ASC, b DESC NULLS FIRST;'),
    ("order_qualified", 'SELECT * FROM t ORDER BY t.a;'),
    ("limit_only", 'SELECT * FROM t LIMIT 5;'),
    ("limit_zero", 'SELECT * FROM t LIMIT 0;'),
    ("distinct_star", 'SELECT DISTINCT * FROM t;'),
    ("distinct_multi", 'SELECT DISTINCT a,b FROM t;'),
    ("compound_order", 'SELECT a FROM t UNION ALL SELECT b FROM u ORDER BY 1 LIMIT 2;'),
    ("agg_all", 'SELECT COUNT(*), COUNT(a), SUM(a), AVG(a), MIN(a), MAX(a) FROM t;'),
    ("agg_qualified", 'SELECT SUM(t.v) FROM t;'),
    ("agg_alias", 'SELECT COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g;'),
    ("explain_insert", 'EXPLAIN INSERT INTO t VALUES (1);'),
    ("blob_type", 'CREATE TABLE t (x BLOB PRIMARY KEY);'),
    ("integer_longform", 'CREATE TABLE t (x INTEGER PRIMARY KEY);'),
    ("default_variants", "CREATE TABLE t (a INT DEFAULT 'x', b INT DEFAULT X'FF', c INT DEFAULT NULL);"),
    ("default_neg_sci", 'CREATE TABLE t (a INT DEFAULT -5, b REAL DEFAULT 1e3);'),
    ("check_nested", 'CREATE TABLE t (a INT CHECK (a>0 AND (b<5 OR c=3)));'),
    ("check_simple", 'CREATE TABLE t (a INT CHECK (a > 0));'),
    ("fk_multi", 'CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES u(x), FOREIGN KEY (b) REFERENCES v(y));'),
    ("constraints_multi", 'CREATE TABLE t (a INT PRIMARY KEY NOT NULL DEFAULT 1 CHECK (a>0));'),
    ("insert_multi", "INSERT INTO t (a,b) VALUES (1,'x'), (2,'y'), (3,X'FF');"),
    ("insert_blob", "INSERT INTO t VALUES (1, X'DEADBEEF', x'deadbeef', X'');"),
    ("update_multi", "UPDATE t SET a=1, b='x' WHERE id=2;"),
    ("update_no_where", 'UPDATE t SET a=1;'),
    ("delete_no_where", 'DELETE FROM t;'),
    ("txn_rollback", 'BEGIN; INSERT INTO t VALUES (1); ROLLBACK;'),
    ("begin_alone", 'BEGIN;'),
    ("commit_alone", 'COMMIT;'),
    ("rollback_alone", 'ROLLBACK;'),
    ("asof_where", 'SELECT id FROM t AS OF SNAPSHOT 5 WHERE a=1;'),
    ("snapshot_as_ident", 'SELECT snapshot FROM t;'),
    ("unterminated_blob", "SELECT X'ABC"),
    ("invalid_char", 'SELECT !;'),
    ("invalid_at", 'SELECT @;'),
    ("number_edge", 'SELECT 9223372036854775808, .5, 0x, 1e, 0xZZ, 1E+10, 1.5e-3;'),
    ("number_scientific", 'SELECT 1e10, 1E+5, 1.5e-3, 0e0;'),
    ("string_escaped", "SELECT 'O''Reilly', '', 'a';"),
    ("blob_variants", "SELECT X'DEADBEEF', x'deadbeef', X'';"),
    ("blob_invalid_hex", "SELECT X'ZZZZ';"),
    ("whitespace_crlf", 'SELECT 1;\r\nSELECT 2;'),
    ("line_comment", 'SELECT 1 -- comment\n;'),
    ("block_comment", 'SELECT /* inline */ 1;'),
    ("no_semicolon", 'SELECT 1'),
    ("double_semi", 'SELECT 1;;'),
    ("semicolon_only", ';'),
    ("leading_semi", '; SELECT 1;'),
    ("underscore_ident", 'SELECT _foo FROM t;'),
    ("qualified_dot", 'SELECT a.b FROM t;'),
    ("qualified_dot2", 'SELECT a.b.c FROM t;'),
    ("quoted_semi", "INSERT INTO t VALUES (1, 'hello; world');"),
    ("long_ident_keyword", 'SELECT REFERENCES FROM t;'),
    ("long_referenceST", 'SELECT REFERENCEST FROM t;'),
    ("insert_hex_default", 'CREATE TABLE t (a INT DEFAULT 0xFF);'),
    ("txn_bad_fk", 'CREATE TABLE products (price INT CHECK (price > 0), FOREIGN KEY (cat) REFERENCES c(idI);'),
    ("create_index", 'CREATE INDEX i_body ON docs (body);'),
    ("create_index_no_cols", 'CREATE INDEX i_body ON docs;'),
    ("create_index_multi_col", 'CREATE INDEX i ON t (a, b);'),
    ("drop_index", 'DROP INDEX i_body;'),
    ("drop_index_bare", 'DROP INDEX;'),
    ("drop_index_on", 'DROP INDEX i ON docs;'),
    ("drop_index_on_bare", 'DROP INDEX i ON;'),
    ("index_or", "SELECT id FROM docs WHERE body = 'a' OR body = 'b';"),
    ("index_or_unusable", "SELECT id FROM docs WHERE body = 'a' OR v = 1;"),
    ("index_and_multi", "SELECT id FROM docs WHERE title = 't' AND body LIKE 'a%';"),
    ("index_cover_col", "SELECT body FROM docs WHERE body = 'a';"),
    ("select_rowid", "SELECT rowid FROM docs WHERE body = 'alpha';"),
    ("select_rowid_prefix", "SELECT rowid FROM docs WHERE body LIKE 'al%';"),
    ("select_rowid_in", "SELECT rowid FROM docs WHERE body IN ('a', 'b');"),
    ("explain_index", "EXPLAIN SELECT rowid FROM docs WHERE body = 'x';"),
]

from promoted_seeds import PROMOTED_SEEDS

SEEDS = HAND_SEEDS + PROMOTED_SEEDS

def deep_subquery(levels):
    s = "SELECT * FROM t"
    for i in range(levels):
        s = f"SELECT * FROM ({s}) AS s{i}"
    return s


def deep_parens(levels):
    return "SELECT * FROM t WHERE " + "(" * levels + "a = 1" + ")" * levels + ";"


def deep_parens_not(levels):
    s = "a = 1"
    for _ in range(levels):
        s = "NOT (" + s + ")"
    return "SELECT * FROM t WHERE " + s + ";"


def main():
    seed_names = seedgen.managed_names(SEEDS)
    seed_names |= {
        "deep_subquery_under_guard",
        "deep_subquery_over_guard",
        "deep_parens",
        "deep_parens_511",
        "deep_parens_513",
        "deep_parens_not",
        "deep_check_nested",
    }

    # Clear the corpus dir of any files we manage (keeps stale seeds from lingering).
    seedgen.clear_managed(CORPUS, seed_names)

    written = seedgen.write_entries(CORPUS, SEEDS)

    with open(os.path.join(CORPUS, "deep_subquery_under_guard"), "w") as f:
        f.write(deep_subquery(500))
    with open(os.path.join(CORPUS, "deep_subquery_over_guard"), "w") as f:
        f.write(deep_subquery(600))
    with open(os.path.join(CORPUS, "deep_parens"), "w") as f:
        f.write(deep_parens(300))
    with open(os.path.join(CORPUS, "deep_parens_511"), "w") as f:
        f.write(deep_parens(511))
    with open(os.path.join(CORPUS, "deep_parens_513"), "w") as f:
        f.write(deep_parens(513))
    with open(os.path.join(CORPUS, "deep_parens_not"), "w") as f:
        f.write(deep_parens_not(200))
    with open(os.path.join(CORPUS, "deep_check_nested"), "w") as f:
        f.write("CREATE TABLE t (a INT CHECK (" + "(" * 300 + "a>0" + ")" * 300 + "));")
    written += 7

    print(f"wrote {written} seeds to {CORPUS}")


if __name__ == "__main__":
    sys.exit(main())
