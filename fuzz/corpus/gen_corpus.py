#!/usr/bin/env python3
"""Generate the AFL++ seed corpus for the SQL parser fuzz target.

The corpus directory is gitignored, so this script is the single source of
truth for seeds. It is deterministic: it clears fuzz/corpus and rewrites every
seed. Run from the repo root:

    python3 fuzz/gen_corpus.py

Seeds are curated by grammar production plus adversarial shapes (malformed,
unterminated, deeply nested). Deep nesting is bounded on purpose: the parser
caps SELECT nesting at MAX_PARSE_NESTING (512) and returns a clean error, so
the over-limit seed exercises that guard rather than crashing.
"""

import os
import sys

CORPUS = os.path.join(os.path.dirname(os.path.abspath(__file__)))

SEEDS = [
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

    # --- adversarial / malformed ---
    ("unterminated_string", "SELECT 'abc; "),
    ("unterminated_comment", "SELECT 1 /* comment"),
    ("malformed", "SELECT FROM WHERE;"),
    ("empty_values", "INSERT INTO t VALUES ();"),
    ("numbers", "SELECT -2147483648, 2147483647, 0.5, 1e10;"),
    ("whitespace", "  \n\tSELECT   1  ;  "),

    # --- generated shapes ---
    ("unicode", "SELECT 'h\u00e9llo w\u00f6rld';".encode("utf-8")),
    ("long_identifier", b"SELECT " + b"x" * 500 + b";"),
    ("long_string", b"SELECT '" + b"x" * 10000 + b"';"),
]


def deep_subquery(levels):
    s = "SELECT * FROM t"
    for i in range(levels):
        s = "SELECT * FROM (" + s + ") AS s%d" % i
    return s


def deep_parens(levels):
    return "SELECT * FROM t WHERE " + "(" * levels + "a = 1" + ")" * levels + ";"


def main():
    seed_names = {name for name, _ in SEEDS}
    seed_names |= {"deep_subquery_under_guard", "deep_subquery_over_guard", "deep_parens"}

    # Clear the corpus dir of any files we manage (keeps stale seeds from lingering).
    for fn in os.listdir(CORPUS):
        if fn in seed_names:
            os.remove(os.path.join(CORPUS, fn))

    written = 0
    for name, content in SEEDS:
        with open(os.path.join(CORPUS, name), "wb") as f:
            data = content.encode("utf-8") if isinstance(content, str) else content
            f.write(data)
        written += 1

    # Deep nesting: one just under the parser guard (MAX_PARSE_NESTING=512) and
    # one over it, which must return a clean parse error rather than crash.
    with open(os.path.join(CORPUS, "deep_subquery_under_guard"), "w") as f:
        f.write(deep_subquery(500))
    with open(os.path.join(CORPUS, "deep_subquery_over_guard"), "w") as f:
        f.write(deep_subquery(600))
    with open(os.path.join(CORPUS, "deep_parens"), "w") as f:
        f.write(deep_parens(300))
    written += 3

    print("wrote %d seeds to %s" % (written, CORPUS))


if __name__ == "__main__":
    sys.exit(main())
