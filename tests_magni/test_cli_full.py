"""CLI full integration tests: black-box binary surface. Port of tests/cli_test.sh.

Method names match the shell `ok` labels (lowercased) for diffability.
Each case uses a fresh database unless the shell section shared one $D
(Time-Travel, Dot-commands) — those rebuild the same setup per method for
isolation; snapshot IDs are deterministic from the setup sequence.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests_magni.clirunner import MagniCLITestCase


class TestBasicCli(MagniCLITestCase):
    def test_help_succeeds(self):
        p = self.run_raw(["--help"])
        self.assertOk(p)
        self.assertHas(p, "usage")

    def test_h_succeeds(self):
        p = self.run_raw(["-h"])
        self.assertOk(p)
        self.assertHas(p, "usage")

    def test_version_prints_version(self):
        p = self.run_raw(["--version"])
        self.assertOk(p)
        self.assertHas(p, "magni")

    def test_eval_executes_sql(self):
        p = self.run_eval(
            "CREATE TABLE t (x INT); INSERT INTO t VALUES (1); SELECT * FROM t;")
        self.assertHas(p, "1")

    def test_file_executes_sql_file(self):
        p = self.run_file(
            "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (42);\n"
            "SELECT * FROM t;\n")
        self.assertHas(p, "42")

    def test_pipe_mode_reads_sql_from_stdin(self):
        p = self.run_stdin(
            "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (99);\n"
            "SELECT * FROM t;\n")
        self.assertHas(p, "99")


class TestDdl(MagniCLITestCase):
    def test_create_table_with_all_types(self):
        p = self.run_stdin("CREATE TABLE t (a INT, b TEXT, c REAL, d BLOB);")
        self.assertNoErr(p)

    def test_primary_key_constraint(self):
        p = self.run_stdin(
            "CREATE TABLE pk_t (id INT PRIMARY KEY, val INT); "
            "INSERT INTO pk_t VALUES (1, 10); SELECT * FROM pk_t;")
        self.assertHas(p, "1")

    def test_not_null_constraint_valid_insert(self):
        p = self.run_stdin(
            "CREATE TABLE nn_t (x INT NOT NULL); INSERT INTO nn_t VALUES (5);")
        self.assertNoErr(p)

    def test_not_null_constraint_rejects_null(self):
        p = self.run_stdin(
            "CREATE TABLE nn_t2 (x INT NOT NULL); INSERT INTO nn_t2 VALUES (NULL);")
        self.assertIsErr(p)

    def test_default_constraint_applied_on_omitted_column(self):
        p = self.run_stdin(
            "CREATE TABLE def_t (x INT DEFAULT 99, y INT); "
            "INSERT INTO def_t (y) VALUES (1); SELECT x FROM def_t;")
        self.assertHas(p, "99")

    def test_check_constraint_accepts_valid_value(self):
        p = self.run_stdin(
            "CREATE TABLE chk_t (x INT CHECK (x > 0)); "
            "INSERT INTO chk_t VALUES (5); SELECT * FROM chk_t;")
        self.assertHas(p, "5")

    def test_check_constraint_rejects_invalid_value(self):
        p = self.run_stdin(
            "CREATE TABLE chk_t2 (x INT CHECK (x > 0)); "
            "INSERT INTO chk_t2 VALUES (-1);")
        self.assertIsErr(p)

    def test_drop_table(self):
        p = self.run_stdin("CREATE TABLE d1 (a INT); DROP TABLE d1;")
        self.assertNoErr(p)

    def test_drop_nonexistent_table_fails(self):
        p = self.run_stdin("DROP TABLE nonexistent;")
        self.assertIsErr(p)

    def test_duplicate_table_name_rejected(self):
        p = self.run_stdin("CREATE TABLE dup (x INT); CREATE TABLE dup (y INT);")
        self.assertIsErr(p)

    def test_max_10_columns_enforced(self):
        p = self.run_stdin(
            "CREATE TABLE wide (c01 INT, c02 INT, c03 INT, c04 INT, c05 INT, "
            "c06 INT, c07 INT, c08 INT, c09 INT, c10 INT, c11 INT);")
        self.assertIsErr(p)

    def test_foreign_key_valid_ref_table(self):
        p = self.run_stdin(
            "CREATE TABLE fk_ref (id INT PRIMARY KEY); "
            "CREATE TABLE fk_child (ref INT REFERENCES fk_ref(id));")
        self.assertNoErr(p)


class TestDml(MagniCLITestCase):
    def test_insert_with_all_types(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b TEXT, c REAL); "
            "INSERT INTO t VALUES (1, 'hello', 3.14); SELECT * FROM t;")
        self.assertHas(p, "hello")

    def test_insert_with_column_reorder(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b TEXT); "
            "INSERT INTO t (b, a) VALUES ('world', 42); SELECT * FROM t;")
        self.assertHas(p, "42")

    def test_insert_with_default_omitted_column(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT DEFAULT 0); "
            "INSERT INTO t (a) VALUES (5); SELECT b FROM t;")
        self.assertHas(p, "0")

    def test_duplicate_primary_key_rejected(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT PRIMARY KEY, b INT); "
            "INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (1, 20);")
        self.assertIsErr(p)

    def test_update_with_where(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); "
            "UPDATE t SET b = 99 WHERE a = 1; SELECT b FROM t;")
        self.assertHas(p, "99")

    def test_delete_with_where(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); "
            "INSERT INTO t VALUES (2, 20); DELETE FROM t WHERE a = 1; "
            "SELECT a FROM t;")
        self.assertHas(p, "2")

    def test_blob_literal(self):
        p = self.run_stdin(
            "CREATE TABLE t (a BLOB); INSERT INTO t VALUES (X'CAFE');")
        self.assertNoErr(p)


class TestSelect(MagniCLITestCase):
    T2 = ("CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'one'); "
          "INSERT INTO t VALUES (2, 'two'); ")

    def test_select_star_returns_all_rows(self):
        p = self.run_stdin(self.T2 + "SELECT * FROM t;")
        self.assertHas(p, "two")

    def test_select_specific_columns(self):
        p = self.run_stdin(self.T2 + "SELECT a FROM t;")
        self.assertHas(p, "1")

    def test_where_eq_filter(self):
        p = self.run_stdin(self.T2 + "SELECT * FROM t WHERE a = 1;")
        self.assertHas(p, "one")

    def test_where_or(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'a'); "
            "INSERT INTO t VALUES (2, 'b'); "
            "SELECT * FROM t WHERE a = 1 OR a = 2;")
        self.assertHas(p, "b")

    def test_where_like(self):
        p = self.run_stdin(
            "CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('apple'); "
            "INSERT INTO t VALUES ('banana'); SELECT * FROM t WHERE a LIKE 'a%';")
        self.assertHas(p, "apple")

    def test_where_in_literal_list(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); "
            "SELECT * FROM t WHERE a IN (1, 3);")
        self.assertHas(p, "3")

    def test_where_mixed_and_or(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT, c INT); "
            "INSERT INTO t VALUES (1, 2, 0); INSERT INTO t VALUES (1, 9, 0); "
            "INSERT INTO t VALUES (0, 2, 3); INSERT INTO t VALUES (0, 9, 3); "
            "SELECT a, b, c FROM t WHERE a = 1 AND b = 2 OR c = 3 "
            "ORDER BY a, b, c;")
        self.assertHas(p, "1|2|0")
        self.assertHas(p, "0|2|3")

    def test_aggregate_as_alias_header(self):
        p = self.run_stdin(
            self.T2 + "SELECT b AS name, COUNT(*) AS n FROM t GROUP BY b;")
        self.assertHas(p, "|name|n|")

    def test_multi_select_script_renders_all_results(self):
        p = self.run_stdin("SELECT 1; SELECT 2;")
        self.assertHas(p, "|1|")
        self.assertHas(p, "|2|")

    def test_where_not_prefix(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b TEXT); INSERT INTO t VALUES (1, 'x'); "
            "INSERT INTO t VALUES (2, 'y'); INSERT INTO t VALUES (3, 'z'); "
            "SELECT a FROM t WHERE NOT a = 1;")
        self.assertHas(p, "2")
        self.assertNoErr(p)

    def test_bare_identifier_alias_header(self):
        p = self.run_stdin(self.T2 + "SELECT a id FROM t;")
        self.assertHas(p, "|id|")

    def test_multi_row_values_insert(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1), (2), (3); "
            "SELECT COUNT(*) FROM t;")
        self.assertHas(p, "3")

    def test_omitted_pk_auto_fills_rowid(self):
        p = self.run_stdin(
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT); "
            "INSERT INTO t (name) VALUES ('a'), ('b'); "
            "SELECT id FROM t ORDER BY id;")
        self.assertHas(p, "(2 rows)")
        self.assertHas(p, "1 ")
        self.assertHas(p, "2 ")

    def test_group_by_having_no_select_aggregate(self):
        p = self.run_stdin(
            "CREATE TABLE s (g INT, v INT); "
            "INSERT INTO s VALUES (1,10),(1,20),(2,5),(3,100); "
            "SELECT g FROM s GROUP BY g HAVING count > 1;")
        self.assertHas(p, "|1|")
        self.assertNoErr(p)

    def test_where_in_subquery(self):
        p = self.run_stdin(
            "CREATE TABLE t1 (a INT); CREATE TABLE t2 (b INT); "
            "INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); "
            "INSERT INTO t2 VALUES (1); "
            "SELECT * FROM t1 WHERE a IN (SELECT b FROM t2);")
        self.assertHas(p, "1")

    def test_order_by_asc(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (3); "
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); "
            "SELECT * FROM t ORDER BY a;")
        self.assertHas(p, "1")

    def test_order_by_desc(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); "
            "SELECT * FROM t ORDER BY a DESC;")
        self.assertHas(p, "3")

    def test_order_by_multi_column(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 9); "
            "INSERT INTO t VALUES (2, 8); SELECT * FROM t ORDER BY a, b;")
        self.assertHas(p, "9")

    def test_limit(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); "
            "SELECT * FROM t ORDER BY a LIMIT 1;")
        self.assertHas(p, "1")

    def test_limit_offset(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); "
            "SELECT * FROM t ORDER BY a LIMIT 1 OFFSET 2;")
        self.assertHas(p, "3")

    def test_distinct(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); "
            "SELECT DISTINCT a FROM t;")
        self.assertNoErr(p)


class TestAggregates(MagniCLITestCase):
    T3 = ("CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
          "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); ")

    def test_count_star(self):
        p = self.run_stdin(self.T3 + "SELECT COUNT(*) FROM t;")
        self.assertHas(p, "3")

    def test_sum_avg_min_max(self):
        p = self.run_stdin(
            self.T3 + "SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t;")
        self.assertHas(p, "6")

    def test_group_by(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); "
            "INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (1, 30); "
            "SELECT a, SUM(b) FROM t GROUP BY a;")
        self.assertHas(p, "40")

    def test_group_by_having_filters(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 10); "
            "INSERT INTO t VALUES (2, 20); INSERT INTO t VALUES (1, 30); "
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING count > 1;")
        self.assertHas(p, "1 rows")


class TestJoins(MagniCLITestCase):
    def test_inner_join(self):
        p = self.run_stdin(
            "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); "
            "INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); "
            "INSERT INTO t2 VALUES (2); INSERT INTO t2 VALUES (3); "
            "SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;")
        self.assertHas(p, "2")

    def test_left_join_preserves_left_rows(self):
        p = self.run_stdin(
            "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); "
            "INSERT INTO t1 VALUES (1); INSERT INTO t1 VALUES (2); "
            "INSERT INTO t2 VALUES (2); "
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;")
        self.assertHas(p, "1")

    def test_cross_join(self):
        p = self.run_stdin(
            "CREATE TABLE t1 (x INT); CREATE TABLE t2 (y INT); "
            "INSERT INTO t1 VALUES (1); INSERT INTO t2 VALUES (2); "
            "SELECT * FROM t1 CROSS JOIN t2;")
        self.assertHas(p, "2")


class TestSubqueries(MagniCLITestCase):
    def test_from_subquery(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); "
            "SELECT * FROM (SELECT * FROM t WHERE a > 1) AS sub;")
        self.assertHas(p, "2")

    def test_from_subquery_without_alias(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); "
            "SELECT * FROM (SELECT * FROM t WHERE a > 1);")
        self.assertHas(p, "2")

    def test_from_subquery_qualified_projection(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); "
            "INSERT INTO t VALUES (2); "
            "SELECT sub.a FROM (SELECT * FROM t WHERE a > 1) AS sub;")
        self.assertHas(p, "2")

    def test_from_subquery_unknown_qualified_column_errors(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); "
            "SELECT nosuch.a FROM (SELECT * FROM t) AS sub;")
        self.assertIsErr(p)
        self.assertNotEqual(p.returncode, 0)

    def test_malformed_sql_errors_and_fails(self):
        p = self.run_eval("SELECT * FROM t WHERE (a = 1;")
        self.assertIsErr(p)
        self.assertNotEqual(p.returncode, 0)


class TestSetOperations(MagniCLITestCase):
    AB = ("CREATE TABLE a (x INT); INSERT INTO a VALUES (1); "
          "INSERT INTO a VALUES (2); CREATE TABLE b (x INT); "
          "INSERT INTO b VALUES (2); INSERT INTO b VALUES (3); ")

    def test_union(self):
        p = self.run_stdin(self.AB + "SELECT x FROM a UNION SELECT x FROM b;")
        self.assertHas(p, "3")

    def test_union_dedups(self):
        p = self.run_stdin(self.AB + "SELECT x FROM a UNION SELECT x FROM b;")
        self.assertHas(p, "2")

    def test_union_all(self):
        p = self.run_stdin(
            self.AB + "SELECT x FROM a UNION ALL SELECT x FROM b;")
        self.assertHas(p, "3")

    def test_intersect(self):
        p = self.run_stdin(
            self.AB + "SELECT x FROM a INTERSECT SELECT x FROM b;")
        self.assertHas(p, "2")

    def test_except(self):
        p = self.run_stdin(
            self.AB + "SELECT x FROM a EXCEPT SELECT x FROM b;")
        self.assertHas(p, "1")

    def test_literal_union(self):
        p = self.run_stdin("SELECT 1 UNION SELECT 2;")
        self.assertHas(p, "2")


class TestTransactions(MagniCLITestCase):
    def test_begin_commit_persists_writes(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); BEGIN; INSERT INTO t VALUES (42); "
            "COMMIT; SELECT * FROM t;")
        self.assertHas(p, "42")

    def test_begin_rollback_discards_writes(self):
        p = self.run_stdin(
            "CREATE TABLE t (a INT); BEGIN; INSERT INTO t VALUES (99); "
            "ROLLBACK; SELECT * FROM t;")
        self.assertNoErr(p)

    def test_expire_in_txn_warns_and_preserves_uncommitted(self):
        # One --file script = one process = one session, so the txn spans
        # the .expire (separate --eval calls can't share txn state).
        d = self.db()
        p = self.run_file(
            "CREATE TABLE t (a INT);\nBEGIN;\nINSERT INTO t VALUES (7);\n"
            ".expire 1\nCOMMIT;\nSELECT a FROM t;\n", d)
        self.assertHas(p, "transaction active")
        self.assertHas(p, "|7|")


class TestTimeTravel(MagniCLITestCase):
    SETUP = (
        "CREATE TABLE tt (a TEXT);",
        "INSERT INTO tt VALUES ('v1');",
        "INSERT INTO tt VALUES ('v2');",
    )

    def _history_db(self) -> Path:
        d = self.db()
        for s in self.SETUP:
            self.run_eval(s, d)
        return d

    def test_as_of_snapshot_returns_historical_data(self):
        d = self._history_db()
        p = self.run_eval("SELECT * FROM tt AS OF SNAPSHOT 2;", d)
        self.assertHas(p, "v1")

    def test_as_of_snapshot_3_returns_latest_data(self):
        d = self._history_db()
        p = self.run_eval("SELECT * FROM tt AS OF SNAPSHOT 3;", d)
        self.assertHas(p, "v2")

    def test_snapshots_lists_chain(self):
        d = self._history_db()
        p = self.run_eval(".snapshots", d)
        self.assertHas(p, "timestamp")


class TestDotCommands(MagniCLITestCase):
    def _t_db(self) -> Path:
        d = self.db()
        self.run_eval("CREATE TABLE t (x INT); INSERT INTO t VALUES (1);", d)
        return d

    def test_tables_lists_tables(self):
        p = self.run_eval(".tables", self._t_db())
        self.assertHas(p, "|name|")

    def test_schema_shows_ddl(self):
        p = self.run_eval(".schema", self._t_db())
        self.assertNoErr(p)

    def test_version_prints_version(self):
        p = self.run_eval(".version", self._t_db())
        self.assertNoErr(p)

    def test_integrity_check_passes(self):
        p = self.run_eval(".integrity", self._t_db())
        self.assertNoErr(p)

    def test_stats_reports_statistics(self):
        p = self.run_eval(".stats", self._t_db())
        self.assertNoErr(p)

    def test_checkpoint_succeeds(self):
        p = self.run_eval(".checkpoint", self._t_db())
        self.assertNoErr(p)

    def test_help_prints_help(self):
        p = self.run_eval(".help", self._t_db())
        self.assertHas(p, "Commands:")

    def test_desc_describes_table(self):
        p = self.run_eval(".desc t", self._t_db())
        self.assertNoErr(p)

    def test_dump_table(self):
        p = self.run_eval(".dump t", self._t_db())
        self.assertNoErr(p)

    def test_snapdiff_renders_diff_table(self):
        p = self.run_eval(".snapdiff 1 2", self._t_db())
        self.assertHas(p, "change")

    def test_snapdiff_bad_args_prints_table_usage(self):
        p = self.run_eval(".snapdiff 1", self._t_db())
        self.assertHas(p, "Usage: .snapdiff <older_id> <newer_id>")

    def test_expire_in_file_parses_keep(self):
        d = self._t_db()
        p = self.run_file(
            "INSERT INTO t VALUES (2);\n.expire 1\nSELECT COUNT(*) FROM t;\n", d)
        self.assertHas(p, "older than last 1")
        self.assertHas(p, "COUNT(*)")

    def test_expire_negative_keep_clamps_to_default(self):
        d = self._t_db()
        p = self.run_eval(".expire -1", d)
        self.assertHas(p, "invalid; using default")
        q = self.run_eval("SELECT COUNT(*) FROM t;", d)
        self.assertHas(q, "(1 rows)")


class TestEdgeCases(MagniCLITestCase):
    def test_missing_table_error(self):
        p = self.run_eval("SELECT * FROM nonexistent;")
        self.assertIsErr(p)

    def test_insert_into_missing_table_error(self):
        p = self.run_eval("INSERT INTO nonexistent VALUES (1);")
        self.assertIsErr(p)

    def test_check_constraint_rejects_invalid(self):
        p = self.run_eval(
            "CREATE TABLE t (a INT CHECK (a > 0)); INSERT INTO t VALUES (-1);")
        self.assertIsErr(p)

    def test_column_count_mismatch_error(self):
        p = self.run_eval(
            "CREATE TABLE t (x INT); INSERT INTO t VALUES (1, 2);")
        self.assertIsErr(p)

    def test_type_mismatch_in_insert(self):
        p = self.run_eval(
            "CREATE TABLE t (x INT); INSERT INTO t VALUES ('not_a_number');")
        self.assertIsErr(p)


if __name__ == "__main__":
    unittest.main()
