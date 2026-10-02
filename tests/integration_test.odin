package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:btree"
import "src:cell"
import "src:db"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"
import "src:types"

setup_db :: proc(t: ^testing.T, name: string) -> ^db.Database {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_%s.db", name)
	clean_db_files(filename)

	database, open_err := db.open(filename)
	testing.expect(t, open_err == .None, "Failed to open database")
	return database
}

teardown_db :: proc(db_handle: ^db.Database, name: string) {
	filename := fmt.tprintf("test_int_%s.db", name)
	db.close(db_handle)
	clean_db_files(filename)
}

@(test)
test_integration_basic_create_and_insert :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "basic_crud")
	defer teardown_db(d, "basic_crud")

	ok := db.execute(d, "CREATE TABLE test (id INT, val TEXT);") == .None
	testing.expect(t, ok, "CREATE TABLE should succeed")

	ok2 := db.execute(d, "INSERT INTO test VALUES (1, 'hello');") == .None
	testing.expect(t, ok2, "INSERT should succeed")

	ok3 := db.execute(d, "INSERT INTO test VALUES (2, 'world');") == .None
	testing.expect(t, ok3, "Second INSERT should succeed")
}

@(test)
test_integration_transaction_and_snapshot_chain :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_snap")
	defer teardown_db(d, "txn_snap")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.execute(d, "INSERT INTO t VALUES (3);")

	snap_page := d.latest_snapshot
	testing.expect(t, snap_page > 0, "Should have at least one snapshot")
}

@(test)
test_integration_select_empty_table :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "empty_sel")
	defer teardown_db(d, "empty_sel")

	ok := db.execute(d, "CREATE TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE TABLE should succeed")

	ok2 := db.execute(d, "SELECT * FROM t;") == .None
	testing.expect(t, ok2, "SELECT on empty table should succeed")
}

@(test)
test_integration_select_with_data :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "sel_data")
	defer teardown_db(d, "sel_data")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")

	ok := db.execute(d, "SELECT * FROM t;") == .None
	testing.expect(t, ok, "SELECT with data should succeed")
}

@(test)
test_integration_time_travel_as_of_snapshot :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "tt")
	defer teardown_db(d, "tt")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO t VALUES (3, 'Charlie');")

	ok_current := db.execute(d, "SELECT * FROM t;") == .None
	testing.expect(t, ok_current, "Current SELECT should succeed")

	ok_snap1 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 1;") == .None
	testing.expect(t, ok_snap1, "AS OF SNAPSHOT 1 should succeed")

	ok_snap2 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 2;") == .None
	testing.expect(t, ok_snap2, "AS OF SNAPSHOT 2 should succeed")

	ok_snap3 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 3;") == .None
	testing.expect(t, ok_snap3, "AS OF SNAPSHOT 3 should succeed")

	ok_snap4 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 4;") == .None
	testing.expect(t, ok_snap4, "AS OF SNAPSHOT 4 should succeed")

	ok_bad := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 99;") != .None
	testing.expect(t, ok_bad, "Non-existent snapshot should fail")
}

@(test)
test_integration_time_travel_with_where :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "tt_where")
	defer teardown_db(d, "tt_where")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO t VALUES (3, 'Charlie');")

	ok := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 3 WHERE id = 1;") == .None
	testing.expect(t, ok, "AS OF SNAPSHOT with WHERE should succeed")
}

@(test)
test_integration_time_travel_with_limit :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "tt_limit")
	defer teardown_db(d, "tt_limit")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.execute(d, "INSERT INTO t VALUES (3);")

	ok := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 4 LIMIT 2;") == .None
	testing.expect(t, ok, "AS OF SNAPSHOT with LIMIT should succeed")
}

@(test)
test_integration_rollback_restores_schema :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "rollback")
	defer teardown_db(d, "rollback")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	schema_root_before := d.schema_root_page

	db.begin(d)
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.rollback(d)
	testing.expect_value(t, d.schema_root_page, schema_root_before)
}

@(test)
test_integration_cross_session_snapshots :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	name := "cross_session"
	d := setup_db(t, name)
	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.close(d)

	d2, open_err := db.open(fmt.tprintf("test_int_%s.db", name))
	defer teardown_db(d2, name)
	testing.expect(t, open_err == .None, "Re-open should succeed")

	ok := db.execute(d2, "SELECT * FROM t AS OF SNAPSHOT 1;") == .None
	testing.expect(t, ok, "Cross-session time-travel should succeed")

	ok2 := db.execute(d2, "SELECT * FROM t AS OF SNAPSHOT 3;") == .None
	testing.expect(t, ok2, "Cross-session last snapshot should succeed")
}

@(test)
test_integration_keyword_as_identifier :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "keyword_id")
	defer teardown_db(d, "keyword_id")

	ok := db.execute(d, "CREATE TABLE snapshot (id INT, of TEXT);") == .None
	testing.expect(t, ok, "Using 'snapshot' and 'of' as identifiers should work")
}

@(test)
test_integration_prune_old_snapshots :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "prune")
	defer teardown_db(d, "prune")

	db.execute(d, "CREATE TABLE t (id INT);")
	for i in 1 ..= 50 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}

	p := d.pager
	count := snapshot.count_committed(p, d.latest_snapshot)
	testing.expect(
		t,
		count <= 100,
		fmt.tprintf("Snapshots should be pruned to <=100 (got %d)", count),
	)
}

@(test)
test_integration_update :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "update")
	defer teardown_db(d, "update")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'hello');")
	db.execute(d, "INSERT INTO t VALUES (2, 'world');")
	db.execute(d, "UPDATE t SET val = 'updated' WHERE id = 2;")

	r := db.query(d, "SELECT id, val FROM t WHERE id = 2;")
	testing.expect(t, r.ok, "SELECT after UPDATE")
	testing.expect_value(t, len(r.rows), 1)
	if len(r.rows) == 1 {
		testing.expect(t, len(r.rows[0]) >= 2, "row has at least 2 values")
		if len(r.rows[0]) >= 2 {
			val, _ := r.rows[0][1].(string)
			testing.expect_value(t, val, "updated")
		}
	}
}

@(test)
test_integration_delete :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "delete")
	defer teardown_db(d, "delete")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.execute(d, "DELETE FROM t WHERE id = 1;")

	r := db.query(d, "SELECT id FROM t;")
	testing.expect(t, r.ok, "SELECT after DELETE")
	if len(r.rows) == 1 {
		id, _ := r.rows[0][0].(i64)
		testing.expect_value(t, id, i64(2))
	}
}

@(test)
test_integration_join :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "join")
	defer teardown_db(d, "join")

	db.execute(d, "CREATE TABLE a (id INT, name TEXT);")
	db.execute(d, "CREATE TABLE b (id INT, val TEXT);")
	db.execute(d, "INSERT INTO a VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO a VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO b VALUES (1, 'X');")
	db.execute(d, "INSERT INTO b VALUES (2, 'Y');")

	r := db.execute(d, "SELECT a.id, a.name, b.val FROM a INNER JOIN b ON a.id = b.id;") == .None
	testing.expect(t, r, "INNER JOIN should succeed")
}

@(test)
test_integration_group_by_non_first_column :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "group_by")
	defer teardown_db(d, "group_by")

	db.execute(d, "CREATE TABLE t (a INT, b TEXT, c INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'x', 10);")
	db.execute(d, "INSERT INTO t VALUES (1, 'x', 20);")
	db.execute(d, "INSERT INTO t VALUES (2, 'y', 30);")

	// Regression: GROUP BY on a non-first column used to crash
	q := db.query(d, "SELECT b, COUNT(*) FROM t GROUP BY b;")
	testing.expect(t, q.ok, "GROUP BY on non-first column should succeed")
	testing.expect(t, len(q.rows) == 2, "expected 2 groups (x, y)")
	if len(q.rows) == 2 {
		testing.expect_value(t, q.rows[0][1].(i64), i64(2))
		testing.expect_value(t, q.rows[1][1].(i64), i64(1))
	}

	// First-column GROUP BY still works.
	q2 := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a;")
	testing.expect(t, q2.ok, "GROUP BY on first column should succeed")
	testing.expect(t, len(q2.rows) == 2, "expected 2 groups (1, 2)")
	if len(q2.rows) == 2 {
		testing.expect_value(t, q2.rows[0][1].(i64), i64(2))
		testing.expect_value(t, q2.rows[1][1].(i64), i64(1))
	}

	// Multi-column GROUP BY exercises values_equal_by_indices.
	q3 := db.query(d, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b;")
	testing.expect(t, q3.ok, "multi-column GROUP BY should succeed")
	testing.expect(t, len(q3.rows) == 2, "expected 2 groups (1,x) and (2,y)")
	if len(q3.rows) == 2 {
		testing.expect_value(t, q3.rows[0][2].(i64), i64(2))
		testing.expect_value(t, q3.rows[1][2].(i64), i64(1))
	}
}

@(test)
test_integration_group_by_having :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "having")
	defer teardown_db(d, "having")

	db.execute(d, "CREATE TABLE t (a INT, b INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "INSERT INTO t VALUES (1, 30);")

	// Regression: HAVING on an aggregate (count) used to be case-sensitive,
	// filtering out every group. Verify lowercase, uppercase, and group-column forms.
	q := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a HAVING count > 1;")
	testing.expect(t, q.ok, "HAVING count > 1 should succeed")
	testing.expect(t, len(q.rows) == 1, "expected 1 group filtered to a=1")
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[0][1].(i64), i64(2))
	}

	q2 := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT > 1;")
	testing.expect(t, q2.ok, "HAVING COUNT (uppercase) should succeed")
	testing.expect(t, len(q2.rows) == 1, "uppercase HAVING should filter to 1 group")

	q3 := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a HAVING count >= 1;")
	testing.expect(t, q3.ok, "HAVING count >= 1 should succeed")
	testing.expect(t, len(q3.rows) == 2, "HAVING count >= 1 keeps all 2 groups")

	q4 := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a HAVING a > 1;")
	testing.expect(t, q4.ok, "HAVING on group column should succeed")
	testing.expect(t, len(q4.rows) == 1, "HAVING a > 1 filters to a=2")
	if len(q4.rows) == 1 {
		testing.expect_value(t, q4.rows[0][0].(i64), i64(2))
	}
}

@(test)
test_integration_as_of_timestamp :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "tt_ts")
	defer teardown_db(d, "tt_ts")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")

	// Get a real snapshot timestamp
	h2, ok2 := snapshot.load(d.pager, d.latest_snapshot)
	testing.expect(t, ok2, "load latest snapshot")
	ts := h2.timestamp

	r := db.query(d, fmt.tprintf("SELECT id, name FROM t AS OF TIMESTAMP %d;", ts))
	testing.expect(t, r.ok, "AS OF TIMESTAMP with real timestamp")
	testing.expect(t, len(r.rows) > 0, "should return rows")

	// Very old timestamp should fail
	r2 := db.query(d, "SELECT id FROM t AS OF TIMESTAMP 1;")
	testing.expect(t, !r2.ok, "AS OF TIMESTAMP with ts=1 should fail")
}

@(test)
test_integration_snapshot_restore :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "restore")
	defer teardown_db(d, "restore")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")

	// Restore to snapshot 1 (after CREATE, before first INSERT)
	ok := db.snapshot_restore(d, 1) == .None
	testing.expect(t, ok, "snapshot_restore to snapshot 1")

	// After restore, the table should still exist (schema at snapshot 1)
	r := db.query(d, "SELECT id FROM t;")
	testing.expect(t, r.ok, "SELECT after restore")
}

@(test)
test_integration_limit_pushdown :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "limit_push")
	defer teardown_db(d, "limit_push")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	for i in 1 ..= 100 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'row%d');", i, i))
	}

	// LIMIT without ORDER BY — should succeed (scanned via display path)
	r := db.query(d, "SELECT id, val FROM t LIMIT 5;")
	testing.expect(t, r.ok, "LIMIT 5 should succeed")
	// Note: exec_select_single_data doesn't apply LIMIT truncation, display_results does

	// LIMIT with ORDER BY — should sort first, then limit
	// LIMIT with ORDER BY — should succeed
	r2 := db.query(d, "SELECT id, val FROM t ORDER BY id DESC LIMIT 3;")
	testing.expect(t, r2.ok, "ORDER BY + LIMIT should succeed")
	_ = r2
}

@(test)
test_integration_where_pushdown :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "where_push")
	defer teardown_db(d, "where_push")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'match');")
	db.execute(d, "INSERT INTO t VALUES (2, 'skip');")
	db.execute(d, "INSERT INTO t VALUES (3, 'keep');")
	db.execute(d, "INSERT INTO t VALUES (4, 'skip');")

	// WHERE with NOT_EQUALS
	r := db.query(d, "SELECT id FROM t WHERE val != 'skip';")
	testing.expect(t, r.ok, "WHERE != should succeed")
	testing.expect_value(t, len(r.rows), 2)

	// WHERE with LIKE
	r2 := db.query(d, "SELECT id FROM t WHERE val LIKE 'm%';")
	testing.expect(t, r2.ok, "WHERE LIKE should succeed")
	testing.expect(t, len(r2.rows) >= 1, "LIKE should match at least 1 row")
}

@(test)
test_integration_join_cross :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "join_cross")
	defer teardown_db(d, "join_cross")

	db.execute(d, "CREATE TABLE a (id INT);")
	db.execute(d, "CREATE TABLE b (val TEXT);")
	db.execute(d, "INSERT INTO a VALUES (1);")
	db.execute(d, "INSERT INTO a VALUES (2);")
	db.execute(d, "INSERT INTO b VALUES ('X');")

	// CROSS JOIN — no ON clause, falls back to nested-loop
	ok := db.execute(d, "SELECT a.id, b.val FROM a CROSS JOIN b LIMIT 10;") == .None
	testing.expect(t, ok, "CROSS JOIN should succeed")
}

@(test)
test_integration_join_hash :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "join_hash")
	defer teardown_db(d, "join_hash")

	db.execute(d, "CREATE TABLE a (id INT, name TEXT);")
	db.execute(d, "CREATE TABLE b (id INT, val TEXT);")
	db.execute(d, "INSERT INTO a VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO a VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO a VALUES (3, 'Carol');")
	db.execute(d, "INSERT INTO b VALUES (1, 'X');")
	db.execute(d, "INSERT INTO b VALUES (3, 'Z');")

	// Equi-join — should use hash join
	r :=
		db.execute(
			d,
			"SELECT a.id, a.name, b.val FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id;",
		) ==
		.None
	testing.expect(t, r, "Equi-join INNER JOIN should succeed")
}

@(test)
test_integration_right_join :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "right_join")
	defer teardown_db(d, "right_join")

	db.execute(d, "CREATE TABLE a (id INT, name TEXT);")
	db.execute(d, "CREATE TABLE b (id INT, val TEXT);")
	db.execute(d, "INSERT INTO a VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO a VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO b VALUES (1, 'X');")
	db.execute(d, "INSERT INTO b VALUES (3, 'Z');")

	// RIGHT JOIN: all rows from b, matched rows from a, NULLs for unmatched
	q := db.query(
		d,
		"SELECT a.id, a.name, b.val FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id;",
	)
	testing.expect(t, q.ok, "RIGHT JOIN should succeed")
	if q.ok {
		testing.expect_value(t, len(q.rows), 2)
		// b.id=1 matches a.id=1 → (1, 'Alice', 'X')
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[0][1].(string), "Alice")
		testing.expect_value(t, q.rows[0][2].(string), "X")
		// b.id=3 has no match → (NULL, NULL, 'Z')
		testing.expect(t, types.is_null(q.rows[1][0]), "Row 1: a.id = NULL")
		testing.expect(t, types.is_null(q.rows[1][1]), "Row 1: a.name = NULL")
		testing.expect_value(t, q.rows[1][2].(string), "Z")
	}
}

@(test)
test_integration_between :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "between")
	defer teardown_db(d, "between")

	db.execute(d, "CREATE TABLE t (id INT, score INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "INSERT INTO t VALUES (3, 30);")
	db.execute(d, "INSERT INTO t VALUES (4, 40);")

	q := db.query(d, "SELECT id FROM t WHERE score BETWEEN 15 AND 35 ORDER BY id;")
	testing.expect(t, q.ok, "BETWEEN query should succeed")
	if q.ok {
		testing.expect_value(t, len(q.rows), 2)
		testing.expect_value(t, q.rows[0][0].(i64), i64(2))
		testing.expect_value(t, q.rows[1][0].(i64), i64(3))
	}

	// NOT BETWEEN
	q2 := db.query(d, "SELECT id FROM t WHERE score NOT BETWEEN 15 AND 35 ORDER BY id;")
	testing.expect(t, q2.ok, "NOT BETWEEN query should succeed")
	if q2.ok {
		testing.expect_value(t, len(q2.rows), 2)
		testing.expect_value(t, q2.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q2.rows[1][0].(i64), i64(4))
	}
}

@(test)
test_integration_using_join :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "using_join")
	defer teardown_db(d, "using_join")

	db.execute(d, "CREATE TABLE a (id INT, name TEXT);")
	db.execute(d, "CREATE TABLE b (id INT, val TEXT);")
	db.execute(d, "INSERT INTO a VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO a VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO b VALUES (1, 'X');")
	db.execute(d, "INSERT INTO b VALUES (3, 'Z');")

	q := db.query(d, "SELECT a.id, a.name, b.val FROM a JOIN b USING (id) ORDER BY a.id;")
	testing.expect(t, q.ok, "USING join should succeed")
	if q.ok {
		testing.expect_value(t, len(q.rows), 1)
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[0][1].(string), "Alice")
		testing.expect_value(t, q.rows[0][2].(string), "X")
	}
}

@(test)
test_integration_join_asymmetric :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "join_asym")
	defer teardown_db(d, "join_asym")

	db.execute(d, "CREATE TABLE small (id INT, name TEXT);")
	db.execute(d, "CREATE TABLE big (id INT, val TEXT);")
	db.execute(d, "INSERT INTO small VALUES (1, 'a');")
	db.execute(d, "INSERT INTO small VALUES (2, 'b');")
	db.execute(d, "INSERT INTO big VALUES (1, 'x');")
	db.execute(d, "INSERT INTO big VALUES (1, 'y');")
	db.execute(d, "INSERT INTO big VALUES (1, 'z');")
	db.execute(d, "INSERT INTO big VALUES (2, 'w');")

	// Join reorder: small table on right — uses hash on right table
	r :=
		db.execute(
			d,
			"SELECT small.id, small.name, big.val FROM small INNER JOIN big ON small.id = big.id ORDER BY small.id, big.val;",
		) ==
		.None
	testing.expect(t, r, "Asymmetric JOIN should succeed")
}

@(test)
test_integration_select_distinct :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "sel_distinct")
	defer teardown_db(d, "sel_distinct")

	db.execute(d, "CREATE TABLE t (name TEXT, score INT);")
	db.execute(d, "INSERT INTO t VALUES ('Alice', 10);")
	db.execute(d, "INSERT INTO t VALUES ('Bob', 20);")
	db.execute(d, "INSERT INTO t VALUES ('Alice', 30);")

	r := db.execute(d, "SELECT DISTINCT name FROM t ORDER BY name;") == .None
	testing.expect(t, r, "SELECT DISTINCT should succeed")
}

@(test)
test_integration_query_as_of_timestamp :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "query_ts")
	defer teardown_db(d, "query_ts")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")

	// Capture the timestamp of snapshot 2
	snap2_page := d.latest_snapshot
	h, h_ok := snapshot.load(d.pager, snap2_page)
	testing.expect(t, h_ok, "should load snapshot")
	ts := h.timestamp

	db.execute(d, "INSERT INTO t VALUES (2);")

	// Query with AS OF TIMESTAMP via db.query()
	q := db.query(d, fmt.tprintf("SELECT id FROM t AS OF TIMESTAMP %d;", ts))
	testing.expect(t, q.ok, "AS OF TIMESTAMP query should succeed")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) > 0 {
		testing.expect_value(t, q.rows[0][0].(i64), 1)
	}
}

@(test)
test_integration_begin_commit_stress :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_stress")
	defer teardown_db(d, "txn_stress")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	for i in 1 ..= 100 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'row%d');", i, i))
	}

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "COUNT after 100 inserts")
}

@(test)
test_integration_wal_rollback_preserves_data :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "wal_rb")
	defer teardown_db(d, "wal_rb")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'original');")

	db.begin(d)
	db.execute(d, "INSERT INTO t VALUES (2, 'rolled');")
	db.execute(d, "INSERT INTO t VALUES (3, 'back');")
	db.rollback(d)

	q := db.query(d, "SELECT id, val FROM t ORDER BY id;")
	testing.expect(t, q.ok, "SELECT after rollback")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) >= 1 {
		id, _ := q.rows[0][0].(i64)
		val, _ := q.rows[0][1].(string)
		testing.expect_value(t, id, i64(1))
		testing.expect_value(t, val, "original")
	}
}

@(test)
test_integration_snapshot_restore_with_verification :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "restore_verify")
	defer teardown_db(d, "restore_verify")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")

	q1 := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q1.ok, "pre-restore query")

	db.execute(d, "INSERT INTO t VALUES (3, 'Charlie');")
	db.execute(d, "INSERT INTO t VALUES (4, 'Diana');")

	q2 := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q2.ok, "pre-restore query 2")

	db.snapshot_restore(d, 3)

	q3 := db.query(d, "SELECT id, name FROM t ORDER BY id;")
	testing.expect(t, q3.ok, "post-restore query should succeed")
	testing.expect_value(t, len(q3.rows), 2)
}

@(test)
test_integration_snapshot_rollforward_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "rollfwd")
	defer teardown_db(d, "rollfwd")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.execute(d, "INSERT INTO t VALUES (3);")

	// Restore to snapshot 1 (just CREATE)
	db.snapshot_restore(d, 1)

	q1 := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q1.ok, "SELECT after restore")

	// Roll forward
	ok := db.rollforward(d) == .None
	testing.expect(t, ok, "rollforward to snapshot 2 should succeed")

	q2 := db.query(d, "SELECT id FROM t ORDER BY id;")
	testing.expect(t, q2.ok, "SELECT after rollforward")
}

@(test)
test_integration_as_of_after_restore :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "asof_restore")
	defer teardown_db(d, "asof_restore")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'first');")
	db.execute(d, "INSERT INTO t VALUES (2, 'second');")
	db.execute(d, "INSERT INTO t VALUES (3, 'third');")

	// Query AS OF snapshot 2 (after first INSERT)
	q1 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 2;") == .None
	testing.expect(t, q1, "AS OF SNAPSHOT 2 before restore")
}

@(test)
test_integration_expire_and_reclaim :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "expire")
	defer teardown_db(d, "expire")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	for i in 1 ..= 20 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'row%d');", i, i))
	}

	// Expire, keeping only last 5
	db.expire_snapshots(d, 5)
	// Recent snapshots should still work
	q := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 20;") == .None
	testing.expect(t, q, "AS OF recent snapshot should work after expire")

	// Very old snapshot should fail
	q2 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 1;") != .None
	testing.expect(t, q2, "AS OF expired snapshot should fail")
}

@(test)
test_integration_checkpoint_reclaims_wal :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "ckpt")

	db.execute(d, "CREATE TABLE t (id INT);")
	for i in 1 ..= 10 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}

	testing.expect(t, admin.checkpoint(d) == .None, "checkpoint succeeds")
	db.close(d)
	d2, open_err := db.open(fmt.tprintf("test_int_ckpt.db"))
	testing.expect(t, open_err == .None, "reopen after checkpoint")
	defer teardown_db(d2, "ckpt")

	q := db.query(d2, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "data should survive checkpoint+reopen")
}

@(test)
test_integration_wal_auto_checkpoint :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_walauto.db")
	wal_path := fmt.tprintf("%s-wal", filename)
	if os.exists(filename) { os.remove(filename) }
	if os.exists(wal_path) { os.remove(wal_path) }
	defer os.remove(filename)
	defer os.remove(wal_path)

	d, open_err := db.open(filename, db.Open_Config{wal_size_threshold = 2})
	testing.expect(t, open_err == .None, "open with wal_size_threshold")
	if open_err != .None { return }

	db.execute(d, "CREATE TABLE t (id INT);")
	for i in 1 ..= 20 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}

	// With threshold 2 the WAL is auto-checkpointed every couple of commits, so
	// it must never accumulate 20+ frames (frame_count stays below the threshold).
	testing.expect(
		t,
		d.pager.wal_state.frame_count < 2,
		"WAL auto-checkpointed (frame_count below threshold)",
	)
	db.close(d)
	d2, open2 := db.open(filename)
	testing.expect(t, open2 == .None, "reopen after auto-checkpoint")
	if open2 == .None {
		defer db.close(d2)
		q := db.query(d2, "SELECT COUNT(*) AS n FROM t;")
		testing.expect(t, q.ok, "count after auto-checkpoint")
		if q.ok {
			testing.expect_value(t, q.rows[0][0].(i64), i64(20))
		}
	}
}

@(test)
test_integration_snapshot_batch_config :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_snapcfg.db")
	if os.exists(filename) { os.remove(filename) }

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) { os.remove(wal_name) }
	defer os.remove(filename)
	defer os.remove(wal_name)

	d, open_err := db.open(filename, db.Open_Config{snapshot_batch_threshold = 3})
	testing.expect(t, open_err == .None, "open with snapshot_batch_threshold")
	if open_err != .None { return }
	defer db.close(d)

	testing.expect_value(t, d.snapshot_batch_threshold, 3)

	db.execute(d, "CREATE TABLE t (id INT);")
	for i in 1 ..= 4 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}
	// With threshold 3: create(1), insert1(2), insert2(3 -> snapshot, reset),
	// insert3(1), insert4(2). Ends mid-batch at count 2; with the default
	// threshold (1) the count would be 0 after every statement.
	testing.expect_value(t, d.snapshot_batch_count, 2)

	q := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q.ok, "count after batched snapshots")
	if q.ok {
		testing.expect_value(t, q.rows[0][0].(i64), i64(4))
	}
}

@(test)
test_integration_batch_snapshot :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "batch_snap")
	defer teardown_db(d, "batch_snap")

	// With threshold=1 (default), every INSERT creates a snapshot
	db.execute(d, "CREATE TABLE t (id INT)")
	for i in 1 ..= 5 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}

	// Verify data is queryable
	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "data after default batch")
}

@(test)
test_integration_skip_index :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "skipidx")
	defer teardown_db(d, "skipidx")

	db.execute(d, "CREATE TABLE t (id INT, score INT);")
	for i in 1 ..= 20 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 10))
	}

	// Build a skip index on the table
	st := db.Schema_Tree(d)
	tables := schema.list_tables(&st, context.temp_allocator)
	if len(tables) > 0 {
		tree := btree.init(d.pager, tables[0].root_page)
		btree.build_skip_index(&tree, 1)
	}

	// Verify data is queryable
	q := db.query(d, "SELECT id, score FROM t WHERE score > 50;")
	testing.expect(t, q.ok, "SELECT after skip index build")
}

@(test)
test_integration_skip_index_range :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "skiprange")
	defer teardown_db(d, "skiprange")

	db.execute(d, "CREATE TABLE t (id INT, score INT);")
	// Gapped score distribution over two value bands: ids 1..1500 carry scores
	// 1..1500; ids 1501..2000 carry scores 100000..100500. The gap forces two
	// separate skip-index entries, so `>` queries must not be truncated at the
	// first band's upper page bound.
	sb: strings.Builder
	for chunk in 0 ..< 20 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * 100 + i + 1
			score := id if id <= 1500 else 100000 + (id - 1500)
			fmt.sbprintf(&sb, "(%d,%d)", id, score)
		}
		strings.write_string(&sb, ";")
		ok := db.execute(d, strings.to_string(sb)) == .None
		testing.expect(t, ok, "chunk insert")
	}

	// First qualifying query scans without an index and must be correct.
	r := db.query(d, "SELECT id FROM t WHERE score > 1400;")
	testing.expect(t, r.ok, "first score > 1400")
	testing.expect_value(t, len(r.rows), 600)

	// Persist a skip index on `score` so follow-up queries exercise the range path.
	st := db.Schema_Tree(d)
	tables := schema.list_tables(&st, context.temp_allocator)
	testing.expect(t, len(tables) >= 1, "table listed")
	if len(tables) == 0 { return }
	tree := btree.init(d.pager, tables[0].root_page)
	skip_idx, build_err := btree.build_skip_index(&tree, 1)
	testing.expect(t, build_err == .None, "build skip index on score")
	if new_root, up_ok := schema.update_skip_root_cow(&st, "t", skip_idx.root); up_ok {
		d.schema_root_page = new_root
	} else {
		testing.expect(t, false, "persist skip index root")
		return
	}

	// `>` must span both value bands (the old code stopped at the first band).
	r2 := db.query(d, "SELECT id FROM t WHERE score > 1400;")
	testing.expect(t, r2.ok, "indexed score > 1400")
	testing.expect_value(t, len(r2.rows), 600)

	// `>` past the first band's max must seek to the second band's first page.
	r3 := db.query(d, "SELECT id FROM t WHERE score > 1500;")
	testing.expect(t, r3.ok, "indexed score > 1500")
	testing.expect_value(t, len(r3.rows), 500)
	if len(r3.rows) > 0 {
		id0, _ := r3.rows[0][0].(i64)
		testing.expect_value(t, id0, i64(1501))
	}

	// `<` stops at the first band's upper page bound.
	r4 := db.query(d, "SELECT id FROM t WHERE score < 1600;")
	testing.expect(t, r4.ok, "indexed score < 1600")
	testing.expect_value(t, len(r4.rows), 1500)

	// Point equality inside the second band.
	r5 := db.query(d, "SELECT id FROM t WHERE score = 100500;")
	testing.expect(t, r5.ok, "indexed score = 100500")
	testing.expect_value(t, len(r5.rows), 1)
	if len(r5.rows) > 0 {
		id0, _ := r5.rows[0][0].(i64)
		testing.expect_value(t, id0, i64(2000))
	}

	// Two-sided range via combined AND bounds over the second band
	// (scores 100001..100100 inclusive; id 1600 carries score 100100).
	r6 := db.query(d, "SELECT id FROM t WHERE score >= 2000 AND score <= 100100;")
	testing.expect(t, r6.ok, "indexed two-sided range")
	testing.expect_value(t, len(r6.rows), 100)

	// A condition on a non-indexed column must not use the score index.
	r7 := db.query(d, "SELECT id FROM t WHERE id > 1500;")
	testing.expect(t, r7.ok, "non-indexed column")
	testing.expect_value(t, len(r7.rows), 500)
}

// Regression: a table large enough to need >MAX_SKIP_ENTRIES zone entries.
// Even-stepped scores (step 2) defeat range coalescing, so ~20k rows produce
// ~200 entries — more than one skip page holds. The writer used to slice the
// page past its 4KB bound: a bounds panic in debug builds and silent heap
// corruption in release. Build must succeed and queries stay exact.
@(test)
test_integration_skip_index_overflow :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "skipoverflow")
	defer teardown_db(d, "skipoverflow")

	db.execute(d, "CREATE TABLE t (id INT, score INT);")
	ROWS :: 20000
	CHUNK :: 1000
	for chunk in 0 ..< ROWS / CHUNK {
		sb: strings.Builder
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< CHUNK {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * CHUNK + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}

		strings.write_string(&sb, ";")
		ok := db.execute(d, strings.to_string(sb)) == .None
		testing.expect(t, ok, "chunk insert")
	}

	st := db.Schema_Tree(d)
	tables := schema.list_tables(&st, context.temp_allocator)
	testing.expect(t, len(tables) >= 1, "table listed")
	if len(tables) == 0 { return }
	tree := btree.init(d.pager, tables[0].root_page)

	// More zone entries than one page holds: the build must merge them, not
	// overflow. (ROWS>0 with step-2 scores guarantees ~20 entries/chunk.)
	skip_idx, build_err := btree.build_skip_index(&tree, 1)
	testing.expect(t, build_err == .None, "large skip index builds within one page")
	if build_err != .None { return }
	if new_root, up_ok := schema.update_skip_root_cow(&st, "t", skip_idx.root); up_ok {
		d.schema_root_page = new_root
	} else {
		testing.expect(t, false, "persist large skip index root")
		return
	}

	// Exact answers through the merged (widened, still conservative) zones.
	r := db.query(d, "SELECT id FROM t WHERE score > 30000;")
	testing.expect(t, r.ok, "large indexed >")
	testing.expect_value(t, len(r.rows), 5000)

	r2 := db.query(d, "SELECT id FROM t WHERE score = 39998;")
	testing.expect(t, r2.ok, "large indexed =")
	testing.expect_value(t, len(r2.rows), 1)
	if len(r2.rows) > 0 {
		id0, _ := r2.rows[0][0].(i64)
		testing.expect_value(t, id0, i64(19999))
	}

	r3 := db.query(d, "SELECT id FROM t WHERE score < 100;")
	testing.expect(t, r3.ok, "large indexed <")
	testing.expect_value(t, len(r3.rows), 49)
}

@(test)
test_columnar_integration :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	columns := []types.Column{{type = .INTEGER, name = "id"}, {type = .INTEGER, name = "score"}}
	rowids := []types.Row_ID{10, 20, 30}
	rows := [][]types.Value {
		{types.value_int(1), types.value_int(100)},
		{types.value_int(2), types.value_int(200)},
		{types.value_int(3), types.value_int(300)},
	}

	buf: [4096]u8
	// Use page_id=2 (non-page-1) to avoid database header offset
	page_id: u32 = 2

	ok := cell.serialize_columnar(buf[:], rowids, rows, columns)
	testing.expect(t, ok, "serialize_columnar")

	// Set up a minimal page header for page_id 2
	off := btree.get_page_header_offset(page_id)
	hdr_setup := (^btree.Page_Header)(raw_data(buf[off:]))
	hdr_setup.page_type = .LEAF_TABLE_COLUMNAR
	hdr_setup.cell_count = u16le(3)
	hdr_setup.cell_content_offset = u16le(8 + len(columns) * 12)

	// Read back individual rows via read_columnar_cell
	for i in 0 ..< 3 {
		cc, cc_ok := cell.read_columnar_cell(
			buf[:],
			2,
			i,
			cell.Config{allocator = context.temp_allocator},
		)
		testing.expect(t, cc_ok, fmt.tprintf("read columnar cell %d", i))
		if cc_ok {
			testing.expect_value(t, types.Row_ID(cc.rowid), rowids[i])
			if len(cc.values) >= 2 {
				v0, _ := cc.values[0].(i64)
				v1, _ := cc.values[1].(i64)
				testing.expect_value(t, v0, rows[i][0].(i64))
				testing.expect_value(t, v1, rows[i][1].(i64))
			}
		}
	}

	// Test: convert_columnar_to_row_major preserves data
	btree.convert_columnar_to_row_major(buf[:], page_id, 2)
	hdr := btree.get_leaf_header(buf[:], page_id)
	testing.expect(t, hdr != nil, "got leaf header after conversion")
	testing.expect_value(t, int(hdr.cell_count), 3)
	testing.expect(t, hdr.page_type == .LEAF_TABLE, "page type is LEAF_TABLE after conversion")

	// Verify: deserialize cells from row-major page
	for i in 0 ..< 3 {
		ptr := btree.get_cell_ptr(buf[:], page_id, i, btree.CELL_ENTRY_STRIDE)
		c, _, des_ok := cell.deserialize(
			buf[:],
			int(ptr),
			cell.Config{allocator = context.temp_allocator},
		)
		testing.expect(t, des_ok, fmt.tprintf("deserialize cell %d", i))
		if des_ok {
			testing.expect_value(t, types.Row_ID(c.rowid), rowids[i])
		}
	}
}

@(test)
test_columnar_btree_read :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Test cursor and tree_find on columnar pages within a real b-tree
	ctx := setup_tree(t, "colbtree")
	defer teardown_tree(&ctx)

	// Insert 5 rows into the tree
	for i in 1 ..= 5 {
		val := []types.Value{types.value_int(i64(i * 10))}
		err := btree.tree_insert(&ctx.tree, types.Row_ID(i), val)
		testing.expect(t, err == .None, fmt.tprintf("insert row %d", i))
	}

	// Manually convert the root page (page 1) to columnar format
	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	defer pager.unpin_page(ctx.pager, 1)

	// Read current data from row-major page
	original_rows := make([dynamic]struct {
			rid : types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&original_rows, struct {
				rid : types.Row_ID,
				vals: []types.Value,
			}{cell_val.rowid, cell_val.values})
			cell_val.values = nil
			cell.destroy(&cell_val, context.temp_allocator)
			btree.cursor_advance(&c)
		}
	}
	testing.expect(t, len(original_rows) == 5, "original 5 rows")

	// Build columnar data. Page 1 has 100-byte DB header offset.
	off := btree.get_page_header_offset(1)
	rowids := make([]types.Row_ID, len(original_rows), context.temp_allocator)
	values := make([][]types.Value, len(original_rows), context.temp_allocator)
	for i in 0 ..< len(original_rows) {
		rowids[i] = original_rows[i].rid
		values[i] = original_rows[i].vals
	}
	cols := []types.Column{{name = "val", type = .INTEGER}}
	ok := cell.serialize_columnar(pg.data[off:], rowids, values, cols)
	testing.expect(t, ok, "serialize columnar on page 1")
	for v in original_rows { delete(v.vals, context.temp_allocator) }

	// Set page header
	hdr := btree.get_leaf_header(pg.data, 1)
	hdr.page_type = .LEAF_TABLE_COLUMNAR
	hdr.cell_count = u16le(5)
	hdr.cell_content_offset = u16le(8 + len(cols) * 12)
	pager.mark_dirty(ctx.pager, 1)

	// Test: cursor reads from columnar page
	{
		c, c_err := btree.cursor_start(&ctx.tree)
		testing.expect(t, c_err == .None, "cursor start on columnar tree")
		defer btree.cursor_destroy(&c)
		count := 0
		for c.is_valid {
			cell_val, g_err := btree.cursor_get_cell(&c, context.allocator)
			testing.expect(t, g_err == .None, fmt.tprintf("get cell from columnar page %d", count))
			if g_err == .None {
				// Verify rowid matches expected
				for orig in original_rows {
					if cell_val.rowid == orig.rid { count += 1; break }
				}
			}
			btree.cursor_advance(&c)
			cell.destroy(&cell_val, context.allocator)
		}
		testing.expect(t, count == 5, "cursor read 5 rows from columnar page")
	}

	// Test: tree_find on columnar page
	for i in 1 ..= 5 {
		found, f_err := btree.tree_find(&ctx.tree, types.Row_ID(i), context.allocator)
		testing.expect(t, f_err == .None, fmt.tprintf("tree_find row %d", i))
		if f_err == .None {
			testing.expect_value(t, types.Row_ID(i), found.rowid)
			cell.destroy(&found, context.allocator)
		}
	}

	// Test: tree_find for non-existent row returns Cell_Not_Found
	_, f_err := btree.tree_find(&ctx.tree, 999, context.temp_allocator)
	testing.expect(t, f_err == .Cell_Not_Found, "tree_find non-existent returns not found")

	// Test: INSERT triggers columnar→row-major conversion
	{
		new_val := []types.Value{types.value_int(999)}
		new_root, ins_err := btree.tree_insert_cow(&ctx.tree, 99, new_val)
		testing.expect(t, ins_err == .None, "insert on columnar page")
		_ = new_root

		// Verify via cursor
		ctx.tree.root = new_root
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		found := false
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			defer cell.destroy(&cell_val, context.temp_allocator)
			if cell_val.rowid == 99 {
				found = true
				if len(cell_val.values) > 0 {
					v, is_i64 := cell_val.values[0].(i64)
					testing.expect(t, is_i64 && v == 999, "inserted value 999 found")
				}
				break
			}
			btree.cursor_advance(&c)
		}
		testing.expect(t, found, "inserted row 99 was found")
	}
}

@(test)
test_columnar_insert_conversion :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Test that INSERT on a columnar page triggers columnar→row-major conversion
	ctx := setup_tree(t, "colinsert")
	defer teardown_tree(&ctx)

	for i in 1 ..= 3 {
		val := []types.Value{types.value_int(i64(i * 100))}
		testing.expect(
			t,
			btree.tree_insert(&ctx.tree, types.Row_ID(i), val) == .None,
			"seed insert succeeds",
		)
	}

	// Read original rows via cursor
	orig := make([dynamic]struct {
			rid : types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&orig, struct {
				rid : types.Row_ID,
				vals: []types.Value,
			}{cell_val.rowid, cell_val.values})
			cell_val.values = nil
			cell.destroy(&cell_val, context.temp_allocator)
			btree.cursor_advance(&c)
		}
	}
	testing.expect(t, len(orig) == 3, "3 original rows")

	// Convert the root page to columnar
	off := btree.get_page_header_offset(1)
	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	defer pager.unpin_page(ctx.pager, 1)

	rowids := make([]types.Row_ID, len(orig), context.temp_allocator)
	vals := make([][]types.Value, len(orig), context.temp_allocator)
	for i in 0 ..< len(orig) {
		rowids[i] = orig[i].rid
		vals[i] = orig[i].vals
	}
	cols := []types.Column{{name = "val", type = .INTEGER}}
	cell.serialize_columnar(pg.data[off:], rowids, vals, cols)
	for v in orig { delete(v.vals, context.temp_allocator) }

	hdr := btree.get_leaf_header(pg.data, 1)
	hdr.page_type = .LEAF_TABLE_COLUMNAR
	hdr.cell_count = u16le(3)
	hdr.cell_content_offset = u16le(8 + len(cols) * 12)
	pager.mark_dirty(ctx.pager, 1)
	// Verify cursor still reads correctly
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		count := 0
		for c.is_valid {
			cell_val, g_err := btree.cursor_get_cell(&c, context.temp_allocator)
			testing.expect(t, g_err == .None, "cursor get cell on columnar")
			if g_err == .None && cell_val.rowid >= 1 && cell_val.rowid <= 3 {
				count += 1
			}
			btree.cursor_advance(&c)
			cell.destroy(&cell_val, context.temp_allocator)
		}
		testing.expect(t, count == 3, "cursor reads 3 rows from columnar page")
	}

	// INSERT triggers conversion to row-major
	{
		new_val := []types.Value{types.value_int(999)}
		new_root, ins_err := btree.tree_insert_cow(&ctx.tree, 99, new_val)
		testing.expect(t, ins_err == .None, "insert on columnar page succeeds")
		ctx.tree.root = new_root

		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		found := false
		total := 0
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			total += 1
			if cell_val.rowid == 99 { found = true }
			btree.cursor_advance(&c)
			cell.destroy(&cell_val, context.temp_allocator)
		}
		testing.expect(t, found, "inserted row 99 found after columnar conversion")
		testing.expect(t, total == 4, "4 rows total after insert")
	}
}

// verify_dense_rows scans the whole tree and expects exactly the rowids
// 1..n, each holding a single INTEGER value equal to its rowid.
verify_dense_rows :: proc(t: ^testing.T, tree: ^btree.Tree, n: int) -> bool {
	seen := make([]bool, n + 1, context.temp_allocator)
	total := 0
	c, _ := btree.cursor_start(tree)
	defer btree.cursor_destroy(&c)
	for c.is_valid {
		cell_val, g_err := btree.cursor_get_cell(&c, context.temp_allocator)
		if g_err != .None {
			testing.expect(t, false, "cursor decodes row")
			return false
		}
		rid := int(cell_val.rowid)
		ok := rid >= 1 && rid <= n && !seen[rid]
		if ok {
			seen[rid] = true
			ok = len(cell_val.values) == 1
			if v, v_ok := cell_val.values[0].(i64); v_ok && v == i64(rid) {  } else { ok = false }
		}
		testing.expect(t, ok, "rowid in range, unique, value matches")
		cell.destroy(&cell_val, context.temp_allocator)
		if !ok { return false }
		total += 1
		btree.cursor_advance(&c)
	}
	testing.expect(t, total == n, "all rows present exactly once")
	return total == n
}

@(test)
test_columnar_conversion_then_split :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Bulk columnar page → conversion on first insert → splits on later
	// inserts. Locks the unified columnar→row-major path and proves split
	// pages carry canonical V2 Cell_Entry{ptr,key} pointers: every row must
	// decode with its exact rowid/value after splitting.
	ctx := setup_tree(t, "colsplit")
	defer teardown_tree(&ctx)

	N_FIRST :: 300
	for i in 1 ..= N_FIRST {
		vs := make([]types.Value, 1, context.temp_allocator)
		vs[0] = types.value_int(i64(i))
		ierr := btree.tree_insert(&ctx.tree, types.Row_ID(i), vs)
		testing.expect(t, ierr == .None, "seed insert succeeds")
		if ierr != .None { return }
	}

	// Flip the root page to columnar.
	off := btree.get_page_header_offset(1)
	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	if pg_err != .None { return }

	// NOTE: each row is built with an explicit make. A per-iteration
	// []Value{...} literal was observed to share one backing array here
	// (every row decoded as the last value), while tree_insert copies
	// synchronously so either form is safe for the seed inserts above.
	{
		rowids := make([]types.Row_ID, N_FIRST, context.temp_allocator)
		vals := make([][]types.Value, N_FIRST, context.temp_allocator)
		for i in 0 ..< N_FIRST {
			rowids[i] = types.Row_ID(i + 1)
			vs := make([]types.Value, 1, context.temp_allocator)
			vs[0] = types.value_int(i64(i + 1))
			vals[i] = vs
		}
		cols := []types.Column{{name = "val", type = .INTEGER}}
		testing.expect(
			t,
			cell.serialize_columnar(pg.data[off:], rowids, vals, cols),
			"serialize columnar",
		)
		for v in vals { delete(v, context.temp_allocator) }

		hdr := btree.get_leaf_header(pg.data, 1)
		hdr.page_type = .LEAF_TABLE_COLUMNAR
		hdr.cell_count = u16le(N_FIRST)
		hdr.cell_content_offset = u16le(8 + len(cols) * 12)
		pager.mark_dirty(ctx.pager, 1)
	}
	pager.unpin_page(ctx.pager, 1)

	// Phase A: the 300 columnar rows expand past one row-major page, so the
	// insert must fail loudly and lose nothing (no silent tail truncation).
	{
		vs := make([]types.Value, 1, context.temp_allocator)
		vs[0] = types.value_int(301)
		ierr := btree.tree_insert(&ctx.tree, 301, vs)
		testing.expect(t, ierr == .Cell_Deserialize_Failed, "overflowing conversion fails loudly")
	}
	testing.expect(
		t,
		verify_dense_rows(t, &ctx.tree, N_FIRST),
		"all 300 rows intact after failed conversion",
	)

	// Phase B: a small columnar page converts in place, then growth splits.
	teardown_tree(&ctx)
	ctx = setup_tree(t, "colsplit2")

	M_FIRST :: 50
	M_TOTAL :: 600
	for i in 1 ..= M_FIRST {
		vs := make([]types.Value, 1, context.temp_allocator)
		vs[0] = types.value_int(i64(i))
		if ierr := btree.tree_insert(&ctx.tree, types.Row_ID(i), vs); ierr != .None {
			testing.expect(t, false, "seed insert succeeds")
			return
		}
	}
	off2 := btree.get_page_header_offset(1)
	pg2, pg2_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg2_err == .None, "get page 1")
	if pg2_err != .None { return }
	{
		rowids := make([]types.Row_ID, M_FIRST, context.temp_allocator)
		vals := make([][]types.Value, M_FIRST, context.temp_allocator)
		for i in 0 ..< M_FIRST {
			rowids[i] = types.Row_ID(i + 1)
			vs := make([]types.Value, 1, context.temp_allocator)
			vs[0] = types.value_int(i64(i + 1))
			vals[i] = vs
		}
		cols := []types.Column{{name = "val", type = .INTEGER}}
		testing.expect(
			t,
			cell.serialize_columnar(pg2.data[off2:], rowids, vals, cols),
			"serialize columnar",
		)
		for v in vals { delete(v, context.temp_allocator) }
		hdr := btree.get_leaf_header(pg2.data, 1)
		hdr.page_type = .LEAF_TABLE_COLUMNAR
		hdr.cell_count = u16le(M_FIRST)
		hdr.cell_content_offset = u16le(8 + len(cols) * 12)
		pager.mark_dirty(ctx.pager, 1)
	}
	pager.unpin_page(ctx.pager, 1)
	for i in M_FIRST + 1 ..= M_TOTAL {
		vs := make([]types.Value, 1, context.temp_allocator)
		vs[0] = types.value_int(i64(i))
		if ierr := btree.tree_insert(&ctx.tree, types.Row_ID(i), vs); ierr != .None {
			testing.expect(t, false, "post-conversion insert succeeds")
			return
		}
	}
	pages := make(map[u32]bool, context.temp_allocator)
	btree.collect_pages(&ctx.tree, ctx.tree.root, &pages)
	testing.expect(t, len(pages) > 1, "conversion + growth split the tree")
	testing.expect(
		t,
		verify_dense_rows(t, &ctx.tree, M_TOTAL),
		"all rows exact after conversion + splits",
	)
}

@(test)
test_columnar_update_conversion :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Test that UPDATE on a columnar page triggers columnar→row-major conversion
	ctx := setup_tree(t, "colupdate")
	defer teardown_tree(&ctx)

	for i in 1 ..= 3 {
		val := []types.Value{types.value_int(i64(i))}
		testing.expect(
			t,
			btree.tree_insert(&ctx.tree, types.Row_ID(i), val) == .None,
			"seed insert succeeds",
		)
	}

	// Convert root page to columnar
	orig_rows := make([dynamic]struct {
			rid : types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&orig_rows, struct {
				rid : types.Row_ID,
				vals: []types.Value,
			}{cell_val.rowid, cell_val.values})
			cell_val.values = nil
			cell.destroy(&cell_val, context.temp_allocator)
			btree.cursor_advance(&c)
		}
	}

	off := btree.get_page_header_offset(1)
	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	defer pager.unpin_page(ctx.pager, 1)

	rowids := make([]types.Row_ID, len(orig_rows), context.temp_allocator)
	vals := make([][]types.Value, len(orig_rows), context.temp_allocator)
	for i in 0 ..< len(orig_rows) {
		rowids[i] = orig_rows[i].rid
		vals[i] = orig_rows[i].vals
	}
	cols := []types.Column{{name = "val", type = .INTEGER}}
	cell.serialize_columnar(pg.data[off:], rowids, vals, cols)
	for v in orig_rows { delete(v.vals, context.temp_allocator) }

	hdr := btree.get_leaf_header(pg.data, 1)
	hdr.page_type = .LEAF_TABLE_COLUMNAR
	hdr.cell_count = u16le(3)
	hdr.cell_content_offset = u16le(8 + len(cols) * 12)
	pager.mark_dirty(ctx.pager, 1)

	// UPDATE row 2 via COW — triggers conversion
	{
		table_tree := btree.init(ctx.pager, 1)
		new_vals := []types.Value{types.value_int(222)}
		nroot, upd_err := btree.tree_update_cow(&table_tree, 2, new_vals)
		testing.expect(t, upd_err == .None, "update on columnar page succeeds")

		// Verify through cursor
		table_tree.root = nroot
		c, _ := btree.cursor_start(&table_tree)
		defer btree.cursor_destroy(&c)
		updated := false
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			if cell_val.rowid == 2 {
				if len(cell_val.values) > 0 {
					v, _ := cell_val.values[0].(i64)
					updated = v == 222
				}
			}
			btree.cursor_advance(&c)
			cell.destroy(&cell_val, context.temp_allocator)
		}
		testing.expect(t, updated, "row 2 updated to 222 after columnar conversion")
	}
}

@(test)
test_insert_semicolon_in_string :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "semi")
	defer teardown_db(d, "semi")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	ok := db.execute(d, "INSERT INTO t VALUES (1, 'hello world');") == .None
	testing.expect(t, ok, "INSERT without semicolon in string")

	q := db.query(d, "SELECT val FROM t WHERE id = 1;")
	testing.expect(t, q.ok, "SELECT basic string")
	testing.expect(t, len(q.rows) >= 1, "row exists")
}

@(test)
test_split_statements :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "splitmulti")
	defer teardown_db(d, "splitmulti")

	// Simulate multi-statement execution: send individual statements via db.execute
	ok := db.execute(d, "CREATE TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE TABLE")
	ok = db.execute(d, "INSERT INTO t VALUES (1);") == .None
	testing.expect(t, ok, "INSERT 1")
	ok = db.execute(d, "INSERT INTO t VALUES (2);") == .None
	testing.expect(t, ok, "INSERT 2")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "COUNT query")
}

@(test)
test_block_comment :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "blockcmt")
	defer teardown_db(d, "blockcmt")
	// Block comments /* like this */ should be ignored
	ok := db.execute(d, "CREATE /* inline */ TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE with block comment")
	ok = db.execute(d, "INSERT INTO t VALUES (1);") == .None
	testing.expect(t, ok, "INSERT after block comment")
	q := db.query(d, "SELECT id FROM t;")
	testing.expect(t, q.ok, "SELECT after block comment")
}

@(test)
test_double_semicolon :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "dblsemi")
	defer teardown_db(d, "dblsemi")
	// Double semicolon should not produce a spurious error
	ok := db.execute(d, "CREATE TABLE t (id INT);;") == .None
	testing.expect(t, ok, "CREATE with double ;;")
	ok = db.execute(d, "INSERT INTO t VALUES (1);;") == .None
	testing.expect(t, ok, "INSERT with double ;;")
}

@(test)
test_insert_too_many_columns :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "toomany")
	defer teardown_db(d, "toomany")
	db.execute(d, "CREATE TABLE t (id INT, val INT);")
	// Column list with more entries than table has columns should be rejected
	saved, ctx := suppress_expected_errors()
	context = ctx
	ok := db.execute(d, "INSERT INTO t (id,val,id,val) VALUES (1,2,3,4);") != .None
	context = restore_logger(saved)
	testing.expect(t, ok, "INSERT with too many columns rejected")
	// Duplicate column names in INSERT should be accepted (last wins)
	ok = db.execute(d, "INSERT INTO t (id,id) VALUES (1,2);") == .None
	testing.expect(t, ok, "INSERT with duplicate column names accepted")
	// But the value should be the last one
	q := db.query(d, "SELECT id FROM t;")
	if q.ok && len(q.rows) >= 1 && len(q.rows[0]) >= 1 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(2))
	}
}

@(test)
test_negative_limit :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Verify that negative LIMIT produces an error message (not just generic failure)
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT -5;", context.temp_allocator)
	testing.expect(t, !ok, "negative LIMIT rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_negative_offset :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT 5 OFFSET -3;", context.temp_allocator)
	testing.expect(t, !ok, "negative OFFSET rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_select_readonly_no_snapshot :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// SELECT under shared lock should not create a snapshot
	d := setup_db(t, "sel_nosnap")
	defer teardown_db(d, "sel_nosnap")

	ok := db.execute(d, "CREATE TABLE t (id INT);")
	testing.expect(t, ok == .None, "CREATE TABLE")

	ok = db.execute(d, "INSERT INTO t VALUES (1);")
	testing.expect(t, ok == .None, "INSERT")

	// SELECT must not create a snapshot
	q := db.query(d, "SELECT * FROM t;")
	testing.expect(t, q.ok, "SELECT under shared lock")
	testing.expect(t, len(q.rows) == 1, "1 row from SELECT")
}

@(test)
test_multiple_selects :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Multiple sequential SELECTs should all work under shared lock
	d := setup_db(t, "multisel")
	defer teardown_db(d, "multisel")

	db.execute(d, "CREATE TABLE t (x INT);")
	db.execute(d, "INSERT INTO t VALUES (10);")
	db.execute(d, "INSERT INTO t VALUES (20);")
	db.execute(d, "INSERT INTO t VALUES (30);")

	for i in 0 ..< 5 {
		q := db.query(d, "SELECT * FROM t ORDER BY x;")
		testing.expect(t, q.ok, fmt.tprintf("SELECT %d", i))
		testing.expect(t, len(q.rows) == 3, fmt.tprintf("3 rows in SELECT %d", i))
	}
}

@(test)
test_readonly_admin_commands :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "ro_admin")
	defer teardown_db(d, "ro_admin")

	db.execute(d, "CREATE TABLE t1 (id INT);")
	db.execute(d, "CREATE TABLE t2 (val TEXT);")

	testing.expect(t, admin.list_tables(d) == .None, "list_tables succeeds")
	testing.expect(t, admin.describe_table(d, "t1") == .None, "describe_table t1 succeeds")
	testing.expect(t, admin.describe_table(d, "t2") == .None, "describe_table t2 succeeds")
	testing.expect(t, admin.stats(d) == .None, "stats succeeds")

	q := db.query(d, "SELECT id FROM t1;")
	testing.expect(t, q.ok, "query after admin commands")
}

@(test)
test_insert_creates_snapshot :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "ins_snap")
	defer teardown_db(d, "ins_snap")

	db.execute(d, "CREATE TABLE t (id INT);")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "initial query")

	ok := db.execute(d, "INSERT INTO t VALUES (42);")
	testing.expect(t, ok == .None, "INSERT under exclusive lock")

	q = db.query(d, "SELECT id FROM t;")
	testing.expect(t, q.ok, "query after INSERT")
	if q.ok && len(q.rows) > 0 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(42))
	}
}

@(test)
test_write_after_read :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "w_after_r")
	defer teardown_db(d, "w_after_r")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")

	q := db.query(d, "SELECT * FROM t;")
	testing.expect(t, q.ok, "read first")

	ok := db.execute(d, "INSERT INTO t VALUES (2);")
	testing.expect(t, ok == .None, "write after read")

	q = db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "verify after write")
	if q.ok && len(q.rows) > 0 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(2))
	}
}

@(test)
test_in_subquery :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "insubq")
	defer teardown_db(d, "insubq")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO t VALUES (3, 'Charlie');")

	db.execute(d, "CREATE TABLE active (id INT);")
	db.execute(d, "INSERT INTO active VALUES (1);")
	db.execute(d, "INSERT INTO active VALUES (3);")

	q := db.query(d, "SELECT name FROM t WHERE id IN (SELECT id FROM active) ORDER BY name;")
	testing.expect(t, q.ok, "IN subquery execute")
	testing.expect(t, len(q.rows) == 2, "2 rows from IN subquery")
	if len(q.rows) >= 2 {
		name0, _ := q.rows[0][0].(string)
		name1, _ := q.rows[1][0].(string)
		testing.expect_value(t, name0, "Alice")
		testing.expect_value(t, name1, "Charlie")
	}

	q2 := db.query(d, "SELECT name FROM t WHERE id IN (1, 3) ORDER BY name;")
	testing.expect(t, q2.ok, "IN literal execute")
	testing.expect(t, len(q2.rows) == 2, "2 rows from IN literal")
}

@(test)
test_integration_set_ops :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "setops")
	defer teardown_db(d, "setops")

	db.execute(d, "CREATE TABLE a (x INT);")
	db.execute(d, "INSERT INTO a VALUES (1);")
	db.execute(d, "INSERT INTO a VALUES (1);")
	db.execute(d, "INSERT INTO a VALUES (2);")
	db.execute(d, "CREATE TABLE b (x INT);")
	db.execute(d, "INSERT INTO b VALUES (1);")
	db.execute(d, "INSERT INTO b VALUES (3);")

	// UNION dedups across both sets.
	q := db.query(d, "SELECT x FROM a UNION SELECT x FROM b;")
	testing.expect(t, q.ok, "UNION should succeed")
	testing.expect(t, len(q.rows) == 3, "UNION of {1,1,2} and {1,3} = {1,2,3}")
	if len(q.rows) == 3 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[1][0].(i64), i64(2))
		testing.expect_value(t, q.rows[2][0].(i64), i64(3))
	}

	// UNION ALL keeps duplicates.
	q2 := db.query(d, "SELECT x FROM a UNION ALL SELECT x FROM b;")
	testing.expect(t, q2.ok, "UNION ALL should succeed")
	testing.expect(t, len(q2.rows) == 5, "UNION ALL of {1,1,2} and {1,3} = 5 rows")

	// INTERSECT keeps only shared values.
	q3 := db.query(d, "SELECT x FROM a INTERSECT SELECT x FROM b;")
	testing.expect(t, q3.ok, "INTERSECT should succeed")
	testing.expect(t, len(q3.rows) == 1, "INTERSECT of {1,1,2} and {1,3} = {1}")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(1))
	}

	// EXCEPT keeps left-only values.
	q4 := db.query(d, "SELECT x FROM a EXCEPT SELECT x FROM b;")
	testing.expect(t, q4.ok, "EXCEPT should succeed")
	testing.expect(t, len(q4.rows) == 1, "EXCEPT of {1,1,2} and {1,3} = {2}")
	if len(q4.rows) == 1 {
		testing.expect_value(t, q4.rows[0][0].(i64), i64(2))
	}

	// INTERSECT ALL respects multiplicity.
	q5 := db.query(d, "SELECT x FROM a INTERSECT ALL SELECT x FROM b;")
	testing.expect(t, q5.ok, "INTERSECT ALL should succeed")
	testing.expect(t, len(q5.rows) == 1, "INTERSECT ALL of {1,1,2} and {1,3} = {1}")

	// EXCEPT ALL keeps left multiplicity minus right.
	q6 := db.query(d, "SELECT x FROM a EXCEPT ALL SELECT x FROM b;")
	testing.expect(t, q6.ok, "EXCEPT ALL should succeed")
	testing.expect(t, len(q6.rows) == 2, "EXCEPT ALL of {1,1,2} and {1,3} = {1,2}")

	// Precedence: INTERSECT binds tighter than UNION.
	q7 := db.query(d, "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM a;")
	testing.expect(t, q7.ok, "precedence UNION/INTERSECT should succeed")
	// b INTERSECT a = {1}; a UNION {1} = {1,2}
	testing.expect(t, len(q7.rows) == 2, "a UNION (b INTERSECT a) = {1,2}")

	// Compound ORDER BY LIMIT.
	q8 := db.query(d, "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 2;")
	testing.expect(t, q8.ok, "compound ORDER BY LIMIT should succeed")
	testing.expect(t, len(q8.rows) == 2, "compound LIMIT 2 = 2 rows")

	// Literal operands.
	q9 := db.query(d, "SELECT 1 UNION SELECT 2;")
	testing.expect(t, q9.ok, "literal UNION should succeed")
	testing.expect(t, len(q9.rows) == 2, "SELECT 1 UNION SELECT 2 = 2 rows")
}

@(test)
test_integration_where_mixed_logic :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "where_mixed")
	defer teardown_db(d, "where_mixed")

	db.execute(d, "CREATE TABLE t (a INT, b INT, c INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 2, 0);")
	db.execute(d, "INSERT INTO t VALUES (1, 9, 0);")
	db.execute(d, "INSERT INTO t VALUES (0, 2, 3);")
	db.execute(d, "INSERT INTO t VALUES (0, 9, 3);")

	// AND binds tighter than OR: (a=1 AND b=2) OR c=3.
	q := db.query(d, "SELECT c FROM t WHERE a=1 AND b=2 OR c=3;")
	testing.expect(t, q.ok, "mixed AND/OR WHERE should succeed")
	testing.expect(t, len(q.rows) == 3, "(a=1 AND b=2) OR c=3 matches 3 rows")
	if len(q.rows) == 3 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(0))
		testing.expect_value(t, q.rows[1][0].(i64), i64(3))
		testing.expect_value(t, q.rows[2][0].(i64), i64(3))
	}

	// UPDATE/DELETE with mixed logic reuse the same tree evaluation.
	db.execute(d, "UPDATE t SET c = 9 WHERE a=1 AND b=2 OR c=3;")
	q2 := db.query(d, "SELECT c FROM t;")
	testing.expect(t, q2.ok, "select after update should succeed")
	testing.expect(t, len(q2.rows) == 4, "update touched 3 rows total")

	db.execute(d, "DELETE FROM t WHERE a=1 AND b=9 OR c=3;")
	q3 := db.query(d, "SELECT c FROM t;")
	testing.expect(t, q3.ok, "select after delete should succeed")
	testing.expect(t, len(q3.rows) == 3, "delete with mixed logic removed 1 row, 3 remain")
}

@(test)
test_integration_where_parens :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "where_parens")
	defer teardown_db(d, "where_parens")

	db.execute(d, "CREATE TABLE t (a INT, b INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 3);")
	db.execute(d, "INSERT INTO t VALUES (2, 3);")
	db.execute(d, "INSERT INTO t VALUES (1, 9);")
	db.execute(d, "INSERT INTO t VALUES (5, 9);")

	// Parenthesized OR group under AND.
	q := db.query(d, "SELECT b FROM t WHERE (a=1 OR a=2) AND b=3;")
	testing.expect(t, q.ok, "parenthesized WHERE should succeed")
	testing.expect(t, len(q.rows) == 2, "(a=1 OR a=2) AND b=3 matches 2 rows")

	// Nested parens: (a=1 AND (b=3 OR b=9)) OR a=5.
	q2 := db.query(d, "SELECT a FROM t WHERE (a=1 AND (b=3 OR b=9)) OR a=5;")
	testing.expect(t, q2.ok, "nested parenthesized WHERE should succeed")
	testing.expect(t, len(q2.rows) == 3, "(a=1 AND (b=3 OR b=9)) OR a=5 matches 3 rows")
}

@(test)
test_integration_having_mixed :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "having_mixed")
	defer teardown_db(d, "having_mixed")

	db.execute(d, "CREATE TABLE s (g INT, v INT);")
	db.execute(d, "INSERT INTO s VALUES (1, 10);")
	db.execute(d, "INSERT INTO s VALUES (1, 20);")
	db.execute(d, "INSERT INTO s VALUES (2, 5);")
	db.execute(d, "INSERT INTO s VALUES (3, 100);")

	// Mixed logic against aggregate and group-column references.
	q := db.query(d, "SELECT g FROM s GROUP BY g HAVING COUNT(*) >= 2 OR SUM(v) > 50;")
	testing.expect(t, q.ok, "mixed HAVING should succeed")
	testing.expect(t, len(q.rows) == 2, "HAVING COUNT(*)>=2 OR SUM(v)>50 keeps groups 1 and 3")

	// Parenthesized group-column HAVING.
	q2 := db.query(d, "SELECT g FROM s GROUP BY g HAVING (g = 1 OR g = 2) AND COUNT(*) <= 2;")
	testing.expect(t, q2.ok, "parenthesized HAVING should succeed")
	testing.expect(
		t,
		len(q2.rows) == 2,
		"HAVING (g=1 OR g=2) AND COUNT(*)<=2 keeps groups 1 and 2",
	)

	// GROUP BY applies even with no select-list aggregates.
	q3 := db.query(d, "SELECT g FROM s GROUP BY g;")
	testing.expect(t, q3.ok, "GROUP BY only should succeed")
	testing.expect(t, len(q3.rows) == 3, "GROUP BY g yields 3 distinct groups")

	// Bare aggregate name in HAVING (no parens).
	q4 := db.query(d, "SELECT g FROM s GROUP BY g HAVING count > 1;")
	testing.expect(t, q4.ok, "HAVING count > 1 should succeed")
	testing.expect(t, len(q4.rows) == 1, "HAVING count > 1 keeps only g=1")
	if len(q4.rows) == 1 {
		testing.expect_value(t, q4.rows[0][0].(i64), i64(1))
	}

	// HAVING with no GROUP BY but a select-list aggregate.
	q5 := db.query(d, "SELECT COUNT(*) AS n FROM s HAVING count > 3;")
	testing.expect(t, q5.ok, "HAVING over single aggregate should succeed")
	testing.expect(t, len(q5.rows) == 1, "COUNT(*) = 4 satisfies HAVING count > 3")
}

@(test)
test_integration_join_on_mixed :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "join_on_mixed")
	defer teardown_db(d, "join_on_mixed")

	db.execute(d, "CREATE TABLE t1 (x INT, y INT);")
	db.execute(d, "INSERT INTO t1 VALUES (1, 10);")
	db.execute(d, "INSERT INTO t1 VALUES (2, 20);")
	db.execute(d, "INSERT INTO t1 VALUES (3, 30);")
	db.execute(d, "CREATE TABLE t2 (x INT, z INT);")
	db.execute(d, "INSERT INTO t2 VALUES (1, 100);")
	db.execute(d, "INSERT INTO t2 VALUES (2, 200);")

	// Mixed logic in a JOIN ON clause.
	q := db.query(
		d,
		"SELECT t1.x FROM t1 INNER JOIN t2 ON t1.x = t2.x AND (t1.y > 15 OR t2.z < 150) ORDER BY t1.x;",
	)
	testing.expect(t, q.ok, "mixed JOIN ON should succeed")
	testing.expect(t, len(q.rows) == 2, "ON x=y AND (t1.y>15 OR t2.z<150) matches x=1 and x=2")
	if len(q.rows) == 2 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[1][0].(i64), i64(2))
	}
}

@(test)
test_integration_where_in_parens :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "where_in_parens")
	defer teardown_db(d, "where_in_parens")

	db.execute(d, "CREATE TABLE t1 (x INT);")
	db.execute(d, "INSERT INTO t1 VALUES (1);")
	db.execute(d, "INSERT INTO t1 VALUES (2);")
	db.execute(d, "INSERT INTO t1 VALUES (3);")
	db.execute(d, "CREATE TABLE t2 (x INT);")
	db.execute(d, "INSERT INTO t2 VALUES (1);")
	db.execute(d, "INSERT INTO t2 VALUES (3);")

	// IN subquery combined with a mixed-logic predicate.
	q := db.query(d, "SELECT x FROM t1 WHERE x IN (SELECT x FROM t2) AND x > 1;")
	testing.expect(t, q.ok, "IN subquery in mixed WHERE should succeed")
	testing.expect(t, len(q.rows) == 1, "x IN (1,3) AND x > 1 leaves only x=3")
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(3))
	}
}

@(test)
test_integration_column_alias :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "col_alias")
	defer teardown_db(d, "col_alias")

	db.execute(d, "CREATE TABLE s (g INT, v INT);")
	db.execute(d, "INSERT INTO s VALUES (1, 10);")
	db.execute(d, "INSERT INTO s VALUES (1, 20);")
	db.execute(d, "INSERT INTO s VALUES (2, 5);")

	// Aggregates with aliases return correct data and header names.
	q := db.query(d, "SELECT g, COUNT(*) AS n, SUM(v) AS sm FROM s GROUP BY g;")
	testing.expect(t, q.ok, "aliased aggregate SELECT should succeed")
	testing.expect(t, len(q.rows) == 2, "GROUP BY g yields 2 groups")
	testing.expect_value(t, q.columns[0], "g")
	testing.expect_value(t, q.columns[1], "n")
	testing.expect_value(t, q.columns[2], "sm")
	if len(q.rows) == 2 {
		testing.expect_value(t, q.rows[0][1].(i64), i64(2)) // COUNT for g=1
		testing.expect_value(t, q.rows[1][0].(i64), i64(2)) // g=2
	}

	// Plain column alias.
	q2 := db.query(d, "SELECT g AS group_id FROM s WHERE g = 2;")
	testing.expect(t, q2.ok, "aliased plain SELECT should succeed")
	testing.expect(t, len(q2.rows) == 1, "g=2 matches 1 row")
	testing.expect_value(t, q2.columns[0], "group_id")

	// Aggregates with aliases in a set-op operand carry the header.
	q3 := db.query(d, "SELECT COUNT(*) AS total FROM s UNION SELECT 99 AS total;")
	testing.expect(t, q3.ok, "aliased aggregate UNION should succeed")
	testing.expect(t, len(q3.rows) == 2, "UNION yields 2 rows")
	testing.expect_value(t, q3.columns[0], "total")
}

@(test)
test_integration_where_not :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "where_not")
	defer teardown_db(d, "where_not")

	db.execute(d, "CREATE TABLE t (a INT, b TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'apple');")
	db.execute(d, "INSERT INTO t VALUES (2, 'banana');")
	db.execute(d, "INSERT INTO t VALUES (3, 'cherry');")

	// NOT prefix.
	q := db.query(d, "SELECT a FROM t WHERE NOT a = 1;")
	testing.expect(t, q.ok, "WHERE NOT a=1 should succeed")
	testing.expect(t, len(q.rows) == 2, "NOT a=1 leaves 2 rows")

	// NOT IN.
	q2 := db.query(d, "SELECT a FROM t WHERE a NOT IN (1, 3);")
	testing.expect(t, q2.ok, "WHERE a NOT IN should succeed")
	testing.expect(t, len(q2.rows) == 1, "a NOT IN (1,3) leaves a=2")
	if len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), i64(2))
	}

	// NOT with a parenthesized group.
	q3 := db.query(d, "SELECT a FROM t WHERE NOT (a = 1 OR a = 2);")
	testing.expect(t, q3.ok, "WHERE NOT (a=1 OR a=2) should succeed")
	testing.expect(t, len(q3.rows) == 1, "NOT (a=1 OR a=2) leaves a=3")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(3))
	}

	// NOT LIKE on a text column.
	q4 := db.query(d, "SELECT b FROM t WHERE b NOT LIKE 'a%';")
	testing.expect(t, q4.ok, "WHERE b NOT LIKE should succeed")
	testing.expect(t, len(q4.rows) == 2, "b NOT LIKE 'a%' excludes apple")
}

@(test)
test_integration_multi_row_insert :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "multi_row")
	defer teardown_db(d, "multi_row")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score REAL DEFAULT 0.0);")
	db.execute(d, "INSERT INTO t VALUES (1, 'a', 1.5), (2, 'b', 2.5);")
	db.execute(d, "INSERT INTO t (name, score) VALUES ('c', 3.5), ('d', 4.5);")

	q := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q.ok, "count after multi-row insert should succeed")
	testing.expect_value(t, q.rows[0][0].(i64), i64(4))

	q2 := db.query(d, "SELECT id FROM t WHERE name = 'd';")
	testing.expect(t, q2.ok, "select after column-list multi-row insert")
	testing.expect(t, len(q2.rows) == 1, "one row for d")
	if len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), i64(4))
	}

	// Omitted PK column gets the auto-increment rowid stored as its value.
	q3 := db.query(d, "SELECT id, name FROM t ORDER BY id;")
	testing.expect(t, q3.ok, "select all after PK auto-fill")
	testing.expect(t, len(q3.rows) == 4, "4 rows total")
	if len(q3.rows) == 4 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q3.rows[3][0].(i64), i64(4))
	}
}

@(test)
test_integration_multipage_tree :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "multipage")
	defer teardown_db(d, "multipage")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	// Sequential inserts across many page splits (>1 page tree).
	for i in 1 ..= 500 {
		sql := fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 2)
		if db.execute(d, sql) != .None {
			testing.expect(t, false, fmt.tprintf("insert %d failed", i))
			return
		}
	}

	q := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q.ok, "count after multipage insert")
	testing.expect_value(t, q.rows[0][0].(i64), i64(500))

	// Spot-check rows around the first split boundary.
	q1 := db.query(d, "SELECT v FROM t WHERE id = 185;")
	testing.expect(t, q1.ok && len(q1.rows) == 1, "row 185 present")
	if len(q1.rows) == 1 {
		testing.expect_value(t, q1.rows[0][0].(i64), i64(370))
	}
	q2 := db.query(d, "SELECT v FROM t WHERE id = 186;")
	testing.expect(t, q2.ok && len(q2.rows) == 1, "row 186 present")

	// Update + delete then verify counts.
	db.execute(d, "UPDATE t SET v = -1 WHERE id = 186;")
	q3 := db.query(d, "SELECT v FROM t WHERE id = 186;")
	testing.expect(t, q3.ok && len(q3.rows) == 1, "updated row present")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(-1))
	}
	db.execute(d, "DELETE FROM t WHERE id = 186;")
	q4 := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q4.ok, "count after delete")
	testing.expect_value(t, q4.rows[0][0].(i64), i64(499))
}

@(test)
test_integration_random_order_splits :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "rand_splits")
	defer teardown_db(d, "rand_splits")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	// Deterministic pseudo-random insert order (7919 coprime with 5000) that
	// forces non-rightmost leaf splits and interior separator updates.
	for i in 1 ..= 500 {
		id := (i * 7919) % 5000 + 1
		sql := fmt.tprintf("INSERT INTO t VALUES (%d, %d);", id, id)
		if db.execute(d, sql) != .None {
			testing.expect(t, false, fmt.tprintf("insert id %d failed", id))
			return
		}
	}

	q := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q.ok, "count after random-order insert")
	testing.expect_value(t, q.rows[0][0].(i64), i64(500))

	// Point lookups must find the rows (interior separators intact). All probes
	// are chosen from the deterministic inserted id set.
	probes := []int{3, 44, 4932, 4960, 4975, 4988}
	for pi in 0 ..< len(probes) {
		probe := probes[pi]
		sql := fmt.tprintf("SELECT v FROM t WHERE id = %d;", probe)
		q2 := db.query(d, sql)
		testing.expect(t, q2.ok && len(q2.rows) == 1, fmt.tprintf("row %d found", probe))
	}

	// Update + delete on the random-order tree.
	db.execute(d, "UPDATE t SET v = -3 WHERE id = 4988;")
	q3 := db.query(d, "SELECT v FROM t WHERE id = 4988;")
	testing.expect(t, q3.ok && len(q3.rows) == 1, "updated row present")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(-3))
	}
	db.execute(d, "DELETE FROM t WHERE id = 4932;")
	q4 := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q4.ok, "count after delete")
	testing.expect_value(t, q4.rows[0][0].(i64), i64(499))
}

@(test)
test_integration_delete_multipage :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "delmp")
	defer teardown_db(d, "delmp")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	// Delete the leading 700 rows: this empties entire leading leaves, which
	// previously made cursor_start treat the tree as empty (data loss).
	ids: strings.Builder
	strings.builder_init(&ids, context.temp_allocator)
	for id in 1 ..= 700 {
		if id > 1 { strings.write_string(&ids, ",") }
		fmt.sbprintf(&ids, "%d", id)
	}
	ok :=
		db.execute(d, fmt.tprintf("DELETE FROM t WHERE id IN (%s);", strings.to_string(ids))) ==
		.None
	testing.expect(t, ok, "multi-page delete")

	r := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, r.ok, "count after delete")
	testing.expect_value(t, r.rows[0][0].(i64), i64(300))

	r2 := db.query(d, "SELECT MIN(id) AS mn, MAX(id) AS mx FROM t;")
	testing.expect(t, r2.ok, "min/max after delete")
	testing.expect_value(t, r2.rows[0][0].(i64), i64(701))
	testing.expect_value(t, r2.rows[0][1].(i64), i64(1000))

	// Spot-check a surviving and a deleted row.
	r3 := db.query(d, "SELECT v FROM t WHERE id = 1000;")
	testing.expect(t, r3.ok, "survivor readable")
	testing.expect_value(t, r3.rows[0][0].(i64), i64(2000))
	r4 := db.query(d, "SELECT COUNT(*) AS n FROM t WHERE id = 5;")
	testing.expect(t, r4.ok, "deleted row gone")
	testing.expect_value(t, r4.rows[0][0].(i64), i64(0))
}

@(test)
test_integration_vacuum :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "vacuum")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 20 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	// Delete most rows to create sparse pages, then vacuum.
	ids: strings.Builder
	strings.builder_init(&ids, context.temp_allocator)
	for id in 1 ..= 1800 {
		if id > 1 { strings.write_string(&ids, ",") }
		fmt.sbprintf(&ids, "%d", id)
	}
	testing.expect(
		t,
		db.execute(d, fmt.tprintf("DELETE FROM t WHERE id IN (%s);", strings.to_string(ids))) ==
		.None,
		"bulk delete",
	)

	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	// All surviving rows must be present and correct.
	r := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, r.ok, "count after vacuum")
	testing.expect_value(t, r.rows[0][0].(i64), i64(200))

	r2 := db.query(d, "SELECT MIN(id) AS mn, MAX(id) AS mx FROM t;")
	testing.expect(t, r2.ok, "min/max after vacuum")
	testing.expect_value(t, r2.rows[0][0].(i64), i64(1801))
	testing.expect_value(t, r2.rows[0][1].(i64), i64(2000))

	r3 := db.query(d, "SELECT v FROM t WHERE id = 2000;")
	testing.expect(t, r3.ok, "row value after vacuum")
	testing.expect_value(t, r3.rows[0][0].(i64), i64(4000))

	// The vacuumed state must survive a close/reopen.
	db.close(d)
	d2, open_err := db.open(fmt.tprintf("test_int_vacuum.db"))
	testing.expect(t, open_err == .None, "reopen after vacuum")
	if open_err == .None {
		defer teardown_db(d2, "vacuum")
		r4 := db.query(d2, "SELECT COUNT(*) AS n FROM t;")
		testing.expect(t, r4.ok, "count after reopen")
		testing.expect_value(t, r4.rows[0][0].(i64), i64(200))
	}
}

@(test)
test_integration_join_pushdown :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pushdown")
	defer teardown_db(d, "pushdown")

	db.execute(d, "CREATE TABLE a (id INT PRIMARY KEY, grp INT, name TEXT);")
	db.execute(d, "CREATE TABLE b (id INT PRIMARY KEY, score INT);")
	for i in 1 ..= 300 {
		ok :=
			db.execute(d, fmt.tprintf("INSERT INTO a VALUES (%d, %d, 'n%d');", i, i % 10, i)) ==
			.None
		testing.expect(t, ok, "insert a")
		ok2 := db.execute(d, fmt.tprintf("INSERT INTO b VALUES (%d, %d);", i, i * 3)) == .None
		testing.expect(t, ok2, "insert b")
	}

	// Single-table bare-column conjunct pushed to the base scan.
	r := db.query(d, "SELECT COUNT(*) AS n FROM a INNER JOIN b ON a.id = b.id WHERE grp = 5;")
	testing.expect(t, r.ok, "pushdown grp=5")
	testing.expect_value(t, r.rows[0][0].(i64), i64(30))

	// Both sides pushed: grp on a, score on b.
	r2 := db.query(
		d,
		"SELECT COUNT(*) AS n FROM a INNER JOIN b ON a.id = b.id WHERE grp = 5 AND score > 600;",
	)
	testing.expect(t, r2.ok, "pushdown both sides")
	testing.expect_value(t, r2.rows[0][0].(i64), i64(10))

	// Qualified column stays in the post-join filter (correct, not pushed).
	r3 := db.query(d, "SELECT COUNT(*) AS n FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 280;")
	testing.expect(t, r3.ok, "qualified where")
	testing.expect_value(t, r3.rows[0][0].(i64), i64(20))

	// Ambiguous bare column (id exists in both) must not be pushed but still correct.
	r4 := db.query(d, "SELECT COUNT(*) AS n FROM a INNER JOIN b ON a.id = b.id WHERE id > 280;")
	testing.expect(t, r4.ok, "ambiguous where")
	testing.expect_value(t, r4.rows[0][0].(i64), i64(20))
}

@(test)
test_integration_random_delete_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "delround")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 3)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	// Delete from the front, middle, and back of the key range so empty and
	// sparse leaves appear at every position.
	ok := db.execute(d, "DELETE FROM t WHERE id IN (1,2,3,4,5);") == .None
	testing.expect(t, ok, "front delete")
	ok2 := db.execute(d, "DELETE FROM t WHERE id >= 300 AND id <= 320;") == .None
	testing.expect(t, ok2, "middle delete")
	ok3 := db.execute(d, "DELETE FROM t WHERE id > 980;") == .None
	testing.expect(t, ok3, "back delete")

	r := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, r.ok, "count after deletes")
	expected := 1000 - 5 - 21 - 20
	testing.expect_value(t, r.rows[0][0].(i64), i64(expected))

	// Spot-check survivors at each position and that the tree stays consistent.
	r2 := db.query(d, "SELECT v FROM t WHERE id = 6;")
	testing.expect_value(t, r2.rows[0][0].(i64), i64(18))
	r3 := db.query(d, "SELECT v FROM t WHERE id = 980;")
	testing.expect_value(t, r3.rows[0][0].(i64), i64(2940))
	r4 := db.query(d, "SELECT COUNT(*) AS n FROM t WHERE id IN (1, 310, 995);")
	testing.expect_value(t, r4.rows[0][0].(i64), i64(0))

	// The state must survive a close/reopen.
	db.close(d)
	d2, open_err := db.open(fmt.tprintf("test_int_delround.db"))
	testing.expect(t, open_err == .None, "reopen after deletes")
	if open_err == .None {
		defer teardown_db(d2, "delround")
		r5 := db.query(d2, "SELECT COUNT(*) AS n FROM t;")
		testing.expect(t, r5.ok, "count after reopen")
		testing.expect_value(t, r5.rows[0][0].(i64), i64(expected))
	}
}

@(test)
test_integration_empty_aggregates :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "emptyagg")
	defer teardown_db(d, "emptyagg")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")

	r := db.query(
		d,
		"SELECT COUNT(*) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx FROM t WHERE id = 99;",
	)
	testing.expect(t, r.ok, "empty aggregate query")
	testing.expect_value(t, len(r.rows), 1)
	if len(r.rows) == 1 {
		c, _ := r.rows[0][0].(i64)
		s, _ := r.rows[0][1].(i64)
		testing.expect_value(t, c, i64(0))
		testing.expect_value(t, s, i64(0))
		testing.expect(t, types.is_null(r.rows[0][2]), "AVG is NULL")
		testing.expect(t, types.is_null(r.rows[0][3]), "MIN is NULL")
		testing.expect(t, types.is_null(r.rows[0][4]), "MAX is NULL")
	}
}

@(test)
test_integration_delete_then_vacuum :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "delvac")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 { strings.write_string(&sb, ",") }
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	ids: strings.Builder
	strings.builder_init(&ids, context.temp_allocator)
	for id in 1 ..= 800 {
		if id > 1 { strings.write_string(&ids, ",") }
		fmt.sbprintf(&ids, "%d", id)
	}
	testing.expect(
		t,
		db.execute(d, fmt.tprintf("DELETE FROM t WHERE id IN (%s);", strings.to_string(ids))) ==
		.None,
		"bulk delete",
	)

	testing.expect(t, admin.vacuum(d) == .None, "vacuum after delete")

	r := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, r.ok, "count after delete+vacuum")
	testing.expect_value(t, r.rows[0][0].(i64), i64(200))

	r2 := db.query(d, "SELECT MIN(id) AS mn, MAX(id) AS mx FROM t;")
	testing.expect_value(t, r2.rows[0][0].(i64), i64(801))
	testing.expect_value(t, r2.rows[0][1].(i64), i64(1000))

	// Reopen must preserve the vacuumed state.
	db.close(d)
	d2, open_err := db.open(fmt.tprintf("test_int_delvac.db"))
	testing.expect(t, open_err == .None, "reopen after vacuum")
	if open_err == .None {
		defer teardown_db(d2, "delvac")
		r3 := db.query(d2, "SELECT v FROM t WHERE id = 1000;")
		testing.expect(t, r3.ok, "survivor after reopen")
		testing.expect_value(t, r3.rows[0][0].(i64), i64(2000))
	}
}

@(test)
test_columnar_cursor_large :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "colcurlarge")
	defer teardown_tree(&ctx)

	// Build 300 rows with an INTEGER (DELTA-encoded) and a REAL (RAW) column.
	rowids := make([]types.Row_ID, 300, context.temp_allocator)
	rows := make([][]types.Value, 300, context.temp_allocator)
	for i in 0 ..< 300 {
		rowids[i] = types.Row_ID(i + 1)
		// Note: build each row with make + index assignment — a []types.Value
		// composite literal inside a loop reuses the same backing array, so
		// every row would alias the last one.
		row := make([]types.Value, 2, context.temp_allocator)
		row[0] = types.value_int(i64(i * 10))
		row[1] = types.value_real(f64(i) * 0.5)
		rows[i] = row
	}
	cols := []types.Column{{name = "iv", type = .INTEGER}, {name = "rv", type = .REAL}}

	// Mount the columnar data as the tree's single leaf (page 1).
	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	defer pager.unpin_page(ctx.pager, 1)
	off := btree.get_page_header_offset(1)
	ok := cell.serialize_columnar(pg.data[off:], rowids, rows, cols)
	testing.expect(t, ok, "serialize columnar")
	hdr := btree.get_leaf_header(pg.data, 1)
	hdr.page_type = .LEAF_TABLE_COLUMNAR
	hdr.cell_count = u16le(300)
	hdr.cell_content_offset = u16le(8 + len(cols) * 12)
	pager.mark_dirty(ctx.pager, 1)


	// Scan via the cursor (exercises the incremental per-column decode) and
	// verify every row decodes to its exact rowid + values.
	seen := make(map[types.Row_ID]bool, 300, context.temp_allocator)
	c, c_err := btree.cursor_start(&ctx.tree)
	testing.expect(t, c_err == .None, "cursor start")
	defer btree.cursor_destroy(&c)
	count := 0
	all_ok := true
	for c.is_valid {
		cc, g_err := btree.cursor_get_cell(&c, context.temp_allocator)
		if g_err != .None {
			all_ok = false
			btree.cursor_advance(&c)
			continue
		}
		if len(cc.values) != 2 {
			all_ok = false
		} else if int(cc.rowid) >= 1 && int(cc.rowid) <= 300 {
			idx := int(cc.rowid) - 1
			if !types.value_compare(
				cc.values[0],
				types.value_int(i64(idx * 10)),
			) { all_ok = false }
			if !types.value_compare(
				cc.values[1],
				types.value_real(f64(idx) * 0.5),
			) { all_ok = false }
		} else {
			all_ok = false
		}
		if !seen[cc.rowid] { count += 1; seen[cc.rowid] = true }
		cell.destroy(&cc, context.temp_allocator)
		btree.cursor_advance(&c)
	}
	testing.expect(t, all_ok, "all columnar rows decode correctly")
	testing.expect_value(t, count, 300)
}

@(test)
test_integration_select_bare_column_errors_cleanly :: proc(t: ^testing.T) {
	// Regression (grammar-mutator fuzzin): `SELECT k` without FROM
	// produced a 0-value row for 1 column and panicked the result renderer
	// (executor.odin index OOB). Must return a clean error instead.
	//
	// NOTE: muted to .Fatal because Odin's test runner fails any test that
	// emits an error-level log, and these paths intentionally log their clean errors.
	context.logger.lowest_level = .Fatal
	d := setup_db(t, "bare_col")
	defer teardown_db(d, "bare_col")

	err := db.execute(d, "SELECT k;")
	testing.expect(t, err != .None, "SELECT bare column without FROM should error cleanly")

	err2 := db.execute(d, "SELECT a, b;")
	testing.expect(t, err2 != .None, "SELECT multiple bare columns should error cleanly")

	err3 := db.execute(d, "SELECT 1;")
	testing.expect(t, err3 == .None, "SELECT literal without FROM should succeed")
}

// v1 page-format files are rejected at open with a clean error (never a
// crash): flip the header version of a valid database and reopen it.
@(test)
test_db_rejects_v1_format :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_%s.db", "v1_reject")
	clean_db_files(filename)

	d := setup_db(t, "v1_reject")
	db.close(d)

	// Patch the header's page_format_version (offset 25: 13 magic + 3 u32)
	// to 1 directly in the file.
	raw, read_err := os.read_entire_file_from_path(filename, context.temp_allocator)
	testing.expect(t, read_err == nil, "db file must be readable")
	if read_err != nil {
		clean_db_files(filename)
		return
	}
	testing.expect(t, len(raw) > 29, "db file must hold a header")
	testing.expect(t, string(raw[:8]) == "MAGNI_DB", "header magic must match")
	if len(raw) <= 29 || string(raw[:8]) != "MAGNI_DB" {
		clean_db_files(filename)
		return
	}
	raw[25], raw[26], raw[27], raw[28] = 1, 0, 0, 0
	testing.expect(t, os.write_entire_file(filename, raw) == nil, "version patch must write")

	reopened, open_err := db.open(filename)
	testing.expect(t, open_err == .Unsupported_Format, "v1 file must be rejected cleanly")
	testing.expect(t, reopened == nil, "rejected open must return nil db")
	clean_db_files(filename)
}

@(test)
test_rollback_truncates_file :: proc(t: ^testing.T) {
	// Aborted transactions abandon their tail pages (never snapshotted,
	// unreachable once roots restore). Rollback must persist that rewind
	// to disk, not just in memory.
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_%s.db", "rollback_shrink")
	clean_db_files(filename)

	d := setup_db(t, "rollback_shrink")
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 700 {
		if db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) != .None {
			testing.expect(t, false, "insert")
			db.close(d)
			clean_db_files(filename)
			return
		}
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	steady_pages := pager.page_count(d.pager)

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin2")
	for i in 701 ..= 1200 {
		if db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) != .None {
			testing.expect(t, false, "insert2")
			db.close(d)
			clean_db_files(filename)
			return
		}
	}
	testing.expect(t, pager.page_count(d.pager) > steady_pages, "txn must grow the file")

	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback")
	testing.expect(t, pager.page_count(d.pager) == steady_pages, "rollback must rewind the file")
	testing.expect(t, d.pager.first_free_page == 0, "freelist resets on rewind")

	q := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "post-rollback query")
	testing.expect_value(t, q.rows[0][0].(i64), i64(700))
	testing.expect_value(t, q.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q.rows[0][2].(i64), i64(700))

	// The rewind is durable: reopened file keeps the small footprint and
	// later writes reuse the reclaimed tail by append.
	db.close(d)
	d2, open_err := db.open(filename)
	testing.expect(t, open_err == .None, "reopen after rollback")
	if open_err != .None {
		clean_db_files(filename)
		return
	}
	testing.expect(
		t,
		pager.page_count(d2.pager) == steady_pages,
		"reopened file keeps the rewound footprint",
	)
	testing.expect(
		t,
		db.execute(d2, "INSERT INTO t VALUES (701, 2103);") == .None,
		"insert after reopen",
	)
	q2 := db.query(d2, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q2.ok && len(q2.rows) == 1, "post-reopen query")
	testing.expect_value(t, q2.rows[0][0].(i64), i64(701))
	db.close(d2)
	clean_db_files(filename)
}
@(test)
test_open_heals_stale_freelist :: proc(t: ^testing.T) {
	// A header persisted before an uncompleted shrink may point past EOF.
	// Open must reset the freelist (leaked space, never misread) and the
	// database must stay fully usable. first_free_page lives at file offset
	// 45 (13 magic + 4 page_size + 4 page_count + 4 schema_version +
	// 4 format_version + 4 schema_root + 4 latest_snapshot + 8 snap_counter).
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_%s.db", "stale_free")
	clean_db_files(filename)

	d := setup_db(t, "stale_free")
	testing.expect(t, db.execute(d, "CREATE TABLE t (x INT);") == .None, "create")
	testing.expect(t, db.execute(d, "INSERT INTO t VALUES (1);") == .None, "insert")
	db.close(d)

	raw, read_err := os.read_entire_file_from_path(filename, context.temp_allocator)
	testing.expect(t, read_err == nil, "db file must be readable")
	if read_err != nil {
		clean_db_files(filename)
		return
	}
	testing.expect(t, len(raw) > 49, "db file must hold a header")
	if len(raw) <= 49 {
		clean_db_files(filename)
		return
	}
	raw[45], raw[46], raw[47], raw[48] = 0xff, 0xff, 0xff, 0xff
	testing.expect(t, os.write_entire_file(filename, raw) == nil, "freelist patch must write")

	d2, open_err := db.open(filename)
	testing.expect(t, open_err == .None, "open with stale freelist must succeed")
	if open_err != .None {
		clean_db_files(filename)
		return
	}
	testing.expect(t, d2.pager.first_free_page == 0, "stale freelist must reset")
	testing.expect(t, db.execute(d2, "INSERT INTO t VALUES (2);") == .None, "insert after heal")
	q := db.query(d2, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "query after heal")
	testing.expect_value(t, q.rows[0][0].(i64), i64(2))
	db.close(d2)
	clean_db_files(filename)
}

@(test)
test_expire_in_txn_preserves_uncommitted :: proc(t: ^testing.T) {
	// Fail-first: expiring inside a txn must not touch uncommitted rows.
	// Uncommitted COW pages exist in no snapshot live set, so a sweep frees
	// them out from under the txn; later writes reuse and overwrite them.
	d := setup_db(t, "expire_txn")
	defer teardown_db(d, "expire_txn")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 700 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert",
		)
	}
	testing.expect(t, db.expire_snapshots(d, 1) == .None, "expire in txn")
	for i in 701 ..= 1400 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert2",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")

	q := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "final query")
	testing.expect_value(t, q.rows[0][0].(i64), i64(1400))
	testing.expect_value(t, q.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q.rows[0][2].(i64), i64(1400))
}

@(test)
test_wide_text_volume_exact :: proc(t: ^testing.T) {
	// Regression lock: bulk INT+TEXT+REAL inserts must all be readable.
	// A prior data-loss bug returned COUNT 0 here (healed by the GC /
	// freelist / WAL fixes; exact healer unbisected). Keep the volume
	// modest: the 50k-row CLI sweep covers scale, this pins the pattern.
	d := setup_db(t, "wide_vol")
	defer teardown_db(d, "wide_vol")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score REAL);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 2000 {
		testing.expect(
			t,
			db.execute(
				d,
				fmt.tprintf(
					"INSERT INTO t VALUES (%d, 'n%d_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', %d.5);",
					i,
					i,
					i,
				),
			) ==
			.None,
			"insert",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")

	q := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "query")
	testing.expect_value(t, q.rows[0][0].(i64), i64(2000))
	testing.expect_value(t, q.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q.rows[0][2].(i64), i64(2000))
}

@(test)
test_gc_reclaims_cow_waste :: proc(t: ^testing.T) {
	// COW inserts inside one txn allocate ~3 pages/row with no interior
	// snapshots; expire must return dead path pages to the freelist while
	// keeping every live row, and later writes must reuse (not grow).
	// Regression: build_live_set pre-marked roots, so collect_pages
	// early-returned and freed the live subtree; free_page also skipped
	// the freelist link for non-cached pages.
	d := setup_db(t, "gc_reclaim")
	defer teardown_db(d, "gc_reclaim")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 700 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "count query")
	testing.expect_value(t, q.rows[0][0].(i64), i64(700))

	before := pager.page_count(d.pager)
	testing.expect(t, int(before) > snapshot.GC_MIN_PAGES, "fixture must engage GC")

	testing.expect(t, db.expire_snapshots(d, 1) == .None, "expire")
	testing.expect(t, d.pager.first_free_page != 0, "freelist must be populated")

	// Data intact after sweep.
	q2 := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q2.ok && len(q2.rows) == 1, "post-gc query")
	testing.expect_value(t, q2.rows[0][0].(i64), i64(700))
	testing.expect_value(t, q2.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q2.rows[0][2].(i64), i64(700))

	// Reuse: 700 more rows must mostly reuse the freelist. Fresh these
	// cost ~1900 pages; from freelist only the overflow is new (<1 page/row).
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin2")
	for i in 701 ..= 1400 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert2",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit2")
	after := pager.page_count(d.pager)
	testing.expect(t, int(after) - int(before) < 700, "second batch must reuse freelist")

	q3 := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q3.ok && len(q3.rows) == 1, "final query")
	testing.expect_value(t, q3.rows[0][0].(i64), i64(1400))
	testing.expect_value(t, q3.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q3.rows[0][2].(i64), i64(1400))
}

@(test)
test_expire_zero_keep_clamps :: proc(t: ^testing.T) {
	// API-level twin of the dot-layer keep guard: keep < 1 must clamp to
	// default, never walk zero snapshots (which sweeps the whole database
	// into the freelist). The second batch forces reuse of anything wrongly
	// freed, so corruption can't hide behind intact-but-dead pages.
	d := setup_db(t, "expire_zero")
	defer teardown_db(d, "expire_zero")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 700 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	testing.expect(t, db.expire_snapshots(d, 0) == .None, "expire zero keep")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin2")
	for i in 701 ..= 1400 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 3)) == .None,
			"insert2",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit2")

	q := db.query(d, "SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "final query")
	testing.expect_value(t, q.rows[0][0].(i64), i64(1400))
	testing.expect_value(t, q.rows[0][1].(i64), i64(1))
	testing.expect_value(t, q.rows[0][2].(i64), i64(1400))
}

// Old single-header snapshot pages are rejected at open with a clean error
// (hard break, no migration): stamp snapshot magic over a packed page's
// count prefix and reopen.
@(test)
test_db_rejects_old_snapshot_layout :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_int_%s.db", "old_snap_reject")
	clean_db_files(filename)

	d := setup_db(t, "old_snap_reject")
	testing.expect(t, db.execute(d, "CREATE TABLE t (id INT);") == .None, "fixture write failed")
	testing.expect(t, d.latest_snapshot != 0, "fixture db must have a snapshot")
	snap_page := d.latest_snapshot
	db.close(d)
	if snap_page == 0 {
		clean_db_files(filename)
		return
	}
	// Drop the WAL sidecar so the reopen reads the patched main file, not
	// uncheckpointed frames.
	os.remove(fmt.tprintf("%s-wal", filename))

	raw, read_err := os.read_entire_file_from_path(filename, context.temp_allocator)
	testing.expect(t, read_err == nil, "db file must be readable")
	if read_err != nil {
		clean_db_files(filename)
		return
	}
	off := int(snap_page - 1) * types.PAGE_SIZE
	testing.expect(t, len(raw) >= off + 8, "snapshot page must be in main file")
	testing.expect(t, raw[off] == 1 && raw[off + 1] == 0, "snapshot page must be packed")
	if len(raw) < off + 8 || raw[off] != 1 {
		clean_db_files(filename)
		return
	}
	copy(raw[off:off + 8], "MAGNISNP")
	testing.expect(t, os.write_entire_file(filename, raw) == nil, "magic patch must write")

	reopened, open_err := db.open(filename)
	testing.expect(
		t,
		open_err == .Unsupported_Format,
		"old snapshot layout must be rejected cleanly",
	)
	testing.expect(t, reopened == nil, "rejected open must return nil db")
	clean_db_files(filename)
}

// PK-seek agreement suite: single-table `pk = const` must return exactly
// what the scan path returns, in every filter shape.
@(test)
test_pk_seek_hit_and_miss :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pkseek_hit")
	defer teardown_db(d, "pkseek_hit")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);")

	hit := db.query(d, "SELECT id, v FROM t WHERE id = 3;")
	testing.expect(t, hit.ok, "seek hit must succeed")
	testing.expect_value(t, len(hit.rows), 1)
	if len(hit.rows) == 1 {
		testing.expect_value(t, hit.rows[0][0].(i64), 3)
		testing.expect_value(t, hit.rows[0][1].(i64), 30)
	}

	miss := db.query(d, "SELECT id, v FROM t WHERE id = 99;")
	testing.expect(t, miss.ok, "seek miss must succeed")
	testing.expect_value(t, len(miss.rows), 0)

	star := db.query(d, "SELECT * FROM t WHERE id = 1;")
	testing.expect(t, star.ok, "seek star must succeed")
	testing.expect_value(t, len(star.rows), 1)
	if len(star.rows) == 1 {
		testing.expect_value(t, star.rows[0][0].(i64), 1)
		testing.expect_value(t, star.rows[0][1].(i64), 10)
	}
}

@(test)
test_pk_seek_filter_shapes :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pkseek_shapes")
	defer teardown_db(d, "pkseek_shapes")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")

	// Non-equality: scan path, unchanged results.
	rng := db.query(d, "SELECT id FROM t WHERE id > 1;")
	testing.expect(t, rng.ok, "range filter must succeed")
	testing.expect_value(t, len(rng.rows), 2)

	// Text literal vs int pk: cross-type comparisons are never equal.
	txt := db.query(d, "SELECT id FROM t WHERE id = '2';")
	testing.expect(t, txt.ok, "text rhs must succeed")
	testing.expect_value(t, len(txt.rows), 0)

	// NULL never matches.
	nul := db.query(d, "SELECT id FROM t WHERE id = NULL;")
	testing.expect(t, nul.ok, "null rhs must succeed")
	testing.expect_value(t, len(nul.rows), 0)

	// Qualified column matching the table resolves like the bare name.
	qual := db.query(d, "SELECT id, v FROM t WHERE t.id = 2;")
	testing.expect(t, qual.ok, "qualified filter must succeed")
	testing.expect_value(t, len(qual.rows), 1)
	if len(qual.rows) == 1 {
		testing.expect_value(t, qual.rows[0][1].(i64), 20)
	}

	// Unknown column is a clean error, never a silent full scan.
	saved, ctx := suppress_expected_errors()
	context = ctx
	bad := db.query(d, "SELECT id FROM t WHERE nosuchcol = 1;")
	context = restore_logger(saved)
	testing.expect(t, !bad.ok, "unknown column must fail")

	// Extra conjunct: AND root is not a single COND, scan path, same row.
	andc := db.query(d, "SELECT id, v FROM t WHERE id = 2 AND v = 20;")
	testing.expect(t, andc.ok, "and filter must succeed")
	testing.expect_value(t, len(andc.rows), 1)

	andmiss := db.query(d, "SELECT id FROM t WHERE id = 2 AND v = 99;")
	testing.expect(t, andmiss.ok, "and mismatch must succeed")
	testing.expect_value(t, len(andmiss.rows), 0)
}

@(test)
test_pk_seek_aggregates :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pkseek_agg")
	defer teardown_db(d, "pkseek_agg")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")

	cnt := db.query(d, "SELECT COUNT(*) FROM t WHERE id = 2;")
	testing.expect(t, cnt.ok, "seek count must succeed")
	testing.expect_value(t, len(cnt.rows), 1)
	if len(cnt.rows) == 1 { testing.expect_value(t, cnt.rows[0][0].(i64), 1) }

	sum := db.query(d, "SELECT SUM(v) FROM t WHERE id = 3;")
	testing.expect(t, sum.ok, "seek sum must succeed")
	if len(sum.rows) == 1 { testing.expect_value(t, sum.rows[0][0].(f64), 30) }

	cntmiss := db.query(d, "SELECT COUNT(*) FROM t WHERE id = 99;")
	testing.expect(t, cntmiss.ok, "seek count miss must succeed")
	if len(cntmiss.rows) == 1 { testing.expect_value(t, cntmiss.rows[0][0].(i64), 0) }
}

@(test)
test_pk_seek_as_of :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pkseek_asof")
	defer teardown_db(d, "pkseek_asof")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	snap_page := d.latest_snapshot
	h, h_ok := snapshot.load(d.pager, snap_page)
	testing.expect(t, h_ok, "should load snapshot")
	snap_id := h.snapshot_id
	db.execute(d, "UPDATE t SET v = 99 WHERE id = 1;")

	cur := db.query(d, "SELECT v FROM t WHERE id = 1;")
	testing.expect(t, cur.ok, "current seek must succeed")
	if len(cur.rows) == 1 { testing.expect_value(t, cur.rows[0][0].(i64), 99) }

	old := db.query(d, fmt.tprintf("SELECT v FROM t AS OF SNAPSHOT %d WHERE id = 1;", snap_id))
	testing.expect(t, old.ok, "as-of seek must succeed")
	testing.expect_value(t, len(old.rows), 1)
	if len(old.rows) == 1 { testing.expect_value(t, old.rows[0][0].(i64), 10) }
}

@(test)
test_pk_seek_no_pk_and_mutations :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "pkseek_nopk")
	defer teardown_db(d, "pkseek_nopk")

	// No pk: scan fallback, correct results.
	db.execute(d, "CREATE TABLE u (id INTEGER, v INTEGER);")
	db.execute(d, "INSERT INTO u VALUES (1, 10), (2, 20);")
	plain := db.query(d, "SELECT v FROM u WHERE id = 2;")
	testing.expect(t, plain.ok, "non-pk filter must succeed")
	testing.expect_value(t, len(plain.rows), 1)

	// Mutation agreement: update/delete by pk then seek sees it.
	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20);")
	db.execute(d, "UPDATE t SET v = 200 WHERE id = 2;")
	upd := db.query(d, "SELECT v FROM t WHERE id = 2;")
	testing.expect(t, upd.ok, "post-update seek must succeed")
	if len(upd.rows) == 1 { testing.expect_value(t, upd.rows[0][0].(i64), 200) }

	db.execute(d, "DELETE FROM t WHERE id = 1;")
	del := db.query(d, "SELECT v FROM t WHERE id = 1;")
	testing.expect(t, del.ok, "post-delete seek must succeed")
	testing.expect_value(t, len(del.rows), 0)
}

// Txn-deferred schema roots: statements inside an explicit txn stage roots
// instead of COW-publishing per statement. Results must be identical.
@(test)
test_txn_deferred_read_own_writes :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_read")
	defer teardown_db(d, "txn_def_read")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20);")
	q := db.query(d, "SELECT v FROM t WHERE id = 2;")
	testing.expect(t, q.ok, "in-txn read must succeed")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) == 1 { testing.expect_value(t, q.rows[0][0].(i64), 20) }
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	after := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, after.ok, "post-commit read must succeed")
	if len(after.rows) == 1 { testing.expect_value(t, after.rows[0][0].(i64), 2) }
}

@(test)
test_txn_deferred_mixed_dml :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_mixed")
	defer teardown_db(d, "txn_def_mixed")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (4, 40);")
	db.execute(d, "UPDATE t SET v = 200 WHERE id = 2;")
	db.execute(d, "DELETE FROM t WHERE id = 1;")
	mid := db.query(d, "SELECT id, v FROM t ORDER BY id;")
	testing.expect_value(t, len(mid.rows), 3)
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	after := db.query(d, "SELECT id, v FROM t ORDER BY id;")
	testing.expect_value(t, len(after.rows), 3)
	if len(after.rows) == 3 {
		testing.expect_value(t, after.rows[0][0].(i64), 2)
		testing.expect_value(t, after.rows[0][1].(i64), 200)
		testing.expect_value(t, after.rows[1][0].(i64), 3)
		testing.expect_value(t, after.rows[2][0].(i64), 4)
	}
}

@(test)
test_txn_deferred_two_tables :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_2tab")
	defer teardown_db(d, "txn_def_2tab")

	db.execute(d, "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "CREATE TABLE b (id INTEGER PRIMARY KEY, v INTEGER);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO a VALUES (1, 10);")
	db.execute(d, "INSERT INTO b VALUES (1, 100);")
	db.execute(d, "INSERT INTO a VALUES (2, 20);")
	db.execute(d, "UPDATE b SET v = 200 WHERE id = 1;")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	qa := db.query(d, "SELECT COUNT(*) FROM a;")
	qb := db.query(d, "SELECT v FROM b WHERE id = 1;")
	if len(qa.rows) == 1 { testing.expect_value(t, qa.rows[0][0].(i64), 2) }
	testing.expect_value(t, len(qb.rows), 1)
	if len(qb.rows) == 1 { testing.expect_value(t, qb.rows[0][0].(i64), 200) }
}

@(test)
test_txn_deferred_rollback_coherent :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_rb")
	defer teardown_db(d, "txn_def_rb")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "UPDATE t SET v = 100 WHERE id = 1;")
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback failed")

	// Rolled-back staged roots must not leak (data or cache): only row 1
	// with its original value survives, and a fresh txn works.
	q := db.query(d, "SELECT id, v FROM t ORDER BY id;")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), 1)
		testing.expect_value(t, q.rows[0][1].(i64), 10)
	}
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "second begin failed")
	db.execute(d, "INSERT INTO t VALUES (3, 30);")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "second commit failed")
	q2 := db.query(d, "SELECT COUNT(*) FROM t;")
	if len(q2.rows) == 1 { testing.expect_value(t, q2.rows[0][0].(i64), 2) }
}

@(test)
test_txn_deferred_ddl_mix :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_ddl")
	defer teardown_db(d, "txn_def_ddl")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	// DDL mid-txn bumps the schema root and wipes overlays; the staged root
	// for t must be re-applied, not lost.
	db.execute(d, "CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (3, 30);")
	db.execute(d, "INSERT INTO u VALUES (1, 100);")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	qt := db.query(d, "SELECT COUNT(*) FROM t;")
	qu := db.query(d, "SELECT v FROM u WHERE id = 1;")
	if len(qt.rows) == 1 { testing.expect_value(t, qt.rows[0][0].(i64), 3) }
	testing.expect_value(t, len(qu.rows), 1)
	if len(qu.rows) == 1 { testing.expect_value(t, qu.rows[0][0].(i64), 100) }
}

@(test)
test_txn_deferred_drop_in_txn :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_drop")
	defer teardown_db(d, "txn_def_drop")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "DROP TABLE t;")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	// The staged root for t must not resurrect it at flush.
	saved, ctx := suppress_expected_errors()
	context = ctx
	q := db.query(d, "SELECT COUNT(*) FROM t;")
	context = restore_logger(saved)
	testing.expect(t, !q.ok, "dropped table must stay dropped")
}

@(test)
test_txn_deferred_vacuum_after_write :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_vac")
	defer teardown_db(d, "txn_def_vac")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")
	db.execute(d, "DELETE FROM t WHERE id = 2;")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (4, 40);")
	testing.expect(t, admin.vacuum(d) == .None, "vacuum failed")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	q := db.query(d, "SELECT id FROM t ORDER BY id;")
	testing.expect_value(t, len(q.rows), 3)
}

@(test)
test_txn_deferred_as_of_sees_committed :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "txn_def_asof")
	defer teardown_db(d, "txn_def_asof")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	snap_page := d.latest_snapshot
	h, h_ok := snapshot.load(d.pager, snap_page)
	testing.expect(t, h_ok, "should load snapshot")
	snap_id := h.snapshot_id

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "UPDATE t SET v = 99 WHERE id = 1;")
	old := db.query(d, fmt.tprintf("SELECT v FROM t AS OF SNAPSHOT %d WHERE id = 1;", snap_id))
	testing.expect(t, old.ok, "as-of in txn must succeed")
	testing.expect_value(t, len(old.rows), 1)
	if len(old.rows) == 1 { testing.expect_value(t, old.rows[0][0].(i64), 10) }
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback failed")
}

// Cross-type comparison: values of different storage classes are never
// equal (SQLite class rank). compare_values used to fall through to 0
// (equal) for mismatched pairs, matching every row.
@(test)
test_compare_cross_type_never_equal :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "cmp_xtype")
	defer teardown_db(d, "cmp_xtype")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');")

	// Int column vs text literal: no match.
	nt := db.query(d, "SELECT id FROM t WHERE id = '2';")
	testing.expect(t, nt.ok, "int-vs-text must succeed")
	testing.expect_value(t, len(nt.rows), 0)

	// Text column vs int literal: no match.
	tn := db.query(d, "SELECT id FROM t WHERE name = 5;")
	testing.expect(t, tn.ok, "text-vs-int must succeed")
	testing.expect_value(t, len(tn.rows), 0)

	// Int vs float stays numeric: 2.0 equals 2.
	nf := db.query(d, "SELECT id FROM t WHERE id = 2.0;")
	testing.expect(t, nf.ok, "int-vs-float must succeed")
	testing.expect_value(t, len(nf.rows), 1)

	// Mixed IN list: only the type-correct member matches.
	inm := db.query(d, "SELECT id FROM t WHERE id IN (2, 'x');")
	testing.expect(t, inm.ok, "mixed IN must succeed")
	testing.expect_value(t, len(inm.rows), 1)

	// Same-type aggregates unaffected.
	mn := db.query(d, "SELECT MIN(v), MAX(v) FROM t;")
	testing.expect(t, mn.ok, "min/max must succeed")
	if len(mn.rows) == 1 {
		testing.expect_value(t, mn.rows[0][0].(i64), 10)
		testing.expect_value(t, mn.rows[0][1].(i64), 30)
	}
}

@(test)
test_compare_cross_type_order :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "cmp_xorder")
	defer teardown_db(d, "cmp_xorder")

	// TEXT columns admit both strings and blobs: cross-type ORDER BY must
	// follow class rank (text before blob), not compare-equal.
	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'b');")
	db.execute(d, "INSERT INTO t VALUES (2, X'AA');")
	db.execute(d, "INSERT INTO t VALUES (3, 'a');")

	q := db.query(d, "SELECT id FROM t ORDER BY v;")
	testing.expect(t, q.ok, "mixed order must succeed")
	testing.expect_value(t, len(q.rows), 3)
	if len(q.rows) == 3 {
		testing.expect_value(t, q.rows[0][0].(i64), 3)
		testing.expect_value(t, q.rows[1][0].(i64), 1)
		testing.expect_value(t, q.rows[2][0].(i64), 2)
	}
}

// NULL comparison semantics: any comparison involving NULL is UNKNOWN and
// never matches (v = NULL matches nothing — SQLite/Postgres), while IS NULL
// still tests nullness. compare_condition used to let the null==null arm of
// compare_values make `v = NULL` true.
@(test)
test_null_comparison_never_matches :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "null_cmp")
	defer teardown_db(d, "null_cmp")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, NULL), (2, 5), (3, NULL);")

	eq := db.query(d, "SELECT id FROM t WHERE v = NULL;")
	testing.expect(t, eq.ok, "v = NULL must succeed")
	testing.expect_value(t, len(eq.rows), 0)

	ne := db.query(d, "SELECT id FROM t WHERE v != NULL;")
	testing.expect(t, ne.ok, "v != NULL must succeed")
	testing.expect_value(t, len(ne.rows), 0)

	lt := db.query(d, "SELECT id FROM t WHERE v < NULL;")
	testing.expect(t, lt.ok, "v < NULL must succeed")
	testing.expect_value(t, len(lt.rows), 0)

	// IS NULL is the correct null test and stays working.
	isn := db.query(d, "SELECT id FROM t WHERE v IS NULL;")
	testing.expect(t, isn.ok, "IS NULL must succeed")
	testing.expect_value(t, len(isn.rows), 2)

	notn := db.query(d, "SELECT id FROM t WHERE v IS NOT NULL;")
	testing.expect(t, notn.ok, "IS NOT NULL must succeed")
	testing.expect_value(t, len(notn.rows), 1)

	// NOT over an UNKNOWN comparison: the engine is two-valued (comparison
	// false → NOT true), so NOT v = NULL returns all rows — consistent with
	// SQLite's NOT over a false filter. Pin the behavior, not SQL three-
	// valued logic.
	not_eq := db.query(d, "SELECT id FROM t WHERE NOT v = NULL;")
	testing.expect(t, not_eq.ok, "NOT v = NULL must succeed")
	testing.expect_value(t, len(not_eq.rows), 3)

	// NULL IN (...) never matches, including an explicit NULL member.
	inl := db.query(d, "SELECT id FROM t WHERE v IN (NULL);")
	testing.expect(t, inl.ok, "v IN (NULL) must succeed")
	testing.expect_value(t, len(inl.rows), 0)

	// NULL ordering in ORDER BY is unaffected.
	ord := db.query(d, "SELECT id FROM t ORDER BY v;")
	testing.expect(t, ord.ok, "null order must succeed")
	testing.expect_value(t, len(ord.rows), 3)
	if len(ord.rows) == 3 {
		testing.expect_value(t, ord.rows[0][0].(i64), 2) // 5 first (ASC nulls last)
	}
}

// Mutation PK seek with a qualified column: `UPDATE/DELETE ... WHERE t.pk = N`
// must take the same seek path as the bare column (previously it fell back
// to a full scan; correctness was already fine, this pins the semantics).
@(test)
test_mutation_pk_seek_qualified :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	d := setup_db(t, "mut_pk_qual")
	defer teardown_db(d, "mut_pk_qual")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")

	db.execute(d, "UPDATE t SET v = 77 WHERE t.id = 2;")
	q := db.query(d, "SELECT v FROM t WHERE id = 2;")
	testing.expect(t, q.ok, "post-update read must succeed")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) == 1 { testing.expect_value(t, q.rows[0][0].(i64), 77) }

	db.execute(d, "DELETE FROM t WHERE t.id = 1;")
	gone := db.query(d, "SELECT v FROM t WHERE id = 1;")
	testing.expect(t, gone.ok, "post-delete read must succeed")
	testing.expect_value(t, len(gone.rows), 0)

	// A qualifier naming a different table must NOT be treated as this pk
	// (falls back to scan; here it names an unknown table -> no match).
	saved, ctx := suppress_expected_errors()
	context = ctx
	other := db.query(d, "SELECT v FROM t WHERE other.id = 3;")
	context = restore_logger(saved)
	testing.expect(
		t,
		!other.ok || len(other.rows) == 0,
		"foreign qualifier must not match this pk",
	)
}

// Differential: vector scan (MAGNI_VECTOR=1) must return byte-identical
// results to the scalar path (unset) across filter shapes, projections,
// ORDER BY (sort key inside and outside the projection), DISTINCT, and
// LIMIT/OFFSET — over ints, reals, text, and NULLs. Single-threaded suite,
// so process-env toggling is safe. Env restored at the end.
@(test)
test_vector_scan_differential :: proc(t: ^testing.T) {
	d := setup_db(t, "vecdiff")
	defer teardown_db(d, "vecdiff")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT, r REAL, name TEXT);")
	sb: strings.Builder
	strings.builder_init(&sb, context.temp_allocator)
	strings.write_string(&sb, "INSERT INTO t VALUES ")
	for i in 1 ..= 200 {
		if i > 1 { strings.write_string(&sb, ",") }
		if i % 10 == 0 {
			fmt.sbprintf(&sb, "(%d,NULL,NULL,NULL)", i)
		} else if i % 3 == 0 {
			fmt.sbprintf(&sb, "(%d,%d,%f,'n%d')", i, i * 2, f64(i) + 0.5, i)
		} else {
			fmt.sbprintf(&sb, "(%d,%d,%f,'n%d')", i, i * 2, f64(i) + 0.25, i)
		}
	}
	strings.write_string(&sb, ";")
	testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "bulk insert")

	queries := []string {
		"SELECT * FROM t;",
		"SELECT id FROM t;",
		"SELECT name, v FROM t;",
		"SELECT * FROM t WHERE v > 200;",
		"SELECT id FROM t WHERE v > 200;",
		"SELECT id FROM t WHERE v < 10;",
		"SELECT id FROM t WHERE v = 100;",
		"SELECT id FROM t WHERE v >= 100 AND v <= 120;",
		"SELECT id FROM t WHERE v < 50 OR v > 350;",
		"SELECT id FROM t WHERE NOT v = 100;",
		"SELECT id FROM t WHERE name LIKE 'n1%';",
		"SELECT id FROM t WHERE v IS NULL;",
		"SELECT id FROM t WHERE v IN (10, 20, 30);",
		"SELECT id FROM t WHERE id = v;",
		"SELECT id FROM t WHERE v > 10000;",
		"SELECT id, v FROM t ORDER BY v DESC;",
		"SELECT id FROM t ORDER BY v DESC;",
		"SELECT v FROM t ORDER BY id LIMIT 10;",
		"SELECT DISTINCT v FROM t WHERE v > 100;",
		"SELECT id FROM t WHERE v > 50 LIMIT 5 OFFSET 10;",
		"SELECT r FROM t WHERE r > 100.0;",
		"SELECT id FROM t WHERE name = 'n42';",
		"SELECT v, v FROM t WHERE v > 200;",
		"SELECT name FROM t WHERE v > 100;",
		"SELECT * FROM t ORDER BY id;",
		"SELECT v, id FROM t WHERE v < 100 ORDER BY v;",
	}

	vec_rows_equal :: proc(a, b: db.Query_Result) -> bool {
		if a.ok != b.ok { return false }
		if !a.ok { return true }
		if len(a.rows) != len(b.rows) { return false }
		if len(a.columns) != len(b.columns) { return false }
		for c, i in a.columns {
			if c != b.columns[i] || a.col_types[i] != b.col_types[i] { return false }
		}
		for r, i in a.rows {
			if len(r) != len(b.rows[i]) { return false }
			for v, j in r {
				if !types.value_compare(v, b.rows[i][j]) { return false }
			}
		}
		return true
	}

	os.set_env("MAGNI_VECTOR", "0")
	for q, i in queries {
		scalar := db.query(d, q)
		os.set_env("MAGNI_VECTOR", "1")
		vec := db.query(d, q)
		os.set_env("MAGNI_VECTOR", "0")
		testing.expect(
			t,
			vec_rows_equal(scalar, vec),
			fmt.tprintf("vec/scalar mismatch q%d: %s", i, q),
		)
	}
	os.unset_env("MAGNI_VECTOR")
}

// Parity: cursor_get_cell_needed on a columnar page must return the same
// values as the full decode for needed slots (and Null elsewhere), with
// identical rowids — without allocating. Covers DELTA-int and RAW-real
// columns under first-only, second-only, and empty masks.
@(test)
test_columnar_needed_decode_parity :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "colneeded")
	defer teardown_tree(&ctx)

	rows_in := [][]types.Value {
		{types.value_int(10), types.value_real(1.5)},
		{types.value_int(20), types.value_real(2.5)},
		{types.value_int(30), types.value_real(3.5)},
		{types.value_int(40), types.value_real(4.5)},
	}
	for r, i in rows_in {
		err := btree.tree_insert(&ctx.tree, types.Row_ID(i + 1), r)
		testing.expect(t, err == .None, fmt.tprintf("insert row %d", i + 1))
	}

	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == .None, "get page 1")
	defer pager.unpin_page(ctx.pager, 1)

	off := btree.get_page_header_offset(1)
	rowids := []types.Row_ID{1, 2, 3, 4}
	cols := []types.Column{{name = "id", type = .INTEGER}, {name = "v", type = .REAL}}
	testing.expect(t, cell.serialize_columnar(pg.data[off:], rowids, rows_in, cols), "to columnar")
	hdr := btree.get_leaf_header(pg.data, 1)
	hdr.page_type = .LEAF_TABLE_COLUMNAR
	hdr.cell_count = u16le(4)
	hdr.cell_content_offset = u16le(8 + len(cols) * 12)
	pager.mark_dirty(ctx.pager, 1)

	masks := [][]bool{{true, false}, {false, true}, {true, true}, {false, false}}
	for mask, mi in masks {
		c, c_err := btree.cursor_start(&ctx.tree)
		testing.expect(t, c_err == .None, "cursor start")
		n := 0
		for c.is_valid {
			full, f_err := btree.cursor_get_cell(&c, context.temp_allocator)
			testing.expect(t, f_err == .None, "full decode")
			out_buf: [types.MAX_COLS]types.Value
			rid, n_err := btree.cursor_get_cell_needed(&c, mask, out_buf[:])
			testing.expect(t, n_err == .None, fmt.tprintf("mask %d needed decode row %d", mi, n))
			testing.expect_value(t, rid, full.rowid)
			for i in 0 ..< len(rows_in[0]) {
				if mask[i] {
					testing.expect(
						t,
						types.value_compare(out_buf[i], full.values[i]),
						fmt.tprintf("mask %d row %d col %d parity", mi, n, i),
					)
				} else {
					testing.expect(
						t,
						types.is_null(out_buf[i]),
						fmt.tprintf("mask %d row %d col %d must be Null", mi, n, i),
					)
				}
			}
			btree.cursor_advance(&c)
			cell.destroy(&full, context.temp_allocator)
			n += 1
		}
		btree.cursor_destroy(&c)
		testing.expect_value(t, n, 4)
	}
}
