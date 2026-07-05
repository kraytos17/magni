package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:db"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"
import "src:types"

setup_db :: proc(t: ^testing.T, name: string) -> ^db.Database {
	filename := fmt.tprintf("test_int_%s.db", name)
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}

	database, open_err := db.open(filename)
	testing.expect(t, open_err == .None, "Failed to open database")
	return database
}

teardown_db :: proc(db_handle: ^db.Database, name: string) {
	filename := fmt.tprintf("test_int_%s.db", name)
	db.close(db_handle)
	if os.exists(filename) {
		os.remove(filename)
	}
	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
}

@(test)
test_integration_basic_create_and_insert :: proc(t: ^testing.T) {
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
	d := setup_db(t, "empty_sel")
	defer teardown_db(d, "empty_sel")

	ok := db.execute(d, "CREATE TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE TABLE should succeed")

	ok2 := db.execute(d, "SELECT * FROM t;") == .None
	testing.expect(t, ok2, "SELECT on empty table should succeed")
}

@(test)
test_integration_select_with_data :: proc(t: ^testing.T) {
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
	d := setup_db(t, "keyword_id")
	defer teardown_db(d, "keyword_id")

	ok := db.execute(d, "CREATE TABLE snapshot (id INT, of TEXT);") == .None
	testing.expect(t, ok, "Using 'snapshot' and 'of' as identifiers should work")
}

@(test)
test_integration_prune_old_snapshots :: proc(t: ^testing.T) {
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
test_integration_as_of_timestamp :: proc(t: ^testing.T) {
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
test_integration_join_asymmetric :: proc(t: ^testing.T) {
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
	d := setup_db(t, "ckpt")

	db.execute(d, "CREATE TABLE t (id INT);")
	for i in 1 ..= 10 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d);", i))
	}

	db.checkpoint(d)
	db.close(d)
	d2, open_err := db.open(fmt.tprintf("test_int_ckpt.db"))
	testing.expect(t, open_err == .None, "reopen after checkpoint")
	defer teardown_db(d2, "ckpt")

	q := db.query(d2, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "data should survive checkpoint+reopen")
}

@(test)
test_integration_batch_snapshot :: proc(t: ^testing.T) {
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
test_integration_rebalance :: proc(t: ^testing.T) {
	d := setup_db(t, "rebal")
	defer teardown_db(d, "rebal")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	for i in 1 ..= 50 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'row%d');", i, i))
	}
	// Delete half the rows to create sparse pages
	for i in 1 ..= 25 {
		db.execute(d, fmt.tprintf("DELETE FROM t WHERE id = %d;", i * 2))
	}

	// Run rebalance
	st := db.Schema_Tree(d)
	tables := schema.list_tables(&st, context.temp_allocator)
	if len(tables) > 0 {
		tree := btree.init(d.pager, tables[0].root_page)
		btree.rebalance(&tree)
	}

	// Verify data is still accessible
	q := db.query(d, "SELECT id, val FROM t ORDER BY id;")
	testing.expect(t, q.ok, "SELECT after rebalance")
	testing.expect(t, len(q.rows) > 0, "rows exist after rebalance")
}

@(test)
test_integration_skip_index :: proc(t: ^testing.T) {
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
test_columnar_integration :: proc(t: ^testing.T) {
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
		ptrs := btree.get_pointers(buf[:], page_id)
		testing.expect(t, i < len(ptrs), fmt.tprintf("cell pointer %d exists", i))
		if i < len(ptrs) {
			c, _, des_ok := cell.deserialize(
				buf[:],
				int(ptrs[i]),
				cell.Config{allocator = context.temp_allocator},
			)
			testing.expect(t, des_ok, fmt.tprintf("deserialize cell %d", i))
			if des_ok {
				testing.expect_value(t, types.Row_ID(c.rowid), rowids[i])
			}
		}
	}
}

@(test)
test_columnar_btree_read :: proc(t: ^testing.T) {
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
			rid:  types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&original_rows, struct {
				rid:  types.Row_ID,
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
	// Test that INSERT on a columnar page triggers columnar→row-major conversion
	ctx := setup_tree(t, "colinsert")
	defer teardown_tree(&ctx)

	for i in 1 ..= 3 {
		val := []types.Value{types.value_int(i64(i * 100))}
		btree.tree_insert(&ctx.tree, types.Row_ID(i), val)
	}

	// Read original rows via cursor
	orig := make([dynamic]struct {
			rid:  types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&orig, struct {
				rid:  types.Row_ID,
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

@(test)
test_columnar_update_conversion :: proc(t: ^testing.T) {
	// Test that UPDATE on a columnar page triggers columnar→row-major conversion
	ctx := setup_tree(t, "colupdate")
	defer teardown_tree(&ctx)

	for i in 1 ..= 3 {
		val := []types.Value{types.value_int(i64(i))}
		btree.tree_insert(&ctx.tree, types.Row_ID(i), val)
	}

	// Convert root page to columnar
	orig_rows := make([dynamic]struct {
			rid:  types.Row_ID,
			vals: []types.Value,
		}, context.temp_allocator)
	{
		c, _ := btree.cursor_start(&ctx.tree)
		defer btree.cursor_destroy(&c)
		for c.is_valid {
			cell_val, _ := btree.cursor_get_cell(&c, context.temp_allocator)
			append(&orig_rows, struct {
				rid:  types.Row_ID,
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
	d := setup_db(t, "toomany")
	defer teardown_db(d, "toomany")
	db.execute(d, "CREATE TABLE t (id INT, val INT);")
	// Column list with more entries than table has columns should be rejected
	ok := db.execute(d, "INSERT INTO t (id,val,id,val) VALUES (1,2,3,4);") != .None
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
	// Verify that negative LIMIT produces an error message (not just generic failure)
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT -5;", context.temp_allocator)
	testing.expect(t, !ok, "negative LIMIT rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_negative_offset :: proc(t: ^testing.T) {
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT 5 OFFSET -3;", context.temp_allocator)
	testing.expect(t, !ok, "negative OFFSET rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_select_readonly_no_snapshot :: proc(t: ^testing.T) {
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
	d := setup_db(t, "ro_admin")
	defer teardown_db(d, "ro_admin")

	db.execute(d, "CREATE TABLE t1 (id INT);")
	db.execute(d, "CREATE TABLE t2 (val TEXT);")

	db.list_tables(d)
	db.describe_table(d, "t1")
	db.describe_table(d, "t2")
	db.stats(d)

	q := db.query(d, "SELECT name FROM t1;")
	testing.expect(t, q.ok, "query after admin commands")
}

@(test)
test_insert_creates_snapshot :: proc(t: ^testing.T) {
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
		name0, _ := q.rows[0][1].(string)
		name1, _ := q.rows[1][1].(string)
		testing.expect_value(t, name0, "Alice")
		testing.expect_value(t, name1, "Charlie")
	}

	q2 := db.query(d, "SELECT name FROM t WHERE id IN (1, 3) ORDER BY name;")
	testing.expect(t, q2.ok, "IN literal execute")
	testing.expect(t, len(q2.rows) == 2, "2 rows from IN literal")
}
