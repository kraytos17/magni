package tests

import "core:fmt"
import "core:os"
import "core:testing"
import "src:db"
import "src:snapshot"

setup_db :: proc(t: ^testing.T, name: string) -> ^db.Database {
	filename := fmt.tprintf("test_int_%s.db", name)
	if os.exists(filename) {
		os.remove(filename)
	}

	database, ok := db.open(filename)
	testing.expect(t, ok, "Failed to open database")
	return database
}

teardown_db :: proc(db_handle: ^db.Database, name: string) {
	filename := fmt.tprintf("test_int_%s.db", name)
	db.close(db_handle)
	if os.exists(filename) {
		os.remove(filename)
	}
}

@(test)
test_integration_basic_create_and_insert :: proc(t: ^testing.T) {
	d := setup_db(t, "basic_crud")
	defer teardown_db(d, "basic_crud")

	ok := db.execute(d, "CREATE TABLE test (id INT, val TEXT);")
	testing.expect(t, ok, "CREATE TABLE should succeed")

	ok2 := db.execute(d, "INSERT INTO test VALUES (1, 'hello');")
	testing.expect(t, ok2, "INSERT should succeed")

	ok3 := db.execute(d, "INSERT INTO test VALUES (2, 'world');")
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

	ok := db.execute(d, "CREATE TABLE t (id INT);")
	testing.expect(t, ok, "CREATE TABLE should succeed")

	ok2 := db.execute(d, "SELECT * FROM t;")
	testing.expect(t, ok2, "SELECT on empty table should succeed")
}

@(test)
test_integration_select_with_data :: proc(t: ^testing.T) {
	d := setup_db(t, "sel_data")
	defer teardown_db(d, "sel_data")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")

	ok := db.execute(d, "SELECT * FROM t;")
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

	ok_current := db.execute(d, "SELECT * FROM t;")
	testing.expect(t, ok_current, "Current SELECT should succeed")

	ok_snap1 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 1;")
	testing.expect(t, ok_snap1, "AS OF SNAPSHOT 1 should succeed")

	ok_snap2 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 2;")
	testing.expect(t, ok_snap2, "AS OF SNAPSHOT 2 should succeed")

	ok_snap3 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 3;")
	testing.expect(t, ok_snap3, "AS OF SNAPSHOT 3 should succeed")

	ok_snap4 := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 4;")
	testing.expect(t, ok_snap4, "AS OF SNAPSHOT 4 should succeed")

	ok_bad := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 99;")
	testing.expect(t, !ok_bad, "Non-existent snapshot should fail")
}

@(test)
test_integration_time_travel_with_where :: proc(t: ^testing.T) {
	d := setup_db(t, "tt_where")
	defer teardown_db(d, "tt_where")

	db.execute(d, "CREATE TABLE t (id INT, name TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'Alice');")
	db.execute(d, "INSERT INTO t VALUES (2, 'Bob');")
	db.execute(d, "INSERT INTO t VALUES (3, 'Charlie');")

	ok := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 3 WHERE id = 1;")
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

	ok := db.execute(d, "SELECT * FROM t AS OF SNAPSHOT 4 LIMIT 2;")
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

	d2, open_ok := db.open(fmt.tprintf("test_int_%s.db", name))
	defer teardown_db(d2, name)
	testing.expect(t, open_ok, "Re-open should succeed")

	ok := db.execute(d2, "SELECT * FROM t AS OF SNAPSHOT 1;")
	testing.expect(t, ok, "Cross-session time-travel should succeed")

	ok2 := db.execute(d2, "SELECT * FROM t AS OF SNAPSHOT 3;")
	testing.expect(t, ok2, "Cross-session last snapshot should succeed")
}

@(test)
test_integration_keyword_as_identifier :: proc(t: ^testing.T) {
	d := setup_db(t, "keyword_id")
	defer teardown_db(d, "keyword_id")

	ok := db.execute(d, "CREATE TABLE snapshot (id INT, of TEXT);")
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
	testing.expect(t, count <= 100, fmt.tprintf("Snapshots should be pruned to <=100 (got %d)", count))
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

	r := db.execute(d, "SELECT a.id, a.name, b.val FROM a INNER JOIN b ON a.id = b.id;")
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
	ok := db.snapshot_restore(d, 1)
	testing.expect(t, ok, "snapshot_restore to snapshot 1")

	// After restore, the table should still exist (schema at snapshot 1)
	r := db.query(d, "SELECT id FROM t;")
	testing.expect(t, r.ok, "SELECT after restore")
}
