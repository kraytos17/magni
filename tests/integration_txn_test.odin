// Integration transaction tests: BEGIN/COMMIT/ROLLBACK, snapshots,
// WAL behavior, vacuum/GC/expiry, crash-adjacent recovery, and deferred
// (staged) transaction semantics. File-backed through setup_db/teardown_db.
package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:db"
import "src:pager"
import "src:snapshot"
import "src:types"

@(test)
test_integration_transaction_and_snapshot_chain :: proc(t: ^testing.T) {
	d := setup_db(t, "txn_snap")
	defer teardown_db(d, "txn_snap")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (id INT);",
			"INSERT INTO t VALUES (1);",
			"INSERT INTO t VALUES (2);",
			"INSERT INTO t VALUES (3);",
		},
	)

	snap_page := d.latest_snapshot
	testing.expect(t, snap_page > 0, "Should have at least one snapshot")
}

@(test)
test_integration_bulk_rollback :: proc(t: ^testing.T) {
	d := setup_db(t, "bulk_rollback")
	defer teardown_db(d, "bulk_rollback")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);")
	schema_root_before := d.schema_root_page

	// Multi-row bulk into the empty table, then rollback: fresh bulk pages
	// truncate away, the schema root is restored, and the table reads empty.
	db.begin(d)
	db.execute(d, "INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b'), (5, 'e'), (4, 'd');")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok && len(q.rows) == 1, "COUNT should run inside txn")
	db.rollback(d)
	testing.expect_value(t, d.schema_root_page, schema_root_before)

	q2 := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q2.ok, "COUNT should run after rollback")
	if q2.ok && len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), 0)
	}
}

@(test)
test_integration_rollback_restores_schema :: proc(t: ^testing.T) {
	d := setup_db(t, "rollback")
	defer teardown_db(d, "rollback")

	run_db_setup(t, d, {"CREATE TABLE t (id INT);", "INSERT INTO t VALUES (1);"})
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
	run_db_setup(
		t,
		d,
		{"CREATE TABLE t (id INT);", "INSERT INTO t VALUES (1);", "INSERT INTO t VALUES (2);"},
	)
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
test_integration_snapshot_restore :: proc(t: ^testing.T) {
	d := setup_db(t, "restore")
	defer teardown_db(d, "restore")

	run_db_setup(
		t,
		d,
		{"CREATE TABLE t (id INT);", "INSERT INTO t VALUES (1);", "INSERT INTO t VALUES (2);"},
	)

	// Restore to snapshot 1 (after CREATE, before first INSERT)
	ok := db.snapshot_restore(d, 1) == .None
	testing.expect(t, ok, "snapshot_restore to snapshot 1")

	// After restore, the table should still exist (schema at snapshot 1)
	r := db.query(d, "SELECT id FROM t;")
	testing.expect(t, r.ok, "SELECT after restore")
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
	if q.ok && len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), 100)
	}
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

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (id INT, name TEXT);",
			"INSERT INTO t VALUES (1, 'Alice');",
			"INSERT INTO t VALUES (2, 'Bob');",
		},
	)

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
	if os.exists(filename) {
		os.remove(filename)
	}
	if os.exists(wal_path) {
		os.remove(wal_path)
	}
	defer os.remove(filename)
	defer os.remove(wal_path)

	d, open_err := db.open(filename, db.Open_Config{wal_size_threshold = 2})
	testing.expect(t, open_err == .None, "open with wal_size_threshold")
	if open_err != .None {
		return
	}

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
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
	defer os.remove(filename)
	defer os.remove(wal_name)

	d, open_err := db.open(filename, db.Open_Config{snapshot_batch_threshold = 3})
	testing.expect(t, open_err == .None, "open with snapshot_batch_threshold")
	if open_err != .None {
		return
	}
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
test_integration_multipage_tree :: proc(t: ^testing.T) {
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
	d := setup_db(t, "delmp")
	defer teardown_db(d, "delmp")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 {
				strings.write_string(&sb, ",")
			}
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	// Delete the leading 700 rows: this empties entire leading leaves, and
	// the cursor must still find the surviving rows (no empty-tree shortcut
	// on partially emptied trees).
	ids: strings.Builder
	strings.builder_init(&ids, context.temp_allocator)
	for id in 1 ..= 700 {
		if id > 1 {
			strings.write_string(&ids, ",")
		}
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
test_integration_vacuum_boundary_sweep :: proc(t: ^testing.T) {
	d := setup_db(t, "vac_sweep")
	defer teardown_db(d, "vac_sweep")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);") == .None,
		"create",
	)
	// Single-row loop inserts (split-built, correct): 2000 rows across
	// many leaves so vacuumed interiors have many chunk boundaries.
	for i in 1 ..= 2000 {
		free_all(context.temp_allocator)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 2)) == .None,
			"insert",
		)
	}
	free_all(context.temp_allocator)

	sweep_all :: proc(t: ^testing.T, d: ^db.Database, label: string) -> int {
		missing := 0
		for i in 1 ..= 2000 {
			free_all(context.temp_allocator)
			r := db.query(d, fmt.tprintf("SELECT v FROM t WHERE id = %d;", i))
			if !r.ok || len(r.rows) != 1 {
				missing += 1
			}
		}
		free_all(context.temp_allocator)
		if missing != 0 {
			testing.expect(t, false, fmt.tprintf("%s: %d rows unfindable", label, missing))
		}
		return missing
	}

	testing.expect_value(t, sweep_all(t, d, "pre-vacuum"), 0)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")
	testing.expect_value(t, sweep_all(t, d, "post-vacuum"), 0)
}

@(test)
test_integration_vacuum_text_boundary_sweep :: proc(t: ^testing.T) {
	d := setup_db(t, "vac_text_sweep")
	defer teardown_db(d, "vac_text_sweep")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)
	for i in 1 ..= 2000 {
		free_all(context.temp_allocator)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO docs VALUES (%d, 'w%04d');", i, i)) == .None,
			"insert",
		)
	}
	free_all(context.temp_allocator)

	sweep_text :: proc(t: ^testing.T, d: ^db.Database, label: string) -> int {
		missing := 0
		for i in 1 ..= 2000 {
			free_all(context.temp_allocator)
			r := db.query(d, fmt.tprintf("SELECT id FROM docs WHERE body = 'w%04d';", i))
			if !r.ok || len(r.rows) != 1 {
				missing += 1
			}
		}
		free_all(context.temp_allocator)
		if missing != 0 {
			testing.expect(t, false, fmt.tprintf("%s: %d bodies unfindable", label, missing))
		}
		return missing
	}

	testing.expect_value(t, sweep_text(t, d, "pre-vacuum"), 0)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")
	testing.expect_value(t, sweep_text(t, d, "post-vacuum"), 0)
}

@(test)
test_integration_vacuum :: proc(t: ^testing.T) {
	d := setup_db(t, "vacuum")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 20 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 {
				strings.write_string(&sb, ",")
			}
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
		if id > 1 {
			strings.write_string(&ids, ",")
		}
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
test_integration_random_delete_roundtrip :: proc(t: ^testing.T) {
	d := setup_db(t, "delround")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 {
				strings.write_string(&sb, ",")
			}
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
test_integration_delete_then_vacuum :: proc(t: ^testing.T) {
	d := setup_db(t, "delvac")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	sb: strings.Builder
	for chunk in 0 ..< 10 {
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< 100 {
			if i > 0 {
				strings.write_string(&sb, ",")
			}
			id := chunk * 100 + i + 1
			fmt.sbprintf(&sb, "(%d,%d)", id, id * 2)
		}
		strings.write_string(&sb, ";")
		testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "chunk insert")
	}

	ids: strings.Builder
	strings.builder_init(&ids, context.temp_allocator)
	for id in 1 ..= 800 {
		if id > 1 {
			strings.write_string(&ids, ",")
		}
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
test_db_rejects_v1_format :: proc(t: ^testing.T) {
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
	// Bulk INT+TEXT+REAL inserts must all be readable (COUNT 0 here is
	// data loss). Keep the volume modest: the 50k-row CLI sweep covers
	// scale, this pins the pattern.
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
	// Roots stay marked (an early return would free the live subtree),
	// and free_page must link non-cached pages into the freelist.
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
test_txn_deferred_read_own_writes :: proc(t: ^testing.T) {
	d := setup_db(t, "txn_def_read")
	defer teardown_db(d, "txn_def_read")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin failed")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20);")
	q := db.query(d, "SELECT v FROM t WHERE id = 2;")
	testing.expect(t, q.ok, "in-txn read must succeed")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), 20)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit failed")

	after := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, after.ok, "post-commit read must succeed")
	if len(after.rows) == 1 {
		testing.expect_value(t, after.rows[0][0].(i64), 2)
	}
}

@(test)
test_txn_deferred_mixed_dml :: proc(t: ^testing.T) {
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
	if len(qa.rows) == 1 {
		testing.expect_value(t, qa.rows[0][0].(i64), 2)
	}
	testing.expect_value(t, len(qb.rows), 1)
	if len(qb.rows) == 1 {
		testing.expect_value(t, qb.rows[0][0].(i64), 200)
	}
}

@(test)
test_txn_deferred_rollback_coherent :: proc(t: ^testing.T) {
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
	if len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), 2)
	}
}

@(test)
test_txn_deferred_ddl_mix :: proc(t: ^testing.T) {
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
	if len(qt.rows) == 1 {
		testing.expect_value(t, qt.rows[0][0].(i64), 3)
	}
	testing.expect_value(t, len(qu.rows), 1)
	if len(qu.rows) == 1 {
		testing.expect_value(t, qu.rows[0][0].(i64), 100)
	}
}

@(test)
test_txn_deferred_drop_in_txn :: proc(t: ^testing.T) {
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
	if len(old.rows) == 1 {
		testing.expect_value(t, old.rows[0][0].(i64), 10)
	}
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback failed")
}

// Cross-type comparison: values of different storage classes are never
// equal (SQLite class rank). Mismatched pairs must not fall through to 0
// (equal): that would match every row.
@(test)
test_txn_cache_survives_temp_free :: proc(t: ^testing.T) {
	// The table cache owns its map keys (cloned): with per-statement temp
	// frees in-txn, unowned keys would dangle and lookups would silently
	// serve stale roots, losing rows with no error.
	d := setup_db(t, "idxctl")
	defer teardown_db(d, "idxctl")
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	bodies := []string{"alpha", "bb", "alpha", "", "gamma"}
	for i in 1 ..= 25 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		sql := fmt.tprintf("INSERT INTO docs VALUES (%d, '%s', %d);", i, body, i * 10)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
	}
	free_all(context.temp_allocator)
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	q := db.query(d, "SELECT COUNT(*) FROM docs;")
	testing.expect(t, q.ok && len(q.rows) == 1, "count queries")
	if q.ok && len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(25))
	}
}
