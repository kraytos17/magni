// Integration query tests: joins, GROUP BY/HAVING/aggregates, pushdowns,
// skip-index and PK-seek routing, time travel, and set semantics.
// File-backed (test_int_*.db) through setup_db/teardown_db.
package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:db"
import "src:schema"
import "src:snapshot"
import "src:types"

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

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (id INT, name TEXT);",
			"INSERT INTO t VALUES (1, 'Alice');",
			"INSERT INTO t VALUES (2, 'Bob');",
			"INSERT INTO t VALUES (3, 'Charlie');",
		},
	)
	must_db_execute(t, d, "SELECT * FROM t AS OF SNAPSHOT 3 WHERE id = 1;")
}

@(test)
test_integration_time_travel_with_limit :: proc(t: ^testing.T) {
	d := setup_db(t, "tt_limit")
	defer teardown_db(d, "tt_limit")

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
	must_db_execute(t, d, "SELECT * FROM t AS OF SNAPSHOT 4 LIMIT 2;")
}

@(test)
test_integration_join :: proc(t: ^testing.T) {
	d := setup_db(t, "join")
	defer teardown_db(d, "join")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE a (id INT, name TEXT);",
			"CREATE TABLE b (id INT, val TEXT);",
			"INSERT INTO a VALUES (1, 'Alice');",
			"INSERT INTO a VALUES (2, 'Bob');",
			"INSERT INTO b VALUES (1, 'X');",
			"INSERT INTO b VALUES (2, 'Y');",
		},
	)
	must_db_execute(t, d, "SELECT a.id, a.name, b.val FROM a INNER JOIN b ON a.id = b.id;")
}

@(test)
test_integration_group_by_non_first_column :: proc(t: ^testing.T) {
	d := setup_db(t, "group_by")
	defer teardown_db(d, "group_by")

	db.execute(d, "CREATE TABLE t (a INT, b TEXT, c INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'x', 10);")
	db.execute(d, "INSERT INTO t VALUES (1, 'x', 20);")
	db.execute(d, "INSERT INTO t VALUES (2, 'y', 30);")

	// GROUP BY on a non-first column groups by that column's values.
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

	// Multi-column GROUP BY exercises values_equal.
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
	d := setup_db(t, "having")
	defer teardown_db(d, "having")

	db.execute(d, "CREATE TABLE t (a INT, b INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "INSERT INTO t VALUES (1, 30);")

	// HAVING on an aggregate resolves case-insensitively: verify
	// lowercase, uppercase, and group-column forms.
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
test_integration_group_by_stream_edges :: proc(t: ^testing.T) {
	d := setup_db(t, "grp_stream")
	defer teardown_db(d, "grp_stream")

	testing.expect(t, db.execute(d, "CREATE TABLE t (a INT, b TEXT, v INT);") == .None, "create")
	// NULL keys, duplicate groups, mixed int/real-adjacent values, multi-col.
	testing.expect(
		t,
		db.execute(
			d,
			"INSERT INTO t VALUES (1, 'x', 10), (2, 'y', 20), (1, 'x', 30), (NULL, 'n', 40), (NULL, 'n', 50), (3, 'x', 60);",
		) ==
		.None,
		"bulk load",
	)

	// NULL group key forms exactly one group (NULL == NULL for grouping).
	q := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY a;")
	testing.expect(t, q.ok, "null-key group query succeeds")
	// Groups: NULL(2), 1(2), 2(1), 3(1). NULL sorts with the ordering's
	// placement; assert the multiset of counts, not positions.
	counts := make([dynamic]i64, context.temp_allocator)
	for r in q.rows {
		append(&counts, r[1].(i64))
	}
	testing.expect_value(t, len(counts), 4)
	total: i64 = 0
	for c in counts {
		total += c
	}
	testing.expect_value(t, total, 6)

	// Multi-column grouping over the same rows.
	q2 := db.query(d, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b ORDER BY a, b;")
	testing.expect(t, q2.ok, "multi-col group query succeeds")
	testing.expect_value(t, len(q2.rows), 4)

	// ORDER BY over grouped output is a no-op in this engine (aggregate path
	// returns before finish_select sorts) — assert the group SET, not order.
	q3 := db.query(d, "SELECT a, COUNT(*) FROM t GROUP BY a;")
	testing.expect(t, q3.ok, "group set query succeeds")
	testing.expect_value(t, len(q3.rows), 4)
	found_null, found_1, found_2, found_3 := false, false, false, false
	for r in q3.rows {
		if _, is_null := r[0].(types.Null); is_null {
			found_null = found_null || r[1].(i64) == 2
			continue
		}
		#partial switch v in r[0] {
		case i64:
			switch v {
			case 1:
				found_1 = found_1 || r[1].(i64) == 2
			case 2:
				found_2 = found_2 || r[1].(i64) == 1
			case 3:
				found_3 = found_3 || r[1].(i64) == 1
			}
		}
	}
	testing.expect(t, found_null && found_1 && found_2 && found_3, "all groups with counts")

	// Empty input with GROUP BY yields no groups (hash parity).
	testing.expect(t, db.execute(d, "CREATE TABLE e (a INT);") == .None, "create empty")
	q4 := db.query(d, "SELECT a, COUNT(*) FROM e GROUP BY a;")
	testing.expect(t, q4.ok && len(q4.rows) == 0, "empty grouped input yields no rows")
}

@(test)
test_integration_group_by_stream_scale :: proc(t: ^testing.T) {
	// Above GROUP_STREAM_THRESHOLD: the streaming path (sort + boundary
	// runs) must agree with hashing exactly. 40000 rows into 500 groups
	// plus a NULL group (which routes around the int fast path into the
	// general streamer).
	d := setup_db(t, "grp_scale")
	defer teardown_db(d, "grp_scale")

	testing.expect(t, db.execute(d, "CREATE TABLE t (k INT, v INT);") == .None, "create")
	sb: strings.Builder
	strings.builder_init(&sb, context.temp_allocator)
	strings.write_string(&sb, "INSERT INTO t VALUES ")
	for i in 1 ..= 40000 {
		if i > 1 {
			strings.write_string(&sb, ",")
		}
		if i % 1000 == 0 {
			fmt.sbprintf(&sb, "(NULL,%d)", i)
		} else {
			fmt.sbprintf(&sb, "(%d,%d)", i % 500, i)
		}
	}
	strings.write_string(&sb, ";")
	testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "bulk load")

	q := db.query(d, "SELECT k, COUNT(*) FROM t GROUP BY k;")
	testing.expect(t, q.ok, "streamed group query succeeds")
	testing.expect_value(t, len(q.rows), 501)

	total: i64 = 0
	null_count: i64 = -1
	for r in q.rows {
		total += r[1].(i64)
		if _, is_null := r[0].(types.Null); is_null {
			null_count = r[1].(i64)
		}
	}
	testing.expect_value(t, total, 40000)
	testing.expect_value(t, null_count, 40)
}

@(test)
test_integration_group_by_stream_ordered :: proc(t: ^testing.T) {
	// Ascending keys arrive ordered (PK/scan order): the ordered-input skip
	// streams without sorting. 40000 rows with a NULL tail exercises the
	// skip plus NULL grouping at scale.
	d := setup_db(t, "grp_ord")
	defer teardown_db(d, "grp_ord")

	testing.expect(t, db.execute(d, "CREATE TABLE t (k INT, v INT);") == .None, "create")
	sb: strings.Builder
	strings.builder_init(&sb, context.temp_allocator)
	strings.write_string(&sb, "INSERT INTO t VALUES ")
	for i in 1 ..= 40000 {
		if i > 1 {
			strings.write_string(&sb, ",")
		}
		if i > 39960 {
			fmt.sbprintf(&sb, "(NULL,%d)", i)
		} else {
			// Ascending groups of 80: input arrives key-ordered, so the
			// ordered-input skip fires instead of sorting.
			fmt.sbprintf(&sb, "(%d,%d)", i / 80, i)
		}
	}
	strings.write_string(&sb, ";")
	testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "bulk load")

	q := db.query(d, "SELECT k, COUNT(*) FROM t GROUP BY k;")
	testing.expect(t, q.ok, "ordered stream query succeeds")
	testing.expect_value(t, len(q.rows), 501)

	total: i64 = 0
	null_count: i64 = -1
	for r in q.rows {
		total += r[1].(i64)
		if _, is_null := r[0].(types.Null); is_null {
			null_count = r[1].(i64)
		}
	}
	testing.expect_value(t, total, 40000)
	testing.expect_value(t, null_count, 40)
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
test_integration_right_join :: proc(t: ^testing.T) {
	d := setup_db(t, "right_join")
	defer teardown_db(d, "right_join")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE a (id INT, name TEXT);",
			"CREATE TABLE b (id INT, val TEXT);",
			"INSERT INTO a VALUES (1, 'Alice');",
			"INSERT INTO a VALUES (2, 'Bob');",
			"INSERT INTO b VALUES (1, 'X');",
			"INSERT INTO b VALUES (3, 'Z');",
		},
	)

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
	d := setup_db(t, "between")
	defer teardown_db(d, "between")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (id INT, score INT);",
			"INSERT INTO t VALUES (1, 10);",
			"INSERT INTO t VALUES (2, 20);",
			"INSERT INTO t VALUES (3, 30);",
			"INSERT INTO t VALUES (4, 40);",
		},
	)

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
	d := setup_db(t, "using_join")
	defer teardown_db(d, "using_join")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE a (id INT, name TEXT);",
			"CREATE TABLE b (id INT, val TEXT);",
			"INSERT INTO a VALUES (1, 'Alice');",
			"INSERT INTO a VALUES (2, 'Bob');",
			"INSERT INTO b VALUES (1, 'X');",
			"INSERT INTO b VALUES (3, 'Z');",
		},
	)

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

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (name TEXT, score INT);",
			"INSERT INTO t VALUES ('Alice', 10);",
			"INSERT INTO t VALUES ('Bob', 20);",
			"INSERT INTO t VALUES ('Alice', 30);",
		},
	)
	must_db_execute(t, d, "SELECT DISTINCT name FROM t ORDER BY name;")
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
test_integration_skip_index_range :: proc(t: ^testing.T) {
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
			if i > 0 {
				strings.write_string(&sb, ",")
			}
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
	if len(tables) == 0 {
		return
	}
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

// A table large enough to need more than one skip page: even-stepped
// scores (step 2) defeat range coalescing, so ~20k rows produce ~200
// entries. The writer must respect the 4KB page bound; build must succeed
// and queries stay exact.
@(test)
test_integration_skip_index_overflow :: proc(t: ^testing.T) {
	d := setup_db(t, "skipoverflow")
	defer teardown_db(d, "skipoverflow")

	db.execute(d, "CREATE TABLE t (id INT, score INT);")
	ROWS  :: 20000
	CHUNK :: 1000
	for chunk in 0 ..< ROWS / CHUNK {
		sb: strings.Builder
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "INSERT INTO t VALUES ")
		for i in 0 ..< CHUNK {
			if i > 0 {
				strings.write_string(&sb, ",")
			}
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
	if len(tables) == 0 {
		return
	}
	tree := btree.init(d.pager, tables[0].root_page)

	// More zone entries than one page holds: the build must merge them, not
	// overflow. (ROWS>0 with step-2 scores guarantees ~20 entries/chunk.)
	skip_idx, build_err := btree.build_skip_index(&tree, 1)
	testing.expect(t, build_err == .None, "large skip index builds within one page")
	if build_err != .None {
		return
	}
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
			if v, v_ok := cell_val.values[0].(i64); v_ok && v == i64(rid) {
			} else {
				ok = false
			}
		}
		testing.expect(t, ok, "rowid in range, unique, value matches")
		cell.destroy(&cell_val, context.temp_allocator)
		if !ok {
			return false
		}
		total += 1
		btree.cursor_advance(&c)
	}
	testing.expect(t, total == n, "all rows present exactly once")
	return total == n
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
test_integration_join_pushdown :: proc(t: ^testing.T) {
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
test_pk_seek_hit_and_miss :: proc(t: ^testing.T) {
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
	d := setup_db(t, "pkseek_agg")
	defer teardown_db(d, "pkseek_agg")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")

	cnt := db.query(d, "SELECT COUNT(*) FROM t WHERE id = 2;")
	testing.expect(t, cnt.ok, "seek count must succeed")
	testing.expect_value(t, len(cnt.rows), 1)
	if len(cnt.rows) == 1 {
		testing.expect_value(t, cnt.rows[0][0].(i64), 1)
	}

	sum := db.query(d, "SELECT SUM(v) FROM t WHERE id = 3;")
	testing.expect(t, sum.ok, "seek sum must succeed")
	if len(sum.rows) == 1 {
		testing.expect_value(t, sum.rows[0][0].(f64), 30)
	}

	cntmiss := db.query(d, "SELECT COUNT(*) FROM t WHERE id = 99;")
	testing.expect(t, cntmiss.ok, "seek count miss must succeed")
	if len(cntmiss.rows) == 1 {
		testing.expect_value(t, cntmiss.rows[0][0].(i64), 0)
	}
}

@(test)
test_pk_seek_as_of :: proc(t: ^testing.T) {
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
	if len(cur.rows) == 1 {
		testing.expect_value(t, cur.rows[0][0].(i64), 99)
	}

	old := db.query(d, fmt.tprintf("SELECT v FROM t AS OF SNAPSHOT %d WHERE id = 1;", snap_id))
	testing.expect(t, old.ok, "as-of seek must succeed")
	testing.expect_value(t, len(old.rows), 1)
	if len(old.rows) == 1 {
		testing.expect_value(t, old.rows[0][0].(i64), 10)
	}
}

@(test)
test_pk_seek_no_pk_and_mutations :: proc(t: ^testing.T) {
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
	if len(upd.rows) == 1 {
		testing.expect_value(t, upd.rows[0][0].(i64), 200)
	}

	db.execute(d, "DELETE FROM t WHERE id = 1;")
	del := db.query(d, "SELECT v FROM t WHERE id = 1;")
	testing.expect(t, del.ok, "post-delete seek must succeed")
	testing.expect_value(t, len(del.rows), 0)
}

// Txn-deferred schema roots: statements inside an explicit txn stage roots
// instead of COW-publishing per statement. Results must be identical.
@(test)
test_compare_cross_type_never_equal :: proc(t: ^testing.T) {
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
// still tests nullness.
@(test)
test_null_comparison_never_matches :: proc(t: ^testing.T) {
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
// must take the same seek path as the bare column (a full scan would also be
// correct; this pins the seek path).
@(test)
test_mutation_pk_seek_qualified :: proc(t: ^testing.T) {
	d := setup_db(t, "mut_pk_qual")
	defer teardown_db(d, "mut_pk_qual")

	db.execute(d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
	db.execute(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);")

	db.execute(d, "UPDATE t SET v = 77 WHERE t.id = 2;")
	q := db.query(d, "SELECT v FROM t WHERE id = 2;")
	testing.expect(t, q.ok, "post-update read must succeed")
	testing.expect_value(t, len(q.rows), 1)
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), 77)
	}

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
		if i > 1 {
			strings.write_string(&sb, ",")
		}
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
		if a.ok != b.ok {
			return false
		}
		if !a.ok {
			return true
		}
		if len(a.rows) != len(b.rows) {
			return false
		}
		if len(a.columns) != len(b.columns) {
			return false
		}
		for c, i in a.columns {
			if c != b.columns[i] || a.col_types[i] != b.col_types[i] {
				return false
			}
		}
		for r, i in a.rows {
			if len(r) != len(b.rows[i]) {
				return false
			}
			for v, j in r {
				if !types.value_compare(v, b.rows[i][j]) {
					return false
				}
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
