package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:db"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"

// drop_index_plan renders the EXPLAIN text for one SELECT through the same
// resolvers execution uses (cf. test_index_explain's local helper).
drop_index_plan :: proc(t: ^testing.T, d: ^db.Database, sql: string) -> string {
	st := db.Schema_Tree(d)
	stmt, parse_ok, _ := parser.parse(sql, context.temp_allocator)
	if !parse_ok {
		testing.expect(t, false, "explain parses")
		return ""
	}
	res := executor.Result{}
	exec_ok, _, _ := executor.execute(&st, stmt, &res, nil, nil)
	if !exec_ok {
		testing.expect(t, false, "explain executes")
		return ""
	}
	if len(res.rows) != 1 || len(res.rows[0].values) != 1 {
		testing.expect(t, false, "explain yields one plan row")
		return ""
	}
	plan_text, is_text := res.rows[0].values[0].(string)
	if !is_text {
		testing.expect(t, false, "plan row is TEXT")
		return ""
	}
	return strings.clone(plan_text, context.temp_allocator)
}

// expect_rowids compares two rowid lists element-wise (expect_value
// needs comparable types; slices aren't).
expect_rowids :: proc(t: ^testing.T, got, want: []i64) {
	testing.expect_value(t, len(got), len(want))
	for i in 0 ..< min(len(got), len(want)) {
		testing.expect_value(t, got[i], want[i])
	}
}
// drop_index_ids runs a SELECT id projection (ORDER BY id at call
// sites) and returns its ids. Real columns stay valid with and without
// the index; covering SELECT rowid is index-only by design.
drop_index_ids :: proc(t: ^testing.T, d: ^db.Database, sql: string) -> []i64 {
	q := db.query(d, sql)
	testing.expect(t, q.ok, "covering select succeeds")
	if !q.ok { return nil }
	out := make([]i64, len(q.rows), context.temp_allocator)
	for row, i in q.rows {
		v, is_int := row[0].(i64)
		testing.expect(t, is_int, "rowid projects as INTEGER")
		if is_int { out[i] = v }
	}
	return out
}

@(test)
test_drop_index :: proc(t: ^testing.T) {
	// DROP INDEX <name>: the definition clears atomically, data stays,
	// routed shapes fall back to full scan with identical results, and
	// EXPLAIN stops routing.
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidx")
	defer teardown_db(d, "dropidx")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)
	expect := make(map[string][dynamic]i64, context.allocator)
	defer destroy_expect(&expect)
	index_workload(t, d, []string{"alpha", "bb", "alpha", "", "gamma"}, 60, &expect)

	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';"),
		"INDEX SCAN ON docs USING body (eq, covering)",
	)
	before := drop_index_ids(t, d, "SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;")
	testing.expect_value(t, len(before), len(expect["alpha"]))
	before_prefix := drop_index_ids(t, d, "SELECT id FROM docs WHERE body LIKE 'al%' ORDER BY id;")
	before_in := drop_index_ids(t, d, "SELECT id FROM docs WHERE body IN ('alpha', 'bb') ORDER BY id;")

	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop succeeds")

	// Definition cleared atomically (root + column + name).
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if found {
		testing.expect_value(t, len(tbl.indexes), 0)
	}

	// Identical results via full scan on every routed shape.
	expect_rowids(
		t,
		drop_index_ids(t, d, "SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;"),
		before,
	)
	expect_rowids(
		t,
		drop_index_ids(t, d, "SELECT id FROM docs WHERE body LIKE 'al%' ORDER BY id;"),
		before_prefix,
	)
	expect_rowids(
		t,
		drop_index_ids(t, d, "SELECT id FROM docs WHERE body IN ('alpha', 'bb') ORDER BY id;"),
		before_in,
	)
	fetch := db.query(d, "SELECT id, body, v FROM docs WHERE body = 'bb';")
	testing.expect(t, fetch.ok, "fetch projection succeeds")
	testing.expect_value(t, len(fetch.rows), len(expect["bb"]))
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';"),
		"FULL SCAN ON docs",
	)

	// Data itself untouched.
	cnt := db.query(d, "SELECT COUNT(*) FROM docs;")
	testing.expect(t, cnt.ok && len(cnt.rows) == 1, "count succeeds")
	testing.expect_value(t, cnt.rows[0][0].(i64), i64(62))
}

@(test)
test_drop_index_errors :: proc(t: ^testing.T) {
	// Unknown names and double drops fail cleanly; the definition is
	// unchanged by a failed drop.
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidxerr")
	defer teardown_db(d, "dropidxerr")

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

	saved, quiet := suppress_expected_errors()
	context = quiet
	neg_missing := db.execute(d, "DROP INDEX nope;")
	neg_bare := db.execute(d, "DROP INDEX;")
	context = restore_logger(saved)
	testing.expect(t, neg_missing != .None, "unknown index rejected")
	testing.expect(t, neg_bare != .None, "bare DROP INDEX rejected")

	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop succeeds")
	saved2, quiet2 := suppress_expected_errors()
	context = quiet2
	neg_twice := db.execute(d, "DROP INDEX i_body;")
	context = restore_logger(saved2)
	testing.expect(t, neg_twice != .None, "second drop rejected")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if found { testing.expect_value(t, len(tbl.indexes), 0) }
}

@(test)
test_drop_index_name_uniqueness :: proc(t: ^testing.T) {
	// Index names are global: a second CREATE with a live name fails even
	// on another table; DROP frees the name for reuse.
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidxuniq")
	defer teardown_db(d, "dropidxuniq")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE a (id INT PRIMARY KEY, body TEXT);") == .None,
		"create a",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE b (id INT PRIMARY KEY, body TEXT);") == .None,
		"create b",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i ON a (body);") == .None,
		"index on a",
	)

	// Same name on another table is fine now (per-table uniqueness).
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i ON b (body);") == .None,
		"same name on other table succeeds",
	)

	// Unqualified DROP with two matches is ambiguous; ON resolves.
	saved, quiet := suppress_expected_errors()
	context = quiet
	amb := db.execute(d, "DROP INDEX i;")
	context = restore_logger(saved)
	testing.expect(t, amb != .None, "ambiguous drop rejected")
	testing.expect(t, db.execute(d, "DROP INDEX i ON a;") == .None, "qualified drop a")
	testing.expect(t, db.execute(d, "DROP INDEX i ON b;") == .None, "qualified drop b")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i ON b (body);") == .None,
		"name reusable after drop",
	)
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "b", context.temp_allocator)
	testing.expect(t, found, "table b found")
	if found {
		def, has := schema.table_index(tbl, "i")
		testing.expect(t, has, "index on b present")
		if has {
			testing.expect(t, def.root > 0, "index root published")
			testing.expect_value(t, def.column, "body")
		}
	}
}

@(test)
test_drop_index_recreate :: proc(t: ^testing.T) {
	// Drop then re-CREATE (same name, other column): routing resumes on
	// the new column, old column falls back.
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidxrecr")
	defer teardown_db(d, "dropidxrecr")

	testing.expect(
		t,
		db.execute(
			d,
			"CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT);",
		) ==
		.None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (1, 't1', 'alpha'), (2, 't2', 'beta');") ==
		.None,
		"insert",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i ON docs (body);") == .None,
		"create index",
	)
	testing.expect(t, db.execute(d, "DROP INDEX i;") == .None, "drop succeeds")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i ON docs (title);") == .None,
		"recreate on title",
	)

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if found {
		def, has := schema.table_index(tbl, "i")
		testing.expect(t, has, "index present")
		if has { testing.expect_value(t, def.column, "title") }
	}
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE title = 't1';"),
		"INDEX SCAN ON docs USING title (eq, covering)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';"),
		"FULL SCAN ON docs",
	)
	got := drop_index_ids(t, d, "SELECT id FROM docs WHERE title = 't1' ORDER BY id;")
	testing.expect_value(t, len(got), 1)
	if len(got) == 1 { testing.expect_value(t, got[0], i64(1)) }
}

@(test)
test_drop_index_in_txn :: proc(t: ^testing.T) {
	// DDL publishes immediately even in-txn (CREATE INDEX precedent): the
	// drop is visible to later statements; staged index writes for the
	// table are discarded with the definition; COMMIT keeps the drop.
	// Inserts after the drop fan out nowhere (no index to maintain).
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidxtxn")
	defer teardown_db(d, "dropidxtxn")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (1, 'alpha');") == .None,
		"insert",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (2, 'beta');") == .None,
		"staged insert fans out",
	)
	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop in txn")
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';"),
		"FULL SCAN ON docs",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (3, 'gamma');") == .None,
		"insert after drop succeeds",
	)
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if found { testing.expect_value(t, len(tbl.indexes), 0) }
	got := drop_index_ids(t, d, "SELECT id FROM docs WHERE body = 'beta' ORDER BY id;")
	testing.expect_value(t, len(got), 1)
	if len(got) == 1 { testing.expect_value(t, got[0], i64(2)) }
	all := db.query(d, "SELECT COUNT(*) FROM docs;")
	testing.expect(t, all.ok && len(all.rows) == 1, "count succeeds")
	testing.expect_value(t, all.rows[0][0].(i64), i64(3))
}

@(test)
test_drop_index_vacuum :: proc(t: ^testing.T) {
	// Vacuum after a drop rebuilds data only; queries stay correct and
	// the definition stays cleared.
	context.logger.lowest_level = .Error
	d := setup_db(t, "dropidxvac")
	defer teardown_db(d, "dropidxvac")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (1, 'alpha'), (2, 'beta');") == .None,
		"insert",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)
	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop succeeds")
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	got := drop_index_ids(t, d, "SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;")
	testing.expect_value(t, len(got), 1)
	if len(got) == 1 { testing.expect_value(t, got[0], i64(1)) }
}

@(test)
test_drop_index_reopen :: proc(t: ^testing.T) {
	// The cleared definition persists across close/reopen.
	d := setup_db(t, "dropidxreopen")
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (1, 'alpha');") == .None,
		"insert",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)
	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop succeeds")
	db.close(d)

	d2, open_err := db.open("test_int_dropidxreopen.db")
	testing.expect(t, open_err == .None, "reopen")
	if open_err == .None {
		defer teardown_db(d2, "dropidxreopen")
		st := db.Schema_Tree(d2)
		tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
		testing.expect(t, found, "table found")
		if found { testing.expect_value(t, len(tbl.indexes), 0) }
		got := drop_index_ids(t, d2, "SELECT id FROM docs WHERE body = 'alpha' ORDER BY id;")
		testing.expect_value(t, len(got), 1)
	}
}

@(test)
test_index_gc_expire :: proc(t: ^testing.T) {
	// GC live-set covers index roots (slot [6] values, not child
	// pointers): a live index survives expire + freelist reuse with
	// routing intact. Twin of test_gc_reclaims_cow_waste with a TEXT
	// column + index; the second batch forces reuse of anything wrongly
	// freed, so corruption can't hide behind intact-but-dead pages.
	context.logger.lowest_level = .Error
	d := setup_db(t, "idxgc")
	defer teardown_db(d, "idxgc")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	for i in 1 ..= 700 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'n%d');", i, i % 50)) ==
			.None,
			"insert",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON t (body);") == .None,
		"create index",
	)

	before := pager.page_count(d.pager)
	testing.expect(t, int(before) > snapshot.GC_MIN_PAGES, "fixture must engage GC")
	testing.expect(t, db.expire_snapshots(d, 1) == .None, "expire")

	// Routed reads intact after the sweep.
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM t WHERE body = 'n7';"),
		"INDEX SCAN ON t USING body (eq, covering)",
	)
	got := drop_index_ids(t, d, "SELECT rowid FROM t WHERE body = 'n7';")
	testing.expect_value(t, len(got), 14)

	// Reuse storm over the swept freelist, then routed reads again.
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin2")
	for i in 701 ..= 1400 {
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, 'm%d');", i, i % 50)) ==
			.None,
			"insert2",
		)
	}
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit2")
	got2 := drop_index_ids(t, d, "SELECT rowid FROM t WHERE body = 'm7';")
	testing.expect_value(t, len(got2), 14)
	got3 := drop_index_ids(t, d, "SELECT rowid FROM t WHERE body = 'n7';")
	testing.expect_value(t, len(got3), 14)
	cnt := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, cnt.ok && len(cnt.rows) == 1, "count succeeds")
	testing.expect_value(t, cnt.rows[0][0].(i64), i64(1400))
}
