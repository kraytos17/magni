// Integration index tests: CREATE/DROP, DML fan-out, transactional index
// updates, vacuum, and covering/fetch/prefix SELECT routing with EXPLAIN
// pins. File-backed through setup_db/teardown_db.
package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:btree"
import "src:db"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

@(test)
test_create_index_backfill_boundaries :: proc(t: ^testing.T) {
	// 2000-row table, then CREATE INDEX: the backfill collects, sorts, and
	// bulk-builds (multi-leaf). Every key is point-queried, pinning
	// leaf-boundary separators exactly like the INSERT-time sweep.
	d := setup_db(t, "idxfill_bounds")
	defer teardown_db(d, "idxfill_bounds")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)
	sb: strings.Builder
	strings.builder_init(&sb, context.temp_allocator)
	strings.write_string(&sb, "INSERT INTO docs VALUES ")
	for i in 1 ..= 2000 {
		if i > 1 {
			strings.write_string(&sb, ",")
		}
		fmt.sbprintf(&sb, "(%d,'b%04d')", i, i)
	}
	strings.write_string(&sb, ";")
	testing.expect(t, db.execute(d, strings.to_string(sb)) == .None, "bulk load")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)

	q := db.query(d, "SELECT COUNT(*) FROM docs;")
	testing.expect(t, q.ok, "count runs")
	if q.ok && len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), 2000)
	}

	for i in 1 ..= 2000 {
		free_all(context.temp_allocator)
		r := db.query(d, fmt.tprintf("SELECT id FROM docs WHERE body = 'b%04d';", i))
		if !r.ok || len(r.rows) != 1 {
			testing.expect(t, false, fmt.tprintf("body b%04d missing after backfill", i))
			break
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_create_index_empty :: proc(t: ^testing.T) {
	d := setup_db(t, "idxempty")
	defer teardown_db(d, "idxempty")

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

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	def, has := schema.table_index(tbl, "i_body")
	testing.expect(t, has, "index present")
	if has {
		testing.expect(t, def.root > 0, "index root published")
		testing.expect_value(t, def.column, "body")
	}
	idx_tree := btree.init(d.pager, index_root_of(t, tbl, "i_body"))
	cnt, c_err := btree.tree_count_rows(&idx_tree)
	testing.expect(t, c_err == .None && cnt == 0, "empty index has no rows")
}

@(test)
test_create_index_backfill :: proc(t: ^testing.T) {
	// Non-empty table with a MIDDLE text column (alignment), duplicate
	// texts, and NULLs: every TEXT row indexed under its exact rowids,
	// NULLs absent. Oracle is the test-side expectation map.
	d := setup_db(t, "idxfill")
	defer teardown_db(d, "idxfill")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create",
	)
	bodies := []string{"alpha", "beta", "alpha", "", "gamma", "beta"}
	// Heap-owned oracle (outlives per-iteration temp frees below).
	expect := make(map[string][dynamic]i64, context.allocator)
	defer {
		for _, v in expect {
			delete(v)
		}
		delete(expect)
	}
	n_indexed := 0
	for i in 1 ..= 30 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		sql := fmt.tprintf("INSERT INTO docs VALUES (%d, '%s', %d);", i, body, i * 10)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
		// Every i inserts its TEXT row (always indexed); i%7==0 inserts an
		// ADDITIONAL NULL-body row (never indexed, never in the oracle).
		if i % 7 == 0 {
			nil_sql := fmt.tprintf("INSERT INTO docs VALUES (%d, NULL, %d);", 100 + i, i)
			testing.expect(t, db.execute(d, nil_sql) == .None, "null insert succeeds")
		}
		lst, lst_ok := expect[body]
		if !lst_ok {
			lst = make([dynamic]i64, context.allocator)
		}
		append(&lst, i64(i))
		expect[body] = lst
		n_indexed += 1
	}
	free_all(context.temp_allocator)

	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	idx_root := index_root_of(t, tbl, "i_body")
	testing.expect(t, idx_root > 0, "index root published")
	idx_tree := btree.init(d.pager, idx_root)
	cnt, c_err := btree.tree_count_rows(&idx_tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, n_indexed)

	for text, want in expect {
		free_all(context.temp_allocator)
		got, g_err := btree.text_find_rowids(&idx_tree, idx_tree.root, transmute([]u8)text)
		testing.expect(t, g_err == .None, "find succeeds")
		testing.expect(t, len(got) == len(want), "all rowids found")
		for i in 0 ..< min(len(got), len(want)) {
			testing.expect_value(t, got[i], types.Row_ID(want[i]))
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_create_index_errors :: proc(t: ^testing.T) {
	// Negative DDL paths log at .Error by design: suppress the expected
	// noise (repo convention — see line ~1651), or the runner flags it.
	d := setup_db(t, "idxerr")
	defer teardown_db(d, "idxerr")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, score REAL, data BLOB);") ==
		.None,
		"create",
	)
	saved, quiet := suppress_expected_errors()
	context = quiet
	neg_table := db.execute(d, "CREATE INDEX i_nope ON missing (body);")
	neg_col := db.execute(d, "CREATE INDEX i_nope ON docs (missing);")
	neg_real := db.execute(d, "CREATE INDEX i_nope ON docs (score);")
	neg_blob := db.execute(d, "CREATE INDEX i_nope ON docs (data);")
	context = restore_logger(saved)
	testing.expect(t, neg_table != .None, "unknown table rejected")
	testing.expect(t, neg_col != .None, "unknown column rejected")
	testing.expect(t, neg_real != .None, "REAL column rejected")
	testing.expect(t, neg_blob != .None, "BLOB column rejected")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_id ON docs (id);") == .None,
		"INTEGER column accepted",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"first index succeeds",
	)
	saved2, quiet2 := suppress_expected_errors()
	context = quiet2
	dup_name := db.execute(d, "CREATE INDEX i_body ON docs (body);")
	context = restore_logger(saved2)
	testing.expect(t, dup_name != .None, "duplicate name on same table rejected")
}

@(test)
test_create_index_in_txn :: proc(t: ^testing.T) {
	// DDL publishes immediately even in-txn (like CREATE TABLE): later
	// statements in the txn resolve the index; COMMIT keeps it.
	d := setup_db(t, "idxtxn")
	defer teardown_db(d, "idxtxn")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT);") == .None,
		"create",
	)

	testing.expect(t, db.execute(d, "INSERT INTO docs VALUES (1, 'hello');") == .None, "insert")
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index in txn",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (2, 'world');") == .None,
		"insert in txn succeeds",
	)
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found after commit")
	if !found {
		return
	}
	testing.expect(t, index_root_of(t, tbl, "i_body") > 0, "index survives commit")
	// Backfilled row present; in-txn row is NOT (post-DDL writes fan out
	// only after commit).
	idx_tree := btree.init(d.pager, index_root_of(t, tbl, "i_body"))
	hello, _ := btree.text_find_rowids(&idx_tree, idx_tree.root, []u8{'h', 'e', 'l', 'l', 'o'})
	testing.expect(t, len(hello) == 1 && hello[0] == 1, "backfilled row indexed")
	// Post-DDL writes maintain the index in the same txn.
	world, _ := btree.text_find_rowids(&idx_tree, idx_tree.root, []u8{'w', 'o', 'r', 'l', 'd'})
	testing.expect(t, len(world) == 1 && world[0] == 2, "in-txn write indexed")
}

// expect maps indexed text -> sorted rowids (heap-owned; destroyed per
// test). verify_text_index checks the catalog root, the total count, and
// every text's exact rowid set — the whole index, no sampling.
// index_oracle_add records one indexed row, keeping each text's rowids
// sorted for exact comparison.
index_oracle_add :: proc(expect: ^map[string][dynamic]i64, text: string, rowid: i64) {
	lst, ok := expect[text]
	if !ok {
		lst = make([dynamic]i64, context.allocator)
	}
	append(&lst, rowid)
	for j := len(lst) - 1; j > 0; j -= 1 {
		if lst[j] < lst[j - 1] {
			lst[j], lst[j - 1] = lst[j - 1], lst[j]
		} else {
			break
		}
	}
	expect[text] = lst
}

// index_oracle_del drops one rowid, deleting the text entry when empty.
index_oracle_del :: proc(expect: ^map[string][dynamic]i64, text: string, rowid: i64) {
	lst, ok := expect[text]
	if !ok {
		return
	}
	keep := make([dynamic]i64, 0, len(lst), context.allocator)
	for r in lst {
		if r != rowid {
			append(&keep, r)
		}
	}
	delete(lst)
	if len(keep) == 0 {
		delete_key(expect, text)
	} else {
		expect[text] = keep
	}
}

// destroy_expect frees every oracle rowid list and the map itself.
destroy_expect :: proc(expect: ^map[string][dynamic]i64) {
	for _, v in expect^ {
		delete(v)
	}
	delete(expect^)
}

// index_root_of resolves one named index root, expecting presence.
index_root_of :: proc(t: ^testing.T, tbl: types.Table, name: string) -> u32 {
	def, ok := schema.table_index(tbl, name)
	testing.expect(t, ok, "index present")
	if !ok {
		return 0
	}
	return def.root
}

// verify_text_index checks one text index end to end: catalog root,
// total row count, and every text's exact rowid set against the oracle.
verify_text_index :: proc(
	t: ^testing.T,
	d: ^db.Database,
	table_name: string,
	expect: map[string][dynamic]i64,
) {
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, table_name, context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	testing.expect(t, len(tbl.indexes) > 0, "index present")
	if len(tbl.indexes) == 0 {
		return
	}
	idx_root := tbl.indexes[0].root
	testing.expect(t, idx_root > 0, "index root published")
	idx_tree := btree.init(d.pager, idx_root)
	total := 0
	for _, want in expect {
		total += len(want)
	}
	cnt, c_err := btree.tree_count_rows(&idx_tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, total)
	for text, want in expect {
		free_all(context.temp_allocator)
		got, g_err := btree.text_find_rowids(&idx_tree, idx_tree.root, transmute([]u8)text)
		testing.expect(t, g_err == .None, "find succeeds")
		testing.expect(t, len(got) == len(want), "all rowids found")
		for i in 0 ..< min(len(got), len(want)) {
			testing.expect_value(t, got[i], types.Row_ID(want[i]))
		}
	}
	free_all(context.temp_allocator)
}

// index_workload inserts N TEXT rows (bodies cycle, middle column) plus
// two NULL rows, recording the oracle. Shared by the index fan-out tests.
index_workload :: proc(
	t: ^testing.T,
	d: ^db.Database,
	bodies: []string,
	n: int,
	expect: ^map[string][dynamic]i64,
) {
	for i in 1 ..= n {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		sql := fmt.tprintf("INSERT INTO docs VALUES (%d, '%s', %d);", i, body, i * 10)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
		index_oracle_add(expect, body, i64(i))
	}
	null_ids := []int{101, 102}
	for id in null_ids {
		free_all(context.temp_allocator)
		sql := fmt.tprintf("INSERT INTO docs VALUES (%d, NULL, %d);", id, id)
		testing.expect(t, db.execute(d, sql) == .None, "null insert succeeds")
	}
	free_all(context.temp_allocator)
}

@(test)
test_index_insert_autocommit :: proc(t: ^testing.T) {
	// Autocommit (COW, nil pending) fan-out: TEXT/dup/empty indexed, NULLs
	// absent, middle-column alignment.
	d := setup_db(t, "idxinsauto")
	defer teardown_db(d, "idxinsauto")

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
	index_workload(t, d, []string{"alpha", "bb", "alpha", "", "gamma"}, 25, &expect)
	verify_text_index(t, d, "docs", expect)
}

@(test)
test_index_insert_txn :: proc(t: ^testing.T) {
	// Same workload inside an explicit txn (COW fan-out + staged commit):
	// identical oracle => autocommit/txn parity by construction (both COW;
	// the dormant Direct paths intentionally do not fan out).
	d := setup_db(t, "idxinscow")
	defer teardown_db(d, "idxinscow")

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
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	expect := make(map[string][dynamic]i64, context.allocator)
	defer destroy_expect(&expect)
	index_workload(t, d, []string{"alpha", "bb", "alpha", "", "gamma"}, 25, &expect)
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	verify_text_index(t, d, "docs", expect)
}

@(test)
test_index_update :: proc(t: ^testing.T) {
	// pk + scan updates across both modes: indexed-col change moves the
	// entry, non-indexed change is a perfect no-op, NULL transitions work.
	d := setup_db(t, "idxupd")
	defer teardown_db(d, "idxupd")

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
	// bodies cycle ["a","bb","a","c"]: ids 1:a 2:bb 3:a 4:c 5:a 6:bb 7:a 8:c ...
	index_workload(t, d, []string{"a", "bb", "a", "c"}, 12, &expect)

	// pk update of the indexed column: (a,3) -> (changed,3).
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET body = 'changed' WHERE id = 3;") == .None,
		"pk update indexed col",
	)
	index_oracle_del(&expect, "a", 3)
	index_oracle_add(&expect, "changed", 3)

	// pk update of a NON-indexed column: oracle identical.
	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "docs", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_body")
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET v = 999 WHERE id = 4;") == .None,
		"pk update plain col",
	)
	st1 := db.Schema_Tree(d)
	tbl1, _ := schema.find_table(&st1, "docs", context.temp_allocator)
	testing.expect(
		t,
		index_root_of(t, tbl1, "i_body") == root_before,
		"untouched index keeps its root",
	)

	// NULL -> text and text -> NULL transitions.
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET body = 'x' WHERE id = 101;") == .None,
		"null to text",
	)
	index_oracle_add(&expect, "x", 101)
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET body = NULL WHERE id = 2;") == .None,
		"text to null",
	)
	index_oracle_del(&expect, "bb", 2)

	// Scan update (non-pk filter): v >= 110 hits ids 11,12 — plus id 4,
	// whose v became 999 two statements ago (oracle must track that).
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET body = 's' WHERE v >= 110;") == .None,
		"scan update",
	)
	index_oracle_del(&expect, "a", 11)
	index_oracle_del(&expect, "c", 12)
	index_oracle_del(&expect, "c", 4)
	index_oracle_add(&expect, "s", 11)
	index_oracle_add(&expect, "s", 12)
	index_oracle_add(&expect, "s", 4)

	verify_text_index(t, d, "docs", expect)
}

@(test)
test_index_delete :: proc(t: ^testing.T) {
	// pk delete, scan delete, and missing-row tolerance.
	d := setup_db(t, "idxdel")
	defer teardown_db(d, "idxdel")

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
	index_workload(t, d, []string{"a", "bb", "a", "c"}, 12, &expect)

	testing.expect(t, db.execute(d, "DELETE FROM docs WHERE id = 3;") == .None, "pk delete")
	index_oracle_del(&expect, "a", 3)
	testing.expect(t, db.execute(d, "DELETE FROM docs WHERE v < 30;") == .None, "scan delete")
	index_oracle_del(&expect, "a", 1)
	index_oracle_del(&expect, "bb", 2)
	testing.expect(
		t,
		db.execute(d, "DELETE FROM docs WHERE id = 9999;") == .None,
		"missing delete succeeds",
	)
	verify_text_index(t, d, "docs", expect)
}

@(test)
test_index_txn_rollback_mixed :: proc(t: ^testing.T) {
	// Mixed insert/update/delete inside a txn, then ROLLBACK: the index
	// matches the pre-txn oracle exactly (staged roots discarded).
	d := setup_db(t, "idxroll")
	defer teardown_db(d, "idxroll")

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
	index_workload(t, d, []string{"a", "bb", "a", "c"}, 12, &expect)
	verify_text_index(t, d, "docs", expect)

	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "docs", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_body")

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (50, 'nope', 1);") == .None,
		"txn insert",
	)
	testing.expect(
		t,
		db.execute(d, "UPDATE docs SET body = 'nope' WHERE id = 1;") == .None,
		"txn update",
	)
	testing.expect(t, db.execute(d, "DELETE FROM docs WHERE id = 2;") == .None, "txn delete")
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback")

	st1 := db.Schema_Tree(d)
	tbl1, _ := schema.find_table(&st1, "docs", context.temp_allocator)
	testing.expect(
		t,
		index_root_of(t, tbl1, "i_body") == root_before,
		"rollback restores index root",
	)
	verify_text_index(t, d, "docs", expect)
}

@(test)
test_index_vacuum_after_delete :: proc(t: ^testing.T) {
	// Indexed table, mass deletes, VACUUM: the oracle holds exactly,
	// every reachable index page validates, counts are exact. The index
	// root changes (fresh packed pages); rowids are logical, so covering
	// lookups are unaffected by the physical rebuild.
	d := setup_db(t, "idxvac")
	defer teardown_db(d, "idxvac")

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

	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "docs", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_body")
	testing.expect(t, root_before > 0, "index present before vacuum")

	// Delete half the rows (every even id), then vacuum.
	for i in 2 ..= 60 {
		if i % 2 != 0 {
			continue
		}
		free_all(context.temp_allocator)
		sql := fmt.tprintf("DELETE FROM docs WHERE id = %d;", i)
		testing.expect(t, db.execute(d, sql) == .None, "delete succeeds")
	}
	free_all(context.temp_allocator)
	// Oracle: drop every even rowid (bodies deterministic from workload).
	del_texts := []string{"alpha", "bb", "", "gamma"}
	for text in del_texts {
		for r := 2; r <= 60; r += 2 {
			index_oracle_del(&expect, text, i64(r))
		}
	}
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	st1 := db.Schema_Tree(d)
	tbl1, found1 := schema.find_table(&st1, "docs", context.temp_allocator)
	testing.expect(t, found1, "table found after vacuum")
	if !found1 {
		return
	}
	vac_root := index_root_of(t, tbl1, "i_body")
	testing.expect(t, vac_root > 0, "index root present after vacuum")
	testing.expect(t, vac_root != root_before, "vacuum rebuilt the index")
	verify_text_index(t, d, "docs", expect)

	// Every reachable index page validates (packed output is well-formed).
	idx_tree := btree.init(d.pager, vac_root)
	pages := make(map[u32]bool, context.temp_allocator)
	defer delete(pages)
	btree.collect_pages(&idx_tree, idx_tree.root, &pages)
	testing.expect(t, len(pages) >= 1, "index reachable after vacuum")
	for page_id in pages {
		pg, pg_err := pager.get_page(d.pager, page_id)
		if pg_err != nil {
			continue
		}
		h := btree.get_header(pg.data, page_id)
		if h == nil {
			pager.unpin_page(d.pager, page_id)
			continue
		}
		#partial switch h.page_type {
		case .LEAF_TEXT:
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"vacuumed leaf validates",
			)
		case .TEXT_INTERIOR:
			testing.expect(
				t,
				btree.text_validate_interior(pg.data, btree.Page_Id(page_id)) == .None,
				"vacuumed interior validates",
			)
		case:
			testing.expect(t, false, "unexpected page type after vacuum")
		}
		pager.unpin_page(d.pager, page_id)
	}
}

@(test)
test_index_vacuum_empty :: proc(t: ^testing.T) {
	// Vacuuming an empty index yields a fresh validating empty LEAF_TEXT
	// root, the same empty-root shape tree_vacuum produces for tables.
	d := setup_db(t, "idxvacempty")
	defer teardown_db(d, "idxvacempty")

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
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "docs", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	empty_root := index_root_of(t, tbl, "i_body")
	testing.expect(t, empty_root > 0, "index root present")
	idx_tree := btree.init(d.pager, empty_root)
	cnt, c_err := btree.tree_count_rows(&idx_tree)
	testing.expect(t, c_err == .None && cnt == 0, "vacuumed empty index counts zero")
	pg, pg_err := pager.get_page(d.pager, empty_root)
	testing.expect(t, pg_err == nil, "root readable")
	if pg_err != nil {
		return
	}
	defer pager.unpin_page(d.pager, empty_root)
	h := btree.get_header(pg.data, empty_root)
	testing.expect(t, h != nil && h.page_type == .LEAF_TEXT, "empty vacuum root is a text leaf")
	testing.expect(
		t,
		btree.text_validate_leaf(pg.data, btree.Page_Id(empty_root)) == .None,
		"empty vacuum root validates",
	)
}

@(test)
test_index_vacuum_in_txn :: proc(t: ^testing.T) {
	// Writes in-txn, VACUUM (publishes staged roots first, like data),
	// COMMIT: the oracle holds. Same staged-roots-first ordering as the
	// deferred-vacuum data test.
	d := setup_db(t, "idxvactxn")
	defer teardown_db(d, "idxvactxn")

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
	expect := make(map[string][dynamic]i64, context.allocator)
	defer destroy_expect(&expect)
	index_workload_simple(t, d, &expect)
	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (101, 'txn');") == .None,
		"txn insert",
	)
	index_oracle_add(&expect, "txn", 101)
	testing.expect(t, db.execute(d, "DELETE FROM docs WHERE id = 1;") == .None, "txn delete")
	index_oracle_del(&expect, "aaa", 1)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum in txn succeeds")
	testing.expect(t, db.execute(d, "COMMIT;") == .None, "commit")
	verify_text_index(t, d, "docs", expect)
}

// index_workload_simple inserts a fixed small corpus (ids 1..3, one NULL)
// for tests that mutate afterward; oracle starts complete.
index_workload_simple :: proc(t: ^testing.T, d: ^db.Database, expect: ^map[string][dynamic]i64) {
	rows := []string{"aaa", "bbb", "aaa"}
	for i in 0 ..< len(rows) {
		free_all(context.temp_allocator)
		sql := fmt.tprintf("INSERT INTO docs VALUES (%d, '%s');", i + 1, rows[i])
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
		index_oracle_add(expect, rows[i], i64(i + 1))
	}
	free_all(context.temp_allocator)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (50, NULL);") == .None,
		"null insert succeeds",
	)
	free_all(context.temp_allocator)
}

@(test)
test_index_select_eq_covering :: proc(t: ^testing.T) {
	// Covering `SELECT rowid ... WHERE body = ?` answers from the text
	// index: exact ascending rowids, `rowid` column typing, and the shared
	// finish_select tail (ORDER BY / DISTINCT / LIMIT / alias) unchanged.
	d := setup_db(t, "idxselcov")
	defer teardown_db(d, "idxselcov")

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

	check_covering :: proc(t: ^testing.T, d: ^db.Database, sql: string, want: []i64) {
		q := db.query(d, sql)
		testing.expect(t, q.ok, "covering select succeeds")
		if !q.ok {
			return
		}
		testing.expect_value(t, len(q.rows), len(want))
		for i in 0 ..< min(len(q.rows), len(want)) {
			v, is_int := q.rows[i][0].(i64)
			testing.expect(t, is_int, "rowid projects as INTEGER")
			if is_int {
				testing.expect_value(t, v, want[i])
			}
		}
	}

	check_covering(t, d, "SELECT rowid FROM docs WHERE body = 'alpha';", expect["alpha"][:])
	check_covering(t, d, "SELECT rowid FROM docs WHERE docs.body = 'bb';", expect["bb"][:])
	check_covering(t, d, "SELECT rowid FROM docs WHERE body = '';", expect[""][:])

	// Missing key: zero rows, success (same shape as a scan miss).
	miss := db.query(d, "SELECT rowid FROM docs WHERE body = 'nope';")
	testing.expect(t, miss.ok, "missing key succeeds")
	testing.expect_value(t, len(miss.rows), 0)

	// Column name and type of the synthetic projection.
	named := db.query(d, "SELECT rowid FROM docs WHERE body = 'gamma';")
	testing.expect(t, named.ok, "named select succeeds")
	if named.ok {
		testing.expect_value(t, len(named.columns), 1)
		if len(named.columns) == 1 {
			testing.expect_value(t, named.columns[0], "rowid")
		}
	}

	// Alias renames the output column; ORDER BY / DISTINCT / LIMIT run
	// through the unchanged finish_select tail.
	aliased := db.query(d, "SELECT rowid AS r FROM docs WHERE body = 'alpha';")
	testing.expect(t, aliased.ok, "alias succeeds")
	if aliased.ok && len(aliased.columns) == 1 {
		testing.expect_value(t, aliased.columns[0], "r")
	}
	ordered := db.query(d, "SELECT rowid FROM docs WHERE body = 'alpha' ORDER BY rowid DESC;")
	testing.expect(t, ordered.ok, "order by succeeds")
	if ordered.ok {
		want := expect["alpha"]
		testing.expect_value(t, len(ordered.rows), len(want))
		for i in 0 ..< min(len(ordered.rows), len(want)) {
			v, is_int := ordered.rows[i][0].(i64)
			if is_int {
				testing.expect_value(t, v, want[len(want) - 1 - i])
			} else {
				testing.expect(t, false, "rowid projects as INTEGER")
			}
		}
	}
	dedup := db.query(d, "SELECT DISTINCT rowid FROM docs WHERE body = 'alpha';")
	testing.expect(t, dedup.ok, "distinct succeeds")
	if dedup.ok {
		testing.expect_value(t, len(dedup.rows), len(expect["alpha"]))
	}
	lim := db.query(d, "SELECT rowid FROM docs WHERE body = 'alpha' LIMIT 2;")
	testing.expect(t, lim.ok, "limit succeeds")
	if lim.ok {
		testing.expect_value(t, len(lim.rows), min(2, len(expect["alpha"])))
		for i in 0 ..< len(lim.rows) {
			v, is_int := lim.rows[i][0].(i64)
			if is_int {
				testing.expect_value(t, v, expect["alpha"][i])
			} else {
				testing.expect(t, false, "rowid projects as INTEGER")
			}
		}
	}
}

@(test)
test_index_select_eq_fallback :: proc(t: ^testing.T) {
	// Shapes E1 does not route still return correct results via the full
	// scan: non-covering projections, AND chains, OR/NOT, negated and
	// cross-type/NULL comparisons.
	d := setup_db(t, "idxselfall")
	defer teardown_db(d, "idxselfall")

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

	// Non-covering projection over an indexed equality: full rows, exact set.
	full := db.query(d, "SELECT id, body FROM docs WHERE body = 'alpha';")
	testing.expect(t, full.ok, "full-row select succeeds")
	if full.ok {
		testing.expect_value(t, len(full.rows), len(expect["alpha"]))
		for row in full.rows {
			testing.expect(t, row[1].(string) == "alpha", "row matches predicate")
		}
	}

	// AND chain: first conjunct usable, whole predicate still exact.
	andq := db.query(d, "SELECT id FROM docs WHERE body = 'alpha' AND v = 1;")
	testing.expect(t, andq.ok, "and filter succeeds")
	if andq.ok {
		for row in andq.rows {
			testing.expect_value(t, row[0].(i64), 1)
		}
	}

	// OR / NOT: unroutable, correct via scan (NULL semantics included —
	// whatever the engine's two-valued rule yields, both paths agree
	// because only the scan path runs here).
	orq := db.query(d, "SELECT id FROM docs WHERE body = 'alpha' OR body = 'bb';")
	testing.expect(t, orq.ok, "or filter succeeds")
	if orq.ok {
		testing.expect_value(t, len(orq.rows), len(expect["alpha"]) + len(expect["bb"]))
	}
	neg := db.query(d, "SELECT id, body FROM docs WHERE NOT body = 'alpha';")
	testing.expect(t, neg.ok, "not filter succeeds")
	if neg.ok {
		for row in neg.rows {
			if s, is_text := row[1].(string); is_text {
				testing.expect(t, s != "alpha", "negated row excluded")
			}
		}
	}

	// NULL and cross-type literals fall through (never match, never route).
	nul := db.query(d, "SELECT id FROM docs WHERE body = NULL;")
	testing.expect(t, nul.ok, "null rhs succeeds")
	if nul.ok {
		testing.expect_value(t, len(nul.rows), 0)
	}
	cross := db.query(d, "SELECT id FROM docs WHERE body = 1;")
	testing.expect(t, cross.ok, "int rhs succeeds")
	if cross.ok {
		testing.expect_value(t, len(cross.rows), 0)
	}
}

@(test)
test_index_select_rowid_guard :: proc(t: ^testing.T) {
	// A user column named "rowid" keeps today's meaning: the index never
	// hijacks the projection. A bare `SELECT rowid` now projects the
	// source rowid generally (no predicate needed).
	d := setup_db(t, "idxselguard")
	defer teardown_db(d, "idxselguard")

	testing.expect(t, db.execute(d, "CREATE TABLE w (rowid TEXT, v INT);") == .None, "create")
	testing.expect(t, db.execute(d, "INSERT INTO w VALUES ('a', 1), ('b', 2);") == .None, "insert")
	testing.expect(t, db.execute(d, "CREATE INDEX i_r ON w (rowid);") == .None, "create index")

	// Indexed equality on the user column: full scan, user values.
	q := db.query(d, "SELECT rowid FROM w WHERE rowid = 'a';")
	testing.expect(t, q.ok, "user-column select succeeds")
	if q.ok {
		testing.expect_value(t, len(q.rows), 1)
		if len(q.rows) == 1 {
			s, is_text := q.rows[0][0].(string)
			testing.expect(t, is_text, "user column value is TEXT")
			if is_text {
				testing.expect_value(t, s, "a")
			}
		}
	}

	// No predicate: bare `SELECT rowid` projects the source rowid.
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE p (id INT PRIMARY KEY, body TEXT);") == .None,
		"create p",
	)
	testing.expect(t, db.execute(d, "INSERT INTO p VALUES (1, 'x');") == .None, "insert p")
	testing.expect(t, db.execute(d, "CREATE INDEX i_p ON p (body);") == .None, "create index p")
	bare := db.query(d, "SELECT rowid FROM p;")
	testing.expect(t, bare.ok, "bare SELECT rowid succeeds")
	if bare.ok {
		testing.expect_value(t, len(bare.rows), 1)
		if len(bare.rows) == 1 {
			v, is_int := bare.rows[0][0].(i64)
			testing.expect(t, is_int, "rowid projects as INTEGER")
			if is_int {
				testing.expect_value(t, v, 1)
			}
		}
	}
}

@(test)
test_index_int_create_backfill :: proc(t: ^testing.T) {
	// CREATE INDEX on INTEGER backfills every value (negatives, dups;
	// NULLs skipped) with the def tagged INTEGER; reads go through
	// btree.int_find_rowids on the published root (the planner does not
	// route int indexes yet, so SQL stays on the scan path).
	d := setup_db(t, "idxintback")
	defer teardown_db(d, "idxintback")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"INSERT INTO t VALUES (10, 'x');",
			"INSERT INTO t VALUES (-5, 'y');",
			"INSERT INTO t VALUES (10, 'z');",
			"INSERT INTO t VALUES (NULL, 'n');",
		},
	)
	testing.expect(t, db.execute(d, "CREATE INDEX i_a ON t (a);") == .None, "create int index")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "t", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	def, dok := schema.table_index(tbl, "i_a")
	testing.expect(t, dok, "def present")
	if dok {
		testing.expect_value(t, def.col_type, types.Column_Type.INTEGER)
		testing.expect(t, def.root != 0, "root published")
	}
	// TEXT defs on the same table keep their kind.
	testing.expect(t, db.execute(d, "CREATE INDEX i_b ON t (b);") == .None, "create text index")
	st2 := db.Schema_Tree(d)
	tbl2, found2 := schema.find_table(&st2, "t", context.temp_allocator)
	testing.expect(t, found2, "table found")
	if found2 {
		if bdef, bok := schema.table_index(tbl2, "i_b"); bok {
			testing.expect_value(t, bdef.col_type, types.Column_Type.TEXT)
		}
		if adef, aok := schema.table_index(tbl2, "i_a"); aok {
			testing.expect_value(t, adef.col_type, types.Column_Type.INTEGER)
		}
	}

	idx_root := index_root_of(t, tbl, "i_a")
	idx_tree := btree.init(d.pager, idx_root)
	ten, t_err := btree.int_find_rowids(&idx_tree, idx_tree.root, 10)
	testing.expect(t, t_err == .None, "find succeeds")
	if t_err == .None {
		testing.expect_value(t, len(ten), 2)
	}
	neg, n_err := btree.int_find_rowids(&idx_tree, idx_tree.root, -5)
	testing.expect(t, n_err == .None, "negative find succeeds")
	if n_err == .None {
		testing.expect_value(t, len(neg), 1)
	}
	miss, m_err := btree.int_find_rowids(&idx_tree, idx_tree.root, 999)
	testing.expect(t, m_err == .None, "miss succeeds")
	if m_err == .None {
		testing.expect_value(t, len(miss), 0)
	}

	// Unrouted SQL still answers correctly through the scan.
	q := db.query(d, "SELECT a, b FROM t WHERE a = 10 ORDER BY b;")
	testing.expect(t, q.ok, "unrouted select succeeds")
	testing.expect_value(t, len(q.rows), 2)
}

@(test)
test_index_cross_projection_fetch :: proc(t: ^testing.T) {
	// Projecting one indexed column while filtering on another must fetch
	// (never cover with the wrong column's values): correct rows here, and
	// the matching projection still covers.
	d := setup_db(t, "idxcrossproj")
	defer teardown_db(d, "idxcrossproj")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a TEXT, b TEXT);",
			"INSERT INTO t VALUES ('m', 'x');",
			"INSERT INTO t VALUES ('n', 'y');",
			"CREATE INDEX i_a ON t (a);",
			"CREATE INDEX i_b ON t (b);",
		},
	)
	cross := db.query(d, "SELECT a FROM t WHERE b = 'x';")
	testing.expect(t, cross.ok, "cross projection succeeds")
	testing.expect_value(t, len(cross.rows), 1)
	if len(cross.rows) == 1 {
		s, is_text := cross.rows[0][0].(string)
		testing.expect(t, is_text, "text value projects")
		if is_text {
			testing.expect_value(t, s, "m")
		}
	}

	// Matching projection still answers covering.
	match := db.query(d, "SELECT b FROM t WHERE b = 'y';")
	testing.expect(t, match.ok, "matching projection succeeds")
	if len(match.rows) == 1 {
		s, is_text := match.rows[0][0].(string)
		testing.expect(t, is_text, "text value projects")
		if is_text {
			testing.expect_value(t, s, "y")
		}
	}

	// Mixed int/text tables route the same way.
	testing.expect(t, db.execute(d, "CREATE TABLE m (v INT, w TEXT);") == .None, "create m")
	testing.expect(t, db.execute(d, "INSERT INTO m VALUES (1, 'p');") == .None, "insert m")
	testing.expect(t, db.execute(d, "CREATE INDEX i_v ON m (v);") == .None, "index v")
	testing.expect(t, db.execute(d, "CREATE INDEX i_w ON m (w);") == .None, "index w")
	mq := db.query(d, "SELECT v FROM m WHERE w = 'p';")
	testing.expect(t, mq.ok, "mixed cross projection succeeds")
	testing.expect_value(t, len(mq.rows), 1)
}

@(test)
test_index_select_covering_in_txn :: proc(t: ^testing.T) {
	// In-txn writes are visible to covering index reads (staged cache
	// overlay, same as data); ROLLBACK removes them again.
	d := setup_db(t, "idxseltxn")
	defer teardown_db(d, "idxseltxn")

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
	expect := make(map[string][dynamic]i64, context.allocator)
	defer destroy_expect(&expect)
	index_workload_simple(t, d, &expect)

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (101, 'txn');") == .None,
		"txn insert",
	)
	staged := db.query(d, "SELECT rowid FROM docs WHERE body = 'txn';")
	testing.expect(t, staged.ok, "staged select succeeds")
	if staged.ok {
		testing.expect_value(t, len(staged.rows), 1)
		if len(staged.rows) == 1 {
			v, is_int := staged.rows[0][0].(i64)
			testing.expect(t, is_int, "rowid projects as INTEGER")
			if is_int {
				testing.expect_value(t, v, 101)
			}
		}
	}
	// Pre-existing rows stay visible through the same path.
	old := db.query(d, "SELECT rowid FROM docs WHERE body = 'aaa';")
	testing.expect(t, old.ok, "old rows visible in txn")
	if old.ok {
		testing.expect_value(t, len(old.rows), 2)
	}
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback")
	gone := db.query(d, "SELECT rowid FROM docs WHERE body = 'txn';")
	testing.expect(t, gone.ok, "post-rollback select succeeds")
	if gone.ok {
		testing.expect_value(t, len(gone.rows), 0)
	}
}

@(test)
test_index_select_prefix_covering :: proc(t: ^testing.T) {
	// Covering prefix: `SELECT rowid ... WHERE body LIKE 'stem%'` matches
	// the oracle union in rowid order; non-canonical LIKE shapes fall back
	// to the scan with identical results.
	d := setup_db(t, "idxselpre")
	defer teardown_db(d, "idxselpre")

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

	// Oracle union for a stem: every indexed text carrying it, rowid-sorted.
	union_for_stem :: proc(expect: map[string][dynamic]i64, stem: string) -> [dynamic]i64 {
		out := make([dynamic]i64, 0, 8, context.temp_allocator)
		for text, rids in expect {
			if len(text) >= len(stem) && text[:len(stem)] == stem {
				append(&out, ..rids[:])
			}
		}
		for i in 1 ..< len(out) {
			for j := i; j > 0; j -= 1 {
				if out[j] < out[j - 1] {
					out[j], out[j - 1] = out[j - 1], out[j]
				} else {
					break
				}
			}
		}
		return out
	}

	stems := []string{"al", "b", "g", "alpha", "z"}
	for stem in stems {
		free_all(context.temp_allocator)
		sql := fmt.tprintf("SELECT rowid FROM docs WHERE body LIKE '%s%%';", stem)
		q := db.query(d, sql)
		testing.expect(t, q.ok, "covering prefix succeeds")
		if !q.ok {
			continue
		}
		want := union_for_stem(expect, stem)
		testing.expect_value(t, len(q.rows), len(want))
		for i in 0 ..< min(len(q.rows), len(want)) {
			v, is_int := q.rows[i][0].(i64)
			testing.expect(t, is_int, "rowid projects as INTEGER")
			if is_int {
				testing.expect_value(t, v, want[i])
			}
		}
	}
	free_all(context.temp_allocator)

	// Non-canonical shapes: leading %, interior %, _ wildcard — scan path,
	// same results as the engine's like_match (verified row by row here
	// against literal expectations, not the index).
	lead := db.query(d, "SELECT id FROM docs WHERE body LIKE '%pha';")
	testing.expect(t, lead.ok, "leading-% succeeds")
	if lead.ok {
		testing.expect_value(t, len(lead.rows), len(expect["alpha"]))
	}
	interior := db.query(d, "SELECT id FROM docs WHERE body LIKE 'a%a%';")
	testing.expect(t, interior.ok, "interior-% succeeds")
	under := db.query(d, "SELECT id FROM docs WHERE body LIKE 'a_pha';")
	testing.expect(t, under.ok, "underscore succeeds")
	if under.ok {
		testing.expect_value(t, len(under.rows), len(expect["alpha"]))
	}
	// LIKE '%' matches every non-NULL body (60 rows, NULLs excluded).
	star := db.query(d, "SELECT id FROM docs WHERE body LIKE '%';")
	testing.expect(t, star.ok, "bare-% succeeds")
	if star.ok {
		testing.expect_value(t, len(star.rows), 60)
	}
}

@(test)
test_index_select_fetch :: proc(t: ^testing.T) {
	// Fetch+recheck: wider projections over routed predicates return full,
	// exact rows — equality, prefix, AND chains, ORDER BY/LIMIT tails.
	d := setup_db(t, "idxselfetch")
	defer teardown_db(d, "idxselfetch")

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

	// Equality fetch: full rows, exact set, values intact.
	full := db.query(d, "SELECT id, body, v FROM docs WHERE body = 'alpha';")
	testing.expect(t, full.ok, "fetch select succeeds")
	if full.ok {
		testing.expect_value(t, len(full.rows), len(expect["alpha"]))
		for row in full.rows {
			testing.expect(t, row[1].(string) == "alpha", "predicate holds")
			testing.expect_value(t, row[2].(i64), row[0].(i64) * 10)
		}
	}

	// Prefix fetch with AND narrowing: recheck proves exactness.
	andq := db.query(d, "SELECT id FROM docs WHERE body LIKE 'a%' AND v > 100;")
	testing.expect(t, andq.ok, "prefix-and succeeds")
	if andq.ok {
		for row in andq.rows {
			testing.expect(t, row[0].(i64) > 10, "recheck narrowed")
		}
		// alpha ids with v = id*10 > 100 → id > 10 among alpha rows.
		narrow := 0
		for r in expect["alpha"] {
			if r > 10 {
				narrow += 1
			}
		}
		testing.expect_value(t, len(andq.rows), narrow)
	}

	// ORDER BY + LIMIT over a routed predicate (tails unchanged).
	ord := db.query(d, "SELECT id FROM docs WHERE body = 'alpha' ORDER BY id DESC LIMIT 3;")
	testing.expect(t, ord.ok, "order+limit succeeds")
	if ord.ok {
		testing.expect_value(t, len(ord.rows), min(3, len(expect["alpha"])))
		want := expect["alpha"]
		for i in 0 ..< len(ord.rows) {
			testing.expect_value(t, ord.rows[i][0].(i64), want[len(want) - 1 - i])
		}
	}

	// Second conjunct usable, first not: routing still finds candidates.
	second := db.query(d, "SELECT id FROM docs WHERE v > 0 AND body = 'bb';")
	testing.expect(t, second.ok, "second-conjunct routing succeeds")
	if second.ok {
		testing.expect_value(t, len(second.rows), len(expect["bb"]))
	}
}

// twin_values_equal compares two result values for the differential twin
// test (same semantics as the engine's NULL-strict equality: NULL never
// equals, cross-class never equals, same-class by natural order).
twin_values_equal :: proc(a, b: types.Value) -> bool {
	// Output comparison (not filter semantics): NULL equals NULL here —
	// both sides read the same stored cell.
	if types.is_null(a) && types.is_null(b) {
		return true
	}
	if types.is_null(a) || types.is_null(b) {
		return false
	}
	#partial switch va in a {
	case i64:
		if vb, ok := b.(i64); ok {
			return va == vb
		}
	case f64:
		if vb, ok := b.(f64); ok {
			return va == vb
		}
	case string:
		if vb, ok := b.(string); ok {
			return va == vb
		}
	case []u8:
		if vb, ok := b.([]u8); ok {
			if len(va) != len(vb) {
				return false
			}
			for i in 0 ..< len(va) {
				if va[i] != vb[i] {
					return false
				}
			}
			return true
		}
	}
	return false
}

@(test)
test_index_select_fetch_matches_scan :: proc(t: ^testing.T) {
	// Differential twin: indexed docs vs unindexed docs2, same rows. Every
	// routed shape — including LIMIT without ORDER BY — returns identical
	// output, proving candidate order == scan order.
	d := setup_db(t, "idxseltwin")
	defer teardown_db(d, "idxseltwin")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create docs",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs2 (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create docs2",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)
	bodies := []string{"alpha", "bb", "alpha", "", "gamma"}
	for i in 1 ..= 60 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO docs VALUES (%d, '%s', %d);", i, body, i)) ==
			.None,
			"insert docs",
		)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO docs2 VALUES (%d, '%s', %d);", i, body, i)) ==
			.None,
			"insert docs2",
		)
	}
	free_all(context.temp_allocator)

	queries := []string {
		"SELECT id, body, v FROM docs WHERE body = 'alpha';",
		"SELECT id FROM docs WHERE body LIKE 'a%';",
		"SELECT id, body FROM docs WHERE body LIKE 'b%' ORDER BY id DESC;",
		"SELECT id FROM docs WHERE body = 'alpha' LIMIT 5;",
		"SELECT id FROM docs WHERE body LIKE 'a%' LIMIT 7;",
		"SELECT id FROM docs WHERE body IN ('alpha', 'bb');",
		"SELECT id FROM docs WHERE body = 'alpha' AND v > 5 LIMIT 4;",
		"SELECT id FROM docs WHERE body = 'missing';",
		"SELECT id FROM docs WHERE body LIKE 'z%';",
	}
	for q in queries {
		free_all(context.temp_allocator)
		twin, _ := strings.replace(q, "docs", "docs2", 1, context.temp_allocator)
		a := db.query(d, q)
		b := db.query(d, twin)
		testing.expect(t, a.ok && b.ok, "twin queries succeed")
		if !a.ok || !b.ok {
			continue
		}
		testing.expectf(t, len(a.rows) == len(b.rows), "twin row counts match for %s", q)
		for i in 0 ..< min(len(a.rows), len(b.rows)) {
			testing.expect(t, len(a.rows[i]) == len(b.rows[i]), "twin widths match")
			for j in 0 ..< min(len(a.rows[i]), len(b.rows[i])) {
				testing.expect(
					t,
					twin_values_equal(a.rows[i][j], b.rows[i][j]),
					"twin values match",
				)
			}
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_index_select_in :: proc(t: ^testing.T) {
	// IN routing: literal lists (covering + fetch), mixed-type members,
	// and subquery IN (scan fallback, still correct).
	d := setup_db(t, "idxselin")
	defer teardown_db(d, "idxselin")

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

	// Covering IN: union, rowid-sorted.
	inq := db.query(d, "SELECT rowid FROM docs WHERE body IN ('alpha', 'bb');")
	testing.expect(t, inq.ok, "covering in succeeds")
	if inq.ok {
		testing.expect_value(t, len(inq.rows), len(expect["alpha"]) + len(expect["bb"]))
		prev := i64(0)
		for row in inq.rows {
			v, is_int := row[0].(i64)
			testing.expect(t, is_int, "rowid projects as INTEGER")
			if is_int {
				testing.expect(t, v > prev, "in results rowid-ordered")
				prev = v
			}
		}
	}

	// Fetch IN with a duplicate member (dedup proves single emission).
	dedup := db.query(d, "SELECT id FROM docs WHERE body IN ('alpha', 'alpha');")
	testing.expect(t, dedup.ok, "dup-member in succeeds")
	if dedup.ok {
		testing.expect_value(t, len(dedup.rows), len(expect["alpha"]))
	}

	// Mixed-type members: non-TEXT members provably match nothing.
	mixed := db.query(d, "SELECT id FROM docs WHERE body IN ('bb', 1, NULL);")
	testing.expect(t, mixed.ok, "mixed in succeeds")
	if mixed.ok {
		testing.expect_value(t, len(mixed.rows), len(expect["bb"]))
	}

	// Subquery IN: scan fallback, correct results.
	testing.expect(t, db.execute(d, "CREATE TABLE keys (k TEXT);") == .None, "create keys")
	testing.expect(t, db.execute(d, "INSERT INTO keys VALUES ('alpha');") == .None, "insert key")
	sub := db.query(d, "SELECT id FROM docs WHERE body IN (SELECT k FROM keys);")
	testing.expect(t, sub.ok, "subquery in succeeds")
	if sub.ok {
		testing.expect_value(t, len(sub.rows), len(expect["alpha"]))
	}
}

@(test)
test_index_select_fetch_in_txn :: proc(t: ^testing.T) {
	// Fetch-path reads see staged in-txn rows through the routed index;
	// ROLLBACK removes them (same visibility rule as the covering-index
	// txn test).
	d := setup_db(t, "idxselfetchtxn")
	defer teardown_db(d, "idxselfetchtxn")

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
	expect := make(map[string][dynamic]i64, context.allocator)
	defer destroy_expect(&expect)
	index_workload_simple(t, d, &expect)

	testing.expect(t, db.execute(d, "BEGIN;") == .None, "begin")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (101, 'txnbody');") == .None,
		"txn insert",
	)
	staged := db.query(d, "SELECT id, body FROM docs WHERE body LIKE 'txn%';")
	testing.expect(t, staged.ok, "staged fetch succeeds")
	if staged.ok {
		testing.expect_value(t, len(staged.rows), 1)
		if len(staged.rows) == 1 {
			testing.expect_value(t, staged.rows[0][0].(i64), 101)
			testing.expect_value(t, staged.rows[0][1].(string), "txnbody")
		}
	}
	testing.expect(t, db.execute(d, "ROLLBACK;") == .None, "rollback")
	gone := db.query(d, "SELECT id FROM docs WHERE body LIKE 'txn%';")
	testing.expect(t, gone.ok, "post-rollback fetch succeeds")
	if gone.ok {
		testing.expect_value(t, len(gone.rows), 0)
	}
}

@(test)
test_index_explain :: proc(t: ^testing.T) {
	// EXPLAIN renders the router's decision and never disagrees with
	// execution (same resolvers, same order): PK SEEK, INDEX SCAN with
	// shape and covering/fetch, or FULL SCAN. Non-single-table SELECTs
	// keep the legacy echo.
	d := setup_db(t, "idxselexplain")
	defer teardown_db(d, "idxselexplain")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE plain (id INT PRIMARY KEY, body TEXT);") == .None,
		"create plain",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"create index",
	)

	explain :: proc(t: ^testing.T, d: ^db.Database, sql: string) -> string {
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

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';"),
		"INDEX SCAN ON docs USING body (eq, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM docs WHERE body LIKE 'al%';"),
		"INDEX SCAN ON docs USING body (prefix, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body IN ('a', 'b');"),
		"INDEX SCAN ON docs USING body (in, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM docs WHERE id = 1;"),
		"PK SEEK ON docs",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'a' OR body = 'b';"),
		"INDEX SCAN ON docs USING body (or, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'a' OR body = 'b';"),
		"INDEX SCAN ON docs USING body (or, covering)",
	)
	// OR with an unusable disjunct (other column, negated, nested)
	// still falls back: the union would be incomplete.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'a' OR v = 1;"),
		"FULL SCAN ON docs",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM docs WHERE NOT body = 'a';"),
		"FULL SCAN ON docs",
	)
	testing.expect_value(t, explain(t, d, "EXPLAIN SELECT id FROM docs;"), "FULL SCAN ON docs")
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT id FROM plain WHERE body = 'a';"),
		"FULL SCAN ON plain",
	)
	// Legacy echo preserved where there is no single-table decision.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT * FROM docs INNER JOIN plain ON docs.id = plain.id;"),
		"SELECT * FROM docs INNER JOIN plain ON docs.id = plain.id;",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN CREATE TABLE x (id INT);"),
		"CREATE TABLE x (id INT);",
	)
}

@(test)
test_index_unique_group_persists_across_reopen :: proc(t: ^testing.T) {
	// Composite groups live in the catalog blob: close/reopen must keep
	// enforcing them, not just freshly created tables.
	name := "idxuniqreopen"
	d := setup_db(t, name)
	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT, UNIQUE (a, b));",
			"INSERT INTO t VALUES (1, 'x', 10);",
		},
	)
	db.close(d)

	d2, open_err := db.open(fmt.tprintf("test_int_%s.db", name))
	defer teardown_db(d2, name)
	testing.expect(t, open_err == .None, "Re-open should succeed")
	if open_err != .None {
		return
	}
	saved, quiet := suppress_expected_errors()
	context = quiet
	dup := db.execute(d2, "INSERT INTO t VALUES (1, 'x', 20);")
	context = restore_logger(saved)
	testing.expect(t, dup != .None, "reopened table still rejects dup tuples")
}
