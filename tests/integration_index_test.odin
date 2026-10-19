// Integration index tests: CREATE/DROP, DML fan-out, transactional index
// updates, vacuum, and covering/fetch/prefix SELECT routing with EXPLAIN
// pins. File-backed through setup_db/teardown_db.
package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:btree"
import "src:cell"
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
		db.execute(
			d,
			"CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, score REAL, data BLOB);",
		) ==
		.None,
		"create",
	)
	saved, quiet := suppress_expected_errors()
	context = quiet
	neg_table := db.execute(d, "CREATE INDEX i_nope ON missing (body);")
	neg_col := db.execute(d, "CREATE INDEX i_nope ON docs (missing);")
	neg_blob := db.execute(d, "CREATE INDEX i_nope ON docs (data);")
	context = restore_logger(saved)
	testing.expect(t, neg_table != .None, "unknown table rejected")
	testing.expect(t, neg_col != .None, "unknown column rejected")
	testing.expect(t, neg_blob != .None, "BLOB column rejected")
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_score ON docs (score);") == .None,
		"REAL column accepted",
	)
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
test_create_composite_index :: proc(t: ^testing.T) {
	// Multi-column DDL validates every component (present, indexable —
	// BLOB fails naming the column), backfills framed tuples with the def
	// tagged per-component, and SQL stays on the scan path (planner
	// untouched this phase).
	d := setup_db(t, "idxcompcreate")
	defer teardown_db(d, "idxcompcreate")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, r REAL, g BLOB);",
			"INSERT INTO t VALUES (10, 'x', 1.5, X'01');",
			"INSERT INTO t VALUES (-5, 'y', -2.5, X'02');",
			"INSERT INTO t VALUES (10, 'z', 1.5, X'03');",
			"INSERT INTO t VALUES (NULL, 'n', NULL, X'04');",
		},
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_ab ON t (a, b);") == .None,
		"composite create succeeds",
	)

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "t", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	def, dok := schema.table_index(tbl, "i_ab")
	testing.expect(t, dok, "def present")
	if dok {
		testing.expect(t, def.root != 0, "root published")
		testing.expect_value(t, def.column, "a,b")
		testing.expect(t, len(def.columns) == 2, "two columns resolve")
		if len(def.col_types) == 2 {
			testing.expect_value(t, def.col_types[0], types.Column_Type.INTEGER)
			testing.expect_value(t, def.col_types[1], types.Column_Type.TEXT)
		}
	}

	// Backfilled tuples read through the published root (NULL row skips:
	// 3 tuples for 4 rows).
	comp_root := index_root_of(t, tbl, "i_ab")
	testing.expect(t, comp_root > 0, "composite root present")
	{
		idx_tree := btree.init(d.pager, comp_root)
		cnt, c_err := btree.tree_count_rows(&idx_tree)
		testing.expect(t, c_err == .None, "count succeeds")
		testing.expect_value(t, cnt, 3)
		free_all(context.temp_allocator)
	}

	saved, quiet := suppress_expected_errors()
	context = quiet
	neg_col := db.execute(d, "CREATE INDEX i_no ON t (a, missing);")
	neg_blob := db.execute(d, "CREATE INDEX i_bl ON t (a, g);")
	dup := db.execute(d, "CREATE INDEX i_ab ON t (a, b);")
	context = restore_logger(saved)
	testing.expect(t, neg_col != .None, "unknown component rejected")
	testing.expect(t, neg_blob != .None, "BLOB component rejected")
	testing.expect(t, dup != .None, "duplicate composite name rejected")

	// Routed SQL answers through the new index.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b FROM t WHERE a = 10 AND b = 'x';"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	q := db.query(d, "SELECT a, b FROM t WHERE a = 10 AND b = 'x' ORDER BY b;")
	testing.expect(t, q.ok, "unrouted select succeeds")
	testing.expect_value(t, len(q.rows), 1)
}

@(test)
test_index_composite_routing :: proc(t: ^testing.T) {
	// Composite routing: full-tuple Eq answers exact, leading-column Eq
	// answers prefix scans (both fetch-only); non-leading filters,
	// contradictions, and cross-class literals behave exactly; EXPLAIN
	// names the composite shape.
	d := setup_db(t, "idxcomproute")
	defer teardown_db(d, "idxcomproute")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT);",
			"INSERT INTO t VALUES (10, 'x', 1);",
			"INSERT INTO t VALUES (-5, 'y', 2);",
			"INSERT INTO t VALUES (10, 'z', 3);",
			"INSERT INTO t VALUES (10, 'x', 4);",
			"INSERT INTO t VALUES (NULL, 'n', 5);",
			"CREATE INDEX i_ab ON t (a, b);",
		},
	)
	full := db.query(d, "SELECT a, b, v FROM t WHERE a = 10 AND b = 'x' ORDER BY v;")
	testing.expect(t, full.ok, "full eq succeeds")
	testing.expect_value(t, len(full.rows), 2)

	lead := db.query(d, "SELECT a, b, v FROM t WHERE a = 10 ORDER BY b, v;")
	testing.expect(t, lead.ok, "leading eq succeeds")
	testing.expect_value(t, len(lead.rows), 3)

	// Non-leading column alone never routes the composite (scan answers).
	second := db.query(d, "SELECT a, b, v FROM t WHERE b = 'x' ORDER BY v;")
	testing.expect(t, second.ok, "second-col succeeds")
	testing.expect_value(t, len(second.rows), 2)

	contra := db.query(d, "SELECT a, b, v FROM t WHERE a = 10 AND a = 11;")
	testing.expect(t, contra.ok, "contradiction succeeds")
	testing.expect_value(t, len(contra.rows), 0)

	// Leading Eq plus an extra conjunct narrows through the recheck.
	narrow := db.query(d, "SELECT a, b, v FROM t WHERE a = 10 AND v > 2 ORDER BY v;")
	testing.expect(t, narrow.ok, "narrowed succeeds")
	testing.expect_value(t, len(narrow.rows), 2)

	// Rowid covering answers from the index alone.
	rid := db.query(d, "SELECT rowid FROM t WHERE a = 10 AND b = 'z' ORDER BY rowid;")
	testing.expect(t, rid.ok, "rowid covering succeeds")
	testing.expect_value(t, len(rid.rows), 1)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a = 10 AND b = 'x';"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a = 10;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE b = 'x';"),
		"FULL SCAN ON t",
	)
}

@(test)
test_index_composite_range :: proc(t: ^testing.T) {
	// Leading-column ranges scan the composite slice: single bounds,
	// tightened pairs, contradictions empty without I/O, Eq dominating
	// ranges, and range-plus-Eq narrowing through the recheck.
	d := setup_db(t, "idxcomprange")
	defer teardown_db(d, "idxcomprange")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT);",
			"INSERT INTO t VALUES (10, 'x', 1);",
			"INSERT INTO t VALUES (-5, 'y', 2);",
			"INSERT INTO t VALUES (7, 'z', 3);",
			"INSERT INTO t VALUES (10, 'w', 4);",
			"INSERT INTO t VALUES (NULL, 'n', 5);",
			"CREATE INDEX i_ab ON t (a, b);",
		},
	)
	cases := []struct {
		sql : string,
		want: int,
	} {
		{"SELECT a, b, v FROM t WHERE a > 5;", 3},
		{"SELECT a, b, v FROM t WHERE a >= 7 AND a <= 10;", 3},
		{"SELECT a, b, v FROM t WHERE a > 100;", 0},
		{"SELECT a, b, v FROM t WHERE a < 10 AND a > 5;", 1},
		{"SELECT a, b, v FROM t WHERE a > 5 AND b = 'w';", 1},
		{"SELECT a, b, v FROM t WHERE a = 10 AND a > 5;", 2},
		{"SELECT a, b, v FROM t WHERE a > 10 AND a < 5;", 0},
		{"SELECT a, b, v FROM t WHERE a > 'x';", 0},
		{"SELECT a, b, v FROM t WHERE a < 10;", 2},
	}
	for c in cases {
		free_all(context.temp_allocator)
		q := db.query(d, c.sql)
		testing.expect(t, q.ok, "range query succeeds")
		if q.ok {
			testing.expectf(t, len(q.rows) == c.want, "row count for %s", c.sql)
		}
	}
	free_all(context.temp_allocator)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a > 5;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a >= 7 AND a <= 10;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
}

@(test)
test_index_composite_text_leading_range :: proc(t: ^testing.T) {
	// Leading TEXT ranges order BINARY (empty string included); leading
	// REAL ranges order numeric with signed zeros shared.
	d := setup_db(t, "idxcomplead")
	defer teardown_db(d, "idxcomplead")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (b TEXT, a INT);",
			"INSERT INTO t VALUES ('x', 10);",
			"INSERT INTO t VALUES ('m', 3);",
			"INSERT INTO t VALUES ('', 1);",
			"INSERT INTO t VALUES ('y', -5);",
			"INSERT INTO t VALUES (NULL, 9);",
			"CREATE INDEX i_ba ON t (b, a);",
			"CREATE TABLE n (r REAL, v INT);",
			"INSERT INTO n VALUES (1.5, 1);",
			"INSERT INTO n VALUES (-0.0, 2);",
			"INSERT INTO n VALUES (0.0, 3);",
			"INSERT INTO n VALUES (-2.5, 4);",
			"CREATE INDEX i_rv ON n (r, v);",
		},
	)
	cases := []struct {
		sql : string,
		want: int,
	} {
		{"SELECT b, a FROM t WHERE b > 'm';", 2},
		{"SELECT b, a FROM t WHERE b >= 'm';", 3},
		{"SELECT b, a FROM t WHERE b > '';", 3},
		{"SELECT b, a FROM t WHERE b BETWEEN 'm' AND 'y';", 3},
		{"SELECT r, v FROM n WHERE r > 0.0;", 1},
		{"SELECT r, v FROM n WHERE r >= -0.0;", 3},
		{"SELECT r, v FROM n WHERE r BETWEEN -3.0 AND 0.0;", 3},
	}
	for c in cases {
		free_all(context.temp_allocator)
		q := db.query(d, c.sql)
		testing.expect(t, q.ok, "range query succeeds")
		if q.ok {
			testing.expectf(t, len(q.rows) == c.want, "row count for %s", c.sql)
		}
	}
	free_all(context.temp_allocator)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT b, a FROM t WHERE b > 'm';"),
		"INDEX SCAN ON t USING b,a (composite, fetch)",
	)
}

@(test)
test_index_composite_or_union :: proc(t: ^testing.T) {
	// OR of ANDs folds each disjunct through its composite def (union of
	// exact scans); mixed scalar/composite disjuncts union too. Unusable
	// disjuncts fail the whole union to scan.
	d := setup_db(t, "idxcompor")
	defer teardown_db(d, "idxcompor")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT);",
			"INSERT INTO t VALUES (10, 'x', 1);",
			"INSERT INTO t VALUES (-5, 'y', 2);",
			"INSERT INTO t VALUES (7, 'z', 3);",
			"INSERT INTO t VALUES (10, 'w', 4);",
			"CREATE INDEX i_ab ON t (a, b);",
		},
	)
	full := db.query(
		d,
		"SELECT a, b, v FROM t WHERE (a = 10 AND b = 'x') OR (a = 7 AND b = 'z') ORDER BY v;",
	)
	testing.expect(t, full.ok, "union succeeds")
	testing.expect_value(t, len(full.rows), 2)

	mixed := db.query(d, "SELECT a, b, v FROM t WHERE a = 10 OR (a = 7 AND b = 'z') ORDER BY v;")
	testing.expect(t, mixed.ok, "mixed union succeeds")
	testing.expect_value(t, len(mixed.rows), 3)

	half := db.query(
		d,
		"SELECT a, b, v FROM t WHERE (a = 10 AND b = 'q') OR (a = 7 AND b = 'z') ORDER BY v;",
	)
	testing.expect(t, half.ok, "half-empty union succeeds")
	testing.expect_value(t, len(half.rows), 1)

	rid := db.query(
		d,
		"SELECT rowid FROM t WHERE (a = 10 AND b = 'x') OR (a = -5 AND b = 'y') ORDER BY rowid;",
	)
	testing.expect(t, rid.ok, "rowid union succeeds")
	testing.expect_value(t, len(rid.rows), 2)

	bad := db.query(d, "SELECT a, b, v FROM t WHERE (a = 10 AND b = 'x') OR v = 2 ORDER BY v;")
	testing.expect(t, bad.ok, "unusable disjunct succeeds")
	testing.expect_value(t, len(bad.rows), 2)

	testing.expect_value(
		t,
		explain(
			t,
			d,
			"EXPLAIN SELECT a, b, v FROM t WHERE (a = 10 AND b = 'x') OR (a = 7 AND b = 'z');",
		),
		"INDEX SCAN ON t USING a,b (or, fetch)",
	)
}

@(test)
test_index_composite_in_expansion :: proc(t: ^testing.T) {
	// Leading IN with bound trailing Eqs expands to a union of exact
	// full-tuple lookups; bare leading IN unions prefix scans. Over-cap
	// and subquery IN stay on scan with identical results.
	d := setup_db(t, "idxcompin")
	defer teardown_db(d, "idxcompin")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT);",
			"INSERT INTO t VALUES (10, 'x', 1);",
			"INSERT INTO t VALUES (-5, 'y', 2);",
			"INSERT INTO t VALUES (7, 'z', 3);",
			"INSERT INTO t VALUES (10, 'w', 4);",
			"CREATE INDEX i_ab ON t (a, b);",
		},
	)
	full := db.query(d, "SELECT a, b, v FROM t WHERE a IN (10, 7) AND b = 'x' ORDER BY v;")
	testing.expect(t, full.ok, "expansion succeeds")
	testing.expect_value(t, len(full.rows), 1)

	pre := db.query(d, "SELECT a, b, v FROM t WHERE a IN (10, -5) ORDER BY a, b, v;")
	testing.expect(t, pre.ok, "prefix union succeeds")
	testing.expect_value(t, len(pre.rows), 3)

	miss := db.query(d, "SELECT a, b, v FROM t WHERE a IN (10, 7) AND b = 'q' ORDER BY v;")
	testing.expect(t, miss.ok, "empty expansion succeeds")
	testing.expect_value(t, len(miss.rows), 0)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a IN (10, 7) AND b = 'x';"),
		"INDEX SCAN ON t USING a,b (or, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a IN (10, -5);"),
		"INDEX SCAN ON t USING a,b (or, fetch)",
	)
}

@(test)
test_index_composite_covering :: proc(t: ^testing.T) {
	// Composite covering: single leading-Eq prefix plans project bound
	// columns (decoded with column types) without touching data pages;
	// anything wider, unbound, or ranged fetches.
	d := setup_db(t, "idxcompcov")
	defer teardown_db(d, "idxcompcov")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"INSERT INTO t VALUES (10, 'x');",
			"INSERT INTO t VALUES (10, 'z');",
			"INSERT INTO t VALUES (-5, 'y');",
			"CREATE INDEX i_ab ON t (a, b);",
			"CREATE TABLE n (r REAL, v INT);",
			"INSERT INTO n VALUES (1.5, 1);",
			"INSERT INTO n VALUES (1.5, 2);",
			"INSERT INTO n VALUES (-2.5, 3);",
			"CREATE INDEX i_rv ON n (r, v);",
		},
	)
	ca := db.query(d, "SELECT a FROM t WHERE a = 10 ORDER BY a;")
	testing.expect(t, ca.ok, "int covering succeeds")
	testing.expect_value(t, len(ca.rows), 2)
	for row in ca.rows {
		v, is_int := row[0].(i64)
		testing.expect(t, is_int, "covering projects INTEGER")
		if is_int {
			testing.expect_value(t, v, 10)
		}
	}

	cr := db.query(d, "SELECT r FROM n WHERE r = 1.5 ORDER BY r;")
	testing.expect(t, cr.ok, "real covering succeeds")
	testing.expect_value(t, len(cr.rows), 2)
	for row in cr.rows {
		v, is_real := row[0].(f64)
		testing.expect(t, is_real, "covering projects REAL")
		if is_real {
			testing.expect_value(t, v, 1.5)
		}
	}

	// Unbound projection, wide projection, and ranges fetch.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a = 10;"),
		"INDEX SCAN ON t USING a,b (composite, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT b FROM t WHERE a = 10;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b FROM t WHERE a = 10;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a > 5;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT r FROM n WHERE r = 1.5;"),
		"INDEX SCAN ON n USING r,v (composite, covering)",
	)
	fb := db.query(d, "SELECT b FROM t WHERE a = 10 ORDER BY b;")
	testing.expect(t, fb.ok, "fetch succeeds")
	testing.expect_value(t, len(fb.rows), 2)
}

@(test)
test_index_composite_trailing_null :: proc(t: ^testing.T) {
	// Prefix scans include leading-filtered rows with NULL in unfiltered
	// trailing columns (framed markers stay inside the prefix run);
	// leading NULLs stay out (no routable predicate matches them).
	d := setup_db(t, "idxcompnull")
	defer teardown_db(d, "idxcompnull")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, v INT);",
			"INSERT INTO t VALUES (5, 'x', 1);",
			"INSERT INTO t VALUES (5, NULL, 2);",
			"INSERT INTO t VALUES (5, 'y', 3);",
			"INSERT INTO t VALUES (NULL, 'q', 4);",
			"CREATE INDEX i_ab ON t (a, b);",
		},
	)
	q := db.query(d, "SELECT a, b, v FROM t WHERE a = 5 ORDER BY v;")
	testing.expect(t, q.ok, "prefix succeeds")
	testing.expect_value(t, len(q.rows), 3)

	ca := db.query(d, "SELECT a FROM t WHERE a = 5 ORDER BY a;")
	testing.expect(t, ca.ok, "leading covering succeeds")
	testing.expect_value(t, len(ca.rows), 3)

	cov := db.query(d, "SELECT b FROM t WHERE a = 5 ORDER BY v;")
	testing.expect(t, cov.ok, "fetch succeeds")
	testing.expect_value(t, len(cov.rows), 3)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, v FROM t WHERE a = 5;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a = 5;"),
		"INDEX SCAN ON t USING a,b (composite, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT b FROM t WHERE a = 5;"),
		"INDEX SCAN ON t USING a,b (composite, fetch)",
	)
}

@(test)
test_index_composite_three_col :: proc(t: ^testing.T) {
	// Three columns: full binds route exact, leading pairs route prefix,
	// middle gaps degrade to the bound prefix (the gap rides the recheck),
	// and a bound non-leading column alone never routes.
	d := setup_db(t, "idxcomp3")
	defer teardown_db(d, "idxcomp3")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT, c INT);",
			"INSERT INTO t VALUES (1, 'x', 10);",
			"INSERT INTO t VALUES (1, 'x', 20);",
			"INSERT INTO t VALUES (1, 'y', 10);",
			"INSERT INTO t VALUES (2, 'x', 10);",
			"CREATE INDEX i_abc ON t (a, b, c);",
		},
	)
	full := db.query(d, "SELECT a, b, c FROM t WHERE a = 1 AND b = 'x' AND c = 10 ORDER BY a;")
	testing.expect(t, full.ok, "full succeeds")
	testing.expect_value(t, len(full.rows), 1)

	pair := db.query(d, "SELECT a, b, c FROM t WHERE a = 1 AND b = 'x' ORDER BY c;")
	testing.expect(t, pair.ok, "pair succeeds")
	testing.expect_value(t, len(pair.rows), 2)

	gap := db.query(d, "SELECT a, b, c FROM t WHERE a = 1 AND c = 10 ORDER BY b;")
	testing.expect(t, gap.ok, "gap succeeds")
	testing.expect_value(t, len(gap.rows), 2)

	second := db.query(d, "SELECT a, b, c FROM t WHERE b = 'x' ORDER BY a;")
	testing.expect(t, second.ok, "non-leading succeeds")
	testing.expect_value(t, len(second.rows), 3)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, c FROM t WHERE a = 1 AND b = 'x' AND c = 10;"),
		"INDEX SCAN ON t USING a,b,c (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, c FROM t WHERE a = 1 AND c = 10;"),
		"INDEX SCAN ON t USING a,b,c (composite, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b, c FROM t WHERE b = 'x';"),
		"FULL SCAN ON t",
	)
}

@(test)
test_index_composite_twin :: proc(t: ^testing.T) {
	// Differential twin: indexed t vs unindexed t2 over 400 mixed rows.
	// Every composite shape — full, prefix, narrowed, union-adjacent,
	// limits — returns identical output.
	d := setup_db(t, "idxcomptwin")
	defer teardown_db(d, "idxcomptwin")

	testing.expect(t, db.execute(d, "CREATE TABLE t (a INT, b TEXT, v INT);") == .None, "create t")
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t2 (a INT, b TEXT, v INT);") == .None,
		"create t2",
	)
	testing.expect(t, db.execute(d, "CREATE INDEX i_ab ON t (a, b);") == .None, "index")
	bodies := []string{"alpha", "bb", "gamma"}
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		val := i % 50 - 25
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, '%s', %d);", val, body, i)) ==
			.None,
			"insert t",
		)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t2 VALUES (%d, '%s', %d);", val, body, i)) ==
			.None,
			"insert t2",
		)
	}
	free_all(context.temp_allocator)
	testing.expect(t, db.execute(d, "INSERT INTO t VALUES (NULL, 'n', 401);") == .None, "null")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO t2 VALUES (NULL, 'n', 401);") == .None,
		"null twin",
	)
	testing.expect(t, db.execute(d, "INSERT INTO t VALUES (7, NULL, 402);") == .None, "trailing null")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO t2 VALUES (7, NULL, 402);") == .None,
		"trailing null twin",
	)

	queries := []string {
		"SELECT a, b, v FROM t WHERE a = 7 AND b = 'alpha';",
		"SELECT a, b, v FROM t WHERE a = 7;",
		"SELECT a, b, v FROM t WHERE a = 7 AND v > 100;",
		"SELECT a, b, v FROM t WHERE a = 7 AND a = 8;",
		"SELECT a, b, v FROM t WHERE b = 'alpha';",
		"SELECT a, b, v FROM t WHERE a = 7 AND b = 'missing';",
		"SELECT a, b, v FROM t WHERE a = 7 OR b = 'alpha';",
		"SELECT a, b, v FROM t WHERE (a = 7 AND b = 'alpha') OR (a = -5 AND b = 'bb');",
		"SELECT a, b, v FROM t WHERE a IN (7, -5) AND b = 'alpha';",
		"SELECT a, b, v FROM t WHERE a IN (7, -5);",
		"SELECT a FROM t WHERE a = 7 ORDER BY a;",
		"SELECT a FROM t WHERE a = 7 AND b = 'alpha' ORDER BY a;",
		"SELECT a, b, v FROM t WHERE a > 5;",
		"SELECT a, b, v FROM t WHERE a >= 7 AND a <= 24;",
		"SELECT a, b, v FROM t WHERE a > 5 AND b = 'alpha';",
		"SELECT a, b, v FROM t WHERE a = 7 AND a > 0;",
		"SELECT a, b, v FROM t WHERE a = 7 LIMIT 5;",
		"SELECT a, b, v FROM t WHERE a = 7 AND b = 'alpha' ORDER BY v DESC LIMIT 3;",
		"SELECT rowid FROM t WHERE a = 7 AND b = 'bb' ORDER BY rowid;",
		"SELECT rowid FROM t WHERE a = 7 ORDER BY rowid LIMIT 4;",
		"SELECT a, b, v FROM t WHERE a = 7 ORDER BY v;",
		"SELECT b FROM t WHERE a = 7 ORDER BY v;",
	}
	for q in queries {
		free_all(context.temp_allocator)
		twin, _ := strings.replace(q, "FROM t ", "FROM t2 ", 1, context.temp_allocator)
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
test_index_composite_vacuum :: proc(t: ^testing.T) {
	// Vacuum rebuilds composite indexes into fresh packed pages: new root,
	// identical key->rowid sets, every reachable page validates as
	// composite pages, and SQL keeps routing.
	d := setup_db(t, "idxcompvac")
	defer teardown_db(d, "idxcompvac")

	testing.expect(t, db.execute(d, "CREATE TABLE t (a INT, b TEXT);") == .None, "create")
	testing.expect(t, db.execute(d, "CREATE INDEX i_ab ON t (a, b);") == .None, "create index")
	bodies := []string{"alpha", "bb", "gamma"}
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		sql := fmt.tprintf(
			"INSERT INTO t VALUES (%d, '%s');",
			i % 50 - 25,
			bodies[(i - 1) % len(bodies)],
		)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
	}
	free_all(context.temp_allocator)

	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "t", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_ab")
	testing.expect(t, root_before > 0, "index present before vacuum")

	for i in 4 ..= 400 {
		if (i - 1) % 4 != 3 {
			continue
		}
		free_all(context.temp_allocator)
		sql := fmt.tprintf(
			"DELETE FROM t WHERE a = %d AND b = '%s';",
			i % 50 - 25,
			bodies[(i - 1) % len(bodies)],
		)
		testing.expect(t, db.execute(d, sql) == .None, "delete succeeds")
	}
	free_all(context.temp_allocator)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	st1 := db.Schema_Tree(d)
	tbl1, found1 := schema.find_table(&st1, "t", context.temp_allocator)
	testing.expect(t, found1, "table found after vacuum")
	if !found1 {
		return
	}
	vac_root := index_root_of(t, tbl1, "i_ab")
	testing.expect(t, vac_root > 0, "index root present after vacuum")
	testing.expect(t, vac_root != root_before, "vacuum rebuilt the index")

	// Oracle: pair-level wipe — a DELETE removes every row sharing the
	// targeted pair, so a pair survives iff no multiple-of-4 row carries
	// it. Wiped residues are precomputed once, independently of the index
	// (heap-owned: the pair loop frees temp per iteration).
	wiped := make([]bool, 150, context.allocator)
	defer delete(wiped)
	for j in 4 ..= 400 {
		if (j - 1) % 4 != 3 {
			continue
		}
		wiped[j % 150] = true
	}
	mk_tuple :: proc(a: i64, b: string) -> []types.Value {
		vals := make([]types.Value, 2, context.temp_allocator)
		vals[0] = types.value(a)
		vals[1] = types.value(b)
		return vals
	}
	ct := []types.Column_Type{.INTEGER, .TEXT}
	idx_tree := btree.init(d.pager, vac_root)
	for k in 0 ..< 50 {
		for b in bodies {
			free_all(context.temp_allocator)
			got := composite_index_rowids(t, d, "t", "i_ab", mk_tuple(i64(k - 25), b), ct)
			want := 0
			for i in 1 ..= 400 {
				if i64(i % 50 - 25) == i64(k - 25) &&
				   bodies[(i - 1) % len(bodies)] == b &&
				   !wiped[i % 150] {
					want += 1
				}
			}
			testing.expectf(t, len(got) == want, "pair (%d,%s) count", k - 25, b)
		}
	}
	free_all(context.temp_allocator)

	// Every reachable index page validates as composite pages.
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
		case .LEAF_COMPOSITE:
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"vacuumed leaf validates",
			)
		case .COMPOSITE_INTERIOR:
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

	// SQL keeps routing through the rebuilt index.
	q := db.query(d, "SELECT a, b FROM t WHERE a = 7 AND b = 'alpha' ORDER BY a;")
	testing.expect(t, q.ok, "routed select succeeds")
	for row in q.rows {
		a, aok := row[0].(i64)
		b, bok := row[1].(string)
		testing.expect(t, aok && a == 7, "routed value matches")
		testing.expect(t, bok && b == "alpha", "routed value matches")
	}
}

@(test)
test_index_composite_dml_fanout :: proc(t: ^testing.T) {
	// Per-row writes maintain composite indexes: INSERTs land framed
	// tuples, UPDATEs move them (no-op rewrites and NULL transitions
	// included), DELETEs remove them. Verified at btree level.
	d := setup_db(t, "idxcompfan")
	defer teardown_db(d, "idxcompfan")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"CREATE INDEX i_ab ON t (a, b);",
			"INSERT INTO t VALUES (5, 'x');",
			"INSERT INTO t VALUES (NULL, 'n');",
			"INSERT INTO t VALUES (5, NULL);",
		},
	)
	mk_tuple :: proc(a: i64, b: string) -> []types.Value {
		vals := make([]types.Value, 2, context.temp_allocator)
		vals[0] = types.value(a)
		vals[1] = types.value(b)
		return vals
	}
	ct := []types.Column_Type{.INTEGER, .TEXT}
	five_x := composite_index_rowids(t, d, "t", "i_ab", mk_tuple(5, "x"), ct)
	testing.expect_value(t, len(five_x), 1)
	if len(five_x) == 1 {
		testing.expect_value(t, five_x[0], 1)
	}

	must_db_execute(t, d, "UPDATE t SET b = 'y' WHERE a = 5 AND b = 'x';")
	testing.expect_value(
		t,
		len(composite_index_rowids(t, d, "t", "i_ab", mk_tuple(5, "x"), ct)),
		0,
	)
	five_y := composite_index_rowids(t, d, "t", "i_ab", mk_tuple(5, "y"), ct)
	testing.expect_value(t, len(five_y), 1)

	// No-op rewrite neither dups nor drops.
	must_db_execute(t, d, "UPDATE t SET b = 'y' WHERE a = 5 AND b = 'y';")
	testing.expect_value(
		t,
		len(composite_index_rowids(t, d, "t", "i_ab", mk_tuple(5, "y"), ct)),
		1,
	)

	// Value-to-NULL removes the entry.
	must_db_execute(t, d, "UPDATE t SET a = NULL WHERE b = 'y';")
	testing.expect_value(
		t,
		len(composite_index_rowids(t, d, "t", "i_ab", mk_tuple(5, "y"), ct)),
		0,
	)

	// Multi-row INSERT exercises the bulk-build path.
	must_db_execute(t, d, "INSERT INTO t VALUES (6, 'p'), (7, 'q');")
	testing.expect_value(
		t,
		len(composite_index_rowids(t, d, "t", "i_ab", mk_tuple(6, "p"), ct)),
		1,
	)
	testing.expect_value(
		t,
		len(composite_index_rowids(t, d, "t", "i_ab", mk_tuple(7, "q"), ct)),
		1,
	)

	must_db_execute(t, d, "DELETE FROM t WHERE b = 'n';")
	cnt := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, cnt.ok, "count succeeds")
	if len(cnt.rows) == 1 {
		testing.expect(t, types.value_compare(cnt.rows[0][0], types.value(4)), "four rows left")
	}
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
test_index_real_vacuum :: proc(t: ^testing.T) {
	// Vacuum rebuilds real indexes into fresh packed pages: new root,
	// identical key->rowid sets, every reachable page validates as real
	// pages, and SQL keeps routing.
	d := setup_db(t, "idxrealvac")
	defer teardown_db(d, "idxrealvac")

	testing.expect(t, db.execute(d, "CREATE TABLE t (v REAL, w TEXT);") == .None, "create")
	testing.expect(t, db.execute(d, "CREATE INDEX i_v ON t (v);") == .None, "create index")
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		sql := fmt.tprintf("INSERT INTO t VALUES (%.2f, 'w%d');", f64(i % 50 - 25) / 4.0, i)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
	}
	free_all(context.temp_allocator)

	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "t", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_v")
	testing.expect(t, root_before > 0, "index present before vacuum")

	// Delete every fourth row, then vacuum.
	for i in 4 ..= 400 {
		if (i - 1) % 4 != 3 {
			continue
		}
		free_all(context.temp_allocator)
		sql := fmt.tprintf("DELETE FROM t WHERE w = 'w%d';", i)
		testing.expect(t, db.execute(d, sql) == .None, "delete succeeds")
	}
	free_all(context.temp_allocator)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	st1 := db.Schema_Tree(d)
	tbl1, found1 := schema.find_table(&st1, "t", context.temp_allocator)
	testing.expect(t, found1, "table found after vacuum")
	if !found1 {
		return
	}
	vac_root := index_root_of(t, tbl1, "i_v")
	testing.expect(t, vac_root > 0, "index root present after vacuum")
	testing.expect(t, vac_root != root_before, "vacuum rebuilt the index")

	// Oracle: keys (k-25)/4 for k in 0..50, each missing its deleted
	// fourth-row members.
	idx_tree := btree.init(d.pager, vac_root)
	for k in 0 ..< 50 {
		free_all(context.temp_allocator)
		key := f64(k - 25) / 4.0
		got, g_err := btree.real_find_rowids(&idx_tree, idx_tree.root, key)
		testing.expect(t, g_err == .None, "find succeeds")
		if g_err != .None {
			continue
		}
		want := 0
		for i in 1 ..= 400 {
			if f64(i % 50 - 25) / 4.0 == key && (i - 1) % 4 != 3 {
				want += 1
			}
		}
		testing.expect_value(t, len(got), want)
	}
	free_all(context.temp_allocator)

	// Every reachable index page validates as real pages.
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
		case .LEAF_REAL:
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"vacuumed leaf validates",
			)
		case .REAL_INTERIOR:
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

	// SQL keeps routing through the rebuilt index.
	q := db.query(d, "SELECT v FROM t WHERE v = 0.75 ORDER BY v;")
	testing.expect(t, q.ok, "routed select succeeds")
	for row in q.rows {
		v, is_real := row[0].(f64)
		testing.expect(t, is_real, "covering projects REAL")
		if is_real {
			testing.expect_value(t, v, 0.75)
		}
	}
}

@(test)
test_index_int_vacuum :: proc(t: ^testing.T) {
	// Vacuum rebuilds int indexes into fresh packed pages: new root,
	// identical key->rowid sets, every reachable page validates as int
	// pages, and SQL keeps routing.
	d := setup_db(t, "idxintvac")
	defer teardown_db(d, "idxintvac")

	testing.expect(t, db.execute(d, "CREATE TABLE t (v INT, w TEXT);") == .None, "create")
	testing.expect(t, db.execute(d, "CREATE INDEX i_v ON t (v);") == .None, "create index")
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		sql := fmt.tprintf("INSERT INTO t VALUES (%d, 'w%d');", i % 50 - 25, i)
		testing.expect(t, db.execute(d, sql) == .None, "insert succeeds")
	}
	free_all(context.temp_allocator)

	st0 := db.Schema_Tree(d)
	tbl0, _ := schema.find_table(&st0, "t", context.temp_allocator)
	root_before := index_root_of(t, tbl0, "i_v")
	testing.expect(t, root_before > 0, "index present before vacuum")

	// Delete every fourth row, then vacuum.
	for i in 4 ..= 400 {
		if (i - 1) % 4 != 3 {
			continue
		}
		free_all(context.temp_allocator)
		sql := fmt.tprintf("DELETE FROM t WHERE w = 'w%d';", i)
		testing.expect(t, db.execute(d, sql) == .None, "delete succeeds")
	}
	free_all(context.temp_allocator)
	testing.expect(t, admin.vacuum(d) == .None, "vacuum succeeds")

	st1 := db.Schema_Tree(d)
	tbl1, found1 := schema.find_table(&st1, "t", context.temp_allocator)
	testing.expect(t, found1, "table found after vacuum")
	if !found1 {
		return
	}
	vac_root := index_root_of(t, tbl1, "i_v")
	testing.expect(t, vac_root > 0, "index root present after vacuum")
	testing.expect(t, vac_root != root_before, "vacuum rebuilt the index")

	// Oracle: keys -25..24, each missing its deleted fourth-row members.
	idx_tree := btree.init(d.pager, vac_root)
	for k in -25 ..< 25 {
		free_all(context.temp_allocator)
		got, g_err := btree.int_find_rowids(&idx_tree, idx_tree.root, i64(k))
		testing.expect(t, g_err == .None, "find succeeds")
		if g_err != .None {
			continue
		}
		want := 0
		for i in 1 ..= 400 {
			if i64(i % 50 - 25) == i64(k) && (i - 1) % 4 != 3 {
				want += 1
			}
		}
		testing.expect_value(t, len(got), want)
	}
	free_all(context.temp_allocator)

	// Every reachable index page validates as int pages.
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
		case .LEAF_INT:
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"vacuumed leaf validates",
			)
		case .INT_INTERIOR:
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

	// SQL keeps routing through the rebuilt index.
	q := db.query(d, "SELECT v FROM t WHERE v = 3 ORDER BY v;")
	testing.expect(t, q.ok, "routed select succeeds")
	for row in q.rows {
		v, is_int := row[0].(i64)
		testing.expect(t, is_int, "covering projects INTEGER")
		if is_int {
			testing.expect_value(t, v, 3)
		}
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

// int_index_rowids reads one int key's rowids through the named index root,
// re-resolving the table each call (roots change per write).
int_index_rowids :: proc(
	t: ^testing.T,
	d: ^db.Database,
	table: string,
	index: string,
	val: i64,
) -> []types.Row_ID {
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, table, context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return nil
	}
	root := index_root_of(t, tbl, index)
	idx_tree := btree.init(d.pager, root)
	rows, f_err := btree.int_find_rowids(&idx_tree, idx_tree.root, val)
	testing.expect(t, f_err == .None, "find succeeds")
	return rows
}

// real_index_rowids reads one real key's rowids through the named index root,
// re-resolving the table each call (roots change per write).
real_index_rowids :: proc(
	t: ^testing.T,
	d: ^db.Database,
	table: string,
	index: string,
	val: f64,
) -> []types.Row_ID {
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, table, context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return nil
	}
	root := index_root_of(t, tbl, index)
	idx_tree := btree.init(d.pager, root)
	rows, f_err := btree.real_find_rowids(&idx_tree, idx_tree.root, val)
	testing.expect(t, f_err == .None, "find succeeds")
	return rows
}

// composite_index_rowids reads one framed tuple's rowids through the named
// index root, re-resolving the table each call (roots change per write).
composite_index_rowids :: proc(
	t: ^testing.T,
	d: ^db.Database,
	table: string,
	index: string,
	vals: []types.Value,
	col_types: []types.Column_Type,
) -> []types.Row_ID {
	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, table, context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return nil
	}
	root := index_root_of(t, tbl, index)
	raw, rok := comp_frame(t, vals, col_types)
	testing.expect(t, rok, "tuple frames")
	if !rok {
		return nil
	}
	idx_tree := btree.init(d.pager, root)
	rows, f_err := btree.composite_find_rowids(&idx_tree, idx_tree.root, raw)
	testing.expect(t, f_err == .None, "find succeeds")
	return rows
}

// comp_frame frames one tuple into temp bytes for composite test lookups.
comp_frame :: proc(
	t: ^testing.T,
	vals: []types.Value,
	col_types: []types.Column_Type,
) -> (
	[]u8,
	bool,
) {
	n := cell.composite_encoded_len(vals, col_types)
	if n == 0 {
		return nil, false
	}
	buf := make([]u8, n, context.temp_allocator)
	wn, wok := cell.composite_encode_raw(vals, col_types, buf)
	if !wok {
		return nil, false
	}
	return buf[:wn], true
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
test_index_real_create_backfill :: proc(t: ^testing.T) {
	// CREATE INDEX on REAL backfills every value (negatives, fractions,
	// dups, signed zeros sharing one key; NULLs skipped) with the def
	// tagged REAL; reads go through btree.real_find_rowids on the published
	// root (later tests pin the planner routing on top).
	d := setup_db(t, "idxrealback")
	defer teardown_db(d, "idxrealback")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (r REAL, b TEXT);",
			"INSERT INTO t VALUES (1.5, 'x');",
			"INSERT INTO t VALUES (-2.5, 'y');",
			"INSERT INTO t VALUES (1.5, 'z');",
			"INSERT INTO t VALUES (-0.0, 'neg');",
			"INSERT INTO t VALUES (0.0, 'pos');",
			"INSERT INTO t VALUES (NULL, 'n');",
		},
	)
	testing.expect(t, db.execute(d, "CREATE INDEX i_r ON t (r);") == .None, "create real index")

	st := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st, "t", context.temp_allocator)
	testing.expect(t, found, "table found")
	if !found {
		return
	}
	def, dok := schema.table_index(tbl, "i_r")
	testing.expect(t, dok, "def present")
	if dok {
		testing.expect_value(t, def.col_type, types.Column_Type.REAL)
		testing.expect(t, def.root != 0, "root published")
	}

	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 1.5)), 2)
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", -2.5)), 1)
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 0.0)), 2)
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", -0.0)), 2)
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 999.25)), 0)

	// Routed SQL answers through the new index.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT r, b FROM t WHERE r = 1.5;"),
		"INDEX SCAN ON t USING r (eq, fetch)",
	)
	q := db.query(d, "SELECT r, b FROM t WHERE r = 1.5 ORDER BY b;")
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
test_index_int_routing :: proc(t: ^testing.T) {
	// INT index routing: Eq and IN answer covering (values, types, rowid
	// order); cross-class literals miss; LIKE stays on the scan path with
	// identical (empty) results; same-column OR covers, cross-column OR
	// fetches with correct values.
	d := setup_db(t, "idxintroute")
	defer teardown_db(d, "idxintroute")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"INSERT INTO t VALUES (10, 'x');",
			"INSERT INTO t VALUES (-5, 'y');",
			"INSERT INTO t VALUES (10, 'z');",
			"CREATE INDEX i_a ON t (a);",
			"CREATE INDEX i_b ON t (b);",
		},
	)
	eq := db.query(d, "SELECT a FROM t WHERE a = 10 ORDER BY rowid;")
	testing.expect(t, eq.ok, "int eq succeeds")
	testing.expect_value(t, len(eq.rows), 2)
	for row in eq.rows {
		v, is_int := row[0].(i64)
		testing.expect(t, is_int, "covering projects INTEGER")
		if is_int {
			testing.expect_value(t, v, 10)
		}
	}
	if len(eq.rows) == 2 {
		testing.expect(t, eq.rows[0][0].(i64) == 10, "rowid order stable")
	}

	in_q := db.query(d, "SELECT a FROM t WHERE a IN (10, -5) ORDER BY rowid;")
	testing.expect(t, in_q.ok, "int in succeeds")
	testing.expect_value(t, len(in_q.rows), 3)

	miss := db.query(d, "SELECT a FROM t WHERE a = 'x';")
	testing.expect(t, miss.ok, "cross-class succeeds")
	testing.expect_value(t, len(miss.rows), 0)

	like_idx := db.query(d, "SELECT a FROM t WHERE a LIKE '1%';")
	testing.expect(t, like_idx.ok, "int like succeeds")
	testing.expect_value(t, len(like_idx.rows), 0)

	or_same := db.query(d, "SELECT a FROM t WHERE a = 10 OR a = -5 ORDER BY a;")
	testing.expect(t, or_same.ok, "same-column or succeeds")
	testing.expect_value(t, len(or_same.rows), 3)

	or_cross := db.query(d, "SELECT b FROM t WHERE a = 10 OR b = 'y' ORDER BY b;")
	testing.expect(t, or_cross.ok, "cross-column or succeeds")
	testing.expect_value(t, len(or_cross.rows), 3)
	for row in or_cross.rows {
		_, is_text := row[0].(string)
		testing.expect(t, is_text, "cross or projects stored values")
	}
	if len(or_cross.rows) == 3 {
		// 'x' and 'z' via the int arm carry their stored texts, not keys.
		got_y := false
		for row in or_cross.rows {
			if s, is_text := row[0].(string); is_text && s == "y" {
				got_y = true
			}
		}
		testing.expect(t, got_y, "text-arm value present")
	}

	// EXPLAIN agrees with execution on the new arms.
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a = 10;"),
		"INDEX SCAN ON t USING a (eq, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT b FROM t WHERE a = 10 OR b = 'y';"),
		"INDEX SCAN ON t USING a, b (or, fetch)",
	)
}

@(test)
test_index_or_cross_column_values :: proc(t: ^testing.T) {
	// Cross-column OR with a matching projection must fetch the stored
	// values, never emit other columns' keys (regression: covering used
	// to project the matched keys).
	d := setup_db(t, "idxorcross")
	defer teardown_db(d, "idxorcross")

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
	q := db.query(d, "SELECT a FROM t WHERE a = 'm' OR b = 'y' ORDER BY a;")
	testing.expect(t, q.ok, "cross or succeeds")
	testing.expect_value(t, len(q.rows), 2)
	if len(q.rows) == 2 {
		m, mok := q.rows[0][0].(string)
		testing.expect(t, mok && m == "m", "first row stored value")
		n, nok := q.rows[1][0].(string)
		testing.expect(t, nok && n == "n", "second row stored value, not the key")
	}
}

@(test)
test_index_range_int :: proc(t: ^testing.T) {
	// Range routing on INTEGER indexes: all four operators answer fetch
	// with exact bounds (NULLs never match); cross-class, negated, and !=
	// stay on the scan path with identical results.
	d := setup_db(t, "idxrangeint")
	defer teardown_db(d, "idxrangeint")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"INSERT INTO t VALUES (10, 'x');",
			"INSERT INTO t VALUES (-5, 'y');",
			"INSERT INTO t VALUES (3, 'm');",
			"INSERT INTO t VALUES (NULL, 'n');",
			"CREATE INDEX i_a ON t (a);",
		},
	)
	cases := []struct {
		sql : string,
		want: int,
	} {
		{"SELECT a, b FROM t WHERE a > 3;", 1},
		{"SELECT a, b FROM t WHERE a >= 3;", 2},
		{"SELECT a, b FROM t WHERE a < 3;", 1},
		{"SELECT a, b FROM t WHERE a <= -5;", 1},
		{"SELECT a, b FROM t WHERE a > 100;", 0},
		{"SELECT a, b FROM t WHERE a < -100;", 0},
		{"SELECT a, b FROM t WHERE a > 'x';", 0},
		{"SELECT a, b FROM t WHERE NOT a > 3;", 3},
		{"SELECT a, b FROM t WHERE a != 10;", 2},
	}
	for c in cases {
		free_all(context.temp_allocator)
		q := db.query(d, c.sql)
		testing.expect(t, q.ok, "range query succeeds")
		if q.ok {
			testing.expectf(t, len(q.rows) == c.want, "row count for %s", c.sql)
		}
	}
	free_all(context.temp_allocator)

	gt := db.query(d, "SELECT a FROM t WHERE a > 3 ORDER BY a;")
	testing.expect(t, gt.ok, "ordered range succeeds")
	testing.expect_value(t, len(gt.rows), 1)
	if len(gt.rows) == 1 {
		a, aok := gt.rows[0][0].(i64)
		testing.expect(t, aok && a == 10, "only matching value")
	}

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b FROM t WHERE a > 3;"),
		"INDEX SCAN ON t USING a (range, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a, b FROM t WHERE a <= -5;"),
		"INDEX SCAN ON t USING a (range, fetch)",
	)
}

@(test)
test_index_range_text :: proc(t: ^testing.T) {
	// Range routing on TEXT indexes: BINARY bounds, empty string included,
	// cross-class literals miss.
	d := setup_db(t, "idxrangetext")
	defer teardown_db(d, "idxrangetext")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (b TEXT, v INT);",
			"INSERT INTO t VALUES ('x', 1);",
			"INSERT INTO t VALUES ('m', 2);",
			"INSERT INTO t VALUES ('', 3);",
			"INSERT INTO t VALUES ('y', 4);",
			"INSERT INTO t VALUES (NULL, 5);",
			"CREATE INDEX i_b ON t (b);",
		},
	)
	cases := []struct {
		sql : string,
		want: int,
	} {
		{"SELECT b FROM t WHERE b > 'm';", 2},
		{"SELECT b FROM t WHERE b >= 'm';", 3},
		{"SELECT b FROM t WHERE b < 'm';", 1},
		{"SELECT b FROM t WHERE b <= 'm';", 2},
		{"SELECT b FROM t WHERE b > '';", 3},
		{"SELECT b FROM t WHERE b >= '';", 4},
		// Cross-class ordered comparisons follow storage-class rank on the
		// scan path (TEXT ranks above INTEGER, so every text matches b > 3).
		{"SELECT b FROM t WHERE b > 3;", 4},
		{"SELECT b FROM t WHERE b < 'a';", 1},
	}
	for c in cases {
		free_all(context.temp_allocator)
		q := db.query(d, c.sql)
		testing.expect(t, q.ok, "range query succeeds")
		if q.ok {
			testing.expectf(t, len(q.rows) == c.want, "row count for %s", c.sql)
		}
	}
	free_all(context.temp_allocator)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT b FROM t WHERE b > 'm';"),
		"INDEX SCAN ON t USING b (range, fetch)",
	)
}

@(test)
test_index_range_between_merge :: proc(t: ^testing.T) {
	// BETWEEN desugars to an AND of two Ranges; the merge folds it to one
	// bounded scan (EXPLAIN names range, not and). Contradictions empty
	// without I/O; Eq dominates its Ranges; In filters by them.
	d := setup_db(t, "idxrangebetween")
	defer teardown_db(d, "idxrangebetween")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"INSERT INTO t VALUES (10, 'x');",
			"INSERT INTO t VALUES (-5, 'y');",
			"INSERT INTO t VALUES (3, 'm');",
			"INSERT INTO t VALUES (7, 'q');",
			"CREATE INDEX i_a ON t (a);",
			"CREATE INDEX i_b ON t (b);",
		},
	)
	cases := []struct {
		sql : string,
		want: int,
	} {
		{"SELECT a FROM t WHERE a BETWEEN 3 AND 10;", 3},
		{"SELECT a FROM t WHERE a BETWEEN 10 AND 3;", 0},
		{"SELECT a FROM t WHERE a NOT BETWEEN 3 AND 10;", 1},
		{"SELECT a FROM t WHERE a > 10 AND a < 5;", 0},
		{"SELECT a FROM t WHERE a >= 3 AND a <= 3;", 1},
		{"SELECT a FROM t WHERE a > 3 AND a < 3;", 0},
		{"SELECT a FROM t WHERE a = 10 AND a > 3;", 1},
		{"SELECT a FROM t WHERE a = 10 AND a > 30;", 0},
		{"SELECT a FROM t WHERE a IN (3, 7, 10) AND a >= 7;", 2},
		{"SELECT a FROM t WHERE a IN (3, 7) AND a > 100;", 0},
		{"SELECT a FROM t WHERE a = 3 AND a = 7;", 0},
		{"SELECT b FROM t WHERE b BETWEEN 'm' AND 'y';", 4},
	}
	for c in cases {
		free_all(context.temp_allocator)
		q := db.query(d, c.sql)
		testing.expect(t, q.ok, "between query succeeds")
		if q.ok {
			testing.expectf(t, len(q.rows) == c.want, "row count for %s", c.sql)
		}
	}
	free_all(context.temp_allocator)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a BETWEEN 3 AND 10;"),
		"INDEX SCAN ON t USING a (range, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a > 10 AND a < 5;"),
		"INDEX SCAN ON t USING a (range, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT a FROM t WHERE a = 10 AND a > 3;"),
		"INDEX SCAN ON t USING a (eq, fetch)",
	)
}

@(test)
test_index_range_twin :: proc(t: ^testing.T) {
	// Differential twin: indexed t vs unindexed t2, same 400 rows. Every
	// range shape — bounds, exclusivity, merges, unions, limits — returns
	// identical output, proving the scan honors bounds exactly (including
	// exclusive bounds straddling leaf boundaries).
	d := setup_db(t, "idxrangetwin")
	defer teardown_db(d, "idxrangetwin")

	testing.expect(t, db.execute(d, "CREATE TABLE t (a INT, b TEXT, v INT);") == .None, "create t")
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t2 (a INT, b TEXT, v INT);") == .None,
		"create t2",
	)
	testing.expect(t, db.execute(d, "CREATE INDEX i_a ON t (a);") == .None, "index a")
	testing.expect(t, db.execute(d, "CREATE INDEX i_b ON t (b);") == .None, "index b")
	bodies := []string{"alpha", "bb", "alpha", "", "gamma", "m"}
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		val := i % 50 - 25
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, '%s', %d);", val, body, i)) ==
			.None,
			"insert t",
		)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t2 VALUES (%d, '%s', %d);", val, body, i)) ==
			.None,
			"insert t2",
		)
	}
	free_all(context.temp_allocator)

	queries := []string {
		"SELECT a, b, v FROM t WHERE a > 0;",
		"SELECT a, b, v FROM t WHERE a >= 0;",
		"SELECT a, b, v FROM t WHERE a < 0;",
		"SELECT a, b, v FROM t WHERE a <= -25;",
		"SELECT a, b, v FROM t WHERE a > 24;",
		"SELECT a, b, v FROM t WHERE a BETWEEN -5 AND 5;",
		"SELECT a, b, v FROM t WHERE a > -5 AND a < 5;",
		"SELECT a, b, v FROM t WHERE a >= 5 AND a <= 5;",
		"SELECT a, b, v FROM t WHERE a > 5 AND a < -5;",
		"SELECT a, b, v FROM t WHERE a NOT BETWEEN -5 AND 5;",
		"SELECT a, b, v FROM t WHERE a = 7 AND a > 0;",
		"SELECT a, b, v FROM t WHERE a IN (-5, 0, 7) AND a >= 0;",
		"SELECT a, b, v FROM t WHERE a > 0 OR a < -20;",
		"SELECT a, b, v FROM t WHERE b > 'bb';",
		"SELECT a, b, v FROM t WHERE b <= 'bb';",
		"SELECT a, b, v FROM t WHERE b BETWEEN 'alpha' AND 'm';",
		"SELECT a, b, v FROM t WHERE b > '';",
		"SELECT a, b, v FROM t WHERE b = '';",
		"SELECT a, b, v FROM t WHERE b <= '';",
		"SELECT a, b, v FROM t WHERE b >= '';",
		"SELECT a, b, v FROM t WHERE a > 0 AND v > 100;",
		"SELECT a, b, v FROM t WHERE a > 0 LIMIT 10;",
		"SELECT a, b, v FROM t WHERE a BETWEEN -5 AND 5 ORDER BY v DESC LIMIT 7;",
		"SELECT rowid FROM t WHERE a > 20 ORDER BY rowid;",
	}
	for q in queries {
		free_all(context.temp_allocator)
		twin, _ := strings.replace(q, "FROM t ", "FROM t2 ", 1, context.temp_allocator)
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
test_index_int_dml_fanout :: proc(t: ^testing.T) {
	// Per-row writes maintain int indexes: single INSERTs land, UPDATEs
	// move entries (no-op rewrites and NULL transitions included), DELETEs
	// remove them. Verified at btree level — the planner does not route
	// int indexes yet.
	d := setup_db(t, "idxintfan")
	defer teardown_db(d, "idxintfan")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (a INT, b TEXT);",
			"CREATE INDEX i_a ON t (a);",
			"INSERT INTO t VALUES (5, 'x');",
			"INSERT INTO t VALUES (NULL, 'n');",
		},
	)
	five := int_index_rowids(t, d, "t", "i_a", 5)
	testing.expect_value(t, len(five), 1)
	if len(five) == 1 {
		testing.expect_value(t, five[0], 1)
	}

	must_db_execute(t, d, "UPDATE t SET a = 6 WHERE b = 'x';")
	testing.expect_value(t, len(int_index_rowids(t, d, "t", "i_a", 5)), 0)
	six := int_index_rowids(t, d, "t", "i_a", 6)
	testing.expect_value(t, len(six), 1)

	// No-op rewrite neither dups nor drops.
	must_db_execute(t, d, "UPDATE t SET a = 6 WHERE b = 'x';")
	testing.expect_value(t, len(int_index_rowids(t, d, "t", "i_a", 6)), 1)

	// Value-to-NULL removes the entry.
	must_db_execute(t, d, "UPDATE t SET a = NULL WHERE b = 'x';")
	testing.expect_value(t, len(int_index_rowids(t, d, "t", "i_a", 6)), 0)

	must_db_execute(t, d, "DELETE FROM t WHERE b = 'n';")
	testing.expect_value(t, len(int_index_rowids(t, d, "t", "i_a", 6)), 0)
	cnt := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, cnt.ok, "count succeeds")
	if len(cnt.rows) == 1 {
		testing.expect(t, types.value_compare(cnt.rows[0][0], types.value(1)), "one row left")
	}
}

@(test)
test_index_real_dml_fanout :: proc(t: ^testing.T) {
	// Per-row writes maintain real indexes: single INSERTs land, UPDATEs
	// move entries (no-op rewrites, signed-zero shares, and NULL
	// transitions included), DELETEs remove them. Verified at btree level.
	d := setup_db(t, "idxrealfan")
	defer teardown_db(d, "idxrealfan")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (r REAL, b TEXT);",
			"CREATE INDEX i_r ON t (r);",
			"INSERT INTO t VALUES (0.5, 'x');",
			"INSERT INTO t VALUES (NULL, 'n');",
		},
	)
	half := real_index_rowids(t, d, "t", "i_r", 0.5)
	testing.expect_value(t, len(half), 1)
	if len(half) == 1 {
		testing.expect_value(t, half[0], 1)
	}

	must_db_execute(t, d, "UPDATE t SET r = 1.5 WHERE b = 'x';")
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 0.5)), 0)
	one_half := real_index_rowids(t, d, "t", "i_r", 1.5)
	testing.expect_value(t, len(one_half), 1)

	// No-op rewrite neither dups nor drops.
	must_db_execute(t, d, "UPDATE t SET r = 1.5 WHERE b = 'x';")
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 1.5)), 1)

	// -0.0 rewrite shares the +0.0 key (normalization, not a move).
	must_db_execute(t, d, "UPDATE t SET r = -0.0 WHERE b = 'x';")
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 1.5)), 0)
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 0.0)), 1)

	// Value-to-NULL removes the entry.
	must_db_execute(t, d, "UPDATE t SET r = NULL WHERE b = 'x';")
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 0.0)), 0)

	must_db_execute(t, d, "DELETE FROM t WHERE b = 'n';")
	testing.expect_value(t, len(real_index_rowids(t, d, "t", "i_r", 0.0)), 0)
	cnt := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, cnt.ok, "count succeeds")
	if len(cnt.rows) == 1 {
		testing.expect(t, types.value_compare(cnt.rows[0][0], types.value(1)), "one row left")
	}
}

@(test)
test_index_real_routing :: proc(t: ^testing.T) {
	// REAL index routing: Eq and IN answer covering (values typed f64);
	// Range answers fetch; cross-class literals miss; LIKE stays on the
	// scan path with identical (empty) results; same-column OR covers,
	// BETWEEN merges to one bounded scan.
	d := setup_db(t, "idxrealroute")
	defer teardown_db(d, "idxrealroute")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (r REAL, b TEXT);",
			"INSERT INTO t VALUES (1.5, 'x');",
			"INSERT INTO t VALUES (-2.5, 'y');",
			"INSERT INTO t VALUES (1.5, 'z');",
			"INSERT INTO t VALUES (-0.0, 'neg');",
			"INSERT INTO t VALUES (0.0, 'pos');",
			"INSERT INTO t VALUES (NULL, 'n');",
			"CREATE INDEX i_r ON t (r);",
			"CREATE INDEX i_b ON t (b);",
		},
	)
	eq := db.query(d, "SELECT r FROM t WHERE r = 1.5 ORDER BY r;")
	testing.expect(t, eq.ok, "real eq succeeds")
	testing.expect_value(t, len(eq.rows), 2)
	for row in eq.rows {
		v, is_real := row[0].(f64)
		testing.expect(t, is_real, "covering projects REAL")
		if is_real {
			testing.expect_value(t, v, 1.5)
		}
	}

	// Signed zeros cover as the canonical key, value-equal either way.
	zero := db.query(d, "SELECT r FROM t WHERE r = -0.0 ORDER BY r;")
	testing.expect(t, zero.ok, "signed-zero eq succeeds")
	testing.expect_value(t, len(zero.rows), 2)
	for row in zero.rows {
		v, is_real := row[0].(f64)
		testing.expect(t, is_real, "covering projects REAL")
		if is_real {
			testing.expect_value(t, v, 0.0)
		}
	}

	in_q := db.query(d, "SELECT r, b FROM t WHERE r IN (1.5, -2.5) ORDER BY r, b;")
	testing.expect(t, in_q.ok, "real in succeeds")
	testing.expect_value(t, len(in_q.rows), 3)

	rng := db.query(d, "SELECT r, b FROM t WHERE r > 0.0 ORDER BY r, b;")
	testing.expect(t, rng.ok, "real range succeeds")
	testing.expect_value(t, len(rng.rows), 2)

	btw := db.query(d, "SELECT r, b FROM t WHERE r BETWEEN -3.0 AND 1.0 ORDER BY r, b;")
	testing.expect(t, btw.ok, "real between succeeds")
	testing.expect_value(t, len(btw.rows), 3)

	miss := db.query(d, "SELECT r, b FROM t WHERE r = 'x';")
	testing.expect(t, miss.ok, "cross-class succeeds")
	testing.expect_value(t, len(miss.rows), 0)

	int_miss := db.query(d, "SELECT r, b FROM t WHERE r = 1;")
	testing.expect(t, int_miss.ok, "int literal succeeds")
	testing.expect_value(t, len(int_miss.rows), 0)

	like_q := db.query(d, "SELECT r, b FROM t WHERE r LIKE '1%';")
	testing.expect(t, like_q.ok, "real like succeeds")
	testing.expect_value(t, len(like_q.rows), 0)

	or_same := db.query(d, "SELECT r FROM t WHERE r = 1.5 OR r = -2.5 ORDER BY r;")
	testing.expect(t, or_same.ok, "same-column or succeeds")
	testing.expect_value(t, len(or_same.rows), 3)

	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT r FROM t WHERE r = 1.5;"),
		"INDEX SCAN ON t USING r (eq, covering)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT r, b FROM t WHERE r BETWEEN -3.0 AND 1.0;"),
		"INDEX SCAN ON t USING r (range, fetch)",
	)
	testing.expect_value(
		t,
		explain(t, d, "EXPLAIN SELECT r, b FROM t WHERE r LIKE '1%';"),
		"FULL SCAN ON t",
	)
}

@(test)
test_index_real_mixed_storage :: proc(t: ^testing.T) {
	// Integers stored in a REAL column (accepted widening) index under
	// their widened key: Eq/Range find them exactly. Fetch preserves the
	// stored classes; covering normalizes to the declared REAL type.
	d := setup_db(t, "idxrealmix")
	defer teardown_db(d, "idxrealmix")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (r REAL, b TEXT);",
			"INSERT INTO t VALUES (2, 'a');",
			"INSERT INTO t VALUES (2.0, 'b');",
			"INSERT INTO t VALUES (2.5, 'c');",
			"CREATE INDEX i_r ON t (r);",
		},
	)
	fetch := db.query(d, "SELECT r, b FROM t WHERE r = 2.0 ORDER BY b;")
	testing.expect(t, fetch.ok, "fetch succeeds")
	testing.expect_value(t, len(fetch.rows), 2)
	if len(fetch.rows) == 2 {
		_, a_int := fetch.rows[0][0].(i64)
		testing.expect(t, a_int, "fetch preserves int storage class")
		_, b_real := fetch.rows[1][0].(f64)
		testing.expect(t, b_real, "fetch preserves real storage class")
	}

	rng := db.query(d, "SELECT r, b FROM t WHERE r >= 2.0 AND r < 2.5 ORDER BY b;")
	testing.expect(t, rng.ok, "range succeeds")
	testing.expect_value(t, len(rng.rows), 2)

	cov := db.query(d, "SELECT r FROM t WHERE r = 2.0 ORDER BY r;")
	testing.expect(t, cov.ok, "covering succeeds")
	testing.expect_value(t, len(cov.rows), 2)
	for row in cov.rows {
		v, is_real := row[0].(f64)
		testing.expect(t, is_real, "covering normalizes to REAL")
		if is_real {
			testing.expect_value(t, v, 2.0)
		}
	}
}

@(test)
test_index_real_twin :: proc(t: ^testing.T) {
	// Differential twin: indexed t vs unindexed t2, same 400 rows
	// (fractions, negatives, signed zeros, NULLs). Every real shape —
	// bounds, exclusivity, merges, unions, limits — returns identical
	// output, proving the transform order matches the filter order.
	d := setup_db(t, "idxrealtwin")
	defer teardown_db(d, "idxrealtwin")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t (r REAL, b TEXT, v INT);") == .None,
		"create t",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE TABLE t2 (r REAL, b TEXT, v INT);") == .None,
		"create t2",
	)
	testing.expect(t, db.execute(d, "CREATE INDEX i_r ON t (r);") == .None, "index r")
	bodies := []string{"alpha", "bb", "gamma"}
	for i in 1 ..= 400 {
		free_all(context.temp_allocator)
		body := bodies[(i - 1) % len(bodies)]
		// Dotted literals only: whole numbers must parse as REAL (a bare
		// "1" would store i64, mixing storage classes under test).
		val := fmt.tprintf("%.2f", f64(i % 50 - 25) / 4.0)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%s, '%s', %d);", val, body, i)) ==
			.None,
			"insert t",
		)
		testing.expect(
			t,
			db.execute(d, fmt.tprintf("INSERT INTO t2 VALUES (%s, '%s', %d);", val, body, i)) ==
			.None,
			"insert t2",
		)
	}
	free_all(context.temp_allocator)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO t VALUES (-0.0, 'neg', 401);") == .None,
		"neg zero",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO t2 VALUES (-0.0, 'neg', 401);") == .None,
		"neg zero twin",
	)
	testing.expect(t, db.execute(d, "INSERT INTO t VALUES (NULL, 'n', 402);") == .None, "null")
	testing.expect(
		t,
		db.execute(d, "INSERT INTO t2 VALUES (NULL, 'n', 402);") == .None,
		"null twin",
	)

	queries := []string {
		"SELECT r, b, v FROM t WHERE r = 1.5;",
		"SELECT r FROM t WHERE r = 0.0;",
		"SELECT r FROM t WHERE r = -0.0;",
		"SELECT r, b, v FROM t WHERE r > 0.0;",
		"SELECT r, b, v FROM t WHERE r >= 0.0;",
		"SELECT r, b, v FROM t WHERE r < -10.0;",
		"SELECT r, b, v FROM t WHERE r <= -6.25;",
		"SELECT r, b, v FROM t WHERE r BETWEEN -5.0 AND 5.0;",
		"SELECT r, b, v FROM t WHERE r > -5.0 AND r < 5.0;",
		"SELECT r, b, v FROM t WHERE r NOT BETWEEN -1.0 AND 1.0;",
		"SELECT r, b, v FROM t WHERE r = 2.5 AND r > 0.0;",
		"SELECT r, b, v FROM t WHERE r IN (1.5, -2.5, 99.0) AND r >= 0.0;",
		"SELECT r, b, v FROM t WHERE r > 0.0 OR r < -10.0;",
		"SELECT r, b, v FROM t WHERE r > 0.0 AND v > 100;",
		"SELECT r, b, v FROM t WHERE r > 0.0 LIMIT 10;",
		"SELECT r, b, v FROM t WHERE r BETWEEN -5.0 AND 5.0 ORDER BY v DESC LIMIT 7;",
		"SELECT rowid FROM t WHERE r > 5.0 ORDER BY rowid;",
		"SELECT r, b, v FROM t WHERE r = 1;",
		"SELECT r, b, v FROM t WHERE r LIKE '1%';",
	}
	for q in queries {
		free_all(context.temp_allocator)
		twin, _ := strings.replace(q, "FROM t ", "FROM t2 ", 1, context.temp_allocator)
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

// explain renders one EXPLAIN statement to its plan string through the
// executor directly (shared with test_index_explain).
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
