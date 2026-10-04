package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:db"

// Routing twin setup: indexed docs vs unindexed docs2, identical rows.
routing_twin_setup :: proc(t: ^testing.T, name: string) -> ^db.Database {
	d := setup_db(t, name)
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
	return d
}

// routing_twin runs each query against indexed docs and unindexed docs2
// (table name swapped) and demands identical output.
routing_twin :: proc(t: ^testing.T, d: ^db.Database, queries: []string) {
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
test_index_or_union :: proc(t: ^testing.T) {
	// Flat OR of usable same-column disjuncts unions candidate sets
	// (covering when the projection is rowid-only); any unusable
	// disjunct falls back to the full scan with identical results.
	context.logger.lowest_level = .Error
	d := routing_twin_setup(t, "idxor")
	defer teardown_db(d, "idxor")

	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'a' OR body = 'b';"),
		"INDEX SCAN ON docs USING body (or, fetch)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha' OR body IN ('bb', 'gamma');"),
		"INDEX SCAN ON docs USING body (or, covering)",
	)
	// Covering OR is rowid-exact (union of exact sets) but has no
	// unindexed twin (SELECT rowid is index-only): assert count +
	// ascending order directly. alphax24 + bbx12 + ""x12 = 48.
	ord := db.query(d, "SELECT rowid FROM docs WHERE body = 'alpha' OR body = 'bb' OR body = '';")
	testing.expect(t, ord.ok, "covering OR succeeds")
	testing.expect_value(t, len(ord.rows), 48)
	for i in 1 ..< len(ord.rows) {
		prev, pok := ord.rows[i - 1][0].(i64)
		cur, cok := ord.rows[i][0].(i64)
		if !pok || !cok || prev >= cur {
			testing.expect(t, false, "covering OR ascends")
			break
		}
	}
	routing_twin(
		t,
		d,
		[]string {
			"SELECT id, body, v FROM docs WHERE body = 'alpha' OR body = 'bb';",
			"SELECT id FROM docs WHERE body = 'alpha' OR body LIKE 'b%';",
			"SELECT id FROM docs WHERE body IN ('alpha', 'x') OR body = 'gamma';",
			"SELECT id FROM docs WHERE body = 'alpha' OR body = 'bb' ORDER BY id DESC LIMIT 5;",
			"SELECT id FROM docs WHERE body = 'missing' OR body = 'absent';",
			// Unusable disjuncts: identical rows via full scan.
			"SELECT id FROM docs WHERE body = 'alpha' OR v = 1;",
			"SELECT id FROM docs WHERE body = 'alpha' OR NOT body = 'bb';",
			"SELECT id FROM docs WHERE (body = 'alpha' OR body = 'bb') AND v > 1;",
			"SELECT id FROM docs WHERE body = 'alpha' OR body LIKE '%pha';",
		},
	)
}

@(test)
test_index_and_intersect :: proc(t: ^testing.T) {
	// Flat AND with 2+ usable conjuncts intersects (narrower candidates);
	// one usable conjunct behaves exactly as before.
	context.logger.lowest_level = .Error
	d := routing_twin_setup(t, "idxand")
	defer teardown_db(d, "idxand")

	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body LIKE 'a%' AND body IN ('alpha', 'bb');"),
		"INDEX SCAN ON docs USING body (and, fetch)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'alpha' AND v > 1;"),
		"INDEX SCAN ON docs USING body (eq, fetch)",
	)
	routing_twin(
		t,
		d,
		[]string {
			"SELECT id, body, v FROM docs WHERE body LIKE 'a%' AND body IN ('alpha', 'bb');",
			"SELECT id FROM docs WHERE body = 'alpha' AND body LIKE 'al%';",
			"SELECT id FROM docs WHERE body IN ('alpha', 'bb') AND body LIKE '%a%';",
			"SELECT id FROM docs WHERE body = 'alpha' AND body = 'alpha';",
			"SELECT id FROM docs WHERE body = 'alpha' AND body = 'bb';",
			"SELECT id FROM docs WHERE body LIKE 'a%' AND v > 1 AND body IN ('alpha', 'gamma');",
		},
	)
}

@(test)
test_index_in_cap :: proc(t: ^testing.T) {
	// Literal IN past MAX_INDEX_IN_MEMBERS falls back to the full scan
	// (identical rows); at the cap it still routes.
	context.logger.lowest_level = .Error
	d := routing_twin_setup(t, "idxincap")
	defer teardown_db(d, "idxincap")

	build_in :: proc(n: int) -> string {
		b: strings.Builder
		strings.builder_init(&b, context.temp_allocator)
		strings.write_string(&b, "SELECT id FROM docs WHERE body IN (")
		for i in 1 ..= n {
			if i > 1 {
				strings.write_string(&b, ", ")
			}
			fmt.sbprintf(&b, "'m%d'", i)
		}
		strings.write_string(&b, ") ORDER BY id;")
		// Heap-owned: routing_twin frees temp per iteration.
		return strings.clone(strings.to_string(b), context.allocator)
	}

	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body IN ('alpha', 'bb');"),
		"INDEX SCAN ON docs USING body (in, fetch)",
	)
	over := build_in(129)
	defer delete(over, context.allocator)
	q128 := build_in(128)
	defer delete(q128, context.allocator)
	// NOTE: no delete — fmt.tprintf's backing must not be heap-freed
	// (tracker flags it); temp reclaims at scope end.
	over_explain := fmt.tprintf("EXPLAIN %s", over)
	testing.expect_value(
		t,
		drop_index_plan(t, d, over_explain),
		"FULL SCAN ON docs",
	)
	routing_twin(t, d, []string{over, q128})
}

@(test)
test_index_covering_col :: proc(t: ^testing.T) {
	// `SELECT <indexed-col>` with Eq/In (or Or thereof) answers from the
	// index alone; Prefix stays on fetch (keys unknown per row).
	context.logger.lowest_level = .Error
	d := routing_twin_setup(t, "idxcovcol")
	defer teardown_db(d, "idxcovcol")

	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT body FROM docs WHERE body = 'alpha';"),
		"INDEX SCAN ON docs USING body (eq, covering)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT body FROM docs WHERE body = 'a' OR body = 'bb';"),
		"INDEX SCAN ON docs USING body (or, covering)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT body FROM docs WHERE body LIKE 'a%';"),
		"INDEX SCAN ON docs USING body (prefix, fetch)",
	)
	routing_twin(
		t,
		d,
		[]string {
			"SELECT body FROM docs WHERE body = 'alpha';",
			"SELECT body FROM docs WHERE body IN ('alpha', 'bb');",
			"SELECT body FROM docs WHERE body = 'a' OR body = 'bb';",
			"SELECT body FROM docs WHERE body LIKE 'a%';",
			"SELECT body FROM docs WHERE body = 'alpha' ORDER BY body DESC LIMIT 4;",
			"SELECT DISTINCT body FROM docs WHERE body IN ('alpha', 'bb', 'alpha');",
			"SELECT body AS b FROM docs WHERE body = 'gamma';",
			"SELECT body FROM docs WHERE body = 'missing';",
		},
	)
}

@(test)
test_multi_index :: proc(t: ^testing.T) {
	// Two indexes on one table: per-column routing, cross-column AND
	// intersection, drop-one-keeps-other, and fan-out maintaining both.
	context.logger.lowest_level = .Error
	d := setup_db(t, "idxmulti")
	defer teardown_db(d, "idxmulti")

	testing.expect(
		t,
		db.execute(d, "CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT);") == .None,
		"create",
	)
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (1, 't1', 'alpha'), (2, 't2', 'beta'), (3, 't1', 'beta');") == .None,
		"insert",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_title ON docs (title);") == .None,
		"index title",
	)
	testing.expect(
		t,
		db.execute(d, "CREATE INDEX i_body ON docs (body);") == .None,
		"index body",
	)

	// Both route independently.
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE title = 't1';"),
		"INDEX SCAN ON docs USING title (eq, fetch)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'beta';"),
		"INDEX SCAN ON docs USING body (eq, fetch)",
	)
	// AND across the two indexes intersects.
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE title = 't1' AND body = 'beta';"),
		"INDEX SCAN ON docs USING title, body (and, fetch)",
	)
	got := db.query(d, "SELECT id FROM docs WHERE title = 't1' AND body = 'beta' ORDER BY id;")
	testing.expect(t, got.ok, "cross-index AND succeeds")
	testing.expect_value(t, len(got.rows), 1)
	if len(got.rows) == 1 {
		testing.expect_value(t, got.rows[0][0].(i64), i64(3))
	}
	// OR across the two indexes unions.
	orq := db.query(d, "SELECT id FROM docs WHERE title = 't2' OR body = 'alpha' ORDER BY id;")
	testing.expect(t, orq.ok, "cross-index OR succeeds")
	testing.expect_value(t, len(orq.rows), 2)

	// Fan-out maintains both: insert after both created.
	testing.expect(
		t,
		db.execute(d, "INSERT INTO docs VALUES (4, 't1', 'beta');") == .None,
		"insert maintains both",
	)
	got2 := db.query(d, "SELECT id FROM docs WHERE title = 't1' AND body = 'beta' ORDER BY id;")
	testing.expect(t, got2.ok, "AND after insert succeeds")
	testing.expect_value(t, len(got2.rows), 2)

	// Drop one: the other keeps routing.
	testing.expect(t, db.execute(d, "DROP INDEX i_body;") == .None, "drop body index")
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE title = 't1';"),
		"INDEX SCAN ON docs USING title (eq, fetch)",
	)
	testing.expect_value(
		t,
		drop_index_plan(t, d, "EXPLAIN SELECT id FROM docs WHERE body = 'beta';"),
		"FULL SCAN ON docs",
	)
	got3 := db.query(d, "SELECT id FROM docs WHERE title = 't1' ORDER BY id;")
	testing.expect(t, got3.ok, "survivor routes")
	testing.expect_value(t, len(got3.rows), 3)
}
