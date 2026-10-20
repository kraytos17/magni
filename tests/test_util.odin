// Shared scaffolding for package tests: logger muting, DB filename
// ownership (mint/drop), and asserting runners for setup statements,
// single statements, and queries. Every runner fails the test at the
// failing step instead of cascading into confusing asserts below.
package tests

import "base:runtime"
import "core:fmt"
import "core:log"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:db"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

// suppress_expected_errors / restore_logger let a test silence log output while
// a DB call that is *expected* to fail (and logs via log.errorf) runs, so that
// error does not count as an assertion failure. Return-value assertions around
// the call remain meaningful.
//
// Odin's `context` is passed by value to procedures, so these helpers return the
// new context; the caller must assign it: `context = ...`.
suppress_expected_errors :: proc() -> (saved: log.Logger, new_ctx: runtime.Context) {
	saved = context.logger
	new_ctx = context
	new_ctx.logger = log.nil_logger()
	return
}

restore_logger :: proc(saved: log.Logger) -> runtime.Context {
	c := context
	c.logger = saved
	return c
}

// clean_db_files removes a test database and its WAL sidecar if present.
// Shared preamble for every setup/teardown env helper below.
clean_db_files :: proc(filename: string) {
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
}

// mute_test_logging silences info/debug logs for a test body. Every env
// helper calls this; tests must not repeat it. Expected-failure paths use
// suppress_expected_errors or .Fatal instead — never weaken this to mute
// real failures.
mute_test_logging :: proc() {
	context.logger.lowest_level = .Error
}

// mint_db_filename builds an owned "<prefix><name>.db" path on the heap
// (pass prefixes like "test_exec_"). Owned is load-bearing: tests run
// free_all(temp) mid-body, so a temp string stashed in the context would
// dangle by teardown. The matching teardown must release it — see
// drop_db_files.
mint_db_filename :: proc(prefix, name: string) -> string {
	filename, _ := strings.clone(fmt.tprintf("%s%s.db", prefix, name), context.allocator)
	return filename
}

// db_filename derives the "<prefix><name>.db" path on temp storage for env
// pairs that re-derive (never stash) the name at teardown. Do not store the
// result past a free_all(temp); use mint_db_filename instead.
db_filename :: proc(prefix, name: string) -> string {
	return fmt.tprintf("%s%s.db", prefix, name)
}

// drop_db_files removes a minted database and its WAL sidecar, then frees
// the filename. Pair with mint_db_filename.
drop_db_files :: proc(filename: string) {
	clean_db_files(filename)
	delete(filename)
}

// must_parse_select parses a SELECT or fails the test outright.
must_parse_select :: proc(t: ^testing.T, sql: string) -> parser.Select_Stmt {
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "query must parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "expected Select_Stmt")
	return sel
}

// run_exec_setup parses + executes each setup statement against a btree
// Tree. Every step must succeed; a setup failure fails the test here
// instead of cascading into confusing asserts below.
run_exec_setup :: proc(t: ^testing.T, tree: ^btree.Tree, setup: []string) {
	for sql in setup {
		stmt, pok, _ := parser.parse(sql, context.temp_allocator)
		testing.expect(t, pok, "setup SQL must parse")
		if !pok {
			continue
		}
		sok, _, _ := executor.execute(tree, stmt)
		testing.expect(t, sok, "setup SQL must execute")
	}
}

// must_execute runs one statement and demands success, discarding rows.
// For SELECTs whose rows matter, use must_exec_query.
must_execute :: proc(t: ^testing.T, tree: ^btree.Tree, stmt: parser.Statement) {
	ok, _, _ := executor.execute(tree, stmt)
	testing.expect(t, ok, "statement must execute")
}

// must_parse parses one SQL string and demands success, returning the
// full statement for variant assertions.
must_parse :: proc(t: ^testing.T, sql: string) -> parser.Statement {
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SQL must parse")
	return stmt
}

// must_execute_sql parses one SQL string and demands successful execution.
// For SELECTs whose rows matter, parse with must_parse_select and run
// must_exec_query instead (this re-parses; tests are not perf-sensitive).
must_execute_sql :: proc(t: ^testing.T, tree: ^btree.Tree, sql: string) {
	must_execute(t, tree, must_parse(t, sql))
}

// must_exec_query runs a SELECT and demands success, returning rows + cols
// for value assertions. Returns nils on failure (callers keep their guards).
must_exec_query :: proc(
	t: ^testing.T,
	tree: ^btree.Tree,
	sel: parser.Select_Stmt,
) -> (
	[]executor.Row_Entry,
	[]types.Column,
) {
	rows, cols, ok := executor.exec_query(tree, sel)
	testing.expect(t, ok, "query must succeed")
	if !ok {
		return nil, nil
	}
	return rows, cols
}

// run_db_setup executes each setup statement against an open Database.
// Every step must return .None; a setup failure fails the test here
// instead of cascading into confusing asserts below.
run_db_setup :: proc(t: ^testing.T, d: ^db.Database, setup: []string) {
	for sql in setup {
		testing.expect(t, db.execute(d, sql) == .None, "setup SQL must succeed")
	}
}

// must_db_execute runs one statement and demands .None. For queries whose
// rows matter, use db.query directly and assert on q.ok / q.rows.
must_db_execute :: proc(t: ^testing.T, d: ^db.Database, sql: string) {
	testing.expect(t, db.execute(d, sql) == .None, "statement must succeed")
}

// check_col runs sql and asserts the first projected column equals want
// element-wise (msg names the query under test). One generic behind the
// per-type column asserts; the type assertion still pins the storage
// class (covering REAL must come back f64, not widened int).
check_col :: proc(t: ^testing.T, d: ^db.Database, sql: string, want: []$T, msg: string) {
	q := db.query(d, sql)
	testing.expect(t, q.ok, msg)
	if !q.ok {
		return
	}
	testing.expect_value(t, len(q.rows), len(want))
	for i in 0 ..< min(len(q.rows), len(want)) {
		v, is_col := q.rows[i][0].(T)
		testing.expect(t, is_col, msg)
		if is_col {
			testing.expect_value(t, v, want[i])
		}
	}
}

// explain renders one EXPLAIN statement to its plan string through the
// executor directly (shared by every EXPLAIN pin).
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

// assert_twin runs each query and its table-swapped twin, demanding
// identical output (success, row counts, widths, values with NULL-safe
// equality). One harness behind the per-file twin loops; swap_from/to is
// the table-name swap ("docs"/"docs2", "FROM t "/"FROM t2 ").
assert_twin :: proc(t: ^testing.T, d: ^db.Database, queries: []string, swap_from, swap_to: string) {
	for q in queries {
		free_all(context.temp_allocator)
		twin, _ := strings.replace(q, swap_from, swap_to, 1, context.temp_allocator)
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

// INDEX_BODIES is the shared 5-body workload distribution (one empty
// string exercises empty-key paths). One spelling so body lists cannot
// skew per call site; the 6-body variant in real tests stays local.
INDEX_BODIES :: []string{"alpha", "bb", "alpha", "", "gamma"}

// workload_expect builds the oracle map and loads the bodies workload,
// returning the owned map (caller defers destroy_expect).
workload_expect :: proc(
	t: ^testing.T,
	d: ^db.Database,
	bodies: []string,
	n: int,
) -> map[string][dynamic]i64 {
	expect := make(map[string][dynamic]i64, context.allocator)
	index_workload(t, d, bodies, n, &expect)
	return expect
}

// open_test_pager opens a pager on an owned file and asserts it is usable.
// Pair with destroy_test_pager. One helper behind the executor/schema/
// pager/WAL env twins (prefix keeps test files isolated).
open_test_pager :: proc(t: ^testing.T, prefix, name: string) -> (^pager.Pager, string) {
	mute_test_logging()
	filename := mint_db_filename(prefix, name)
	clean_db_files(filename)

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "Failed to open pager")
	testing.expect(t, p != nil, "Pager should not be nil")
	return p, filename
}

// destroy_test_pager closes the pager and removes the database, its WAL
// sidecar, and the owned filename.
destroy_test_pager :: proc(p: ^pager.Pager, filename: string) {
	_ = pager.close(p)
	drop_db_files(filename)
}

// boot_schema_catalog allocates a slotdir schema root on p and boots the
// schema catalog, returning its tree.
boot_schema_catalog :: proc(t: ^testing.T, p: ^pager.Pager) -> btree.Tree {
	schema_page, alloc_err := pager.allocate_page(p)
	testing.expect(t, alloc_err == .None, "Failed to allocate schema page")
	// Schema roots are slotdir post-flip, matching what production creators write.
	if !btree.init_slot_leaf_page(schema_page.data, schema_page.page_num) {
		testing.fail_now(t, "Failed to init slotdir schema page")
	}
	pager.mark_dirty(p, schema_page.page_num)
	pager.unpin_page(p, schema_page.page_num)

	tree := btree.init(p, schema_page.page_num)
	ok := schema.init(&tree)
	testing.expect(t, ok, "Failed to init schema")
	return tree
}

// must_tree_insert inserts one row and demands .None. Collapses the
// repeated seed-insert assertion blocks across btree tests.
must_tree_insert :: proc(
	t: ^testing.T,
	tree: ^btree.Tree,
	id: types.Row_ID,
	values: []types.Value,
) {
	testing.expect(t, btree.tree_insert(tree, id, values) == .None, "seed insert succeeds")
}
