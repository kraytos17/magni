// Timing baseline for the engine's hot paths. Run with:
//   make perf
// Prints wall-clock timings to stdout. Not part of the correctness test suite;
// use the printed numbers to compare before/after a structural change.
//
// Coverage tracks recently restructured paths: the COUNT(*) fast path
// (ported to the data path), post-projection DISTINCT dedup, ORDER BY sort,
// and GROUP BY aggregation. A BUG-class regression here is a >2x slowdown
// on any single line, not small noise (the join in particular is noisy).
package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import "src:db"
import "src:pager"

DB_NAME :: "perf_bench.db"

fail :: proc(msg: string) -> ! {
	fmt.eprintln("perf:", msg)
	os.exit(1)
}

// timed_query runs one SELECT, checks the row count when expected_rows >= 0,
// and prints "perf_<label>: ... in %.1f ms (rows=N)".
timed_query :: proc(
	d: ^db.Database,
	label: string,
	human: string,
	sql: string,
	expect_rows: int = -1,
) {
	start := time.now()
	q := db.query(d, sql)
	el := time.duration_milliseconds(time.since(start))
	if !q.ok { fail(human) }
	if expect_rows >= 0 && len(q.rows) != expect_rows {
		fail(fmt.tprintf("%s: expected %d rows, got %d", human, expect_rows, len(q.rows)))
	}
	fmt.printf("perf_%s: %s in %.1f ms (rows=%d)\n", label, human, el, len(q.rows))
}

main :: proc() {
	if os.exists(DB_NAME) { os.remove(DB_NAME) }
	if os.exists(DB_NAME + "-wal") { os.remove(DB_NAME + "-wal") }

	d, err := db.open(DB_NAME)
	if err != .None { fail("open") }
	defer db.close(d)
	defer os.remove(DB_NAME)
	defer os.remove(DB_NAME + "-wal")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")

	// Build: one transaction so WAL fsync cost is amortized across the batch.
	db.execute(d, "BEGIN;")
	start := time.now()
	for i in 1 ..= 100000 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 2))
	}

	db.execute(d, "COMMIT;")
	el := time.duration_milliseconds(time.since(start))
	fmt.printf("perf_build:  100000-row batched insert in %.1f ms\n", el)

	// Full scan (page-cache + cursor + columnar decode path). ~5000 pages, far
	// beyond the 256-slot cache, so eviction + find_slot dominate.
	timed_query(d, "scan", "100000-row full scan", "SELECT * FROM t;", 100000)

	// COUNT(*) fast path (tree_count_rows, O(depth)): must stay orders of
	// magnitude below the full scan. If this approaches scan time, the fast
	// path stopped firing and COUNT(*) falls back to scan + aggregate.
	timed_query(d, "count", "COUNT(*) over 100000 rows", "SELECT COUNT(*) FROM t;", 1)

	// DISTINCT (post-projection dedup over 100000 distinct values).
	timed_query(d, "distinct", "DISTINCT over 100000 rows", "SELECT DISTINCT v FROM t;", 100000)

	// ORDER BY + LIMIT (sort full rows, slice after).
	timed_query(
		d,
		"order",
		"ORDER BY v DESC LIMIT 100",
		"SELECT * FROM t ORDER BY v DESC LIMIT 100;",
		100,
	)

	// GROUP BY aggregation (build_groups + per-group finalize) over a
	// low-cardinality key: 10000 rows in 100 groups.
	db.execute(d, "CREATE TABLE g (k INT, v INT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 10000 {
		db.execute(d, fmt.tprintf("INSERT INTO g VALUES (%d, %d);", i % 100, i))
	}
	db.execute(d, "COMMIT;")
	timed_query(
		d,
		"group",
		"GROUP BY with 100 groups",
		"SELECT k, COUNT(*), SUM(v) FROM g GROUP BY k;",
		100,
	)

	// IN-list membership (linear scan today: O(rows × list size)).
	// 500 literals over the 100000-row table.
	in_list: strings.Builder
	strings.builder_init(&in_list, context.temp_allocator)
	for i in 1 ..= 500 {
		if i > 1 { strings.write_string(&in_list, ",") }
		strings.write_int(&in_list, i)
	}
	timed_query(
		d,
		"inlist",
		"IN-list with 500 literals",
		fmt.tprintf("SELECT id FROM t WHERE id IN (%s);", strings.to_string(in_list)),
		500,
	)

	// CHECK enforcement (string split + column resolve + int parse per row).
	db.execute(d, "CREATE TABLE chk (price INT CHECK (price > 0));")
	db.execute(d, "BEGIN;")
	start = time.now()
	for i in 1 ..= 20000 {
		db.execute(d, fmt.tprintf("INSERT INTO chk VALUES (%d);", i))
	}
	db.execute(d, "COMMIT;")
	el = time.duration_milliseconds(time.since(start))
	fmt.printf("perf_check:  20000-row CHECK-enforced insert in %.1f ms\n", el)
	timed_query(d, "chkcnt", "COUNT of CHECK table", "SELECT COUNT(*) FROM chk;", 1)

	// Point lookups (tree_find + get_page/find_slot per level). Per-statement
	// overhead (parse + temp-arena growth) dominates here, so treat this as
	// informational; the scan is the cache-index-sensitive measurement.
	start = time.now()
	for i in 1 ..= 500 {
		r := db.query(d, fmt.tprintf("SELECT v FROM t WHERE id = %d;", i * 40))
		if !r.ok { fail("pk lookup") }
	}

	el = time.duration_milliseconds(time.since(start))
	fmt.printf("perf_lookup: 500 pk lookups in %.1f ms\n", el)

	// Hash join (builds the fingerprint index on the smaller side).
	db.execute(d, "CREATE TABLE b (id INT PRIMARY KEY, w INT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 1000 {
		db.execute(d, fmt.tprintf("INSERT INTO b VALUES (%d, %d);", i, i * 3))
	}

	db.execute(d, "COMMIT;")
	timed_query(
		d,
		"join",
		"1000x1000 join",
		"SELECT t.id, b.w FROM t JOIN b ON t.id = b.id;",
		1000,
	)

	pager.pager_stats_report(d.pager)
}
