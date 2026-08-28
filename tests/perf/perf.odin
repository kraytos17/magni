// Timing baseline for the engine's hot paths. Run with:
//   make perf
// Prints wall-clock timings to stdout. Not part of the correctness test suite;
// use the printed numbers to compare before/after a structural change.
package main

import "core:fmt"
import "core:os"
import "core:time"
import "src:db"

DB_NAME :: "perf_bench.db"

fail :: proc(msg: string) -> ! {
	fmt.eprintln("perf:", msg)
	os.exit(1)
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
	fmt.printf("perf_build: 100000-row batched insert in %.1f ms\n", el)

	// Full scan (page-cache + cursor + columnar decode path). ~5000 pages, far
	// beyond the 256-slot cache, so eviction + find_slot dominate.
	start = time.now()
	q := db.query(d, "SELECT * FROM t;")
	el = time.duration_milliseconds(time.since(start))
	fmt.printf("perf_scan:  100000-row full scan in %.1f ms (rows=%d ok=%v)\n", el, len(q.rows), q.ok)

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
	start = time.now()
	j := db.query(d, "SELECT t.id, b.w FROM t JOIN b ON t.id = b.id;")
	el = time.duration_milliseconds(time.since(start))
	fmt.printf("perf_join:  1000x1000 join in %.1f ms (rows=%d ok=%v)\n", el, len(j.rows), j.ok)
}
