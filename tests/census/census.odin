// Allocation census: per-query allocation counts/bytes for the engine's hot
// paths, split heap vs temp arena. Run with:
//   make census
//
// Counts only — never compare wall-clock from this run: the counting wrapper
// adds per-call overhead. Reading the output:
//   heap covers context.allocator (pager/schema/DB structures + any explicit
//   heap allocations); temp covers context.temp_allocator (per-statement
//   parse, row materialization, sort/dedup/join scratch). The engine does not
//   rewind temp between statements (same as the perf harness), so temp live
//   grows monotonically across queries; windows show per-query churn.
//   `peak` is the window's max live bytes; `per-row` is allocated bytes and
//   allocation calls divided by result rows.
package main

import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "src:cell"
import "src:db"
import "src:executor"
import "src:types"

DB_NAME :: "census_bench.db"

fail :: proc(msg: string) -> ! {
	fmt.eprintln("census:", msg)
	os.exit(1)
}

// The counting allocator. Forward every request to `backing` unchanged and
// count only successful operations. No allocations happen in here (counters
// are fixed fields on the struct), so it is safe to wrap any allocator.
Census :: struct {
	backing   : mem.Allocator,
	allocs    : u64,
	bytes     : u64,
	frees     : u64,
	freed     : u64,
	live      : i64,
	peak      : i64,
	free_alls : u64,
	hist      : [7]u64,
	hist_bytes: [7]u64,
}

HIST_LABELS := [7]string{"<=32", "<=64", "<=128", "<=256", "<=1K", "<=4K", ">4K"}

hist_bucket :: proc(size: int) -> int {
	switch {
	case size <= 32:
		return 0
	case size <= 64:
		return 1
	case size <= 128:
		return 2
	case size <= 256:
		return 3
	case size <= 1024:
		return 4
	case size <= 4096:
		return 5
	case:
		return 6
	}
}

census_proc :: proc(
	data: rawptr,
	mode: mem.Allocator_Mode,
	size, alignment: int,
	old_memory: rawptr,
	old_size: int,
	loc := #caller_location,
) -> (
	[]byte,
	mem.Allocator_Error,
) {
	c := (^Census)(data)
	res, err := c.backing.procedure(
		c.backing.data,
		mode,
		size,
		alignment,
		old_memory,
		old_size,
		loc,
	)
	if err != .None {
		return res, err
	}

	switch mode {
	case .Alloc, .Alloc_Non_Zeroed:
		c.allocs += 1
		c.bytes += u64(size)
		c.live += i64(size)
		if c.live > c.peak {
			c.peak = c.live
		}

		b := hist_bucket(size)
		c.hist[b] += 1
		c.hist_bytes[b] += u64(size)
	case .Resize, .Resize_Non_Zeroed:
		delta := i64(size) - i64(old_size)
		if delta > 0 {
			c.allocs += 1
			c.bytes += u64(delta)
		}

		c.live += delta
		if c.live > c.peak {
			c.peak = c.live
		}
	case .Free:
		c.frees += 1
		c.freed += u64(old_size)
		c.live -= i64(old_size)
	case .Free_All:
		c.free_alls += 1
		c.live = 0
	case .Query_Features, .Query_Info:
	}
	return res, err
}

Snap :: struct {
	allocs    : u64,
	bytes     : u64,
	frees     : u64,
	freed     : u64,
	live      : i64,
	peak      : i64,
	free_alls : u64,
	hist      : [7]u64,
	hist_bytes: [7]u64,
}

snap :: proc(c: ^Census) -> Snap {
	return {
		allocs = c.allocs,
		bytes = c.bytes,
		frees = c.frees,
		freed = c.freed,
		live = c.live,
		peak = c.peak,
		free_alls = c.free_alls,
		hist = c.hist,
		hist_bytes = c.hist_bytes,
	}
}

bytes_str :: proc(buf: []byte, b: u64) -> string {
	if b >= (1 << 20) {
		return fmt.bprintf(buf, "%.2fMB", f64(b) / f64(1 << 20))
	}
	if b >= (1 << 10) {
		return fmt.bprintf(buf, "%.1fKB", f64(b) / f64(1 << 10))
	}
	return fmt.bprintf(buf, "%dB", b)
}

// measure runs one query inside a census window on both allocators.
measure :: proc(
	d: ^db.Database,
	heap, temp: ^Census,
	label, sql: string,
	expect: int,
	show_hist := false,
) {
	// Baseline the window peaks so `peak` reads as max live during the query.
	heap.peak = heap.live
	temp.peak = temp.live
	h0 := snap(heap)
	t0 := snap(temp)

	q := db.query(d, sql)
	h1 := snap(heap)
	t1 := snap(temp)
	if !q.ok {
		fail(fmt.tprintf("query failed: %s", label))
	}

	rows := len(q.rows)
	if expect >= 0 && rows != expect {
		fail(fmt.tprintf("%s: expected %d rows, got %d", label, expect, rows))
	}

	hbuf, tbuf, hpbuf, tpbuf: [32]byte
	ha := h1.allocs - h0.allocs
	hb := h1.bytes - h0.bytes
	ta := t1.allocs - t0.allocs
	tb := t1.bytes - t0.bytes
	fmt.printf(
		"%-26s rows=%7d  heap[a=%7d B=%9s peak=%9s]  temp[a=%7d B=%9s peak=%9s]\n",
		label,
		rows,
		ha,
		bytes_str(hbuf[:], hb),
		bytes_str(hpbuf[:], u64(max(h1.peak, 0))),
		ta,
		bytes_str(tbuf[:], tb),
		bytes_str(tpbuf[:], u64(max(t1.peak, 0))),
	)
	if rows > 0 {
		ba := f64(ha + ta) / f64(rows)
		bb := f64(hb + tb) / f64(rows)
		fmt.printf("    per-row: allocs=%.2f bytes=%.1f\n", ba, bb)
	}
	if show_hist {
		fmt.printf("    temp hist:")
		for i in 0 ..< 7 {
			cnt := t1.hist[i] - t0.hist[i]
			if cnt > 0 {
				fmt.printf(" %s:%d", HIST_LABELS[i], cnt)
			}
		}
		fmt.println()
	}
}

// measure_lookups aggregates N single-row PK lookups (each a full parse +
// execute) into one window. Statements are prebuilt so the window contains
// only engine work, not harness string formatting.
measure_lookups :: proc(d: ^db.Database, heap, temp: ^Census, n: int) {
	stmts := make([dynamic]string, 0, n, context.allocator)
	defer delete(stmts)
	for i in 1 ..= n {
		append(&stmts, fmt.tprintf("SELECT v FROM t WHERE id = %d;", i * 40))
	}

	h0 := snap(heap)
	t0 := snap(temp)
	for s in stmts {
		q := db.query(d, s)
		if !q.ok {
			fail("pk lookup failed")
		}
	}

	h1 := snap(heap)
	t1 := snap(temp)
	ha := h1.allocs - h0.allocs
	hb := h1.bytes - h0.bytes
	ta := t1.allocs - t0.allocs
	tb := t1.bytes - t0.bytes
	hbuf, tbuf: [32]byte
	fmt.printf(
		"%-26s rows=%7d  heap[a=%7d B=%9s]  temp[a=%7d B=%9s]\n",
		fmt.tprintf("%d pk lookups", n),
		n,
		ha,
		bytes_str(hbuf[:], hb),
		ta,
		bytes_str(tbuf[:], tb),
	)
	fmt.printf(
		"    per-lookup: heap allocs=%.1f bytes=%.0f  temp allocs=%.1f bytes=%.0f\n",
		f64(ha) / f64(n),
		f64(hb) / f64(n),
		f64(ta) / f64(n),
		f64(tb) / f64(n),
	)
}

main :: proc() {
	// Install counters around the real allocators before the DB opens, so
	// every allocator the DB captures (e.g. Pager.allocator) is counted.
	orig_heap := context.allocator
	orig_temp := context.temp_allocator
	heap: Census
	heap.backing = orig_heap
	temp: Census
	temp.backing = orig_temp
	context.allocator = mem.Allocator {
		procedure = census_proc,
		data      = &heap,
	}
	context.temp_allocator = mem.Allocator {
		procedure = census_proc,
		data      = &temp,
	}
	fmt.printf(
		"sizes: Value=%dB Row_Entry=%dB Cell=%dB Column=%dB\n",
		size_of(types.Value),
		size_of(executor.Row_Entry),
		size_of(cell.Cell),
		size_of(types.Column),
	)

	if os.exists(DB_NAME) {
		os.remove(DB_NAME)
	}
	if os.exists(DB_NAME + "-wal") {
		os.remove(DB_NAME + "-wal")
	}

	d, err := db.open(DB_NAME)
	if err != .None {
		fail("open")
	}

	// Build the same dataset as tests/perf (plus a TEXT table for clone cost).
	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 100000 {
		db.execute(d, fmt.tprintf("INSERT INTO t VALUES (%d, %d);", i, i * 2))
	}

	db.execute(d, "COMMIT;")
	db.execute(d, "CREATE TABLE g (k INT, v INT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 10000 {
		db.execute(d, fmt.tprintf("INSERT INTO g VALUES (%d, %d);", i % 100, i))
	}

	db.execute(d, "COMMIT;")
	db.execute(d, "CREATE TABLE b (id INT PRIMARY KEY, w INT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 1000 {
		db.execute(d, fmt.tprintf("INSERT INTO b VALUES (%d, %d);", i, i * 3))
	}

	db.execute(d, "COMMIT;")
	db.execute(d, "CREATE TABLE txt (s TEXT);")
	db.execute(d, "BEGIN;")
	for i in 1 ..= 10000 {
		db.execute(d, fmt.tprintf("INSERT INTO txt VALUES ('row-%08d-abcdefghij');", i))
	}

	db.execute(d, "COMMIT;")
	fmt.printf("--- per-query windows (heap=context.allocator, temp=context.temp_allocator) ---\n")

	measure(d, &heap, &temp, "SELECT 1", "SELECT 1;", 1)
	measure(d, &heap, &temp, "count(*)", "SELECT COUNT(*) FROM t;", 1)
	measure(d, &heap, &temp, "scan all (2 cols)", "SELECT * FROM t;", 100000, true)
	measure(d, &heap, &temp, "scan id only", "SELECT id FROM t;", 100000)
	measure(d, &heap, &temp, "distinct", "SELECT DISTINCT v FROM t;", 100000)
	measure(d, &heap, &temp, "order+limit", "SELECT * FROM t ORDER BY v DESC LIMIT 100;", 100)
	measure(d, &heap, &temp, "group by", "SELECT k, COUNT(*), SUM(v) FROM g GROUP BY k;", 100)
	measure(d, &heap, &temp, "text scan (10k)", "SELECT * FROM txt;", 10000, true)
	measure(d, &heap, &temp, "filter 50%", "SELECT * FROM t WHERE v > 100000;", 50000)
	measure(d, &heap, &temp, "filter 0.5%", "SELECT * FROM t WHERE v > 199000;", 500)

	in_list: strings.Builder
	strings.builder_init(&in_list, context.allocator)
	defer strings.builder_destroy(&in_list)

	strings.write_string(&in_list, "SELECT id FROM t WHERE id IN (")
	for i in 1 ..= 500 {
		if i > 1 {
			strings.write_string(&in_list, ",")
		}
		fmt.sbprint(&in_list, i)
	}

	strings.write_string(&in_list, ");")
	measure(d, &heap, &temp, "in-list 500", strings.to_string(in_list), 500)
	measure(
		d,
		&heap,
		&temp,
		"join 1000x1000",
		"SELECT t.id, b.w FROM t JOIN b ON t.id = b.id;",
		1000,
	)

	measure_lookups(d, &heap, &temp, 500)
	db.close(d)
	os.remove(DB_NAME)

	// Restore context before the process exits (the census structs die with
	// `main`).
	context.allocator = orig_heap
	context.temp_allocator = orig_temp
}
