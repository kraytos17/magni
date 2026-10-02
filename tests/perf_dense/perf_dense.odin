// Dense-search microbench.
//
// Run manually with release flags (NOT wired into `make perf`, so the
// band never moves on a measurement harness):
//   odin run tests/perf_dense -collection:src=src -o:aggressive -lto:thin \
//     -no-bounds-check -no-type-assert -disable-assert -microarch:native \
//     -source-code-locations:none
//
// Question it answers: how much does interior search cost per probe chain
// versus a full point-lookup level (~0.8µs from perf_lookup: page fetch +
// decode + compare)? If search is single-digit percent of a level, SIMD
// probing (B2b) is pointless — report the ratio, drop or schedule B2b.
package main

import "core:fmt"
import "core:time"
import "src:btree"
import "src:types"

SEARCH_ITERS :: 2000000

// bench_page times `iters` lower-bound searches over one dense page with
// strided targets (defeats branch prediction learning, mimics random PKs).
// key_base shifts targets into the page's key range (else every search
// returns 0 and the loop measures nothing). Returns nanoseconds per
// search. The checksum sink prevents elimination.
bench_page :: proc(
	page: []u8,
	id: btree.Page_Id,
	key_base: i64,
	key_span: int,
	iters: int,
) -> f64 {
	start := time.now()
	checksum := 0
	for i in 0 ..< iters {
		// Wide-stride targets across the page's full key range (gaps
		// included): defeats branch learning, exercises all probe depths.
		target := types.Row_ID(key_base + i64((i * 7919000) % key_span))
		idx, l_err := btree.dense_page_lower_bound(page, id, target)
		if l_err != .None { checksum += 1 }
		checksum += idx
	}

	el := time.duration_nanoseconds(time.since(start))
	fmt.printf("perf_dense_search: checksum=%d (sink)\n", checksum)
	return f64(el) / f64(iters)
}

main :: proc() {
	// Full-u64 page: 339 sparse keys (span > u32 forces full encoding;
	// exactly fills the page: 24+2712+1360 = 4096).
	fkeys := make([]types.Row_ID, 339, context.temp_allocator)
	for i in 0 ..< 339 { fkeys[i] = types.Row_ID(i64(i) * 20000000 - 3000000000) }

	fchildren := make([]u32, 340, context.temp_allocator)
	for i in 0 ..< 340 { fchildren[i] = u32(100 + i) }

	fpage := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	if err := btree.dense_build_from_sorted(fpage, 2, fkeys, fchildren); err != .None {
		fmt.eprintln("perf_dense_search: full build failed")
		return
	}

	// FOR page: 500 small-range keys (delta encoding, the post-split shape).
	dkeys := make([]types.Row_ID, 500, context.temp_allocator)
	for i in 0 ..< 500 { dkeys[i] = types.Row_ID(100000 + i64(i)) }

	dchildren := make([]u32, 501, context.temp_allocator)
	for i in 0 ..< 501 { dchildren[i] = u32(200 + i) }

	dpage := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	if err := btree.dense_build_from_sorted(dpage, 3, dkeys, dchildren); err != .None {
		fmt.eprintln("perf_dense_search: FOR build failed")
		return
	}

	fns := bench_page(fpage, 2, -3000000000, 6780000000, SEARCH_ITERS)
	fmt.printf("perf_dense_search: full-u64 339-key page %.1f ns/search\n", fns)
	dns := bench_page(dpage, 3, 100000, 5000, SEARCH_ITERS)
	fmt.printf("perf_dense_search: FOR 500-key page %.1f ns/search\n", dns)
}
