package btree

import "core:mem"
import "core:slice"
import "src:pager"

// Stats holds B-tree statistics indexed by page id. The arrays must outlive the
// transient btree.Tree instances created per statement (so COUNT(*) and rowid
// allocation stay O(1) across statements), so the Stats value is stored on the
// pager through an opaque pointer that this package owns. The pager never
// interprets the contents; it only clears entries on eviction/free via its
// on_evict hook and releases the allocation on close via free_stats.
//
// Arrays are indexed directly by page id (a direct load vs. a hash+probe on the
// per-mutation hot path). row_counts uses -1 as the "uncached" sentinel (counts
// are always >= 0); page_int_ranges uses nil for uncached. Entries are reset to
// the sentinel on eviction/free so a reused page never reads a stale value.
Stats :: struct {
	row_counts     : [dynamic]int, // index = page id; -1 = uncached
	page_int_ranges: [dynamic]Maybe(pager.Page_Int_Range), // index = page id; nil = uncached
	index_ndv      : [dynamic]int, // index = index root page id; -1 = unknown
}

// tree_stats returns the pager-attached Stats (non-nil: btree.init attaches it).
@(private)
tree_stats :: proc(t: ^Tree) -> ^Stats {
	return cast(^Stats)t.pager.stats
}

// attach_stats lazily creates and attaches a Stats instance to the pager, the
// first time any btree.Tree is created against it.
@(private)
attach_stats :: proc(t: ^Tree) {
	p := t.pager
	if p.stats != nil {
		return
	}

	s := new(Stats, p.allocator)
	s.row_counts = make([dynamic]int, 0, 64, p.allocator)
	s.page_int_ranges = make([dynamic]Maybe(pager.Page_Int_Range), 0, 64, p.allocator)
	s.index_ndv = make([dynamic]int, 0, 64, p.allocator)
	p.stats = s
	p.on_evict = on_evict_stats
	p.free_stats = free_stats_proc
}

// stat_grow extends a per-page cache array to cover page_id, filling new
// slots with the array's uncached sentinel, and returns the index. The
// index must be used in a separate statement: indexing with this call
// inline snapshots the pre-grow header and traps.
@(private = "file")
stat_grow :: proc(arr: ^[dynamic]$T, page_id: u32, fill: T) -> int {
	idx := int(page_id)
	if idx >= len(arr) {
		old := len(arr)
		resize(arr, idx + 1)
		slice.fill(arr[old:], fill)
	}
	return idx
}

// stat_reset marks a page's cached entry as uncached (bounds-guarded;
// never grows the array — eviction/free of a page we never tracked is a
// no-op).
@(private = "file")
stat_reset :: proc(arr: ^[dynamic]$T, page_id: u32, sentinel: T) {
	if idx := int(page_id); idx < len(arr) {
		arr[idx] = sentinel
	}
}

// stats_row_count_get returns the cached row count for page_id, if present.
@(private)
stats_row_count_get :: proc(s: ^Stats, page_id: u32) -> (int, bool) {
	if int(page_id) >= len(s.row_counts) {
		return 0, false
	}

	count := s.row_counts[page_id]
	return count, count >= 0
}

// stats_row_count_set caches a row count for page_id, growing the array (new
// slots initialized to the -1 sentinel) as needed.
@(private)
stats_row_count_set :: proc(s: ^Stats, page_id: u32, count: int) {
	idx := stat_grow(&s.row_counts, page_id, -1)
	s.row_counts[idx] = count
}

// stats_range_get returns the cached integer range for page_id, if present.
@(private)
stats_range_get :: proc(s: ^Stats, page_id: u32) -> (pager.Page_Int_Range, bool) {
	if int(page_id) >= len(s.page_int_ranges) {
		return {}, false
	}
	if r, ok := s.page_int_ranges[page_id].?; ok {
		return r, true
	}
	return {}, false
}

// stats_range_set caches an integer range for page_id, growing the array (new
// slots initialized to nil) as needed.
@(private)
stats_range_set :: proc(s: ^Stats, page_id: u32, r: pager.Page_Int_Range) {
	idx := stat_grow(&s.page_int_ranges, page_id, nil)
	s.page_int_ranges[idx] = r
}

// stats_row_count_reset marks a page's cached count as uncached (bounds-guarded;
// never grows the array — eviction/free of a page we never tracked is a no-op).
@(private)
stats_row_count_reset :: proc(s: ^Stats, page_id: u32) {
	stat_reset(&s.row_counts, page_id, -1)
}

// stats_range_reset marks a page's cached range as uncached (bounds-guarded).
@(private)
stats_range_reset :: proc(s: ^Stats, page_id: u32) {
	stat_reset(&s.page_int_ranges, page_id, nil)
}

// index_ndv_get returns the cached distinct-key count for an index root, if
// known. Public: tests and the future cost model read through it; unknown
// (never analyzed, or invalidated by a write) reports ok=false so callers
// fall back to today's behavior.
index_ndv_get :: proc(p: ^pager.Pager, root: u32) -> (int, bool) {
	if p.stats == nil {
		return 0, false
	}

	s := cast(^Stats)p.stats
	if int(root) >= len(s.index_ndv) {
		return 0, false
	}

	ndv := s.index_ndv[root]
	return ndv, ndv >= 0
}

// stats_index_ndv_set caches a distinct-key count for an index root,
// growing the array (new slots initialized to the -1 sentinel) as needed.
@(private)
stats_index_ndv_set :: proc(s: ^Stats, root: u32, ndv: int) {
	idx := stat_grow(&s.index_ndv, root, -1)
	s.index_ndv[idx] = ndv
}

// stats_index_ndv_reset marks an index root's count unknown (bounds-guarded;
// never grows — resetting an untracked root is a no-op). Called on every
// index mutation: any insert or delete can change the distinct count.
@(private)
stats_index_ndv_reset :: proc(s: ^Stats, root: u32) {
	stat_reset(&s.index_ndv, root, -1)
}

// on_evict_stats clears a page's cached stats when the pager evicts or frees
// it, so a later reuse of the same page id never reads stale values.
@(private = "file")
on_evict_stats :: proc(data: rawptr, page_num: u32) {
	if data == nil {
		return
	}

	s := cast(^Stats)data
	stats_row_count_reset(s, page_num)
	stats_range_reset(s, page_num)
	stats_index_ndv_reset(s, page_num)
}

// free_stats_proc releases a Stats instance (pager close hook).
@(private = "file")
free_stats_proc :: proc(data: rawptr) {
	if data == nil {
		return
	}

	s := cast(^Stats)data
	delete(s.row_counts)
	delete(s.page_int_ranges)
	delete(s.index_ndv)
	mem.free(s)
}

// invalidate_page_int_range drops the cached integer range for a page, e.g.
// after a mutation changes its contents.
@(private)
invalidate_page_int_range :: proc(t: ^Tree, page_id: u32) {
	stats_range_reset(tree_stats(t), page_id)
}
