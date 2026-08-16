package btree

import "core:mem"
import "src:pager"

// Stats holds B-tree statistics keyed by page id. The maps must outlive the
// transient btree.Tree instances created per statement (so COUNT(*) and
// rowid allocation stay O(1) across statements), so the Stats value is stored
// on the pager through an opaque pointer that this package owns. The pager
// never interprets the contents; it only clears entries on eviction/free via
// its on_evict hook and releases the allocation on close via free_stats.
Stats :: struct {
	row_counts:      map[u32]int,
	page_int_ranges: map[u32]pager.Page_Int_Range,
}

// tree_stats returns the pager-attached Stats (must be non-nil: btree.init
// attaches it lazily).
tree_stats :: proc(t: ^Tree) -> ^Stats {
	return cast(^Stats)t.pager.stats
}

// attach_stats lazily creates and attaches a Stats instance to the pager, the
// first time any btree.Tree is created against it.
attach_stats :: proc(t: ^Tree) {
	p := t.pager
	if p.stats != nil { return }

	s := new(Stats, p.allocator)
	s.row_counts = make(map[u32]int, 64, p.allocator)
	s.page_int_ranges = make(map[u32]pager.Page_Int_Range, 64, p.allocator)
	p.stats = s
	p.on_evict = on_evict_stats
	p.free_stats = free_stats_proc
}

on_evict_stats :: proc(data: rawptr, page_num: u32) {
	if data == nil { return }
	s := cast(^Stats)data
	delete_key(&s.row_counts, page_num)
	delete_key(&s.page_int_ranges, page_num)
}

free_stats_proc :: proc(data: rawptr) {
	if data == nil { return }
	s := cast(^Stats)data
	delete(s.row_counts)
	delete(s.page_int_ranges)
	mem.free(s)
}

// invalidate_page_int_range drops the cached integer range for a page, e.g.
// after a mutation changes its contents.
invalidate_page_int_range :: proc(t: ^Tree, page_id: u32) {
	delete_key(&tree_stats(t).page_int_ranges, page_id)
}
