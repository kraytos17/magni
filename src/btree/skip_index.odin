package btree

import "core:encoding/endian"
import "core:sort"
import "src:cell"
import "src:pager"

SKIP_FORMAT_MAGIC :: u32(0x4B495054)

// MAX_SKIP_ENTRIES bounds one skip page: the 12-byte header plus packed
// entries must fit in PAGE_SIZE. Larger zone sets are merged down before write.
MAX_SKIP_ENTRIES :: (PAGE_SIZE - 12) / size_of(Skip_Entry)

// Skip_Op selects how a skip-index range bound is derived from a comparison
// operator. Only these operators can be safely answered from the zone-map
// min/max ranges; anything else disables skipping entirely.
Skip_Op :: enum u8 {
	EQ,
	LT,
	LTE,
	GT,
	GTE,
}

Skip_Entry :: struct #packed {
	page_min: u32,
	page_max: u32,
	min_int : i64,
	max_int : i64,
}

Skip_Index :: struct {
	root: u32,
}

// skip_collect_entries walks every leaf, accumulating (page, min, max) zone
// entries for col_index. Owns its cursor end to end (open, walk, destroy),
// so callers hold no cursor state across the later sort/merge/write steps.
@(private = "file")
skip_collect_entries :: proc(t: ^Tree, col_index: int, entries: ^[dynamic]Skip_Entry) -> Error {
	cursor, c_err := cursor_start(t, context.temp_allocator)
	if c_err != .None {
		return c_err
	}

	defer cursor_destroy(&cursor)
	acc := Skip_Accumulator{}
	for cursor.is_valid {
		item := cursor.path[cursor.depth - 1]
		page_id := item.page_id
		node, n_err := load_node(t, page_id)
		if n_err != .None {
			return n_err
		}
		if !is_leaf(node) {
			unpin_node(t, node)
			cursor_advance(&cursor)
			continue
		}

		cell_count := get_cell_count(node.data, page_id)
		if cell_count == 0 {
			unpin_node(t, node)
			cursor_advance(&cursor)
			continue
		}

		min_val, max_val := page_int_range(t, node, page_id, cell_count, col_index)
		unpin_node(t, node)
		if min_val <= max_val {
			accumulator_add(&acc, page_id, min_val, max_val, entries)
		}
		cursor_advance(&cursor)
	}

	accumulator_flush(&acc, entries)
	return .None
}

// skip_merge_to_fit pairwise-merges min-sorted zones left-first until the
// set fits one skip page, returning the surviving count. Widening a zone's
// range keeps query_skip_index_range a safe superset window (rows are still
// filtered afterwards); only pruning granularity drops.
@(private = "file")
skip_merge_to_fit :: proc(entries: []Skip_Entry) -> int {
	n := len(entries)
	for n > MAX_SKIP_ENTRIES {
		out := 0
		for i := 0; i < n; i += 2 {
			e := entries[i]
			if i + 1 < n {
				f := entries[i + 1]
				if f.max_int > e.max_int {
					e.max_int = f.max_int
				}
				if f.page_min < e.page_min {
					e.page_min = f.page_min
				}
				if f.page_max > e.page_max {
					e.page_max = f.page_max
				}
			}

			entries[out] = e
			out += 1
		}
		n = out
	}
	return n
}

build_skip_index :: proc(t: ^Tree, col_index: int) -> (Skip_Index, Error) {
	entries := make([dynamic]Skip_Entry, context.temp_allocator)
	defer delete(entries)
	if c_err := skip_collect_entries(t, col_index, &entries); c_err != .None {
		return {}, c_err
	}
	if len(entries) == 0 {
		return {}, .None
	}

	sort.quick_sort_proc(entries[:], proc(a, b: Skip_Entry) -> int {
		if a.min_int < b.min_int {
			return -1
		}
		if a.min_int > b.min_int {
			return 1
		}
		return 0
	})

	n := skip_merge_to_fit(entries[:])
	return write_skip_page(t, entries[:n], col_index)
}

// Skip_Accumulator coalesces adjacent pages whose integer ranges overlap or
// touch into single zone-map entries (page spans with a running min/max).
Skip_Accumulator :: struct {
	current: Skip_Entry,
	active : bool,
}

// add extends the current entry when `min_val` continues its integer range,
// otherwise flushes it and starts a new one.
accumulator_add :: proc(
	a: ^Skip_Accumulator,
	page_id: u32,
	min_val, max_val: i64,
	entries: ^[dynamic]Skip_Entry,
) {
	if a.active && a.current.max_int + 1 >= min_val {
		a.current.page_max = page_id
		if max_val > a.current.max_int {
			a.current.max_int = max_val
		}
		return
	}

	accumulator_flush(a, entries)
	a.current = Skip_Entry {
		page_min = page_id,
		page_max = page_id,
		min_int  = min_val,
		max_int  = max_val,
	}
	a.active = true
}

accumulator_flush :: proc(a: ^Skip_Accumulator, entries: ^[dynamic]Skip_Entry) {
	if !a.active {
		return
	}

	append(entries, a.current)
	a.active = false
}

// page_int_range returns the [min, max] integer range for col_index among a
// leaf page's cells, preferring the cached stats range and falling back to a
// full cell scan. `min > max` means "no integer values" (page contributes no
// entry). When the scan derives a range and stats are enabled, it is cached.
@(private = "file")
page_int_range :: proc(
	t: ^Tree,
	node: Node,
	page_id: u32,
	cell_count: int,
	col_index: int,
) -> (
	min_val: i64,
	max_val: i64,
) {
	min_val = max(i64)
	max_val = min(i64)
	has_dir := true
	if has_dir {
		if r, cached := stats_range_get(tree_stats(t), page_id);
		   cached && int(r.col_index) == col_index {
			min_val = r.min_int
			max_val = r.max_int
		}
	}
	if min_val > max_val {
		min_val, max_val = scan_page_int_range(node, page_id, cell_count, col_index)
		if min_val <= max_val && has_dir {
			stats_range_set(
				tree_stats(t),
				page_id,
				pager.Page_Int_Range {
					col_index = u8(col_index),
					min_int = min_val,
					max_int = max_val,
				},
			)
		}
	}
	return min_val, max_val
}

@(private = "file")
scan_page_int_range :: proc(
	node: Node,
	page_id: u32,
	cell_count: int,
	col_index: int,
) -> (
	i64,
	i64,
) {
	min_val := max(i64)
	max_val := min(i64)
	if col_index == -1 {
		return min_val, max_val
	}
	for i in 0 ..< cell_count {
		ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, Page_Id(page_id), i)
		if p_err != .None {
			continue
		}

		c, _, ok := cell.deserialize(
			node.data,
			int(ptr),
			cell.Config{zero_copy = true, allocator = context.temp_allocator},
		)
		if !ok {
			continue
		}
		if col_index < len(c.values) {
			if v, is_int := c.values[col_index].(i64); is_int {
				if v < min_val {
					min_val = v
				}
				if v > max_val {
					max_val = v
				}
			}
		}
		cell.destroy(&c, context.temp_allocator)
	}
	return min_val, max_val
}

// write_skip_page serializes the sorted entries into a fresh skip-index page:
// [magic u32][count u32][col_index u32] then packed Skip_Entry rows.
@(private = "file")
write_skip_page :: proc(t: ^Tree, entries: []Skip_Entry, col_index: int) -> (Skip_Index, Error) {
	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None {
		return {}, .Page_Full
	}

	n := len(entries)
	if 12 + n * size_of(Skip_Entry) > PAGE_SIZE {
		return {}, .Page_Full
	}

	data := page.data[:12 + n * size_of(Skip_Entry)]
	endian.unchecked_put_u32le(data[0:4], SKIP_FORMAT_MAGIC)
	endian.unchecked_put_u32le(data[4:8], u32(n))
	endian.unchecked_put_u32le(data[8:12], u32(col_index))
	copy(data[12:], transmute([]byte)entries)

	pager.mark_dirty(t.pager, page.page_num)
	pager.unpin_page(t.pager, page.page_num)
	return Skip_Index{root = page.page_num}, .None
}

// query_skip_index_range returns a conservative page window [start, end] for
// `col_index <op> val` (start = first leaf to scan, end = last leaf to scan;
// 0 = unbounded). The window is a superset of every matching row, so applying
// it is always safe: rows inside the window are still filtered by the WHERE
// predicate, and rows outside it cannot match. Returns ok=false (no skipping)
// when the index is missing, unreadable, legacy-format, built for a different
// column, or the operator is not skip-safe.
query_skip_index_range :: proc(
	p: ^pager.Pager,
	skip_root: u32,
	col_index: int,
	op: Skip_Op,
	val: i64,
) -> (
	start: u32,
	end: u32,
	ok: bool,
) {
	pg, err := pager.get_page(p, skip_root)
	if err != .None {
		return 0, 0, false
	}
	defer pager.unpin_page(p, skip_root)

	data := pg.data
	if len(data) < 12 {
		return 0, 0, false
	}

	magic := endian.unchecked_get_u32le(data[0:4])
	if magic != SKIP_FORMAT_MAGIC {
		return 0, 0, false
	}

	count := int(endian.unchecked_get_u32le(data[4:8]))
	if int(endian.unchecked_get_u32le(data[8:12])) != col_index {
		return 0, 0, false
	}

	need := 12 + count * size_of(Skip_Entry)
	if len(data) < need {
		return 0, 0, false
	}

	entries := transmute([]Skip_Entry)data[12:need]
	lo, hi := 0, count - 1
	for lo <= hi {
		mid := (lo + hi) / 2
		if entries[mid].min_int <= val {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	#partial switch op {
	case .EQ:
		if hi < 0 || entries[hi].max_int < val {
			return 0, 0, false
		}
		return entries[hi].page_min, entries[hi].page_max, true
	case .LT:
		h := hi
		for h >= 0 && entries[h].min_int >= val {
			h -= 1
		}
		if h < 0 {
			return 0, 0, false
		}
		return 0, entries[h].page_max, true
	case .LTE:
		if hi < 0 {
			return 0, 0, false
		}
		return 0, entries[hi].page_max, true
	case .GT:
		for i in 0 ..< count {
			if entries[i].max_int > val {
				return entries[i].page_min, 0, true
			}
		}
		return 0, 0, false
	case .GTE:
		for i in 0 ..< count {
			if entries[i].max_int >= val {
				return entries[i].page_min, 0, true
			}
		}
		return 0, 0, false
	}
	return 0, 0, false
}
