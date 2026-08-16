package btree

import "core:encoding/endian"
import "core:sort"
import "src:cell"
import "src:pager"

SKIP_FORMAT_MAGIC :: u32(0x4B495054)

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
	min_int:  i64,
	max_int:  i64,
}

Skip_Index :: struct {
	root: u32,
}

build_skip_index :: proc(t: ^Tree, col_index: int) -> (Skip_Index, Error) {
	entries := make([dynamic]Skip_Entry, context.temp_allocator)
	defer delete(entries)

	cursor, c_err := cursor_start(t, context.temp_allocator)
	if c_err != .None { return {}, c_err }
	defer cursor_destroy(&cursor)

	has_dir := tree_stats(t).page_int_ranges != nil
	current_entry: Skip_Entry
	entry_active := false
	for cursor.is_valid {
		item := cursor.path[cursor.depth - 1]
		page_id := item.page_id
		node, n_err := load_node(t, page_id)
		if n_err != .None { return {}, n_err }
		if !is_leaf(node) {
			unpin_node(t, node)
			cursor_advance(&cursor); continue
		}

		cell_count := get_cell_count(node.data, page_id)
		if cell_count == 0 {
			unpin_node(t, node)
			cursor_advance(&cursor)
			continue
		}

		min_val := max(i64)
		max_val := min(i64)
		if has_dir {
			if r, cached := tree_stats(t).page_int_ranges[page_id];
			   cached && int(r.col_index) == col_index {
				min_val = r.min_int
				max_val = r.max_int
			}
		}
		if min_val > max_val {
			for i in 0 ..< cell_count {
				if col_index == -1 { break }

				ptr := get_cell_ptr(node.data, page_id, i, node.layout.stride)
				c, _, ok := cell.deserialize(node.data, int(ptr), cell.Config{zero_copy = true})

				if !ok { continue }
				if col_index < len(c.values) {
					if v, is_int := c.values[col_index].(i64); is_int {
						if v < min_val { min_val = v }
						if v > max_val { max_val = v }
					}
				}
				cell.destroy(&c)
			}
			if min_val <= max_val && has_dir {
				tree_stats(t).page_int_ranges[page_id] = pager.Page_Int_Range {
					col_index = u8(col_index),
					min_int   = min_val,
					max_int   = max_val,
				}
			}
		}

		unpin_node(t, node)
		if min_val <= max_val {
			if entry_active && current_entry.max_int + 1 >= min_val {
				current_entry.page_max = page_id
				if max_val > current_entry.max_int { current_entry.max_int = max_val }
			} else {
				if entry_active { append(&entries, current_entry) }
				current_entry = Skip_Entry {
					page_min = page_id,
					page_max = page_id,
					min_int  = min_val,
					max_int  = max_val,
				}
				entry_active = true
			}
		}
		cursor_advance(&cursor)
	}
	if entry_active { append(&entries, current_entry) }
	if len(entries) == 0 { return {}, .None }

	sort.quick_sort_proc(entries[:], proc(a, b: Skip_Entry) -> int {
		if a.min_int < b.min_int { return -1 }
		if a.min_int > b.min_int { return 1 }
		return 0
	})

	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None { return {}, .Page_Full }

	n := len(entries)
	// Header: [magic u32][count u32][col_index u32] then packed Skip_Entry rows.
	data := page.data[:12 + n * size_of(Skip_Entry)]
	endian.unchecked_put_u32le(data[0:4], SKIP_FORMAT_MAGIC)
	endian.unchecked_put_u32le(data[4:8], u32(n))
	endian.unchecked_put_u32le(data[8:12], u32(col_index))
	dst := data[12:]
	src := transmute([]byte)entries[:]
	copy(dst, src)

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
	if err != .None { return 0, 0, false }
	defer pager.unpin_page(p, skip_root)

	data := pg.data
	if len(data) < 12 { return 0, 0, false }
	magic := endian.unchecked_get_u32le(data[0:4])
	if magic != SKIP_FORMAT_MAGIC { return 0, 0, false }

	count := int(endian.unchecked_get_u32le(data[4:8]))
	if int(endian.unchecked_get_u32le(data[8:12])) != col_index { return 0, 0, false }
	need := 12 + count * size_of(Skip_Entry)
	if len(data) < need { return 0, 0, false }

	entries := transmute([]Skip_Entry)data[12:need]
	// hi = last entry with min_int <= val (entries are sorted by min_int).
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
		if hi < 0 || entries[hi].max_int < val { return 0, 0, false }
		return entries[hi].page_min, entries[hi].page_max, true
	case .LT:
		h := hi
		for h >= 0 && entries[h].min_int >= val { h -= 1 }
		if h < 0 { return 0, 0, false }
		return 0, entries[h].page_max, true
	case .LTE:
		if hi < 0 { return 0, 0, false }
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
