package btree

import "core:encoding/endian"
import "core:sort"
import "src:cell"
import "src:pager"
import "src:types"

SKIP_FORMAT_MAGIC :: u32(0x4B495053)

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

	has_dir := t.pager.page_int_ranges != nil
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
			if r, cached := t.pager.page_int_ranges[page_id];
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
				t.pager.page_int_ranges[page_id] = pager.Page_Int_Range {
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
	data := page.data[:8 + n * size_of(Skip_Entry)]
	endian.unchecked_put_u32le(data[0:4], SKIP_FORMAT_MAGIC)
	endian.unchecked_put_u32le(data[4:8], u32(n))
	dst := data[8:]
	src := transmute([]byte)entries[:]
	copy(dst, src)

	pager.mark_dirty(t.pager, page.page_num)
	pager.unpin_page(t.pager, page.page_num)
	return Skip_Index{root = page.page_num}, .None
}

query_skip_index :: proc(
	p: ^pager.Pager,
	skip_root: u32,
	val: i64,
) -> (
	page_min: u32,
	page_max: u32,
	ok: bool,
) {
	pg, err := pager.get_page(p, skip_root)
	if err != .None { return 0, 0, false }
	defer pager.unpin_page(p, skip_root)

	data := pg.data
	if len(data) < 8 { return 0, 0, false }

	magic := endian.unchecked_get_u32le(data[0:4])
	if magic == SKIP_FORMAT_MAGIC {
		count := int(endian.unchecked_get_u32le(data[4:8]))
		need := 8 + count * size_of(Skip_Entry)
		if len(data) < need { return 0, 0, false }

		entries := transmute([]Skip_Entry)data[8:need]
		lo, hi := 0, count - 1
		for lo <= hi {
			mid := (lo + hi) / 2
			if entries[mid].min_int <= val { lo = mid + 1 } else { hi = mid - 1 }
		}
		if hi >= 0 && hi < count && entries[hi].min_int <= val {
			return entries[hi].page_min, entries[hi].page_max, true
		}
		return 0, 0, false
	}

	skip_tree := Tree {
		pager = p,
		root  = skip_root,
	}

	dk := Descend_Key_Ctx {
		key    = types.Row_ID(val),
		layout = get_layout(p.page_format_version),
	}

	leaf, err2 := descend_to_leaf(&skip_tree, descend_by_key, &dk)
	if err2 != .None { return 0, 0, false }
	defer unpin_node(&skip_tree, leaf)

	cell_count := get_cell_count(leaf.data, leaf.id)
	idx, lb_ok := leaf_lower_bound(leaf.data, leaf.id, types.Row_ID(val), leaf.layout)
	if !lb_ok || cell_count == 0 { return 0, 0, false }

	rid: types.Row_ID
	rid_ok := false
	if idx < cell_count {
		ptr := get_cell_ptr(leaf.data, leaf.id, idx, leaf.layout.stride)
		rid, rid_ok = cell.get_rowid(leaf.data, int(ptr))
	}

	read_idx := idx if rid_ok && rid == types.Row_ID(val) else idx - 1
	if read_idx < 0 || read_idx >= cell_count { return 0, 0, false }

	ptr := get_cell_ptr(leaf.data, leaf.id, read_idx, leaf.layout.stride)
	c, _, des_ok := cell.deserialize(
		leaf.data,
		int(ptr),
		cell.Config{allocator = context.temp_allocator, zero_copy = skip_tree.config.zero_copy},
	)

	defer cell.destroy(&c, context.temp_allocator)
	if !des_ok || len(c.values) < 2 { return 0, 0, false }

	pmin, pmin_ok := c.values[0].(i64)
	pmax, pmax_ok := c.values[1].(i64)
	if pmin_ok && pmax_ok {
		return u32(pmin), u32(pmax), true
	}
	return 0, 0, false
}
