package btree

import "src:cell"
import "src:pager"
import "src:types"


Skip_Entry :: struct #packed {
	col_index: u8,
	page_min:  u32,
	page_max:  u32,
	min_int:   i64,
	max_int:   i64,
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

		ptrs := get_pointers(node.data, page_id)
		if len(ptrs) == 0 {
			unpin_node(t, node)
			cursor_advance(&cursor)
			continue
		}

		min_val := max(i64)
		max_val := min(i64)
		for ptr in ptrs {
			if col_index == -1 { break }

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

		unpin_node(t, node)
		if min_val <= max_val {
			if entry_active && current_entry.max_int + 1 >= min_val {
				current_entry.page_max = page_id
				if max_val > current_entry.max_int { current_entry.max_int = max_val }
			} else {
				if entry_active { append(&entries, current_entry) }
				current_entry = Skip_Entry {
					col_index = u8(col_index),
					page_min  = page_id,
					page_max  = page_id,
					min_int   = min_val,
					max_int   = max_val,
				}
				entry_active = true
			}
		}
		cursor_advance(&cursor)
	}
	if entry_active { append(&entries, current_entry) }

	root_page, a_err := pager.allocate_page(t.pager)
	if a_err != .None { return {}, .Page_Full }

	_, r_err := node_from_bytes(root_page.page_num, root_page.data)
	if r_err != .None { return {}, r_err }

	btree := Tree {
		pager = t.pager,
		root  = root_page.page_num,
	}
	for e in entries {
		data := []types.Value{types.value_int(i64(e.page_min)), types.value_int(i64(e.page_max))}
		tree_insert(&btree, types.Row_ID(e.min_int), data)
	}

	pager.mark_dirty(t.pager, root_page.page_num)
	pager.unpin_page(t.pager, root_page.page_num)
	return Skip_Index{root = root_page.page_num}, .None
}

query_skip_index :: proc(skip_tree: ^Tree, val: i64) -> (page_min: u32, page_max: u32, ok: bool) {
	key := types.Row_ID(val)
	leaf, err := descend_to_leaf(skip_tree, descend_by_key, &key)
	if err != .None { return 0, 0, false }
	defer unpin_node(skip_tree, leaf)

	pointers := get_pointers(leaf.data, leaf.id)
	idx, lb_ok := leaf_lower_bound(leaf.data, leaf.id, key)
	if !lb_ok || len(pointers) == 0 { return 0, 0, false }

	// Prefer exact match (idx < len and rid == val).
	// Otherwise use predecessor at idx-1 (largest entry < val).
	rid: types.Row_ID
	rid_ok := false
	if idx < len(pointers) {
		rid, rid_ok = cell.get_rowid(leaf.data, int(pointers[idx]))
	}

	read_idx := idx if rid_ok && rid == key else idx - 1
	if read_idx < 0 || read_idx >= len(pointers) { return 0, 0, false }

	c, _, des_ok := cell.deserialize(
		leaf.data,
		int(pointers[read_idx]),
		cell.Config{allocator = context.temp_allocator, zero_copy = skip_tree.config.zero_copy},
	)
	if !des_ok { return 0, 0, false }
	if len(c.values) >= 2 {
		pmin, pmin_ok := c.values[0].(i64)
		pmax, pmax_ok := c.values[1].(i64)
		if pmin_ok && pmax_ok {
			return u32(pmin), u32(pmax), true
		}
	}
	return 0, 0, false
}
