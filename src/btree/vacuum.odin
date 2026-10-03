package btree

import "src:cell"
import "src:pager"
import "src:types"

// tree_vacuum rebuilds the tree into fresh, densely packed pages and returns
// the new root. The original pages are left untouched (COW-safe), so
// time-travel snapshots remain readable; the old pages are reclaimed by the
// next garbage-collection pass. It is an O(n) maintenance operation intended
// for an explicit VACUUM, not for hot-path use.
tree_vacuum :: proc(t: ^Tree, allocator := context.allocator) -> (new_root: u32, err: Error) {
	handles := make([dynamic]Node_Handle, 0, 64, context.temp_allocator)
	vc := vacuum_ctx {
		t          = t,
		leaf_empty = true,
		handles    = &handles,
	}

	if e := tree_foreach(t, vacuum_collect_cb, &vc); e != .None {
		return 0, e
	}
	if vc.failed {
		return 0, .Page_Full
	}
	if !vc.leaf_empty {
		if e := vacuum_finish_leaf(&vc); e != .None {
			return 0, e
		}
	}
	if len(handles) == 0 {
		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None { return 0, .Page_Full }
		if !init_slot_leaf_page(page.data, page.page_num) {
			pager.unpin_page(t.pager, page.page_num)
			return 0, .Invalid_Page_Header
		}

		root := page.page_num
		pager.unpin_page(t.pager, root)
		return root, .None
	}

	// Build interior levels bottom-up. Each interior node holds cells for all
	// children except the last, which becomes the rightmost pointer.
	level := handles
	for len(level) > 1 {
		next := make([dynamic]Node_Handle, 0, 64, context.temp_allocator)
		i := 0
		for i < len(level) {
			// Greedy chunk: children [i..e] with separators max_keys[i..e)
			// (the last child becomes the rightmost). Capacity from the
			// chunk range via the FOR rule — arithmetic only, no trial
			// builds. Single-child chunks always fit (~40 bytes).
			e := i
			for e + 1 < len(level) {
				first := level[i].max_key
				last := level[e].max_key
				use_for, _ := dense_choose_encoding(first, last)
				kw := DENSE_DELTA_WIDTH if use_for else DENSE_FULL_KEY_WIDTH
				m_new := e + 2 - i
				if size_of(Dense_Interior_Header) + (m_new - 1) * kw + m_new * DENSE_CHILD_WIDTH > PAGE_SIZE {
					break
				}
				e += 1
			}

			ckeys := make([dynamic]types.Row_ID, 0, e - i + 1, context.temp_allocator)
			cchildren := make([dynamic]u32, 0, e - i + 2, context.temp_allocator)
			for k in i ..< e {
				append(&ckeys, level[k].max_key)
				append(&cchildren, level[k].id)
			}
			append(&cchildren, level[e].id)

			page, a_err := pager.allocate_page(t.pager)
			if a_err != .None { return 0, .Page_Full }
			if b_err := dense_build_from_sorted(
				page.data,
				Page_Id(page.page_num),
				ckeys[:],
				cchildren[:],
			); b_err != .None {
				pager.unpin_page(t.pager, page.page_num)
				return 0, b_err
			}

			max_key := level[e].max_key
			page_id := page.page_num
			pager.unpin_page(t.pager, page_id)
			append(&next, Node_Handle{id = page_id, max_key = max_key})
			i = e + 1
		}
		level = next
	}
	return level[0].id, .None
}

// Node_Handle identifies a packed node and the maximum key in its subtree,
// used while building the new interior levels of a vacuumed tree.
@(private)
Node_Handle :: struct {
	id     : u32,
	max_key: types.Row_ID,
}

// vacuum_ctx carries the bulk-loader state across tree_foreach callbacks.
@(private)
vacuum_ctx :: struct {
	t         : ^Tree,
	leaf      : Node,
	leaf_empty: bool,
	handles   : ^[dynamic]Node_Handle,
	leaf_max  : types.Row_ID,
	failed    : bool,
}

// vacuum_collect_cb serializes each row into the current packed leaf, starting
// a fresh leaf when the current one is full.
@(private)
vacuum_collect_cb :: proc(c: ^cell.Cell, ud: rawptr) -> bool {
	vc := cast(^vacuum_ctx)ud
	if vc.failed { return false }
	if vc.leaf_empty {
		if v_err := vacuum_start_leaf(vc); v_err != .None {
			vc.failed = true
			return false
		}
	}
	if e := node_insert_leaf_cell(vc.t, &vc.leaf, c.rowid, c.values); e == .Page_Full {
		if f_err := vacuum_finish_leaf(vc); f_err != .None {
			vc.failed = true
			return false
		}
		if s_err := vacuum_start_leaf(vc); s_err != .None {
			vc.failed = true
			return false
		}
		if r_err := node_insert_leaf_cell(vc.t, &vc.leaf, c.rowid, c.values); r_err != .None {
			vc.failed = true
			return false
		}
	} else if e != .None {
		vc.failed = true
		return false
	}

	vc.leaf_max = c.rowid
	return true
}

@(private)
vacuum_start_leaf :: proc(vc: ^vacuum_ctx) -> Error {
	page, a_err := pager.allocate_page(vc.t.pager)
	if a_err != .None { return .Page_Full }
	if !init_slot_leaf_page(page.data, page.page_num) {
		pager.unpin_page(vc.t.pager, page.page_num)
		return .Invalid_Page_Header
	}

	leaf_layout, _, l_err := layout_for_page(page.data, Page_Id(page.page_num))
	if l_err != .None {
		pager.unpin_page(vc.t.pager, page.page_num)
		return l_err
	}

	n, n_err := node_from_bytes(page.page_num, page.data, leaf_layout)
	if n_err != .None {
		pager.unpin_page(vc.t.pager, page.page_num)
		return n_err
	}

	vc.leaf = n
	vc.leaf_empty = false
	return .None
}

@(private)
vacuum_finish_leaf :: proc(vc: ^vacuum_ctx) -> Error {
	if vc.leaf_empty { return .None }

	append(vc.handles, Node_Handle{id = vc.leaf.id, max_key = vc.leaf_max})
	pager.unpin_page(vc.t.pager, vc.leaf.id)
	vc.leaf_empty = true
	return .None
}
