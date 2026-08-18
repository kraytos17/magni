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
	layout := get_layout(t.pager.page_format_version)
	handles := make([dynamic]Node_Handle, 0, 64, context.temp_allocator)
	vc := vacuum_ctx {
		t          = t,
		layout     = layout,
		leaf_empty = true,
		handles    = &handles,
	}

	if e := tree_foreach(t, vacuum_collect_cb, &vc); e != .None {
		return 0, e
	}
	if vc.failed { return 0, .Page_Full }
	if !vc.leaf_empty {
		if e := vacuum_finish_leaf(&vc); e != .None { return 0, e }
	}
	if len(handles) == 0 {
		// Empty tree: a single fresh empty leaf is the new root.
		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None { return 0, .Page_Full }

		init_leaf_page(page.data, page.page_num)
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
			page, a_err := pager.allocate_page(t.pager)
			if a_err != .None { return 0, .Page_Full }

			init_interior_page(page.data, page.page_num)
			j := i
			for j < len(level) - 1 {
				if !insert_interior_cell(page.data, page.page_num, level[j].id, level[j].max_key, layout) {
					break
				}
				j += 1
			}

			set_right_ptr(page.data, page.page_num, level[j].id)
			max_key := level[j].max_key
			page_id := page.page_num
			pager.unpin_page(t.pager, page_id)
			append(&next, Node_Handle {id = page_id, max_key = max_key})
			i = j + 1
		}
		level = next
	}
	return level[0].id, .None
}

// Node_Handle identifies a packed node and the maximum key in its subtree,
// used while building the new interior levels of a vacuumed tree.
@(private)
Node_Handle :: struct {
	id:      u32,
	max_key: types.Row_ID,
}

// vacuum_ctx carries the bulk-loader state across tree_foreach callbacks.
@(private)
vacuum_ctx :: struct {
	t:          ^Tree,
	layout:     ^Cell_Layout,
	leaf:       Node,
	leaf_empty: bool,
	handles:    ^[dynamic]Node_Handle,
	leaf_max:   types.Row_ID,
	failed:     bool,
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

	init_leaf_page(page.data, page.page_num)
	n, n_err := node_from_bytes(page.page_num, page.data, vc.layout)
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

	append(vc.handles, Node_Handle {id = vc.leaf.id, max_key = vc.leaf_max})
	pager.unpin_page(vc.t.pager, vc.leaf.id)
	vc.leaf_empty = true
	return .None
}
