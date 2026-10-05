// Package btree — in-order cursor over the leaf chain.
//
// A cursor materializes one root→leaf path plus a page cache (one pin, one
// resolved layout per visited page). Traversal is leaf-local until the leaf
// is exhausted, then walks up to the next sibling and drills back down.
// Borrowed cell bytes (TEXT/BLOB) stay valid only while the cursor holds
// the page; clone before advancing.
package btree

import "base:intrinsics"
import "core:mem"
import "src:cell"
import "src:pager"
import "src:types"

// Cursor_Stack_Item is one level of the cursor's root→leaf path: the page
// and the child/cell index taken at that level.
Cursor_Stack_Item :: struct {
	page_id   : u32,
	cell_index: u16,
}

// Cursor is a single-threaded in-order iterator. Zero value is invalid;
// start with cursor_start. Destroy with cursor_destroy to release the pin.
Cursor :: struct {
	tree             : ^Tree,
	path             : [MAX_TREE_DEPTH]Cursor_Stack_Item,
	depth            : u8,
	is_valid         : bool,
	cached_page_id   : u32,
	cached_page_data : []u8,
	cached_cell_count: u16,
	cached_is_leaf   : bool,
	// Resolved layout for the cached page (one interface hop per page
	// visit, not per cell). Set on every cache fill; the hit path reuses
	// it without re-resolving.
	cached_layout    : Page_Layout,
}

// drill_down_leftmost pushes a path from start_page down the leftmost spine,
// leaving the cursor on the first leaf. Fails when the tree is deeper than
// MAX_TREE_DEPTH or a child pointer is unreadable.
@(private = "file")
drill_down_leftmost :: proc(c: ^Cursor, start_page: u32) -> Error {
	curr := start_page
	for {
		if int(c.depth) >= MAX_TREE_DEPTH {
			return .Invalid_Page_Header
		}

		c.path[c.depth] = Cursor_Stack_Item {
			page_id    = curr,
			cell_index = 0,
		}

		c.depth += 1
		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)

		if is_leaf(node) {
			break
		}

		child, c_err := node.layout.vtable.child_at(node.data, Page_Id(curr), 0)
		if c_err != .None {
			return .Invalid_Cell_Pointer
		}
		curr = child
	}
	return .None
}

// cursor_destroy releases the cursor's pin on its cached page.
cursor_destroy :: proc(c: ^Cursor) {
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}
}

// cursor_start initializes an in-order traversal at the leftmost leaf.
// Returns an invalid cursor when the tree is empty. Empty leaves (possible
// after deletes empty a leading leaf) are skipped so the cursor lands on the
// first cell-bearing leaf.
cursor_start :: proc(t: ^Tree, allocator := context.allocator) -> (c: Cursor, err: Error) {
	c = Cursor {
		tree     = t,
		is_valid = true,
	}

	drill_down_leftmost(&c, t.root) or_return
	if c.depth > 0 {
		for c.is_valid {
			top := c.path[c.depth - 1]
			node, e := load_node(t, top.page_id)
			if e != .None {
				c.is_valid = false
				break
			}

			non_empty := is_leaf(node) && node.header.cell_count > 0
			pager.unpin_page(t.pager, node.id)
			if non_empty {
				break
			}
			if a_err := cursor_advance(&c); a_err != .None {
				c.is_valid = false
				break
			}
		}
	} else {
		c.is_valid = false
	}
	return
}

// cursor_start_at_page positions a cursor at the first cell of the leaf page
// `page_id`, building the full root→leaf path so traversal can continue past
// the leaf. Entry point for scans starting at a skip-index lower bound.
@(private = "file")
cursor_start_at_page :: proc(
	t: ^Tree,
	page_id: u32,
	allocator := context.allocator,
) -> (
	c: Cursor,
	err: Error,
) {
	c.tree = t
	cursor_seek_to_page(&c, page_id) or_return
	return c, .None
}

// cursor_seek_to_page descends from the root to the leaf `page_id`, pushing
// the ancestor chain onto the path stack (interior cell indices included) so
// cursor_advance can leave the leaf correctly. Returns .Page_Not_Found when
// page_id is not reachable as a leaf (e.g. a stale skip-index page); callers
// should fall back to a full scan in that case.
// require_results: an unhandled seek error leaves the cursor invalid while
// the caller scans from nowhere — always check.
@(require_results)
cursor_seek_to_page :: proc(c: ^Cursor, page_id: u32) -> Error {
	c.depth = 0
	c.is_valid = false
	c.cached_page_id = 0
	c.cached_page_data = nil
	c.cached_cell_count = 0
	c.cached_is_leaf = false
	c.cached_layout = {}

	curr := c.tree.root
	for {
		if int(c.depth) >= MAX_TREE_DEPTH {
			return .Invalid_Page_Header
		}

		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)
		if is_leaf(node) {
			if curr != page_id {
				return .Cell_Not_Found
			}

			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = 0,
			}

			c.depth += 1
			c.is_valid = true
			return .None
		}

		cell_count := get_cell_count(node.data, curr)
		nid := Page_Id(curr)
		idx := find_interior_cell_for_child(node.data, curr, page_id, node.layout)
		if idx >= 0 {
			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = u16(idx),
			}

			c.depth += 1
			child, c_err := node.layout.vtable.child_at(node.data, nid, idx)
			if c_err != .None {
				return .Invalid_Cell_Pointer
			}
			curr = child
		} else {
			right, r_err := node.layout.vtable.child_at(node.data, nid, cell_count)
			if r_err != .None {
				return .Invalid_Cell_Pointer
			}
			if right != page_id {
				return .Cell_Not_Found
			}

			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = u16(cell_count),
			}

			c.depth += 1
			curr = page_id
		}
	}
}

// load_cached_page returns the node for page_id, reusing the cursor's cached
// page when it matches (hot path: sequential scans hit it once per cell).
// The page stays pinned until the cursor moves on or is destroyed.
// require_results: using a zero Node after a failed load reads garbage.
@(private = "file", require_results)
load_cached_page :: proc(c: ^Cursor, page_id: u32) -> (Node, Error) {
	if intrinsics.likely(page_id == c.cached_page_id) {
		// Hit: reuse the cached bytes AND the resolved layout (no
		// re-resolve per cell). cached_layout is set on every fill below
		// and cleared on seek; the hit path always follows a fill.
		return node_from_bytes(page_id, c.cached_page_data, c.cached_layout)
	}
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}

	page, err := pager.get_page(c.tree.pager, page_id)
	if err != nil {
		return {}, .Page_Read_Failed
	}

	c.cached_page_id = page_id
	c.cached_page_data = page.data
	layout, _, l_err := layout_for_page(page.data, Page_Id(page_id))
	if l_err != .None {
		return {}, l_err
	}

	n, n_err := node_from_bytes(page_id, page.data, layout)
	if n_err != .None {
		return {}, n_err
	}

	c.cached_layout = layout
	c.cached_cell_count = u16(n.header.cell_count)
	c.cached_is_leaf = is_leaf(n)
	return n, .None
}

// cursor_advance moves to the next cell in in-order, setting is_valid=false
// at end of tree.
cursor_advance :: proc(c: ^Cursor) -> Error {
	if !c.is_valid || c.depth == 0 {
		return .None
	}

	// Fast path: the cached cell count avoids load_cached_page entirely.
	item := &c.path[c.depth - 1]
	if c.cached_is_leaf {
		item.cell_index += 1
		if int(item.cell_index) < int(c.cached_cell_count) {
			return .None
		}

		c.depth -= 1
		if c.depth == 0 {
			c.is_valid = false
			return .None
		}
	}
	return descend_to_next_leaf(c)
}

// descend_to_next_leaf walks up from a finished leaf/interior cell to the next
// sibling in in-order, drilling down its leftmost leaf. Sets is_valid=false at
// end of tree. Caller has already popped the finished leaf's cell_index.
@(private = "file")
descend_to_next_leaf :: proc(c: ^Cursor) -> Error {
	for c.depth > 0 {
		top_idx := c.depth - 1
		item := &c.path[top_idx]
		node := load_cached_page(c, item.page_id) or_return
		item.cell_index += 1
		limit := int(node.header.cell_count)
		if is_leaf(node) {
			if int(item.cell_index) < limit {
				return .None
			}
			c.depth -= 1
		} else {
			if int(item.cell_index) <= limit {
				child, c_err := node.layout.vtable.child_at(
					node.data,
					Page_Id(item.page_id),
					int(item.cell_index),
				)
				if c_err != .None {
					return .Invalid_Cell_Pointer
				}
				return drill_down_leftmost(c, child)
			}
			c.depth -= 1
		}
	}

	c.is_valid = false
	return .None
}

// cursor_get_cell_needed decodes the cell at the cursor position but
// materializes only the columns flagged in `needed` (index = serial
// position) into `out_values` (caller storage: stack or reused batch
// buffer). Unneeded positions are set to Null.
//
// TEXT/BLOB for needed columns are ALWAYS borrowed from the page: valid only
// while the cursor stays on the page (single-page pin, slot-buffer reuse on
// eviction) — clone survivors before advancing. Zero allocations, via
// cell.deserialize_needed.
cursor_get_cell_needed :: proc(
	c: ^Cursor,
	needed: []bool,
	out_values: []types.Value,
) -> (
	rowid: types.Row_ID,
	err: Error,
) {
	if !c.is_valid || c.depth == 0 {
		return 0, .Cell_Not_Found
	}

	item := c.path[c.depth - 1]
	node, l_err := load_cached_page(c, item.page_id)
	if l_err != .None {
		return 0, l_err
	}
	if !is_leaf(node) {
		return 0, .Invalid_Page_Header
	}

	nid := Page_Id(item.page_id)
	cell_count := get_cell_count(node.data, item.page_id)
	if int(item.cell_index) >= cell_count {
		return 0, .Cell_Not_Found
	}

	cell_ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, int(item.cell_index))
	if p_err != .None {
		return 0, .Cell_Deserialize_Failed
	}

	rid, _, ok := cell.deserialize_needed(node.data, int(cell_ptr), needed, out_values)
	if !ok {
		return 0, .Cell_Deserialize_Failed
	}
	return rid, .None
}

// cursor_get_cell deserializes the cell at the cursor position. Values are
// allocated from allocator (defaults to context.allocator); with the tree's
// zero_copy config, string/blob values point into the page and are valid
// only until the cursor advances.
cursor_get_cell :: proc(c: ^Cursor, allocator: mem.Allocator) -> (cell.Cell, Error) {
	if !c.is_valid || c.depth == 0 {
		return {}, .Cell_Not_Found
	}

	item := c.path[c.depth - 1]
	node, err := load_cached_page(c, item.page_id)
	if err != .None {
		return {}, err
	}
	if !is_leaf(node) {
		return {}, .Invalid_Page_Header
	}

	actual_alloc := allocator
	if actual_alloc.procedure == nil {
		actual_alloc = context.allocator
	}

	nid := Page_Id(item.page_id)
	cell_count := get_cell_count(node.data, item.page_id)
	if int(item.cell_index) >= cell_count {
		return {}, .Cell_Not_Found
	}

	cell_ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, int(item.cell_index))
	if p_err != .None {
		return {}, .Cell_Deserialize_Failed
	}

	cell_cfg := cell.Config {
		allocator = actual_alloc,
		zero_copy = c.tree.config.zero_copy,
	}

	res_cell, _, ok := cell.deserialize(node.data, int(cell_ptr), cell_cfg)
	if !ok {
		return {}, .Cell_Deserialize_Failed
	}
	return res_cell, .None
}
