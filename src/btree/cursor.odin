package btree

import "core:encoding/endian"
import "src:cell"
import "src:pager"

Cursor_Stack_Item :: struct {
	page_id:    u32,
	cell_index: u16,
}

Cursor :: struct {
	tree:              ^Tree,
	path:              [32]Cursor_Stack_Item, // fixed-size stack; no heap alloc
	depth:             u8,
	is_valid:          bool,
	// Cached leaf page info — avoids pager lookup across adjacent cell reads
	cached_page_id:    u32,
	cached_page_data:  []u8,
	cached_cell_count: u16,
	cached_is_leaf:    bool,
}

drill_down_leftmost :: proc(c: ^Cursor, start_page: u32) -> Error {
	curr := start_page
	for {
		c.path[c.depth] = Cursor_Stack_Item {
			page_id    = curr,
			cell_index = 0,
		}

		c.depth += 1
		node, err := load_node(c.tree, curr)
		if err != .None {
			return err
		}
		defer pager.unpin_page(c.tree.pager, node.id)

		if is_leaf(node) { break }
		if node.header.cell_count > 0 {
			ptrs := get_pointers(node.data, curr)
			child, ok := endian.get_u32(node.data[int(ptrs[0]):], .Big)
			if !ok { return .Invalid_Cell_Pointer }
			curr = child
		} else {
			curr = get_right_ptr(node.data, curr)
		}
	}
	return .None
}

cursor_destroy :: proc(c: ^Cursor) {
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}
}

// Initialize a cursor for in-order traversal starting at the leftmost leaf.
// Returns an invalid cursor if the tree is empty.
cursor_start :: proc(t: ^Tree, allocator := context.allocator) -> (Cursor, Error) {
	c := Cursor {
		tree     = t,
		is_valid = true,
	}

	err := drill_down_leftmost(&c, t.root)
	if err != .None {
		return Cursor{}, err
	}
	if c.depth > 0 {
		top := c.path[c.depth - 1]
		node, e := load_node(t, top.page_id)
		if e != .None {
			c.is_valid = false
		} else {
			defer pager.unpin_page(t.pager, node.id)
			if node.header.cell_count == 0 {
				c.is_valid = false
			}
		}
	} else {
		c.is_valid = false
	}
	return c, .None
}

// Loads a node, caching the page in the cursor to avoid repeated loads.
// The page stays pinned until the cursor moves to a different page or is destroyed.
load_cached_page :: proc(c: ^Cursor, page_id: u32) -> (Node, Error) {
	if page_id == c.cached_page_id {
		return node_from_bytes(page_id, c.cached_page_data)
	}
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}

	page, err := pager.get_page(c.tree.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }

	c.cached_page_id = page_id
	c.cached_page_data = page.data
	n, n_err := node_from_bytes(page_id, page.data)
	if n_err != .None { return {}, n_err }

	c.cached_cell_count = u16(n.header.cell_count)
	c.cached_is_leaf = is_leaf(n)
	return n, .None
}

// Move the cursor to the next cell in in-order. Sets is_valid=false at end of tree.
cursor_advance :: proc(c: ^Cursor) -> Error {
	if !c.is_valid || c.depth == 0 {
		return .None
	}

	// Leaf fast path: use cached cell count to avoid load_cached_page
	top_idx := c.depth - 1
	item := &c.path[top_idx]
	if c.cached_is_leaf {
		item.cell_index += 1
		if int(item.cell_index) < int(c.cached_cell_count) {
			return .None
		}

		c.depth -= 1
		if c.depth == 0 { c.is_valid = false; return .None }
		// Fall through to walk up the stack
	}
	for c.depth > 0 {
		top_idx = c.depth - 1
		item = &c.path[top_idx]
		node, err := load_cached_page(c, item.page_id)
		if err != .None { return err }

		item.cell_index += 1
		limit := int(node.header.cell_count)
		if is_leaf(node) {
			if int(item.cell_index) < limit {
				return .None
			}
			c.depth -= 1
		} else {
			if int(item.cell_index) <= limit {
				child_page: u32
				if int(item.cell_index) == limit {
					child_page = get_right_ptr(node.data, item.page_id)
				} else {
					ptrs := get_pointers(node.data, item.page_id)
					cell_ptr := ptrs[item.cell_index]
					child_page, _ = endian.get_u32(node.data[int(cell_ptr):], .Big)
				}
				return drill_down_leftmost(c, child_page)
			}
			c.depth -= 1
		}
	}
	c.is_valid = false
	return .None
}

// Deserialize and return the cell at the current cursor position.
// Values are allocated per the allocator. Zero-copy mode returns string/blob pointing into the page.
cursor_get_cell :: proc(c: ^Cursor, allocator := context.allocator) -> (cell.Cell, Error) {
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

	// Columnar page: read individual row by index
	if is_columnar(node.data, item.page_id) {
		num_cols, found := detect_columnar_col_count(node.data, item.page_id)
		if !found || int(item.cell_index) < 0 { return {}, .Cell_Not_Found }
		
		boff := get_page_header_offset(item.page_id)
		cc, cc_ok := cell.read_columnar_cell(
			node.data,
			num_cols,
			int(item.cell_index),
			cell.Config{allocator = actual_alloc, zero_copy = c.tree.config.zero_copy},
			boff,
		)
		if !cc_ok { return {}, .Cell_Deserialize_Failed }
		return cc, .None
	}

	pointers := get_pointers(node.data, item.page_id)
	if int(item.cell_index) >= len(pointers) {
		return {}, .Cell_Not_Found
	}

	cell_ptr := pointers[item.cell_index]
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
