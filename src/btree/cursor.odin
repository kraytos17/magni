package btree

import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:pager"

Cursor_Stack_Item :: struct {
	page_id:    u32,
	cell_index: u16,
}

Cursor :: struct {
	tree:              ^Tree,
	path:              [MAX_TREE_DEPTH]Cursor_Stack_Item,
	depth:             u8,
	is_valid:          bool,
	cached_page_id:    u32,
	cached_page_data:  []u8,
	cached_cell_count: u16,
	cached_is_leaf:    bool,
	col_num_cols:      u8, // >0 when on a columnar page; caches the column count
	col_rowid:         u64, // accumulated rowid at the current cell_index on a columnar page
	col_rowid_pos:     int, // byte position in the rowid region for the current row
}

drill_down_leftmost :: proc(c: ^Cursor, start_page: u32) -> Error {
	curr := start_page
	for {
		if int(c.depth) >= MAX_TREE_DEPTH { return .Invalid_Page_Header }
		c.path[c.depth] = Cursor_Stack_Item {
			page_id    = curr,
			cell_index = 0,
		}

		c.depth += 1
		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)

		if is_leaf(node) { break }
		if node.header.cell_count > 0 {
			stride := node.layout.stride
			ptr := get_cell_ptr(node.data, curr, 0, stride)
			child, ok := endian.get_u32(node.data[int(ptr):], .Big)
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
cursor_start :: proc(t: ^Tree, allocator := context.allocator) -> (c: Cursor, err: Error) {
	c = Cursor {
		tree     = t,
		is_valid = true,
	}

	drill_down_leftmost(&c, t.root) or_return
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
	return
}

// cursor_start_at_page positions a cursor at the first cell of the leaf page
// `page_id`, building the full root→leaf path so traversal can continue past
// the leaf. Used to start a scan at a skip-index lower bound.
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
cursor_seek_to_page :: proc(c: ^Cursor, page_id: u32) -> Error {
	c.depth = 0
	c.is_valid = false
	c.cached_page_id = 0
	c.cached_page_data = nil
	c.cached_cell_count = 0
	c.cached_is_leaf = false
	c.col_num_cols = 0
	c.col_rowid = 0
	c.col_rowid_pos = 0

	curr := c.tree.root
	for {
		if int(c.depth) >= MAX_TREE_DEPTH { return .Invalid_Page_Header }

		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)
		if is_leaf(node) {
			if curr != page_id { return .Cell_Not_Found }
			c.path[c.depth] = Cursor_Stack_Item {page_id = curr, cell_index = 0}
			c.depth += 1
			c.is_valid = true
			return .None
		}

		cell_count := get_cell_count(node.data, curr)
		idx := find_interior_cell_for_child(node.data, curr, page_id, node.layout)
		if idx >= 0 {
			c.path[c.depth] = Cursor_Stack_Item {page_id = curr, cell_index = u16(idx)}
			c.depth += 1
			ptr := get_cell_ptr(node.data, curr, idx, node.layout.stride)
			child, ok := endian.get_u32(node.data[int(ptr):], .Big)
			if !ok { return .Invalid_Cell_Pointer }
			curr = child
		} else if get_right_ptr(node.data, curr) == page_id {
			// Target lies in the rightmost subtree: mark this level as fully
			// visited (cell_index == cell_count) so advance pops past it.
			c.path[c.depth] = Cursor_Stack_Item {page_id = curr, cell_index = u16(cell_count)}
			c.depth += 1
			curr = page_id
		} else {
			return .Cell_Not_Found
		}
	}
}

// Loads a node, caching the page in the cursor to avoid repeated loads.
// The page stays pinned until the cursor moves to a different page or is destroyed.
load_cached_page :: proc(c: ^Cursor, page_id: u32) -> (Node, Error) {
	if page_id == c.cached_page_id {
		return node_from_bytes(
			page_id,
			c.cached_page_data,
			get_layout(c.tree.pager.page_format_version),
		)
	}
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}

	page, err := pager.get_page(c.tree.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }

	c.cached_page_id = page_id
	c.cached_page_data = page.data
	c.col_num_cols = 0
	c.col_rowid_pos = 0
	n, n_err := node_from_bytes(page_id, page.data, get_layout(c.tree.pager.page_format_version))

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
			if c.col_num_cols > 0 && c.col_rowid_pos > 0 {
				// Advance rowid for columnar page
				delta, n, ok := cell.varint_decode(c.cached_page_data, c.col_rowid_pos)
				if ok {
					c.col_rowid += delta
					c.col_rowid_pos += n
				}
			}
			return .None
		}

		c.col_num_cols = 0
		c.depth -= 1
		if c.depth == 0 {
			c.is_valid = false
			return .None
		}
	}
	for c.depth > 0 {
		top_idx = c.depth - 1
		item = &c.path[top_idx]
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
				child_page: u32
				if int(item.cell_index) == limit {
					child_page = get_right_ptr(node.data, item.page_id)
				} else {
					stride := node.layout.stride
					ptr := get_cell_ptr(node.data, item.page_id, int(item.cell_index), stride)
					child_page, _ = endian.get_u32(node.data[int(ptr):], .Big)
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
	// Columnar page: read individual row by index
	if is_columnar(node.data, item.page_id) {
		num_cols, found := detect_columnar_col_count(node.data, item.page_id)
		if !found || int(item.cell_index) < 0 { return {}, .Cell_Not_Found }

		c.col_num_cols = u8(num_cols)
		if c.col_rowid_pos == 0 {
			boff := get_page_header_offset(item.page_id)
			c.col_rowid_pos = boff + cell.COLUMNAR_DIR_OFFSET + num_cols * size_of(cell.Col_Header)
			c.col_rowid = 0
			for _ in 0 ..< int(item.cell_index) {
				delta, n, ok := cell.varint_decode(node.data, c.col_rowid_pos)
				if !ok { break }

				c.col_rowid += delta
				c.col_rowid_pos += n
			}
		}

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

	stride := node.layout.stride
	cell_count := get_cell_count(node.data, item.page_id)
	if int(item.cell_index) >= cell_count {
		return {}, .Cell_Not_Found
	}

	cell_ptr := get_cell_ptr(node.data, item.page_id, int(item.cell_index), stride)
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
