package btree

import "src:cell"
import "src:pager"
import "src:utils"

Cursor_Stack_Item :: struct {
	page_id:    u32,
	cell_index: u16,
}

Cursor :: struct {
	tree:     ^Tree,
	path:     [32]Cursor_Stack_Item,
	depth:    u8,
	is_valid: bool,
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
			child, ok := utils.read_u32_be(node.data, int(ptrs[0]))
			if !ok { return .Invalid_Cell_Pointer }
			curr = child
		} else {
			curr = get_right_ptr(node.data, curr)
		}
	}
	return .None
}

cursor_destroy :: proc(c: ^Cursor) {  }

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

cursor_advance :: proc(c: ^Cursor) -> Error {
	if !c.is_valid || c.depth == 0 {
		return .None
	}
	for c.depth > 0 {
		top_idx := c.depth - 1
		item := &c.path[top_idx]
		node, err := load_node(c.tree, item.page_id)
		if err != .None {
			return err
		}
		defer pager.unpin_page(c.tree.pager, node.id)

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
					child_page, _ = utils.read_u32_be(node.data, int(cell_ptr))
				}
				return drill_down_leftmost(c, child_page)
			}
			c.depth -= 1
		}
	}
	c.is_valid = false
	return .None
}

cursor_get_cell :: proc(c: ^Cursor, allocator := context.allocator) -> (cell.Cell, Error) {
	if !c.is_valid || c.depth == 0 {
		return {}, .Cell_Not_Found
	}

	item := c.path[c.depth - 1]
	node, err := load_node(c.tree, item.page_id)
	if err != .None {
		return {}, err
	}
	defer pager.unpin_page(c.tree.pager, node.id)

	if !is_leaf(node) {
		return {}, .Invalid_Page_Header
	}

	pointers := get_pointers(node.data, item.page_id)
	if int(item.cell_index) >= len(pointers) {
		return {}, .Cell_Not_Found
	}

	cell_ptr := pointers[item.cell_index]
	actual_alloc := allocator
	if actual_alloc.procedure == nil {
		actual_alloc = context.allocator
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
