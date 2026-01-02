package btree

import "core:mem"
import "src:cell"
import "src:utils"

Cursor_Stack_Item :: struct {
	page_id:    u32,
	cell_index: int,
}

Cursor :: struct {
	tree:      ^Tree,
	path:      [dynamic]Cursor_Stack_Item,
	is_valid:  bool,
	allocator: mem.Allocator,
}

drill_down_leftmost :: proc(c: ^Cursor, start_page: u32) -> Error {
	curr := start_page
	for {
		append(&c.path, Cursor_Stack_Item{page_id = curr, cell_index = 0})
		node, err := load_node(c.tree, curr)
		if err != .None {
			return err
		}
		if is_leaf(node) {
			break
		}
		if node.header.cell_count > 0 {
			ptrs := get_pointers(node.data, curr)
			child, _ := utils.read_u32_be(node.data, int(ptrs[0]))
			curr = child
		} else {
			curr = get_right_ptr(node.data, curr)
		}
	}
	return .None
}

cursor_destroy :: proc(c: ^Cursor) {
	delete(c.path)
}

cursor_start :: proc(t: ^Tree, allocator := context.allocator) -> (Cursor, Error) {
	c := Cursor {
		tree      = t,
		path      = make([dynamic]Cursor_Stack_Item, allocator),
		is_valid  = true,
		allocator = allocator,
	}

	err := drill_down_leftmost(&c, t.root)
	if err != .None {
		cursor_destroy(&c)
		return Cursor{}, err
	}
	if len(c.path) > 0 {
		top := c.path[len(c.path) - 1]
		node, _ := load_node(t, top.page_id)
		if node.header.cell_count == 0 {
			c.is_valid = false
		}
	} else {
		c.is_valid = false
	}
	return c, .None
}

cursor_advance :: proc(c: ^Cursor) -> Error {
	if !c.is_valid || len(c.path) == 0 {
		return .None
	}

	for len(c.path) > 0 {
		top_idx := len(c.path) - 1
		item := &c.path[top_idx]
		node, err := load_node(c.tree, item.page_id)
		if err != .None {
			return err
		}

		item.cell_index += 1
		limit := int(node.header.cell_count)
		if is_leaf(node) {
			if item.cell_index < limit {
				return .None
			}
			pop(&c.path)
		} else {
			if item.cell_index <= limit {
				child_page: u32
				if item.cell_index == limit {
					child_page = get_right_ptr(node.data, item.page_id)
				} else {
					ptrs := get_pointers(node.data, item.page_id)
					cell_ptr := ptrs[item.cell_index]
					child_page, _ = utils.read_u32_be(node.data, int(cell_ptr))
				}
				return drill_down_leftmost(c, child_page)
			}
			pop(&c.path)
		}
	}
	c.is_valid = false
	return .None
}

cursor_get_cell :: proc(c: ^Cursor, allocator := context.allocator) -> (cell.Cell, Error) {
	if !c.is_valid || len(c.path) == 0 {
		return {}, .Cell_Not_Found
	}

	item := c.path[len(c.path) - 1]
	node, err := load_node(c.tree, item.page_id)
	if err != .None {
		return {}, err
	}
	if !is_leaf(node) {
		return {}, .Invalid_Page_Header
	}

	pointers := get_pointers(node.data, item.page_id)
	if item.cell_index >= len(pointers) {
		return {}, .Cell_Not_Found
	}

	cell_ptr := pointers[item.cell_index]
	actual_alloc := allocator
	if actual_alloc.procedure == nil {
		actual_alloc = c.allocator
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
