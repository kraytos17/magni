package btree

import "core:fmt"
import "core:mem"
import "core:strings"
import "src:cell"
import "src:pager"
import "src:types"
import "src:utils"

Tree :: struct {
	pager:  ^pager.Pager,
	root:   u32,
	config: Config,
}

Config :: struct {
	allocator:        mem.Allocator,
	zero_copy:        bool, // Unsafe: strings point to page buffer
	check_duplicates: bool, // Safer but slower inserts
}

DEFAULT_CONFIG := Config {
	allocator        = {}, // Defaults to context.allocator if nil
	zero_copy        = false,
	check_duplicates = true,
}

init :: proc(p: ^pager.Pager, root_page: u32, config := DEFAULT_CONFIG) -> Tree {
	c := config
	if c.allocator.procedure == nil {
		c.allocator = context.allocator
	}
	return Tree{pager = p, root = root_page, config = c}
}

// Error types representing specific failure modes in B-Tree operations.
Error :: enum {
	None,
	Page_Read_Failed, // Underlying pager failed to retrieve the page
	Invalid_Page_Header, // Page data does not match expected B-Tree header format
	Invalid_Cell_Pointer, // Cell pointer offset is out of bounds
	Cell_Deserialize_Failed, // Raw bytes could not be converted to a Cell struct
	Page_Full, // Not enough contiguous space to insert new cell
	Duplicate_Rowid, // Attempted to insert a Row ID that already exists
	Cell_Not_Found, // Search completed without finding the target
	Invalid_Bounds, // Generic out-of-bounds memory access error
	Serialization_Failed, // Failed to write Cell data into the page buffer
}

Node :: struct {
	id:       u32,
	data:     []u8,
	header:   ^Page_Header,
	leaf:     ^Leaf_Header,
	interior: ^Interior_Header,
}

is_leaf :: proc(n: Node) -> bool {
	return n.header.page_type == .LEAF_TABLE
}

load_node :: proc(t: ^Tree, page_id: u32) -> (Node, Error) {
	page, err := pager.get_page(t.pager, page_id)
	if err != nil {
		return {}, .Page_Read_Failed
	}
	return node_from_bytes(page_id, page.data)
}

node_from_bytes :: proc(id: u32, data: []u8) -> (Node, Error) {
	common_hdr := get_header(data, id)
	if common_hdr == nil {
		return {}, .Invalid_Page_Header
	}

	node := Node {
		id     = id,
		data   = data,
		header = common_hdr,
	}

	if common_hdr.page_type == .LEAF_TABLE {
		node.leaf = get_leaf_header(data, id)
		if node.leaf == nil { return {}, .Invalid_Page_Header }
	} else {
		node.interior = get_interior_header(data, id)
		if node.interior == nil { return {}, .Invalid_Page_Header }
	}
	return node, .None
}

Split_Result :: struct {
	did_split:  bool,
	right_page: u32, // The new sibling page number
	split_key:  types.Row_ID, // The key separating left/right
}

@(private = "file")
node_move_leaf_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if src.leaf == nil || dst.leaf == nil { return false }

	src_ptrs := get_pointers(src.data, src.id)
	if start_idx + count > len(src_ptrs) { return false }

	for i in 0 ..< count {
		src_index := start_idx + i
		src_ptr := src_ptrs[src_index]
		cell_size, ok := cell.get_size(src.data, int(src_ptr))
		if !ok { return false }

		cell_bytes := src.data[int(src_ptr):int(src_ptr) + cell_size]
		new_offset := int(dst.header.cell_content_offset) - cell_size
		dst.header.cell_content_offset = u16le(new_offset)

		copy(dst.data[new_offset:], cell_bytes)
		header_size := page_header_size(dst.header.page_type)
		base_offset := get_page_header_offset(dst.id)
		ptr_loc := base_offset + header_size + int(dst.header.cell_count) * 2
		utils.write_u16_le(dst.data, ptr_loc, u16(new_offset))
		dst.header.cell_count += 1
	}
	return true
}

@(private = "file")
node_move_interior_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if src.interior == nil || dst.interior == nil { return false }

	ptrs := get_pointers(src.data, src.id)
	for i in 0 ..< count {
		off := ptrs[start_idx + i]
		size := interior_cell_size_from_page(src.data, int(off))
		new_off := int(dst.interior.cell_content_offset) - size
		dst.interior.cell_content_offset = u16le(new_off)

		copy(dst.data[new_off:], src.data[int(off):int(off) + size])
		hdr_sz := size_of(Interior_Header)
		base := get_page_header_offset(dst.id)
		ptr_loc := base + hdr_sz + int(dst.interior.cell_count) * 2
		utils.write_u16_le(dst.data, ptr_loc, u16(new_off))
		dst.interior.cell_count += 1
	}
	return true
}

@(private = "file")
split_leaf_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }

	init_leaf_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(new_page.page_num, new_page.data)
	total := int(curr.leaf.cell_count)
	mid := total / 2
	count_moving := total - mid
	if !node_move_leaf_cells(curr, &right_node, mid, count_moving) {
		return {}, .Serialization_Failed
	}

	temp_data, alloc_err := mem.alloc_bytes(PAGE_SIZE, mem.DEFAULT_ALIGNMENT, context.temp_allocator)
	if alloc_err != .None { return {}, .Page_Full }

	defer mem.free_bytes(temp_data, context.temp_allocator)
	init_leaf_page(temp_data, curr.id)
	temp_node, _ := node_from_bytes(curr.id, temp_data)
	if !node_move_leaf_cells(curr, &temp_node, 0, mid) {
		return {}, .Serialization_Failed
	}

	copy(curr.data, temp_data)
	ptrs := get_pointers(right_node.data, right_node.id)
	sep, ok := cell.get_rowid(right_node.data, int(ptrs[0]))
	if !ok { return {}, .Invalid_Cell_Pointer }

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, right_node.id)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

@(private = "file")
split_interior_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }

	init_interior_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(new_page.page_num, new_page.data)
	total := int(curr.interior.cell_count)
	mid := total / 2

	ptrs := get_pointers(curr.data, curr.id)
	mid_ptr := ptrs[mid]
	sep_u64, _, ok := utils.varint_decode(curr.data, int(mid_ptr) + 4)
	if !ok { return {}, .Invalid_Cell_Pointer }

	sep := types.Row_ID(sep_u64)
	child_from_mid_cell, _ := utils.read_u32_be(curr.data, int(mid_ptr))
	count_right := total - (mid + 1)
	if count_right > 0 {
		node_move_interior_cells(curr, &right_node, mid + 1, count_right)
	}

	orig_rightmost := get_right_ptr(curr.data, curr.id)
	set_right_ptr(right_node.data, right_node.id, orig_rightmost)
	temp_data, alloc_err := mem.alloc_bytes(PAGE_SIZE, mem.DEFAULT_ALIGNMENT, context.temp_allocator)
	if alloc_err != .None { return {}, .Page_Full }

	defer mem.free_bytes(temp_data, context.temp_allocator)
	init_interior_page(temp_data, curr.id)
	temp_node, _ := node_from_bytes(curr.id, temp_data)
	if mid > 0 {
		node_move_interior_cells(curr, &temp_node, 0, mid)
	}

	copy(curr.data, temp_data)
	set_right_ptr(curr.data, curr.id, child_from_mid_cell)
	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, right_node.id)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

split_leaf_root :: proc(t: ^Tree) -> Error {
	left_page, l_err := pager.allocate_page(t.pager)
	if l_err != nil { return .Page_Full }

	right_page, r_err := pager.allocate_page(t.pager)
	if r_err != nil { return .Page_Full }

	init_leaf_page(left_page.data, left_page.page_num)
	init_leaf_page(right_page.data, right_page.page_num)

	left_node, _ := node_from_bytes(left_page.page_num, left_page.data)
	right_node, _ := node_from_bytes(right_page.page_num, right_page.data)
	root_node, err := load_node(t, t.root)
	if err != .None { return err }
	if !is_leaf(root_node) { return .Invalid_Page_Header }

	total := int(root_node.leaf.cell_count)
	mid := total / 2
	if !node_move_leaf_cells(&root_node, &left_node, 0, mid) { return .Serialization_Failed }
	if !node_move_leaf_cells(&root_node, &right_node, mid, total - mid) { return .Serialization_Failed }

	ptrs := get_pointers(right_node.data, right_node.id)
	sep, ok := cell.get_rowid(right_node.data, int(ptrs[0]))
	if !ok { return .Invalid_Cell_Pointer }

	init_interior_page(root_node.data, root_node.id)
	root_node, _ = load_node(t, t.root)
	set_right_ptr(root_node.data, root_node.id, right_node.id)
	insert_interior_cell(root_node.data, root_node.id, left_node.id, sep)

	pager.mark_dirty(t.pager, left_node.id)
	pager.mark_dirty(t.pager, right_node.id)
	pager.mark_dirty(t.pager, root_node.id)
	return .None
}

split_interior_root :: proc(t: ^Tree, split: Split_Result) -> Error {
	left_page, err := pager.allocate_page(t.pager)
	if err != nil { return .Page_Full }

	init_interior_page(left_page.data, left_page.page_num)
	left_node, _ := node_from_bytes(left_page.page_num, left_page.data)
	root_node, r_err := load_node(t, t.root)
	if r_err != .None { return r_err }
	if root_node.interior == nil { return .Invalid_Page_Header }

	total := int(root_node.interior.cell_count)
	if !node_move_interior_cells(&root_node, &left_node, 0, total) { return .Serialization_Failed }

	old_right := get_right_ptr(root_node.data, root_node.id)
	set_right_ptr(left_node.data, left_node.id, old_right)
	init_interior_page(root_node.data, root_node.id)
	root_node, _ = load_node(t, t.root)

	set_right_ptr(root_node.data, root_node.id, split.right_page)
	insert_interior_cell(root_node.data, root_node.id, left_node.id, split.split_key)
	pager.mark_dirty(t.pager, t.root)
	pager.mark_dirty(t.pager, left_node.id)
	return .None
}

// Replaces 'find_child_page'
node_find_child :: proc(n: ^Node, key: types.Row_ID) -> u32 {
	pointers := get_pointers(n.data, n.id)
	if len(pointers) == 0 {
		return get_right_ptr(n.data, n.id)
	}

	left := 0
	right := len(pointers) - 1
	res_idx := len(pointers)
	for left <= right {
		mid := left + (right - left) / 2
		cell_offset := int(pointers[mid])
		sep_val, _, ok := utils.varint_decode(n.data, cell_offset + 4)
		if !ok { break }

		separator := types.Row_ID(sep_val)
		if key < separator {
			res_idx = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	if res_idx == len(pointers) {
		return get_right_ptr(n.data, n.id)
	}

	cell_offset := int(pointers[res_idx])
	child, _ := utils.read_u32_be(n.data, cell_offset)
	return child
}

// Replaces 'insert_cell_into_leaf'
node_insert_leaf_cell :: proc(t: ^Tree, n: ^Node, rowid: types.Row_ID, values: []types.Value) -> Error {
	if n.leaf == nil { return .Invalid_Page_Header }
	if t.config.check_duplicates && rowid_exists(n.data, n.header, n.id, rowid) {
		return .Duplicate_Rowid
	}

	cell_size := cell.calculate_size(rowid, values)
	base_offset := get_page_header_offset(n.id)
	header_size := page_header_size(n.header.page_type)
	ptr_area_end := base_offset + header_size + int(n.header.cell_count + 1) * size_of(Cell_Pointer)
	if ptr_area_end >= int(n.header.cell_content_offset) { return .Page_Full }
	if cell_size > int(n.header.cell_content_offset) - ptr_area_end { return .Page_Full }

	new_offset := int(n.header.cell_content_offset) - cell_size
	bytes_written, ok := cell.serialize(n.data[new_offset:], rowid, values)
	if !ok || bytes_written != cell_size { return .Serialization_Failed }

	insert_index := node_find_insert_index(n, rowid)
	raw_ptrs := get_raw_pointers(n.data, n.id)
	if insert_index < int(n.header.cell_count) {
		copy(raw_ptrs[insert_index + 1:], raw_ptrs[insert_index:n.header.cell_count])
	}

	raw_ptrs[insert_index] = Cell_Pointer(new_offset)
	n.header.cell_count += 1
	n.header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(t.pager, n.id)
	return .None
}

@(private = "file")
node_update_child_ptr :: proc(n: ^Node, old_child: u32, new_sibling: u32) -> bool {
	if get_right_ptr(n.data, n.id) == old_child {
		set_right_ptr(n.data, n.id, new_sibling)
		return true
	}

	pointers := get_pointers(n.data, n.id)
	for ptr in pointers {
		cell_offset := int(ptr)
		stored_child, _ := utils.read_u32_be(n.data, cell_offset)
		if stored_child == old_child {
			utils.write_u32_be(n.data, cell_offset, new_sibling)
			return true
		}
	}
	return false
}

@(private = "file")
node_find_insert_index :: proc(n: ^Node, target_rowid: types.Row_ID) -> int {
	left := 0
	right := int(n.header.cell_count)
	pointers := get_pointers(n.data, n.id)
	for left < right {
		mid := left + (right - left) / 2
		cell_ptr := pointers[mid]
		rowid, ok := cell.get_rowid(n.data, int(cell_ptr))
		if !ok { return left }
		if rowid < target_rowid {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

insert_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	Split_Result,
	Error,
) {
	curr, err := load_node(t, page_id)
	if err != .None { return {}, err }
	if is_leaf(curr) {
		e := node_insert_leaf_cell(t, &curr, rowid, values)
		if e == .Page_Full {
			split, s_err := split_leaf_node(t, &curr)
			if s_err != .None { return {}, s_err }

			target_id := curr.id
			if rowid >= split.split_key {
				target_id = split.right_page
			}

			target_node, t_err := load_node(t, target_id)
			if t_err != .None { return {}, t_err }

			retry_err := node_insert_leaf_cell(t, &target_node, rowid, values)
			if retry_err != .None { return {}, retry_err }
			return split, .None
		}
		return Split_Result{did_split = false}, err
	}

	child_id := node_find_child(&curr, rowid)
	child_split, c_err := insert_recursive(t, child_id, rowid, values)
	if c_err != .None { return {}, c_err }
	if !child_split.did_split {
		return Split_Result{did_split = false}, .None
	}

	is_rightmost := child_id == get_right_ptr(curr.data, curr.id)
	ptr_for_insert := child_split.right_page
	insert_key := child_split.split_key
	if !is_rightmost {
		idx := find_interior_cell_for_child(curr.data, curr.id, child_id)
		if idx == -1 { return {}, .Invalid_Page_Header }

		ptrs := get_pointers(curr.data, curr.id)
		cell_offset := int(ptrs[idx])
		old_sep_u64, _, ok := utils.varint_decode(curr.data, cell_offset + 4)
		if !ok { return {}, .Invalid_Cell_Pointer }

		insert_key = types.Row_ID(old_sep_u64)
		ptr_for_insert = child_split.right_page
		utils.varint_encode(curr.data[cell_offset + 4:], u64(child_split.split_key))
	} else {
		ptr_for_insert = child_id
	}

	ok := insert_interior_cell(curr.data, curr.id, ptr_for_insert, insert_key)
	if ok {
		if is_rightmost {
			set_right_ptr(curr.data, curr.id, child_split.right_page)
		}
		pager.mark_dirty(t.pager, curr.id)
		return Split_Result{did_split = false}, .None
	}

	interior_split, split_err := split_interior_node(t, &curr)
	if split_err != .None { return {}, split_err }

	target_id := curr.id
	if insert_key > interior_split.split_key {
		target_id = interior_split.right_page
	}

	target_node, t_err := load_node(t, target_id)
	if t_err != .None { return {}, t_err }
	if is_rightmost {
		set_right_ptr(target_node.data, target_id, child_split.right_page)
		insert_interior_cell(target_node.data, target_id, ptr_for_insert, insert_key)
	} else {
		insert_interior_cell(target_node.data, target_id, ptr_for_insert, insert_key)
	}
	pager.mark_dirty(t.pager, target_id)
	return interior_split, .None
}

@(private = "file")
rowid_exists :: proc(data: []u8, header: ^Page_Header, page_id: u32, target_rowid: types.Row_ID) -> bool {
	pointers := get_pointers(data, page_id)
	for ptr in pointers {
		rowid, ok := cell.get_rowid(data, int(ptr))
		if ok && rowid == target_rowid {
			return true
		}
	}
	return false
}

tree_insert :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	root_node, err := load_node(t, t.root)
	if err != .None { return err }
	if is_leaf(root_node) {
		e := node_insert_leaf_cell(t, &root_node, rowid, values)
		if e != .Page_Full {
			return e
		}
		if s_err := split_leaf_root(t); s_err != .None {
			return s_err
		}

		split, r_err := insert_recursive(t, t.root, rowid, values)
		if r_err != .None { return r_err }
		if split.did_split {
			return split_interior_root(t, split)
		}
		return .None
	}

	split, i_err := insert_recursive(t, t.root, rowid, values)
	if i_err != .None { return i_err }
	if split.did_split {
		return split_interior_root(t, split)
	}
	return .None
}

tree_find :: proc(t: ^Tree, key: types.Row_ID, allocator := context.allocator) -> (cell.Cell, Error) {
	curr := t.root
	for {
		node, err := load_node(t, curr)
		if err != .None { return {}, err }
		if is_leaf(node) {
			break
		}

		curr = node_find_child(&node, key)
		if curr == 0 { return {}, .Invalid_Page_Header }
	}

	leaf_node, _ := load_node(t, curr)
	pointers := get_pointers(leaf_node.data, curr)
	left := 0
	right := len(pointers) - 1
	for left <= right {
		mid := left + (right - left) / 2
		cell_ptr := pointers[mid]
		rid, ok := cell.get_rowid(leaf_node.data, int(cell_ptr))
		if !ok {
			return {}, .Invalid_Cell_Pointer
		}
		if rid == key {
			cell_cfg := cell.Config {
				allocator = allocator,
				zero_copy = t.config.zero_copy,
			}

			c, _, des_ok := cell.deserialize(leaf_node.data, int(cell_ptr), cell_cfg)
			if !des_ok { return {}, .Cell_Deserialize_Failed }
			return c, .None
		}
		if rid < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return {}, .Cell_Not_Found
}

tree_next_rowid :: proc(t: ^Tree) -> (types.Row_ID, Error) {
	curr := t.root
	for {
		node, err := load_node(t, curr)
		if err != .None { return 0, err }
		if is_leaf(node) {
			if node.header.cell_count == 0 {
				return 1, .None
			}

			pointers := get_pointers(node.data, curr)
			last_ptr := pointers[node.header.cell_count - 1]
			last_id, ok := cell.get_rowid(node.data, int(last_ptr))
			if !ok { return 0, .Invalid_Cell_Pointer }
			return last_id + 1, .None
		}
		curr = get_right_ptr(node.data, curr)
	}
}

tree_count_rows :: proc(t: ^Tree) -> (int, Error) {
	return count_recursive(t, t.root)
}

@(private = "file")
count_recursive :: proc(t: ^Tree, page_id: u32) -> (int, Error) {
	node, err := load_node(t, page_id)
	if err != .None { return 0, err }
	if is_leaf(node) {
		return int(node.header.cell_count), .None
	}

	total := 0
	pointers := get_pointers(node.data, page_id)
	for ptr in pointers {
		child_id, _ := utils.read_u32_be(node.data, int(ptr))
		count, c_err := count_recursive(t, child_id)
		if c_err != .None { return 0, c_err }
		total += count
	}

	right_child := get_right_ptr(node.data, page_id)
	right_count, r_err := count_recursive(t, right_child)
	if r_err != .None { return 0, r_err }
	return total + right_count, .None
}

tree_delete :: proc(t: ^Tree, key: types.Row_ID) -> Error {
	curr := t.root
	for {
		node, err := load_node(t, curr)
		if err != .None { return err }
		if is_leaf(node) { break }
		curr = node_find_child(&node, key)
	}

	leaf, err := load_node(t, curr)
	if err != .None { return err }

	pointers := get_raw_pointers(leaf.data, curr)
	limit := int(leaf.header.cell_count)
	delete_idx := -1
	cell_off := 0
	cell_sz := 0
	for i in 0 ..< limit {
		ptr_val := int(pointers[i])
		rid, ok := cell.get_rowid(leaf.data, ptr_val)
		if !ok { return .Invalid_Cell_Pointer }
		if rid == key {
			delete_idx = i
			cell_off = ptr_val
			sz, ok2 := cell.get_size(leaf.data, ptr_val)
			if ok2 { cell_sz = sz }
			break
		}
	}

	if delete_idx == -1 { return .Cell_Not_Found }
	if delete_idx < limit - 1 {
		copy(pointers[delete_idx:], pointers[delete_idx + 1:limit])
	}

	leaf.header.cell_count -= 1
	if cell_off == int(leaf.header.cell_content_offset) {
		leaf.header.cell_content_offset += u16le(cell_sz)
	} else {
		if cell_sz > 0 && cell_sz < 255 {
			leaf.header.fragmented_bytes += u8(cell_sz)
		}
	}
	pager.mark_dirty(t.pager, curr)
	return .None
}

tree_foreach :: proc(
	t: ^Tree,
	callback: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	user_data: rawptr = nil,
) -> Error {
	return foreach_recursive(t, t.root, callback, user_data)
}

@(private = "file")
foreach_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	cb: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	ud: rawptr,
) -> Error {
	node, err := load_node(t, page_id)
	if err != .None { return err }
	if is_leaf(node) {
		ptrs := get_pointers(node.data, page_id)
		for ptr in ptrs {
			alloc := t.config.allocator
			cell_cfg := cell.Config {
				allocator = alloc,
				zero_copy = t.config.zero_copy,
			}

			c, _, ok := cell.deserialize(node.data, int(ptr), cell_cfg)
			if !ok { return .Cell_Deserialize_Failed }

			continue_iter := cb(&c, ud)
			if !t.config.zero_copy {
				cell.destroy(&c)
			}
			if !continue_iter { return .None }
		}
		return .None
	}

	ptrs := get_pointers(node.data, page_id)
	for ptr in ptrs {
		child, _ := utils.read_u32_be(node.data, int(ptr))
		if e := foreach_recursive(t, child, cb, ud); e != .None { return e }
	}
	right := get_right_ptr(node.data, page_id)
	return foreach_recursive(t, right, cb, ud)
}

tree_debug_print_node :: proc(t: ^Tree, page_id: u32) {
	node, err := load_node(t, page_id)
	if err != .None {
		fmt.printf("Error reading page %d\n", page_id)
		return
	}

	fmt.printf(
		"Page %d (type=%v, cells=%d, off=%d, frag=%d)\n",
		page_id,
		node.header.page_type,
		node.header.cell_count,
		node.header.cell_content_offset,
		node.header.fragmented_bytes,
	)

	pointers := get_pointers(node.data, page_id)
	for ptr, i in pointers {
		cell_cfg := cell.Config {
			allocator = t.config.allocator,
			zero_copy = false,
		}

		c, _, ok := cell.deserialize(node.data, int(ptr), cell_cfg)
		if !ok {
			fmt.printf("  Cell %d: [Error Deserializing]\n", i)
			continue
		}

		fmt.printf("  Cell %d: ", i)
		cell.debug_print(c)
		cell.destroy(&c)
	}
}

tree_verify :: proc(t: ^Tree) -> bool {
	return verify_recursive(t, t.root, 0, types.Row_ID(max(i64)), 0)
}

@(private = "file")
verify_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	min_k: types.Row_ID,
	max_k: types.Row_ID,
	depth: int,
) -> bool {
	node, err := load_node(t, page_id)
	if err != .None {
		fmt.printf("❌ Failed to load page %d\n", page_id)
		return false
	}

	indent := strings.repeat("  ", depth, context.temp_allocator)
	fmt.printf("%sPage %d [%v] count=%d\n", indent, page_id, node.header.page_type, node.header.cell_count)
	if is_leaf(node) {
		ptrs := get_pointers(node.data, page_id)
		prev := min_k
		for ptr in ptrs {
			rowid, _ := cell.get_rowid(node.data, int(ptr))
			if rowid < prev {
				fmt.printf("❌ Leaf key disorder: %d came after %d\n", rowid, prev)
				return false
			}
			if rowid > max_k {
				fmt.printf("❌ Leaf key %d > max %d\n", rowid, max_k)
				return false
			}
			prev = rowid
		}
		return true
	}

	ptrs := get_pointers(node.data, page_id)
	prev_k := min_k
	for ptr in ptrs {
		offset := int(ptr)
		child, _ := utils.read_u32_be(node.data, offset)
		sep_val, _, _ := utils.varint_decode(node.data, offset + 4)
		key := types.Row_ID(sep_val)
		if key < prev_k || key > max_k {
			fmt.printf("❌ Interior key %d out of bounds [%d, %d]\n", key, prev_k, max_k)
			return false
		}
		if !verify_recursive(t, child, prev_k, key, depth + 1) {
			return false
		}
		prev_k = key
	}
	right := get_right_ptr(node.data, page_id)
	return verify_recursive(t, right, prev_k, max_k, depth + 1)
}
