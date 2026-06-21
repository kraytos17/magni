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

MAX_TREE_DEPTH :: 1000

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
	id:     u32,
	data:   []u8,
	header: ^Page_Header,
}

is_leaf :: proc(n: Node) -> bool {
	return n.header.page_type == .LEAF_TABLE
}

node_leaf :: proc(n: Node) -> ^Leaf_Header {
	return get_leaf_header(n.data, n.id)
}

node_interior :: proc(n: Node) -> ^Interior_Header {
	return get_interior_header(n.data, n.id)
}

unpin_node :: proc(t: ^Tree, n: Node) {
	pager.unpin_page(t.pager, n.id)
}

load_node :: proc(t: ^Tree, page_id: u32) -> (Node, Error) {
	page, err := pager.get_page(t.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }
	return node_from_bytes(page_id, page.data)
}

node_from_bytes :: proc(id: u32, data: []u8) -> (Node, Error) {
	common_hdr := get_header(data, id)
	if common_hdr == nil {
		return {}, .Invalid_Page_Header
	}
	return Node{id = id, data = data, header = common_hdr}, .None
}

Split_Result :: struct {
	did_split:  bool,
	right_page: u32, // The new sibling page number
	split_key:  types.Row_ID, // The key separating left/right
}

@(private = "file")
node_move_leaf_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if !is_leaf(src^) || !is_leaf(dst^) { return false }

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
	if is_leaf(src^) || is_leaf(dst^) { return false }

	ptrs := get_pointers(src.data, src.id)
	for i in 0 ..< count {
		off := ptrs[start_idx + i]
		size := interior_cell_size_from_page(src.data, int(off))
		dst_int := node_interior(dst^)
		new_off := int(dst_int.cell_content_offset) - size
		dst_int.cell_content_offset = u16le(new_off)

		copy(dst.data[new_off:], src.data[int(off):int(off) + size])
		hdr_sz := size_of(Interior_Header)
		base := get_page_header_offset(dst.id)
		ptr_loc := base + hdr_sz + int(dst_int.cell_count) * 2
		utils.write_u16_le(dst.data, ptr_loc, u16(new_off))
		dst_int.cell_count += 1
	}
	return true
}

@(private = "file")
split_leaf_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	if node_leaf(curr^).cell_count == 0 { return {}, .Page_Full }

	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }
	defer pager.unpin_page(t.pager, new_page.page_num)

	init_leaf_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(new_page.page_num, new_page.data)
	total := int(node_leaf(curr^).cell_count)
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
	defer pager.unpin_page(t.pager, new_page.page_num)

	init_interior_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(new_page.page_num, new_page.data)
	total := int(node_interior(curr^).cell_count)
	mid := total / 2

	ptrs := get_pointers(curr.data, curr.id)
	if len(ptrs) == 0 { return {}, .Invalid_Cell_Pointer }

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
	pager.unpin_page(t.pager, new_page.page_num)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

split_leaf_root :: proc(t: ^Tree, root_page: u32) -> (new_root: u32, err: Error) {
	left_page, l_err := pager.allocate_page(t.pager)
	if l_err != .None { return 0, .Page_Full }
	left_id := left_page.page_num
	defer pager.unpin_page(t.pager, left_id)

	right_page, r_err := pager.allocate_page(t.pager)
	if r_err != .None { return 0, .Page_Full }
	right_id := right_page.page_num
	defer pager.unpin_page(t.pager, right_id)

	init_leaf_page(left_page.data, left_page.page_num)
	init_leaf_page(right_page.data, right_page.page_num)

	left_node, _ := node_from_bytes(left_page.page_num, left_page.data)
	right_node, _ := node_from_bytes(right_page.page_num, right_page.data)
	root_node, load_err := load_node(t, root_page)
	if load_err != .None { return 0, load_err }
	defer unpin_node(t, root_node)

	if !is_leaf(root_node) { return 0, .Invalid_Page_Header }
	if node_leaf(root_node).cell_count == 0 { return 0, .Page_Full }

	total := int(node_leaf(root_node).cell_count)
	mid := total / 2
	if !node_move_leaf_cells(&root_node, &left_node, 0, mid) { return 0, .Serialization_Failed }
	if !node_move_leaf_cells(&root_node, &right_node, mid, total - mid) { return 0, .Serialization_Failed }

	ptrs := get_pointers(right_node.data, right_node.id)
	sep, s_ok := cell.get_rowid(right_node.data, int(ptrs[0]))
	if !s_ok { return 0, .Invalid_Cell_Pointer }

	init_interior_page(root_node.data, root_node.id)
	unpin_node(t, root_node)

	root_node, load_err = load_node(t, root_page)
	if load_err != .None { return 0, load_err }
	set_right_ptr(root_node.data, root_node.id, right_node.id)
	insert_interior_cell(root_node.data, root_node.id, left_node.id, sep)

	pager.mark_dirty(t.pager, left_node.id)
	pager.mark_dirty(t.pager, right_node.id)
	pager.mark_dirty(t.pager, root_node.id)
	unpin_node(t, root_node)
	return root_page, .None
}

split_interior_root :: proc(t: ^Tree, split: Split_Result) -> Error {
	left_page, err := pager.allocate_page(t.pager)
	if err != nil { return .Page_Full }
	defer pager.unpin_page(t.pager, left_page.page_num)

	init_interior_page(left_page.data, left_page.page_num)
	left_node, _ := node_from_bytes(left_page.page_num, left_page.data)
	root_node, r_err := load_node(t, t.root)
	if r_err != .None { return r_err }
	if is_leaf(root_node) {
		unpin_node(t, root_node)
		return .Invalid_Page_Header
	}

	total := int(node_interior(root_node).cell_count)
	if !node_move_interior_cells(&root_node, &left_node, 0, total) {
		unpin_node(t, root_node)
		return .Serialization_Failed
	}

	old_right := get_right_ptr(root_node.data, root_node.id)
	set_right_ptr(left_node.data, left_node.id, old_right)
	init_interior_page(root_node.data, root_node.id)
	unpin_node(t, root_node)

	root_node, r_err = load_node(t, t.root)
	if r_err != .None { return r_err }

	set_right_ptr(root_node.data, root_node.id, split.right_page)
	insert_interior_cell(root_node.data, root_node.id, left_node.id, split.split_key)
	pager.mark_dirty(t.pager, t.root)
	pager.mark_dirty(t.pager, left_node.id)
	unpin_node(t, root_node)
	return .None
}

@(private = "file")
leaf_lower_bound :: proc(data: []u8, page_id: u32, target: types.Row_ID) -> (int, bool) {
	pointers := get_pointers(data, page_id)
	left := 0
	right := len(pointers)
	for left < right {
		mid := left + (right - left) / 2
		rowid, ok := cell.get_rowid(data, int(pointers[mid]))
		if !ok { return left, false }
		if rowid < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}

node_find_child :: proc(n: ^Node, key: types.Row_ID) -> u32 {
	return node_find_child_data(n.data, n.id, key)
}

node_insert_leaf_cell :: proc(t: ^Tree, n: ^Node, rowid: types.Row_ID, values: []types.Value) -> Error {
	if !is_leaf(n^) { return .Invalid_Page_Header }

	pointers := get_pointers(n.data, n.id)
	idx, lb_ok := leaf_lower_bound(n.data, n.id, rowid)
	if t.config.check_duplicates {
		if lb_ok && idx < len(pointers) {
			rid, _ := cell.get_rowid(n.data, int(pointers[idx]))
			if rid == rowid { return .Duplicate_Rowid }
		}
	}

	cinfo := cell.compute_info(rowid, values)
	// Try freeblock first
	free_off := freeblock_alloc(
		n.data,
		n.header.first_freeblock,
		u16(cinfo.total_size),
		&n.header.first_freeblock,
	)
	if free_off != 0 {
		bytes_written, ok := cell.serialize(n.data[int(free_off):], rowid, values, cinfo)
		if !ok || bytes_written != cinfo.total_size { return .Serialization_Failed }

		raw_ptrs := get_raw_pointers(n.data, n.id)
		if idx < int(n.header.cell_count) {
			copy(raw_ptrs[idx + 1:], raw_ptrs[idx:n.header.cell_count])
		}
		
		raw_ptrs[idx] = Cell_Pointer(free_off)
		n.header.cell_count += 1
		pager.mark_dirty(t.pager, n.id)
		return .None
	}

	// No freeblock — allocate from end
	base_offset := get_page_header_offset(n.id)
	header_size := page_header_size(n.header.page_type)
	ptr_area_end := base_offset + header_size + int(n.header.cell_count + 1) * size_of(Cell_Pointer)
	if ptr_area_end >= int(n.header.cell_content_offset) { return .Page_Full }
	if cinfo.total_size > int(n.header.cell_content_offset) - ptr_area_end { return .Page_Full }

	new_offset := int(n.header.cell_content_offset) - cinfo.total_size
	bytes_written, ok := cell.serialize(n.data[new_offset:], rowid, values, cinfo)
	if !ok || bytes_written != cinfo.total_size { return .Serialization_Failed }

	raw_ptrs := get_raw_pointers(n.data, n.id)
	if idx < int(n.header.cell_count) {
		copy(raw_ptrs[idx + 1:], raw_ptrs[idx:n.header.cell_count])
	}

	raw_ptrs[idx] = Cell_Pointer(new_offset)
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
	idx, _ := leaf_lower_bound(n.data, n.id, target_rowid)
	return idx
}

insert_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	cow: bool,
) -> (
	Insert_COW_Result,
	Error,
) {
	new_page_num := page_id
	if cow {
		var, cow_err := copy_on_write(t, page_id)
		if cow_err != .None { return {}, cow_err }
		new_page_num = var
	}

	curr, err := load_node(t, new_page_num)
	if err != .None { return {}, err }
	defer unpin_node(t, curr)

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
			defer unpin_node(t, target_node)

			retry_err := node_insert_leaf_cell(t, &target_node, rowid, values)
			if retry_err != .None { return {}, retry_err }
			return Insert_COW_Result {
					new_page = curr.id,
					did_split = true,
					right_page = split.right_page,
					split_key = split.split_key,
				},
				.None
		}
		return Insert_COW_Result{new_page = new_page_num}, e
	}

	child_id := node_find_child(&curr, rowid)
	child_result, c_err := insert_recursive(t, child_id, rowid, values, cow)
	if c_err != .None { return {}, c_err }
	if cow && child_result.new_page != child_id {
		node_update_child_ptr(&curr, child_id, child_result.new_page)
	}
	if !child_result.did_split {
		pager.mark_dirty(t.pager, curr.id)
		return Insert_COW_Result{new_page = new_page_num}, .None
	}

	is_rightmost := child_id == get_right_ptr(curr.data, curr.id)
	ptr_for_insert := child_result.right_page
	insert_key := child_result.split_key
	if !is_rightmost {
		idx := find_interior_cell_for_child(curr.data, curr.id, child_id)
		if idx == -1 { return {}, .Invalid_Page_Header }

		ptrs := get_pointers(curr.data, curr.id)
		if len(ptrs) == 0 { return {}, .Invalid_Cell_Pointer }
		cell_offset := int(ptrs[idx])
		old_sep_u64, _, ok := utils.varint_decode(curr.data, cell_offset + 4)
		if !ok { return {}, .Invalid_Cell_Pointer }

		insert_key = types.Row_ID(old_sep_u64)
		ptr_for_insert = child_result.right_page
		utils.varint_encode(curr.data[cell_offset + 4:], u64(child_result.split_key))
	} else {
		ptr_for_insert = child_id
	}

	ok := insert_interior_cell(curr.data, curr.id, ptr_for_insert, insert_key)
	if ok {
		if is_rightmost {
			set_right_ptr(curr.data, curr.id, child_result.right_page)
		}
		pager.mark_dirty(t.pager, curr.id)
		return Insert_COW_Result{new_page = new_page_num}, .None
	}

	interior_split, split_err := split_interior_node(t, &curr)
	if split_err != .None { return {}, split_err }

	target_id := curr.id
	if insert_key > interior_split.split_key {
		target_id = interior_split.right_page
	}

	target_node, t_err := load_node(t, target_id)
	if t_err != .None { return {}, t_err }
	defer unpin_node(t, target_node)

	if is_rightmost {
		set_right_ptr(target_node.data, target_id, child_result.right_page)
		insert_interior_cell(target_node.data, target_id, ptr_for_insert, insert_key)
	} else {
		insert_interior_cell(target_node.data, target_id, ptr_for_insert, insert_key)
	}

	pager.mark_dirty(t.pager, target_id)
	return Insert_COW_Result {
			new_page = new_page_num,
			did_split = true,
			right_page = interior_split.right_page,
			split_key = interior_split.split_key,
		},
		.None
}

@(private = "file")
rowid_exists :: proc(data: []u8, page_id: u32, target_rowid: types.Row_ID) -> bool {
	pointers := get_pointers(data, page_id)
	idx, ok := leaf_lower_bound(data, page_id, target_rowid)
	if !ok || idx >= len(pointers) { return false }

	rowid, ok2 := cell.get_rowid(data, int(pointers[idx]))
	return ok2 && rowid == target_rowid
}

tree_insert :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	root_node, err := load_node(t, t.root)
	if err != .None { return err }
	defer unpin_node(t, root_node)

	if is_leaf(root_node) {
		e := node_insert_leaf_cell(t, &root_node, rowid, values)
		if e != .Page_Full { return e }
		if _, s_err := split_leaf_root(t, t.root); s_err != .None {
			return s_err
		}

		result, r_err := insert_recursive(t, t.root, rowid, values, false)
		if r_err != .None { return r_err }
		if result.did_split {
			return split_interior_root(
				t,
				{did_split = true, right_page = result.right_page, split_key = result.split_key},
			)
		}
		return .None
	}

	result, i_err := insert_recursive(t, t.root, rowid, values, false)
	if i_err != .None { return i_err }
	if result.did_split {
		return split_interior_root(
			t,
			{did_split = true, right_page = result.right_page, split_key = result.split_key},
		)
	}
	return .None
}

@(private = "file")
descend_to_leaf :: proc(
	t: ^Tree,
	get_child: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32,
	ctx: rawptr,
	root_override: u32 = 0,
) -> (
	Node,
	Error,
) {
	curr := t.root if root_override == 0 else root_override
	for {
		n, err := load_node(t, curr)
		if err != .None { return {}, err }
		if is_leaf(n) { return n, .None }
		curr = get_child(n.data, n.id, ctx)
		unpin_node(t, n)
	}
}

@(private = "file")
descend_by_key :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	key := (cast(^types.Row_ID)ctx)^
	return node_find_child_data(data, page_id, key)
}

@(private = "file")
node_find_child_data :: proc(data: []u8, page_id: u32, key: types.Row_ID) -> u32 {
	pointers := get_pointers(data, page_id)
	if len(pointers) == 0 { return get_right_ptr(data, page_id) }
	idx, ok := interior_lower_bound(data, page_id, key)
	if !ok || idx == len(pointers) { return get_right_ptr(data, page_id) }
	child, _ := utils.read_u32_be(data, int(pointers[idx]))
	return child
}

@(private = "file")
descend_by_rightmost :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	return get_right_ptr(data, page_id)
}

tree_find :: proc(t: ^Tree, key: types.Row_ID, allocator := context.allocator) -> (cell.Cell, Error) {
	k := key
	leaf, err := descend_to_leaf(t, descend_by_key, &k)
	if err != .None { return {}, err }
	defer unpin_node(t, leaf)

	pointers := get_pointers(leaf.data, leaf.id)
	idx, ok := leaf_lower_bound(leaf.data, leaf.id, key)
	if !ok { return {}, .Invalid_Cell_Pointer }
	if idx < len(pointers) {
		rid, ok1 := cell.get_rowid(leaf.data, int(pointers[idx]))
		if ok1 && rid == key {
			cell_cfg := cell.Config {
				allocator = allocator,
				zero_copy = t.config.zero_copy,
			}

			c, _, des_ok := cell.deserialize(leaf.data, int(pointers[idx]), cell_cfg)
			if !des_ok { return {}, .Cell_Deserialize_Failed }
			return c, .None
		}
	}
	return {}, .Cell_Not_Found
}

tree_next_rowid :: proc(t: ^Tree) -> (types.Row_ID, Error) {
	leaf, err := descend_to_leaf(t, descend_by_rightmost, nil)
	if err != .None { return 0, err }
	defer unpin_node(t, leaf)

	if leaf.header.cell_count == 0 {
		return 1, .None
	}

	pointers := get_pointers(leaf.data, leaf.id)
	last_ptr := pointers[leaf.header.cell_count - 1]
	last_id, ok := cell.get_rowid(leaf.data, int(last_ptr))
	if !ok { return 0, .Invalid_Cell_Pointer }
	return last_id + 1, .None
}

tree_count_rows :: proc(t: ^Tree) -> (int, Error) {
	return count_recursive(t, t.root)
}

@(private = "file")
count_recursive :: proc(t: ^Tree, page_id: u32) -> (int, Error) {
	node, err := load_node(t, page_id)
	if err != .None { return 0, err }
	defer unpin_node(t, node)

	if is_leaf(node) {
		return int(node.header.cell_count), .None
	}

	total := 0
	pointers := get_pointers(node.data, page_id)
	for ptr in pointers {
		child_id, ok := utils.read_u32_be(node.data, int(ptr))
		if !ok { return 0, .Invalid_Cell_Pointer }

		count, c_err := count_recursive(t, child_id)
		if c_err != .None { return 0, c_err }
		total += count
	}

	right_child := get_right_ptr(node.data, page_id)
	right_count, r_err := count_recursive(t, right_child)
	if r_err != .None { return 0, r_err }
	return total + right_count, .None
}

@(private = "file")
delete_from_leaf :: proc(t: ^Tree, leaf_node: ^Node, key: types.Row_ID) -> Error {
	pointers := get_raw_pointers(leaf_node.data, leaf_node.id)
	limit := int(leaf_node.header.cell_count)
	delete_idx := -1
	cell_off := 0
	cell_sz := 0

	idx, ok := leaf_lower_bound(leaf_node.data, leaf_node.id, key)
	if ok && idx < limit {
		rid, ok2 := cell.get_rowid(leaf_node.data, int(pointers[idx]))
		if ok2 && rid == key {
			delete_idx = idx
			cell_off = int(pointers[idx])
			sz, ok3 := cell.get_size(leaf_node.data, cell_off)
			if ok3 { cell_sz = sz }
		}
	}
	if delete_idx == -1 { return .Cell_Not_Found }
	if delete_idx < limit - 1 {
		copy(pointers[delete_idx:], pointers[delete_idx + 1:limit])
	}

	leaf_node.header.cell_count -= 1
	if cell_off == int(leaf_node.header.cell_content_offset) {
		leaf_node.header.cell_content_offset += u16le(cell_sz)
	} else if cell_sz >= FREEBLOCK_HDR_SIZE {
		freeblock_insert(
			leaf_node.data,
			u16(cell_off),
			u16(cell_sz),
			&leaf_node.header.first_freeblock,
		)
	} else {
		if cell_sz > 0 && cell_sz < 255 {
			new_frag := u16(leaf_node.header.fragmented_bytes) + u16(cell_sz)
			leaf_node.header.fragmented_bytes = u8(min(new_frag, 255))
		}
	}
	pager.mark_dirty(t.pager, leaf_node.id)
	return .None
}

tree_delete :: proc(t: ^Tree, key: types.Row_ID) -> Error {
	k := key
	leaf_node, err := descend_to_leaf(t, descend_by_key, &k)
	if err != .None { return err }
	defer unpin_node(t, leaf_node)
	return delete_from_leaf(t, &leaf_node, key)
}

// Replaces an existing cell's values in a single traversal (non-COW).
tree_update :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	k := rowid
	leaf_node, err := descend_to_leaf(t, descend_by_key, &k)
	if err != .None { return err }
	defer unpin_node(t, leaf_node)

	if d_err := delete_from_leaf(t, &leaf_node, rowid); d_err != .None {
		return d_err
	}
	return node_insert_leaf_cell(t, &leaf_node, rowid, values)
}

tree_delete_cow :: proc(t: ^Tree, key: types.Row_ID) -> (new_root: u32, err: Error) {
	Update_COW_Result :: struct {
		new_page: u32,
	}

	delete_cow_recursive :: proc(
		t: ^Tree,
		pid: u32,
		key: types.Row_ID,
		cow: bool,
	) -> (
		Update_COW_Result,
		Error,
	) {
		page_id := pid
		if cow {
			new_id, c_err := copy_on_write(t, page_id)
			if c_err != .None { return {}, c_err }
			page_id = new_id
		}

		node, n_err := load_node(t, page_id)
		if n_err != .None { return {}, n_err }
		defer unpin_node(t, node)

		if is_leaf(node) {
			return Update_COW_Result{new_page = node.id}, delete_from_leaf(t, &node, key)
		}

		child_id := node_find_child(&node, key)
		child_result, c_err := delete_cow_recursive(t, child_id, key, true)
		if c_err != .None { return {}, c_err }

		if child_result.new_page != child_id {
			node_update_child_ptr(&node, child_id, child_result.new_page)
		}
		pager.mark_dirty(t.pager, node.id)
		return Update_COW_Result{new_page = node.id}, .None
	}

	result, rec_err := delete_cow_recursive(t, t.root, key, true)
	if rec_err != .None { return 0, rec_err }
	return result.new_page, .None
}

// Replaces an existing cell's values in a single COW traversal.
// The key (rowid) must already exist in the tree.
tree_update_cow :: proc(
	t: ^Tree,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	new_root: u32,
	err: Error,
) {
	Update_Result :: struct {
		new_page: u32,
	}

	update_recursive :: proc(
		t: ^Tree,
		pid: u32,
		rowid: types.Row_ID,
		values: []types.Value,
		cow: bool,
	) -> (
		Update_Result,
		Error,
	) {
		page_id := pid
		if cow {
			new_id, c_err := copy_on_write(t, page_id)
			if c_err != .None { return {}, c_err }
			page_id = new_id
		}

		node, n_err := load_node(t, page_id)
		if n_err != .None { return {}, n_err }
		defer unpin_node(t, node)

		if is_leaf(node) {
			if d_err := delete_from_leaf(t, &node, rowid); d_err != .None {
				return {}, d_err
			}
			if i_err := node_insert_leaf_cell(t, &node, rowid, values); i_err != .None {
				return {}, i_err
			}
			return Update_Result{new_page = node.id}, .None
		}

		child_id := node_find_child(&node, rowid)
		child_result, c_err := update_recursive(t, child_id, rowid, values, true)
		if c_err != .None { return {}, c_err }

		if child_result.new_page != child_id {
			node_update_child_ptr(&node, child_id, child_result.new_page)
		}
		pager.mark_dirty(t.pager, node.id)
		return Update_Result{new_page = node.id}, .None
	}

	result, rec_err := update_recursive(t, t.root, rowid, values, true)
	if rec_err != .None { return 0, rec_err }
	return result.new_page, .None
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
	defer unpin_node(t, node)

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
		child, ok := utils.read_u32_be(node.data, int(ptr))
		if !ok { return .Invalid_Cell_Pointer }
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
	visited := make(map[u32]bool, context.temp_allocator)
	defer delete(visited)
	return verify_recursive(t, t.root, 0, types.Row_ID(max(i64)), 0, &visited)
}

@(private = "file")
verify_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	min_k: types.Row_ID,
	max_k: types.Row_ID,
	depth: int,
	visited: ^map[u32]bool,
) -> bool {
	if page_id == 0 { return false }
	if page_id in visited {
		fmt.printf("Cycle detected: page %d revisited\n", page_id)
		return false
	}
	if depth > MAX_TREE_DEPTH {
		fmt.printf("Tree too deep (depth=%d), possible cycle\n", depth)
		return false
	}

	visited[page_id] = true
	node, err := load_node(t, page_id)
	if err != .None {
		fmt.printf("Failed to load page %d\n", page_id)
		return false
	}
	defer unpin_node(t, node)

	indent := strings.repeat("  ", depth, context.temp_allocator)
	fmt.printf("%sPage %d [%v] count=%d\n", indent, page_id, node.header.page_type, node.header.cell_count)
	if is_leaf(node) {
		ptrs := get_pointers(node.data, page_id)
		prev := min_k
		for ptr in ptrs {
			rowid, ok := cell.get_rowid(node.data, int(ptr))
			if !ok { fmt.printf("Corrupt cell at offset %d\n", int(ptr)); return false }
			if rowid < prev {
				fmt.printf("Leaf key disorder: %d came after %d\n", rowid, prev)
				return false
			}
			if rowid > max_k {
				fmt.printf("Leaf key %d > max %d\n", rowid, max_k)
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
		child, r_ok := utils.read_u32_be(node.data, offset)
		if !r_ok { fmt.printf("Corrupt interior cell at offset %d\n", offset); return false }

		sep_val, _, v_ok := utils.varint_decode(node.data, offset + 4)
		if !v_ok { fmt.printf("Corrupt separator key at offset %d\n", offset + 4); return false }

		key := types.Row_ID(sep_val)
		if key < prev_k || key > max_k {
			fmt.printf("Interior key %d out of bounds [%d, %d]\n", key, prev_k, max_k)
			return false
		}
		if !verify_recursive(t, child, prev_k, key, depth + 1, visited) {
			return false
		}
		prev_k = key
	}
	right := get_right_ptr(node.data, page_id)
	return verify_recursive(t, right, prev_k, max_k, depth + 1, visited)
}

Insert_COW_Result :: struct {
	new_page:   u32,
	did_split:  bool,
	right_page: u32, // New sibling if split
	split_key:  types.Row_ID, // Separator key if split
}

// Creates a COW copy of a page and returns the new page number.
@(private)
copy_on_write :: proc(t: ^Tree, page_id: u32) -> (u32, Error) {
	new_page, err := pager.copy_page(t.pager, page_id)
	if err != .None { return 0, .Page_Read_Failed }

	// When copying page 1, the btree header and cell pointers are at offset 100+/108+
	// (after the 100-byte database header), but on the new page they must be at offset 0+/8+.
	if page_id == 1 {
		hdr := get_header(new_page.data, 1)
		if hdr == nil { return 0, .Invalid_Page_Header }

		hdr_sz := page_header_size(hdr.page_type)
		cell_count := int(hdr.cell_count)
		ptr_sz := cell_count * size_of(Cell_Pointer)

		SRC_HDR_OFF :: types.DATABASE_HEADER_SIZE
		DST_HDR_OFF :: 0

		if ptr_sz > 0 {
			ptr_copy := make([]u8, ptr_sz, context.temp_allocator)
			copy(ptr_copy, new_page.data[SRC_HDR_OFF + hdr_sz:])
			mem.zero_slice(new_page.data[SRC_HDR_OFF + hdr_sz:SRC_HDR_OFF + hdr_sz + ptr_sz])
			copy(new_page.data[DST_HDR_OFF + hdr_sz:], ptr_copy)
		}

		hdr_copy := make([]u8, hdr_sz, context.temp_allocator)
		copy(hdr_copy, new_page.data[SRC_HDR_OFF:])
		mem.zero_slice(new_page.data[SRC_HDR_OFF:SRC_HDR_OFF + hdr_sz])
		copy(new_page.data[DST_HDR_OFF:], hdr_copy)
	}
	return new_page.page_num, .None
}

// Insert a row into the tree using copy-on-write. Returns the new root page
// number (the caller must use this instead of t.root going forward).
tree_insert_cow :: proc(
	t: ^Tree,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	new_root: u32,
	err: Error,
) {
	root_node, load_err := load_node(t, t.root)
	if load_err != .None { return 0, load_err }
	defer unpin_node(t, root_node)

	if is_leaf(root_node) {
		new_root, err = copy_on_write(t, t.root)
		if err != .None { return 0, err }

		cow_node, n_err := load_node(t, new_root)
		if n_err != .None { return 0, n_err }
		defer unpin_node(t, cow_node)

		e := node_insert_leaf_cell(t, &cow_node, rowid, values)
		if e != .Page_Full { return new_root, e }
		return split_leaf_root(t, new_root)
	}

	result, r_err := insert_recursive(t, t.root, rowid, values, true)
	if r_err != .None { return 0, r_err }

	new_root = result.new_page
	if result.did_split {
		new_root_page, a_err := pager.allocate_page(t.pager)
		if a_err != .None { return 0, .Page_Full }

		init_interior_page(new_root_page.data, new_root_page.page_num)
		set_right_ptr(new_root_page.data, new_root_page.page_num, result.right_page)
		insert_interior_cell(new_root_page.data, new_root_page.page_num, result.new_page, result.split_key)
		pager.mark_dirty(t.pager, new_root_page.page_num)
		pager.unpin_page(t.pager, new_root_page.page_num)
		new_root = new_root_page.page_num
	}
	return new_root, .None
}

// Collects all page numbers reachable from a given root into a set.
// Used for garbage collection of orphan pages.
collect_pages :: proc(t: ^Tree, root: u32, pages: ^map[u32]bool) {
	if root == 0 || root in pages { return }
	pages[root] = true

	node, err := load_node(t, root)
	if err != .None { return }
	defer unpin_node(t, node)

	if is_leaf(node) { return }

	pointers := get_pointers(node.data, node.id)
	for ptr in pointers {
		child, _ := utils.read_u32_be(node.data, int(ptr))
		collect_pages(t, child, pages)
	}

	right_ptr := get_right_ptr(node.data, node.id)
	if right_ptr != 0 {
		collect_pages(t, right_ptr, pages)
	}
}
