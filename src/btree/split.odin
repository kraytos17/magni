package btree

import "core:encoding/endian"
import "src:cell"
import "src:pager"
import "src:types"

Split_Result :: struct {
	did_split:  bool,
	right_page: u32,
	split_key:  types.Row_ID,
}

node_move_leaf_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if !is_leaf(src^) || !is_leaf(dst^) || count == 0 { return count == 0 }

	src_ptrs := get_pointers(src.data, src.id)
	if start_idx + count > len(src_ptrs) { return false }

	hdr_sz := page_header_size(dst.header.page_type)
	base := get_page_header_offset(dst.id)
	dst_off := int(dst.header.cell_content_offset)
	for i in 0 ..< count {
		idx := start_idx + i
		src_ptr := int(src_ptrs[idx])
		cell_sz, ok := cell.get_size(src.data, src_ptr)
		if !ok { return false }

		dst_off -= cell_sz
		copy(dst.data[dst_off:dst_off + cell_sz], src.data[src_ptr:src_ptr + cell_sz])
		ptr_loc := base + hdr_sz + (int(dst.header.cell_count) + i) * 2
		endian.put_u16(dst.data[ptr_loc:], .Little, u16(dst_off))
	}

	dst.header.cell_content_offset = u16le(dst_off)
	dst.header.cell_count = u16le(int(dst.header.cell_count) + count)
	return true
}

node_move_interior_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if is_leaf(src^) || is_leaf(dst^) || count == 0 { return count == 0 }

	ptrs := get_pointers(src.data, src.id)
	dst_int := node_interior(dst^)
	hdr_sz := size_of(Interior_Header)

	base := get_page_header_offset(dst.id)
	dst_off := int(dst_int.cell_content_offset)
	for i in 0 ..< count {
		off := int(ptrs[start_idx + i])
		size := interior_cell_size_from_page(src.data, off)
		dst_off -= size

		copy(dst.data[dst_off:dst_off + size], src.data[off:off + size])
		ptr_loc := base + hdr_sz + (int(dst_int.cell_count) + i) * 2
		endian.put_u16(dst.data[ptr_loc:], .Little, u16(dst_off))
	}

	dst_int.cell_content_offset = u16le(dst_off)
	dst_int.cell_count = u16le(int(dst_int.cell_count) + count)
	return true
}

split_leaf_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	if node_leaf(curr^).cell_count == 0 { return {}, .Page_Full }

	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }

	defer pager.unpin_page(t.pager, new_page.page_num)
	init_leaf_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(new_page.page_num, new_page.data)

	total := int(node_leaf(curr^).cell_count)
	mid := total / 2
	if !node_move_leaf_cells(curr, &right_node, mid, total - mid) {
		return {}, .Serialization_Failed
	}
	if mid > 0 {
		curr_ptrs := get_pointers(curr.data, curr.id)
		hdr_sz := page_header_size(curr.header.page_type)
		base := get_page_header_offset(curr.id)
		dst_off := PAGE_SIZE
		for i in 0 ..< mid {
			src_ptr := int(curr_ptrs[i])
			cell_sz, ok := cell.get_size(curr.data, src_ptr)
			if !ok { return {}, .Serialization_Failed }

			dst_off -= cell_sz
			copy(curr.data[dst_off:dst_off + cell_sz], curr.data[src_ptr:src_ptr + cell_sz])
			ptr_loc := base + hdr_sz + i * 2
			endian.put_u16(curr.data[ptr_loc:], .Little, u16(dst_off))
		}

		curr.header.cell_content_offset = u16le(dst_off)
		curr.header.cell_count = u16le(mid)
	}

	ptrs := get_pointers(right_node.data, right_node.id)
	sep, ok := cell.get_rowid(right_node.data, int(ptrs[0]))
	if !ok { return {}, .Invalid_Cell_Pointer }

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, right_node.id)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

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
	sep_u64, _, ok := cell.varint_decode(curr.data, int(mid_ptr) + 4)
	if !ok { return {}, .Invalid_Cell_Pointer }

	sep := types.Row_ID(sep_u64)
	child_from_mid_cell, _ := endian.get_u32(curr.data[int(mid_ptr):], .Big)
	count_right := total - (mid + 1)
	if count_right > 0 { node_move_interior_cells(curr, &right_node, mid + 1, count_right) }

	orig_rightmost := get_right_ptr(curr.data, curr.id)
	set_right_ptr(right_node.data, right_node.id, orig_rightmost)
	if mid > 0 {
		hdr_sz := size_of(Interior_Header)
		base := get_page_header_offset(curr.id)
		dst_off := PAGE_SIZE
		for i in 0 ..< mid {
			off := int(ptrs[i])
			size := interior_cell_size_from_page(curr.data, off)
			dst_off -= size

			copy(curr.data[dst_off:dst_off + size], curr.data[off:off + size])
			ptr_loc := base + hdr_sz + i * 2
			endian.put_u16(curr.data[ptr_loc:], .Little, u16(dst_off))
		}

		curr_int := node_interior(curr^)
		curr_int.cell_content_offset = u16le(dst_off)
		curr_int.cell_count = u16le(mid)
	} else {
		curr.header.cell_content_offset = PAGE_SIZE
		curr.header.cell_count = 0
	}

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
	if !node_move_leaf_cells(&root_node, &left_node, 0, mid) {
		return 0, .Serialization_Failed
	}
	if !node_move_leaf_cells(&root_node, &right_node, mid, total - mid) {
		return 0, .Serialization_Failed
	}

	ptrs := get_pointers(right_node.data, right_node.id)
	sep, s_ok := cell.get_rowid(right_node.data, int(ptrs[0]))
	if !s_ok { return 0, .Invalid_Cell_Pointer }

	init_interior_page(root_node.data, root_node.id)
	pager.mark_dirty(t.pager, root_node.id)
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
	if is_leaf(root_node) { unpin_node(t, root_node); return .Invalid_Page_Header }

	total := int(node_interior(root_node).cell_count)
	if !node_move_interior_cells(
		&root_node,
		&left_node,
		0,
		total,
	) { unpin_node(t, root_node); return .Serialization_Failed }

	old_right := get_right_ptr(root_node.data, root_node.id)
	set_right_ptr(left_node.data, left_node.id, old_right)
	init_interior_page(root_node.data, root_node.id)

	pager.mark_dirty(t.pager, t.root)
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
