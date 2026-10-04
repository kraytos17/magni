package btree

import "src:cell"
import "src:pager"
import "src:types"

Split_Result :: struct #all_or_none {
	did_split : bool,
	right_page: u32,
	split_key : types.Row_ID,
}

// move_src_count reports the source entry count for the pre-move bounds
// check: entries are fixed-stride, so the header count rules.
@(private = "file")
move_src_count :: proc(src: ^Node) -> int {
	return int(src.header.cell_count)
}

// move_leaf_cells moves count cells to a leaf sibling through the layout
// primitives. dst is a fresh page: its count is pre-bumped so slot_repoint
// (which range-checks) accepts every slot; content_offset lands at the end.
@(private)
move_leaf_cells :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if !is_leaf(src^) || !is_leaf(dst^) || count == 0 {
		return count == 0
	}
	if start_idx + count > move_src_count(src) {
		return false
	}

	dst_off := int(dst.header.cell_content_offset)
	dst_cell_count := int(dst.header.cell_count)
	dst.header.cell_count = u16le(dst_cell_count + count)
	for i in 0 ..< count {
		idx := start_idx + i
		src_ptr, p_err := src.layout.vtable.cell_ptr_at(src.data, Page_Id(src.id), idx)
		if p_err != .None {
			return false
		}

		src_key, k_err := src.layout.vtable.key_at(src.data, Page_Id(src.id), idx)
		if k_err != .None {
			return false
		}

		cell_sz, ok := cell.get_size(src.data, int(src_ptr))
		if !ok {
			return false
		}

		dst_off -= cell_sz
		copy(dst.data[dst_off:dst_off + cell_sz], src.data[int(src_ptr):int(src_ptr) + cell_sz])
		if rp_err := dst.layout.vtable.slot_repoint(
			dst.data,
			Page_Id(dst.id),
			dst_cell_count + i,
			src_key,
			Cell_Off(u16(dst_off)),
		); rp_err != .None {
			return false
		}
	}

	dst.header.cell_content_offset = u16le(dst_off)
	return true
}

// pack_left_half repacks the first `mid` cells of `curr` to the top of the page
// (downward, compact) and sets the header count/offset. Cells are snapshotted to
// a temp buffer first: after random-order inserts/splits the bodies are not
// offset-contiguous, so an in-place downward pack would overwrite unprocessed
// source cells. `cell_size_at` supplies each cell's size from its offset.
@(private = "file")
pack_left_half :: proc(
	curr: ^Node,
	mid: int,
	cell_size_at: proc(data: []u8, off: int) -> (int, bool),
) -> bool {
	total := int(curr.header.cell_count)
	if mid <= 0 {
		curr.header.cell_content_offset = PAGE_SIZE
		curr.header.cell_count = 0
		return true
	}

	refs := make([]int, total, context.temp_allocator) // offset per cell
	sizes := make([]int, total, context.temp_allocator)
	total_sz := 0
	for i in 0 ..< total {
		off, p_err := curr.layout.vtable.cell_ptr_at(curr.data, Page_Id(curr.id), i)
		if p_err != .None {
			return false
		}

		sz, ok := cell_size_at(curr.data, int(off))
		if !ok {
			return false
		}

		refs[i] = int(off)
		sizes[i] = sz
		total_sz += sz
	}

	buf := make([]u8, total_sz, context.temp_allocator)
	pos := 0
	for i in 0 ..< total {
		copy(buf[pos:pos + sizes[i]], curr.data[refs[i]:refs[i] + sizes[i]])
		pos += sizes[i]
	}

	dst_off := PAGE_SIZE
	pos = 0
	for i in 0 ..< mid {
		sz := sizes[i]
		dst_off -= sz
		copy(curr.data[dst_off:dst_off + sz], buf[pos:pos + sz])

		pos += sz
		// Repack writes slots 0..mid-1 in place; the header still holds the
		// full count here, so the range check passes by construction.
		key, k_err := curr.layout.vtable.key_at(curr.data, Page_Id(curr.id), i)
		if k_err != .None {
			return false
		}
		if rp_err := curr.layout.vtable.slot_repoint(
			curr.data,
			Page_Id(curr.id),
			i,
			key,
			Cell_Off(u16(dst_off)),
		); rp_err != .None {
			return false
		}
	}

	curr.header.cell_content_offset = u16le(dst_off)
	curr.header.cell_count = u16le(mid)
	return true
}

// leaf_cell_size adapts the cell-size query to pack_left_half's function
// parameter.
@(private = "file")
leaf_cell_size :: proc(data: []u8, off: int) -> (int, bool) {
	return cell.get_size(data, off)
}

@(private)
split_leaf_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	if node_leaf(curr^).cell_count == 0 {
		return {}, .Page_Full
	}
	// Slotdir-only: V2 leaves cannot occur (unloadable since full
	// migration) — anything else fails fast, never reinterpreted.
	if curr.header.page_type != .LEAF_SLOTDIR {
		return {}, .Invalid_Page_Header
	}

	cl, _, cl_err := layout_for_page(curr.data, Page_Id(curr.id))
	if cl_err != .None {
		return {}, cl_err
	}

	curr.layout = cl
	new_page, err := pager.allocate_page(t.pager)
	if err != nil {
		return {}, .Page_Full
	}

	defer pager.unpin_page(t.pager, new_page.page_num)
	if !init_slot_leaf_page(new_page.data, new_page.page_num) {
		return {}, .Invalid_Page_Header
	}

	right_layout, _, rl_err := layout_for_page(new_page.data, Page_Id(new_page.page_num))
	if rl_err != .None {
		return {}, rl_err
	}

	right_node, _ := node_from_bytes(new_page.page_num, new_page.data, right_layout)
	total := int(node_leaf(curr^).cell_count)
	mid := total / 2
	if !move_leaf_cells(curr, &right_node, mid, total - mid) {
		return {}, .Serialization_Failed
	}
	if !pack_left_half(curr, mid, leaf_cell_size) {
		return {}, .Serialization_Failed
	}

	sep, s_err := right_node.layout.vtable.key_at(right_node.data, Page_Id(right_node.id), 0)
	if s_err != .None {
		return {}, .Invalid_Cell_Pointer
	}

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, right_node.id)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

@(private)
split_interior_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	pid := Page_Id(curr.id)
	total := int(curr.header.cell_count)
	if total == 0 {
		return {}, .Invalid_Cell_Pointer
	}

	keys := make([dynamic]types.Row_ID, 0, total, context.temp_allocator)
	children := make([dynamic]u32, 0, total + 1, context.temp_allocator)
	for i in 0 ..< total {
		k, k_err := curr.layout.vtable.key_at(curr.data, pid, i)
		if k_err != .None {
			return {}, k_err
		}

		append(&keys, k)
		c, c_err := curr.layout.vtable.child_at(curr.data, pid, i)
		if c_err != .None {
			return {}, c_err
		}
		append(&children, c)
	}

	rc, rc_err := curr.layout.vtable.child_at(curr.data, pid, total)
	if rc_err != .None {
		return {}, rc_err
	}
	append(&children, rc)

	mid := dense_split_mid(total)
	sep := keys[mid]
	new_page, err := pager.allocate_page(t.pager)
	if err != nil {
		return {}, .Page_Full
	}

	defer pager.unpin_page(t.pager, new_page.page_num)
	if lb_err := dense_build_from_sorted(curr.data, pid, keys[:mid], children[:mid + 1]);
	   lb_err != .None {
		return {}, lb_err
	}
	if rb_err := dense_build_from_sorted(
		new_page.data,
		Page_Id(new_page.page_num),
		keys[mid + 1:],
		children[mid + 1:],
	); rb_err != .None {
		return {}, rb_err
	}

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, new_page.page_num)
	return Split_Result{did_split = true, right_page = new_page.page_num, split_key = sep}, .None
}

@(private)
split_leaf_root :: proc(
	t: ^Tree,
	root_page: u32,
	rowid: Maybe(types.Row_ID) = nil,
	values: Maybe([]types.Value) = nil,
) -> (
	new_root: u32,
	err: Error,
) {
	left_page, l_err := pager.allocate_page(t.pager)
	if l_err != .None {
		return 0, .Page_Full
	}

	left_id := left_page.page_num
	defer pager.unpin_page(t.pager, left_id)

	right_page, r_err := pager.allocate_page(t.pager)
	if r_err != .None {
		return 0, .Page_Full
	}

	right_id := right_page.page_num
	defer pager.unpin_page(t.pager, right_id)
	if !init_slot_leaf_page(left_page.data, left_page.page_num) {
		return 0, .Invalid_Page_Header
	}
	if !init_slot_leaf_page(right_page.data, right_page.page_num) {
		return 0, .Invalid_Page_Header
	}

	l_layout, _ := layout_for_page(left_page.data, Page_Id(left_page.page_num)) or_return
	left_node, _ := node_from_bytes(left_page.page_num, left_page.data, l_layout)
	r_layout, _ := layout_for_page(right_page.data, Page_Id(right_page.page_num)) or_return
	right_node, _ := node_from_bytes(right_page.page_num, right_page.data, r_layout)
	root_node, load_err := load_node(t, root_page)
	if load_err != .None {
		return 0, load_err
	}

	defer unpin_node(t, root_node)
	if !is_leaf(root_node) {
		return 0, .Invalid_Page_Header
	}
	if node_leaf(root_node).cell_count == 0 {
		return 0, .Page_Full
	}
	if root_node.header.page_type != .LEAF_SLOTDIR {
		return 0, .Invalid_Page_Header
	}

	rl, _, rl_err := layout_for_page(root_node.data, Page_Id(root_node.id))
	if rl_err != .None {
		return 0, rl_err
	}

	root_node.layout = rl
	total := int(node_leaf(root_node).cell_count)
	mid := total / 2
	if !move_leaf_cells(&root_node, &left_node, 0, mid) {
		return 0, .Serialization_Failed
	}
	if !move_leaf_cells(&root_node, &right_node, mid, total - mid) {
		return 0, .Serialization_Failed
	}

	sep, s_err := right_node.layout.vtable.key_at(right_node.data, Page_Id(right_node.id), 0)
	if s_err != .None {
		return 0, .Invalid_Cell_Pointer
	}
	if rid, has_rid := rowid.?; has_rid {
		vals, has_vals := values.?
		if !has_vals {
			return 0, .Serialization_Failed
		}
		if rid >= sep {
			if e := node_insert_leaf_cell(t, &right_node, rid, vals); e != .None {
				return 0, e
			}
		} else {
			if e := node_insert_leaf_cell(t, &left_node, rid, vals); e != .None {
				return 0, e
			}
		}
	}

	rkeys := [1]types.Row_ID{sep}
	rchildren := [2]u32{left_node.id, right_node.id}
	if rb_err := dense_build_from_sorted(
		root_node.data,
		Page_Id(root_page),
		rkeys[:],
		rchildren[:],
	); rb_err != .None {
		return 0, rb_err
	}

	pager.mark_dirty(t.pager, left_node.id)
	pager.mark_dirty(t.pager, right_node.id)
	pager.mark_dirty(t.pager, root_node.id)
	return root_page, .None
}

@(private)
split_interior_root :: proc(t: ^Tree, split: Split_Result) -> (err: Error) {
	root_node := load_node(t, t.root) or_return
	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		return .Invalid_Page_Header
	}

	rpid := Page_Id(t.root)
	total := int(root_node.header.cell_count)
	keys := make([dynamic]types.Row_ID, 0, total, context.temp_allocator)
	children := make([dynamic]u32, 0, total + 1, context.temp_allocator)
	for i in 0 ..< total {
		k, k_err := root_node.layout.vtable.key_at(root_node.data, rpid, i)
		if k_err != .None {
			return k_err
		}

		append(&keys, k)
		c, c_err := root_node.layout.vtable.child_at(root_node.data, rpid, i)
		if c_err != .None {
			return c_err
		}
		append(&children, c)
	}

	rc, rc_err := root_node.layout.vtable.child_at(root_node.data, rpid, total)
	if rc_err != .None {
		return rc_err
	}
	append(&children, rc)

	left_page, a_err := pager.allocate_page(t.pager)
	if a_err != nil {
		return .Page_Full
	}
	defer pager.unpin_page(t.pager, left_page.page_num)
	if lb_err := dense_build_from_sorted(
		left_page.data,
		Page_Id(left_page.page_num),
		keys[:],
		children[:],
	); lb_err != .None {
		return lb_err
	}

	rkeys := [1]types.Row_ID{split.split_key}
	rchildren := [2]u32{left_page.page_num, split.right_page}
	if rb_err := dense_build_from_sorted(root_node.data, rpid, rkeys[:], rchildren[:]);
	   rb_err != .None {
		return rb_err
	}

	pager.mark_dirty(t.pager, t.root)
	pager.mark_dirty(t.pager, left_page.page_num)
	return .None
}
