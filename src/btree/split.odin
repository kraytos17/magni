package btree

import "core:encoding/endian"
import "src:cell"
import "src:pager"
import "src:util/varint"
import "src:types"

Split_Result :: struct #all_or_none {
	did_split:  bool,
	right_page: u32,
	split_key:  types.Row_ID,
}

@(private)
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

@(private)
node_move_leaf_cells_v2 :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if !is_leaf(src^) || !is_leaf(dst^) || count == 0 { return count == 0 }

	dst_off := int(dst.header.cell_content_offset)
	dst_cell_count := int(dst.header.cell_count)
	for i in 0 ..< count {
		idx := start_idx + i
		src_ptr := get_cell_ptr(src.data, src.id, idx, CELL_ENTRY_STRIDE)
		cell_sz, ok := cell.get_size(src.data, int(src_ptr))
		if !ok { return false }

		dst_off -= cell_sz
		copy(dst.data[dst_off:dst_off + cell_sz], src.data[int(src_ptr):int(src_ptr) + cell_sz])

		raw_dst := get_raw_entries(dst.data, dst.id)
		raw_src := get_raw_entries(src.data, src.id)
		raw_dst[dst_cell_count + i] = Cell_Entry {
			ptr = Cell_Pointer(u16(dst_off)),
			key = raw_src[idx].key,
		}
	}

	dst.header.cell_content_offset = u16le(dst_off)
	dst.header.cell_count = u16le(dst_cell_count + count)
	return true
}

@(private)
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

@(private)
node_move_interior_cells_v2 :: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool {
	if is_leaf(src^) || is_leaf(dst^) || count == 0 { return count == 0 }

	dst_int := node_interior(dst^)
	dst_off := int(dst_int.cell_content_offset)
	dst_cell_count := int(dst_int.cell_count)
	for i in 0 ..< count {
		idx := start_idx + i
		ptr := get_cell_ptr(src.data, src.id, idx, CELL_ENTRY_STRIDE)
		cell_sz := interior_cell_size_from_page(src.data, int(ptr))
		dst_off -= cell_sz

		copy(dst.data[dst_off:dst_off + cell_sz], src.data[int(ptr):int(ptr) + cell_sz])

		raw_dst := get_raw_entries(dst.data, dst.id)
		raw_src := get_raw_entries(src.data, src.id)
		raw_dst[dst_cell_count + i] = Cell_Entry {
			ptr = Cell_Pointer(u16(dst_off)),
			key = raw_src[idx].key,
		}
	}

	dst_int.cell_content_offset = u16le(dst_off)
	dst_int.cell_count = u16le(dst_cell_count + count)
	return true
}

@(private)
split_leaf_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	if node_leaf(curr^).cell_count == 0 { return {}, .Page_Full }
	// If the page is columnar, convert to row-major before splitting
	if is_columnar(curr.data, curr.id) {
		num_cols, found := detect_columnar_col_count(curr.data, curr.id)
		if !found { return {}, .Page_Full }

		row_count := int(curr.header.cell_count)
		boff := get_page_header_offset(curr.id)
		rowids := make([]types.Row_ID, row_count, context.temp_allocator)
		for i in 0 ..< row_count {
			rid, rid_ok := cell.read_columnar_rowid(curr.data, num_cols, i, boff)
			if !rid_ok { return {}, .Serialization_Failed }
			rowids[i] = rid
		}

		values := make([][]types.Value, row_count, context.temp_allocator)
		for col_i in 0 ..< num_cols {
			col_vals := cell.decode_column(
				curr.data,
				num_cols,
				col_i,
				boff,
				context.temp_allocator,
			)
			if col_vals == nil { return {}, .Serialization_Failed }
			for ri in 0 ..< row_count {
				if values[ri] == nil {
					values[ri] = make([]types.Value, num_cols, context.temp_allocator)
				}
				if ri < len(col_vals) {
					values[ri][col_i] = col_vals[ri]
				}
			}
		}

		// Reinitialize page as row-major and insert all rows
		init_leaf_page(curr.data, curr.id)
		for ri in 0 ..< row_count {
			info := cell.compute_info(rowids[ri], values[ri])
			off := get_page_header_offset(curr.id)
			header := (^Leaf_Header)(raw_data(curr.data[off:]))
			dest_off := int(header.cell_content_offset) - info.total_size
			if dest_off < off + size_of(Leaf_Header) + (int(header.cell_count) + 1) * 2 {
				return {}, .Page_Full
			}

			cell.serialize(
				curr.data[dest_off:dest_off + info.total_size],
				rowids[ri],
				values[ri],
				info,
			)

			header.cell_content_offset = u16le(dest_off)
			ptr_loc := off + size_of(Leaf_Header) + int(header.cell_count) * 2
			endian.put_u16(curr.data[ptr_loc:], .Little, u16(dest_off))
			header.cell_count = u16le(int(header.cell_count) + 1)
		}
	}

	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }

	defer pager.unpin_page(t.pager, new_page.page_num)
	init_leaf_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(
		new_page.page_num,
		new_page.data,
		get_layout(t.pager.page_format_version),
	)

	total := int(node_leaf(curr^).cell_count)
	mid := total / 2
	if !curr.layout.move_leaf(curr, &right_node, mid, total - mid) {
		return {}, .Serialization_Failed
	}
	if mid > 0 {
		// Snapshot all original cell bodies to a temp buffer before repacking:
		// with random insert order the bodies are not offset-contiguous, so an
		// in-place downward pack would overwrite unprocessed source cells.
		Cell_Ref :: struct {
			off: int,
			sz:  int,
		}

		refs := make([]Cell_Ref, total, context.temp_allocator)
		total_sz := 0
		for i in 0 ..< total {
			off := int(get_cell_ptr(curr.data, curr.id, i, curr.layout.stride))
			sz, ok := cell.get_size(curr.data, off)
			if !ok { return {}, .Serialization_Failed }

			refs[i] = Cell_Ref{off = off, sz = sz}
			total_sz += sz
		}

		buf := make([]u8, total_sz, context.temp_allocator)
		pos := 0
		for i in 0 ..< total {
			copy(buf[pos:pos + refs[i].sz], curr.data[refs[i].off:refs[i].off + refs[i].sz])
			pos += refs[i].sz
		}

		dst_off := PAGE_SIZE
		pos = 0
		for i in 0 ..< mid {
			sz := refs[i].sz
			dst_off -= sz
			copy(curr.data[dst_off:dst_off + sz], buf[pos:pos + sz])
			pos += sz
			curr.layout.set_entry(
				curr.data,
				curr.id,
				i,
				u16(dst_off),
				get_cell_key(curr.data, curr.id, i, curr.layout),
			)
		}

		curr.header.cell_content_offset = u16le(dst_off)
		curr.header.cell_count = u16le(mid)
	}

	sep := get_cell_key(right_node.data, right_node.id, 0, right_node.layout)
	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, right_node.id)
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

@(private)
split_interior_node :: proc(t: ^Tree, curr: ^Node) -> (Split_Result, Error) {
	new_page, err := pager.allocate_page(t.pager)
	if err != nil { return {}, .Page_Full }

	defer pager.unpin_page(t.pager, new_page.page_num)
	init_interior_page(new_page.data, new_page.page_num)
	right_node, _ := node_from_bytes(
		new_page.page_num,
		new_page.data,
		get_layout(t.pager.page_format_version),
	)

	total := int(node_interior(curr^).cell_count)
	mid := total / 2

	sep: types.Row_ID
	child_from_mid_cell: u32
	if total == 0 { return {}, .Invalid_Cell_Pointer }

	mid_ptr := int(get_cell_ptr(curr.data, curr.id, mid, curr.layout.stride))
	sep_u64, _, ok := varint.decode(curr.data, mid_ptr + 4)
	if !ok { return {}, .Invalid_Cell_Pointer }

	sep = types.Row_ID(sep_u64)
	child_from_mid_cell, _ = endian.get_u32(curr.data[mid_ptr:], .Big)
	count_right := total - (mid + 1)
	if count_right > 0 {
		if !curr.layout.move_interior(curr, &right_node, mid + 1, count_right) {
			return {}, .Serialization_Failed
		}
	}

	orig_rightmost := get_right_ptr(curr.data, curr.id)
	set_right_ptr(right_node.data, right_node.id, orig_rightmost)
	if mid > 0 {
		// Snapshot all original interior cell bodies to a temp buffer before
		// repacking (interior cells are also non-offset-contiguous after splits).
		Cell_Ref :: struct { off: int, sz: int }

		refs := make([]Cell_Ref, total, context.temp_allocator)
		total_sz := 0
		for i in 0 ..< total {
			off := int(get_cell_ptr(curr.data, curr.id, i, curr.layout.stride))
			sz := interior_cell_size_from_page(curr.data, off)
			if sz == 0 { return {}, .Serialization_Failed }

			refs[i] = Cell_Ref{off = off, sz = sz}
			total_sz += sz
		}

		buf := make([]u8, total_sz, context.temp_allocator)
		pos := 0
		for i in 0 ..< total {
			copy(buf[pos:pos + refs[i].sz], curr.data[refs[i].off:refs[i].off + refs[i].sz])
			pos += refs[i].sz
		}

		dst_off := PAGE_SIZE
		pos = 0
		for i in 0 ..< mid {
			sz := refs[i].sz
			dst_off -= sz
			copy(curr.data[dst_off:dst_off + sz], buf[pos:pos + sz])
			pos += sz
			curr.layout.set_entry(
				curr.data,
				curr.id,
				i,
				u16(dst_off),
				get_cell_key(curr.data, curr.id, i, curr.layout),
			)
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
	return Split_Result{did_split = true, right_page = right_node.id, split_key = sep}, .None
}

@(private)
split_leaf_root :: proc(
	t: ^Tree,
	root_page: u32,
	rowid: Maybe(types.Row_ID) = nil,
	values: Maybe([]types.Value) = nil,
) -> (new_root: u32, err: Error) {
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

	l := get_layout(t.pager.page_format_version)
	left_node, _ := node_from_bytes(left_page.page_num, left_page.data, l)
	right_node, _ := node_from_bytes(right_page.page_num, right_page.data, l)
	root_node, load_err := load_node(t, root_page)
	if load_err != .None { return 0, load_err }

	defer unpin_node(t, root_node)
	if !is_leaf(root_node) { return 0, .Invalid_Page_Header }
	if node_leaf(root_node).cell_count == 0 { return 0, .Page_Full }

	total := int(node_leaf(root_node).cell_count)
	mid := total / 2
	if !root_node.layout.move_leaf(&root_node, &left_node, 0, mid) {
		return 0, .Serialization_Failed
	}
	if !root_node.layout.move_leaf(&root_node, &right_node, mid, total - mid) {
		return 0, .Serialization_Failed
	}

	sep := get_cell_key(right_node.data, right_node.id, 0, right_node.layout)
	// Insert the row that overflowed the leaf into the correct half. The COW
	// caller passes it here; the non-COW caller re-inserts it via insert_recursive.
	if rid, has_rid := rowid.?; has_rid {
		vals, has_vals := values.?
		if !has_vals { return 0, .Serialization_Failed }
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

	init_interior_page(root_node.data, root_node.id)
	pager.mark_dirty(t.pager, root_node.id)
	unpin_node(t, root_node)

	root_node, load_err = load_node(t, root_page)
	if load_err != .None { return 0, load_err }

	set_right_ptr(root_node.data, root_node.id, right_node.id)
	insert_interior_cell(root_node.data, root_node.id, left_node.id, sep, root_node.layout)
	pager.mark_dirty(t.pager, left_node.id)
	pager.mark_dirty(t.pager, right_node.id)
	pager.mark_dirty(t.pager, root_node.id)
	unpin_node(t, root_node)
	return root_page, .None
}

@(private)
split_interior_root :: proc(t: ^Tree, split: Split_Result) -> Error {
	left_page, err := pager.allocate_page(t.pager)
	if err != nil { return .Page_Full }

	defer pager.unpin_page(t.pager, left_page.page_num)
	init_interior_page(left_page.data, left_page.page_num)
	left_node, _ := node_from_bytes(
		left_page.page_num,
		left_page.data,
		get_layout(t.pager.page_format_version),
	)

	root_node := load_node(t, t.root) or_return
	if is_leaf(root_node) { unpin_node(t, root_node); return .Invalid_Page_Header }

	total := int(node_interior(root_node).cell_count)
	if !root_node.layout.move_interior(&root_node, &left_node, 0, total) {
		unpin_node(t, root_node)
		return .Serialization_Failed
	}

	old_right := get_right_ptr(root_node.data, root_node.id)
	set_right_ptr(left_node.data, left_node.id, old_right)
	init_interior_page(root_node.data, root_node.id)

	pager.mark_dirty(t.pager, t.root)
	unpin_node(t, root_node)
	root_node = load_node(t, t.root) or_return

	set_right_ptr(root_node.data, root_node.id, split.right_page)
	insert_interior_cell(
		root_node.data,
		root_node.id,
		left_node.id,
		split.split_key,
		root_node.layout,
	)

	pager.mark_dirty(t.pager, t.root)
	pager.mark_dirty(t.pager, left_node.id)
	unpin_node(t, root_node)
	return .None
}
