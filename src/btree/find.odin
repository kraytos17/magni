package btree

import "base:intrinsics"
import "core:encoding/endian"
import "src:types"
import "src:util/varint"

@(private)
find_interior_cell_for_child :: #force_inline proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	layout: Page_Layout,
) -> int {
	cell_count := get_cell_count(data, page_id)
	pid := Page_Id(page_id)
	for i in 0 ..< cell_count {
		ptr, p_err := layout.vtable.cell_ptr_at(data, pid, i)
		if p_err != .None { continue }

		stored_child, ok := endian.get_u32(data[int(ptr):], .Big)
		if intrinsics.unlikely(!ok) { continue }
		if stored_child == child_page {
			return i
		}
	}
	return -1
}

@(private)
interior_lower_bound :: #force_inline proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: Page_Layout,
) -> (
	int,
	bool,
) {
	cell_count := get_cell_count(data, page_id)
	pid := Page_Id(page_id)
	left := 0
	right := cell_count
	for left < right {
		mid := left + (right - left) / 2
		k, k_err := layout.vtable.key_at(data, pid, mid)
		if k_err != .None { return left, false }
		if key >= k {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}

@(private = "file")
find_interior_insert_index :: #force_inline proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: Page_Layout,
) -> int {
	idx, _ := interior_lower_bound(data, page_id, key, layout)
	return idx
}

@(private = "file")
interior_cell_size :: #force_inline proc(key: types.Row_ID) -> int {
	return 4 + varint.size(u64(key))
}

@(private)
interior_cell_size_from_page :: proc(data: []u8, offset: int) -> int {
	_, n, ok := varint.decode(data, offset + 4)
	if !ok { return 0 }
	return 4 + n
}

@(private, require_results)
insert_interior_cell :: proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	key: types.Row_ID,
	layout: Page_Layout,
) -> bool {
	header := get_interior_header(data, page_id)
	if header == nil { return false }

	size := interior_cell_size(key)
	hdr_sz := size_of(Interior_Header)
	base_off := get_page_header_offset(page_id)
	ptrs_end := base_off + hdr_sz + int(header.cell_count + 1) * CELL_ENTRY_STRIDE
	content_start := int(header.cell_content_offset)
	if ptrs_end + size > content_start {
		return false
	}

	new_offset := content_start - size
	header.cell_content_offset = u16le(new_offset)
	endian.put_u32(data[new_offset:], .Big, child_page)
	varint.encode(data[new_offset + 4:], u64(key))
	insert_idx := find_interior_insert_index(data, page_id, key, layout)
	if layout.vtable.slot_insert(
		   data,
		   Page_Id(page_id),
		   insert_idx,
		   key,
		   Cell_Off(u16(new_offset)),
	   ) !=
	   .None {
		header.cell_content_offset = u16le(content_start)
		return false
	}

	header.cell_count += 1
	return true
}
