package btree

import "core:encoding/endian"
import "src:types"
import "src:util/varint"

@(private)
find_interior_cell_for_child :: proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	layout: ^Cell_Layout,
) -> int {
	cell_count := get_cell_count(data, page_id)
	for i in 0 ..< cell_count {
		ptr := get_cell_ptr(data, page_id, i, layout.stride)
		stored_child, _ := endian.get_u32(data[int(ptr):], .Big)
		if stored_child == child_page {
			return i
		}
	}
	return -1
}

@(private)
interior_lower_bound :: proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: ^Cell_Layout,
) -> (
	int,
	bool,
) {
	cell_count := get_cell_count(data, page_id)
	left := 0
	right := cell_count
	for left < right {
		mid := left + (right - left) / 2
		if key >= get_cell_key(data, page_id, mid, layout) {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}


@(private="file")
find_interior_insert_index :: proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: ^Cell_Layout,
) -> int {
	idx, _ := interior_lower_bound(data, page_id, key, layout)
	return idx
}


@(private="file")
interior_cell_size :: proc(key: types.Row_ID) -> int {
	return 4 + varint.size(u64(key))
}


@(private)
interior_cell_size_from_page :: proc(data: []u8, offset: int) -> int {
	_, n, ok := varint.decode(data, offset + 4)
	if !ok { return 0 }
	return 4 + n
}


@(private)
insert_interior_cell :: proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	key: types.Row_ID,
	layout: ^Cell_Layout,
) -> bool {
	header := get_interior_header(data, page_id)
	if header == nil { return false }

	size := interior_cell_size(key)
	hdr_sz := size_of(Interior_Header)
	base_off := get_page_header_offset(page_id)
	entry_sz := layout.stride
	ptrs_end := base_off + hdr_sz + int(header.cell_count + 1) * entry_sz
	content_start := int(header.cell_content_offset)
	if ptrs_end + size > content_start {
		return false
	}

	new_offset := content_start - size
	header.cell_content_offset = u16le(new_offset)
	endian.put_u32(data[new_offset:], .Big, child_page)
	varint.encode(data[new_offset + 4:], u64(key))
	insert_idx := find_interior_insert_index(data, page_id, key, layout)

	// Shift entries right at insert_idx
	if insert_idx < int(header.cell_count) {
		src := data[base_off +
		hdr_sz +
		insert_idx * entry_sz:base_off +
		hdr_sz +
		int(header.cell_count) * entry_sz]
		dst := data[base_off + hdr_sz + (insert_idx + 1) * entry_sz:]
		copy(dst, src)
	}

	layout.set_entry(data, page_id, insert_idx, u16(new_offset), key)
	header.cell_count += 1
	return true
}

// Freeblock format:
//   [offset+0]: next freeblock offset (u16le, 0 = end of list)
//   [offset+2]: block size        (u16le, total bytes including header)
