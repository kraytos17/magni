package btree

import "core:mem"
import "src:types"
import "src:utils"

// Interior Node Layout
// Header: 12 bytes
// Bytes 0-7: Standard Page Header
// Bytes 8-11: Right-most Child Pointer (u32)
Interior_Header :: struct #packed {
	using common:  Page_Header,
	rightmost_ptr: u32be,
}

#assert(size_of(Interior_Header) == 12)

Leaf_Header :: struct #packed {
	using common: Page_Header,
	next_leaf:    u32be,
}

#assert(size_of(Leaf_Header) == 12)

// Helper to cast raw bytes to Interior Header pointer, respecting Page 0 offset
get_interior_header :: proc(page_data: []u8, page_num: u32) -> ^Interior_Header {
	offset := get_page_header_offset(page_num)
	if len(page_data) < offset + size_of(Interior_Header) {
		return nil
	}
	return (^Interior_Header)(raw_data(page_data[offset:]))
}

get_leaf_header :: proc(page_data: []u8, page_num: u32) -> ^Leaf_Header {
	offset := get_page_header_offset(page_num)
	if len(page_data) < offset + size_of(Leaf_Header) {
		return nil
	}
	return (^Leaf_Header)(raw_data(page_data[offset:]))
}

init_interior_page :: proc(page_data: []u8, page_num: u32) {
	offset := get_page_header_offset(page_num)
	mem.zero_slice(page_data[offset:])
	header := (^Interior_Header)(raw_data(page_data[offset:]))
	header.page_type = .INTERIOR_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = u16le(len(page_data))
	header.fragmented_bytes = 0
	header.rightmost_ptr = 0
}

set_right_ptr :: proc(page_data: []u8, page_num: u32, right_ptr: u32) {
	header := get_interior_header(page_data, page_num)
	if header != nil {
		header.rightmost_ptr = u32be(right_ptr)
	}
}

get_right_ptr :: proc(page_data: []u8, page_num: u32) -> u32 {
	header := get_interior_header(page_data, page_num)
	if header == nil { return 0 }
	return u32(header.rightmost_ptr)
}

// Format: [LeftChildPtr(4b)] [Key(Varint)]
interior_cell_size :: proc(key: types.Row_ID) -> int {
	return 4 + utils.varint_size(u64(key))
}

interior_cell_size_from_page :: proc(page_data: []u8, cell_offset: int) -> int {
	_, bytes_read, ok := utils.varint_decode(page_data, cell_offset + 4)
	if !ok {
		return 0
	}
	return 4 + bytes_read
}

// Find the index where a key should be inserted in an Interior Node
find_interior_insert_index :: proc(page_data: []u8, page_num: u32, key: types.Row_ID) -> int {
	header := get_interior_header(page_data, page_num)
	if header == nil { return 0 }

	pointers := get_pointers(page_data, page_num)
	left := 0
	right := int(header.cell_count)
	for left < right {
		mid := left + (right - left) / 2
		ptr := pointers[mid]
		cell_offset := int(ptr)
		cell_key_val, _, _ := utils.varint_decode(page_data, cell_offset + 4)
		cell_key := types.Row_ID(cell_key_val)
		if cell_key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

insert_interior_cell :: proc(
	page_data: []u8,
	page_num: u32,
	child_page: u32,
	max_key: types.Row_ID,
) -> bool {
	header := get_interior_header(page_data, page_num)
	if header == nil { return false }

	size := interior_cell_size(max_key)
	header_size := 12
	offset_base := get_page_header_offset(page_num)
	pointers_end := offset_base + header_size + int(header.cell_count + 1) * size_of(Cell_Pointer)
	content_start := int(header.cell_content_offset)
	if pointers_end + size > content_start {
		return false
	}

	new_offset := content_start - size
	header.cell_content_offset = u16le(new_offset)
	utils.write_u32_be(page_data, new_offset, child_page)
	utils.varint_encode(page_data[new_offset + 4:], u64(max_key))

	insert_idx := find_interior_insert_index(page_data, page_num, max_key)
	ptr_start_idx := offset_base + header_size
	ptr_data := raw_data(page_data[ptr_start_idx:])
	raw_ptrs := ([^]Cell_Pointer)(ptr_data)[:header.cell_count + 1]
	if insert_idx < int(header.cell_count) {
		copy(raw_ptrs[insert_idx + 1:], raw_ptrs[insert_idx:header.cell_count])
	}

	raw_ptrs[insert_idx] = Cell_Pointer(new_offset)
	header.cell_count += 1
	return true
}
