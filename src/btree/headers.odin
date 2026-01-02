package btree

import "core:mem"
import "src:types"
import "src:utils"

PAGE_HEADER_OFFSET_ROOT :: 100
PAGE_SIZE :: 4096

Page_Type :: enum u8 {
	INTERIOR_TABLE = 5, // Internal node: pointers to pages
	LEAF_TABLE     = 13, // Leaf node: pointers to data
}

Cell_Pointer :: distinct u16le

Page_Header :: struct #packed {
	page_type:           Page_Type, // Byte 0
	first_freeblock:     u16le, // Bytes 1-2
	cell_count:          u16le, // Bytes 3-4
	cell_content_offset: u16le, // Bytes 5-6
	fragmented_bytes:    u8, // Byte 7
}
#assert(size_of(Page_Header) == 8)

Interior_Header :: struct #packed {
	using common:  Page_Header,
	rightmost_ptr: u32be,
}
#assert(size_of(Interior_Header) == 12)

Leaf_Header :: struct #packed {
	using common: Page_Header,
}
#assert(size_of(Leaf_Header) == 8)

get_page_header_offset :: proc(page_num: u32) -> int {
	return int(page_num == 1 ? PAGE_HEADER_OFFSET_ROOT : 0)
}

page_header_size :: proc(page_type: Page_Type) -> int {
	return int(page_type == .INTERIOR_TABLE ? size_of(Interior_Header) : size_of(Leaf_Header))
}

get_header :: proc(data: []u8, page_id: u32) -> ^Page_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Page_Header) { return nil }
	return (^Page_Header)(raw_data(data[off:]))
}

get_interior_header :: proc(data: []u8, page_id: u32) -> ^Interior_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Interior_Header) { return nil }
	return (^Interior_Header)(raw_data(data[off:]))
}

get_leaf_header :: proc(data: []u8, page_id: u32) -> ^Leaf_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) { return nil }
	return (^Leaf_Header)(raw_data(data[off:]))
}

init_interior_page :: proc(data: []u8, page_id: u32) {
	off := get_page_header_offset(page_id)
	mem.zero_slice(data)

	header := (^Interior_Header)(raw_data(data[off:]))
	header.page_type = .INTERIOR_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
	header.rightmost_ptr = 0
}

init_leaf_page :: proc(data: []u8, page_id: u32) {
	off := get_page_header_offset(page_id)
	mem.zero_slice(data)

	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .LEAF_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
}

get_pointers :: proc(data: []u8, page_id: u32) -> []Cell_Pointer {
	header := get_header(data, page_id)
	if header == nil { return nil }

	off := get_page_header_offset(page_id)
	hdr_sz := page_header_size(header.page_type)
	ptr_start := raw_data(data[off + hdr_sz:])
	return ([^]Cell_Pointer)(ptr_start)[:header.cell_count]
}

get_raw_pointers :: proc(data: []u8, page_id: u32) -> []Cell_Pointer {
	header := get_header(data, page_id)
	if header == nil { return nil }

	off := get_page_header_offset(page_id)
	hdr_sz := page_header_size(header.page_type)
	start := off + hdr_sz
	if start >= len(data) { return nil }

	ptr_start := raw_data(data[start:])
	max_ptrs := (len(data) - start) / size_of(Cell_Pointer)
	return ([^]Cell_Pointer)(ptr_start)[:max_ptrs]
}

get_right_ptr :: proc(data: []u8, page_id: u32) -> u32 {
	h := get_interior_header(data, page_id)
	if h == nil { return 0 }
	return u32(h.rightmost_ptr)
}

set_right_ptr :: proc(data: []u8, page_id: u32, ptr: u32) {
	h := get_interior_header(data, page_id)
	if h != nil {
		h.rightmost_ptr = u32be(ptr)
	}
}

node_get_leftmost_child :: proc(n: ^Node) -> u32 {
	if n.interior == nil {
		return 0
	}
	if n.header.cell_count > 0 {
		ptrs := get_pointers(n.data, n.id)
		first_offset := int(ptrs[0])
		child, _ := utils.read_u32_be(n.data, first_offset)
		return child
	}
	return u32(n.interior.rightmost_ptr)
}

find_interior_cell_for_child :: proc(data: []u8, page_id: u32, child_page: u32) -> int {
	pointers := get_pointers(data, page_id)
	for ptr, i in pointers {
		off := int(ptr)
		stored_child, _ := utils.read_u32_be(data, off)
		if stored_child == child_page {
			return i
		}
	}
	return -1
}

find_interior_insert_index :: proc(data: []u8, page_id: u32, key: types.Row_ID) -> int {
	header := get_header(data, page_id)
	if header == nil { return 0 }

	pointers := get_pointers(data, page_id)
	left := 0
	right := int(header.cell_count)
	for left < right {
		mid := left + (right - left) / 2
		off := int(pointers[mid])
		sep_val, _, ok := utils.varint_decode(data, off + 4)
		if !ok {
			return left
		}
		if key >= types.Row_ID(sep_val) {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

interior_cell_size :: proc(key: types.Row_ID) -> int {
	return 4 + utils.varint_size(u64(key))
}

interior_cell_size_from_page :: proc(data: []u8, offset: int) -> int {
	_, n, ok := utils.varint_decode(data, offset + 4)
	if !ok { return 0 }
	return 4 + n
}

insert_interior_cell :: proc(data: []u8, page_id: u32, child_page: u32, key: types.Row_ID) -> bool {
	header := get_interior_header(data, page_id)
	if header == nil { return false }

	size := interior_cell_size(key)
	hdr_sz := size_of(Interior_Header)
	base_off := get_page_header_offset(page_id)
	ptrs_end := base_off + hdr_sz + int(header.cell_count + 1) * size_of(Cell_Pointer)
	content_start := int(header.cell_content_offset)
	if ptrs_end + size > content_start {
		return false
	}

	new_offset := content_start - size
	header.cell_content_offset = u16le(new_offset)
	utils.write_u32_be(data, new_offset, child_page)
	utils.varint_encode(data[new_offset + 4:], u64(key))
	insert_idx := find_interior_insert_index(data, page_id, key)

	ptr_start_idx := base_off + hdr_sz
	raw_ptr_data := raw_data(data[ptr_start_idx:])
	ptr_slice := ([^]Cell_Pointer)(raw_ptr_data)[:header.cell_count + 1]
	if insert_idx < int(header.cell_count) {
		copy(ptr_slice[insert_idx + 1:], ptr_slice[insert_idx:header.cell_count])
	}

	ptr_slice[insert_idx] = Cell_Pointer(new_offset)
	header.cell_count += 1
	return true
}
