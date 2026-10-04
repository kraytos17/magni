package btree

import "src:types"

PAGE_SIZE :: types.PAGE_SIZE

// Page types are exactly the live encodings. V2 row-major variants were
// removed in the full V3 migration: nothing writes them, and
// layout_for_page rejects their bytes loudly, so keeping the discriminants
// would only invite dead routes.
Page_Type :: enum u8 {
	INTERIOR_DENSE = 6, // Interior node: dense u64/FOR keys + u32le children
	LEAF_SLOTDIR   = 15, // Leaf node: sorted (rowid, offset) slots
	LEAF_TEXT      = 16, // Leaf node: prefix-compressed secondary text index
	TEXT_INTERIOR  = 17, // Interior node: full-key text separators + u32le children
}


Page_Header :: struct #packed #simple {
	page_type          : Page_Type, // Byte 0
	first_freeblock    : u16le, // Bytes 1-2
	cell_count         : u16le, // Bytes 3-4
	cell_content_offset: u16le, // Bytes 5-6
	fragmented_bytes   : u8, // Byte 7
}
#assert(size_of(Page_Header) == 8)


Leaf_Header :: struct #packed #simple {
	using common: Page_Header,
}
#assert(size_of(Leaf_Header) == 8)

// Page 1 has a 100-byte database header prefix (types.DATABASE_HEADER_SIZE);
// all other pages start at offset 0.
// force_inline + contextless: per-cell hot path, no context use.
get_page_header_offset :: #force_inline proc "contextless" (page_num: u32) -> int {
	return int(page_num == 1 ? types.DATABASE_HEADER_SIZE : 0)
}

page_header_size :: #force_inline proc "contextless" (page_type: Page_Type) -> int {
	// Assigned, not returned, per arm: adding a Page_Type is a compile
	// error here, forcing a conscious size decision
	sz := size_of(Leaf_Header)
	switch page_type {
	case .INTERIOR_DENSE:
		sz = size_of(Dense_Interior_Header)
	case .LEAF_SLOTDIR, .LEAF_TEXT, .TEXT_INTERIOR:
	// LEAF_TEXT carries a variable prefix past the stock header
	// (text_entry_area_end owns that math); TEXT_INTERIOR carries
	// children + sep slots past it (interior geometry procs own it).
	}
	return sz
}

get_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Page_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Page_Header) {
		return nil
	}
	return (^Page_Header)(raw_data(data[off:]))
}

get_leaf_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Leaf_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) {
		return nil
	}
	return (^Leaf_Header)(raw_data(data[off:]))
}

get_cell_count :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> int {
	hdr := get_header(data, page_id)
	return hdr != nil ? int(hdr.cell_count) : 0
}

entry_area_end :: proc(data: []u8, page_id: u32, stride: int) -> int {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	cell_count := int(hdr.cell_count)
	return off + hdr_sz + cell_count * stride
}

move_cells_to :: proc(
	dst: []u8,
	dst_id: u32,
	src: []u8,
	src_id: u32,
	src_start: int,
	count: int,
	stride: int,
) {
	src_off := get_page_header_offset(src_id)
	src_hdr := get_header(src, src_id)
	src_hdr_sz := page_header_size(src_hdr.page_type)
	src_base := src_off + src_hdr_sz

	dst_off := get_page_header_offset(dst_id)
	dst_hdr := get_header(dst, dst_id)
	dst_hdr_sz := page_header_size(dst_hdr.page_type)
	dst_cell_count := int(dst_hdr.cell_count)
	dst_base := dst_off + dst_hdr_sz

	src_start_off := src_base + src_start * stride
	dst_start_off := dst_base + dst_cell_count * stride
	byte_count := count * stride
	copy(
		dst[dst_start_off:dst_start_off + byte_count],
		src[src_start_off:src_start_off + byte_count],
	)
}
