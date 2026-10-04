// Package btree — page layout primitives shared by all page kinds: headers,
// cell-area geometry, and bulk cell moves. Decoders validate page bytes;
// writers store little-endian fields explicitly.
package btree

import "src:types"

PAGE_SIZE :: types.PAGE_SIZE

// Page_Type covers exactly the live on-disk encodings; layout_for_page
// rejects any other byte loudly rather than guessing a layout.
Page_Type :: enum u8 {
	INTERIOR_DENSE = 6, // Interior node: dense u64/FOR keys + u32le children
	LEAF_SLOTDIR   = 15, // Leaf node: sorted (rowid, offset) slots
	LEAF_TEXT      = 16, // Leaf node: prefix-compressed secondary text index
	TEXT_INTERIOR  = 17, // Interior node: full-key text separators + u32le children
}

// Page_Header is the fixed 8-byte header at the start of every page (after
// the 100-byte database header on page 1). first_freeblock and cell_content
// _offset are byte offsets within the page; a zero freeblock means "none".
Page_Header :: struct #packed #simple {
	page_type          : Page_Type, // Byte 0
	first_freeblock    : u16le, // Bytes 1-2
	cell_count         : u16le, // Bytes 3-4
	cell_content_offset: u16le, // Bytes 5-6
	fragmented_bytes   : u8, // Byte 7
}
#assert(size_of(Page_Header) == 8)

// Leaf_Header is a Page_Header alias for leaves; kept distinct so leaf code
// reads as leaves.
Leaf_Header :: struct #packed #simple {
	using common: Page_Header,
}
#assert(size_of(Leaf_Header) == 8)

// get_page_header_offset returns the header offset for a page: 100 on page 1
// (database header prefix), 0 elsewhere. force_inline: per-cell hot path.
get_page_header_offset :: #force_inline proc "contextless" (page_num: u32) -> int {
	return int(page_num == 1 ? types.DATABASE_HEADER_SIZE : 0)
}

// page_header_size returns the fixed header size for a page type. Assignment
// (not per-arm return) means adding a Page_Type is a compile error here,
// forcing a conscious size decision.
page_header_size :: #force_inline proc "contextless" (page_type: Page_Type) -> int {
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

// get_header returns the page header, or nil when the buffer is too short.
get_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Page_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Page_Header) {
		return nil
	}
	return (^Page_Header)(raw_data(data[off:]))
}

// get_leaf_header returns the leaf header (currently a Page_Header alias),
// or nil when the buffer is too short.
get_leaf_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Leaf_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) {
		return nil
	}
	return (^Leaf_Header)(raw_data(data[off:]))
}

// get_cell_count returns a page's cell count; 0 when the header is unreadable.
get_cell_count :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> int {
	hdr := get_header(data, page_id)
	return hdr != nil ? int(hdr.cell_count) : 0
}

// entry_area_end returns the byte offset just past the fixed-stride entry
// area of a page: header offset + header size + count * stride.
entry_area_end :: proc(data: []u8, page_id: u32, stride: int) -> int {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	cell_count := int(hdr.cell_count)
	return off + hdr_sz + cell_count * stride
}

// move_cells_to appends count fixed-stride cells from src (starting at entry
// index src_start) onto the end of dst's entry area. Fixed-stride entries
// only; variable-width cells are rebuilt by their page-kind code.
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
