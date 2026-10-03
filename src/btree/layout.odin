package btree

import "src:cell"
import "src:types"
import "src:util/varint"

PAGE_SIZE :: types.PAGE_SIZE

// Page types are exactly the live encodings. V2 row-major variants were
// removed in the full V3 migration: nothing writes them, and
// layout_for_page rejects their bytes loudly, so keeping the discriminants
// would only invite dead routes.
Page_Type :: enum u8 {
	LEAF_TABLE_COLUMNAR = 14, // Leaf node: columnar-encoded data (test-only)
	INTERIOR_DENSE      = 6, // Interior node: dense u64/FOR keys + u32le children
	LEAF_SLOTDIR        = 15, // Leaf node: sorted (rowid, offset) slots
	LEAF_TEXT           = 16, // Leaf node: prefix-compressed secondary text index
	TEXT_INTERIOR       = 17, // Interior node: full-key text separators + u32le children
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
	case .LEAF_TABLE_COLUMNAR, .LEAF_SLOTDIR, .LEAF_TEXT, .TEXT_INTERIOR:
	// LEAF_TEXT carries a variable prefix past the stock header
	// (text_entry_area_end owns that math); TEXT_INTERIOR carries
	// children + sep slots past it (interior geometry procs own it).
	}
	return sz
}

get_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Page_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Page_Header) { return nil }
	return (^Page_Header)(raw_data(data[off:]))
}

get_leaf_header :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> ^Leaf_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) { return nil }
	return (^Leaf_Header)(raw_data(data[off:]))
}

@(private)
is_columnar :: #force_inline proc "contextless" (data: []u8, page_id: u32) -> bool {
	h := get_header(data, page_id)
	return h != nil && h.page_type == .LEAF_TABLE_COLUMNAR
}

// Columnar_Decode is a columnar page fully decoded into row-major rows,
// ready for reinsert. Slices borrow the temp allocator.
Columnar_Decode :: struct {
	rowids: []types.Row_ID,
	values: [][]types.Value,
}

// decode_columnar_page reads every rowid and column value out of a columnar
// page without modifying it. Single O(n) rowid walk (not per-row seeks).
// Returns ok=false on truncated or garbage data; the page is untouched.
@(private, cold)
decode_columnar_page :: proc(
	data: []u8,
	page_id: u32,
	num_cols: int,
) -> (
	decoded: Columnar_Decode,
	ok: bool,
) {
	off := get_page_header_offset(page_id)
	hdr := (^Page_Header)(raw_data(data[off:]))
	row_count := int(hdr.cell_count)
	if row_count == 0 { return {}, true }

	rowids := make([]types.Row_ID, row_count, context.temp_allocator)
	rid_pos := off + cell.COLUMNAR_DIR_OFFSET + num_cols * size_of(cell.Col_Header)
	total: types.Row_ID = 0
	for i in 0 ..< row_count {
		delta, n, dok := varint.decode(data, rid_pos)
		if !dok { return {}, false }

		total += types.Row_ID(delta)
		rowids[i] = total
		rid_pos += n
	}

	values := make([][]types.Value, row_count, context.temp_allocator)
	for ri in 0 ..< row_count {
		values[ri] = make([]types.Value, num_cols, context.temp_allocator)
	}
	for col_i in 0 ..< num_cols {
		col_vals := cell.decode_column(data, num_cols, col_i, off, context.temp_allocator)
		if col_vals == nil { return {}, false }
		for ri in 0 ..< row_count {
			if ri < len(col_vals) {
				values[ri][col_i] = col_vals[ri]
			}
		}
	}
	return Columnar_Decode{rowids = rowids, values = values}, true
}

// reinsert_row_major reinitializes the page as a slotdir leaf
// (LEAF_SLOTDIR) and serializes every decoded row with Slot{rowid, off}
// entries — row-major cells plus a slot directory. (Post-flip there are no
// Cell_Entry leaves; the slot size equals the old stride, so the capacity
// math is unchanged.)
// Atomic: the expansion is measured first and .Page_Full returns with the
// page untouched when it cannot fit (a half-converted page would silently
// drop the tail rows, so callers must fail the op instead).
@(private, cold)
reinsert_row_major :: proc(data: []u8, page_id: u32, decoded: Columnar_Decode) -> Error {
	total := size_of(Leaf_Header) + len(decoded.rowids) * size_of(Slot)
	for ri in 0 ..< len(decoded.rowids) {
		if decoded.values[ri] == nil { continue }
		total += cell.compute_info(decoded.rowids[ri], decoded.values[ri]).total_size
	}
	if total > PAGE_SIZE { return .Page_Full }

	off := get_page_header_offset(page_id)
	if !init_slot_leaf_page(data, page_id) { return .Invalid_Page_Header }

	header := (^Leaf_Header)(raw_data(data[off:]))
	for ri in 0 ..< len(decoded.rowids) {
		if decoded.values[ri] == nil { continue }

		info := cell.compute_info(decoded.rowids[ri], decoded.values[ri])
		dest_off := int(header.cell_content_offset) - info.total_size
		if dest_off <
		   off + int(size_of(Leaf_Header)) + (int(header.cell_count) + 1) * size_of(Slot) {
			return .Page_Full
		}

		bytes_written, ser_ok := cell.serialize(
			data[dest_off:dest_off + info.total_size],
			decoded.rowids[ri],
			decoded.values[ri],
			info,
		)
		if !ser_ok || bytes_written != info.total_size { return .Serialization_Failed }

		header.cell_content_offset = u16le(dest_off)
		entry := (^Slot)(
			raw_data(
				data[off + int(size_of(Leaf_Header)) + int(header.cell_count) * size_of(Slot):],
			),
		)
		entry^ = Slot {
			rowid = u64le(rowid_bias_encode(decoded.rowids[ri])),
			off   = u16le(u16(dest_off)),
		}
		header.cell_count = u16le(int(header.cell_count) + 1)
	}
	return .None
}

@(cold)
convert_columnar_to_row_major :: proc(data: []u8, page_id: u32, num_cols: int) {
	off := get_page_header_offset(page_id)
	hdr := (^Page_Header)(raw_data(data[off:]))
	if int(hdr.cell_count) == 0 { return }

	decoded, ok := decode_columnar_page(data, page_id, num_cols)
	if !ok { return }
	_ = reinsert_row_major(data, page_id, decoded)
}

@(private, require_results)
ensure_row_major :: proc(data: []u8, page_id: u32) -> bool {
	if !is_columnar(data, page_id) { return true }

	num_cols, found := detect_columnar_col_count(data, page_id)
	if !found { return false }

	decoded, ok := decode_columnar_page(data, page_id, num_cols)
	if !ok { return false }
	if reinsert_row_major(data, page_id, decoded) != .None { return false }
	return !is_columnar(data, page_id)
}

@(private, require_results)
detect_columnar_col_count :: proc(data: []u8, page_id: u32) -> (int, bool) {
	if !is_columnar(data, page_id) { return 0, false }
	hdr := get_header(data, page_id)
	if hdr == nil { return 0, false }

	col_start := 8 // COLUMNAR_DIR_OFFSET
	col_sz := 12 // size_of(Col_Header)
	data_start := int(hdr.cell_content_offset)
	if data_start <= col_start { return 0, false }

	n := (data_start - col_start) / col_sz
	if n < 1 || n > 100 { return 0, false }
	return n, true
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
