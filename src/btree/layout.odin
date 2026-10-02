package btree

import "core:mem"
import "src:cell"
import "src:util/varint"
import "src:types"

PAGE_SIZE :: types.PAGE_SIZE

Page_Type :: enum u8 {
	INTERIOR_TABLE      = 5, // Internal node: pointers to pages
	LEAF_TABLE          = 13, // Leaf node: pointers to data (row-major)
	LEAF_TABLE_COLUMNAR = 14, // Leaf node: columnar-encoded data
}

Cell_Pointer :: distinct u16le

Cell_Entry :: struct #packed {
	ptr: Cell_Pointer,
	key: types.Row_ID,
}
#assert(size_of(Cell_Entry) == 10)

Page_Header :: struct #packed #simple {
	page_type:           Page_Type, // Byte 0
	first_freeblock:     u16le, // Bytes 1-2
	cell_count:          u16le, // Bytes 3-4
	cell_content_offset: u16le, // Bytes 5-6
	fragmented_bytes:    u8, // Byte 7
}
#assert(size_of(Page_Header) == 8)

Interior_Header :: struct #packed #simple {
	using common:  Page_Header,
	rightmost_ptr: u32be,
}
#assert(size_of(Interior_Header) == 12)

Leaf_Header :: struct #packed #simple {
	using common: Page_Header,
}
#assert(size_of(Leaf_Header) == 8)

// Page 1 has a 100-byte database header prefix (types.DATABASE_HEADER_SIZE);
// all other pages start at offset 0.
get_page_header_offset :: proc(page_num: u32) -> int {
	return int(page_num == 1 ? types.DATABASE_HEADER_SIZE : 0)
}

@(private)
page_header_size :: proc(page_type: Page_Type) -> int {
	return int(page_type == .INTERIOR_TABLE ? size_of(Interior_Header) : size_of(Leaf_Header))
}

get_header :: proc(data: []u8, page_id: u32) -> ^Page_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Page_Header) { return nil }
	return (^Page_Header)(raw_data(data[off:]))
}

@(private)
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

@(private)
is_columnar :: proc(data: []u8, page_id: u32) -> bool {
	h := get_header(data, page_id)
	return h != nil && h.page_type == .LEAF_TABLE_COLUMNAR
}

@(private)
init_interior_page :: proc(data: []u8, page_id: u32) {
	off := get_page_header_offset(page_id)
	mem.zero_slice(data[off:])

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
	mem.zero_slice(data[off:])

	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .LEAF_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
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
@(private)
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

// reinsert_row_major reinitializes the page as row-major LEAF_TABLE and
// serializes every decoded row with canonical V2 Cell_Entry{ptr, key}
// pointers — the u16-only pointer array must never be written again.
// Atomic: the expansion is measured first and .Page_Full returns with the
// page untouched when it cannot fit (a half-converted page would silently
// drop the tail rows, so callers must fail the op instead).
@(private)
reinsert_row_major :: proc(data: []u8, page_id: u32, decoded: Columnar_Decode) -> Error {
	total := size_of(Leaf_Header) + len(decoded.rowids) * CELL_ENTRY_STRIDE
	for ri in 0 ..< len(decoded.rowids) {
		if decoded.values[ri] == nil { continue }
		total += cell.compute_info(decoded.rowids[ri], decoded.values[ri]).total_size
	}
	if total > PAGE_SIZE { return .Page_Full }

	off := get_page_header_offset(page_id)
	init_leaf_page(data, page_id)
	header := (^Leaf_Header)(raw_data(data[off:]))
	for ri in 0 ..< len(decoded.rowids) {
		if decoded.values[ri] == nil { continue }

		info := cell.compute_info(decoded.rowids[ri], decoded.values[ri])
		dest_off := int(header.cell_content_offset) - info.total_size
		if dest_off < off + int(size_of(Leaf_Header)) + (int(header.cell_count) + 1) * CELL_ENTRY_STRIDE {
			return .Page_Full
		}

		cell.serialize(data[dest_off:dest_off + info.total_size], decoded.rowids[ri], decoded.values[ri], info)
		header.cell_content_offset = u16le(dest_off)
		entry := (^Cell_Entry)(raw_data(data[off + int(size_of(Leaf_Header)) + int(header.cell_count) * CELL_ENTRY_STRIDE:]))
		entry^ = Cell_Entry {
			ptr = Cell_Pointer(u16(dest_off)),
			key = decoded.rowids[ri],
		}
		header.cell_count = u16le(int(header.cell_count) + 1)
	}
	return .None
}

convert_columnar_to_row_major :: proc(data: []u8, page_id: u32, num_cols: int) {
	off := get_page_header_offset(page_id)
	hdr := (^Page_Header)(raw_data(data[off:]))
	if int(hdr.cell_count) == 0 { return }

	decoded, ok := decode_columnar_page(data, page_id, num_cols)
	if !ok { return }
	_ = reinsert_row_major(data, page_id, decoded)
}

@(private)
ensure_row_major :: proc(data: []u8, page_id: u32) -> bool {
	if !is_columnar(data, page_id) { return true }

	num_cols, found := detect_columnar_col_count(data, page_id)
	if !found { return false }

	decoded, ok := decode_columnar_page(data, page_id, num_cols)
	if !ok { return false }
	if reinsert_row_major(data, page_id, decoded) != .None { return false }
	return !is_columnar(data, page_id)
}

@(private)
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

@(private)
get_raw_entries :: proc(data: []u8, page_id: u32) -> []Cell_Entry {
	header := get_header(data, page_id)
	if header == nil { return nil }

	off := get_page_header_offset(page_id)
	hdr_sz := page_header_size(header.page_type)
	start := off + hdr_sz
	if start >= len(data) { return nil }

	max_entries := (len(data) - start) / size_of(Cell_Entry)
	entry_start := raw_data(data[start:])
	return ([^]Cell_Entry)(entry_start)[:max_entries]
}

CELL_ENTRY_STRIDE :: size_of(Cell_Entry) // 10

get_cell_count :: proc(data: []u8, page_id: u32) -> int {
	hdr := get_header(data, page_id)
	return hdr != nil ? int(hdr.cell_count) : 0
}

get_cell_ptr :: proc(data: []u8, page_id: u32, i: int, stride: int) -> u16 {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	return u16((^u16le)(raw_data(data[start + i * stride:]))^)
}

get_cell_key :: proc(data: []u8, page_id: u32, i: int, layout: ^Cell_Layout) -> types.Row_ID {
	return layout.get_key(data, page_id, i)
}

insert_cell_at :: proc(
	data: []u8,
	page_id: u32,
	i: int,
	ptr: u16,
	key: types.Row_ID,
	stride: int,
) {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	cell_count := int(hdr.cell_count)

	// Shift entries [i..cell_count) right by 1
	if i < cell_count {
		src := data[start + i * stride:start + cell_count * stride]
		dst := data[start + (i + 1) * stride:]
		copy(dst, src)
	}

	// Write new entry
	entry := (^Cell_Entry)(raw_data(data[start + i * stride:]))
	entry^ = Cell_Entry {
		ptr = Cell_Pointer(ptr),
		key = key,
	}
}

delete_cell_at :: proc(data: []u8, page_id: u32, i: int, stride: int) {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	cell_count := int(hdr.cell_count)
	if i < cell_count - 1 {
		src := data[start + (i + 1) * stride:start + cell_count * stride]
		dst := data[start + i * stride:]
		copy(dst, src)
	}
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

@(private)
get_right_ptr :: proc(data: []u8, page_id: u32) -> u32 {
	h := get_interior_header(data, page_id)
	if h == nil { return 0 }
	return u32(h.rightmost_ptr)
}

@(private)
set_right_ptr :: proc(data: []u8, page_id: u32, ptr: u32) {
	h := get_interior_header(data, page_id)
	if h != nil {
		h.rightmost_ptr = u32be(ptr)
	}
}
