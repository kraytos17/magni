package btree

import "core:encoding/endian"
import "core:mem"
import "src:cell"
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

is_columnar :: proc(data: []u8, page_id: u32) -> bool {
	h := get_header(data, page_id)
	return h != nil && h.page_type == .LEAF_TABLE_COLUMNAR
}

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

convert_columnar_to_row_major :: proc(data: []u8, page_id: u32, num_cols: int) {
	off := get_page_header_offset(page_id)
	hdr := (^Page_Header)(raw_data(data[off:]))
	row_count := int(hdr.cell_count)
	if row_count == 0 { return }

	rowids := make([]types.Row_ID, row_count, context.temp_allocator)
	for i in 0 ..< row_count {
		rid, ok := cell.read_columnar_rowid(data, num_cols, i, off)
		if !ok { return }
		rowids[i] = rid
	}

	values := make([][]types.Value, row_count, context.temp_allocator)
	for col_i in 0 ..< num_cols {
		col_vals := cell.decode_column(data, num_cols, col_i, off, context.temp_allocator)
		if col_vals == nil { return }
		for ri in 0 ..< row_count {
			if values[ri] == nil {
				values[ri] = make([]types.Value, num_cols, context.temp_allocator)
			}
			if ri < len(col_vals) {
				values[ri][col_i] = col_vals[ri]
			}
		}
	}

	// Reinitialize page as row-major LEAF_TABLE
	mem.zero_slice(data[off:])
	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .LEAF_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0

	for ri in 0 ..< row_count {
		if values[ri] == nil { continue }
		info := cell.compute_info(rowids[ri], values[ri])
		dest_off := int(header.cell_content_offset) - info.total_size
		if dest_off < off + int(size_of(Leaf_Header)) + (int(header.cell_count) + 1) * 2 {
			return
		}

		cell.serialize(data[dest_off:dest_off + info.total_size], rowids[ri], values[ri], info)
		header.cell_content_offset = u16le(dest_off)
		ptr_loc := off + size_of(Leaf_Header) + int(header.cell_count) * 2
		endian.put_u16(data[ptr_loc:], .Little, u16(dest_off))
		header.cell_count = u16le(int(header.cell_count) + 1)
	}
}

ensure_row_major :: proc(data: []u8, page_id: u32) {
	if !is_columnar(data, page_id) { return }
	num_cols, found := detect_columnar_col_count(data, page_id)
	if !found { return }
	convert_columnar_to_row_major(data, page_id, num_cols)
}

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

get_pointers :: proc(data: []u8, page_id: u32) -> []Cell_Pointer {
	header := get_header(data, page_id)
	if header == nil { return nil }

	off := get_page_header_offset(page_id)
	hdr_sz := page_header_size(header.page_type)
	start := off + hdr_sz
	if start >= len(data) { return nil }

	max_ptrs := (len(data) - start) / size_of(Cell_Pointer)
	n := min(int(header.cell_count), max_ptrs)
	ptr_start := raw_data(data[start:])
	return ([^]Cell_Pointer)(ptr_start)[:n]
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

get_entries :: proc(data: []u8, page_id: u32) -> []Cell_Entry {
	header := get_header(data, page_id)
	if header == nil { return nil }

	off := get_page_header_offset(page_id)
	hdr_sz := page_header_size(header.page_type)
	start := off + hdr_sz
	if start >= len(data) { return nil }

	max_entries := (len(data) - start) / size_of(Cell_Entry)
	n := min(int(header.cell_count), max_entries)
	entry_start := raw_data(data[start:])
	return ([^]Cell_Entry)(entry_start)[:n]
}

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

// --- Page Accessor API ---
// These 7 functions encapsulate the v1 (Cell_Pointer, stride=2) vs v2 (Cell_Entry, stride=10)
// entry-array layout. Callers pass stride = size_of(Cell_Entry) for v2, size_of(Cell_Pointer) for v1.
// All asserts on bounds; indices are page-internal (never user-supplied).

CELL_POINTER_STRIDE :: size_of(Cell_Pointer) // 2
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
	if stride == CELL_ENTRY_STRIDE {
		entry := (^Cell_Entry)(raw_data(data[start + i * stride:]))
		entry^ = Cell_Entry {
			ptr = Cell_Pointer(ptr),
			key = key,
		}
	} else {
		cell_ptr := (^Cell_Pointer)(raw_data(data[start + i * stride:]))
		cell_ptr^ = Cell_Pointer(ptr)
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

find_interior_insert_index :: proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: ^Cell_Layout,
) -> int {
	idx, _ := interior_lower_bound(data, page_id, key, layout)
	return idx
}

interior_cell_size :: proc(key: types.Row_ID) -> int {
	return 4 + cell.varint_size(u64(key))
}

interior_cell_size_from_page :: proc(data: []u8, offset: int) -> int {
	_, n, ok := cell.varint_decode(data, offset + 4)
	if !ok { return 0 }
	return 4 + n
}

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
	cell.varint_encode(data[new_offset + 4:], u64(key))
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
FREEBLOCK_HDR_SIZE :: 4

freeblock_read_next :: proc(data: []u8, off: u16) -> u16le {
	return (^u16le)(raw_data(data[int(off):]))^
}

freeblock_read_size :: proc(data: []u8, off: u16) -> u16le {
	return (^u16le)(raw_data(data[int(off) + 2:]))^
}

freeblock_write_next :: proc(data: []u8, off: u16, next: u16le) {
	(^u16le)(raw_data(data[int(off):]))^ = next
}

freeblock_write_size :: proc(data: []u8, off: u16, sz: u16le) {
	(^u16le)(raw_data(data[int(off) + 2:]))^ = sz
}

// Inserts a cell-sized freeblock into the chain. cell_off/cell_sz are native u16.
freeblock_insert :: proc(data: []u8, cell_off: u16, cell_sz: u16, first: ^u16le) {
	if cell_sz < FREEBLOCK_HDR_SIZE { return }

	co := u16le(cell_off)
	cs := u16le(cell_sz)
	if first^ == 0 {
		freeblock_write_next(data, cell_off, 0)
		freeblock_write_size(data, cell_off, cs)
		first^ = co
		return
	}

	// Before first block
	f := first^
	if co + cs == f {
		sz := freeblock_read_size(data, u16(f)) + cs
		freeblock_write_size(data, cell_off, sz)
		freeblock_write_next(data, cell_off, freeblock_read_next(data, u16(f)))
		first^ = co
		return
	}
	if co < f {
		freeblock_write_next(data, cell_off, f)
		freeblock_write_size(data, cell_off, cs)
		first^ = co
		return
	}

	// Walk chain
	prev := f
	for {
		nxt := freeblock_read_next(data, u16(prev))
		if nxt == 0 {
			end_prev := u16(prev) + u16(freeblock_read_size(data, u16(prev)))
			if u16le(end_prev) == co {
				sz := freeblock_read_size(data, u16(prev)) + cs
				freeblock_write_size(data, u16(prev), sz)
			} else {
				freeblock_write_next(data, u16(prev), co)
				freeblock_write_next(data, cell_off, 0)
				freeblock_write_size(data, cell_off, cs)
			}
			return
		}

		end_prev := u16(prev) + u16(freeblock_read_size(data, u16(prev)))
		if u16le(end_prev) > co { return }
		if u16le(end_prev) == co {
			// Merge with prev
			new_sz := freeblock_read_size(data, u16(prev)) + cs
			if co + cs == nxt {
				// Three-way merge: prev + new + next
				new_sz += freeblock_read_size(data, u16(nxt))
				freeblock_write_size(data, u16(prev), new_sz)
				freeblock_write_next(data, u16(prev), freeblock_read_next(data, u16(nxt)))
			} else {
				freeblock_write_size(data, u16(prev), new_sz)
			}
			return
		}
		if co + cs == nxt {
			sz := cs + freeblock_read_size(data, u16(nxt))
			freeblock_write_size(data, cell_off, sz)
			freeblock_write_next(data, cell_off, freeblock_read_next(data, u16(nxt)))
			freeblock_write_next(data, u16(prev), co)
			return
		}
		if co < nxt {
			freeblock_write_next(data, cell_off, nxt)
			freeblock_write_size(data, cell_off, cs)
			freeblock_write_next(data, u16(prev), co)
			return
		}
		prev = nxt
	}
}

// Allocates space from a freeblock. Returns offset (native u16), or 0.
freeblock_alloc :: proc(data: []u8, first_hdr: u16le, need: u16, first: ^u16le) -> u16 {
	f := u16(first_hdr)
	if f == 0 { return 0 }

	walk_and_alloc :: proc(data: []u8, prev: u16, curr: u16, need: u16, first: ^u16le) -> u16 {
		fsz := u16(freeblock_read_size(data, curr))
		if fsz >= need {
			if fsz == need {
				nxt := freeblock_read_next(data, curr)
				if prev == 0 { first^ = nxt } else { freeblock_write_next(data, prev, nxt) }
				return curr
			}

			remain := fsz - need
			if remain >= FREEBLOCK_HDR_SIZE {
				freeblock_write_size(data, curr, u16le(remain))
				return curr + remain
			}

			nxt := freeblock_read_next(data, curr)
			if prev == 0 { first^ = nxt } else { freeblock_write_next(data, prev, nxt) }
			return curr
		}
		return 0
	}

	if r := walk_and_alloc(data, 0, f, need, first); r != 0 { return r }
	for {
		nxt_u16 := u16(freeblock_read_next(data, f))
		if nxt_u16 == 0 { return 0 }
		if r := walk_and_alloc(data, f, nxt_u16, need, first); r != 0 { return r }
		f = nxt_u16
	}
	return 0
}
