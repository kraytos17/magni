package btree

import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:types"

PAGE_HEADER_OFFSET_ROOT :: 100
#assert(PAGE_HEADER_OFFSET_ROOT == types.DATABASE_HEADER_SIZE)
PAGE_SIZE :: types.PAGE_SIZE

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

find_interior_cell_for_child :: proc(data: []u8, page_id: u32, child_page: u32) -> int {
	pointers := get_pointers(data, page_id)
	for ptr, i in pointers {
		off := int(ptr)
		stored_child, _ := endian.get_u32(data[off:], .Big)
		if stored_child == child_page {
			return i
		}
	}
	return -1
}

interior_lower_bound :: proc(data: []u8, page_id: u32, key: types.Row_ID) -> (int, bool) {
	pointers := get_pointers(data, page_id)
	left := 0
	right := len(pointers)
	for left < right {
		mid := left + (right - left) / 2
		sep_val, _, ok := cell.varint_decode(data, int(pointers[mid]) + 4)
		if !ok { return left, false }
		if key >= types.Row_ID(sep_val) {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}

find_interior_insert_index :: proc(data: []u8, page_id: u32, key: types.Row_ID) -> int {
	idx, _ := interior_lower_bound(data, page_id, key)
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
	endian.put_u32(data[new_offset:], .Big, child_page)
	cell.varint_encode(data[new_offset + 4:], u64(key))
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
