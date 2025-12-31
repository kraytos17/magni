package btree

import "core:fmt"
import "core:mem"
import "core:strings"
import "src:cell"
import "src:pager"
import "src:types"
import "src:utils"

PAGE_HEADER_OFFSET_ROOT :: 100

// Page types identifying the content structure of a B-Tree page.
// These values typically align with SQLite's file format specifications.
Page_Type :: enum u8 {
	INTERIOR_TABLE = 5, // Internal node containing pointers to other pages
	LEAF_TABLE     = 13, // Leaf node containing actual row data
}

// Represents the raw header found at the start of every B-Tree page.
// The header occupies the first 8 bytes of the page buffer.
Page_Header :: struct #packed {
	page_type:           Page_Type, // Byte 0: The type of page
	first_freeblock:     u16le, // Bytes 1-2: Offset to the first block of free space
	cell_count:          u16le, // Bytes 3-4: Number of cells currently stored
	cell_content_offset: u16le, // Bytes 5-6: Offset where cell content area begins (grows downwards)
	fragmented_bytes:    u8, // Byte 7: Number of fragmented free bytes within the cell area
}

#assert(size_of(Page_Header) == 8)

// A cell pointer is a 2-byte integer offset into the page.
Cell_Pointer :: u16le

// Error types representing specific failure modes in B-Tree operations.
Error :: enum {
	None,
	Page_Read_Failed, // Underlying pager failed to retrieve the page
	Invalid_Page_Header, // Page data does not match expected B-Tree header format
	Invalid_Cell_Pointer, // Cell pointer offset is out of bounds
	Cell_Deserialize_Failed, // Raw bytes could not be converted to a Cell struct
	Page_Full, // Not enough contiguous space to insert new cell
	Duplicate_Rowid, // Attempted to insert a Row ID that already exists
	Cell_Not_Found, // Search completed without finding the target
	Invalid_Bounds, // Generic out-of-bounds memory access error
	Serialization_Failed, // Failed to write Cell data into the page buffer
}

// A smart reference to a Cell retrieved from the B-Tree.
//
// MEMORY SAFETY:
// This struct OWNS the heap memory associated with `cell.values`.
// The caller is responsible for calling `cell_ref_destroy` when done.
// Failing to do so will result in a memory leak.
Cell_Ref :: struct {
	cell:      cell.Cell,
	allocator: mem.Allocator,
}

// Releases the memory owned by a Cell_Ref.
// Must be called exactly once for every Cell_Ref obtained.
cell_ref_destroy :: proc(ref: ^Cell_Ref) {
	cell.destroy(&ref.cell)
}

// Configuration for B-tree operations.
Config :: struct {
	allocator:        mem.Allocator, // Allocator for cell deserialization
	zero_copy:        bool, // If true, strings/blobs point directly to page buffer (unsafe if page is freed)
	check_duplicates: bool, // If true, insert verifies uniqueness (expensive)
}

DEFAULT_CONFIG := Config {
	allocator        = {}, // Will default to context.allocator or cursor allocator if nil
	zero_copy        = false, // Default to safe copies
	check_duplicates = true, // Default to safety over speed
}

// Iterator for traversing B-Tree pages sequentially.
// Maintains state to track position across multiple calls.
Cursor :: struct {
	page_num:     u32, // The physical page number being iterated
	cell_index:   int, // Current index into the cell pointer array
	end_of_table: bool, // Flag indicating if iteration is complete
	allocator:    mem.Allocator, // Allocator used for operations involving this cursor
}

page_header_size :: proc(page_type: Page_Type) -> int {
	if page_type == .INTERIOR_TABLE {
		return size_of(Interior_Header)
	}
	return size_of(Leaf_Header)
}

// Determine header start based on page number
get_page_header_offset :: proc(page_num: u32) -> int {
	if page_num == 0 {
		return PAGE_HEADER_OFFSET_ROOT
	}
	return 0
}

get_header :: proc(page_data: []u8, page_num: u32) -> ^Page_Header {
	offset := get_page_header_offset(page_num)
	if len(page_data) < offset + size_of(Page_Header) {
		return nil
	}
	return (^Page_Header)(raw_data(page_data[offset:]))
}

get_pointers :: proc(page_data: []u8, page_num: u32) -> []Cell_Pointer {
	header := get_header(page_data, page_num)
	if header == nil {
		return nil
	}

	offset := get_page_header_offset(page_num)
	header_size := page_header_size(header.page_type)
	ptr_start := raw_data(page_data[offset + header_size:])
	return ([^]Cell_Pointer)(ptr_start)[:header.cell_count]
}

// Returns a slice view allowing access to "potential" pointers beyond cell_count.
// Used during insertion to shift memory.
get_raw_pointers :: proc(page_data: []u8, page_num: u32) -> []Cell_Pointer {
	header := get_header(page_data, page_num)
	if header == nil {
		return nil
	}

	offset := get_page_header_offset(page_num)
	header_size := page_header_size(header.page_type)
	start_idx := offset + header_size
	if start_idx >= len(page_data) {
		return nil
	}

	ptr_start := raw_data(page_data[start_idx:])
	max_ptrs := (len(page_data) - start_idx) / size_of(Cell_Pointer)
	return ([^]Cell_Pointer)(ptr_start)[:max_ptrs]
}

init_leaf_page :: proc(page_data: []u8, page_num: u32) -> Error {
	offset := get_page_header_offset(page_num)
	mem.zero_slice(page_data[offset:])
	hdr := get_leaf_header(page_data, page_num)
	if hdr == nil {
		return .Invalid_Bounds
	}

	hdr.page_type = .LEAF_TABLE
	hdr.first_freeblock = 0
	hdr.cell_count = 0
	hdr.cell_content_offset = u16le(len(page_data))
	hdr.fragmented_bytes = 0
	hdr.next_leaf = 0

	return .None
}

// Return value for recursive inserts.
// If a node splits, it returns the info needed to update its parent.
Split_Result :: struct {
	did_split:  bool,
	right_page: u32, // The new sibling page number
	split_key:  types.Row_ID, // The key separating left/right
}

@(private = "file")
move_cells :: proc(
	src_data: []u8,
	src_pg_num: u32,
	dst_data: []u8,
	dst_page_num: u32,
	start_idx: int,
	count: int,
) -> bool {
	src_ptrs := get_pointers(src_data, src_pg_num)
	if start_idx + count > len(src_ptrs) {
		fmt.println("Error: move_cells requesting more cells than available.")
		return false
	}

	for i in 0 ..< count {
		src_index := start_idx + i
		src_ptr := src_ptrs[src_index]
		cell_size, ok := cell.get_size(src_data, int(src_ptr))
		if !ok {
			return false
		}

		cell_bytes := src_data[int(src_ptr):int(src_ptr) + cell_size]
		dst_hdr := get_header(dst_data, dst_page_num)
		new_offset := int(dst_hdr.cell_content_offset) - cell_size
		dst_hdr.cell_content_offset = u16le(new_offset)

		copy(dst_data[new_offset:], cell_bytes)
		header_size := page_header_size(dst_hdr.page_type)
		base_offset := get_page_header_offset(dst_page_num)
		ptr_loc := base_offset + header_size + int(dst_hdr.cell_count) * 2

		utils.write_u16_le(dst_data, ptr_loc, u16(new_offset))
		dst_hdr.cell_count += 1
	}

	return true
}

@(private = "file")
move_interior_cells :: proc(src: []u8, src_pg: u32, dst: []u8, dst_pg: u32, start: int, count: int) -> bool {
	ptrs := get_pointers(src, src_pg)
	for i in 0 ..< count {
		off := ptrs[start + i]
		size := interior_cell_size_from_page(src, int(off))
		dst_hdr := get_interior_header(dst, dst_pg)
		new_off := int(dst_hdr.cell_content_offset) - size
		dst_hdr.cell_content_offset = u16le(new_off)

		copy(dst[new_off:], src[int(off):int(off) + size])
		hdr_sz := size_of(Interior_Header)
		base := get_page_header_offset(dst_pg)
		ptr_loc := base + hdr_sz + int(dst_hdr.cell_count) * 2

		utils.write_u16_le(dst, ptr_loc, u16(new_off))
		dst_hdr.cell_count += 1
	}
	return true
}

@(private = "file")
split_leaf_node :: proc(p: ^pager.Pager, page_num: u32) -> (Split_Result, Error) {
	right, err := pager.allocate_page(p)
	if err != nil { return {}, .Page_Full }

	init_leaf_page(right.data, right.page_num)
	left_page, _ := pager.get_page(p, page_num)
	left_hdr := get_leaf_header(left_page.data, page_num)
	right_hdr := get_leaf_header(right.data, right.page_num)

	total := int(left_hdr.cell_count)
	mid := total / 2
	move_cells(left_page.data, page_num, right.data, right.page_num, mid, total - mid)

	left_hdr.cell_count = u16le(mid)
	right_hdr.next_leaf = left_hdr.next_leaf
	left_hdr.next_leaf = u32be(right.page_num)

	ptrs := get_pointers(right.data, right.page_num)
	sep, _ := cell.get_rowid(right.data, int(ptrs[0]))
	pager.mark_dirty(p, page_num)
	pager.mark_dirty(p, right.page_num)
	return Split_Result{did_split = true, right_page = right.page_num, split_key = sep}, .None
}

@(private = "file")
find_cell_index_with_child :: proc(page_data: []u8, page_num: u32, child_page: u32) -> int {
	pointers := get_pointers(page_data, page_num)
	for ptr, i in pointers {
		val, _ := utils.read_u32_be(page_data, int(ptr))
		if val == child_page {
			return i
		}
	}
	return -1
}

split_leaf_root :: proc(p: ^pager.Pager, root: u32) -> Error {
	left, _ := pager.allocate_page(p)
	right, _ := pager.allocate_page(p)

	init_leaf_page(left.data, left.page_num)
	init_leaf_page(right.data, right.page_num)
	root_page, _ := pager.get_page(p, root)
	hdr := get_leaf_header(root_page.data, root)

	total := int(hdr.cell_count)
	mid := total / 2
	move_cells(root_page.data, root, left.data, left.page_num, 0, mid)
	move_cells(root_page.data, root, right.data, right.page_num, mid, total - mid)

	left_hdr := get_leaf_header(left.data, left.page_num)
	right_hdr := get_leaf_header(right.data, right.page_num)
	left_hdr.next_leaf = u32be(right.page_num)

	ptrs := get_pointers(right.data, right.page_num)
	sep, _ := cell.get_rowid(right.data, int(ptrs[0]))
	init_interior_page(root_page.data, root)
	set_right_ptr(root_page.data, root, right.page_num)
	insert_interior_cell(root_page.data, root, left.page_num, sep)

	pager.mark_dirty(p, left.page_num)
	pager.mark_dirty(p, right.page_num)
	pager.mark_dirty(p, root)

	return .None
}

split_interior_node :: proc(p: ^pager.Pager, page_num: u32) -> (Split_Result, Error) {
	right, _ := pager.allocate_page(p)
	init_interior_page(right.data, right.page_num)

	page, _ := pager.get_page(p, page_num)
	hdr := get_interior_header(page.data, page_num)
	total := int(hdr.cell_count)
	mid := total / 2
	ptrs := get_pointers(page.data, page_num)

	mid_ptr := ptrs[mid]
	sep_u64, _, _ := utils.varint_decode(page.data, int(mid_ptr) + 4)
	sep := types.Row_ID(sep_u64)
	move_interior_cells(page.data, page_num, right.data, right.page_num, mid + 1, total - (mid + 1))

	hdr.cell_count = u16le(mid)
	sep_cell_size := interior_cell_size_from_page(page.data, int(mid_ptr))
	mem.zero_slice(page.data[int(mid_ptr):int(mid_ptr) + sep_cell_size])

	left_child, _ := utils.read_u32_be(page.data, int(mid_ptr))
	set_right_ptr(right.data, right.page_num, get_right_ptr(page.data, page_num))
	set_right_ptr(page.data, page_num, left_child)

	pager.mark_dirty(p, page_num)
	pager.mark_dirty(p, right.page_num)
	return Split_Result{did_split = true, right_page = right.page_num, split_key = sep}, .None
}

split_interior_root :: proc(p: ^pager.Pager, root: u32, split: Split_Result) -> Error {
	left, _ := pager.allocate_page(p)
	right, _ := pager.allocate_page(p)

	init_interior_page(left.data, left.page_num)
	init_interior_page(right.data, right.page_num)

	root_page, _ := pager.get_page(p, root)
	move_interior_cells(
		root_page.data,
		root,
		left.data,
		left.page_num,
		0,
		int(get_interior_header(root_page.data, root).cell_count),
	)

	set_right_ptr(left.data, left.page_num, get_right_ptr(root_page.data, root))
	set_right_ptr(right.data, right.page_num, split.right_page)

	init_interior_page(root_page.data, root)
	set_right_ptr(root_page.data, root, right.page_num)
	insert_interior_cell(root_page.data, root, left.page_num, split.split_key)

	pager.mark_dirty(p, root)
	pager.mark_dirty(p, left.page_num)
	pager.mark_dirty(p, right.page_num)

	return .None
}

insert_recursive :: proc(
	p: ^pager.Pager,
	page_num: u32,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	Split_Result,
	Error,
) {
	page, err := pager.get_page(p, page_num)
	if err != nil {
		return {}, .Page_Read_Failed
	}

	header := get_header(page.data, page_num)
	if header.page_type == .LEAF_TABLE {
		err := insert_cell_into_leaf(p, page_num, rowid, values)
		if err == .Page_Full {
			return split_leaf_node(p, page_num)
		}
		return Split_Result{did_split = false}, err
	}
	if header.page_type == .INTERIOR_TABLE {
		child_page_num := find_child_page(page.data, header, page_num, rowid)
		child_split, child_err := insert_recursive(p, child_page_num, rowid, values)
		if child_err != .None {
			return {}, child_err
		}
		if child_split.did_split {
			ok := insert_interior_cell(page.data, page_num, child_page_num, child_split.split_key)
			if !ok {
				split, err := split_interior_node(p, page_num)
				if err != .None {
					return {}, err
				}
				return split, .None
			}
			if get_right_ptr(page.data, page_num) == child_page_num {
				set_right_ptr(page.data, page_num, child_split.right_page)
			}
		}
		return Split_Result{did_split = false}, .None
	}
	return {}, .Invalid_Page_Header
}

insert_cell_into_leaf :: proc(
	p: ^pager.Pager,
	page_num: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	config := DEFAULT_CONFIG,
) -> Error {
	page, err := pager.get_page(p, page_num)
	if err != nil {
		return .Page_Read_Failed
	}

	header := get_header(page.data, page_num)
	if config.check_duplicates && rowid_exists(page.data, header, page_num, rowid) {
		return .Duplicate_Rowid
	}

	cell_size := cell.calculate_size(rowid, values)
	base_offset := get_page_header_offset(page_num)
	header_size := 8

	ptr_area_end := base_offset + header_size + int(header.cell_count + 1) * size_of(Cell_Pointer)
	if ptr_area_end >= int(header.cell_content_offset) { return .Page_Full }
	if cell_size > int(header.cell_content_offset) - ptr_area_end { return .Page_Full }

	new_offset := int(header.cell_content_offset) - cell_size
	bytes_written, ok := cell.serialize(page.data[new_offset:], rowid, values)
	if !ok || bytes_written != cell_size { return .Serialization_Failed }

	insert_index := find_insert_index(page.data, header, page_num, rowid)
	raw_ptrs := get_raw_pointers(page.data, page_num)
	if insert_index < int(header.cell_count) {
		copy(raw_ptrs[insert_index + 1:], raw_ptrs[insert_index:header.cell_count])
	}

	raw_ptrs[insert_index] = Cell_Pointer(new_offset)
	header.cell_count += 1
	header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(p, page_num)
	return .None
}

find_child_page :: proc(page_data: []u8, header: ^Page_Header, page_num: u32, key: types.Row_ID) -> u32 {
	pointers := get_pointers(page_data, page_num)
	for ptr in pointers {
		cell_offset := int(ptr)
		key_val, _, _ := utils.varint_decode(page_data, cell_offset + 4)
		if u64(key) < key_val {
			child, _ := utils.read_u32_be(page_data, cell_offset)
			return child
		}
	}
	return get_right_ptr(page_data, page_num)
}

find_insert_index :: proc(
	page_data: []u8,
	header: ^Page_Header,
	page_num: u32,
	target_rowid: types.Row_ID,
) -> int {
	left := 0
	right := int(header.cell_count)
	pointers := get_pointers(page_data, page_num)
	for left < right {
		mid := left + (right - left) / 2
		cell_ptr := pointers[mid]
		rowid, ok := cell.get_rowid(page_data, int(cell_ptr))
		if !ok {
			return left
		}
		if rowid < target_rowid {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

rowid_exists :: proc(
	page_data: []u8,
	header: ^Page_Header,
	page_num: u32,
	target_rowid: types.Row_ID,
) -> bool {
	pointers := get_pointers(page_data, page_num)
	for ptr in pointers {
		rowid, ok := cell.get_rowid(page_data, int(ptr))
		if ok && rowid == target_rowid {
			return true
		}
	}
	return false
}

// Inserts a new row into a specific B-Tree Leaf Page.
// Returns .Page_Full if the page must be split.
insert_cell :: proc(
	p: ^pager.Pager,
	root_page_num: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	config := DEFAULT_CONFIG,
) -> Error {
	page, err := pager.get_page(p, root_page_num)
	if err != nil { return .Page_Read_Failed }

	hdr := get_header(page.data, root_page_num)
	if hdr.page_type == .INTERIOR_TABLE {
		split, err := insert_recursive(p, root_page_num, rowid, values)
		if err != .None {
			return err
		}
		if split.did_split {
			return split_interior_root(p, root_page_num, split)
		}
		return .None
	}
	return insert_cell_into_leaf(p, root_page_num, rowid, values)
}

find_leaf_page :: proc(p: ^pager.Pager, page_num: u32, key: types.Row_ID) -> (u32, Error) {
	curr := page_num
	for {
		page, _ := pager.get_page(p, curr)
		hdr := get_header(page.data, curr)
		if hdr.page_type == .LEAF_TABLE {
			return curr, .None
		}

		moved := false
		ptrs := get_pointers(page.data, curr)
		for ptr in ptrs {
			off := int(ptr)
			sep, _, _ := utils.varint_decode(page.data, off + 4)
			if u64(key) < sep {
				child, _ := utils.read_u32_be(page.data, off)
				curr = child
				moved = true
				break
			}
		}

		if !moved {
			curr = get_right_ptr(page.data, curr)
		}
	}
}

cursor_start :: proc(
	p: ^pager.Pager,
	root_page_num: u32,
	allocator := context.allocator,
) -> (
	Cursor,
	Error,
) {
	current_page_num := root_page_num
	for {
		page, err := pager.get_page(p, current_page_num)
		if err != nil {
			return Cursor{}, .Page_Read_Failed
		}

		header := get_header(page.data, current_page_num)
		if header.page_type == .LEAF_TABLE {
			break
		}
		if header.cell_count > 0 {
			pointers := get_pointers(page.data, current_page_num)
			first_cell_offset := int(pointers[0])
			child_page, _ := utils.read_u32_be(page.data, first_cell_offset)
			current_page_num = child_page
		} else {
			i_header := get_interior_header(page.data, current_page_num)
			current_page_num = u32(i_header.rightmost_ptr)
		}
	}

	return Cursor{page_num = current_page_num, cell_index = 0, end_of_table = false, allocator = allocator},
		.None
}

// Initializes a cursor at the end of a page
cursor_end :: proc(p: ^pager.Pager, page_num: u32, allocator := context.allocator) -> (Cursor, Error) {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return Cursor{}, .Page_Read_Failed
	}

	header := get_header(page.data, page_num)
	if header == nil {
		return Cursor{}, .Invalid_Page_Header
	}

	return Cursor {
			page_num = page_num,
			cell_index = int(header.cell_count),
			end_of_table = true,
			allocator = allocator,
		},
		.None
}

cursor_advance :: proc(p: ^pager.Pager, c: ^Cursor) -> Error {
	page, _ := pager.get_page(p, c.page_num)
	hdr := get_leaf_header(page.data, c.page_num)
	c.cell_index += 1
	if c.cell_index >= int(hdr.cell_count) {
		if hdr.next_leaf != 0 {
			c.page_num = u32(hdr.next_leaf)
			c.cell_index = 0
		} else {
			c.end_of_table = true
		}
	}
	return .None
}

cursor_get_cell :: proc(p: ^pager.Pager, cursor: ^Cursor, config := DEFAULT_CONFIG) -> (Cell_Ref, Error) {
	if cursor.end_of_table {
		return Cell_Ref{}, .Cell_Not_Found
	}

	page, p_err := pager.get_page(p, cursor.page_num)
	if p_err != nil {
		return Cell_Ref{}, .Page_Read_Failed
	}

	pointers := get_pointers(page.data, page.page_num)
	if cursor.cell_index >= len(pointers) {
		return Cell_Ref{}, .Cell_Not_Found
	}

	cell_ptr := pointers[cursor.cell_index]
	alloc := config.allocator
	if alloc.procedure == nil {
		alloc = cursor.allocator
	}

	cell_cfg := cell.Config {
		allocator = alloc,
		zero_copy = config.zero_copy,
	}

	c, _, ok := cell.deserialize(page.data, int(cell_ptr), cell_cfg)
	if !ok {
		return Cell_Ref{}, .Cell_Deserialize_Failed
	}
	return Cell_Ref{cell = c, allocator = alloc}, .None
}

find_by_rowid :: proc(
	p: ^pager.Pager,
	root_page_num: u32,
	target_rowid: types.Row_ID,
	config := DEFAULT_CONFIG,
) -> (
	Cell_Ref,
	Error,
) {
	target_page_num, find_err := find_leaf_page(p, root_page_num, target_rowid)
	if find_err != .None {
		return Cell_Ref{}, find_err
	}

	page, p_err := pager.get_page(p, target_page_num)
	if p_err != nil {
		return Cell_Ref{}, .Page_Read_Failed
	}

	header := get_header(page.data, target_page_num)
	if header.page_type != .LEAF_TABLE {
		return Cell_Ref{}, .Cell_Not_Found
	}
	if header.cell_count == 0 {
		return Cell_Ref{}, .Cell_Not_Found
	}

	left := 0
	right := int(header.cell_count) - 1
	pointers := get_pointers(page.data, target_page_num)
	for left <= right {
		mid := left + (right - left) / 2
		cell_ptr := pointers[mid]
		rowid, ok := cell.get_rowid(page.data, int(cell_ptr))
		if !ok {
			return Cell_Ref{}, .Invalid_Cell_Pointer
		}
		if rowid == target_rowid {
			alloc := config.allocator
			if alloc.procedure == nil {
				alloc = context.allocator
			}

			cell_cfg := cell.Config {
				allocator = alloc,
				zero_copy = config.zero_copy,
			}

			c, _, deserialize_ok := cell.deserialize(page.data, int(cell_ptr), cell_cfg)
			if !deserialize_ok {
				return Cell_Ref{}, .Cell_Deserialize_Failed
			}
			return Cell_Ref{cell = c, allocator = alloc}, .None
		} else if rowid < target_rowid {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return Cell_Ref{}, .Cell_Not_Found
}

// Suggests the next available RowID by looking at the last item in the table.
get_next_rowid :: proc(p: ^pager.Pager, page_num: u32) -> (types.Row_ID, Error) {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return 1, .Page_Read_Failed
	}

	header := get_header(page.data, page_num)
	if header.cell_count == 0 {
		return 1, .None
	}

	pointers := get_pointers(page.data, page_num)
	last_ptr := pointers[header.cell_count - 1]
	last_rowid, ok := cell.get_rowid(page.data, int(last_ptr))
	if !ok {
		return 1, .Invalid_Cell_Pointer
	}
	return last_rowid + 1, .None
}

// Counts total rows in a page.
count_rows :: proc(p: ^pager.Pager, page_num: u32) -> (int, Error) {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return 0, .Page_Read_Failed
	}
	header := get_header(page.data, page_num)
	return int(header.cell_count), .None
}

delete_cell :: proc(p: ^pager.Pager, page_num: u32, target_rowid: types.Row_ID) -> Error {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return .Page_Read_Failed
	}

	header := get_header(page.data, page_num)
	pointers := get_raw_pointers(page.data, page_num)
	active_pointers := pointers[:header.cell_count]
	delete_index := -1
	cell_size := 0
	for i in 0 ..< int(header.cell_count) {
		cell_ptr := active_pointers[i]
		rowid, ok := cell.get_rowid(page.data, int(cell_ptr))
		if !ok {
			return .Invalid_Cell_Pointer
		}
		if rowid == target_rowid {
			delete_index = i
			size, ok2 := cell.get_size(page.data, int(cell_ptr))
			if ok2 {
				cell_size = size
			}
			break
		}
	}

	if delete_index == -1 {
		return .Cell_Not_Found
	}
	if delete_index < int(header.cell_count) - 1 {
		copy(pointers[delete_index:], pointers[delete_index + 1:header.cell_count])
	}

	header.cell_count -= 1
	if cell_size > 0 && cell_size < 256 {
		header.fragmented_bytes += u8(cell_size)
	}

	pager.mark_dirty(p, page_num)
	return .None
}

debug_print_page :: proc(p: ^pager.Pager, page_num: u32, allocator := context.temp_allocator) {
	page, err := pager.get_page(p, page_num)
	if err != nil {
		fmt.printf("Error reading page %d\n", page_num)
		return
	}

	header := get_header(page.data, page_num)
	pointers := get_pointers(page.data, page_num)
	fmt.printf(
		"Page %d (type=%v, cells=%d, content_offset=%d, fragmented=%d)\n",
		page_num,
		header.page_type,
		header.cell_count,
		header.cell_content_offset,
		header.fragmented_bytes,
	)

	for ptr, i in pointers {
		cell_cfg := cell.Config {
			allocator = allocator,
			zero_copy = false,
		}

		c, _, ok := cell.deserialize(page.data, int(ptr), cell_cfg)
		if !ok {
			fmt.printf("  Cell %d: Error deserializing\n", i)
			continue
		}

		fmt.printf("  Cell %d: ", i)
		cell.debug_print(c)
		cell.destroy(&c)
	}
}

foreach_cell :: proc(
	p: ^pager.Pager,
	page_num: u32,
	callback: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	user_data: rawptr = nil,
) -> Error {
	cursor, _ := cursor_start(p, page_num, context.temp_allocator)
	for !cursor.end_of_table {
		free_all(context.temp_allocator)
		config := Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := cursor_get_cell(p, &cursor, config)
		if err != .None {
			return err
		}

		should_continue := callback(&cell_ref.cell, user_data)
		if !should_continue {
			break
		}
		cursor_advance(p, &cursor) or_return
	}
	return .None
}

verify_page :: proc(
	p: ^pager.Pager,
	page_num: u32,
	min_key: types.Row_ID,
	max_key: types.Row_ID,
	depth: int = 0,
) -> bool {
	page, err := pager.get_page(p, page_num)
	if err != nil {
		fmt.printf("❌ Failed to load page %d\n", page_num)
		return false
	}

	header := get_header(page.data, page_num)
	indent := strings.repeat("  ", depth, context.temp_allocator)
	fmt.printf(
		"%sPage %d type=%v cells=%d range=[%d, %d]\n",
		indent,
		page_num,
		header.page_type,
		header.cell_count,
		min_key,
		max_key,
	)

	if header.page_type == .LEAF_TABLE {
		ptrs := get_pointers(page.data, page_num)
		for ptr, i in ptrs {
			rowid, _ := cell.get_rowid(page.data, int(ptr))
			fmt.printf("%s  Leaf[%d] key=%d\n", indent, i, rowid)
			if rowid < min_key || rowid >= max_key {
				fmt.printf("❌ Leaf key %d out of range\n", rowid)
				return false
			}
		}
		return true
	}

	pointers := get_pointers(page.data, page_num)
	prev_key := min_key
	for ptr, i in pointers {
		cell_offset := int(ptr)
		child, _ := utils.read_u32_be(page.data, cell_offset)
		sep, _, _ := utils.varint_decode(page.data, cell_offset + 4)
		key := types.Row_ID(sep)
		fmt.printf("%s  Interior[%d] child=%d sep=%d\n", indent, i, child, key)
		if key < prev_key || key > max_key {
			fmt.printf("❌ Separator key %d out of range\n", key)
			return false
		}
		if !verify_page(p, child, prev_key, key, depth + 1) {
			return false
		}
		prev_key = key
	}

	right := get_right_ptr(page.data, page_num)
	fmt.printf("%s  Rightmost child=%d\n", indent, right)
	return verify_page(p, right, prev_key, max_key, depth + 1)
}
