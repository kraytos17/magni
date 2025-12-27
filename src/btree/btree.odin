package btree

import "core:fmt"
import "core:mem"
import "src:cell"
import "src:pager"
import "src:types"

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

// Casts the raw page buffer directly to a Page_Header struct pointer.
// Returns nil if the buffer is too small.
get_header :: proc(page_data: []u8) -> ^Page_Header {
	if len(page_data) < size_of(Page_Header) {
		return nil
	}
	return (^Page_Header)(raw_data(page_data))
}

// Returns a slice view of the active cell pointers.
get_pointers :: proc(page_data: []u8) -> []Cell_Pointer {
	header := get_header(page_data)
	if header == nil {
		return nil
	}
	ptr_start := raw_data(page_data[size_of(Page_Header):])
	return ([^]Cell_Pointer)(ptr_start)[:header.cell_count]
}

// Returns a slice view allowing access to "potential" pointers beyond cell_count.
// Used during insertion to shift memory.
get_raw_pointers :: proc(page_data: []u8) -> []Cell_Pointer {
	max_ptrs := (len(page_data) - size_of(Page_Header)) / size_of(Cell_Pointer)
	ptr_start := raw_data(page_data[size_of(Page_Header):])
	return ([^]Cell_Pointer)(ptr_start)[:max_ptrs]
}

init_leaf_page :: proc(page_data: []u8) -> Error {
	if len(page_data) < size_of(Page_Header) {
		return .Invalid_Bounds
	}

	mem.zero_slice(page_data)
	header := get_header(page_data)
	header.page_type = .LEAF_TABLE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = u16le(len(page_data))
	header.fragmented_bytes = 0

	return .None
}

find_insert_index :: proc(page_data: []u8, header: ^Page_Header, target_rowid: types.Row_ID) -> int {
	left := 0
	right := int(header.cell_count)
	pointers := get_pointers(page_data)
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

rowid_exists :: proc(page_data: []u8, header: ^Page_Header, target_rowid: types.Row_ID) -> bool {
	pointers := get_pointers(page_data)
	for ptr in pointers {
		rowid, ok := cell.get_rowid(page_data, int(ptr))
		if ok && rowid == target_rowid {
			return true
		}
	}
	return false
}

// Inserts a new row into a specific B-Tree Leaf Page.
//
// Logic:
// 1. Checks bounds to ensure the cell fits in the "unallocated" space in the middle.
// 2. Writes the cell data at the bottom of the free space (growing upwards).
// 3. Shifts existing pointers to keep the pointer array sorted by Key/RowID.
// 4. Updates header stats.
//
// Returns .Page_Full if the page must be split.
insert_cell :: proc(
	p: ^pager.Pager,
	page_num: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	config := DEFAULT_CONFIG,
) -> Error {
	page, err := pager.get_page(p, page_num)
	if err != nil { return .Page_Read_Failed }

	header := get_header(page.data)
	if config.check_duplicates && rowid_exists(page.data, header, rowid) {
		return .Duplicate_Rowid
	}

	cell_size := cell.calculate_size(rowid, values)
	ptr_area_end := size_of(Page_Header) + int(header.cell_count + 1) * size_of(Cell_Pointer)
	if ptr_area_end >= int(header.cell_content_offset) {
		return .Page_Full
	}

	available_space := int(header.cell_content_offset) - ptr_area_end
	if cell_size > available_space {
		return .Page_Full
	}

	new_offset := int(header.cell_content_offset) - cell_size
	bytes_written, ok := cell.serialize(page.data[new_offset:], rowid, values)
	if !ok || bytes_written != cell_size {
		return .Serialization_Failed
	}

	insert_index := find_insert_index(page.data, header, rowid)
	raw_ptrs := get_raw_pointers(page.data)
	if insert_index < int(header.cell_count) {
		copy(raw_ptrs[insert_index + 1:], raw_ptrs[insert_index:header.cell_count])
	}

	raw_ptrs[insert_index] = Cell_Pointer(new_offset)
	header.cell_count += 1
	header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(p, page_num)
	return .None
}

cursor_start :: proc(page_num: u32, allocator := context.allocator) -> Cursor {
	return Cursor{page_num = page_num, cell_index = 0, end_of_table = false, allocator = allocator}
}

// Initializes a cursor at the end of a page
cursor_end :: proc(p: ^pager.Pager, page_num: u32, allocator := context.allocator) -> (Cursor, Error) {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return Cursor{}, .Page_Read_Failed
	}

	header := get_header(page.data)
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

cursor_advance :: proc(p: ^pager.Pager, cursor: ^Cursor) -> Error {
	page, p_err := pager.get_page(p, cursor.page_num)
	if p_err != nil {
		return .Page_Read_Failed
	}

	header := get_header(page.data)
	cursor.cell_index += 1
	if cursor.cell_index >= int(header.cell_count) {
		cursor.end_of_table = true
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

	pointers := get_pointers(page.data)
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
	page_num: u32,
	target_rowid: types.Row_ID,
	config := DEFAULT_CONFIG,
) -> (
	Cell_Ref,
	Error,
) {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return Cell_Ref{}, .Page_Read_Failed
	}

	header := get_header(page.data)
	if header.cell_count == 0 {
		return Cell_Ref{}, .Cell_Not_Found
	}

	left := 0
	right := int(header.cell_count) - 1
	pointers := get_pointers(page.data)
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

	header := get_header(page.data)
	if header.cell_count == 0 {
		return 1, .None
	}

	pointers := get_pointers(page.data)
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
	header := get_header(page.data)
	return int(header.cell_count), .None
}

delete_cell :: proc(p: ^pager.Pager, page_num: u32, target_rowid: types.Row_ID) -> Error {
	page, p_err := pager.get_page(p, page_num)
	if p_err != nil {
		return .Page_Read_Failed
	}
	
	header := get_header(page.data)
	pointers := get_raw_pointers(page.data)
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

	header := get_header(page.data)
	pointers := get_pointers(page.data)
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
	cursor := cursor_start(page_num, context.temp_allocator)
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
