package btree

import "core:fmt"
import "core:mem"
import "src:cell"
import "src:pager"
import "src:types"
import "src:utils"

// Page types
Page_Type :: enum u8 {
	INTERIOR_TABLE = 5,
	LEAF_TABLE     = 13,
}

// B-tree page header
BTree_Page_Header :: struct {
	page_type:           Page_Type,
	first_freeblock:     u16,
	cell_count:          u16,
	cell_content_offset: u16,
	fragmented_bytes:    u8,
}

BTREE_HEADER_SIZE :: 8
Cell_Pointer :: u16

// Error types for better error handling
BTree_Error :: enum {
	None,
	Page_Read_Failed,
	Invalid_Header,
	Invalid_Cell_Pointer,
	Cell_Deserialize_Failed,
	Page_Full,
	Duplicate_Rowid,
	Cell_Not_Found,
	Invalid_Bounds,
	Serialization_Failed,
}

// Result type for operations that can fail
BTree_Result :: union($T: typeid) {
	T,
	BTree_Error,
}

// B-tree cursor with memory management
BTree_Cursor :: struct {
	page_num:     u32,
	cell_index:   int,
	end_of_table: bool,
	allocator:    mem.Allocator,
}

// Cell reference that owns its memory
// Caller MUST call btree_cell_ref_destroy when done
BTree_Cell_Ref :: struct {
	cell:      cell.Cell,
	allocator: mem.Allocator,
}

btree_cell_ref_destroy :: proc(ref: ^BTree_Cell_Ref) {
	cell.cell_destroy(&ref.cell)
}

// Configuration for B-tree operations
BTree_Config :: struct {
	allocator:        mem.Allocator,
	zero_copy:        bool,
	check_duplicates: bool,
}

DEFAULT_CONFIG := BTree_Config {
	allocator        = {},
	zero_copy        = false,
	check_duplicates = true,
}

// Parse page header from page data
btree_parse_header :: proc(page_data: []u8) -> (BTree_Page_Header, BTree_Error) {
	if len(page_data) < BTREE_HEADER_SIZE {
		return BTree_Page_Header{}, .Invalid_Header
	}

	header: BTree_Page_Header
	header.page_type = Page_Type(page_data[0])
	first_freeblock, ok1 := utils.read_u16_le(page_data, 1)
	if !ok1 {
		return BTree_Page_Header{}, .Invalid_Header
	}

	header.first_freeblock = first_freeblock
	cell_count, ok2 := utils.read_u16_le(page_data, 3)
	if !ok2 {
		return BTree_Page_Header{}, .Invalid_Header
	}

	header.cell_count = cell_count
	cell_content_offset, ok3 := utils.read_u16_le(page_data, 5)
	if !ok3 {
		return BTree_Page_Header{}, .Invalid_Header
	}

	header.cell_content_offset = cell_content_offset
	header.fragmented_bytes = page_data[7]
	if int(header.cell_content_offset) > len(page_data) {
		return BTree_Page_Header{}, .Invalid_Header
	}
	return header, .None
}

// Write page header to page data
btree_write_header :: proc(page_data: []u8, header: BTree_Page_Header) -> BTree_Error {
	if len(page_data) < BTREE_HEADER_SIZE {
		return .Invalid_Bounds
	}

	page_data[0] = u8(header.page_type)
	utils.write_u16_le(page_data, 1, header.first_freeblock)
	utils.write_u16_le(page_data, 3, header.cell_count)
	utils.write_u16_le(page_data, 5, header.cell_content_offset)
	page_data[7] = header.fragmented_bytes
	return .None
}

// Initialize a new leaf page
btree_init_leaf_page :: proc(page_data: []u8) -> BTree_Error {
	if len(page_data) < BTREE_HEADER_SIZE {
		return .Invalid_Bounds
	}

	mem.zero_slice(page_data)
	header := BTree_Page_Header {
		page_type           = .LEAF_TABLE,
		first_freeblock     = 0,
		cell_count          = 0,
		cell_content_offset = u16(len(page_data)),
		fragmented_bytes    = 0,
	}
	return btree_write_header(page_data, header)
}

// Get cell pointer offset for given cell index
btree_cell_pointer_offset :: proc(cell_index: int) -> int {
	return BTREE_HEADER_SIZE + cell_index * 2
}

// Read a cell pointer from page
btree_read_cell_pointer :: proc(page_data: []u8, cell_index: int) -> (u16, BTree_Error) {
	offset := btree_cell_pointer_offset(cell_index)
	if offset + 2 > len(page_data) {
		return 0, .Invalid_Cell_Pointer
	}
	x, _ := utils.read_u16_le(page_data, offset)
	return x, .None
}

// Write a cell pointer to page
btree_write_cell_pointer :: proc(page_data: []u8, cell_index: int, cell_offset: u16) -> BTree_Error {
	offset := btree_cell_pointer_offset(cell_index)
	if offset + 2 > len(page_data) {
		return .Invalid_Bounds
	}
	utils.write_u16_le(page_data, offset, cell_offset)
	return .None
}

// Check if a rowid already exists in the page
btree_rowid_exists :: proc(page_data: []u8, header: BTree_Page_Header, target_rowid: types.Row_ID) -> bool {
	for i in 0 ..< int(header.cell_count) {
		cell_ptr, err := btree_read_cell_pointer(page_data, i)
		if err != .None {
			continue
		}

		rowid, ok := cell.cell_get_rowid(page_data, int(cell_ptr))
		if ok && rowid == target_rowid {
			return true
		}
	}
	return false
}

// Insert a cell into a leaf page with proper error handling
btree_insert_cell :: proc(
	p: ^pager.Pager,
	page_num: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	config := DEFAULT_CONFIG,
) -> BTree_Error {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return parse_err
	}
	if config.check_duplicates && btree_rowid_exists(page.data, header, rowid) {
		return .Duplicate_Rowid
	}

	cell_size := cell.cell_calculate_size(rowid, values)
	fmt.printfln(
		"DEBUG INSERT: page=%d, rowid=%d, cell_size=%d, current_offset=%d, new_offset=%d",
		page_num,
		rowid,
		cell_size,
		header.cell_content_offset,
		int(header.cell_content_offset) - cell_size,
	)

	cell_pointer_area_end := BTREE_HEADER_SIZE + int(header.cell_count + 1) * 2
	if cell_pointer_area_end >= int(header.cell_content_offset) {
		return .Page_Full
	}

	available_space := int(header.cell_content_offset) - cell_pointer_area_end
	if cell_size > available_space {
		return .Page_Full
	}
	if cell_size > int(header.cell_content_offset) {
		return .Invalid_Bounds
	}

	new_content_offset := int(header.cell_content_offset) - cell_size
	if new_content_offset < cell_pointer_area_end {
		return .Page_Full
	}

	bytes_written, serialize_ok := cell.cell_serialize(page.data[new_content_offset:], rowid, values)
	if !serialize_ok || bytes_written != cell_size {
		return .Serialization_Failed
	}

	insert_index := btree_find_insert_index(page.data, header, rowid)
	if insert_index < int(header.cell_count) {
		src_offset := btree_cell_pointer_offset(insert_index)
		dst_offset := btree_cell_pointer_offset(insert_index + 1)
		bytes_to_move := int(header.cell_count - u16(insert_index)) * 2
		if dst_offset + bytes_to_move > len(page.data) {
			return .Invalid_Bounds
		}

		copy(
			page.data[dst_offset:dst_offset + bytes_to_move],
			page.data[src_offset:src_offset + bytes_to_move],
		)
	}

	write_err := btree_write_cell_pointer(page.data, insert_index, u16(new_content_offset))
	if write_err != .None {
		return write_err
	}

	header.cell_count += 1
	header.cell_content_offset = u16(new_content_offset)
	return btree_write_header(page.data, header)
}

// Create cursor at start of table
btree_cursor_start :: proc(page_num: u32, allocator := context.allocator) -> BTree_Cursor {
	return BTree_Cursor{page_num = page_num, cell_index = 0, end_of_table = false, allocator = allocator}
}

// Create cursor at end of table (for appending)
btree_cursor_end :: proc(
	p: ^pager.Pager,
	page_num: u32,
	allocator := context.allocator,
) -> (
	BTree_Cursor,
	BTree_Error,
) {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return BTree_Cursor{page_num = page_num, cell_index = 0, end_of_table = true, allocator = allocator},
			.Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return BTree_Cursor{page_num = page_num, cell_index = 0, end_of_table = true, allocator = allocator},
			parse_err
	}

	return BTree_Cursor {
			page_num = page_num,
			cell_index = int(header.cell_count),
			end_of_table = true,
			allocator = allocator,
		},
		.None
}

// Advance cursor to next cell
btree_cursor_advance :: proc(p: ^pager.Pager, cursor: ^BTree_Cursor) -> BTree_Error {
	page, err := pager.pager_get_page(p, cursor.page_num)
	if err != nil {
		cursor.end_of_table = true
		return .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		cursor.end_of_table = true
		return parse_err
	}

	cursor.cell_index += 1
	if cursor.cell_index >= int(header.cell_count) {
		cursor.end_of_table = true
	}

	return .None
}

// Get current cell from cursor position
// IMPORTANT: Caller MUST call btree_cell_ref_destroy on the returned reference
btree_cursor_get_cell :: proc(
	p: ^pager.Pager,
	cursor: ^BTree_Cursor,
	config := DEFAULT_CONFIG,
) -> (
	BTree_Cell_Ref,
	BTree_Error,
) {
	if cursor.end_of_table {
		return BTree_Cell_Ref{}, .Cell_Not_Found
	}

	page, err := pager.pager_get_page(p, cursor.page_num)
	if err != nil {
		return BTree_Cell_Ref{}, .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return BTree_Cell_Ref{}, parse_err
	}
	if cursor.cell_index >= int(header.cell_count) {
		return BTree_Cell_Ref{}, .Cell_Not_Found
	}

	cell_ptr, ptr_err := btree_read_cell_pointer(page.data, cursor.cell_index)
	if ptr_err != .None {
		return BTree_Cell_Ref{}, ptr_err
	}

	alloc := config.allocator
	if alloc.procedure == nil {
		alloc = cursor.allocator
	}

	deserialize_opts := cell.Deserialize_Options {
		allocator = alloc,
		zero_copy = config.zero_copy,
	}

	c, _, ok := cell.cell_deserialize(page.data, int(cell_ptr), deserialize_opts)
	if !ok {
		return BTree_Cell_Ref{}, .Cell_Deserialize_Failed
	}
	return BTree_Cell_Ref{cell = c, allocator = alloc}, .None
}

// Find a cell by rowid using binary search
// IMPORTANT: Caller MUST call btree_cell_ref_destroy on the returned reference
btree_find_by_rowid :: proc(
	p: ^pager.Pager,
	page_num: u32,
	target_rowid: types.Row_ID,
	config := DEFAULT_CONFIG,
) -> (
	BTree_Cell_Ref,
	BTree_Error,
) {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return BTree_Cell_Ref{}, .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return BTree_Cell_Ref{}, parse_err
	}
	if header.cell_count == 0 {
		return BTree_Cell_Ref{}, .Cell_Not_Found
	}

	left := 0
	right := int(header.cell_count) - 1
	for left <= right {
		mid := left + (right - left) / 2
		cell_ptr, ptr_err := btree_read_cell_pointer(page.data, mid)
		if ptr_err != .None {
			return BTree_Cell_Ref{}, ptr_err
		}

		rowid, ok := cell.cell_get_rowid(page.data, int(cell_ptr))
		if !ok {
			return BTree_Cell_Ref{}, .Invalid_Cell_Pointer
		}
		if rowid == target_rowid {
			alloc := config.allocator
			if alloc.procedure == nil {
				alloc = context.allocator
			}

			deserialize_opts := cell.Deserialize_Options {
				allocator = alloc,
				zero_copy = config.zero_copy,
			}

			c, _, deserialize_ok := cell.cell_deserialize(page.data, int(cell_ptr), deserialize_opts)
			if !deserialize_ok {
				return BTree_Cell_Ref{}, .Cell_Deserialize_Failed
			}
			return BTree_Cell_Ref{cell = c, allocator = alloc}, .None
		} else if rowid < target_rowid {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return BTree_Cell_Ref{}, .Cell_Not_Found
}

// Returns the index where the rowid should be inserted
btree_find_insert_index :: proc(
	page_data: []u8,
	header: BTree_Page_Header,
	target_rowid: types.Row_ID,
) -> int {
	left := 0
	right := int(header.cell_count)
	for left < right {
		mid := left + (right - left) / 2
		cell_ptr, err := btree_read_cell_pointer(page_data, mid)
		if err != .None {
			return left
		}

		rowid, ok := cell.cell_get_rowid(page_data, int(cell_ptr))
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

// Get the next available rowid for a table
btree_get_next_rowid :: proc(p: ^pager.Pager, page_num: u32) -> (types.Row_ID, BTree_Error) {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return 1, .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None || header.cell_count == 0 {
		return 1, parse_err
	}

	last_index := int(header.cell_count) - 1
	cell_ptr, ptr_err := btree_read_cell_pointer(page.data, last_index)
	if ptr_err != .None {
		return 1, ptr_err
	}

	last_rowid, ok := cell.cell_get_rowid(page.data, int(cell_ptr))
	if !ok {
		return 1, .Invalid_Cell_Pointer
	}
	return last_rowid + 1, .None
}

// Get number of rows in a table
btree_count_rows :: proc(p: ^pager.Pager, page_num: u32) -> (int, BTree_Error) {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return 0, .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return 0, parse_err
	}
	return int(header.cell_count), .None
}

// Delete a cell by rowid
btree_delete_cell :: proc(p: ^pager.Pager, page_num: u32, target_rowid: types.Row_ID) -> BTree_Error {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		return .Page_Read_Failed
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		return parse_err
	}

	delete_index := -1
	cell_offset := u16(0)
	cell_size := 0
	for i in 0 ..< int(header.cell_count) {
		cell_ptr, ptr_err := btree_read_cell_pointer(page.data, i)
		if ptr_err != .None {
			return ptr_err
		}

		rowid, ok := cell.cell_get_rowid(page.data, int(cell_ptr))
		if !ok {
			return .Invalid_Cell_Pointer
		}
		if rowid == target_rowid {
			delete_index = i
			cell_offset = cell_ptr
			size, ok2 := cell.cell_get_size(page.data, int(cell_ptr))
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
		src_offset := btree_cell_pointer_offset(delete_index + 1)
		dst_offset := btree_cell_pointer_offset(delete_index)
		bytes_to_move := (int(header.cell_count) - delete_index - 1) * 2

		copy(
			page.data[dst_offset:dst_offset + bytes_to_move],
			page.data[src_offset:src_offset + bytes_to_move],
		)
	}

	header.cell_count -= 1
	if cell_size > 0 && cell_size < 256 {
		header.fragmented_bytes += u8(cell_size)
	}
	return btree_write_header(page.data, header)
}

// Debug: Print all cells in a page with proper cleanup
btree_debug_print_page :: proc(p: ^pager.Pager, page_num: u32, allocator := context.temp_allocator) {
	page, err := pager.pager_get_page(p, page_num)
	if err != nil {
		fmt.printf("Error reading page %d\n", page_num)
		return
	}

	header, parse_err := btree_parse_header(page.data)
	if parse_err != .None {
		fmt.println("Error parsing page header")
		return
	}

	fmt.printf(
		"Page %d (type=%v, cells=%d, content_offset=%d, fragmented=%d)\n",
		page_num,
		header.page_type,
		header.cell_count,
		header.cell_content_offset,
		header.fragmented_bytes,
	)

	for i in 0 ..< int(header.cell_count) {
		cell_ptr, ptr_err := btree_read_cell_pointer(page.data, i)
		if ptr_err != .None {
			fmt.printf("  Cell %d: Error reading pointer\n", i)
			continue
		}

		deserialize_opts := cell.Deserialize_Options {
			allocator = allocator,
			zero_copy = false,
		}

		c, _, ok := cell.cell_deserialize(page.data, int(cell_ptr), deserialize_opts)
		if !ok {
			fmt.printf("  Cell %d: Error deserializing\n", i)
			continue
		}

		fmt.printf("  Cell %d: ", i)
		cell.cell_debug_print(c)
		cell.cell_destroy(&c)
	}
}

// Helper: Iterate all cells with automatic cleanup
btree_foreach_cell :: proc(
	p: ^pager.Pager,
	page_num: u32,
	callback: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	user_data: rawptr = nil,
) -> BTree_Error {
	cursor := btree_cursor_start(page_num, context.temp_allocator)
	for !cursor.end_of_table {
		free_all(context.temp_allocator)
		config := BTree_Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree_cursor_get_cell(p, &cursor, config)
		if err != .None {
			return err
		}

		should_continue := callback(&cell_ref.cell, user_data)
		if !should_continue {
			break
		}

		advance_err := btree_cursor_advance(p, &cursor)
		if advance_err != .None {
			return advance_err
		}
	}
	return .None
}
