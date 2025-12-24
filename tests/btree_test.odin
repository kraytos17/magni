package tests

import "core:fmt"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

// Helper to create a test pager with a single page
create_test_pager :: proc(t: ^testing.T) -> (^pager.Pager, u32) {
	p := new(pager.Pager)
	p.page_size = types.PAGE_SIZE
	p.page_cache = make(map[u32]^pager.Page)
	p.max_cache_pages = 256
	page := pager.page_new(1, p.page_size)
	testing.expect(t, page != nil, "Failed to create page")

	p.page_cache[1] = page
	p.file_len = i64(p.page_size)

	err := btree.btree_init_leaf_page(page.data)
	testing.expect(t, err == .None, "Failed to initialize leaf page")

	return p, 1
}

// Helper to destroy test pager
destroy_test_pager :: proc(p: ^pager.Pager) {
	for _, page in p.page_cache {
		pager.page_destroy(page)
	}
	delete(p.page_cache)
	free(p)
}

@(test)
test_btree_parse_header :: proc(t: ^testing.T) {
	page_data := make([]u8, types.PAGE_SIZE)
	defer delete(page_data)

	err := btree.btree_init_leaf_page(page_data)
	testing.expect(t, err == .None, "Failed to initialize page")

	header, parse_err := btree.btree_parse_header(page_data)
	testing.expect(t, parse_err == .None, "Failed to parse header")
	testing.expect(t, header.page_type == .LEAF_TABLE, "Wrong page type")
	testing.expect(t, header.cell_count == 0, "Cell count should be 0")
	testing.expect(t, header.cell_content_offset == types.PAGE_SIZE, "Wrong content offset")
	testing.expect(t, header.fragmented_bytes == 0, "Fragmented bytes should be 0")
}

@(test)
test_btree_parse_header_invalid :: proc(t: ^testing.T) {
	page_data := make([]u8, 4)
	defer delete(page_data)

	_, err := btree.btree_parse_header(page_data)
	testing.expect(t, err == .Invalid_Header, "Should fail with invalid header")
}

@(test)
test_btree_write_header :: proc(t: ^testing.T) {
	page_data := make([]u8, types.PAGE_SIZE)
	defer delete(page_data)

	header := btree.BTree_Page_Header {
		page_type           = .LEAF_TABLE,
		first_freeblock     = 100,
		cell_count          = 5,
		cell_content_offset = 3000,
		fragmented_bytes    = 10,
	}

	err := btree.btree_write_header(page_data, header)
	testing.expect(t, err == .None, "Failed to write header")

	read_header, parse_err := btree.btree_parse_header(page_data)
	testing.expect(t, parse_err == .None, "Failed to parse written header")
	testing.expect(t, read_header.page_type == header.page_type, "Page type mismatch")
	testing.expect(t, read_header.first_freeblock == header.first_freeblock, "First freeblock mismatch")
	testing.expect(t, read_header.cell_count == header.cell_count, "Cell count mismatch")
	testing.expect(
		t,
		read_header.cell_content_offset == header.cell_content_offset,
		"Content offset mismatch",
	)
	testing.expect(t, read_header.fragmented_bytes == header.fragmented_bytes, "Fragmented bytes mismatch")
}

@(test)
test_btree_init_leaf_page :: proc(t: ^testing.T) {
	page_data := make([]u8, types.PAGE_SIZE)
	defer delete(page_data)

	err := btree.btree_init_leaf_page(page_data)
	testing.expect(t, err == .None, "Failed to initialize leaf page")

	header, parse_err := btree.btree_parse_header(page_data)
	testing.expect(t, parse_err == .None, "Failed to parse header")
	testing.expect(t, header.page_type == .LEAF_TABLE, "Should be leaf table")
	testing.expect(t, header.cell_count == 0, "Should have no cells")
	testing.expect(t, header.cell_content_offset == u16(len(page_data)), "Content offset should be at end")
}

@(test)
test_btree_cell_pointer_operations :: proc(t: ^testing.T) {
	page_data := make([]u8, types.PAGE_SIZE)
	defer delete(page_data)

	err := btree.btree_init_leaf_page(page_data)
	testing.expect(t, err == .None, "Failed to initialize page")

	test_offset := u16(3000)
	write_err := btree.btree_write_cell_pointer(page_data, 0, test_offset)
	testing.expect(t, write_err == .None, "Failed to write cell pointer")

	read_offset, read_err := btree.btree_read_cell_pointer(page_data, 0)
	testing.expect(t, read_err == .None, "Failed to read cell pointer")
	testing.expect(t, read_offset == test_offset, "Cell pointer mismatch")
}

@(test)
test_btree_insert_cell_single :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	values := []types.Value{types.value_int(42), types.value_text("Hello")}
	err := btree.btree_insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .None, "Failed to insert cell")

	page, _ := pager.pager_get_page(p, page_num)
	header, _ := btree.btree_parse_header(page.data)
	testing.expect(t, header.cell_count == 1, "Cell count should be 1")
	testing.expect(t, header.cell_content_offset < types.PAGE_SIZE, "Content offset should have decreased")
}

@(test)
test_btree_insert_cell_multiple_ordered :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 10))}
		err := btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
		testing.expect(t, err == .None, fmt.tprintf("Failed to insert cell %d", i))
	}

	page, _ := pager.pager_get_page(p, page_num)
	header, _ := btree.btree_parse_header(page.data)
	testing.expect(t, header.cell_count == 5, "Should have 5 cells")
}

@(test)
test_btree_insert_cell_unordered :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	rowids := []types.Row_ID{5, 2, 8, 1, 4}
	for rowid in rowids {
		values := []types.Value{types.value_int(i64(rowid) * 100)}
		err := btree.btree_insert_cell(p, page_num, rowid, values)
		testing.expect(t, err == .None, fmt.tprintf("Failed to insert rowid %d", rowid))
	}

	page, _ := pager.pager_get_page(p, page_num)
	header, _ := btree.btree_parse_header(page.data)
	prev_rowid := types.Row_ID(0)
	for i in 0 ..< int(header.cell_count) {
		cell_ptr, _ := btree.btree_read_cell_pointer(page.data, i)
		rowid, ok := cell.cell_get_rowid(page.data, int(cell_ptr))
		testing.expect(t, ok, "Failed to get rowid")
		testing.expect(t, rowid > prev_rowid, "Rowids should be sorted")
		prev_rowid = rowid
	}
}

@(test)
test_btree_insert_cell_duplicate :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	values := []types.Value{types.value_int(42)}
	err := btree.btree_insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .None, "Failed to insert first cell")

	err = btree.btree_insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .Duplicate_Rowid, "Should reject duplicate rowid")
}

@(test)
test_btree_cursor_start :: proc(t: ^testing.T) {
	cursor := btree.btree_cursor_start(1)
	testing.expect(t, cursor.page_num == 1, "Wrong page number")
	testing.expect(t, cursor.cell_index == 0, "Should start at index 0")
	testing.expect(t, !cursor.end_of_table, "Should not be at end")
}

@(test)
test_btree_cursor_end :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 3 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	cursor, err := btree.btree_cursor_end(p, page_num)
	testing.expect(t, err == .None, "Failed to create end cursor")
	testing.expect(t, cursor.cell_index == 3, "Should be at position 3")
	testing.expect(t, cursor.end_of_table, "Should be at end")
}

@(test)
test_btree_cursor_advance :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 3 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	cursor := btree.btree_cursor_start(page_num)
	for i in 0 ..< 3 {
		testing.expect(t, !cursor.end_of_table, "Should not be at end")
		err := btree.btree_cursor_advance(p, &cursor)
		testing.expect(t, err == .None, "Failed to advance cursor")
	}
	testing.expect(t, cursor.end_of_table, "Should be at end after advancing past last cell")
}

@(test)
test_btree_cursor_get_cell :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	test_rowid := types.Row_ID(42)
	test_value := i64(123)
	values := []types.Value{types.value_int(test_value)}
	btree.btree_insert_cell(p, page_num, test_rowid, values)

	cursor := btree.btree_cursor_start(page_num)
	cell_ref, err := btree.btree_cursor_get_cell(p, &cursor)
	defer btree.btree_cell_ref_destroy(&cell_ref)

	testing.expect(t, err == .None, "Failed to get cell")
	testing.expect(t, cell_ref.cell.rowid == test_rowid, "Rowid mismatch")
	testing.expect(t, len(cell_ref.cell.values) == 1, "Should have 1 value")

	val := cell_ref.cell.values[0].(i64)
	testing.expect(t, val == test_value, "Value mismatch")
}

@(test)
test_btree_find_by_rowid :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 10 {
		values := []types.Value{types.value_int(i64(i * 100))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	target_rowid := types.Row_ID(7)
	cell_ref, err := btree.btree_find_by_rowid(p, page_num, target_rowid)
	defer btree.btree_cell_ref_destroy(&cell_ref)

	testing.expect(t, err == .None, "Failed to find cell")
	testing.expect(t, cell_ref.cell.rowid == target_rowid, "Found wrong rowid")

	val := cell_ref.cell.values[0].(i64)
	testing.expect(t, val == 700, "Value should be 700")
}

@(test)
test_btree_find_by_rowid_not_found :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	cell_ref, err := btree.btree_find_by_rowid(p, page_num, 99)
	testing.expect(t, err == .Cell_Not_Found, "Should not find cell")
}

@(test)
test_btree_get_next_rowid :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	next_id, err := btree.btree_get_next_rowid(p, page_num)
	testing.expect(t, err == .None, "Failed to get next rowid")
	testing.expect(t, next_id == 1, "First rowid should be 1")

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	next_id, err = btree.btree_get_next_rowid(p, page_num)
	testing.expect(t, err == .None, "Failed to get next rowid")
	testing.expect(t, next_id == 6, "Next rowid should be 6")
}

@(test)
test_btree_count_rows :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	count, err := btree.btree_count_rows(p, page_num)
	testing.expect(t, err == .None, "Failed to count rows")
	testing.expect(t, count == 0, "Empty page should have 0 rows")

	for i in 1 ..= 7 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	count, err = btree.btree_count_rows(p, page_num)
	testing.expect(t, err == .None, "Failed to count rows")
	testing.expect(t, count == 7, "Should have 7 rows")
}

@(test)
test_btree_delete_cell :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 10))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	err := btree.btree_delete_cell(p, page_num, 3)
	testing.expect(t, err == .None, "Failed to delete cell")

	count, _ := btree.btree_count_rows(p, page_num)
	testing.expect(t, count == 4, "Should have 4 rows after deletion")

	_, find_err := btree.btree_find_by_rowid(p, page_num, 3)
	testing.expect(t, find_err == .Cell_Not_Found, "Deleted cell should not be found")

	ids := []types.Row_ID{1, 2, 4, 5}
	for rowid in ids {
		cell_ref, find_err := btree.btree_find_by_rowid(p, page_num, rowid)
		defer btree.btree_cell_ref_destroy(&cell_ref)
		testing.expect(t, find_err == .None, fmt.tprintf("Should find rowid %d", rowid))
	}
}

@(test)
test_btree_delete_cell_not_found :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	err := btree.btree_delete_cell(p, page_num, 1)
	testing.expect(t, err == .Cell_Not_Found, "Should not find cell to delete")
}

@(test)
test_btree_delete_cell_first :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 3 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	err := btree.btree_delete_cell(p, page_num, 1)
	testing.expect(t, err == .None, "Failed to delete first cell")

	count, _ := btree.btree_count_rows(p, page_num)
	testing.expect(t, count == 2, "Should have 2 rows")
}

@(test)
test_btree_delete_cell_last :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 3 {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	err := btree.btree_delete_cell(p, page_num, 3)
	testing.expect(t, err == .None, "Failed to delete last cell")

	count, _ := btree.btree_count_rows(p, page_num)
	testing.expect(t, count == 2, "Should have 2 rows")
}

@(test)
test_btree_foreach_cell :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 100))}
		btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
	}

	Context :: struct {
		count: int,
		sum:   i64,
	}

	ctx := Context {
		count = 0,
		sum   = 0,
	}

	callback :: proc(c: ^cell.Cell, user_data: rawptr) -> bool {
		ctx := cast(^Context)user_data
		ctx.count += 1
		val := c.values[0].(i64)
		ctx.sum += val
		return true
	}

	err := btree.btree_foreach_cell(p, page_num, callback, &ctx)
	testing.expect(t, err == .None, "Failed to iterate cells")
	testing.expect(t, ctx.count == 5, "Should iterate 5 cells")
	testing.expect(t, ctx.sum == 1500, "Sum should be 1500")
}

@(test)
test_btree_rowid_exists :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	values := []types.Value{types.value_int(42)}

	btree.btree_insert_cell(p, page_num, 5, values)
	page, _ := pager.pager_get_page(p, page_num)
	header, _ := btree.btree_parse_header(page.data)
	exists := btree.btree_rowid_exists(page.data, header, 5)
	testing.expect(t, exists, "Rowid 5 should exist")

	not_exists := btree.btree_rowid_exists(page.data, header, 10)
	testing.expect(t, !not_exists, "Rowid 10 should not exist")
}

@(test)
test_btree_large_insertion :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	num_cells := 50
	for i in 1 ..= num_cells {
		text_str := fmt.tprintf("Row %d", i)
		values := []types.Value{types.value_int(i64(i)), types.value_text(text_str)}
		err := btree.btree_insert_cell(p, page_num, types.Row_ID(i), values)
		if err != .None {
			fmt.printf("Failed to insert cell %d: %v\n", i, err)
			break
		}
	}

	count, _ := btree.btree_count_rows(p, page_num)
	testing.expect(t, count > 0, "Should have inserted some cells")
}

@(test)
test_btree_find_insert_index :: proc(t: ^testing.T) {
	p, page_num := create_test_pager(t)
	defer destroy_test_pager(p)

	ids := []types.Row_ID{1, 3, 5, 7, 9}
	for i in ids {
		values := []types.Value{types.value_int(i64(i))}
		btree.btree_insert_cell(p, page_num, i, values)
	}

	page, _ := pager.pager_get_page(p, page_num)
	header, _ := btree.btree_parse_header(page.data)

	index := btree.btree_find_insert_index(page.data, header, 0)
	testing.expect(t, index == 0, "Should insert at beginning")

	index = btree.btree_find_insert_index(page.data, header, 2)
	testing.expect(t, index == 1, "Should insert after 1")

	index = btree.btree_find_insert_index(page.data, header, 6)
	testing.expect(t, index == 3, "Should insert after 5")

	index = btree.btree_find_insert_index(page.data, header, 10)
	testing.expect(t, index == 5, "Should insert at end")
}
