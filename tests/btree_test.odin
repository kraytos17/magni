package tests

import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

create_test_pager :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string, u32) {
	temp_name := fmt.tprintf("test_btree_%s.db", test_name)
	filename, _ := strings.clone(temp_name, context.allocator)

	os.remove(filename)
	p, err := pager.open(filename)
	testing.expect(t, err == nil, "Failed to open pager")

	pager.allocate_page(p)
	page, alloc_err := pager.allocate_page(p)
	testing.expect(t, alloc_err == nil, "Failed to allocate page")

	init_err := btree.init_leaf_page(page.data)
	testing.expect(t, init_err == .None, "Failed to initialize leaf page")

	return p, filename, page.page_num
}

destroy_test_pager :: proc(p: ^pager.Pager, filename: string) {
	pager.close(p)
	os.remove(filename)
	delete(filename, context.allocator)
}

@(test)
test_init_leaf_page :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	page_data := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	err := btree.init_leaf_page(page_data)
	testing.expect(t, err == .None, "Failed to initialize leaf page")

	header := btree.get_header(page_data)
	testing.expect(t, header != nil, "Header should not be nil")
	testing.expect(t, header.page_type == .LEAF_TABLE, "Wrong page type")
	testing.expect(t, header.cell_count == 0, "Cell count should be 0")
	testing.expect(t, header.cell_content_offset == u16le(types.PAGE_SIZE), "Wrong content offset")
}

@(test)
test_insert_cell_single :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "insert_single")
	defer destroy_test_pager(p, file)

	values := []types.Value{types.value_int(42), types.value_text("Hello")}
	err := btree.insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .None, "Failed to insert cell")

	page, _ := pager.get_page(p, page_num)
	header := btree.get_header(page.data)
	testing.expect(t, header.cell_count == 1, "Cell count should be 1")
	testing.expect(
		t,
		header.cell_content_offset < u16le(types.PAGE_SIZE),
		"Content offset should have decreased",
	)
}

@(test)
test_insert_cell_multiple_ordered :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "insert_ordered")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 10))}
		err := btree.insert_cell(p, page_num, types.Row_ID(i), values)
		testing.expect(t, err == .None, fmt.tprintf("Failed to insert cell %d", i))
	}

	page, _ := pager.get_page(p, page_num)
	header := btree.get_header(page.data)
	testing.expect(t, header.cell_count == 5, "Should have 5 cells")
}

@(test)
test_insert_cell_unordered :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "insert_unordered")
	defer destroy_test_pager(p, file)

	rowids := []types.Row_ID{5, 2, 8, 1, 4}
	for rowid in rowids {
		values := []types.Value{types.value_int(i64(rowid) * 100)}
		err := btree.insert_cell(p, page_num, rowid, values)
		testing.expect(t, err == .None, fmt.tprintf("Failed to insert rowid %d", rowid))
	}

	page, _ := pager.get_page(p, page_num)
	pointers := btree.get_pointers(page.data)
	prev_rowid := types.Row_ID(0)
	for ptr in pointers {
		rowid, ok := cell.get_rowid(page.data, int(ptr))
		testing.expect(t, ok, "Failed to get rowid")
		testing.expect(
			t,
			rowid > prev_rowid,
			fmt.tprintf("Rowids not sorted! Got %d after %d", rowid, prev_rowid),
		)
		prev_rowid = rowid
	}
}

@(test)
test_insert_cell_duplicate :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "insert_dup")
	defer destroy_test_pager(p, file)

	values := []types.Value{types.value_int(42)}
	err := btree.insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .None, "Failed to insert first cell")

	err = btree.insert_cell(p, page_num, 1, values)
	testing.expect(t, err == .Duplicate_Rowid, "Should reject duplicate rowid")
}

@(test)
test_cursor_traversal :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "cursor")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 3 {
		values := []types.Value{types.value_int(i64(i))}
		btree.insert_cell(p, page_num, types.Row_ID(i), values)
	}

	cursor := btree.cursor_start(page_num)
	testing.expect(t, !cursor.end_of_table, "Should not be at end")

	ref1, err1 := btree.cursor_get_cell(p, &cursor)
	testing.expect(t, err1 == .None, "Failed to get cell 1")
	testing.expect(t, ref1.cell.rowid == 1, "RowID mismatch")

	btree.cell_ref_destroy(&ref1)
	btree.cursor_advance(p, &cursor)

	ref2, err2 := btree.cursor_get_cell(p, &cursor)
	testing.expect(t, err2 == .None, "Failed to get cell 2")
	testing.expect(t, ref2.cell.rowid == 2, "RowID mismatch")

	btree.cell_ref_destroy(&ref2)
	btree.cursor_advance(p, &cursor)

	ref3, err3 := btree.cursor_get_cell(p, &cursor)
	testing.expect(t, err3 == .None, "Failed to get cell 3")
	testing.expect(t, ref3.cell.rowid == 3, "RowID mismatch")

	btree.cell_ref_destroy(&ref3)
	btree.cursor_advance(p, &cursor)
	testing.expect(t, cursor.end_of_table, "Should be at end")
}

@(test)
test_find_by_rowid :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "find")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 10 {
		values := []types.Value{types.value_int(i64(i * 100))}
		btree.insert_cell(p, page_num, types.Row_ID(i), values)
	}

	target_rowid := types.Row_ID(7)
	cell_ref, err := btree.find_by_rowid(p, page_num, target_rowid)
	defer btree.cell_ref_destroy(&cell_ref)

	testing.expect(t, err == .None, "Failed to find cell")
	testing.expect(t, cell_ref.cell.rowid == target_rowid, "Found wrong rowid")

	val := cell_ref.cell.values[0].(i64)
	testing.expect(t, val == 700, "Value should be 700")
}

@(test)
test_find_by_rowid_not_found :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "find_fail")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i))}
		btree.insert_cell(p, page_num, types.Row_ID(i), values)
	}

	_, err := btree.find_by_rowid(p, page_num, 99)
	testing.expect(t, err == .Cell_Not_Found, "Should not find cell")
}

@(test)
test_delete_cell :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "delete")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 10))}
		btree.insert_cell(p, page_num, types.Row_ID(i), values)
	}

	err := btree.delete_cell(p, page_num, 3)
	testing.expect(t, err == .None, "Failed to delete cell")

	count, _ := btree.count_rows(p, page_num)
	testing.expect(t, count == 4, "Should have 4 rows after deletion")

	_, find_err := btree.find_by_rowid(p, page_num, 3)
	testing.expect(t, find_err == .Cell_Not_Found, "Deleted cell should not be found")

	ref, _ := btree.find_by_rowid(p, page_num, 4)
	defer btree.cell_ref_destroy(&ref)
	testing.expect(t, ref.cell.rowid == 4, "Row 4 should still exist")
}

@(test)
test_foreach_cell :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file, page_num := create_test_pager(t, "foreach")
	defer destroy_test_pager(p, file)

	for i in 1 ..= 5 {
		values := []types.Value{types.value_int(i64(i * 100))}
		btree.insert_cell(p, page_num, types.Row_ID(i), values)
	}

	Context :: struct {
		count: int,
		sum:   i64,
	}
	ctx := Context{0, 0}

	callback :: proc(c: ^cell.Cell, user_data: rawptr) -> bool {
		ctx := cast(^Context)user_data
		ctx.count += 1
		val := c.values[0].(i64)
		ctx.sum += val
		return true
	}

	err := btree.foreach_cell(p, page_num, callback, &ctx)
	testing.expect(t, err == .None, "Failed to iterate cells")
	testing.expect(t, ctx.count == 5, "Should iterate 5 cells")
	testing.expect(t, ctx.sum == 1500, "Sum should be 1500")
}
