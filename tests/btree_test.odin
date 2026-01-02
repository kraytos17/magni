package tests

import "core:fmt"
import "core:mem"
import os "core:os/os2"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

Test_Context :: struct {
	pager:    ^pager.Pager,
	tree:     btree.Tree,
	filename: string,
}

setup_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	filename := fmt.tprintf("test_%s.db", name)
	if os.exists(filename) {
		os.remove(filename)
	}

	p, err := pager.open(filename)
	if err != nil {
		testing.fail_now(t, fmt.tprintf("FATAL: Failed to open pager for %s", name))
	}

	pg0, alloc_err := pager.allocate_page(p)
	if alloc_err != nil {
		pager.close(p)
		testing.fail_now(t, "FATAL: Failed to allocate root page 0")
	}
	if pg0.page_num != 0 {
		pager.close(p)
		testing.fail_now(t, fmt.tprintf("FATAL: Allocated page was %d, expected 0", pg0.page_num))
	}

	btree.init_leaf_page(pg0.data, pg0.page_num)
	tree_inst := btree.init(p, 0)
	return Test_Context{pager = p, tree = tree_inst, filename = filename}
}

teardown_tree :: proc(ctx: ^Test_Context) {
	if ctx.pager != nil {
		pager.close(ctx.pager)
	}
	if os.exists(ctx.filename) {
		os.remove(ctx.filename)
	}
}

make_large_text :: proc(allocator: mem.Allocator, size: int) -> string {
	data := make([]u8, size, allocator)
	for i in 0 ..< size {
		data[i] = 'A' + u8(i % 26)
	}
	return string(data)
}

@(test)
test_basic_operations :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "basic_ops")
	defer teardown_tree(&ctx)

	val1 := []types.Value{types.value_int(100), types.value_text("Row One")}
	err := btree.tree_insert(&ctx.tree, 1, val1)
	testing.expect_value(t, err, btree.Error.None)

	val2 := []types.Value{types.value_int(200), types.value_text("Row Two")}
	err = btree.tree_insert(&ctx.tree, 2, val2)
	testing.expect_value(t, err, btree.Error.None)

	c, find_err := btree.tree_find(&ctx.tree, 1)
	defer cell.destroy(&c)

	testing.expect_value(t, find_err, btree.Error.None)
	if find_err == .None {
		testing.expect_value(t, c.rowid, 1)
		val := c.values[0].(i64)
		testing.expect_value(t, val, 100)
	}

	_, missing_err := btree.tree_find(&ctx.tree, 99)
	testing.expect_value(t, missing_err, btree.Error.Cell_Not_Found)

	count, count_err := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, count_err, btree.Error.None)
	testing.expect_value(t, count, 2)
}

@(test)
test_persistence :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "persistence")
	vals := []types.Value{types.value_int(999)}
	btree.tree_insert(&ctx.tree, 42, vals)

	pager.close(ctx.pager)
	ctx.pager = nil
	p2, err := pager.open(ctx.filename)
	if !testing.expect(t, err == nil, "Re-open of DB file failed") {
		testing.fail_now(t, "Aborting persistence test due to file open failure")
	}

	ctx.pager = p2
	defer teardown_tree(&ctx)

	tree2 := btree.init(p2, 0)
	c, find_err := btree.tree_find(&tree2, 42)
	defer cell.destroy(&c)

	testing.expect_value(t, find_err, btree.Error.None)
	if find_err == .None {
		val := c.values[0].(i64)
		testing.expect_value(t, val, 999)
	}
}

@(test)
test_heavy_split_logic :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "splits")
	defer teardown_tree(&ctx)

	payload := make_large_text(context.temp_allocator, 100)
	item_count := 200
	for i in 1 ..= item_count {
		vals := []types.Value{types.value_int(i64(i)), types.value_text(payload)}
		err := btree.tree_insert(&ctx.tree, types.Row_ID(i), vals)
		if err != .None {
			testing.fail_now(t, fmt.tprintf("Insert failed at index %d with error: %v", i, err))
		}
	}

	is_valid := btree.tree_verify(&ctx.tree)
	if !is_valid {
		testing.fail_now(t, "Tree verification failed (Nodes disordered or keys out of bounds)")
	}

	c_start, _ := btree.tree_find(&ctx.tree, 1)
	defer cell.destroy(&c_start)
	testing.expect_value(t, c_start.values[0].(i64), 1)

	c_end, _ := btree.tree_find(&ctx.tree, types.Row_ID(item_count))
	defer cell.destroy(&c_end)
	testing.expect_value(t, c_end.values[0].(i64), i64(item_count))

	c_mid, _ := btree.tree_find(&ctx.tree, types.Row_ID(item_count / 2))
	defer cell.destroy(&c_mid)
	testing.expect_value(t, c_mid.values[0].(i64), i64(item_count / 2))
}

@(test)
test_duplicates :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "duplicates")
	defer teardown_tree(&ctx)

	vals := []types.Value{types.value_int(1)}
	btree.tree_insert(&ctx.tree, 10, vals)
	err := btree.tree_insert(&ctx.tree, 10, vals)
	testing.expect_value(t, err, btree.Error.Duplicate_Rowid)

	ctx.tree.config.check_duplicates = false
	err_unsafe := btree.tree_insert(&ctx.tree, 10, vals)
	testing.expect_value(t, err_unsafe, btree.Error.None)
}

@(test)
test_cursor :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "cursor")
	defer teardown_tree(&ctx)

	keys := []types.Row_ID{50, 10, 30, 40, 20}
	for k in keys {
		vals := []types.Value{types.value_int(i64(k))}
		btree.tree_insert(&ctx.tree, k, vals)
	}

	cursor, err := btree.cursor_start(&ctx.tree)
	if !testing.expect_value(t, err, btree.Error.None) {
		testing.fail_now(t, "Could not start cursor")
	}
	defer btree.cursor_destroy(&cursor)

	expected := []i64{10, 20, 30, 40, 50}
	idx := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor)
		if !testing.expect_value(t, get_err, btree.Error.None) {
			break
		}
		if idx >= len(expected) {
			testing.fail_now(t, "Cursor returned more items than expected")
		}

		val := c.values[0].(i64)
		if val != expected[idx] {
			testing.expect(t, false, fmt.tprintf("Index %d: Expected %d, Got %d", idx, expected[idx], val))
		}

		cell.destroy(&c)
		btree.cursor_advance(&cursor)
		idx += 1
	}
	testing.expect_value(t, idx, 5)
}

@(test)
test_deletion :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "deletion")
	defer teardown_tree(&ctx)

	btree.tree_insert(&ctx.tree, 1, []types.Value{types.value_int(1)})
	btree.tree_insert(&ctx.tree, 2, []types.Value{types.value_int(2)})
	btree.tree_insert(&ctx.tree, 3, []types.Value{types.value_int(3)})

	err := btree.tree_delete(&ctx.tree, 2)
	testing.expect_value(t, err, btree.Error.None)

	_, find_err := btree.tree_find(&ctx.tree, 2)
	testing.expect_value(t, find_err, btree.Error.Cell_Not_Found)

	c1, _ := btree.tree_find(&ctx.tree, 1)
	defer cell.destroy(&c1)

	c3, _ := btree.tree_find(&ctx.tree, 3)
	defer cell.destroy(&c3)

	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 2)
}

@(test)
test_auto_increment :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "autoincrement")
	defer teardown_tree(&ctx)

	next, err := btree.tree_next_rowid(&ctx.tree)
	testing.expect_value(t, err, btree.Error.None)
	testing.expect_value(t, next, 1)

	btree.tree_insert(&ctx.tree, 10, []types.Value{})

	next2, _ := btree.tree_next_rowid(&ctx.tree)
	testing.expect_value(t, next2, 11)
}
