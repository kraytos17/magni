package tests

import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

Test_Context :: struct {
	pager   : ^pager.Pager,
	tree    : btree.Tree,
	filename: string,
}

// setup_tree_with opens a pager, allocates + inits page 1 as a btree root,
// and returns the test context. Shared by setup_tree (slotdir) and
// setup_text_tree (text leaf): only the filename prefix, the root-page
// initializer, and the FATAL page label differ.
setup_tree_with :: proc(
	t: ^testing.T,
	name: string,
	file_prefix: string,
	page_kind: string,
	init_root: proc "contextless" (data: []u8, page_id: u32) -> bool,
) -> Test_Context {
	context.logger.lowest_level = .Error
	// Owned (heap) filename: tests free_all(temp) mid-run, so a temp string
	// stashed in ctx would dangle by teardown (this left test_text_*.db
	// behind). teardown_tree deletes it — same contract as
	// setup_schema_env / setup_executor_env.
	filename, _ := strings.clone(fmt.tprintf("%s%s.db", file_prefix, name), context.allocator)
	if os.exists(filename) {
		os.remove(filename)
	}

	p, err := pager.open(filename)
	if err != nil {
		testing.fail_now(t, fmt.tprintf("FATAL: Failed to open pager for %s", name))
	}

	pg1, alloc_err := pager.allocate_page(p)
	if alloc_err != nil {
		_ = pager.close(p)
		testing.fail_now(t, "FATAL: Failed to allocate root page 0")
	}
	if pg1.page_num != 1 {
		_ = pager.close(p)
		testing.fail_now(t, fmt.tprintf("FATAL: Allocated page was %d, expected 1", pg1.page_num))
	}
	if !init_root(pg1.data, pg1.page_num) {
		_ = pager.close(p)
		testing.fail_now(t, fmt.tprintf("FATAL: Failed to init %s root page", page_kind))
	}

	tree_inst := btree.init(p, 1)
	return Test_Context{pager = p, tree = tree_inst, filename = filename}
}

setup_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	return setup_tree_with(t, name, "test_", "slotdir", btree.init_slot_leaf_page)
}

teardown_tree :: proc(ctx: ^Test_Context) {
	if ctx.pager != nil {
		_ = pager.close(ctx.pager)
	}
	if os.exists(ctx.filename) {
		os.remove(ctx.filename)
	}
	wal_name := fmt.tprintf("%s-wal", ctx.filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
	delete(ctx.filename)
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
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "basic_ops")
	defer teardown_tree(&ctx)

	val1 := []types.Value{types.value(100), types.value("Row One")}
	err := btree.tree_insert(&ctx.tree, 1, val1)
	testing.expect_value(t, err, btree.Error.None)

	val2 := []types.Value{types.value(200), types.value("Row Two")}
	err = btree.tree_insert(&ctx.tree, 2, val2)
	testing.expect_value(t, err, btree.Error.None)

	c, find_err := btree.tree_find(&ctx.tree, 1, context.temp_allocator)
	defer cell.destroy(&c, context.temp_allocator)

	testing.expect_value(t, find_err, btree.Error.None)
	if find_err == .None {
		testing.expect_value(t, c.rowid, 1)
		val := c.values[0].(i64)
		testing.expect_value(t, val, 100)
	}

	_, missing_err := btree.tree_find(&ctx.tree, 99, context.temp_allocator)
	testing.expect_value(t, missing_err, btree.Error.Cell_Not_Found)

	count, count_err := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, count_err, btree.Error.None)
	testing.expect_value(t, count, 2)
}

@(test)
test_persistence :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "persistence")
	vals := []types.Value{types.value(999)}
	testing.expect(t, btree.tree_insert(&ctx.tree, 42, vals) == .None, "seed insert succeeds")

	_ = pager.close(ctx.pager)
	ctx.pager = nil
	p2, err := pager.open(ctx.filename)
	if !testing.expect(t, err == nil, "Re-open of DB file failed") {
		testing.fail_now(t, "Aborting persistence test due to file open failure")
	}

	ctx.pager = p2
	defer teardown_tree(&ctx)

	tree2 := btree.init(p2, 1)
	c, find_err := btree.tree_find(&tree2, 42, context.temp_allocator)
	defer cell.destroy(&c, context.temp_allocator)

	testing.expect_value(t, find_err, btree.Error.None)
	if find_err == .None {
		val := c.values[0].(i64)
		testing.expect_value(t, val, 999)
	}
}

@(test)
test_heavy_split_logic :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "splits")
	defer teardown_tree(&ctx)

	payload := make_large_text(context.temp_allocator, 100)
	item_count := 200
	for i in 1 ..= item_count {
		vals := []types.Value{types.value(i64(i)), types.value(payload)}
		err := btree.tree_insert(&ctx.tree, types.Row_ID(i), vals)
		if err != .None {
			testing.fail_now(t, fmt.tprintf("Insert failed at index %d with error: %v", i, err))
		}
	}

	is_valid := btree.tree_verify(&ctx.tree)
	if !is_valid {
		testing.fail_now(t, "Tree verification failed (Nodes disordered or keys out of bounds)")
	}

	c_start, _ := btree.tree_find(&ctx.tree, 1, context.temp_allocator)
	defer cell.destroy(&c_start, context.temp_allocator)
	testing.expect_value(t, c_start.values[0].(i64), 1)

	c_end, _ := btree.tree_find(&ctx.tree, types.Row_ID(item_count), context.temp_allocator)
	defer cell.destroy(&c_end, context.temp_allocator)
	testing.expect_value(t, c_end.values[0].(i64), i64(item_count))

	c_mid, _ := btree.tree_find(&ctx.tree, types.Row_ID(item_count / 2), context.temp_allocator)
	defer cell.destroy(&c_mid, context.temp_allocator)
	testing.expect_value(t, c_mid.values[0].(i64), i64(item_count / 2))
}

@(test)
test_duplicates :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "duplicates")
	defer teardown_tree(&ctx)

	vals := []types.Value{types.value(1)}
	testing.expect(t, btree.tree_insert(&ctx.tree, 10, vals) == .None, "first insert succeeds")
	err := btree.tree_insert(&ctx.tree, 10, vals)
	testing.expect_value(t, err, btree.Error.Duplicate_Rowid)

	ctx.tree.config.check_duplicates = false
	err_unsafe := btree.tree_insert(&ctx.tree, 10, vals)
	testing.expect_value(t, err_unsafe, btree.Error.None)
}

@(test)
test_cursor :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "cursor")
	defer teardown_tree(&ctx)

	keys := []types.Row_ID{50, 10, 30, 40, 20}
	for k in keys {
		vals := []types.Value{types.value(i64(k))}
		testing.expect(t, btree.tree_insert(&ctx.tree, k, vals) == .None, "seed insert succeeds")
	}

	cursor, err := btree.cursor_start(&ctx.tree)
	if !testing.expect_value(t, err, btree.Error.None) {
		testing.fail_now(t, "Could not start cursor")
	}
	defer btree.cursor_destroy(&cursor)

	expected := []i64{10, 20, 30, 40, 50}
	idx := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if !testing.expect_value(t, get_err, btree.Error.None) {
			break
		}
		if idx >= len(expected) {
			testing.fail_now(t, "Cursor returned more items than expected")
		}

		val := c.values[0].(i64)
		if val != expected[idx] {
			testing.expect(
				t,
				false,
				fmt.tprintf("Index %d: Expected %d, Got %d", idx, expected[idx], val),
			)
		}

		cell.destroy(&c, context.temp_allocator)
		btree.cursor_advance(&cursor)
		idx += 1
	}
	testing.expect_value(t, idx, 5)
}

@(test)
test_deletion :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "deletion")
	defer teardown_tree(&ctx)

	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 1, []types.Value{types.value(1)}) == .None,
		"seed insert succeeds",
	)
	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 2, []types.Value{types.value(2)}) == .None,
		"seed insert succeeds",
	)
	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 3, []types.Value{types.value(3)}) == .None,
		"seed insert succeeds",
	)

	err := btree.tree_delete(&ctx.tree, 2)
	testing.expect_value(t, err, btree.Error.None)

	_, find_err := btree.tree_find(&ctx.tree, 2, context.temp_allocator)
	testing.expect_value(t, find_err, btree.Error.Cell_Not_Found)

	c1, _ := btree.tree_find(&ctx.tree, 1, context.temp_allocator)
	defer cell.destroy(&c1, context.temp_allocator)

	c3, _ := btree.tree_find(&ctx.tree, 3, context.temp_allocator)
	defer cell.destroy(&c3, context.temp_allocator)

	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 2)
}

@(test)
test_auto_increment :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "autoincrement")
	defer teardown_tree(&ctx)

	next, err := btree.tree_next_rowid(&ctx.tree)
	testing.expect_value(t, err, btree.Error.None)
	testing.expect_value(t, next, 1)

	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 10, []types.Value{}) == .None,
		"seed insert succeeds",
	)

	next2, _ := btree.tree_next_rowid(&ctx.tree)
	testing.expect_value(t, next2, 11)
}

@(test)
test_tree_verify :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "verify")
	defer teardown_tree(&ctx)

	items := 50
	for i in 1 ..= items {
		testing.expect(
			t,
			btree.tree_insert(
				&ctx.tree,
				types.Row_ID(i),
				[]types.Value{types.value(i64(i))},
			) ==
			.None,
			"seed insert succeeds",
		)
	}
	testing.expect(t, btree.tree_verify(&ctx.tree), "Tree verification failed after inserts")

	testing.expect(t, btree.tree_delete(&ctx.tree, 25) == .None, "delete existing succeeds")
	testing.expect(t, btree.tree_verify(&ctx.tree), "Tree verification failed after delete")
}

@(test)
test_delete_non_existent :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "del_miss")
	defer teardown_tree(&ctx)

	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 10, []types.Value{types.value(10)}) == .None,
		"seed insert succeeds",
	)
	err := btree.tree_delete(&ctx.tree, 99)
	testing.expect_value(t, err, btree.Error.Cell_Not_Found)
	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 1)
}

@(test)
test_delete_first_last_key :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "del_first_last")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		testing.expect(
			t,
			btree.tree_insert(
				&ctx.tree,
				types.Row_ID(i),
				[]types.Value{types.value(i64(i))},
			) ==
			.None,
			"seed insert succeeds",
		)
	}

	err := btree.tree_delete(&ctx.tree, 1)
	testing.expect_value(t, err, btree.Error.None)
	_, find_err := btree.tree_find(&ctx.tree, 1, context.temp_allocator)
	testing.expect_value(t, find_err, btree.Error.Cell_Not_Found)
	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 4)

	err = btree.tree_delete(&ctx.tree, 5)
	testing.expect_value(t, err, btree.Error.None)
	_, find_err = btree.tree_find(&ctx.tree, 5, context.temp_allocator)
	testing.expect_value(t, find_err, btree.Error.Cell_Not_Found)
	cnt, _ = btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 3)

	testing.expect(t, btree.tree_verify(&ctx.tree), "Tree verify failed after first/last delete")
}

@(test)
test_consecutive_deletes :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "consec_del")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		testing.expect(
			t,
			btree.tree_insert(
				&ctx.tree,
				types.Row_ID(i),
				[]types.Value{types.value(i64(i))},
			) ==
			.None,
			"seed insert succeeds",
		)
	}

	for i in 1 ..= 5 {
		err := btree.tree_delete(&ctx.tree, types.Row_ID(i))
		testing.expect_value(t, err, btree.Error.None)
	}

	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 0)
	testing.expect(t, btree.tree_verify(&ctx.tree), "Tree verify failed after all deletes")
}

@(test)
test_reinsert_after_delete :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "reinsert")
	defer teardown_tree(&ctx)

	testing.expect(
		t,
		btree.tree_insert(&ctx.tree, 10, []types.Value{types.value(1)}) == .None,
		"seed insert succeeds",
	)
	testing.expect(t, btree.tree_delete(&ctx.tree, 10) == .None, "delete existing succeeds")

	vals := []types.Value{types.value(999)}
	err := btree.tree_insert(&ctx.tree, 10, vals)
	testing.expect_value(t, err, btree.Error.None)

	c, _ := btree.tree_find(&ctx.tree, 10, context.temp_allocator)
	testing.expect_value(t, c.values[0].(i64), 999)
	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 1)
}

@(test)
test_empty_tree_cursor :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "empty_cursor")
	defer teardown_tree(&ctx)

	cursor, err := btree.cursor_start(&ctx.tree)
	testing.expect_value(t, err, btree.Error.None)
	defer btree.cursor_destroy(&cursor)

	testing.expect(t, !cursor.is_valid, "Cursor on empty tree should be invalid")
}

@(test)
test_max_fit_value :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "max_fit")
	defer teardown_tree(&ctx)

	payload := make_large_text(context.temp_allocator, 3000)
	vals := []types.Value{types.value(1), types.value(payload)}
	err := btree.tree_insert(&ctx.tree, 1, vals)
	testing.expect_value(t, err, btree.Error.None)

	c, _ := btree.tree_find(&ctx.tree, 1, context.temp_allocator)
	testing.expect_value(t, c.values[1].(string), payload)
	testing.expect(t, btree.tree_verify(&ctx.tree), "Tree verify failed after large insert")
}

@(test)
test_overflow_value :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "overflow")
	defer teardown_tree(&ctx)

	payload := make_large_text(context.temp_allocator, 4096)
	vals := []types.Value{types.value(1), types.value(payload)}
	err := btree.tree_insert(&ctx.tree, 1, vals)
	testing.expect(t, err != .None, "Oversized value should have failed")
}

@(test)
test_load_node_page_zero :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "page0_tree")
	defer teardown_tree(&ctx)

	bad_tree := btree.init(ctx.tree.pager, 0)
	_, err := btree.tree_find(&bad_tree, 1, context.temp_allocator)
	testing.expect(t, err != .None, "tree_find on page 0 root should fail")
}

@(test)
test_page_one_header_offset :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	offset_1 := btree.get_page_header_offset(1)
	offset_2 := btree.get_page_header_offset(2)
	offset_3 := btree.get_page_header_offset(3)
	testing.expect_value(t, offset_1, 100)
	testing.expect_value(t, offset_2, 0)
	testing.expect_value(t, offset_3, 0)
}

@(test)
test_tree_update_cow :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "tree_update_cow")
	defer teardown_tree(&ctx)

	vals := []types.Value{types.value("initial")}
	root, ins_err := btree.tree_insert_cow(&ctx.tree, 10, vals)
	testing.expect(t, ins_err == .None, "cow insert 10")
	ctx.tree.root = root

	new_vals := []types.Value{types.value("updated")}
	root2, upd_err := btree.tree_update_cow(&ctx.tree, 10, new_vals)
	testing.expect(t, upd_err == .None, "cow update 10")
	ctx.tree.root = root2

	c, err := btree.tree_find(&ctx.tree, 10, context.temp_allocator)
	testing.expect(t, err == .None, "find updated 10 via cow")
	val, ok := c.values[0].(string)
	testing.expect(t, ok && val == "updated", "cow update value")
}

@(test)
test_find_on_empty_tree :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "empty_find")
	defer teardown_tree(&ctx)

	_, err := btree.tree_find(&ctx.tree, 999, context.temp_allocator)
	testing.expect(t, err == .Cell_Not_Found, "find on empty tree returns Cell_Not_Found")
}

@(test)
test_delete_on_empty_tree :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "empty_del")
	defer teardown_tree(&ctx)

	testing.expect(
		t,
		btree.tree_delete(&ctx.tree, 999) == .Cell_Not_Found,
		"delete on empty tree fails loudly",
	)
	_, err := btree.tree_find(&ctx.tree, 999, context.temp_allocator)
	testing.expect(t, err == .Cell_Not_Found, "still empty after delete on empty tree")
}

@(test)
test_tree_collect_pages :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "collect_pg")
	defer teardown_tree(&ctx)

	val := []types.Value{types.value(10)}
	testing.expect(t, btree.tree_insert(&ctx.tree, 1, val) == .None, "seed insert succeeds")
	testing.expect(t, btree.tree_insert(&ctx.tree, 2, val) == .None, "seed insert succeeds")
	testing.expect(t, btree.tree_insert(&ctx.tree, 3, val) == .None, "seed insert succeeds")

	pages := make(map[u32]bool, context.temp_allocator)
	btree.collect_pages(&ctx.tree, ctx.tree.root, &pages)
	testing.expect(t, len(pages) >= 1, "at least one page collected")
	found_root := false
	for p in pages {
		if p == ctx.tree.root {
			found_root = true
		}
	}
	testing.expect(t, found_root, "root page in collected pages")
}

@(test)
test_foreach_callback :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "foreach")
	defer teardown_tree(&ctx)

	val := []types.Value{types.value(100)}
	testing.expect(t, btree.tree_insert(&ctx.tree, 1, val) == .None, "seed insert succeeds")
	testing.expect(t, btree.tree_insert(&ctx.tree, 2, val) == .None, "seed insert succeeds")

	count := 0
	testing.expect(
		t,
		btree.tree_foreach(&ctx.tree, proc(c: ^cell.Cell, user_data: rawptr) -> bool {
				(^int)(user_data)^ += 1
				return true
			}, &count) == .None,
		"foreach over 2 rows succeeds",
	)
	testing.expect_value(t, count, 2)
}


@(test)
test_page_accessor_move :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	p, err := pager.open("test_accessor_move.db", 8)
	defer pager.close(p)
	os.remove("test_accessor_move.db"); os.remove("test_accessor_move.db-wal")
	if err != nil {
		testing.fail_now(t, "open failed")
	}

	src_pg, a_err := pager.allocate_page(p)
	if a_err != nil {
		testing.fail_now(t, "alloc failed")
	}
	defer pager.unpin_page(p, src_pg.page_num)

	dst_pg, a2_err := pager.allocate_page(p)
	if a2_err != nil {
		testing.fail_now(t, "alloc dst failed")
	}
	defer pager.unpin_page(p, dst_pg.page_num)

	// Slotdir pages post-flip (move_cells_to is stride-generic; the 10-byte
	// stride is unchanged, only the entry view differs).
	if !btree.init_slot_leaf_page(src_pg.data, src_pg.page_num) {
		testing.fail_now(t, "alloc src failed")
	}
	if !btree.init_slot_leaf_page(dst_pg.data, dst_pg.page_num) {
		testing.fail_now(t, "alloc dst failed")
	}

	// Create 2 entries in src
	vals := [][]types.Value{{types.value(1)}, {types.value(2)}}
	rids := []types.Row_ID{10, 20}
	off := int(types.PAGE_SIZE)
	for i in 0 ..< 2 {
		info := cell.compute_info(rids[i], vals[i])
		off -= info.total_size
		_, ser_ok := cell.serialize(src_pg.data[off:], rids[i], vals[i], info)
		testing.expect(t, ser_ok, "serialize src cell succeeds")
	}

	src_hdr := btree.get_leaf_header(src_pg.data, src_pg.page_num)
	src_base := btree.get_page_header_offset(src_pg.page_num)
	src_hdr_sz := size_of(btree.Leaf_Header)
	src_hdr.cell_count = 2
	src_hdr.cell_content_offset = u16le(off)

	src_ptr_offsets := []u16{u16(off), u16(off + cell.compute_info(rids[0], vals[0]).total_size)}
	for i in 0 ..< 2 {
		loc := src_base + src_hdr_sz + i * size_of(btree.Slot)
		(^btree.Slot)(raw_data(src_pg.data[loc:]))^ = btree.Slot {
			rowid = u64le(btree.rowid_bias_encode(rids[i])),
			off   = u16le(src_ptr_offsets[i]),
		}
	}

	// Move 1 entry from src[1] to dst
	btree.move_cells_to(
		dst_pg.data,
		dst_pg.page_num,
		src_pg.data,
		src_pg.page_num,
		1,
		1,
		size_of(btree.Slot),
	)

	dst_hdr := btree.get_leaf_header(dst_pg.data, dst_pg.page_num)
	dst_hdr.cell_count = 1
	// Verify dst has entry with key=20
	testing.expect_value(t, btree.get_cell_count(dst_pg.data, dst_pg.page_num), 1)
	dst_layout, _, dst_err := btree.layout_for_page(dst_pg.data, btree.Page_Id(dst_pg.page_num))
	testing.expect(t, dst_err == .None, "resolve dst layout")
	k, k_err := dst_layout.vtable.key_at(dst_pg.data, btree.Page_Id(dst_pg.page_num), 0)
	testing.expect(t, k_err == .None, "dst key_at succeeds")
	testing.expect_value(t, k, types.Row_ID(20))
	// entry_area_end stays covered (moved here from the retired conformance
	// test): the entry area of a 1-slot page is positive.
	eae := btree.entry_area_end(dst_pg.data, dst_pg.page_num, size_of(btree.Slot))
	testing.expect(t, eae > 0, "entry_area_end > 0")
}
