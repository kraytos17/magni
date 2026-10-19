// Int secondary-index tree tests: online inserts (with splits), exact
// finds, deletes, and COW isolation over int-kind pages. Mirrors the text
// e2e shape; int-specific coverage (negative keys, dup values, rowid
// tiebreak) lives here because text oracles cannot express it.
package tests

import "core:testing"
import "src:btree"
import "src:types"

// setup_int_tree opens a scratch tree with an empty int leaf root.
setup_int_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	return setup_tree_with(t, name, "test_int_", "int", btree.init_int_leaf_page)
}

@(test)
test_int_tree_e2e :: proc(t: ^testing.T) {
	// 600 inserts through int_insert_cow across negatives, zero, positives
	// and duplicate values: every key finds its exact rowids, count exact.
	// Exercises leaf inserts, splits, absorbs, and root growth end to end.
	ctx := setup_int_tree(t, "e2e")
	defer teardown_tree(&ctx)

	N := 600
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		v := i64(i - 250)
		new_root, ins_err := btree.int_insert_cow(&ctx.tree, v, types.Row_ID(i64(i)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	// Duplicate values under fresh rowids share the key.
	for i in 1 ..= 10 {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.int_insert_cow(&ctx.tree, 42, types.Row_ID(i64(1000 + i)))
		testing.expect(t, ins_err == .None, "dup insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N + 10)

	for i in 1 ..= N {
		free_all(context.temp_allocator)
		v := i64(i - 250)
		if v == 42 {
			continue // eleven rowids; asserted below
		}
		found, f_err := btree.int_find_rowids(&ctx.tree, ctx.tree.root, v)
		testing.expect(t, f_err == .None, "find succeeds")
		if f_err != .None {
			continue
		}
		testing.expect(t, len(found) == 1, "unique key finds one rowid")
		if len(found) == 1 {
			testing.expect_value(t, found[0], types.Row_ID(i64(i)))
		}
	}

	dups, d_err := btree.int_find_rowids(&ctx.tree, ctx.tree.root, 42)
	testing.expect(t, d_err == .None, "dup find succeeds")
	if d_err == .None {
		testing.expect_value(t, len(dups), 11)
	}

	miss, m_err := btree.int_find_rowids(&ctx.tree, ctx.tree.root, 99999)
	testing.expect(t, m_err == .None, "miss succeeds")
	if m_err == .None {
		testing.expect_value(t, len(miss), 0)
	}
	free_all(context.temp_allocator)
}

@(test)
test_int_tree_delete :: proc(t: ^testing.T) {
	// Deletes remove exactly the (value,rowid) pair: neighbors with the
	// same value survive, missing pairs report Cell_Not_Found, and the
	// count is exact.
	ctx := setup_int_tree(t, "del")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.int_insert_cow(&ctx.tree, 7, types.Row_ID(i64(i)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	testing.expect(
		t,
		btree.int_delete(&ctx.tree, 7, 3) == .None,
		"present pair deletes",
	)
	testing.expect(
		t,
		btree.int_delete(&ctx.tree, 7, 3) == .Cell_Not_Found,
		"repeat delete misses",
	)
	testing.expect(
		t,
		btree.int_delete(&ctx.tree, 8, 1) == .Cell_Not_Found,
		"wrong value misses",
	)

	found, f_err := btree.int_find_rowids(&ctx.tree, ctx.tree.root, 7)
	testing.expect(t, f_err == .None, "find succeeds")
	if f_err == .None {
		testing.expect_value(t, len(found), 4)
	}

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, 4)
	free_all(context.temp_allocator)
}

@(test)
test_int_tree_cow_isolation :: proc(t: ^testing.T) {
	// COW inserts publish new roots while the old root still answers:
	// snapshot reads see pre-insert state.
	ctx := setup_int_tree(t, "cow")
	defer teardown_tree(&ctx)

	old_root := ctx.tree.root
	new_root, ins_err := btree.int_insert_cow(&ctx.tree, -5, types.Row_ID(1))
	testing.expect(t, ins_err == .None, "insert succeeds")
	if ins_err != .None {
		return
	}

	old_found, o_err := btree.int_find_rowids(&ctx.tree, old_root, -5)
	testing.expect(t, o_err == .None, "old root reads")
	if o_err == .None {
		testing.expect_value(t, len(old_found), 0)
	}

	new_found, n_err := btree.int_find_rowids(&ctx.tree, new_root, -5)
	testing.expect(t, n_err == .None, "new root reads")
	if n_err == .None {
		testing.expect_value(t, len(new_found), 1)
	}
	free_all(context.temp_allocator)
}
