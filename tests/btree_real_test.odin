// Real secondary-index tree tests: online inserts (with splits), exact
// finds, deletes, range scans, and -0.0 normalization over real-kind pages.
// Mirrors the int e2e shape; real-specific coverage (fractional keys,
// signed zeros sharing one key, infinities) lives here because int oracles
// cannot express it.
package tests

import "core:testing"
import "src:btree"
import "src:types"

// setup_real_tree opens a scratch tree with an empty real leaf root.
setup_real_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	return setup_tree_with(t, name, "test_real_", "real", btree.init_real_leaf_page)
}

@(test)
test_real_tree_e2e :: proc(t: ^testing.T) {
	// 600 inserts through real_insert_cow across negatives, zero, positives
	// and duplicate values: every key finds its exact rowids, count exact.
	// Exercises leaf inserts, splits, absorbs, and root growth end to end.
	ctx := setup_real_tree(t, "e2e")
	defer teardown_tree(&ctx)

	N := 600
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		v := f64(i-250) / 4.0
		new_root, ins_err := btree.real_insert_cow(&ctx.tree, v, types.Row_ID(i64(i)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	// Duplicate values under fresh rowids share the key.
	for i in 1 ..= 10 {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.real_insert_cow(&ctx.tree, 42.5, types.Row_ID(i64(1000 + i)))
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
		v := f64(i-250) / 4.0
		if v == 42.5 {
			continue // eleven rowids; asserted below
		}
		found, f_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, v)
		testing.expect(t, f_err == .None, "find succeeds")
		if f_err != .None {
			continue
		}
		testing.expect(t, len(found) == 1, "unique key finds one rowid")
		if len(found) == 1 {
			testing.expect_value(t, found[0], types.Row_ID(i64(i)))
		}
	}

	dups, d_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, 42.5)
	testing.expect(t, d_err == .None, "dup find succeeds")
	if d_err == .None {
		testing.expect_value(t, len(dups), 11)
	}

	miss, m_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, 99999.25)
	testing.expect(t, m_err == .None, "miss succeeds")
	if m_err == .None {
		testing.expect_value(t, len(miss), 0)
	}
	free_all(context.temp_allocator)
}

@(test)
test_real_tree_signed_zero_and_inf :: proc(t: ^testing.T) {
	// -0.0 and +0.0 share one key (normalization); infinities sort at the
	// extremes; deletes match across the zero sign.
	ctx := setup_real_tree(t, "zeroinf")
	defer teardown_tree(&ctx)

	neg_zero := -0.0
	pos_inf := transmute(f64)u64(0x7ff0000000000000)
	neg_inf := transmute(f64)u64(0xfff0000000000000)
	vals := []f64{neg_zero, 0, 1.5, pos_inf, neg_inf, -1.5}
	for v, i in vals {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.real_insert_cow(&ctx.tree, v, types.Row_ID(i64(i + 1)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	zeros, z_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, 0)
	testing.expect(t, z_err == .None, "zero find succeeds")
	if z_err == .None {
		testing.expect_value(t, len(zeros), 2)
	}
	nzeros, n_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, neg_zero)
	testing.expect(t, n_err == .None, "neg-zero find succeeds")
	if n_err == .None {
		testing.expect_value(t, len(nzeros), 2)
	}

	testing.expect(t, btree.real_delete(&ctx.tree, neg_zero, 1) == .None, "neg-zero deletes pos-zero entry")
	gone, g_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, 0)
	testing.expect(t, g_err == .None, "find succeeds")
	if g_err == .None {
		testing.expect_value(t, len(gone), 1)
	}

	lo, l_err := btree.real_range_rowids(&ctx.tree, ctx.tree.root, neg_inf, pos_inf, true, true, true, true)
	testing.expect(t, l_err == .None, "full range succeeds")
	if l_err == .None {
		testing.expect_value(t, len(lo), 5)
		// Index order: neg_inf, -1.5, 0, 1.5, pos_inf → rowids 5, 6, 2, 3, 4.
		want := []types.Row_ID{5, 6, 2, 3, 4}
		for r, i in lo {
			if i < len(want) {
				testing.expect_value(t, r, want[i])
			}
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_real_tree_delete :: proc(t: ^testing.T) {
	// Deletes remove exactly the (value,rowid) pair: neighbors with the
	// same value survive, missing pairs report Cell_Not_Found, and the
	// count is exact.
	ctx := setup_real_tree(t, "del")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.real_insert_cow(&ctx.tree, 7.5, types.Row_ID(i64(i)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	testing.expect(t, btree.real_delete(&ctx.tree, 7.5, 3) == .None, "present pair deletes")
	testing.expect(t, btree.real_delete(&ctx.tree, 7.5, 3) == .Cell_Not_Found, "repeat delete misses")
	testing.expect(t, btree.real_delete(&ctx.tree, 8.5, 1) == .Cell_Not_Found, "wrong value misses")

	found, f_err := btree.real_find_rowids(&ctx.tree, ctx.tree.root, 7.5)
	testing.expect(t, f_err == .None, "find succeeds")
	if f_err == .None {
		testing.expect_value(t, len(found), 4)
	}

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, 4)
	free_all(context.temp_allocator)
}
