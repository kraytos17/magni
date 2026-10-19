// Composite secondary-index tree tests: online inserts (with splits),
// exact finds, leading-prefix scans, and deletes over framed (INT, TEXT)
// keys. Mirrors the int/real e2e shape; composite-specific coverage
// (framing disambiguation, prefix runs straddling separators, mixed
// leading values) lives here because scalar oracles cannot express it.
package tests

import "core:testing"
import "src:btree"
import "src:cell"
import "src:types"

// setup_composite_tree opens a scratch tree with an empty composite leaf root.
setup_composite_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	return setup_tree_with(t, name, "test_comp_", "composite", btree.init_composite_leaf_page)
}

// comp_raw frames one (INT, TEXT) pair into a temp raw for the test.
comp_raw :: proc(a: i64, b: string) -> []u8 {
	vals := make([]types.Value, 2, context.temp_allocator)
	vals[0] = types.value(a)
	vals[1] = types.value(b)
	buf := make([]u8, 64, context.temp_allocator)
	n, ok := cell.composite_encode_raw(vals, {.INTEGER, .TEXT}, buf)
	assert(ok)
	return buf[:n]
}

// comp_lead frames one leading INT bound into a temp prefix raw.
comp_lead :: proc(a: i64) -> []u8 {
	vals := make([]types.Value, 1, context.temp_allocator)
	vals[0] = types.value(a)
	buf := make([]u8, 64, context.temp_allocator)
	n, ok := cell.composite_encode_raw(vals, {.INTEGER}, buf)
	assert(ok)
	return buf[:n]
}

@(test)
test_composite_tree_e2e :: proc(t: ^testing.T) {
	// 600 inserts through composite_insert_cow: every full key finds its
	// exact rowid, leading-prefix scans return whole groups, count exact.
	ctx := setup_composite_tree(t, "e2e")
	defer teardown_tree(&ctx)

	bodies := []string{"alpha", "bb", "gamma"}
	N := 600
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		raw := comp_raw(i64(i % 50 - 25), bodies[(i - 1) % len(bodies)])
		new_root, ins_err := btree.composite_insert_cow(&ctx.tree, raw, types.Row_ID(i64(i)))
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N)

	// Exact finds: each (a, body) pair repeats 4 times (12 values of i
	// per a spread evenly over 3 bodies).
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		raw := comp_raw(i64(i % 50 - 25), bodies[(i - 1) % len(bodies)])
		found, f_err := btree.composite_find_rowids(&ctx.tree, ctx.tree.root, raw)
		testing.expect(t, f_err == .None, "find succeeds")
		if f_err != .None {
			continue
		}
		testing.expect(t, len(found) == 4, "each pair repeats 4 times")
	}
	miss, m_err := btree.composite_find_rowids(&ctx.tree, ctx.tree.root, comp_raw(999, "zzz"))
	testing.expect(t, m_err == .None, "miss succeeds")
	if m_err == .None {
		testing.expect_value(t, len(miss), 0)
	}

	// Leading-prefix scan: first component 7 appears in 12 rows.
	seven, s_err := btree.composite_prefix_rowids(&ctx.tree, ctx.tree.root, comp_lead(7))
	testing.expect(t, s_err == .None, "prefix succeeds")
	if s_err == .None {
		testing.expect_value(t, len(seven), 12)
	}
	free_all(context.temp_allocator)
}

@(test)
test_composite_tree_delete :: proc(t: ^testing.T) {
	// Deletes remove exactly the (framed,rowid) pair; same-key neighbors
	// survive and missing pairs report Cell_Not_Found.
	ctx := setup_composite_tree(t, "del")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.composite_insert_cow(
			&ctx.tree,
			comp_raw(7, "x"),
			types.Row_ID(i64(i)),
		)
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None {
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	testing.expect(
		t,
		btree.composite_delete(&ctx.tree, comp_raw(7, "x"), 3) == .None,
		"present pair deletes",
	)
	testing.expect(
		t,
		btree.composite_delete(&ctx.tree, comp_raw(7, "x"), 3) == .Cell_Not_Found,
		"repeat delete misses",
	)
	testing.expect(
		t,
		btree.composite_delete(&ctx.tree, comp_raw(7, "y"), 1) == .Cell_Not_Found,
		"wrong key misses",
	)

	found, f_err := btree.composite_find_rowids(&ctx.tree, ctx.tree.root, comp_raw(7, "x"))
	testing.expect(t, f_err == .None, "find succeeds")
	if f_err == .None {
		testing.expect_value(t, len(found), 4)
	}
	free_all(context.temp_allocator)
}
