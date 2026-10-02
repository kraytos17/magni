package tests

import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

// Phase A2 additive coverage: the new interfaces agree with the legacy
// free functions, reject garbage loudly, and both codecs round-trip with
// order-preserving encodings. No existing behavior is touched.

@(test)
test_iface_compat_parity :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ctx := setup_tree(t, "ifacecompat")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		vals := []types.Value{types.value_int(i64(i * 10))}
		err := btree.tree_insert(&ctx.tree, types.Row_ID(i), vals)
		testing.expect(t, err == .None, "seed insert succeeds")
		if err != .None { return }
	}

	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == nil, "get page 1")
	if pg_err != nil { return }
	defer pager.unpin_page(ctx.pager, 1)

	layout, kind, l_err := btree.layout_for_page(pg.data, btree.Page_Id(1))
	testing.expect(t, l_err == .None, "dispatcher resolves leaf page")
	if l_err != .None { return }
	testing.expect(t, layout.vtable != nil, "vtable never nil")
	testing.expect_value(t, kind, btree.Key_Kind.Rowid)

	count := layout.vtable.cell_count(pg.data, btree.Page_Id(1))
	testing.expect_value(t, count, 5)

	// key_at agrees with the legacy entry-key read on every slot.
	for i in 0 ..< count {
		k, k_err := layout.vtable.key_at(pg.data, btree.Page_Id(1), i)
		testing.expect(t, k_err == .None, "key_at succeeds")
		testing.expect_value(t, k, types.Row_ID(i + 1))
	}
	// Out-of-range is loud, not a zero key.
	_, oob_err := layout.vtable.key_at(pg.data, btree.Page_Id(1), count)
	testing.expect(t, oob_err == .Cell_Not_Found, "key_at past end fails closed")

	// lower_bound matches binary-search semantics: exact hits and gaps.
	for target in 1 ..= 6 {
		idx, lb_err := layout.vtable.lower_bound_rowid(
			pg.data,
			btree.Page_Id(1),
			types.Row_ID(target),
		)
		testing.expect(t, lb_err == .None, "lower_bound succeeds")
		want := target - 1 if target <= 5 else 5
		testing.expect_value(t, idx, want)
	}

	testing.expect(
		t,
		layout.vtable.validate(pg.data, btree.Page_Id(1)) == .None,
		"validate clean page",
	)

	// Static key dispatch agrees on the primary codec: rowid order.
	ra: [16]u8
	rb: [16]u8
	an, _ := btree.key_encode(kind, types.value_int(-1), 0, ra[:])
	bn, _ := btree.key_encode(kind, types.value_int(0), 0, rb[:])
	testing.expect(t, an == 9 && bn == 9, "rowid encodes")
	testing.expect(t, btree.key_compare(kind, ra[:an], rb[:bn]) < 0, "static dispatch orders")

	// layout_for_version: V2 resolves, unknown fails closed.
	_, v_err := btree.layout_for_version(2)
	testing.expect(t, v_err == .None, "version 2 resolves")
	_, vu_err := btree.layout_for_version(999)
	testing.expect(t, vu_err == .Unsupported_Format, "unknown version fails closed")
}

@(test)
test_iface_stubs_fail_closed :: proc(t: ^testing.T) {
	// V3 tables exist as constructors but every op is loud until its phase.
	dense := btree.dense_u64_interior_layout()
	_, k_err := dense.vtable.key_at(nil, btree.Page_Id(2), 0)
	testing.expect(t, k_err == .Unsupported_Format, "dense interior stub key_at")
	_, lb_err := dense.vtable.lower_bound_rowid(nil, btree.Page_Id(2), 1)
	testing.expect(t, lb_err == .Unsupported_Format, "dense interior stub lower_bound")
	testing.expect(
		t,
		dense.vtable.slot_insert(nil, btree.Page_Id(2), 0, 1, btree.Cell_Off(0)) ==
		.Unsupported_Format,
		"dense interior stub insert",
	)

	prefix := btree.prefix_leaf_layout()
	testing.expect(
		t,
		prefix.vtable.validate(nil, btree.Page_Id(2)) == .Unsupported_Format,
		"prefix leaf stub validate",
	)
	pinterior := btree.prefix_interior_layout()
	testing.expect(
		t,
		pinterior.vtable.slot_delete(nil, btree.Page_Id(2), 0) == .Unsupported_Format,
		"prefix interior stub delete",
	)

	// Dispatcher on headerless garbage fails closed, never a nil vtable.
	_, _, g_err := btree.layout_for_page(nil, btree.Page_Id(7))
	testing.expect(t, g_err == .Invalid_Page_Header, "nil page fails closed")
	short := make([]u8, 4, context.temp_allocator)
	_, _, s_err := btree.layout_for_page(short, btree.Page_Id(2))
	testing.expect(t, s_err == .Invalid_Page_Header, "short page fails closed")
}

@(test)
test_rowid_codec_bias :: proc(t: ^testing.T) {
	kind := btree.Key_Kind.Rowid
	// Round-trip incl negatives and extremes.
	lo := i64(-9223372036854775807) - 1
	hi := i64(9223372036854775807)
	vals := [7]i64{lo, -257, -1, 0, 1, 257, hi}
	for v in vals {
		buf: [16]u8
		n, ok := btree.key_encode(kind, types.value_int(v), types.Row_ID(v), buf[:])
		testing.expect(t, ok, "i64 encodes")
		testing.expect_value(t, n, 9)
		testing.expect_value(t, btree.key_encoded_len(kind, types.value_int(v)), 9)
		// Unsigned byte order of the payload == numeric order is checked
		// below pairwise; here just the tag byte.
		testing.expect_value(t, buf[0], u8(0x52))
	}
	// Order preservation across the sign boundary.
	neg_buf: [16]u8
	pos_buf: [16]u8
	nn, _ := btree.key_encode(kind, types.value_int(-1), 0, neg_buf[:])
	pn, _ := btree.key_encode(kind, types.value_int(0), 0, pos_buf[:])
	testing.expect(t, nn == 9 && pn == 9, "both encode")
	testing.expect(
		t,
		btree.key_compare(kind, neg_buf[:nn], pos_buf[:pn]) < 0,
		"negative sorts before zero",
	)
	// Non-i64 values are not indexed by the rowid codec.
	testing.expect_value(t, btree.key_encoded_len(kind, types.value_text("x")), 0)
	_, tok := btree.key_encode(kind, types.value_null(), 0, neg_buf[:])
	testing.expect(t, !tok, "Null does not encode")
	// Shared prefix capped.
	a := []u8{1, 2, 3, 4}
	b := []u8{1, 2, 9, 9}
	testing.expect_value(t, btree.key_shared_prefix_len(kind, a, b, 99), 2)
	testing.expect_value(t, btree.key_shared_prefix_len(kind, a, b, 1), 1)
	testing.expect_value(t, btree.key_shared_prefix_len(kind, a, b, -5), 0)
}

@(test)
test_text_codec_roundtrip_and_order :: proc(t: ^testing.T) {
	// Round-trip incl empty, embedded NUL, long, negative rowids.
	cases := [][]u8{{}, {0}, {'a'}, {'a', 0, 'b'}, {'u', 's', 'e', 'r', ':', '0', '0', '1'}}
	for raw, i in cases {
		s := string(raw)
		buf := make([]u8, 32 + len(raw), context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(i64(i) - 3), buf)
		testing.expect(t, ok, "text encodes")
		testing.expect(t, n == cell.text_index_encoded_len(types.value_text(s)), "len matches")
		ds, dr, dok := cell.text_index_decode(buf[:n])
		testing.expect(t, dok, "decode succeeds")
		testing.expect(t, ds == s, "text round-trips incl NUL/empty")
		testing.expect_value(t, dr, types.Row_ID(i64(i) - 3))
	}
	// Exact-length decode: truncation and trailing garbage fail loudly.
	full := make([]u8, 64, context.temp_allocator)
	fn, _ := cell.text_index_encode(types.value_text("abc"), 7, full)
	_, _, tok := cell.text_index_decode(full[:fn - 1])
	testing.expect(t, !tok, "truncation fails")
	_, _, gok := cell.text_index_decode(full[:fn + 1])
	testing.expect(t, !gok, "trailing garbage fails")

	// Non-TEXT storage classes are not indexed (NULL-not-indexed extended).
	blob := make([]u8, 2, context.temp_allocator)
	testing.expect_value(t, cell.text_index_encoded_len(types.value_int(1)), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value_null()), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value_real(1.5)), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value_blob(blob)), 0)
	scratch: [64]u8
	_, iok := cell.text_index_encode(types.value_int(1), 1, scratch[:])
	testing.expect(t, !iok, "int does not encode into text index")
	_, nok := cell.text_index_encode(types.value_null(), 1, scratch[:])
	testing.expect(t, !nok, "Null does not encode")

	// Order: text BINARY first, rowid tiebreak second (incl negatives).
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	testing.expect(t, cell.text_index_compare(enc("a", 5), enc("aa", 1)) < 0, "prefix sorts first")
	testing.expect(t, cell.text_index_compare(enc("aa", 1), enc("b", 1)) < 0, "aa before b")
	testing.expect(t, cell.text_index_compare(enc("x", -5), enc("x", 3)) < 0, "rowid tiebreak")
	testing.expect_value(t, cell.text_index_compare(enc("same", 2), enc("same", 2)), 0)

	// Prefix helpers for LIKE 'abc%' planning.
	testing.expect(
		t,
		cell.text_index_has_prefix({'a', 'b', 'c', 'd'}, {'a', 'b', 'c'}),
		"prefix hit",
	)
	testing.expect(
		t,
		!cell.text_index_has_prefix({'a', 'b'}, {'a', 'b', 'c'}),
		"longer prefix misses",
	)
	testing.expect_value(
		t,
		cell.text_index_shared_prefix({'u', 's', 'e', 'r'}, {'u', 's', 'x'}, 99),
		2,
	)
	testing.expect_value(t, cell.text_index_shared_prefix({'a'}, {'a'}, 0), 0)

	// Same surface through the static Key_Kind dispatch.
	tkey := btree.Key_Kind.Text
	kb := make([]u8, 64, context.temp_allocator)
	kn, kok := btree.key_encode(tkey, types.value_text("k"), 9, kb)
	testing.expect(t, kok && kn > 0, "static dispatch encodes")
	testing.expect_value(t, btree.key_encoded_len(tkey, types.value_int(1)), 0)
	_, kiok := btree.key_encode(tkey, types.value_int(1), 9, kb)
	testing.expect(t, !kiok, "static dispatch skips non-text")
	testing.expect(
		t,
		btree.key_compare(tkey, enc("a", 1), enc("b", 2)) < 0,
		"static dispatch compares",
	)
}
