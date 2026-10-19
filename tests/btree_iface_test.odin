// Btree interface tests: dispatcher parity between vtable and free
// functions, fail-closed stubs, and rowid/text codec round-trips.
package tests

import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

@(test)
test_iface_dispatcher_parity :: proc(t: ^testing.T) {
	ctx := setup_tree(t, "ifacecompat")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		vals := []types.Value{types.value(i64(i * 10))}
		must_tree_insert(t, &ctx.tree, types.Row_ID(i), vals)
	}

	pg, pg_err := pager.get_page(ctx.pager, 1)
	testing.expect(t, pg_err == nil, "get page 1")
	if pg_err != nil {
		return
	}
	defer pager.unpin_page(ctx.pager, 1)

	layout, kind, l_err := btree.layout_for_page(pg.data, btree.Page_Id(1))
	testing.expect(t, l_err == .None, "dispatcher resolves leaf page")
	if l_err != .None {
		return
	}

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
	an, _ := btree.key_encode(kind, types.value(-1), 0, ra[:])
	bn, _ := btree.key_encode(kind, types.value(0), 0, rb[:])
	testing.expect(t, an == 9 && bn == 9, "rowid encodes")
	testing.expect(t, btree.key_compare(kind, ra[:an], rb[:bn]) < 0, "static dispatch orders")
}

@(test)
test_iface_stubs_fail_closed :: proc(t: ^testing.T) {
	dense := btree.dense_u64_interior_layout()
	_, k_err := dense.vtable.key_at(nil, btree.Page_Id(2), 0)

	testing.expect(t, k_err == .Invalid_Page_Header, "dense key_at on nil data fails")
	_, lb_err := dense.vtable.lower_bound_rowid(nil, btree.Page_Id(2), 1)

	testing.expect(t, lb_err == .Invalid_Page_Header, "dense search on nil data fails")
	testing.expect(
		t,
		dense.vtable.slot_insert(nil, btree.Page_Id(2), 0, 1, btree.Cell_Off(0)) ==
		.Unsupported_Format,
		"dense interior stub insert",
	)

	prefix := btree.prefix_leaf_layout()
	testing.expect(
		t,
		prefix.vtable.validate(nil, btree.Page_Id(2)) == .Invalid_Page_Header,
		"prefix leaf validate on nil fails closed",
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
		n, ok := btree.key_encode(kind, types.value(v), types.Row_ID(v), buf[:])
		testing.expect(t, ok, "i64 encodes")
		testing.expect_value(t, n, 9)
		testing.expect_value(t, btree.key_encoded_len(kind, types.value(v)), 9)
		// Unsigned byte order of the payload == numeric order is checked
		// below pairwise; here just the tag byte.
		testing.expect_value(t, buf[0], u8(0x52))
	}
	// Order preservation across the sign boundary.
	neg_buf: [16]u8
	pos_buf: [16]u8
	nn, _ := btree.key_encode(kind, types.value(-1), 0, neg_buf[:])
	pn, _ := btree.key_encode(kind, types.value(0), 0, pos_buf[:])
	testing.expect(t, nn == 9 && pn == 9, "both encode")
	testing.expect(
		t,
		btree.key_compare(kind, neg_buf[:nn], pos_buf[:pn]) < 0,
		"negative sorts before zero",
	)
	// Non-i64 values are not indexed by the rowid codec.
	testing.expect_value(t, btree.key_encoded_len(kind, types.value("x")), 0)
	_, tok := btree.key_encode(kind, types.value(), 0, neg_buf[:])
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
		n, ok := cell.text_index_encode(types.value(s), types.Row_ID(i64(i) - 3), buf)
		testing.expect(t, ok, "text encodes")
		testing.expect(t, n == cell.text_index_encoded_len(types.value(s)), "len matches")
		ds, dr, dok := cell.text_index_decode(buf[:n])
		testing.expect(t, dok, "decode succeeds")
		testing.expect(t, ds == s, "text round-trips incl NUL/empty")
		testing.expect_value(t, dr, types.Row_ID(i64(i) - 3))
	}
	// Exact-length decode: truncation and trailing garbage fail loudly.
	full := make([]u8, 64, context.temp_allocator)
	fn, _ := cell.text_index_encode(types.value("abc"), 7, full)
	_, _, tok := cell.text_index_decode(full[:fn - 1])
	testing.expect(t, !tok, "truncation fails")
	_, _, gok := cell.text_index_decode(full[:fn + 1])
	testing.expect(t, !gok, "trailing garbage fails")

	// Non-TEXT storage classes are not indexed (NULL-not-indexed extended).
	blob := make([]u8, 2, context.temp_allocator)
	testing.expect_value(t, cell.text_index_encoded_len(types.value(1)), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value()), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value(1.5)), 0)
	testing.expect_value(t, cell.text_index_encoded_len(types.value(blob)), 0)
	scratch: [64]u8
	_, iok := cell.text_index_encode(types.value(1), 1, scratch[:])
	testing.expect(t, !iok, "int does not encode into text index")
	_, nok := cell.text_index_encode(types.value(), 1, scratch[:])
	testing.expect(t, !nok, "Null does not encode")

	// Order: text BINARY first, rowid tiebreak second (incl negatives).
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value(s), types.Row_ID(r), b)
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
	kn, kok := btree.key_encode(tkey, types.value("k"), 9, kb)
	testing.expect(t, kok && kn > 0, "static dispatch encodes")
	testing.expect_value(t, btree.key_encoded_len(tkey, types.value(1)), 0)
	_, kiok := btree.key_encode(tkey, types.value(1), 9, kb)
	testing.expect(t, !kiok, "static dispatch skips non-text")
	testing.expect(
		t,
		btree.key_compare(tkey, enc("a", 1), enc("b", 2)) < 0,
		"static dispatch compares",
	)
}

@(test)
test_int_codec_roundtrip_and_order :: proc(t: ^testing.T) {
	// Round-trip incl extremes and negative rowids; fixed 17-byte width.
	lo := i64(-9223372036854775807) - 1
	hi := i64(9223372036854775807)
	vals := []i64{lo, -257, -1, 0, 1, 257, hi}
	for v, i in vals {
		buf: [32]u8
		n, ok := cell.int_index_encode(types.value(v), types.Row_ID(i64(i) - 3), buf[:])
		testing.expect(t, ok, "int encodes")
		testing.expect_value(t, n, 17)
		testing.expect(t, n == cell.int_index_encoded_len(types.value(v)), "len matches")
		dv, dr, dok := cell.int_index_decode(buf[:n])
		testing.expect(t, dok, "decode succeeds")
		testing.expect_value(t, dv, v)
		testing.expect_value(t, dr, types.Row_ID(i64(i) - 3))
	}
	// Exact-length decode: truncation and trailing garbage fail loudly.
	full := make([]u8, 32, context.temp_allocator)
	fn, _ := cell.int_index_encode(types.value(42), 7, full)
	_, _, tok := cell.int_index_decode(full[:fn - 1])
	testing.expect(t, !tok, "truncation fails")
	_, _, gok := cell.int_index_decode(full[:fn + 1])
	testing.expect(t, !gok, "trailing garbage fails")

	// Non-i64 storage classes are not indexed.
	blob := make([]u8, 2, context.temp_allocator)
	testing.expect_value(t, cell.int_index_encoded_len(types.value("x")), 0)
	testing.expect_value(t, cell.int_index_encoded_len(types.value()), 0)
	testing.expect_value(t, cell.int_index_encoded_len(types.value(1.5)), 0)
	testing.expect_value(t, cell.int_index_encoded_len(types.value(blob)), 0)
	scratch: [32]u8
	_, sok := cell.int_index_encode(types.value("x"), 1, scratch[:])
	testing.expect(t, !sok, "text does not encode into int index")
	_, nok := cell.int_index_encode(types.value(), 1, scratch[:])
	testing.expect(t, !nok, "Null does not encode")

	// Order: numeric value first (across the sign boundary), rowid tiebreak.
	enc := proc(v: i64, r: i64) -> []u8 {
		b := make([]u8, 32, context.temp_allocator)
		n, ok := cell.int_index_encode(types.value(v), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	testing.expect(t, cell.int_index_compare(enc(-1, 9), enc(0, 0)) < 0, "negative sorts first")
	testing.expect(t, cell.int_index_compare(enc(0, 9), enc(1, 0)) < 0, "zero before positive")
	testing.expect(t, cell.int_index_compare(enc(5, -5), enc(5, 3)) < 0, "rowid tiebreak")
	testing.expect_value(t, cell.int_index_compare(enc(7, 2), enc(7, 2)), 0)

	// Same surface through the static Key_Kind dispatch.
	ikey := btree.Key_Kind.Int
	kb := make([]u8, 32, context.temp_allocator)
	kn, kok := btree.key_encode(ikey, types.value(11), 9, kb)
	testing.expect(t, kok && kn == 17, "static dispatch encodes")
	testing.expect_value(t, btree.key_encoded_len(ikey, types.value("x")), 0)
	_, kiok := btree.key_encode(ikey, types.value("x"), 9, kb)
	testing.expect(t, !kiok, "static dispatch skips non-int")
	testing.expect(
		t,
		btree.key_compare(ikey, enc(-5, 1), enc(3, 2)) < 0,
		"static dispatch compares",
	)
	testing.expect_value(t, btree.key_shared_prefix_len(ikey, enc(1, 1), enc(1, 2), 99), 16)
}

@(test)
test_real_codec_roundtrip_and_order :: proc(t: ^testing.T) {
	// Round-trip incl -0.0 normalization (decodes as +0.0, matching
	// cmp_f64), infinities, and negative rowids; fixed 17-byte width.
	vals := []f64{-1e308, -257.5, -1, -0.0, 0, 1, 257.5, 1e308}
	for v, i in vals {
		buf: [32]u8
		n, ok := cell.real_index_encode(types.value(v), types.Row_ID(i64(i) - 3), buf[:])
		testing.expect(t, ok, "real encodes")
		testing.expect_value(t, n, 17)
		testing.expect(t, n == cell.real_index_encoded_len(types.value(v)), "len matches")
		dv, dr, dok := cell.real_index_decode(buf[:n])
		testing.expect(t, dok, "decode succeeds")
		testing.expect_value(t, transmute(u64)dv, transmute(u64)(v if v != 0 else 0))
		testing.expect_value(t, dr, types.Row_ID(i64(i) - 3))
	}
	pos_inf := transmute(f64)u64(0x7ff0000000000000)
	neg_inf := transmute(f64)u64(0xfff0000000000000)
	nan := transmute(f64)u64(0x7ff8000000000000)
	ibuf: [32]u8
	in_, iok := cell.real_index_encode(types.value(pos_inf), 1, ibuf[:])
	testing.expect(t, iok && in_ == 17, "infinities encode")
	testing.expect(t, !cell.real_index_encodable(nan), "NaN not encodable")

	// Exact-length decode: truncation and trailing garbage fail loudly.
	full := make([]u8, 32, context.temp_allocator)
	fn, _ := cell.real_index_encode(types.value(4.5), 7, full)
	_, _, tok := cell.real_index_decode(full[:fn - 1])
	testing.expect(t, !tok, "truncation fails")
	_, _, gok := cell.real_index_decode(full[:fn + 1])
	testing.expect(t, !gok, "trailing garbage fails")

	// Non-f64 storage classes and NaN are not indexed.
	blob := make([]u8, 2, context.temp_allocator)
	testing.expect_value(t, cell.real_index_encoded_len(types.value("x")), 0)
	testing.expect_value(t, cell.real_index_encoded_len(types.value()), 0)
	testing.expect_value(t, cell.real_index_encoded_len(types.value(7)), 0)
	testing.expect_value(t, cell.real_index_encoded_len(types.value(blob)), 0)
	testing.expect_value(t, cell.real_index_encoded_len(types.value(nan)), 0)
	scratch: [32]u8
	_, sok := cell.real_index_encode(types.value("x"), 1, scratch[:])
	testing.expect(t, !sok, "text does not encode into real index")
	_, nok := cell.real_index_encode(types.value(nan), 1, scratch[:])
	testing.expect(t, !nok, "NaN does not encode")

	// Order: numeric value first (-Inf < finite < +Inf, -0.0 == +0.0),
	// rowid tiebreak.
	enc := proc(v: f64, r: i64) -> []u8 {
		b := make([]u8, 32, context.temp_allocator)
		n, ok := cell.real_index_encode(types.value(v), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	testing.expect(t, cell.real_index_compare(enc(neg_inf, 0), enc(-1e308, 0)) < 0, "-Inf sorts first")
	testing.expect(t, cell.real_index_compare(enc(1e308, 0), enc(pos_inf, 0)) < 0, "+Inf sorts last")
	testing.expect(t, cell.real_index_compare(enc(-1, 9), enc(0, 0)) < 0, "negative sorts first")
	testing.expect_value(t, cell.real_index_compare(enc(-0.0, 4), enc(0, 4)), 0)
	testing.expect(t, cell.real_index_compare(enc(5.5, -5), enc(5.5, 3)) < 0, "rowid tiebreak")
	testing.expect_value(t, cell.real_index_compare(enc(7.25, 2), enc(7.25, 2)), 0)

	// Same surface through the static Key_Kind dispatch.
	rkey := btree.Key_Kind.Real
	rb := make([]u8, 32, context.temp_allocator)
	rn, rok := btree.key_encode(rkey, types.value(1.5), 9, rb)
	testing.expect(t, rok && rn == 17, "static dispatch encodes")
	testing.expect_value(t, btree.key_encoded_len(rkey, types.value("x")), 0)
	_, rkok := btree.key_encode(rkey, types.value("x"), 9, rb)
	testing.expect(t, !rkok, "static dispatch skips non-real")
	testing.expect(
		t,
		btree.key_compare(rkey, enc(-5.5, 1), enc(3.25, 2)) < 0,
		"static dispatch compares",
	)
	testing.expect_value(t, btree.key_shared_prefix_len(rkey, enc(1.5, 1), enc(1.5, 2), 99), 16)
}

@(test)
test_int_page_types_resolve :: proc(t: ^testing.T) {
	leaf := make([]u8, btree.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_int_leaf_page(leaf, 2), "int leaf inits")
	layout, kind, l_err := btree.layout_for_page(leaf, btree.Page_Id(2))
	testing.expect(t, l_err == .None, "int leaf resolves")
	testing.expect(t, kind == btree.Key_Kind.Int, "int leaf kind is Int")
	testing.expect(
		t,
		layout.vtable.validate(leaf, btree.Page_Id(2)) == .None,
		"empty int leaf validates",
	)

	interior := make([]u8, btree.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_int_interior_page(interior, 2), "int interior inits")
	ilayout, ikind, i_err := btree.layout_for_page(interior, btree.Page_Id(2))
	testing.expect(t, i_err == .None, "int interior resolves")
	testing.expect(t, ikind == btree.Key_Kind.Int, "int interior kind is Int")
	testing.expect(
		t,
		ilayout.vtable.validate(interior, btree.Page_Id(2)) == .None,
		"empty int interior validates",
	)

	// Text pages keep their kind; unknown types still fail closed.
	tleaf := make([]u8, btree.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(tleaf, 2), "text leaf inits")
	_, tkind, _ := btree.layout_for_page(tleaf, btree.Page_Id(2))
	testing.expect(t, tkind == btree.Key_Kind.Text, "text leaf stays Text")
	garbage := make([]u8, btree.PAGE_SIZE, context.temp_allocator)
	garbage[0] = 99
	_, _, g_err := btree.layout_for_page(garbage, btree.Page_Id(2))
	testing.expect(t, g_err != .None, "unknown type fails closed")
}
