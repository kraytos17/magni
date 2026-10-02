package tests

import "core:encoding/endian"
import "core:testing"
import "src:btree"
import "src:types"

W3 :: btree.Page_Id

// v3_build_dense assembles a dense interior page body (after the 24-byte
// header) with independent writes: biased u64le keys or u32le deltas, then
// u32le children. Returns the full page buffer.
v3_build_dense :: proc(
	t: ^testing.T,
	page_id: u32,
	keys: []types.Row_ID,
	children: []u32,
	use_for: bool,
) -> []u8 {
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(buf, page_id), "init dense page")
	if len(keys) == 0 { return buf }

	_, base := btree.dense_choose_encoding(keys[0], keys[len(keys) - 1])
	_ = base
	hdr := btree.get_dense_interior_header(buf, page_id)
	testing.expect(t, hdr != nil, "dense header readable")
	hdr.cell_count = u16le(len(keys))
	if use_for {
		hdr.flags = u16le(btree.DENSE_FLAG_FOR)
		bias_base := btree.rowid_bias_encode(keys[0])
		hdr.base = u64le(bias_base)
		off := 24
		for k in keys {
			biased := btree.rowid_bias_encode(k)
			testing.expect(
				t,
				endian.put_u32(buf[off:], .Little, u32(biased - bias_base)),
				"write delta",
			)
			off += 4
		}
		for c in children {
			testing.expect(t, endian.put_u32(buf[off:], .Little, c), "write child")
			off += 4
		}
	} else {
		off := 24
		for k in keys {
			testing.expect(
				t,
				endian.put_u64(buf[off:], .Little, btree.rowid_bias_encode(k)),
				"write key",
			)
			off += 8
		}
		for c in children {
			testing.expect(t, endian.put_u32(buf[off:], .Little, c), "write child")
			off += 4
		}
	}
	return buf
}

@(test)
test_v3_header_layout :: proc(t: ^testing.T) {
	testing.expect_value(t, size_of(btree.Dense_Interior_Header), 24)
	testing.expect_value(t, size_of(btree.Slot), 10)
	testing.expect_value(t, size_of(btree.Slot), btree.CELL_ENTRY_STRIDE)
	testing.expect_value(t, btree.V3_FORMAT_VERSION, 3)

	// Discriminator bytes land where get_header reads them, and the common
	// 8-byte prefix parses through the shared header view.
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(buf, 2), "init dense")
	testing.expect_value(t, buf[0], u8(btree.Page_Type.INTERIOR_DENSE))
	h := btree.get_header(buf, 2)
	testing.expect(t, h != nil, "common header view")
	testing.expect_value(t, h.page_type, btree.Page_Type.INTERIOR_DENSE)
	testing.expect_value(t, btree.page_header_size(.INTERIOR_DENSE), 24)

	sbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_slot_leaf_page(sbuf, 2), "init slot leaf")
	testing.expect_value(t, sbuf[0], u8(btree.Page_Type.LEAF_SLOTDIR))
	testing.expect_value(t, btree.page_header_size(.LEAF_SLOTDIR), size_of(btree.Leaf_Header))

	// Page-1 100-byte offset rule applies to the new types too.
	p1 := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(p1, 1), "init dense page 1")
	testing.expect_value(t, p1[100], u8(btree.Page_Type.INTERIOR_DENSE))
	dh := btree.get_dense_interior_header(p1, 1)
	testing.expect(t, dh != nil && dh.cell_count == 0, "page-1 dense header")

	// Short buffers fail instead of writing out of bounds (each guard is
	// sized to its own header: 24 for dense, 8 for slot).
	short_dense := make([]u8, 16, context.temp_allocator)
	testing.expect(t, !btree.init_dense_interior_page(short_dense, 2), "short dense init fails")
	short_slot := make([]u8, 4, context.temp_allocator)
	testing.expect(t, !btree.init_slot_leaf_page(short_slot, 2), "short slot init fails")
	testing.expect(t, btree.get_dense_interior_header(short_dense, 2) == nil, "short header nil")
}

@(test)
test_v3_dense_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Table shapes: full-u64 and FOR, incl. negatives, extremes, gaps.
	full_keys := []types.Row_ID {
		-9223372036854775807 - 1,
		-1000,
		-1,
		0,
		1,
		42,
		1000000,
		9223372036854775807,
	}
	full_children := []u32{11, 12, 13, 14, 15, 16, 17, 18, 19}
	fbuf := v3_build_dense(t, 2, full_keys, full_children, false)
	fid := W3(2)
	for k, i in full_keys {
		got, g_err := btree.dense_key_at(fbuf, fid, i)
		testing.expect(t, g_err == .None, "full key_at succeeds")
		testing.expect_value(t, got, k)
	}
	for c, i in full_children {
		got, g_err := btree.dense_child_at(fbuf, fid, i)
		testing.expect(t, g_err == .None, "full child_at succeeds")
		testing.expect_value(t, got, c)
	}
	// Child index == count addresses the rightmost child (asymmetry).
	_, r_err := btree.dense_child_at(fbuf, fid, len(full_children) - 1)
	testing.expect(t, r_err == .None, "rightmost child_at succeeds")
	// Key index == count is out of range (keys are 0..<count).
	_, k_err := btree.dense_key_at(fbuf, fid, len(full_keys))
	testing.expect(t, k_err == .Cell_Not_Found, "key_at past end fails")

	for_keys := []types.Row_ID{1000, 1001, 1050, 1200, 5000}
	for_children := []u32{21, 22, 23, 24, 25, 26}
	dbuf := v3_build_dense(t, 3, for_keys, for_children, true)
	did := W3(3)
	for k, i in for_keys {
		got, g_err := btree.dense_key_at(dbuf, did, i)
		testing.expect(t, g_err == .None, "FOR key_at succeeds")
		testing.expect_value(t, got, k)
	}
	for c, i in for_children {
		got, g_err := btree.dense_child_at(dbuf, did, i)
		testing.expect(t, g_err == .None, "FOR child_at succeeds")
		testing.expect_value(t, got, c)
	}

	// Page-level search agrees on both encodings (gaps included).
	search_targets := [10]types.Row_ID{999, 1000, 1001, 1049, 1050, 1199, 1200, 4999, 5000, 5001}

	for target in search_targets {
		idx, lb_err := btree.dense_page_lower_bound(dbuf, did, target)
		testing.expect(t, lb_err == .None, "FOR page search succeeds")
		// Oracle: first index with key >= target.
		want_idx := len(for_keys)
		for k, i in for_keys {
			if k >= target { want_idx = i; break }
		}
		testing.expect(t, idx == want_idx, "FOR page search matches oracle")
	}

	// Corruption is loud, never silent.
	_, oob := btree.dense_key_at(fbuf, fid, -1)
	testing.expect(t, oob == .Cell_Not_Found, "negative key index fails")
	_, och := btree.dense_child_at(fbuf, fid, 100)
	testing.expect(t, och == .Cell_Not_Found, "child past rightmost fails")
	trunc := fbuf[:100]
	_, terr := btree.dense_key_at(trunc, fid, 0)
	testing.expect(t, terr != .None, "truncated page fails")
	badflag := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	copy(badflag, fbuf)
	badflag[12] |= 0x04 // unknown flag bit
	_, ferr := btree.dense_key_at(badflag, fid, 0)
	testing.expect(t, ferr == .Unsupported_Format, "unknown flag fails closed")
	bigcount := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	copy(bigcount, fbuf)
	bigcount[3] = 0xFF // count low byte: 8 -> 0xFF08, far past the page
	bigcount[4] = 0xFF
	_, cerr := btree.dense_key_at(bigcount, fid, 0)
	testing.expect(t, cerr == .Cell_Deserialize_Failed, "count overflow fails")
}

@(test)
test_v3_dense_lower_bound_oracle :: proc(t: ^testing.T) {
	// Pure branchless search vs linear scan over patterned biased keys.
	linear_oracle :: proc(keys: []u64, target: u64) -> int {
		for k, i in keys {
			if k >= target { return i }
		}
		return len(keys)
	}

	patterns := make([dynamic][]u64, 0, 16, context.temp_allocator)
	append(
		&patterns,
		[]u64{},
		[]u64{10},
		[]u64{10, 20},
		[]u64{5, 5, 5, 9, 9, 20},
		[]u64{0, 7, 14, 21, 28, 35, 42, 49, 56},
		[]u64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		[]u64{0xFFFFFFFFFFFFFF00, 0xFFFFFFFFFFFFFF80, 0xFFFFFFFFFFFFFFFF},
		[]u64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	)

	big := make([]u64, 1000, context.temp_allocator)
	for i in 0 ..< 1000 { big[i] = u64(i * 7919) }
	append(&patterns, big)

	targets := []u64{0, 1, 5, 9, 10, 15, 100, 1000, 7919 * 500, max(u64) - 1, max(u64)}
	for keys, pi in patterns {
		for target in targets {
			got := btree.dense_lower_bound_u64(keys, target)
			want := linear_oracle(keys, target)
			testing.expectf(
				t,
				got == want,
				"pattern %d target %d: got %d want %d",
				pi,
				target,
				got,
				want,
			)
			if got != want { return }
		}
	}
}

@(test)
test_v3_for_rule :: proc(t: ^testing.T) {
	// Boundary: diff == max(u32) fits, +1 does not.
	use, base := btree.dense_choose_encoding(100, 100 + types.Row_ID(max(u32)))
	testing.expect(t, use, "max-u32 span fits FOR")
	testing.expect_value(t, base, btree.rowid_bias_encode(100))
	use2, _ := btree.dense_choose_encoding(100, 100 + types.Row_ID(max(u32)) + 1)
	testing.expect(t, !use2, "max-u32+1 span rejects FOR")
	use3, _ := btree.dense_choose_encoding(500, 100)
	testing.expect(t, !use3, "unordered pair rejects FOR")
	use4, base4 := btree.dense_choose_encoding(-5000, 5000)
	testing.expect(t, use4, "negative-spanning range fits")
	testing.expect_value(t, base4, btree.rowid_bias_encode(-5000))

	// Bias roundtrip incl extremes; encoded order == numeric order.
	lo := types.Row_ID(-9223372036854775807) - 1
	hi := types.Row_ID(9223372036854775807)
	sweep := [7]types.Row_ID{lo, -1000000, -1, 0, 1, 1000000, hi}
	prev_enc: u64 = 0
	for v, i in sweep {
		w := btree.rowid_bias_encode(v)
		testing.expect_value(t, btree.rowid_bias_decode(w), v)
		if i > 0 { testing.expect(t, prev_enc < w, "biased order matches numeric") }
		prev_enc = w
	}

	// Single source of truth: bias pair agrees with the rowid codec bytes.
	kb := make([]u8, 16, context.temp_allocator)
	for v in sweep {
		n, ok := btree.key_encode(.Rowid, types.value_int(i64(v)), 0, kb)
		testing.expect(t, ok && n == 9, "codec encodes")
		// Payload bytes [1:9] equal the bias word big-endian.
		wire: u64 = 0
		for b in kb[1:9] { wire = wire << 8 | u64(b) }
		testing.expect_value(t, wire, btree.rowid_bias_encode(v))
	}
}

@(test)
test_v3_slot_leaf :: proc(t: ^testing.T) {
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_slot_leaf_page(buf, 2), "init slot leaf")
	pid := W3(2)

	// Hand-place 4 slots with independent writes (rowid, offset).
	entries := [][2]u64{{100, 4000}, {200, 3900}, {300, 3800}, {400, 3700}}
	hdr := btree.get_leaf_header(buf, 2)
	testing.expect(t, hdr != nil, "slot header readable")
	for e, i in entries {
		off := 8 + i * 10
		testing.expect(
			t,
			endian.put_u64(buf[off:], .Little, btree.rowid_bias_encode(types.Row_ID(e[0]))),
			"write slot rowid",
		)
		testing.expect(t, endian.put_u16(buf[off + 8:], .Little, u16(e[1])), "write slot off")
	}
	hdr.cell_count = 4
	hdr.cell_content_offset = u16le(3600)

	for e, i in entries {
		k, off, k_err := btree.slot_at(buf, pid, i)
		testing.expect(t, k_err == .None, "slot_at succeeds")
		testing.expect_value(t, k, types.Row_ID(e[0]))
		testing.expect_value(t, off, u16(e[1]))
	}
	_, _, oob := btree.slot_at(buf, pid, 4)
	testing.expect(t, oob == .Cell_Not_Found, "slot past end fails")
	_, _, neg := btree.slot_at(buf, pid, -1)
	testing.expect(t, neg == .Cell_Not_Found, "slot negative fails")

	// Search incl gaps.
	slot_cases := [5][2]int{{99, 0}, {100, 0}, {150, 1}, {400, 3}, {401, 4}}
	for c in slot_cases {
		idx, ok := btree.slot_lower_bound(buf, pid, types.Row_ID(c[0]))
		testing.expect(t, ok, "slot search succeeds")
		testing.expect_value(t, idx, c[1])
	}

	// Validate clean, then each corruption class.
	testing.expect(t, btree.validate_slot_leaf(buf, pid) == .None, "validate clean")
	unsorted := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	copy(unsorted, buf)
	testing.expect(
		t,
		endian.put_u64(unsorted[8 + 2 * 10:], .Little, btree.rowid_bias_encode(50)),
		"unsort a slot",
	)
	testing.expect(
		t,
		btree.validate_slot_leaf(unsorted, pid) == .Cell_Deserialize_Failed,
		"validate rejects unsorted",
	)
	bigcount := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	copy(bigcount, buf)
	bigcount[3] = 0xFF
	bigcount[4] = 0xFF
	testing.expect(
		t,
		btree.validate_slot_leaf(bigcount, pid) == .Cell_Deserialize_Failed,
		"validate rejects count overflow",
	)
	// Count claims slots past the buffer: slot_at must fail, not panic.
	// (Index 500 needs bytes up to 8+501*10 = 5018 > 4096.)
	_, _, big_err := btree.slot_at(bigcount, pid, 500)
	testing.expect(t, big_err == .Cell_Deserialize_Failed, "slot OOB fails loudly")
	badoff := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	copy(badoff, buf)
	testing.expect(t, endian.put_u16(badoff[8 + 8:], .Little, 12), "point a slot at the header")
	testing.expect(
		t,
		btree.validate_slot_leaf(badoff, pid) == .Cell_Deserialize_Failed,
		"validate rejects header-overlapping offset",
	)
}

@(test)
test_v3_dense_table_ops :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Table behavior through the dispatcher (not the free fns): correct
	// values prove dense routing (compat would misread the bytes), and the
	// leaf-slot ops must refuse (no cell bytes on interiors).
	keys := [8]types.Row_ID{10, 20, 30, 40, 50, 60, 70, 80}
	children := [9]u32{101, 102, 103, 104, 105, 106, 107, 108, 109}
	page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.dense_build_from_sorted(page, 2, keys[:], children[:]) == .None,
		"build table page",
	)
	pid := btree.Page_Id(2)
	layout, kind, l_err := btree.layout_for_page(page, pid)
	testing.expect(t, l_err == .None, "dense page resolves")
	testing.expect_value(t, kind, btree.Key_Kind.Rowid)
	testing.expect_value(t, layout.vtable.header_size(.INTERIOR_DENSE), 24)
	testing.expect_value(t, layout.vtable.cell_count(page, pid), 8)

	for k, i in keys {
		got, g_err := layout.vtable.key_at(page, pid, i)
		testing.expect(t, g_err == .None, "table key_at succeeds")
		testing.expect_value(t, got, k)
	}
	lb_cases := [5][2]int{{5, 0}, {10, 0}, {25, 2}, {80, 7}, {81, 8}}
	for c in lb_cases {
		idx, lb_err := layout.vtable.lower_bound_rowid(page, pid, types.Row_ID(c[0]))
		testing.expect(t, lb_err == .None, "table search succeeds")
		testing.expect_value(t, idx, c[1])
	}
	for c, i in children {
		got, g_err := layout.vtable.child_at(page, pid, i)
		testing.expect(t, g_err == .None, "table child_at succeeds")
		testing.expect_value(t, got, c)
	}
	// Rightmost asymmetry: index == count addresses the last child.
	got_r, r_err := layout.vtable.child_at(page, pid, 8)
	testing.expect(t, r_err == .None, "rightmost child_at succeeds")
	testing.expect_value(t, got_r, u32(109))
	_, past_r := layout.vtable.child_at(page, pid, 9)
	testing.expect(t, past_r == .Cell_Not_Found, "child past rightmost fails")

	// Leaf-slot ops have no meaning on interiors: loud refusal, and the
	// refusal itself proves the dispatcher routed to the dense table (the
	// compat table would happily corrupt these bytes).
	_, cp_err := layout.vtable.cell_ptr_at(page, pid, 0)
	testing.expect(t, cp_err == .Unsupported_Format, "interior cell_ptr_at refused")
	testing.expect(
		t,
		layout.vtable.slot_insert(page, pid, 0, 5, btree.Cell_Off(100)) == .Unsupported_Format,
		"interior slot_insert refused",
	)

	// separator_insert in the middle: count owned by the table (Option A).
	testing.expect(
		t,
		layout.vtable.separator_insert(page, pid, 4, 45, 999) == .None,
		"separator insert succeeds",
	)
	testing.expect_value(t, layout.vtable.cell_count(page, pid), 9)
	want_keys := [9]types.Row_ID{10, 20, 30, 40, 45, 50, 60, 70, 80}
	for k, i in want_keys {
		got, g_err := layout.vtable.key_at(page, pid, i)
		testing.expect(t, g_err == .None, "post-insert key_at succeeds")
		testing.expect_value(t, got, k)
	}
	got_c, c_err := layout.vtable.child_at(page, pid, 4)
	testing.expect(t, c_err == .None, "inserted child reads back")
	testing.expect_value(t, got_c, u32(999))
	testing.expect(t, layout.vtable.validate(page, pid) == .None, "validate after insert")

	// Duplicate separators allowed (matches V2: no interior uniqueness).
	testing.expect(
		t,
		layout.vtable.separator_insert(page, pid, 0, 10, 1000) == .None,
		"duplicate separator allowed",
	)
	testing.expect_value(t, layout.vtable.cell_count(page, pid), 10)

	// FOR pages: in-range inserts fit, out-of-range fail without re-encoding.
	fkeys := [5]types.Row_ID{1000, 1001, 1002, 1003, 1004}
	fchildren := [6]u32{201, 202, 203, 204, 205, 206}
	fpage := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.dense_build_from_sorted(fpage, 3, fkeys[:], fchildren[:]) == .None,
		"build FOR page",
	)
	fpid := btree.Page_Id(3)
	flayout, _, fl_err := btree.layout_for_page(fpage, fpid)
	testing.expect(t, fl_err == .None, "FOR page resolves")
	testing.expect(
		t,
		flayout.vtable.separator_insert(fpage, fpid, 2, 1001, 299) == .None,
		"in-range FOR insert fits",
	)
	// 5e9 span exceeds u32 (base 1000): fails without re-encoding.
	testing.expect(
		t,
		flayout.vtable.separator_insert(fpage, fpid, 0, 5000001000, 300) == .Page_Full,
		"out-of-range FOR insert fails without re-encoding",
	)
	testing.expect(t, flayout.vtable.validate(fpage, fpid) == .None, "validate FOR after insert")
}

@(test)
test_v3_dense_build_shapes :: proc(t: ^testing.T) {
	// Empty keys: single-child page, still valid.
	empty_page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	empty_children := [1]u32{77}
	testing.expect(
		t,
		btree.dense_build_from_sorted(empty_page, 2, {}, empty_children[:]) == .None,
		"build empty page",
	)
	testing.expect(t, btree.validate_dense_interior(empty_page, W3(2)) == .None, "empty validates")
	rc, rc_err := btree.dense_child_at(empty_page, W3(2), 0)
	testing.expect(t, rc_err == .None, "empty rightmost reads")
	testing.expect_value(t, rc, u32(77))

	// Children/key count mismatch fails before touching the buffer.
	bad_page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	two_children := [2]u32{1, 2}
	testing.expect(
		t,
		btree.dense_build_from_sorted(bad_page, 2, {}, two_children[:]) == .Invalid_Bounds,
		"children mismatch fails",
	)

	// Overflow: sparse keys defeat FOR (span > u32 ⇒ full u64), and 500
	// full keys need 24+4000+2004 > 4096.
	many_keys := make([]types.Row_ID, 500, context.temp_allocator)
	for i in 0 ..< 500 { many_keys[i] = types.Row_ID(i * 10000000) }

	many_children := make([]u32, 501, context.temp_allocator)
	for i in 0 ..< 501 { many_children[i] = u32(1000 + i) }

	full_page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.dense_build_from_sorted(full_page, 2, many_keys, many_children) == .Page_Full,
		"overflowing build fails",
	)

	// Full-capacity build succeeds: 339 sparse (full-u64) keys need
	// exactly 24+2712+1360 = 4096 — the exact-fit boundary.
	cap_keys := make([]types.Row_ID, 339, context.temp_allocator)
	for i in 0 ..< 339 { cap_keys[i] = types.Row_ID(i * 20000000) }

	cap_children := make([]u32, 340, context.temp_allocator)
	for i in 0 ..< 340 { cap_children[i] = u32(5000 + i) }

	cap_page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.dense_build_from_sorted(cap_page, 2, cap_keys, cap_children) == .None,
		"full-capacity build succeeds",
	)
	testing.expect(
		t,
		btree.validate_dense_interior(cap_page, W3(2)) == .None,
		"full page validates",
	)

	// Unsorted input fails (sortedness is the caller's contract).
	unsorted := [3]types.Row_ID{30, 10, 20}
	uchildren := [4]u32{1, 2, 3, 4}
	upage := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.dense_build_from_sorted(upage, 2, unsorted[:], uchildren[:]) ==
		.Cell_Deserialize_Failed,
		"unsorted build fails",
	)
}

@(test)
test_v3_split_mid :: proc(t: ^testing.T) {
	testing.expect_value(t, btree.dense_split_mid(0), 0)
	testing.expect_value(t, btree.dense_split_mid(1), 0)
	testing.expect_value(t, btree.dense_split_mid(2), 1)
	testing.expect_value(t, btree.dense_split_mid(9), 4)
	testing.expect_value(t, btree.dense_split_mid(10), 5)
	testing.expect_value(t, btree.dense_split_mid(339), 169)
}

@(test)
test_v3_new_slots_fail_closed :: proc(t: ^testing.T) {
	vbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	btree.init_leaf_page(vbuf, 2)
	vpid := btree.Page_Id(2)
	vlayout, v_err := btree.layout_for_version(2)
	testing.expect(t, v_err == .None, "compat resolves")

	_, vc_err := vlayout.vtable.child_at(vbuf, vpid, 0)
	testing.expect(t, vc_err == .Unsupported_Format, "compat child_at refused")
	testing.expect(
		t,
		vlayout.vtable.separator_insert(vbuf, vpid, 0, 5, 9) == .Unsupported_Format,
		"compat separator_insert refused",
	)

	// The dense table, by contrast, works on an empty page: child_at
	// reports the missing child loudly, and the first separator insert
	// succeeds and validates.
	ibuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(ibuf, 2), "init dense")
	il, _, il_err := btree.layout_for_page(ibuf, W3(2))
	testing.expect(t, il_err == .None, "dense resolves")

	null_child, sc_err := il.vtable.child_at(ibuf, W3(2), 0)
	// Empty page: index 0 is in 0..=count, so it reads the (null) rightmost
	// child and reports it — callers treat page 0 as absent, same as V2's
	// right_ptr convention (collect_pages null-checks page 0).
	testing.expect(t, sc_err == .None, "dense child_at on empty page reads null child")
	testing.expect_value(t, null_child, u32(0))
	testing.expect(
		t,
		il.vtable.separator_insert(ibuf, W3(2), 0, 42, 7) == .None,
		"dense separator_insert on empty page succeeds",
	)
	testing.expect(t, il.vtable.validate(ibuf, W3(2)) == .None, "validate after first insert")
}

@(test)
test_v3_dispatcher_stubs :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	ibuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(ibuf, 2), "init dense interior")

	il, ikind, il_err := btree.layout_for_page(ibuf, W3(2))
	testing.expect(t, il_err == .None, "dense interior resolves")
	testing.expect_value(t, ikind, btree.Key_Kind.Rowid)

	_, k_err := il.vtable.key_at(ibuf, W3(2), 0)
	testing.expect(t, k_err == .Cell_Not_Found, "dense key_at on empty page fails by range")
	testing.expect(t, il.vtable.validate(ibuf, W3(2)) == .None, "dense empty page validates")

	_, cp_err := il.vtable.cell_ptr_at(ibuf, W3(2), 0)
	testing.expect(t, cp_err == .Unsupported_Format, "dense cell_ptr_at refused")
	_, ch_err := il.vtable.child_at(ibuf, W3(2), 1)
	testing.expect(t, ch_err == .Cell_Not_Found, "dense child_at past rightmost fails")

	sbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_slot_leaf_page(sbuf, 2), "init slot leaf")
	sl, skind, sl_err := btree.layout_for_page(sbuf, W3(2))
	testing.expect(t, sl_err == .None, "slot leaf resolves")
	testing.expect_value(t, skind, btree.Key_Kind.Rowid)
	_, s_err := sl.vtable.key_at(sbuf, W3(2), 0)

	testing.expect(t, s_err == .Unsupported_Format, "slot key_at is stubbed")
	_, sc_err := sl.vtable.child_at(sbuf, W3(2), 0)
	testing.expect(t, sc_err == .Unsupported_Format, "slot child_at is stubbed")
	testing.expect(
		t,
		sl.vtable.separator_insert(sbuf, W3(2), 0, 1, 2) == .Unsupported_Format,
		"slot separator_insert is stubbed",
	)

	// Unknown discriminant still fails at resolve time.
	ubuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	ubuf[0] = 99

	_, _, u_err := btree.layout_for_page(ubuf, W3(2))
	testing.expect(t, u_err == .Invalid_Page_Header, "unknown type fails closed")
}

@(test)
test_v3_header_dispatch_arms :: proc(t: ^testing.T) {
	testing.expect_value(t, btree.page_header_size(.INTERIOR_DENSE), 24)
	testing.expect_value(t, btree.page_header_size(.LEAF_SLOTDIR), size_of(btree.Leaf_Header))
	testing.expect_value(
		t,
		btree.page_header_size(.INTERIOR_TABLE),
		size_of(btree.Interior_Header),
	)
	testing.expect_value(t, btree.page_header_size(.LEAF_TABLE), size_of(btree.Leaf_Header))

	mk_node :: proc(buf: []u8, id: u32) -> btree.Node {
		h := btree.get_header(buf, id)
		assert(h != nil)
		// Only header is read by is_leaf; the rest stays zero.
		return btree.Node{id = id, data = buf, header = h}
	}

	ibuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(ibuf, 2), "init dense")
	testing.expect(t, !btree.is_leaf(mk_node(ibuf, 2)), "dense interior is interior")
	sbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_slot_leaf_page(sbuf, 2), "init slot leaf")
	testing.expect(t, btree.is_leaf(mk_node(sbuf, 2)), "slot leaf is leaf")
}
