// Phase C1 tests: text leaf layout, builders, search-vs-oracle, and
// fail-closed corruption. Buffer-level only (no DB): pages are raw PAGE_SIZE
// buffers, mirroring tests/btree_v3_test.odin.
package tests

import "core:encoding/endian"
import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:types"

WT :: btree.Page_Id

// text_build_raw assembles a text leaf with independent writes (no builder):
// header + prefix + slots, cells packed down from the top. The oracle the
// builder is measured against.
text_build_raw :: proc(
	t: ^testing.T,
	page_id: u32,
	prefix: []u8,
	suffixes: [][]u8,
	rowids: []types.Row_ID,
) -> []u8 {
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(buf, page_id), "init text leaf")
	hdr := btree.get_leaf_header(buf, page_id)
	testing.expect(t, hdr != nil, "text header readable")
	if hdr == nil { return buf }

	base := btree.get_page_header_offset(page_id)
	hdr.cell_count = u16le(u16(len(suffixes)))
	buf[base + 8] = u8(len(prefix))
	buf[base + 9] = u8(len(prefix) >> 8)
	copy(buf[base + 10:base + 10 + len(prefix)], prefix)

	slots_start := base + 10 + len(prefix)
	dest := int(types.PAGE_SIZE)
	for i in 0 ..< len(suffixes) {
		elen := 8 + len(suffixes[i])
		dest -= elen
		testing.expect(
			t,
			endian.put_u64(buf[dest:dest + 8], .Big, u64(i64(rowids[i]) ~ min(i64))),
			"write rowid",
		)
		copy(buf[dest + 8:dest + elen], suffixes[i])
		slot := (^btree.Text_Slot)(raw_data(buf[slots_start + i * size_of(btree.Text_Slot):]))
		slot^ = btree.Text_Slot {
			off = u16le(u16(dest)),
			len = u16le(u16(elen)),
		}
	}
	hdr.cell_content_offset = u16le(u16(dest))
	return buf
}

@(test)
test_text_header_layout :: proc(t: ^testing.T) {
	testing.expect_value(t, size_of(btree.Text_Slot), 4)
	testing.expect_value(t, btree.TEXT_LEAF_FIXED, 10)
	testing.expect_value(t, btree.TEXT_ENTRY_ROWID_LEN, 8)
	testing.expect_value(t, btree.page_header_size(.LEAF_TEXT), size_of(btree.Leaf_Header))

	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(buf, 2), "init text leaf")
	testing.expect_value(t, buf[0], u8(btree.Page_Type.LEAF_TEXT))
	h := btree.get_header(buf, 2)
	testing.expect(t, h != nil, "common header view")
	testing.expect_value(t, h.page_type, btree.Page_Type.LEAF_TEXT)
	testing.expect_value(t, btree.get_cell_count(buf, 2), 0)

	// Empty page validates and searches clean.
	testing.expect(t, btree.text_validate_leaf(buf, WT(2)) == .None, "empty validates")
	testing.expect_value(t, btree.get_cell_count(buf, 2), 0)

	// Page-1 100-byte offset rule applies.
	p1 := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(p1, 1), "init text page 1")
	testing.expect_value(t, p1[100], u8(btree.Page_Type.LEAF_TEXT))

	// Short buffers fail instead of writing out of bounds.
	short := make([]u8, 4, context.temp_allocator)
	testing.expect(t, !btree.init_text_leaf_page(short, 2), "short init fails")
}

@(test)
test_text_dispatch_resolves :: proc(t: ^testing.T) {
	// C2: LEAF_TEXT resolves through the dispatcher with kind .Text —
	// reads flow via Key_Kind.Text dispatch, mechanics via the table.
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(buf, 2), "init text leaf")
	layout, kind, r_err := btree.layout_for_page(buf, WT(2))
	testing.expect(t, r_err == .None, "text page resolves")
	if r_err != .None { return }
	testing.expect(t, layout.vtable != nil, "vtable never nil")
	testing.expect_value(t, kind, btree.Key_Kind.Text)
	testing.expect_value(t, layout.vtable.cell_count(buf, WT(2)), btree.get_cell_count(buf, 2))
	testing.expect(t, layout.vtable.validate(buf, WT(2)) == .None, "table validate agrees")

	// is_leaf still tells the truth about the live variant.
	h := btree.get_header(buf, 2)
	testing.expect(t, h != nil, "header readable")
	if h == nil { return }
	n := btree.Node {
		id     = 2,
		data   = buf,
		header = h,
	}
	testing.expect(t, btree.is_leaf(n), "text leaf is a leaf")
}

@(test)
test_text_table_ops :: proc(t: ^testing.T) {
	// Text table through the dispatcher over a real text page: key-agnostic
	// mechanics agree with the free fns, and every Row_ID-keyed slot
	// refuses (text keys never flow through the vtable).
	context.logger.lowest_level = .Error
	texts := [][]u8{{'a', 'p', 'p', 'l', 'e'}, {'a', 'p', 'p', 'l', 'e', 't'}}
	rids := []types.Row_ID{2, 5}
	page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(page, 2, texts, rids) == .None,
		"build succeeds",
	)
	pid := WT(2)
	layout, kind, l_err := btree.layout_for_page(page, pid)
	testing.expect(t, l_err == .None, "text page resolves")
	if l_err != .None { return }
	testing.expect_value(t, kind, btree.Key_Kind.Text)

	testing.expect_value(t, layout.vtable.cell_count(page, pid), 2)
	testing.expect(
		t,
		layout.vtable.header_size(.LEAF_TEXT) == btree.page_header_size(.LEAF_TEXT),
		"header size agrees",
	)
	testing.expect(t, layout.vtable.validate(page, pid) == .None, "validate agrees")

	_, k_err := layout.vtable.key_at(page, pid, 0)
	testing.expect(t, k_err == .Unsupported_Format, "rowid key_at refused")
	_, lb_err := layout.vtable.lower_bound_rowid(page, pid, 2)
	testing.expect(t, lb_err == .Unsupported_Format, "rowid search refused")
	testing.expect(
		t,
		layout.vtable.slot_insert(page, pid, 0, 9, btree.Cell_Off(100)) == .Unsupported_Format,
		"rowid insert refused",
	)
	testing.expect(
		t,
		layout.vtable.slot_delete(page, pid, 0) == .Unsupported_Format,
		"rowid delete refused",
	)
	_, cp_err := layout.vtable.cell_ptr_at(page, pid, 0)
	testing.expect(t, cp_err == .Unsupported_Format, "cell_ptr_at refused")
	testing.expect(
		t,
		layout.vtable.slot_repoint(page, pid, 0, 9, btree.Cell_Off(100)) == .Unsupported_Format,
		"repoint refused",
	)
	_, c_err := layout.vtable.child_at(page, pid, 0)
	testing.expect(t, c_err == .Unsupported_Format, "child refused on leaf")
	testing.expect(
		t,
		layout.vtable.separator_insert(page, pid, 0, 9, 7) == .Unsupported_Format,
		"separator refused",
	)

	// Table validate on foreign bytes refuses (never reinterpreted).
	fbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_slot_leaf_page(fbuf, 2), "init slotdir")
	testing.expect(
		t,
		layout.vtable.validate(fbuf, pid) == .Invalid_Page_Header,
		"validate refuses slotdir bytes",
	)
}

@(test)
test_text_build_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Shared-prefix page ("appl") + empty-prefix page + single + empty.
	texts := [][]u8 {
		{'a', 'p', 'p', 'l', 'e'},
		{'a', 'p', 'p', 'l', 'e', 't'},
		{'a', 'p', 'p', 'l', 'i', 'c', 'a', 't', 'i', 'o', 'n'},
	}
	rids := []types.Row_ID{2, 5, 4}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.text_build_from_sorted(buf, 2, texts, rids) == .None, "build succeeds")

	prefix, p_err := btree.text_prefix(buf, WT(2))
	testing.expect(t, p_err == .None, "prefix reads")
	testing.expect(t, string(prefix) == "appl", "shared prefix factored")
	testing.expect_value(t, btree.get_cell_count(buf, 2), 3)

	expected_suf := []string{"e", "et", "ication"}
	expected_rid := []types.Row_ID{2, 5, 4}
	for i in 0 ..< 3 {
		suf, rid, k_err := btree.text_entry_at(buf, WT(2), i)
		testing.expect(t, k_err == .None, "entry reads")
		testing.expect(t, string(suf) == expected_suf[i], "suffix round-trips")
		testing.expect_value(t, rid, expected_rid[i])
	}
	_, _, oob_err := btree.text_entry_at(buf, WT(2), 3)
	testing.expect(t, oob_err == .Cell_Not_Found, "past end fails")
	eae, eae_err := btree.text_entry_area_end(buf, WT(2))
	testing.expect(t, eae_err == .None && eae > 0, "entry area positive")
	testing.expect(t, btree.text_validate_leaf(buf, WT(2)) == .None, "build validates")

	// Empty-prefix page: no shared bytes, suffixes are full texts.
	// Sorted: "aa" < "b" < "c".
	sorted_flat := [][]u8{{'a', 'a'}, {'b'}, {'c'}}
	sorted_rids := []types.Row_ID{2, 1, 3}
	fbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(fbuf, 2, sorted_flat, sorted_rids) == .None,
		"flat build succeeds",
	)
	fprefix, _ := btree.text_prefix(fbuf, WT(2))
	testing.expect(t, len(fprefix) == 0, "no shared prefix")

	// Single entry: prefix is the whole text, suffix empty.
	one := [][]u8{{'s', 'o', 'l', 'o'}}
	one_rids := []types.Row_ID{9}
	obuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(obuf, 2, one, one_rids) == .None,
		"single build succeeds",
	)
	oprefix, _ := btree.text_prefix(obuf, WT(2))
	testing.expect(t, string(oprefix) == "solo", "single prefix is full text")
	osuf, orid, _ := btree.text_entry_at(obuf, WT(2), 0)
	testing.expect(t, len(osuf) == 0 && orid == 9, "single suffix empty")

	// Empty page builds and validates.
	ebuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(ebuf, 2, [][]u8{}, []types.Row_ID{}) == .None,
		"empty build succeeds",
	)
	testing.expect_value(t, btree.get_cell_count(ebuf, 2), 0)

	// Mismatched inputs fail closed.
	mbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(mbuf, 2, texts, rids[:2]) == .Invalid_Bounds,
		"length mismatch fails",
	)
}

@(test)
test_text_search_vs_oracle :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Oracle: text_index_compare over wire-codec keys. Corpus pre-sorted by
	// the oracle (pairwise assert below); covers empty text, prefix
	// relations ("app" < "apple"), duplicate text with rowid tiebreak, and
	// a negative rowid through the bias codec.
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	ctext := []string{"", "app", "app", "apple", "applet", "application", "banana"}
	crid := []i64{7, 1, 4, 2, 3, 5, -5}
	N := len(ctext)
	keys := make([][]u8, N, context.temp_allocator)
	for i in 0 ..< N { keys[i] = enc(ctext[i], crid[i]) }
	for i in 0 ..< N - 1 {
		testing.expect(
			t,
			cell.text_index_compare(keys[i], keys[i + 1]) < 0,
			"corpus sorted by oracle",
		)
	}

	texts := make([][]u8, N, context.temp_allocator)
	rids := make([]types.Row_ID, N, context.temp_allocator)
	for i in 0 ..< N {
		texts[i] = transmute([]u8)ctext[i]
		rids[i] = types.Row_ID(crid[i])
	}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(buf, 2, texts, rids) == .None,
		"oracle corpus builds",
	)

	// Targets: exact hits, rowid-straddles, gaps, prefix edges, out-of-range.
	tt := []string{"", "app", "app", "apple", "appl", "applic", "applet", "b", "z"}
	tr := []i64{7, 1, 3, 2, 0, 9, 3, 0, 0}
	for ti in 0 ..< len(tt) {
		tkey := enc(tt[ti], tr[ti])
		want := N
		for j in 0 ..< N {
			if cell.text_index_compare(keys[j], tkey) >= 0 {
				want = j
				break
			}
		}
		got, g_err := btree.text_lower_bound(
			buf,
			WT(2),
			transmute([]u8)tt[ti],
			types.Row_ID(tr[ti]),
		)
		testing.expect(t, g_err == .None, "search succeeds")
		testing.expectf(t, got == want, "target %s:%d -> %d, want %d", tt[ti], tr[ti], got, want)
	}

	// Prefixed page ("appl"): prefix-edge targets resolve against the header.
	ptexts := [][]u8 {
		{'a', 'p', 'p', 'l', 'e'},
		{'a', 'p', 'p', 'l', 'e', 't'},
		{'a', 'p', 'p', 'l', 'i', 'c', 'a', 't', 'i', 'o', 'n'},
	}
	prids := []types.Row_ID{2, 3, 5}
	pbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_build_from_sorted(pbuf, 2, ptexts, prids) == .None,
		"prefixed page builds",
	)
	pt := []string{"appl", "applix", "b", "a", "apple"}
	pr := []i64{0, 0, 0, 0, 2}
	pwant := []int{0, 3, 3, 0, 0}
	for ti in 0 ..< len(pt) {
		got, g_err := btree.text_lower_bound(
			pbuf,
			WT(2),
			transmute([]u8)pt[ti],
			types.Row_ID(pr[ti]),
		)
		testing.expect(t, g_err == .None, "prefixed search succeeds")
		testing.expectf(t, got == pwant[ti], "prefixed %s -> %d, want %d", pt[ti], got, pwant[ti])
	}
}

@(test)
test_text_exact_prefix_rule :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Short shared prefixes are legal page states (inserts never grow P,
	// deletes never shrink it): ordering holds under any common prefix.
	// Raw helper bypasses the builder.
	bad := text_build_raw(
		t,
		2,
		[]u8{'a', 'b'},
		[][]u8{{'x', 'c'}, {'x', 'd'}},
		[]types.Row_ID{1, 2},
	)
	testing.expect(t, btree.text_validate_leaf(bad, WT(2)) == .None, "short prefix accepts")

	good := text_build_raw(t, 2, []u8{'a', 'b'}, [][]u8{{'c'}, {'d'}}, []types.Row_ID{1, 2})
	testing.expect(t, btree.text_validate_leaf(good, WT(2)) == .None, "exact accepts")

	// Single entry with non-empty suffix: still order-correct (the
	// builder always emits exact prefixes; the validator does not demand
	// them back).
	single_bad := text_build_raw(t, 2, []u8{'a'}, [][]u8{{'b'}}, []types.Row_ID{1})
	testing.expect(
		t,
		btree.text_validate_leaf(single_bad, WT(2)) == .None,
		"single short prefix accepts",
	)
	single_good := text_build_raw(t, 2, []u8{'a', 'b'}, [][]u8{{}}, []types.Row_ID{1})
	testing.expect(
		t,
		btree.text_validate_leaf(single_good, WT(2)) == .None,
		"single full prefix accepts",
	)
}

@(test)
test_text_corruption_loud :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	texts := [][]u8{{'a'}, {'b'}}
	rids := []types.Row_ID{1, 2}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.text_build_from_sorted(buf, 2, texts, rids) == .None, "build succeeds")

	// Wrong discriminant: never reinterpreted.
	buf[0] = u8(btree.Page_Type.LEAF_SLOTDIR)
	testing.expect(
		t,
		btree.text_validate_leaf(buf, WT(2)) == .Invalid_Page_Header,
		"foreign bytes refused",
	)
	_, _, k_err := btree.text_entry_at(buf, WT(2), 0)
	testing.expect(t, k_err == .Invalid_Page_Header, "entry on foreign bytes fails")
	buf[0] = u8(btree.Page_Type.LEAF_TEXT)

	// Absurd prefix_len runs past the buffer (prefix_len is bytes 8,9).
	buf[8], buf[9] = 0xff, 0xff
	testing.expect(
		t,
		btree.text_validate_leaf(buf, WT(2)) == .Cell_Deserialize_Failed,
		"absurd prefix fails",
	)
	_, p_err := btree.text_prefix(buf, WT(2))
	testing.expect(t, p_err == .Cell_Deserialize_Failed, "prefix read fails")

	// Unsorted entries (swap the two suffixes via independent writes).
	ubuf := text_build_raw(t, 2, {}, [][]u8{{'b'}, {'a'}}, []types.Row_ID{1, 2})
	testing.expect(
		t,
		btree.text_validate_leaf(ubuf, WT(2)) == .Cell_Deserialize_Failed,
		"unsorted rejected",
	)

	// Duplicate (text,rowid) pair: not orderable, corruption.
	dbuf := text_build_raw(t, 2, {}, [][]u8{{'a'}, {'a'}}, []types.Row_ID{1, 1})
	testing.expect(
		t,
		btree.text_validate_leaf(dbuf, WT(2)) == .Cell_Deserialize_Failed,
		"duplicate pair rejected",
	)

	// Short entry (rowid truncated).
	sbuf := text_build_raw(t, 2, {}, [][]u8{{'a'}}, []types.Row_ID{1})
	shdr := btree.get_leaf_header(sbuf, 2)
	testing.expect(t, shdr != nil, "header readable")
	if shdr != nil {
		// Corrupt the slot length to 4 (< 8-byte rowid).
		slots_start := 10
		sbuf[slots_start + 2], sbuf[slots_start + 3] = 4, 0
		testing.expect(
			t,
			btree.text_validate_leaf(sbuf, WT(2)) == .Cell_Deserialize_Failed,
			"short entry rejected",
		)
	}

	// Builder overflow: page untouched, still validates as empty.
	big_text := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	for i in 0 ..< len(big_text) { big_text[i] = 'x' }
	big := [][]u8{big_text}
	big_rids := []types.Row_ID{1}
	obuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_leaf_page(obuf, 2), "overflow target starts valid")
	testing.expect(
		t,
		btree.text_build_from_sorted(obuf, 2, big, big_rids) == .Page_Full,
		"oversize fails",
	)
	testing.expect(
		t,
		btree.text_validate_leaf(obuf, WT(2)) == .None,
		"failed build leaves valid page",
	)
	testing.expect_value(t, btree.get_cell_count(obuf, 2), 0)
}

@(test)
test_text_interior_header_layout :: proc(t: ^testing.T) {
	testing.expect_value(t, btree.page_header_size(.TEXT_INTERIOR), size_of(btree.Leaf_Header))

	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_interior_page(buf, 2), "init text interior")
	testing.expect_value(t, buf[0], u8(btree.Page_Type.TEXT_INTERIOR))
	h := btree.get_header(buf, 2)
	testing.expect(t, h != nil, "common header view")
	testing.expect_value(t, h.page_type, btree.Page_Type.TEXT_INTERIOR)
	testing.expect_value(t, btree.get_cell_count(buf, 2), 0)

	// Empty interior (single rightmost child) validates.
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(buf, 2, [][]u8{}, []u32{42}) == .None,
		"empty interior builds",
	)
	testing.expect(
		t,
		btree.text_validate_interior(buf, WT(2)) == .None,
		"empty interior validates",
	)
	c, c_err := btree.text_interior_child_at(buf, WT(2), 0)
	testing.expect(t, c_err == .None && c == 42, "rightmost readable")

	// Dispatcher resolves with kind .Text; interiors are not leaves.
	layout, kind, r_err := btree.layout_for_page(buf, WT(2))
	testing.expect(t, r_err == .None, "interior resolves")
	if r_err != .None { return }
	testing.expect_value(t, kind, btree.Key_Kind.Text)
	testing.expect(t, layout.vtable != nil, "vtable never nil")
	n := btree.Node {
		id     = 2,
		data   = buf,
		header = h,
	}
	testing.expect(t, !btree.is_leaf(n), "text interior is interior")

	// Page-1 offset + short buffer.
	p1 := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_interior_page(p1, 1), "init interior page 1")
	testing.expect_value(t, p1[100], u8(btree.Page_Type.TEXT_INTERIOR))
	short := make([]u8, 4, context.temp_allocator)
	testing.expect(t, !btree.init_text_interior_page(short, 2), "short init fails")
}

@(test)
test_text_interior_build_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	keys := [][]u8{enc("apple", 2), enc("applet", 3), enc("application", 5)}
	children := []u32{11, 12, 13, 14}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(buf, 2, keys, children) == .None,
		"interior builds",
	)
	testing.expect_value(t, btree.get_cell_count(buf, 2), 3)

	for i in 0 ..< 4 {
		c, c_err := btree.text_interior_child_at(buf, WT(2), i)
		testing.expect(t, c_err == .None, "child reads")
		testing.expect_value(t, c, children[i])
	}
	_, past_err := btree.text_interior_child_at(buf, WT(2), 4)
	testing.expect(t, past_err == .Cell_Not_Found, "past rightmost fails")

	for i in 0 ..< 3 {
		sep, s_err := btree.text_interior_sep_at(buf, WT(2), i)
		testing.expect(t, s_err == .None, "sep reads")
		testing.expect(t, cell.text_index_compare(sep, keys[i]) == 0, "separator round-trips")
	}
	_, sep_oob := btree.text_interior_sep_at(buf, WT(2), 3)
	testing.expect(t, sep_oob == .Cell_Not_Found, "sep past end fails")
	testing.expect(t, btree.text_validate_interior(buf, WT(2)) == .None, "build validates")

	// Contract violations fail closed with the page untouched.
	bad_n := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_text_interior_page(bad_n, 2), "bad target valid")
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(bad_n, 2, keys, children[:3]) == .Invalid_Bounds,
		"children mismatch fails",
	)
	testing.expect(
		t,
		btree.text_validate_interior(bad_n, WT(2)) == .None,
		"failed build leaves valid page",
	)
	bad_key := [][]u8{enc("b", 1), enc("a", 1)}
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(bad_n, 2, bad_key, []u32{1, 2, 3}) ==
		.Cell_Deserialize_Failed,
		"unsorted fails",
	)
	dup_key := [][]u8{enc("a", 1), enc("a", 1)}
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(bad_n, 2, dup_key, []u32{1, 2, 3}) ==
		.Cell_Deserialize_Failed,
		"duplicate separators rejected",
	)
	garbage := [][]u8{{0x00, 0x01, 0x02}}
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(bad_n, 2, garbage, []u32{1, 2}) ==
		.Cell_Deserialize_Failed,
		"non-key blob rejected",
	)
}

@(test)
test_text_interior_find_vs_oracle :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Upper-bound routing oracle: first separator strictly greater than
	// target (equality skips right); none → rightmost with idx -1.
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	seps := [][]u8{enc("apple", 2), enc("applet", 3), enc("application", 5)}
	children := []u32{11, 12, 13, 14}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(buf, 2, seps, children) == .None,
		"interior builds",
	)

	tt := []string{"aardvark", "apple", "apple", "applet", "applet", "appl", "z", ""}
	tr := []i64{0, 1, 2, 2, 3, 0, 0, 0}
	// Hand-derived oracle rows (child, idx):
	// aardvark->(11,0); apple:1->(11,0); apple:2(==sep)->(12,1);
	// applet:2->(12,1); applet:3(==sep)->(13,2); appl->(11,0);
	// z->(14,-1); ""->(11,0).
	want_c := []u32{11, 11, 12, 12, 13, 11, 14, 11}
	want_i := []int{0, 0, 1, 1, 2, 0, -1, 0}
	for ti in 0 ..< len(tt) {
		tkey := enc(tt[ti], tr[ti])
		// Independent linear oracle (not the page search).
		wi := 0
		for wi < len(seps) && cell.text_index_compare(seps[wi], tkey) <= 0 {
			wi += 1
		}
		want_child := children[wi] if wi < len(seps) else children[len(seps)]
		want_idx := wi if wi < len(seps) else -1
		testing.expect_value(t, want_c[ti], want_child)
		testing.expect_value(t, want_i[ti], want_idx)

		got_c, got_i := btree.text_interior_find_child(buf, 2, tkey)
		testing.expectf(
			t,
			got_c == want_c[ti] && got_i == want_i[ti],
			"target %s:%d -> (%d,%d), want (%d,%d)",
			tt[ti],
			tr[ti],
			got_c,
			got_i,
			want_c[ti],
			want_i[ti],
		)
	}

	// Empty interior routes everything to its only child.
	ebuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(ebuf, 2, [][]u8{}, []u32{7}) == .None,
		"empty builds",
	)
	ec, ei := btree.text_interior_find_child(ebuf, 2, enc("anything", 1))
	testing.expect(t, ec == 7 && ei == -1, "empty routes to rightmost")
}

@(test)
test_text_interior_table_ops :: proc(t: ^testing.T) {
	// Interior table through the dispatcher: child_at is real and agrees,
	// everything Row_ID-keyed refuses, validate agrees and refuses foreign.
	context.logger.lowest_level = .Error
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	keys := [][]u8{enc("m", 1), enc("z", 2)}
	children := []u32{21, 22, 23}
	page := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(page, 2, keys, children) == .None,
		"interior builds",
	)
	pid := WT(2)
	layout, kind, l_err := btree.layout_for_page(page, pid)
	testing.expect(t, l_err == .None, "interior resolves")
	if l_err != .None { return }
	testing.expect_value(t, kind, btree.Key_Kind.Text)

	testing.expect_value(t, layout.vtable.cell_count(page, pid), 2)
	for i in 0 ..< 3 {
		c, c_err := layout.vtable.child_at(page, pid, i)
		testing.expect(t, c_err == .None, "table child_at works")
		testing.expect_value(t, c, children[i])
	}
	_, rc_err := layout.vtable.child_at(page, pid, 3)
	testing.expect(t, rc_err == .Cell_Not_Found, "past rightmost fails")
	testing.expect(t, layout.vtable.validate(page, pid) == .None, "validate agrees")

	_, k_err := layout.vtable.key_at(page, pid, 0)
	testing.expect(t, k_err == .Unsupported_Format, "rowid key_at refused")
	testing.expect(
		t,
		layout.vtable.separator_insert(page, pid, 0, 9, 7) == .Unsupported_Format,
		"separator refused until C3b",
	)
	testing.expect(
		t,
		layout.vtable.slot_insert(page, pid, 0, 9, btree.Cell_Off(100)) == .Unsupported_Format,
		"slot insert refused",
	)

	fbuf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(t, btree.init_dense_interior_page(fbuf, 2), "init dense")
	testing.expect(
		t,
		layout.vtable.validate(fbuf, pid) == .Invalid_Page_Header,
		"validate refuses dense bytes",
	)
}

@(test)
test_text_interior_corruption_loud :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	keys := [][]u8{enc("a", 1), enc("b", 2)}
	children := []u32{1, 2, 3}
	buf := make([]u8, types.PAGE_SIZE, context.temp_allocator)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(buf, 2, keys, children) == .None,
		"interior builds",
	)

	// Wrong discriminant: never reinterpreted.
	buf[0] = u8(btree.Page_Type.LEAF_TEXT)
	testing.expect(
		t,
		btree.text_validate_interior(buf, WT(2)) == .Invalid_Page_Header,
		"leaf bytes refused",
	)
	_, s_err := btree.text_interior_sep_at(buf, WT(2), 0)
	testing.expect(t, s_err == .Invalid_Page_Header, "sep on leaf fails")
	buf[0] = u8(btree.Page_Type.TEXT_INTERIOR)

	// Inflated count runs past the buffer.
	hdr := btree.get_leaf_header(buf, 2)
	testing.expect(t, hdr != nil, "header readable")
	if hdr == nil { return }
	hdr.cell_count = u16le(5000)
	testing.expect(
		t,
		btree.text_validate_interior(buf, WT(2)) == .Cell_Deserialize_Failed,
		"absurd count fails",
	)
	// Geometry (not the range check) fires first: 5000 entries cannot
	// address inside an 8KB page.
	_, c_err := btree.text_interior_child_at(buf, WT(2), 2)
	testing.expect(t, c_err == .Cell_Deserialize_Failed, "child on inflated count fails")
	hdr.cell_count = u16le(2)

	// Corrupt separator 0's text byte ("a" -> "c"): ("c",1) sorts after
	// ("b",2) — order break. The borrow shares page bytes, so this edits
	// the page in place.
	sep0, _ := btree.text_interior_sep_at(buf, WT(2), 0)
	testing.expect(t, len(sep0) >= 13, "sep readable")
	if len(sep0) >= 13 {
		sep0[5] = 'c'
		testing.expect(
			t,
			btree.text_validate_interior(buf, WT(2)) == .Cell_Deserialize_Failed,
			"reordered separators rejected",
		)
	}
}

@(test)
test_text_insert_e2e :: proc(t: ^testing.T) {
	// Online inserts through text_insert_cow: 600 unique keys, every key
	// found with its exact rowid, total count exact. Exercises leaf inserts,
	// leaf splits, absorbs, and root growth end to end.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "e2e")
	defer teardown_tree(&ctx)

	N := 600
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("key-%04d", i)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)key,
			types.Row_ID(i64(i)),
		)
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None { return }
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N)

	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("key-%04d", i)
		found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, transmute([]u8)key)
		testing.expect(t, f_err == .None, "find succeeds")
		if f_err != .None { continue }
		testing.expect(t, len(found) == 1, "unique key finds one rowid")
		if len(found) == 1 {
			testing.expect_value(t, found[0], types.Row_ID(i64(i)))
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_text_split_census :: proc(t: ^testing.T) {
	// 1500 rows over 150 texts: splits fire, an interior level forms.
	// Census: every reachable page is text-typed and validates; the count
	// is exact. Mirrors test_v3_split_produces_slotdir.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "census")
	defer teardown_tree(&ctx)

	N := 1500
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("user-%03d", i % 150)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)key,
			types.Row_ID(i64(i)),
		)
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N)

	pages := make(map[u32]bool, context.temp_allocator)
	defer delete(pages)
	btree.collect_pages(&ctx.tree, ctx.tree.root, &pages)
	testing.expect(t, len(pages) > 2, "splits actually happened")
	n_leaf, n_interior := 0, 0
	for page_id in pages {
		pg, pg_err := pager.get_page(ctx.pager, page_id)
		testing.expect(t, pg_err == nil, "page readable")
		if pg_err != nil { continue }
		h := btree.get_header(pg.data, page_id)
		testing.expect(t, h != nil, "header readable")
		if h == nil {
			pager.unpin_page(ctx.pager, page_id)
			continue
		}
		#partial switch h.page_type {
		case .LEAF_TEXT:
			n_leaf += 1
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"split leaf validates",
			)
		case .TEXT_INTERIOR:
			n_interior += 1
			testing.expect(
				t,
				btree.text_validate_interior(pg.data, btree.Page_Id(page_id)) == .None,
				"split interior validates",
			)
		case:
			testing.expect(t, false, "unexpected page type after splits")
		}
		pager.unpin_page(ctx.pager, page_id)
	}
	testing.expect(t, n_leaf >= 2, "multiple text leaves exist")
	testing.expect(t, n_interior >= 1, "text interior level exists")
}

@(test)
test_text_boundary_routing :: proc(t: ^testing.T) {
	// Separator-equality routing on text keys: a key equal to a separator
	// descends RIGHT (exclusive separators); below goes left, above right.
	// Mirrors test_separator_boundary_routing — the B4b1 bug class.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "boundary")
	defer teardown_tree(&ctx)

	enc := proc(s: string, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	mk_leaf := proc(
		t: ^testing.T,
		ctx: ^Test_Context,
		texts: [][]u8,
		rids: []types.Row_ID,
	) -> u32 {
		pg, a_err := pager.allocate_page(ctx.pager)
		testing.expect(t, a_err == nil, "alloc leaf")
		if a_err != nil { return 0 }
		defer pager.unpin_page(ctx.pager, pg.page_num)
		testing.expect(
			t,
			btree.text_build_from_sorted(pg.data, btree.Page_Id(pg.page_num), texts, rids) ==
			.None,
			"leaf builds",
		)
		pager.mark_dirty(ctx.pager, pg.page_num)
		return pg.page_num
	}

	left := mk_leaf(t, &ctx, [][]u8{{'a'}, {'l'}, {'m'}}, []types.Row_ID{1, 2, 4})
	right := mk_leaf(t, &ctx, [][]u8{{'m'}, {'z'}}, []types.Row_ID{6, 7})
	testing.expect(t, left != 0 && right != 0, "leaves allocated")
	if left == 0 || right == 0 { return }

	// Separator is right's first key: ("m",6).
	ipg, a_err := pager.allocate_page(ctx.pager)
	testing.expect(t, a_err == nil, "alloc interior")
	if a_err != nil { return }
	defer pager.unpin_page(ctx.pager, ipg.page_num)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(
			ipg.data,
			btree.Page_Id(ipg.page_num),
			[][]u8{enc("m", 6)},
			[]u32{left, right},
		) ==
		.None,
		"interior builds",
	)
	pager.mark_dirty(ctx.pager, ipg.page_num)
	ctx.tree.root = ipg.page_num

	// ("m",6) equal to separator -> right; ("m",4)/("m",5) -> left.
	at, _ := btree.text_interior_find_child(ipg.data, ipg.page_num, enc("m", 6))
	testing.expect(t, at == right, "separator-equal routes right")
	below, _ := btree.text_interior_find_child(ipg.data, ipg.page_num, enc("m", 4))
	testing.expect(t, below == left, "below separator routes left")
	above, _ := btree.text_interior_find_child(ipg.data, ipg.page_num, enc("m", 7))
	testing.expect(t, above == right, "above separator routes right")

	// Multi-leaf collection: ("m",4) left and ("m",6) right both match.
	mrows, m_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'m'})
	testing.expect(t, m_err == .None, "find m succeeds")
	testing.expect(t, len(mrows) == 2, "m finds both rowids across leaves")
	if len(mrows) == 2 {
		testing.expect_value(t, mrows[0], types.Row_ID(4))
		testing.expect_value(t, mrows[1], types.Row_ID(6))
	}
}

@(test)
test_text_dup_texts :: proc(t: ^testing.T) {
	// Duplicate texts, distinct rowids: all returned in index order.
	// Single leaf (no splits) — cross-leaf duplicates are a documented
	// C3 limit, owned by D's full-range scans.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "dups")
	defer teardown_tree(&ctx)

	inserts := []string{"dup", "dup", "dup", "dup", "dup", "aaa", "zzz"}
	rids := []i64{3, 1, 5, 2, 4, 6, 7}
	for i in 0 ..< len(inserts) {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)inserts[i],
			types.Row_ID(rids[i]),
		)
		testing.expect(t, ins_err == .None, "insert succeeds")
		if ins_err != .None { return }
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'d', 'u', 'p'})
	testing.expect(t, f_err == .None, "find succeeds")
	testing.expect(t, len(found) == 5, "all duplicates found")
	want := []types.Row_ID{1, 2, 3, 4, 5}
	for i in 0 ..< min(len(found), 5) {
		testing.expect_value(t, found[i], want[i])
	}

	missing, m_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'n', 'o', 'p', 'e'})
	testing.expect(t, m_err == .None && len(missing) == 0, "missing finds nothing")
	free_all(context.temp_allocator)
}

@(test)
test_text_absorb_overflow :: proc(t: ^testing.T) {
	// Absorb into a full interior forces the overflow split directly
	// (no deep tree needed): halves validate, all 301 separators present
	// in order across the halves.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "absorb")
	defer teardown_tree(&ctx)

	// 151 separators (4089/4096 bytes: full) + 152 real (empty) children.
	// Absorbing one more forces the overflow split. Sizing is exact:
	// 12 + 27n bytes for 19-byte keys (n=151 fits, n=152 does not).
	N := 151
	seps := make([][]u8, N, context.temp_allocator)
	for i in 0 ..< N {
		b := make([]u8, 32, context.temp_allocator)
		s := fmt.tprintf("k-%04d", i)
		n, ok := cell.text_index_encode(types.value_text(s), types.Row_ID(i64(i)), b)
		assert(ok)
		seps[i] = b[:n]
	}
	children := make([]u32, N + 1, context.temp_allocator)
	for i in 0 ..< N + 1 {
		pg, a_err := pager.allocate_page(ctx.pager)
		testing.expect(t, a_err == nil, "alloc child")
		if a_err != nil { return }
		defer pager.unpin_page(ctx.pager, pg.page_num)
		testing.expect(t, btree.init_text_leaf_page(pg.data, pg.page_num), "init child")
		pager.mark_dirty(ctx.pager, pg.page_num)
		children[i] = pg.page_num
	}
	ipg, a_err := pager.allocate_page(ctx.pager)
	testing.expect(t, a_err == nil, "alloc interior")
	if a_err != nil { return }
	defer pager.unpin_page(ctx.pager, ipg.page_num)
	testing.expect(
		t,
		btree.text_interior_build_from_sorted(
			ipg.data,
			btree.Page_Id(ipg.page_num),
			seps,
			children,
		) ==
		.None,
		"full interior builds",
	)
	pager.mark_dirty(ctx.pager, ipg.page_num)

	// Manual Node over the pinned page (absorb never touches layout).
	// No load/unpin dance: the page is already pinned by allocate.
	ihdr := btree.get_header(ipg.data, ipg.page_num)
	testing.expect(t, ihdr != nil, "interior header readable")
	if ihdr == nil { return }
	node := btree.Node {
		id     = ipg.page_num,
		data   = ipg.data,
		header = ihdr,
	}

	// Split halves must be loadable: the overflow path recounts through
	// them (count_recursive), so they start as empty text leaves.
	lp, lp_err := pager.allocate_page(ctx.pager)
	testing.expect(t, lp_err == nil, "alloc left half page")
	if lp_err != nil { return }
	defer pager.unpin_page(ctx.pager, lp.page_num)
	testing.expect(t, btree.init_text_leaf_page(lp.data, lp.page_num), "init left")
	rp, rp_err := pager.allocate_page(ctx.pager)
	testing.expect(t, rp_err == nil, "alloc right half page")
	if rp_err != nil { return }
	defer pager.unpin_page(ctx.pager, rp.page_num)
	testing.expect(t, btree.init_text_leaf_page(rp.data, rp.page_num), "init right")
	nb := make([]u8, 32, context.temp_allocator)
	nn, _ := cell.text_index_encode(types.value_text("k-0074a"), types.Row_ID(999), nb)
	child_result := btree.Text_Insert_Result {
		new_page   = lp.page_num,
		did_split  = true,
		right_page = rp.page_num,
		split_key  = nb[:nn],
	}
	res, ab_err := btree.text_absorb_child_split(
		&ctx.tree,
		&node,
		&child_result,
		false,
		75,
		ipg.page_num,
	)
	testing.expect(t, ab_err == .None, "absorb succeeds")
	testing.expect(t, res.did_split, "full interior overflows into split")
	if !res.did_split { return }
	testing.expect(
		t,
		btree.text_validate_interior(node.data, btree.Page_Id(node.id)) == .None,
		"left half validates",
	)
	rpg, rpg_err := pager.get_page(ctx.pager, res.right_page)
	testing.expect(t, rpg_err == nil, "load right half")
	if rpg_err != nil { return }
	defer pager.unpin_page(ctx.pager, res.right_page)
	testing.expect(
		t,
		btree.text_validate_interior(rpg.data, btree.Page_Id(res.right_page)) == .None,
		"right half validates",
	)

	// All 301 separators present in order across the halves.
	rhdr := btree.get_header(rpg.data, res.right_page)
	testing.expect(t, rhdr != nil, "right header readable")
	if rhdr == nil { return }
	// Absorbed N+1 across the halves, minus the promoted separator (it
	// lives in the parent result, like every B-tree split).
	total := int(node.header.cell_count) + int(rhdr.cell_count)
	testing.expect_value(t, total, N)
	prev: []u8 = nil
	check := proc(t: ^testing.T, data: []u8, id: u32, prev: ^[]u8) {
		n := btree.get_cell_count(data, id)
		for i in 0 ..< n {
			s, s_err := btree.text_interior_sep_at(data, btree.Page_Id(id), i)
			testing.expect(t, s_err == .None, "sep reads")
			if s_err != .None { continue }
			if prev^ != nil {
				testing.expect(
					t,
					cell.text_index_compare(prev^, s) < 0,
					"separators ordered across halves",
				)
			}
			prev^ = s
		}
	}
	check(t, node.data, node.id, &prev)
	check(t, rpg.data, res.right_page, &prev)
	free_all(context.temp_allocator)
}

// setup_text_tree mirrors setup_tree with a LEAF_TEXT root: the harness
// for C3b chain tests (D wires real index trees through the catalog).
setup_text_tree :: proc(t: ^testing.T, name: string) -> Test_Context {
	context.logger.lowest_level = .Error
	// Owned filename (see setup_tree): temp strings dangle across the
	// test's own free_all calls; teardown_tree owns and deletes this.
	filename, _ := strings.clone(fmt.tprintf("test_text_%s.db", name), context.allocator)
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
		testing.fail_now(t, "FATAL: Failed to allocate root page")
	}
	if pg1.page_num != 1 {
		_ = pager.close(p)
		testing.fail_now(t, fmt.tprintf("FATAL: Allocated page was %d, expected 1", pg1.page_num))
	}
	if !btree.init_text_leaf_page(pg1.data, pg1.page_num) {
		_ = pager.close(p)
		testing.fail_now(t, "FATAL: Failed to init text root page")
	}

	tree_inst := btree.init(p, 1)
	return Test_Context{pager = p, tree = tree_inst, filename = filename}
}

@(test)
test_text_dup_across_leaves :: proc(t: ^testing.T) {
	// 500 identical texts MUST span leaves (leaf capacity ~300 for tiny
	// entries): find returns all 500 in order. Regression test for the
	// rightmost-sentinel advance bug (revisit loop) and the leaf-local
	// scan limit it replaced — separators routinely equal run members.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "dupspan")
	defer teardown_tree(&ctx)

	N := 500
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.text_insert_cow(&ctx.tree, []u8{'x'}, types.Row_ID(i64(i)))
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	ar, ar_err := btree.text_insert_cow(&ctx.tree, []u8{'a'}, types.Row_ID(1000))
	testing.expect(t, ar_err == .None, "insert a succeeds")
	if ar_err == .None { ctx.tree.root = ar }
	zr, zr_err := btree.text_insert_cow(&ctx.tree, []u8{'z'}, types.Row_ID(1001))
	testing.expect(t, zr_err == .None, "insert z succeeds")
	if zr_err == .None { ctx.tree.root = zr }
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N + 2)

	found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'x'})
	testing.expect(t, f_err == .None, "find succeeds")
	testing.expect_value(t, len(found), N)
	for i in 0 ..< min(len(found), N) {
		testing.expect_value(t, found[i], types.Row_ID(i64(i + 1)))
	}

	afound, _ := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'a'})
	testing.expect(t, len(afound) == 1 && afound[0] == 1000, "a finds its rowid")
	free_all(context.temp_allocator)
}

@(test)
test_text_delete_basic :: proc(t: ^testing.T) {
	// Delete half of 60 unique keys (Direct mode, in place): the deleted
	// vanish, the kept resolve, the count is exact, every page validates.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "delbasic")
	defer teardown_tree(&ctx)

	N := 60
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("del-%03d", i)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)key,
			types.Row_ID(i64(i)),
		)
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	for i in 2 ..= N {
		if i % 2 != 0 { continue }
		free_all(context.temp_allocator)
		key := fmt.tprintf("del-%03d", i)
		testing.expect(
			t,
			btree.text_delete(&ctx.tree, transmute([]u8)key, types.Row_ID(i64(i))) == .None,
			"delete succeeds",
		)
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N / 2)

	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("del-%03d", i)
		found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, transmute([]u8)key)
		testing.expect(t, f_err == .None, "find succeeds")
		if i % 2 == 0 {
			testing.expect(t, len(found) == 0, "deleted key finds nothing")
		} else if len(found) == 1 {
			testing.expect_value(t, found[0], types.Row_ID(i64(i)))
		} else {
			testing.expect(t, false, "kept key finds its rowid")
		}
	}
	free_all(context.temp_allocator)

	pages := make(map[u32]bool, context.temp_allocator)
	defer delete(pages)
	btree.collect_pages(&ctx.tree, ctx.tree.root, &pages)
	for page_id in pages {
		pg, pg_err := pager.get_page(ctx.pager, page_id)
		if pg_err != nil { continue }
		h := btree.get_header(pg.data, page_id)
		if h == nil {
			pager.unpin_page(ctx.pager, page_id)
			continue
		}
		if h.page_type == .LEAF_TEXT {
			testing.expect(
				t,
				btree.text_validate_leaf(pg.data, btree.Page_Id(page_id)) == .None,
				"post-delete leaf validates",
			)
		}
		pager.unpin_page(ctx.pager, page_id)
	}
}

@(test)
test_text_delete_missing :: proc(t: ^testing.T) {
	// Absent keys fail closed without touching the tree: unknown text,
	// and — the exact-match rule — known text with another rowid.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "delmiss")
	defer teardown_tree(&ctx)

	for i in 1 ..= 5 {
		free_all(context.temp_allocator)
		key := fmt.tprintf("m-%d", i)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)key,
			types.Row_ID(i64(i) * 10),
		)
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	testing.expect(
		t,
		btree.text_delete(&ctx.tree, []u8{'n', 'o', 'p', 'e'}, 1) == .Cell_Not_Found,
		"unknown text fails",
	)
	testing.expect(
		t,
		btree.text_delete(&ctx.tree, []u8{'m', '-', '3'}, 999) == .Cell_Not_Found,
		"known text, wrong rowid fails",
	)

	// Original intact after the failed deletes.
	found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'m', '-', '3'})
	testing.expect(t, f_err == .None && len(found) == 1 && found[0] == 30, "row intact")
	cnt, _ := btree.tree_count_rows(&ctx.tree)
	testing.expect_value(t, cnt, 5)
	free_all(context.temp_allocator)
}

@(test)
test_text_delete_cow :: proc(t: ^testing.T) {
	// COW delete forks the root: the new tree misses the row, the old
	// tree keeps it. Mirrors test_tree_update_cow.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "delcow")
	defer teardown_tree(&ctx)

	for i in 1 ..= 10 {
		free_all(context.temp_allocator)
		key := fmt.tprintf("c-%d", i)
		new_root, ins_err := btree.text_insert_cow(
			&ctx.tree,
			transmute([]u8)key,
			types.Row_ID(i64(i)),
		)
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)
	old_root := ctx.tree.root

	new_root, del_err := btree.text_delete_cow(&ctx.tree, []u8{'c', '-', '5'}, 5)
	testing.expect(t, del_err == .None, "cow delete succeeds")
	if del_err != .None { return }

	gone, _ := btree.text_find_rowids(&ctx.tree, new_root, []u8{'c', '-', '5'})
	testing.expect(t, len(gone) == 0, "new root misses deleted row")
	kept, _ := btree.text_find_rowids(&ctx.tree, new_root, []u8{'c', '-', '4'})
	testing.expect(t, len(kept) == 1 && kept[0] == 4, "new root keeps others")

	old, _ := btree.text_find_rowids(&ctx.tree, old_root, []u8{'c', '-', '5'})
	testing.expect(t, len(old) == 1 && old[0] == 5, "old root preserves row")
	free_all(context.temp_allocator)
}

@(test)
test_text_delete_dup_texts :: proc(t: ^testing.T) {
	// Delete one member of a duplicate run: siblings survive in order
	// (exact (text,rowid) match — never a text wipe).
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "deldup")
	defer teardown_tree(&ctx)

	dup_rids := []i64{1, 2, 3, 4}
	for r in dup_rids {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.text_insert_cow(&ctx.tree, []u8{'d'}, types.Row_ID(r))
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return
		}
		ctx.tree.root = new_root
	}
	free_all(context.temp_allocator)

	testing.expect(
		t,
		btree.text_delete(&ctx.tree, []u8{'d'}, 2) == .None,
		"delete one dup succeeds",
	)
	found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, []u8{'d'})
	testing.expect(t, f_err == .None, "find succeeds")
	testing.expect_value(t, len(found), 3)
	want := []types.Row_ID{1, 3, 4}
	for i in 0 ..< min(len(found), 3) {
		testing.expect_value(t, found[i], want[i])
	}
	free_all(context.temp_allocator)
}

@(test)
test_text_insert_direct :: proc(t: ^testing.T) {
	// Non-COW entry: 100 sequential inserts in place (root id stable),
	// every key found with its exact rowid. Covers text_insert, the only
	// non-COW text writer (dml Direct fan-out is dormant, but the entry
	// itself is live API and must not rot).
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "insertdirect")
	defer teardown_tree(&ctx)

	N := 100
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("dkey-%04d", i)
		testing.expect(
			t,
			btree.text_insert(&ctx.tree, transmute([]u8)key, types.Row_ID(i64(i))) == .None,
			"direct insert succeeds",
		)
	}
	free_all(context.temp_allocator)

	cnt, c_err := btree.tree_count_rows(&ctx.tree)
	testing.expect(t, c_err == .None, "count succeeds")
	testing.expect_value(t, cnt, N)
	for i in 1 ..= N {
		free_all(context.temp_allocator)
		key := fmt.tprintf("dkey-%04d", i)
		found, f_err := btree.text_find_rowids(&ctx.tree, ctx.tree.root, transmute([]u8)key)
		testing.expect(t, f_err == .None, "find succeeds")
		testing.expect(t, len(found) == 1, "unique key finds one rowid")
		if len(found) == 1 {
			testing.expect_value(t, found[0], types.Row_ID(i64(i)))
		}
	}
	free_all(context.temp_allocator)
}

@(test)
test_text_find_prefix_vs_oracle :: proc(t: ^testing.T) {
	// Prefix scan oracle: 150 padded "alpha%03d" texts (multi-leaf by
	// volume) plus boundary neighbors. Every prefix query matches an
	// independent linear filter over the inserted pairs, in (text,rowid)
	// order — the index's own sort order.
	context.logger.lowest_level = .Error
	ctx := setup_text_tree(t, "prefixscan")
	defer teardown_tree(&ctx)

	inserted_texts := make([dynamic]string, 0, 160, context.allocator)
	defer {
		for s in inserted_texts { delete(s, context.allocator) }
		delete(inserted_texts)
	}
	inserted_rids := make([dynamic]i64, 0, 160, context.allocator)
	defer delete(inserted_rids)
	next_rid := i64(1)
	insert_one :: proc(t: ^testing.T, ctx: ^Test_Context, s: string, rid: i64) -> bool {
		free_all(context.temp_allocator)
		new_root, ins_err := btree.text_insert_cow(&ctx.tree, transmute([]u8)s, types.Row_ID(rid))
		if ins_err != .None {
			testing.expect(t, false, "insert succeeds")
			return false
		}
		ctx.tree.root = new_root
		return true
	}
	for i in 1 ..= 150 {
		free_all(context.temp_allocator)
		s := fmt.tprintf("alpha%03d", i)
		if !insert_one(t, &ctx, s, next_rid) { return }
		append(&inserted_texts, strings.clone(s, context.allocator))
		append(&inserted_rids, next_rid)
		next_rid += 1
	}
	extra := []string{"alpha", "alphabet", "alpine", "beta", "b", "", "alph"}
	for s in extra {
		if !insert_one(t, &ctx, s, next_rid) { return }
		append(&inserted_texts, strings.clone(s, context.allocator))
		append(&inserted_rids, next_rid)
		next_rid += 1
	}
	free_all(context.temp_allocator)

	has_prefix :: proc(s, pre: string) -> bool {
		if len(s) < len(pre) { return false }
		return s[:len(pre)] == pre
	}
	pres := []string{"alpha", "alph", "alpha1", "alpha150", "b", "beta", "z", "alphabetical"}
	for pre in pres {
		free_all(context.temp_allocator)
		// Independent oracle: filter + insertion-sort by (text,rowid).
		want_t := make([dynamic]string, 0, 8, context.temp_allocator)
		want_r := make([dynamic]i64, 0, 8, context.temp_allocator)
		for i in 0 ..< len(inserted_texts) {
			if has_prefix(inserted_texts[i], pre) {
				append(&want_t, inserted_texts[i])
				append(&want_r, inserted_rids[i])
			}
		}
		for i in 1 ..< len(want_t) {
			for j := i; j > 0; j -= 1 {
				if want_t[j] < want_t[j - 1] ||
				   (want_t[j] == want_t[j - 1] && want_r[j] < want_r[j - 1]) {
					want_t[j], want_t[j - 1] = want_t[j - 1], want_t[j]
					want_r[j], want_r[j - 1] = want_r[j - 1], want_r[j]
				} else {
					break
				}
			}
		}

		got, g_err := btree.text_find_prefix(&ctx.tree, ctx.tree.root, transmute([]u8)pre)
		testing.expect(t, g_err == .None, "prefix find succeeds")
		if g_err != .None { continue }
		testing.expectf(
			t,
			len(got) == len(want_r),
			"prefix %q: got %d rowids, want %d",
			pre,
			len(got),
			len(want_r),
		)
		for i in 0 ..< min(len(got), len(want_r)) {
			testing.expectf(
				t,
				got[i] == types.Row_ID(want_r[i]),
				"prefix %q [%d]: got %d, want %d",
				pre,
				i,
				i64(got[i]),
				want_r[i],
			)
		}
	}

	// Empty prefix matches everything only by router contract — the btree
	// honors it literally (full ordered scan), proving no hidden guard.
	free_all(context.temp_allocator)
	all, a_err := btree.text_find_prefix(&ctx.tree, ctx.tree.root, []u8{})
	testing.expect(t, a_err == .None, "empty prefix succeeds")
	if a_err == .None {
		testing.expect_value(t, len(all), len(inserted_texts))
	}
}
