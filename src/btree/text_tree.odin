// Text secondary-index insert chain: online COW inserts over
// single-kind text trees (LEAF_TEXT leaves + TEXT_INTERIOR interiors).
// Mirrors the primary chain (tree/split/cow) with codec-key routing —
// primary paths are untouched. DML fan-out drives text_insert_cow; tests drive it directly.
//
// Differences from the primary chain (deliberate, each documented):
// - No kind dispatch: a text tree holds only text pages. Per-proc type
//   checks fail closed on foreign bytes (never reinterpreted).
// - Absorb-then-split: the pending key joins the snapshot arrays BEFORE
//   the split (sorted-insert), so no retry-insert path exists. Fewer page
//   writes than split-then-retry, same result.
// - Prefix-divergent inserts rebuild the page with a shrunk prefix in
//   place (byte totals are prefix-invariant, so a fitting page still fits).
//   Splits recompute each half's prefix via the builder.
// - Split points are byte-balanced (variable-length entries), not count
//   midpoints like dense_split_mid.
// - text_find_rowids is leaf-local: duplicates spanning a leaf boundary
//   are missed.
package btree

import "core:bytes"
import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:pager"
import "src:types"

Text_Insert_Result :: struct #all_or_none {
	new_page  : u32,
	did_split : bool,
	right_page: u32,
	// split_key is a temp-allocator codec copy owned by the splitter,
	// consumed by the parent absorb within the same statement.
	split_key : []u8,
}

// text_full_compare orders full (text,rowid) pairs: text BINARY, then
// rowid numeric. The temp-array twin of text_entry_compare (which orders
// prefix-shared suffixes); both reduce to the codec order.
@(private = "file")
text_full_compare :: #force_inline proc "contextless" (
	a: []u8,
	ar: types.Row_ID,
	b: []u8,
	br: types.Row_ID,
) -> int {
	if r := mem.compare(a, b); r != 0 {
		return r
	}
	if ar == br {
		return 0
	}
	return -1 if ar < br else 1
}

// text_split_mid is the first index with cumulative bytes >= half the
// total, always in [1, n-1] (both halves non-empty). Callers guarantee
// n >= 2 (a lone entry never splits — it either fits or is Page_Full).
@(private = "file")
text_split_mid :: proc "contextless" (sizes: []int, total: int) -> int {
	n := len(sizes)
	cum := 0
	for i in 0 ..< n - 1 {
		cum += sizes[i]
		if cum * 2 >= total {
			return i + 1
		}
	}
	return n - 1
}

// text_make_key encodes one index key into a temp buffer (caller-owned
// for the statement — same lifetime as the dense path's temp arrays).
@(private, require_results)
text_make_key :: proc(text: []u8, rowid: types.Row_ID) -> ([]u8, Error) {
	buf := make([]u8, 5 + len(text) + 8, context.temp_allocator)
	n, ok := cell.text_index_encode(types.value_text(string(text)), rowid, buf)
	if !ok {
		return nil, .Serialization_Failed
	}
	return buf[:n], .None
}

// text_snapshot_with_pending snapshots a leaf's entries as full texts
// (temp concats — page borrows die on rebuild) with the pending key
// sorted-inserted. Feeds splits, root-splits, and prefix-shrink rebuilds.
@(private = "file", require_results)
text_snapshot_with_pending :: proc(
	data: []u8,
	id: Page_Id,
	text: []u8,
	rowid: types.Row_ID,
) -> (
	fulls: [dynamic][]u8,
	rids: [dynamic]types.Row_ID,
	err: Error,
) {
	prefix, p_err := text_prefix(data, id)
	if p_err != .None {
		return {}, {}, p_err
	}

	hdr := get_header(data, u32(id))
	if hdr == nil {
		return {}, {}, .Invalid_Page_Header
	}

	count := int(hdr.cell_count)
	fulls = make([dynamic][]u8, 0, count + 1, context.temp_allocator)
	rids = make([dynamic]types.Row_ID, 0, count + 1, context.temp_allocator)
	for i in 0 ..< count {
		suf, rid, k_err := text_entry_at(data, id, i)
		if k_err != .None {
			return {}, {}, k_err
		}

		full := make([]u8, len(prefix) + len(suf), context.temp_allocator)
		copy(full, prefix)
		copy(full[len(prefix):], suf)
		append(&fulls, full)
		append(&rids, rid)
	}

	// Sorted insert of the pending key (arrays are sorted; binary search
	// the slot, shift right — same memmove pattern as slot_insert).
	lo, hi := 0, len(fulls)
	for lo < hi {
		mid := lo + (hi - lo) / 2
		if text_full_compare(fulls[mid], rids[mid], text, rowid) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	full_text := make([]u8, len(text), context.temp_allocator)
	copy(full_text, text)
	inject_at(&fulls, lo, full_text)
	inject_at(&rids, lo, rowid)
	return fulls, rids, .None
}

// text_node_insert_leaf_cell inserts one entry into a text leaf, splitting
// nothing (callers own Page_Full). Mirrors node_insert_leaf_cell with no
// duplicate check (non-UNIQUE index —
// duplicate texts with distinct rowids are legal; exact (text,rowid)
// duplicates are caller bugs the validator rejects loudly).
@(require_results)
text_node_insert_leaf_cell :: proc(t: ^Tree, n: ^Node, text: []u8, rowid: types.Row_ID) -> Error {
	if !is_leaf(n^) {
		return .Invalid_Page_Header
	}
	if n.header.page_type != .LEAF_TEXT {
		return .Invalid_Page_Header
	}

	prefix, p_err := text_prefix(n.data, Page_Id(n.id))
	if p_err != .None {
		return p_err
	}
	// Prefix-divergent key: rebuild the page around the shrunk prefix.
	// Byte totals are prefix-invariant, so a fitting page still fits —
	// this never converts .None into .Page_Full, only re-lays-out.
	if !cell.text_index_has_prefix(text, prefix) {
		fulls, rids, s_err := text_snapshot_with_pending(n.data, Page_Id(n.id), text, rowid)
		if s_err != .None {
			return s_err
		}
		if b_err := text_build_from_sorted(n.data, Page_Id(n.id), fulls[:], rids[:]);
		   b_err != .None {
			return b_err
		}

		pager.mark_dirty(t.pager, n.id)
		invalidate_page_int_range(t, n.id)
		return .None
	}

	idx, lb_err := text_lower_bound(n.data, Page_Id(n.id), text, rowid)
	if lb_err != .None {
		return lb_err
	}

	elen := 8 + (len(text) - len(prefix))
	free_off := freeblock_alloc(
		n.data,
		n.header.first_freeblock,
		u16(elen),
		&n.header.first_freeblock,
	)
	if free_off != 0 {
		if int(free_off) + elen > len(n.data) {
			return .Cell_Deserialize_Failed
		}
		if !endian.put_u64(n.data[int(free_off):], .Big, rowid_bias_encode(rowid)) {
			return .Serialization_Failed
		}

		copy(n.data[int(free_off) + 8:int(free_off) + elen], text[len(prefix):])
		if s_err := text_slot_insert(n.data, Page_Id(n.id), idx, int(free_off), elen);
		   s_err != .None {
			return s_err
		}

		n.header.cell_count += 1
		pager.mark_dirty(t.pager, n.id)
		invalidate_page_int_range(t, n.id)
		return .None
	}

	entry_end, e_err := text_entry_area_end(n.data, Page_Id(n.id))
	if e_err != .None {
		return e_err
	}
	if entry_end + size_of(Text_Slot) >= int(n.header.cell_content_offset) {
		return .Page_Full
	}
	if elen > int(n.header.cell_content_offset) - (entry_end + size_of(Text_Slot)) {
		return .Page_Full
	}

	new_offset := int(n.header.cell_content_offset) - elen
	if !endian.put_u64(n.data[new_offset:], .Big, rowid_bias_encode(rowid)) {
		return .Serialization_Failed
	}

	copy(n.data[new_offset + 8:new_offset + elen], text[len(prefix):])
	if s_err := text_slot_insert(n.data, Page_Id(n.id), idx, new_offset, elen); s_err != .None {
		return s_err
	}

	n.header.cell_count += 1
	n.header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(t.pager, n.id)
	invalidate_page_int_range(t, n.id)
	return .None
}

// text_insert_into_leaf inserts, absorbing the pending key into the split:
// snapshot (sorted) + byte-balanced mid + rebuild halves. No retry path
// (deviation from split-then-retry — fewer page writes, same result).
@(private = "file", require_results)
text_insert_into_leaf :: proc(
	t: ^Tree,
	curr: ^Node,
	text: []u8,
	rowid: types.Row_ID,
	tkey: []u8,
	new_page_num: u32,
) -> (
	Text_Insert_Result,
	Error,
) {
	e := text_node_insert_leaf_cell(t, curr, text, rowid)
	if e == .Page_Full {
		fulls, rids, s_err := text_snapshot_with_pending(curr.data, Page_Id(curr.id), text, rowid)

		if s_err != .None {
			return {}, s_err
		}
		if len(fulls) < 2 {
			return {}, .Page_Full
		}

		sizes := make([dynamic]int, 0, len(fulls), context.temp_allocator)
		total := 0
		for i in 0 ..< len(fulls) {
			sz := TEXT_ENTRY_ROWID_LEN + len(fulls[i])
			append(&sizes, sz)
			total += sz
		}

		mid := text_split_mid(sizes[:], total)
		if lb_err := text_build_from_sorted(curr.data, Page_Id(curr.id), fulls[:mid], rids[:mid]);
		   lb_err != .None {
			return {}, lb_err
		}

		new_page, a_err := pager.allocate_page(t.pager)
		if a_err != nil {
			return {}, .Page_Full
		}

		defer pager.unpin_page(t.pager, new_page.page_num)
		if rb_err := text_build_from_sorted(
			new_page.data,
			Page_Id(new_page.page_num),
			fulls[mid:],
			rids[mid:],
		); rb_err != .None {
			return {}, rb_err
		}

		sep, sep_err := text_make_key(fulls[mid], rids[mid])
		if sep_err != .None {
			return {}, sep_err
		}

		stats_row_count_set(tree_stats(t), curr.id, mid)
		stats_row_count_set(tree_stats(t), new_page.page_num, len(fulls) - mid)

		pager.mark_dirty(t.pager, curr.id)
		pager.mark_dirty(t.pager, new_page.page_num)
		return Text_Insert_Result {
				new_page = curr.id,
				did_split = true,
				right_page = new_page.page_num,
				split_key = sep,
			},
			.None
	}
	if e == .None {
		stats_row_count_set(tree_stats(t), curr.id, int(curr.header.cell_count))
	}
	return Text_Insert_Result {
			new_page = new_page_num,
			did_split = false,
			right_page = 0,
			split_key = nil,
		},
		e
}

// Helpers for the text-interior absorb/split chain below.
// decode_text_interior reads the current separators + children (including
// the rightmost child). Separators are temp-copied: borrows die on rebuild,
// so the absorbed arrays must own their bytes before any build runs.
@(private = "file")
decode_text_interior :: proc(
	curr: ^Node,
) -> (
	seps: [dynamic][]u8,
	children: [dynamic]u32,
	err: Error,
) {
	pid := Page_Id(curr.id)
	n := get_cell_count(curr.data, curr.id)
	seps = make([dynamic][]u8, 0, n + 1, context.temp_allocator)
	children = make([dynamic]u32, 0, n + 2, context.temp_allocator)
	for i in 0 ..< n {
		s, s_err := text_interior_sep_at(curr.data, pid, i)
		if s_err != .None {
			return nil, nil, s_err
		}

		sc := make([]u8, len(s), context.temp_allocator)
		copy(sc, s)
		append(&seps, sc)

		c, c_err := text_interior_child_at(curr.data, pid, i)
		if c_err != .None {
			return nil, nil, c_err
		}
		append(&children, c)
	}

	rc, rc_err := text_interior_child_at(curr.data, pid, n)
	if rc_err != .None {
		return nil, nil, rc_err
	}

	append(&children, rc)
	return seps, children, .None
}

// split_text_halves rebuilds an overfull text interior in place as the left
// half (byte-balanced via text_split_mid) and spills the right half to a
// fresh page. Returns the upward split result.
@(private = "file")
split_text_halves :: proc(
	t: ^Tree,
	curr: ^Node,
	seps: [][]u8,
	children: []u32,
	new_page_num: u32,
) -> (
	Text_Insert_Result,
	Error,
) {
	pid := Page_Id(curr.id)
	m := len(seps)
	sizes := make([dynamic]int, 0, m, context.temp_allocator)
	total := 0
	for i in 0 ..< m {
		append(&sizes, len(seps[i]))
		total += len(seps[i])
	}

	mid := text_split_mid(sizes[:], total)
	if lb_err := text_interior_build_from_sorted(curr.data, pid, seps[:mid], children[:mid + 1]);
	   lb_err != .None {
		return {}, lb_err
	}

	new_page, a_err := pager.allocate_page(t.pager)
	if a_err != nil {
		return {}, .Page_Full
	}

	defer pager.unpin_page(t.pager, new_page.page_num)
	if rb_err := text_interior_build_from_sorted(
		new_page.data,
		Page_Id(new_page.page_num),
		seps[mid + 1:],
		children[mid + 1:],
	); rb_err != .None {
		return {}, rb_err
	}
	if _, c_err := count_recursive(t, curr.id); c_err != .None {
		return {}, c_err
	}
	if _, c_err := count_recursive(t, new_page.page_num); c_err != .None {
		return {}, c_err
	}

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, new_page.page_num)
	return Text_Insert_Result {
			new_page = new_page_num,
			did_split = true,
			right_page = new_page.page_num,
			split_key = seps[mid],
		},
		.None
}

// text_absorb_child_split absorbs a split child into a text interior:
// snapshot all pairs (temp copies — borrows die on rebuild), splice at the
// proven child_idx+1 position (never a raw bound — same rule as the dense
// absorb), rebuild; overflow splits the absorbed arrays directly.
@(require_results)
text_absorb_child_split :: proc(
	t: ^Tree,
	curr: ^Node,
	child_result: ^Text_Insert_Result,
	was_rightmost: bool,
	child_idx: int,
	new_page_num: u32,
) -> (
	Text_Insert_Result,
	Error,
) {
	pid := Page_Id(curr.id)
	n := get_cell_count(curr.data, curr.id)
	seps, children, d_err := decode_text_interior(curr)
	if d_err != .None {
		return {}, d_err
	}

	split_key := child_result.split_key
	if was_rightmost {
		append(&seps, split_key)
		children[n] = child_result.new_page
		append(&children, child_result.right_page)
	} else {
		idx := child_idx
		if idx < 0 || idx >= n {
			return {}, .Invalid_Page_Header
		}

		old_sep := seps[idx]
		seps[idx] = split_key
		children[idx] = child_result.new_page
		nseps := make([dynamic][]u8, 0, len(seps) + 1, context.temp_allocator)
		append(&nseps, ..seps[:idx + 1])
		append(&nseps, old_sep)
		append(&nseps, ..seps[idx + 1:])

		nchildren := make([dynamic]u32, 0, len(children) + 1, context.temp_allocator)
		append(&nchildren, ..children[:idx + 1])
		append(&nchildren, child_result.right_page)
		append(&nchildren, ..children[idx + 1:])

		seps, children = nseps, nchildren
		split_key = old_sep
	}

	if b_err := text_interior_build_from_sorted(curr.data, pid, seps[:], children[:]);
	   b_err == .None {
		update_row_count(t, curr.id, 1)
		pager.mark_dirty(t.pager, curr.id)
		return Text_Insert_Result {
				new_page = new_page_num,
				did_split = false,
				right_page = 0,
				split_key = nil,
			},
			.None
	} else if b_err != .Page_Full {
		return {}, b_err
	}

	return split_text_halves(t, curr, seps[:], children[:], new_page_num)
}

// text_insert_into_interior descends, then repoints or absorbs. Mirrors
// insert_into_interior (COW repoint via indexed store, stats, dirty).
@(private = "file", require_results)
text_insert_into_interior :: proc(
	t: ^Tree,
	curr: ^Node,
	text: []u8,
	rowid: types.Row_ID,
	tkey: []u8,
	cow: bool,
	new_page_num: u32,
) -> (
	Text_Insert_Result,
	Error,
) {
	child_id, child_idx := text_interior_find_child(curr.data, curr.id, tkey)
	was_rightmost := child_idx == -1
	child_result, c_err := text_insert_recursive(t, child_id, text, rowid, tkey, cow)
	if c_err != .None {
		return {}, c_err
	}
	if cow && child_result.new_page != child_id {
		pager.unpin_page(t.pager, child_result.new_page)
	}
	if !child_result.did_split {
		if cow && child_result.new_page != child_id {
			store_idx := child_idx
			if was_rightmost {
				store_idx = get_cell_count(curr.data, curr.id)
			}
			if s_err := text_interior_child_store(
				curr.data,
				Page_Id(curr.id),
				store_idx,
				child_result.new_page,
			); s_err != .None {
				return {}, .Invalid_Cell_Pointer
			}
		}

		update_row_count(t, curr.id, 1)
		pager.mark_dirty(t.pager, curr.id)
		return Text_Insert_Result {
				new_page = new_page_num,
				did_split = false,
				right_page = 0,
				split_key = nil,
			},
			.None
	}

	return text_absorb_child_split(t, curr, &child_result, was_rightmost, child_idx, new_page_num)
}

// text_insert_recursive COW-copies, loads, and dispatches leaf/interior.
// Single-kind tree: no layout dispatch (per-proc type checks own safety).
@(private = "file", require_results)
text_insert_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	text: []u8,
	rowid: types.Row_ID,
	tkey: []u8,
	cow: bool,
) -> (
	result: Text_Insert_Result,
	err: Error,
) {
	new_page_num := page_id
	if cow {
		var, cow_err := copy_on_write(t, page_id)
		if cow_err != .None {
			return {}, cow_err
		}
		new_page_num = var
	}

	curr := load_node(t, new_page_num) or_return
	defer unpin_node(t, curr)
	if is_leaf(curr) {
		return text_insert_into_leaf(t, &curr, text, rowid, tkey, new_page_num)
	}
	return text_insert_into_interior(t, &curr, text, rowid, tkey, cow, new_page_num)
}

// text_split_leaf_root splits a full leaf root with the pending key
// absorbed: snapshot + sorted pending + byte-balanced halves + single-sep
// interior root. Mirrors split_leaf_root without the move/retry dance.
@(require_results)
text_split_leaf_root :: proc(
	t: ^Tree,
	root_page: u32,
	text: []u8,
	rowid: types.Row_ID,
) -> (
	new_root: u32,
	err: Error,
) {
	root_node, load_err := load_node(t, root_page)
	if load_err != .None {
		return 0, load_err
	}

	defer unpin_node(t, root_node)
	if !is_leaf(root_node) {
		return 0, .Invalid_Page_Header
	}
	if root_node.header.page_type != .LEAF_TEXT {
		return 0, .Invalid_Page_Header
	}
	if int(root_node.header.cell_count) == 0 {
		return 0, .Page_Full
	}

	fulls, rids, s_err := text_snapshot_with_pending(
		root_node.data,
		Page_Id(root_page),
		text,
		rowid,
	)
	if s_err != .None {
		return 0, s_err
	}
	if len(fulls) < 2 {
		return 0, .Page_Full
	}

	left_page, l_err := pager.allocate_page(t.pager)
	if l_err != nil {
		return 0, .Page_Full
	}
	defer pager.unpin_page(t.pager, left_page.page_num)

	right_page, r_err := pager.allocate_page(t.pager)
	if r_err != nil {
		return 0, .Page_Full
	}
	defer pager.unpin_page(t.pager, right_page.page_num)

	sizes := make([dynamic]int, 0, len(fulls), context.temp_allocator)
	total := 0
	for i in 0 ..< len(fulls) {
		sz := TEXT_ENTRY_ROWID_LEN + len(fulls[i])
		append(&sizes, sz)
		total += sz
	}

	mid := text_split_mid(sizes[:], total)
	if lb_err := text_build_from_sorted(
		left_page.data,
		Page_Id(left_page.page_num),
		fulls[:mid],
		rids[:mid],
	); lb_err != .None {
		return 0, lb_err
	}
	if rb_err := text_build_from_sorted(
		right_page.data,
		Page_Id(right_page.page_num),
		fulls[mid:],
		rids[mid:],
	); rb_err != .None {
		return 0, rb_err
	}

	sep, sep_err := text_make_key(fulls[mid], rids[mid])
	if sep_err != .None {
		return 0, sep_err
	}
	if rb_err := text_interior_build_from_sorted(
		root_node.data,
		Page_Id(root_page),
		[][]u8{sep},
		[]u32{left_page.page_num, right_page.page_num},
	); rb_err != .None {
		return 0, rb_err
	}

	stats_row_count_set(tree_stats(t), left_page.page_num, mid)
	stats_row_count_set(tree_stats(t), right_page.page_num, len(fulls) - mid)
	if _, c_err := count_recursive(t, root_page); c_err != .None {
		return 0, c_err
	}

	pager.mark_dirty(t.pager, left_page.page_num)
	pager.mark_dirty(t.pager, right_page.page_num)
	pager.mark_dirty(t.pager, root_page)
	return root_page, .None
}

// text_split_interior_root grows a new single-separator interior root over
// the split halves. Mirrors split_interior_root.
@(require_results)
text_split_interior_root :: proc(
	t: ^Tree,
	split: Text_Insert_Result,
) -> (
	new_root: u32,
	err: Error,
) {
	root_node := load_node(t, t.root) or_return
	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		return 0, .Invalid_Page_Header
	}

	rpid := Page_Id(t.root)
	total := int(root_node.header.cell_count)
	seps := make([dynamic][]u8, 0, total, context.temp_allocator)
	children := make([dynamic]u32, 0, total + 1, context.temp_allocator)
	for i in 0 ..< total {
		s, s_err := text_interior_sep_at(root_node.data, rpid, i)
		if s_err != .None {
			return 0, s_err
		}

		sc := make([]u8, len(s), context.temp_allocator)
		copy(sc, s)
		append(&seps, sc)

		c, c_err := text_interior_child_at(root_node.data, rpid, i)
		if c_err != .None {
			return 0, c_err
		}
		append(&children, c)
	}

	rc, rc_err := text_interior_child_at(root_node.data, rpid, total)
	if rc_err != .None {
		return 0, rc_err
	}

	append(&children, rc)
	left_page, a_err := pager.allocate_page(t.pager)
	if a_err != nil {
		return 0, .Page_Full
	}

	defer pager.unpin_page(t.pager, left_page.page_num)
	if lb_err := text_interior_build_from_sorted(
		left_page.data,
		Page_Id(left_page.page_num),
		seps[:],
		children[:],
	); lb_err != .None {
		return 0, lb_err
	}
	if rb_err := text_interior_build_from_sorted(
		root_node.data,
		rpid,
		[][]u8{split.split_key},
		[]u32{left_page.page_num, split.right_page},
	); rb_err != .None {
		return 0, rb_err
	}

	pager.mark_dirty(t.pager, t.root)
	pager.mark_dirty(t.pager, left_page.page_num)
	return t.root, .None
}

// text_insert_cow inserts one (text,rowid) into a text tree, COW. Entry
// point for DML fan-out (Phase D) and C3b tests. Mirrors tree_insert_cow.
@(require_results)
text_insert_cow :: proc(t: ^Tree, text: []u8, rowid: types.Row_ID) -> (new_root: u32, err: Error) {
	tkey, k_err := text_make_key(text, rowid)
	if k_err != .None {
		return 0, k_err
	}

	root_node, load_err := load_node(t, t.root)
	if load_err != .None {
		return 0, load_err
	}

	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		if root_node.header.page_type != .LEAF_TEXT {
			return 0, .Invalid_Page_Header
		}

		new_root, err = copy_on_write(t, t.root)
		if err != .None {
			return 0, err
		}

		cow_node, n_err := load_node(t, new_root)
		if n_err != .None {
			return 0, n_err
		}

		defer unpin_node(t, cow_node)
		e := text_node_insert_leaf_cell(t, &cow_node, text, rowid)
		if e != .Page_Full {
			pager.unpin_page(t.pager, new_root)
			return new_root, e
		}

		result_root, split_err := text_split_leaf_root(t, new_root, text, rowid)
		pager.unpin_page(t.pager, new_root)
		return result_root, split_err
	}

	result, r_err := text_insert_recursive(t, t.root, text, rowid, tkey, true)
	if r_err != .None {
		return 0, r_err
	}

	new_root = result.new_page
	if result.did_split {
		grown, g_err := text_split_interior_root(t, result)
		if g_err != .None {
			return 0, g_err
		}
		new_root = grown
	}

	pager.unpin_page(t.pager, new_root)
	return new_root, .None
}

// text_find_rowids returns every indexed rowid for text, in index order.
// Multi-leaf forward scan over a recorded parent path (same algorithm as
// the primary cursor's descend_to_next_leaf: sibling-or-carry advance,
// drill-down via child 0 — all kind-agnostic index arithmetic on the real
// child_at). REQUIRED for correctness, not an optimization: separators are
// full (text,rowid) keys, so an equal-text run routinely straddles a
// separator (any entry that became a right-first key lives right of a
// separator equal to itself) — leaf-local scans silently lose those rows.
// Advance is strictly rightward, so the loop terminates at the last leaf.
// Text_Path_Item records one interior level during a text-tree descent so a
// leaf scan can advance rightward (sibling-or-carry) across leaf boundaries.
Text_Path_Item :: struct {
	page_id  : u32,
	child_idx: int,
}

// Text_Scan_Mode selects the leaf-run match rule: exact text equality or
// prefix containment. A loop-invariant enum, not a callback: no indirect
// call per entry on this hot path (branch predictor learns the constant).
Text_Scan_Mode :: enum {
	Exact,
	Prefix,
}

// text_concat_full joins a leaf page prefix + entry suffix into the full key.
@(private = "file")
text_concat_full :: proc(pre, suffix: []u8) -> []u8 {
	full := make([]u8, len(pre) + len(suffix), context.temp_allocator)
	copy(full, pre)
	copy(full[len(pre):], suffix)
	return full
}

// text_descend_to_leaf walks from root to the leaf holding key, recording the
// interior path for later rightward advance. Returns the leaf page id.
@(private = "file")
text_descend_to_leaf :: proc(
	t: ^Tree,
	root: u32,
	key: []u8,
	path: ^[dynamic]Text_Path_Item,
) -> (
	leaf_id: u32,
	err: Error,
) {
	curr := root
	leaf_id = 0
	{
		descending := true
		for descending {
			node := load_node(t, curr) or_return
			if is_leaf(node) {
				if node.header.page_type != .LEAF_TEXT {
					unpin_node(t, node)
					return 0, .Invalid_Page_Header
				}

				leaf_id = curr
				unpin_node(t, node)
				descending = false
			} else {
				if node.header.page_type != .TEXT_INTERIOR {
					unpin_node(t, node)
					return 0, .Invalid_Page_Header
				}

				tkey, k_err := text_make_key(key, min(types.Row_ID))
				if k_err != .None {
					unpin_node(t, node)
					return 0, k_err
				}

				child, cidx := text_interior_find_child(node.data, node.id, tkey)
				if child == 0 {
					unpin_node(t, node)
					return 0, .Invalid_Cell_Pointer
				}
				// Map the -1 rightmost sentinel to the real index
				// (cell_count): advance does idx++ and must land past
				// the end, not wrap to child 0 (the primary cursor
				// records cell_count for the same reason).
				rec_idx := cidx
				if rec_idx < 0 {
					rec_idx = get_cell_count(node.data, node.id)
				}

				unpin_node(t, node)
				append(path, Text_Path_Item{page_id = curr, child_idx = rec_idx})
				curr = child
			}
		}
	}
	return leaf_id, .None
}

// text_advance_path moves the recorded path to the next leaf rightward
// (sibling-or-carry), drilling down its leftmost leaf. Returns 0 past the
// last leaf.
@(private = "file")
text_advance_path :: proc(t: ^Tree, path: ^[dynamic]Text_Path_Item) -> (next: u32, ok: bool) {
	for len(path^) > 0 {
		top := &path^[len(path^) - 1]
		top.child_idx += 1
		node, l_err := load_node(t, top.page_id)
		if l_err != .None {
			return 0, false
		}

		n := int(node.header.cell_count)
		if top.child_idx <= n {
			child, c_err := text_interior_child_at(node.data, Page_Id(node.id), top.child_idx)
			if c_err != .None {
				unpin_node(t, node)
				return 0, false
			}

			unpin_node(t, node)
			// Drill down the leftmost spine, recording it.
			curr := child
			for {
				dnode, d_err := load_node(t, curr)
				if d_err != .None {
					return 0, false
				}
				if is_leaf(dnode) {
					unpin_node(t, dnode)
					return curr, true
				}
				if dnode.header.page_type != .TEXT_INTERIOR {
					unpin_node(t, dnode)
					return 0, false
				}

				leftmost, dl_err := text_interior_child_at(dnode.data, Page_Id(dnode.id), 0)
				unpin_node(t, dnode)
				if dl_err != .None {
					return 0, false
				}

				append(path, Text_Path_Item{page_id = curr, child_idx = 0})
				curr = leftmost
			}
		}

		unpin_node(t, node)
		pop(path)
	}
	return 0, true
}

// text_scan_leaf_run collects matching rowids from leaf_id rightward: skips
// below-key entries, collects matches, stops past the run; the run follows
// across boundaries while the last examined entry still orders <= the query.
@(private = "file")
text_scan_leaf_run :: proc(
	t: ^Tree,
	leaf_id: u32,
	path: ^[dynamic]Text_Path_Item,
	key: []u8,
	mode: Text_Scan_Mode,
) -> (
	found: []types.Row_ID,
	err: Error,
) {
	out := make([dynamic]types.Row_ID, 0, 8, context.temp_allocator)
	first_round := true
	curr := leaf_id
	for curr != 0 {
		node := load_node(t, curr) or_return
		page_prefix, p_err := text_prefix(node.data, Page_Id(node.id))
		if p_err != .None {
			unpin_node(t, node)
			return nil, p_err
		}

		count := int(node.header.cell_count)
		start := 0
		if first_round {
			first_round = false
			idx, lb_err := text_lower_bound(node.data, Page_Id(node.id), key, min(types.Row_ID))
			if lb_err != .None {
				unpin_node(t, node)
				return nil, lb_err
			}
			start = idx
		}

		last_le := true
		i := start
		for i < count {
			suf, rid, k_err := text_entry_at(node.data, Page_Id(node.id), i)
			if k_err != .None {
				unpin_node(t, node)
				return nil, k_err
			}

			full := text_concat_full(page_prefix, suf)
			raw := mem.compare(full, key)
			if raw < 0 {
				i += 1
				continue
			}

			hit := raw == 0
			if mode == .Prefix && !hit {
				hit = bytes.has_prefix(full, key)
			}
			if !hit {
				last_le = false
				break
			}

			append(&out, rid)
			i += 1
		}

		unpin_node(t, node)
		if i < count || !last_le {
			break
		}

		next, adv_ok := text_advance_path(t, path)
		if !adv_ok {
			return nil, .Invalid_Cell_Pointer
		}
		curr = next
	}
	return out[:], .None
}

@(require_results)
text_find_rowids :: proc(t: ^Tree, root: u32, text: []u8) -> (found: []types.Row_ID, err: Error) {
	path := make([dynamic]Text_Path_Item, 0, 8, context.temp_allocator)
	leaf_id, d_err := text_descend_to_leaf(t, root, text, &path)
	if d_err != .None {
		return nil, d_err
	}
	return text_scan_leaf_run(t, leaf_id, &path, text, .Exact)
}

// text_find_prefix collects the rowids of every entry whose text starts with
// prefix (BINARY collation). Mirrors text_find_rowids exactly — descend once
// with (prefix, -inf), lower-bound the first leaf, collect while entries
// carry the prefix, stop at the first greater non-prefix entry, sibling-or-
// carry across leaves while the run continues. An entry smaller than the
// prefix can never carry it (any P-prefixed string sorts >= P), and the
// first greater non-prefix entry ends the run (later keys sort higher still).
// Empty prefix matches everything (a full-index scan); the router never
// sends one (LIKE '%' falls back to the scan path), and the btree honors it
// literally rather than second-guessing the caller.
@(require_results)
text_find_prefix :: proc(
	t: ^Tree,
	root: u32,
	prefix: []u8,
) -> (
	found: []types.Row_ID,
	err: Error,
) {
	path := make([dynamic]Text_Path_Item, 0, 8, context.temp_allocator)
	leaf_id, d_err := text_descend_to_leaf(t, root, prefix, &path)
	if d_err != .None {
		return nil, d_err
	}
	return text_scan_leaf_run(t, leaf_id, &path, prefix, .Prefix)
}

// text_node_delete_leaf_cell removes one exact (text,rowid) entry from a
// text leaf. Mirrors delete_from_leaf (no merge — reclamation is
// freeblock/fragmented, vacuum compacts later): lower_bound, exact-match
// verify (suffix + rowid — a neighbor with the same text but another rowid
// must NOT delete), slot shift, count--, cell reclaim. .Cell_Not_Found
// when absent (never a wrong delete).
@(require_results)
text_node_delete_leaf_cell :: proc(
	t: ^Tree,
	leaf_node: ^Node,
	text: []u8,
	rowid: types.Row_ID,
) -> Error {
	if !is_leaf(leaf_node^) {
		return .Invalid_Page_Header
	}
	if leaf_node.header.page_type != .LEAF_TEXT {
		return .Invalid_Page_Header
	}

	prefix, p_err := text_prefix(leaf_node.data, Page_Id(leaf_node.id))
	if p_err != .None {
		return p_err
	}
	if !cell.text_index_has_prefix(text, prefix) {
		return .Cell_Not_Found
	}

	idx, lb_err := text_lower_bound(leaf_node.data, Page_Id(leaf_node.id), text, rowid)
	if lb_err != .None {
		return lb_err
	}

	limit := int(leaf_node.header.cell_count)
	// Exact match: lower_bound lands first >= (text,rowid); entry must
	// equal both parts (same text, other rowid is a neighbor, not a hit).
	delete_idx := -1
	cell_off := 0
	cell_sz := 0
	if idx < limit {
		suf, rid, k_err := text_entry_at(leaf_node.data, Page_Id(leaf_node.id), idx)
		if k_err != .None {
			return k_err
		}

		ts := text[len(prefix):]
		if rid == rowid && len(suf) == len(ts) && mem.compare(suf, ts) == 0 {
			off0 := get_page_header_offset(leaf_node.id)
			slots_start := off0 + TEXT_LEAF_FIXED + len(prefix)
			slot := (^Text_Slot)(raw_data(leaf_node.data[slots_start + idx * size_of(Text_Slot):]))

			delete_idx = idx
			cell_off = int(slot.off)
			cell_sz = int(slot.len)
		}
	}
	if delete_idx == -1 {
		return .Cell_Not_Found
	}
	if delete_idx < limit - 1 {
		if d_err := text_slot_delete(leaf_node.data, Page_Id(leaf_node.id), delete_idx);
		   d_err != .None {
			return d_err
		}
	}

	leaf_node.header.cell_count -= 1
	if cell_off == int(leaf_node.header.cell_content_offset) {
		leaf_node.header.cell_content_offset += u16le(cell_sz)
	} else if cell_sz >= FREEBLOCK_HDR_SIZE {
		freeblock_insert(
			leaf_node.data,
			u16(cell_off),
			u16(cell_sz),
			&leaf_node.header.first_freeblock,
		)
	} else if cell_sz > 0 && cell_sz < 255 {
		leaf_node.header.fragmented_bytes = u8(
			min(u16(leaf_node.header.fragmented_bytes) + u16(cell_sz), 255),
		)
	}

	pager.mark_dirty(t.pager, leaf_node.id)
	invalidate_page_int_range(t, leaf_node.id)
	return .None
}

// text_delete_recursive removes one exact entry, non-COW. Mirrors
// delete_recursive (stats maintained on the way out).
@(private = "file", require_results)
text_delete_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	text: []u8,
	rowid: types.Row_ID,
	tkey: []u8,
) -> (
	bool,
	Error,
) {
	node, err := load_node(t, page_id)
	if err != .None {
		return false, err
	}
	defer unpin_node(t, node)

	if is_leaf(node) {
		e := text_node_delete_leaf_cell(t, &node, text, rowid)
		if e != .None {
			return false, e
		}

		stats_row_count_set(tree_stats(t), page_id, int(node.header.cell_count))
		return true, .None
	}

	child_id, _ := text_interior_find_child(node.data, node.id, tkey)
	deleted, d_err := text_delete_recursive(t, child_id, text, rowid, tkey)
	if d_err != .None {
		return false, d_err
	}
	if deleted {
		update_row_count(t, page_id, -1)
	}
	return deleted, .None
}

// text_delete removes one exact (text,rowid) entry in place (Direct mode).
// Mirrors tree_delete.
@(require_results)
text_delete :: proc(t: ^Tree, text: []u8, rowid: types.Row_ID) -> Error {
	tkey, k_err := text_make_key(text, rowid)
	if k_err != .None {
		return k_err
	}

	_, err := text_delete_recursive(t, t.root, text, rowid, tkey)
	return err
}

// text_delete_cow removes one exact entry copy-on-write. Mirrors
// tree_delete_cow (nested cow-recursive, repoint on change, root dance).
// Unlike the primary COW path it maintains stats (leaf set + interior -1:
// both exact here — the primary path omits them, leaving stale counts
// until recount; no legacy forces the same quirk on a new tree type).
@(require_results)
text_delete_cow :: proc(t: ^Tree, text: []u8, rowid: types.Row_ID) -> (new_root: u32, err: Error) {
	Text_Update_COW_Result :: struct {
		new_page: u32,
	}

	tkey, k_err := text_make_key(text, rowid)
	if k_err != .None {
		return 0, k_err
	}

	delete_cow_recursive :: proc(
		t: ^Tree,
		pid: u32,
		text: []u8,
		rowid: types.Row_ID,
		tkey: []u8,
		cow: bool,
	) -> (
		Text_Update_COW_Result,
		Error,
	) {
		page_id := pid
		if cow {
			new_id, c_err := copy_on_write(t, page_id)
			if c_err != .None {
				return {}, c_err
			}
			page_id = new_id
		}

		node, n_err := load_node(t, page_id)
		if n_err != .None {
			return {}, n_err
		}

		defer unpin_node(t, node)
		if is_leaf(node) {
			if node.header.page_type != .LEAF_TEXT {
				return {}, .Invalid_Page_Header
			}

			e := text_node_delete_leaf_cell(t, &node, text, rowid)
			if e == .None {
				stats_row_count_set(tree_stats(t), node.id, int(node.header.cell_count))
			}
			return Text_Update_COW_Result{new_page = node.id}, e
		}

		child_id, child_idx := text_interior_find_child(node.data, node.id, tkey)
		child_result, c_err := delete_cow_recursive(t, child_id, text, rowid, tkey, true)
		if c_err != .None {
			return {}, c_err
		}
		if child_result.new_page != child_id {
			pager.unpin_page(t.pager, child_result.new_page)
		}
		if child_result.new_page != child_id {
			store_idx := child_idx
			if store_idx < 0 {
				store_idx = get_cell_count(node.data, node.id)
			}
			if s_err := text_interior_child_store(
				node.data,
				Page_Id(node.id),
				store_idx,
				child_result.new_page,
			); s_err != .None {
				return {}, .Invalid_Cell_Pointer
			}
			update_row_count(t, node.id, -1)
		}

		pager.mark_dirty(t.pager, node.id)
		return Text_Update_COW_Result{new_page = node.id}, .None
	}

	result, rec_err := delete_cow_recursive(t, t.root, text, rowid, tkey, true)
	if rec_err != .None {
		return 0, rec_err
	}

	pager.unpin_page(t.pager, result.new_page)
	return result.new_page, .None
}

// finish_text_root_split grows the text root when a recursive insert split
// it, then recounts. Mirrors finish_root_split (dense): the leaf fast path
// below additionally recounts unconditionally.
@(private = "file")
finish_text_root_split :: proc(t: ^Tree, result: Text_Insert_Result) -> Error {
	if result.did_split {
		if _, s_err := text_split_interior_root(t, result); s_err != .None {
			return s_err
		}
		if _, c_err := count_recursive(t, t.root); c_err != .None {
			return c_err
		}
	}
	return .None
}

// text_insert inserts one (text,rowid) in place (Direct mode, no COW).
// Mirrors tree_insert: leaf fast-path, root split in place (root id is
// stable — splits rebuild it as an interior), recursive absorb, recount.
@(require_results)
text_insert :: proc(t: ^Tree, text: []u8, rowid: types.Row_ID) -> Error {
	tkey, k_err := text_make_key(text, rowid)
	if k_err != .None {
		return k_err
	}

	root_node := load_node(t, t.root) or_return
	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		if root_node.header.page_type != .LEAF_TEXT {
			return .Invalid_Page_Header
		}

		e := text_node_insert_leaf_cell(t, &root_node, text, rowid)
		if e != .Page_Full {
			if e == .None {
				stats_row_count_set(tree_stats(t), t.root, int(root_node.header.cell_count))
			}
			return e
		}
		if _, s_err := text_split_leaf_root(t, t.root, text, rowid); s_err != .None {
			return s_err
		}

		result, r_err := text_insert_recursive(t, t.root, text, rowid, tkey, false)
		if r_err != .None {
			return r_err
		}
		if f_err := finish_text_root_split(t, result); f_err != .None {
			return f_err
		}
		if _, c_err := count_recursive(t, t.root); c_err != .None {
			return c_err
		}
		return .None
	}

	result, i_err := text_insert_recursive(t, t.root, text, rowid, tkey, false)
	if i_err != .None {
		return i_err
	}
	return finish_text_root_split(t, result)
}
