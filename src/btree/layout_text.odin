// B-tree text leaf pages (LEAF_TEXT): prefix-compressed secondary text index
// leaves.
//
// V3.0 scope: BINARY collation only, NULL-not-indexed (NULL rows never reach
// these pages — DML skips them, so the format has no NULL encoding),
// single-column text, covering only SELECT rowid (entries carry rowids).
//
// Page layout (PAGE_SIZE bytes, page 1 starts at the 100-byte DB prefix):
//   [Leaf_Header: 8][prefix_len: u16le][prefix: prefix_len bytes]
//   [slots: count * Text_Slot{off u16le, len u16le}]
//   ...free space...
//   [cells packed down from PAGE_SIZE: [rowid u64be biased][suffix bytes]]
// A slot's len covers rowid+suffix (len >= 8 always); suffix may be empty.
// Rowid-first at a fixed offset: the uniqueness tiebreak reads without
// parsing text. Rowid bias matches the wire codec (u64be biased), so page
// order and codec order agree by construction.
//
// Invariants (writers own them, the validator checks them):
// - Sorted by (full text BINARY, rowid numeric), strictly increasing pairs
//   (a duplicate (text,rowid) is corruption, not a dup-key — rowids are unique).
// - Shared prefix P is EXACT: P == shared(first_full, last_full). Checkable
//   page-locally (see validator); a short P would still order correctly but
//   is rejected loudly — silent compression misses become failures.
// - All borrows (prefix, suffix) die on page move: the cursor pins exactly
//   one page, same contract as text_index_decode.
//
// Ordering authority is cell.text_index_compare; page order must equal codec
// order on the same key set.
//
// Intra-page suffix comparison is equivalent to full-text comparison ONLY under the
// shared-prefix invariant above.
package btree

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:types"

// TEXT_LEAF_FIXED is the fixed header footprint past the page offset:
// stock Leaf_Header (8) + prefix_len u16le (2). The prefix bytes follow.
TEXT_LEAF_FIXED :: 8 + 2

// TEXT_ENTRY_ROWID_LEN is the fixed rowid prefix of every cell entry.
TEXT_ENTRY_ROWID_LEN :: 8

// Text_Slot locates one entry's cell: absolute offset + total entry length
// (rowid + suffix). 4 bytes — denser than Slot (10) because text pages need
// no rowid inline: the rowid lives at a fixed offset inside the cell.
Text_Slot :: struct #packed {
	off: u16le, // absolute cell offset
	len: u16le, // entry length incl. the 8-byte rowid (>= 8 always)
}
#assert(size_of(Text_Slot) == 4)

// init_text_leaf_page zeroes and headers a fresh LEAF_TEXT page.
// Same short-buffer contract as init_slot_leaf_page.
@(require_results)
init_text_leaf_page :: proc "contextless" (data: []u8, page_id: u32) -> bool {
	off := get_page_header_offset(page_id)
	if len(data) < off + TEXT_LEAF_FIXED {
		return false
	}

	mem.zero_slice(data[off:])
	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .LEAF_TEXT
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
	return true
}

// text_entry_area_end is where the slot array ends (cells must start at or
// past it). Error source for corrupt prefix_len/count — callers fail closed.
@(require_results)
text_entry_area_end :: proc "contextless" (data: []u8, id: Page_Id) -> (int, Error) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return 0, .Invalid_Page_Header
	}

	off0 := get_page_header_offset(u32(id))
	if intrinsics.unlikely(len(data) < off0 + TEXT_LEAF_FIXED) {
		return 0, .Cell_Deserialize_Failed
	}

	plen := int(u16(data[off0 + 8]) | u16(data[off0 + 9]) << 8)
	end := off0 + TEXT_LEAF_FIXED + plen + int(hdr.cell_count) * size_of(Text_Slot)
	if intrinsics.unlikely(end > len(data)) {
		return 0, .Cell_Deserialize_Failed
	}
	return end, .None
}

// text_prefix returns the page's shared prefix (borrowed — dies on move).
@(require_results)
text_prefix :: proc "contextless" (data: []u8, id: Page_Id) -> ([]u8, Error) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return nil, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return nil, .Invalid_Page_Header
	}

	off0 := get_page_header_offset(u32(id))
	if intrinsics.unlikely(len(data) < off0 + TEXT_LEAF_FIXED) {
		return nil, .Cell_Deserialize_Failed
	}

	plen := int(u16(data[off0 + 8]) | u16(data[off0 + 9]) << 8)
	if intrinsics.unlikely(len(data) < off0 + TEXT_LEAF_FIXED + plen) {
		return nil, .Cell_Deserialize_Failed
	}
	return data[off0 + TEXT_LEAF_FIXED:off0 + TEXT_LEAF_FIXED + plen], .None
}

// text_entry_at reads entry i: borrowed suffix + rowid. Layout mirrors
// slot_at: header/type/index checks, then count-derived span checks (corrupt
// counts trap here, never slice out of bounds).
@(require_results)
text_entry_at :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	suffix: []u8,
	rowid: types.Row_ID,
	err: Error,
) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return nil, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return nil, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(i < 0 || i >= int(hdr.cell_count)) {
		return nil, 0, .Cell_Not_Found
	}

	entry_end, e_err := text_entry_area_end(data, id)
	if e_err != .None {
		return nil, 0, e_err
	}

	slots_start := entry_end - int(hdr.cell_count) * size_of(Text_Slot)
	if intrinsics.unlikely(slots_start + (i + 1) * size_of(Text_Slot) > len(data)) {
		return nil, 0, .Cell_Deserialize_Failed
	}

	slot := (^Text_Slot)(raw_data(data[slots_start + i * size_of(Text_Slot):]))
	off := int(slot.off)
	elen := int(slot.len)
	if intrinsics.unlikely(elen < TEXT_ENTRY_ROWID_LEN) {
		return nil, 0, .Cell_Deserialize_Failed
	}

	cco := int(hdr.cell_content_offset)
	if intrinsics.unlikely(off < cco || off + elen > PAGE_SIZE || off + elen > len(data)) {
		return nil, 0, .Cell_Deserialize_Failed
	}

	rb := endian.unchecked_get_u64be(data[off:off + 8])
	return data[off + TEXT_ENTRY_ROWID_LEN:off + elen], rowid_bias_decode(rb), .None
}

// text_entry_compare orders (suffix,rowid) pairs: suffix BINARY, then rowid
// numeric. Equivalent to full-text codec order iff both sides share the
// page prefix (the writer-owned invariant) — the oracle test pins this.
@(private = "file")
text_entry_compare :: #force_inline proc "contextless" (
	suf: []u8,
	rid: types.Row_ID,
	ts: []u8,
	trid: types.Row_ID,
) -> int {
	if r := mem.compare(suf, ts); r != 0 {
		return r
	}
	if rid == trid {
		return 0
	}
	return -1 if rid < trid else 1
}

// text_lower_bound is first-index-with-entry->=-(target_text, target_rowid)
// under page order. Targets outside the page prefix resolve without touching
// entries: every entry shares the full prefix, so the first differing byte
// orders the target against all of them at once.
@(require_results)
text_lower_bound :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	target: []u8,
	target_rowid: types.Row_ID,
) -> (
	int,
	Error,
) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return 0, .Invalid_Page_Header
	}

	count := int(hdr.cell_count)
	prefix, p_err := text_prefix(data, id)
	if p_err != .None {
		return 0, p_err
	}
	if count == 0 {
		return 0, .None
	}

	m := cell.text_index_shared_prefix(target, prefix, min(len(target), len(prefix)))
	if m < len(target) && m < len(prefix) {
		if target[m] < prefix[m] {
			return 0, .None
		}
		return count, .None
	}
	if len(target) < len(prefix) {
		return 0, .None
	}

	ts := target[len(prefix):]
	left, right := 0, count
	for left < right {
		mid := left + (right - left) / 2
		suf, rid, k_err := text_entry_at(data, id, mid)
		if k_err != .None {
			return left, k_err
		}
		if text_entry_compare(suf, rid, ts, target_rowid) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, .None
}

// text_build_from_sorted builds a text leaf from sorted (text,rowid) pairs
// with the shared prefix factored into the header (exact: shared of first
// and last — sorted input shares it across all). Atomic like the dense
// builder: measured first, .Page_Full leaves the page untouched; unsorted
// or prefix-violating input fails via the trailing validator, never a
// half-built page (each failure path returns before/around writes that the
// validator would reject — build writes, then validates, so any bad input
// the measurement missed still fails closed).
@(require_results)
text_build_from_sorted :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	texts: [][]u8,
	rowids: []types.Row_ID,
) -> Error {
	n := len(texts)
	if len(rowids) != n {
		return .Invalid_Bounds
	}
	if n > int(max(u16)) {
		return .Page_Full
	}

	off0 := get_page_header_offset(u32(id))
	plen := 0
	if n > 0 {
		plen = cell.text_index_shared_prefix(texts[0], texts[n - 1], PAGE_SIZE)
	}

	total := off0 + TEXT_LEAF_FIXED + plen + n * size_of(Text_Slot)
	for i in 0 ..< n {
		if !cell.text_index_has_prefix(texts[i], texts[0][:plen]) {
			return .Cell_Deserialize_Failed
		}
		total += TEXT_ENTRY_ROWID_LEN + (len(texts[i]) - plen)
	}
	if total > len(data) {
		return .Page_Full
	}
	if !init_text_leaf_page(data, u32(id)) {
		return .Invalid_Page_Header
	}

	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}

	hdr.cell_count = u16le(u16(n))
	data[off0 + 8] = u8(plen)
	data[off0 + 9] = u8(plen >> 8)
	if plen > 0 {
		copy(data[off0 + TEXT_LEAF_FIXED:off0 + TEXT_LEAF_FIXED + plen], texts[0][:plen])
	}

	slots_start := off0 + TEXT_LEAF_FIXED + plen
	dest := PAGE_SIZE
	for i in 0 ..< n {
		suflen := len(texts[i]) - plen
		elen := TEXT_ENTRY_ROWID_LEN + suflen
		dest -= elen
		if !endian.put_u64(
			data[dest:dest + TEXT_ENTRY_ROWID_LEN],
			.Big,
			rowid_bias_encode(rowids[i]),
		) {
			return .Serialization_Failed
		}

		copy(data[dest + TEXT_ENTRY_ROWID_LEN:dest + elen], texts[i][plen:])
		slot := (^Text_Slot)(raw_data(data[slots_start + i * size_of(Text_Slot):]))
		slot^ = Text_Slot {
			off = u16le(u16(dest)),
			len = u16le(u16(elen)),
		}
	}

	hdr.cell_content_offset = u16le(u16(dest))
	return text_validate_leaf(data, id)
}

// text_validate_leaf checks structure, bounds, strict (suffix,rowid) order,
// and prefix exactness (P == shared(first_full,last_full), provable without
// allocation: shared == len(P) iff the first/last suffixes share nothing).
// Never rejects a page text_build_from_sorted can validly produce. Cannot
// detect well-formed-but-wrong data (an entry whose true text lacks P still
// reconstructs) — writers own content, the validator owns structure.
@(require_results)
text_validate_leaf :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if hdr.page_type != .LEAF_TEXT {
		return .Invalid_Page_Header
	}

	entry_end, e_err := text_entry_area_end(data, id)
	if e_err != .None {
		return e_err
	}

	cco := int(hdr.cell_content_offset)
	if cco < entry_end || cco > PAGE_SIZE {
		return .Cell_Deserialize_Failed
	}

	count := int(hdr.cell_count)
	prev_suf: []u8 = nil
	prev_rid: types.Row_ID = 0
	have_prev := false
	for i in 0 ..< count {
		suf, rid, k_err := text_entry_at(data, id, i)
		if k_err != .None {
			return k_err
		}
		if have_prev && text_entry_compare(prev_suf, prev_rid, suf, rid) >= 0 {
			return .Cell_Deserialize_Failed
		}
		prev_suf, prev_rid, have_prev = suf, rid, true
	}
	return .None
}

// Layout (PAGE_SIZE bytes):
//   [Leaf-compatible 8B header][children: (n+1) u32le][sep slots: n * Text_Slot]
//   ...free space...
//   [cells packed down from PAGE_SIZE: full index keys (wire codec bytes)]
// Separators are full codec keys verbatim (TAG+len+text+rowid8) — never
// prefix-stripped — so text_index_compare is the ordering authority with
// zero translation, and the exclusive-separator invariant reads exactly
// like the dense path: child[i] holds keys < sep[i], child[n] is rightmost.
// cell_count = n separators. Reuses Text_Slot (off+len of the codec blob).
//
// Writers live in text_tree.odin (C3b: online COW inserts, splits, root
// growth); separator_insert stays refused at the table (full-key
// separators flow through the free functions, never the vtable).

// init_text_interior_page zeroes and headers a fresh TEXT_INTERIOR page.
// Same short-buffer contract as init_text_leaf_page.
@(require_results)
init_text_interior_page :: proc "contextless" (data: []u8, page_id: u32) -> bool {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) {
		return false
	}

	mem.zero_slice(data[off:])
	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .TEXT_INTERIOR
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
	return true
}

// text_interior_children_off is the absolute offset of child[0].
// -1 on any header fault (callers fail closed, never index garbage).
@(private = "file")
text_interior_children_off :: #force_inline proc "contextless" (
	data: []u8,
	id: Page_Id,
	n: int,
) -> int {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return -1
	}
	if hdr.page_type != .TEXT_INTERIOR {
		return -1
	}

	off0 := get_page_header_offset(u32(id))
	end := off0 + size_of(Leaf_Header) + (n + 1) * 4 + n * size_of(Text_Slot)
	if end > len(data) {
		return -1
	}
	return off0 + size_of(Leaf_Header)
}

// text_interior_child_at returns child page i, range 0..=n (index n is the
// rightmost child — same contract as the vtable child_at).
@(require_results)
text_interior_child_at :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u32, Error) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .TEXT_INTERIOR) {
		return 0, .Invalid_Page_Header
	}

	n := int(hdr.cell_count)
	if intrinsics.unlikely(i < 0 || i > n) {
		return 0, .Cell_Not_Found
	}

	coff := text_interior_children_off(data, id, n)
	if coff < 0 {
		return 0, .Cell_Deserialize_Failed
	}

	o := coff + i * 4
	return endian.unchecked_get_u32le(data[o:o + 4]), .None
}

// text_interior_sep_at returns separator i's full codec key (borrowed).
@(require_results)
text_interior_sep_at :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	key: []u8,
	err: Error,
) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return nil, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .TEXT_INTERIOR) {
		return nil, .Invalid_Page_Header
	}

	n := int(hdr.cell_count)
	if intrinsics.unlikely(i < 0 || i >= n) {
		return nil, .Cell_Not_Found
	}

	coff := text_interior_children_off(data, id, n)
	if coff < 0 {
		return nil, .Cell_Deserialize_Failed
	}

	slots_start := coff + (n + 1) * 4
	if intrinsics.unlikely(slots_start + (i + 1) * size_of(Text_Slot) > len(data)) {
		return nil, .Cell_Deserialize_Failed
	}

	slot := (^Text_Slot)(raw_data(data[slots_start + i * size_of(Text_Slot):]))
	off := int(slot.off)
	elen := int(slot.len)
	cco := int(hdr.cell_content_offset)
	if intrinsics.unlikely(off < cco || off + elen > PAGE_SIZE || off + elen > len(data)) {
		return nil, .Cell_Deserialize_Failed
	}
	if _, _, ok := cell.text_index_split(data[off:off + elen]); !ok {
		return nil, .Cell_Deserialize_Failed
	}
	return data[off:off + elen], .None
}

// text_interior_find_upper routes a target codec key: first separator
// strictly greater than target (exclusive separators — mirrors
// node_find_child_data, including the equality skip and the -1 rightmost
// conventions). Corrupt pages fail to (0,-1): loud at the caller, which
// treats child 0 as an error, never a descent.
@(private = "file")
text_interior_find_upper :: #force_inline proc "contextless" (
	data: []u8,
	page_id: u32,
	target: []u8,
) -> (
	u32,
	int,
) {
	pid := Page_Id(page_id)
	cell_count := get_cell_count(data, page_id)
	rightmost, r_err := text_interior_child_at(data, pid, cell_count)
	if r_err != .None {
		return 0, -1
	}
	if cell_count == 0 {
		return rightmost, -1
	}

	idx := 0
	for idx < cell_count {
		sep, s_err := text_interior_sep_at(data, pid, idx)
		if s_err != .None {
			return 0, -1
		}
		if cell.text_index_compare(sep, target) > 0 {
			break
		}
		idx += 1
	}
	if idx >= cell_count {
		return rightmost, -1
	}

	child, c_err := text_interior_child_at(data, pid, idx)
	if c_err != .None {
		return rightmost, -1
	}
	return child, idx
}

// text_interior_find_child is the public routing primitive (C3b descent
// uses it; tests pin the boundary behavior through it).
@(require_results)
text_interior_find_child :: proc "contextless" (
	data: []u8,
	page_id: u32,
	target: []u8,
) -> (
	u32,
	int,
) {
	return text_interior_find_upper(data, page_id, target)
}

// text_interior_build_from_sorted builds a text interior from full codec
// keys + children. Same atomic contract as the dense builder: measured
// first (.Page_Full leaves the page untouched), per-key well-formedness
// guard (mirrors the dense FOR guard), trailing validator (unsorted input
// fails closed, never half-built).
@(require_results)
text_interior_build_from_sorted :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	keys: [][]u8,
	children: []u32,
) -> Error {
	n := len(keys)
	if len(children) != n + 1 {
		return .Invalid_Bounds
	}
	if n > int(max(u16)) {
		return .Page_Full
	}

	off0 := get_page_header_offset(u32(id))
	total := off0 + size_of(Leaf_Header) + (n + 1) * 4 + n * size_of(Text_Slot)
	for i in 0 ..< n {
		if _, _, ok := cell.text_index_split(keys[i]); !ok {
			return .Cell_Deserialize_Failed
		}
		total += len(keys[i])
	}
	if total > len(data) {
		return .Page_Full
	}
	if !init_text_interior_page(data, u32(id)) {
		return .Invalid_Page_Header
	}

	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}

	hdr.cell_count = u16le(u16(n))
	coff := off0 + size_of(Leaf_Header)
	for c, i in children {
		if !endian.put_u32(data[coff + i * 4:], .Little, c) {
			return .Serialization_Failed
		}
	}

	slots_start := coff + (n + 1) * 4
	dest := PAGE_SIZE
	for i in 0 ..< n {
		elen := len(keys[i])
		dest -= elen
		copy(data[dest:dest + elen], keys[i])

		slot := (^Text_Slot)(raw_data(data[slots_start + i * size_of(Text_Slot):]))
		slot^ = Text_Slot {
			off = u16le(u16(dest)),
			len = u16le(u16(elen)),
		}
	}

	hdr.cell_content_offset = u16le(u16(dest))
	return text_validate_interior(data, id)
}

// text_validate_interior checks type, spans, child readability, separator
// well-formedness, and STRICT full-key order via text_index_compare (text
// BINARY, then rowid — duplicate separators are corruption: with the rowid
// tiebreak two separators compare equal only if identical).
// Never rejects a page text_interior_build_from_sorted can produce.
@(require_results)
text_validate_interior :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if hdr.page_type != .TEXT_INTERIOR {
		return .Invalid_Page_Header
	}

	n := int(hdr.cell_count)
	off0 := get_page_header_offset(u32(id))
	if off0 + size_of(Leaf_Header) + (n + 1) * 4 + n * size_of(Text_Slot) > len(data) {
		return .Cell_Deserialize_Failed
	}

	cco := int(hdr.cell_content_offset)
	entry_end := off0 + size_of(Leaf_Header) + (n + 1) * 4 + n * size_of(Text_Slot)
	if cco < entry_end || cco > PAGE_SIZE {
		return .Cell_Deserialize_Failed
	}
	for i in 0 ..< n + 1 {
		if _, c_err := text_interior_child_at(data, id, i); c_err != .None {
			return c_err
		}
	}

	prev: []u8 = nil
	have_prev := false
	for i in 0 ..< n {
		sep, s_err := text_interior_sep_at(data, id, i)
		if s_err != .None {
			return s_err
		}
		if have_prev && cell.text_index_compare(prev, sep) >= 0 {
			return .Cell_Deserialize_Failed
		}
		prev, have_prev = sep, true
	}
	return .None
}

// text_slot_insert shifts slots right from idx and writes Text_Slot{off,len}.
// Mirrors slot_leaf_insert exactly: NO count bump (the caller owns it — same
// Option-A asymmetry as the rowid families), NO cell I/O (the caller places
// cell bytes via freeblock/bump first), capacity-vs-cells pre-checked by the
// caller (.Page_Full leaves the page untouched one level up).
@(require_results)
text_slot_insert :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	off: int,
	elen: int,
) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return .Invalid_Page_Header
	}

	count := int(hdr.cell_count)
	if intrinsics.unlikely(idx < 0 || idx > count) {
		return .Invalid_Bounds
	}
	if intrinsics.unlikely(
		off < 0 || off > PAGE_SIZE || elen < TEXT_ENTRY_ROWID_LEN || elen > PAGE_SIZE,
	) {
		return .Invalid_Bounds
	}

	off0 := get_page_header_offset(u32(id))
	prefix, p_err := text_prefix(data, id)
	if p_err != .None {
		return p_err
	}

	start := off0 + TEXT_LEAF_FIXED + len(prefix)
	if intrinsics.unlikely(start + (count + 1) * size_of(Text_Slot) > len(data)) {
		return .Invalid_Bounds
	}
	if idx < count {
		src := data[start + idx * size_of(Text_Slot):start + count * size_of(Text_Slot)]
		dst := data[start + (idx + 1) * size_of(Text_Slot):]
		copy(dst, src)
	}

	entry := (^Text_Slot)(raw_data(data[start + idx * size_of(Text_Slot):]))
	entry^ = Text_Slot {
		off = u16le(u16(off)),
		len = u16le(u16(elen)),
	}
	return .None
}

// text_interior_child_store writes children[idx] (index count addresses the
// rightmost slot — the array holds it, no header surgery). The indexed
// counterpart to node_update_child_ptr for COW repoints.
@(require_results)
text_interior_child_store :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	child: u32,
) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .TEXT_INTERIOR) {
		return .Invalid_Page_Header
	}

	n := int(hdr.cell_count)
	if intrinsics.unlikely(idx < 0 || idx > n) {
		return .Invalid_Bounds
	}

	coff := text_interior_children_off(data, id, n)
	if coff < 0 {
		return .Cell_Deserialize_Failed
	}
	if !endian.put_u32(data[coff + idx * 4:], .Little, child) {
		return .Serialization_Failed
	}
	return .None
}

// text_slot_delete shifts slots left from idx, dropping it. Mirrors
// slot_leaf_delete: NO count change (the caller owns the bump, same Option-A
// asymmetry as insert), NO cell I/O (the caller reclaims the cell).
@(require_results)
text_slot_delete :: proc "contextless" (data: []u8, id: Page_Id, idx: int) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_TEXT) {
		return .Invalid_Page_Header
	}

	count := int(hdr.cell_count)
	if intrinsics.unlikely(idx < 0 || idx >= count) {
		return .Invalid_Bounds
	}

	off0 := get_page_header_offset(u32(id))
	prefix, p_err := text_prefix(data, id)
	if p_err != .None {
		return p_err
	}

	start := off0 + TEXT_LEAF_FIXED + len(prefix)
	if intrinsics.unlikely(start + count * size_of(Text_Slot) > len(data)) {
		return .Invalid_Bounds
	}
	if idx < count - 1 {
		src := data[start + (idx + 1) * size_of(Text_Slot):start + count * size_of(Text_Slot)]
		dst := data[start + idx * size_of(Text_Slot):]
		copy(dst, src)
	}
	return .None
}
