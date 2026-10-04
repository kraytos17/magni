package btree

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:types"

// V3 layout/key interfaces.
//
// Zig inspiration (std.mem.Allocator): layout is a plain value built ONLY by
// the constructors below — never a struct literal, never a pointer into a
// global.
//
// Rust inspiration: fallible ops return (T, Error), dispatch on Page_Type
// fails closed (never a nil vtable), and key dispatch is an exhaustive
// switch over Key_Kind (compiler-checked match, no default arm).
//
// Deliberate split, by call frequency:
//   - Page_Layout stays a vtable: 7 ops resolved ONCE per page visit
//     (cursor caches it next to cached_page_id), so one indirect hop per
//     page is noise.
//   - Key_Codec is a static enum dispatch: compare runs O(log n) per
//     search INSIDE the binary-search loop, where an indirect call per
//     probe would be load-bearing. Static + #force_inline lets it melt
//     into the search loop; @(require_results) on key_encode is enforced
//     (unlike vtable slots, where Odin types can't carry the attribute).
//
// Odin notes:
//   - Every slot/impl is "contextless": layout queries never allocate and
//     never touch context. The compiler rejects any future context use.
//   - Page-byte indexing from on-disk headers STAYS bounds-checked: a
//     corrupt cell_count must trap, never read out of bounds.
//   - #no_bounds_check appears only on in-memory spans with a proven cap
//     (each site names its proof). Release builds elide checks globally
//     via -no-bounds-check anyway; these pay off in debug/test builds.

Page_Layout :: struct {
	vtable: ^Page_Layout_VTable,
}

Page_Layout_VTable :: struct {
	header_size      : proc "contextless" (pt: Page_Type) -> int,
	cell_count       : proc "contextless" (data: []u8, id: Page_Id) -> int,
	key_at           : proc "contextless" (
		data: []u8,
		id: Page_Id,
		i: int,
	) -> (
		types.Row_ID,
		Error,
	),
	lower_bound_rowid: proc "contextless" (
		data: []u8,
		id: Page_Id,
		key: types.Row_ID,
	) -> (
		int,
		Error,
	),
	slot_insert      : proc "contextless" (
		data: []u8,
		id: Page_Id,
		idx: int,
		rowid: types.Row_ID,
		off: Cell_Off,
	) -> Error,
	slot_delete      : proc "contextless" (data: []u8, id: Page_Id, idx: int) -> Error,
	// cell_ptr_at returns the cell-area offset of slot i (the u16 entry
	// prefix). Needed everywhere cell bytes are read (deserialize, child
	// extraction, rowid probes). Bounds-checked: corrupt counts trap.
	cell_ptr_at      : proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u16, Error),
	// slot_repoint rewrites slot i in place (ptr+key). The repoint path
	// for COW child updates and split fixups — insert shifts, this doesn't.
	slot_repoint     : proc "contextless" (
		data: []u8,
		id: Page_Id,
		idx: int,
		rowid: types.Row_ID,
		off: Cell_Off,
	) -> Error,
	// child_at returns child page i, range 0..=count (index count is the
	// rightmost child). Leaf tables fail closed (no children exist).
	child_at         : proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u32, Error),
	separator_insert : proc "contextless" (
		data: []u8,
		id: Page_Id,
		idx: int,
		key: types.Row_ID,
		child: u32,
	) -> Error,
	validate         : proc "contextless" (data: []u8, id: Page_Id) -> Error,
}

// Key_Kind selects the codec; the key_* procs below dispatch statically.
// Rowid arms live here; Text arms delegate to cell/text_codec.odin
// (single implementation, no wrapper layer).
Key_Kind :: enum u8 {
	Rowid,
	Text,
}

// key_compare orders two ENCODED keys (-1/0/+1): numeric rowid order for
// Rowid pages, (text BINARY, rowid) for Text pages. Raw (unencoded) text is
// not its input. One entry point on purpose: order and layout share the
// encoding, so a second entry would be speculative duplication — split them
// if an encoding ever divorces order from layout.
key_compare :: #force_inline proc "contextless" (kind: Key_Kind, a: []u8, b: []u8) -> int {
	res := 0
	switch kind {
	case .Rowid:
		res = mem.compare(a, b)
	case .Text:
		res = cell.text_index_compare(a, b)
	}
	return res
}

// key_shared_prefix_len returns the common byte prefix of a,b capped at
// max_cap (Masstree insight, page-local use).
key_shared_prefix_len :: #force_inline proc "contextless" (
	kind: Key_Kind,
	a: []u8,
	b: []u8,
	max_cap: int,
) -> int {
	res := 0
	switch kind {
	case .Rowid:
		res = rowid_shared_prefix_len(a, b, max_cap)
	case .Text:
		res = cell.text_index_shared_prefix(a, b, max_cap)
	}
	return res
}

// key_encode maps (value,rowid) -> key bytes, returning bytes written.
// Only the codec's own storage class encodes (i64 for Rowid, string for
// Text); everything else — including NULL — reports ok=false so the caller
// SKIPS the entry instead of failing the row.
@(require_results)
key_encode :: proc "contextless" (
	kind: Key_Kind,
	val: types.Value,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	n, ok := 0, false
	switch kind {
	case .Rowid:
		n, ok = rowid_encode(val, rowid, buf)
	case .Text:
		n, ok = cell.text_index_encode(val, rowid, buf)
	}
	return n, ok
}

// key_encoded_len returns the wire length for val, or 0 when val is not
// indexed by this codec (caller skips).
key_encoded_len :: #force_inline proc "contextless" (kind: Key_Kind, val: types.Value) -> int {
	res := 0
	switch kind {
	case .Rowid:
		res = rowid_encoded_len(val)
	case .Text:
		res = cell.text_index_encoded_len(val)
	}
	return res
}

// ---- Rowid codec arms (primary keys) --------------------------------------
//
// Order-preserving bias: encoded u64 = u64(i64 ^ MIN_I64) big-endian, so
// unsigned byte order == signed numeric order (negatives sort first).
@(private = "file")
rowid_encode_u64 :: #force_inline proc "contextless" (v: types.Row_ID) -> u64 {
	return rowid_bias_encode(v)
}

@(private = "file")
rowid_shared_prefix_len :: #force_inline proc "contextless" (
	a: []u8,
	b: []u8,
	max_cap: int,
) -> int {
	cap := max_cap
	if cap > len(a) {
		cap = len(a)
	}
	if cap > len(b) {
		cap = len(b)
	}
	if cap <= 0 {
		return 0
	}

	n := 0
	#no_bounds_check for n < cap && a[n] == b[n] { n += 1 }
	return n
}

@(private = "file")
ROWID_INDEX_ENCODED_LEN :: 9 // tag + 8-byte biased word
#assert(ROWID_INDEX_ENCODED_LEN == 1 + 8)

@(private = "file")
rowid_encode :: proc "contextless" (
	val: types.Value,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	v, ok := val.(i64)
	if intrinsics.unlikely(!ok) {
		return 0, false
	}
	if intrinsics.unlikely(len(buf) < ROWID_INDEX_ENCODED_LEN) {
		return 0, false
	}

	buf[0] = 0x52 // 'R': rowid tag, reserves tag space for composite keys
	w := rowid_encode_u64(types.Row_ID(v))
	// rowid is interface-mandated (Text arms need the tiebreak) but unused
	// here: for Rowid pages the key IS the value, unique by construction.
	_ = rowid

	endian.unchecked_put_u64be(buf[1:ROWID_INDEX_ENCODED_LEN], w)
	return ROWID_INDEX_ENCODED_LEN, true
}

@(private = "file")
rowid_encoded_len :: #force_inline proc "contextless" (val: types.Value) -> int {
	_, ok := val.(i64)
	return ROWID_INDEX_ENCODED_LEN if ok else 0
}


// dense_separator_insert inserts (key, child) at idx on a dense interior
// page and OWNS the cell_count bump (Option A: mirrors the deleted V2
// builder; asymmetric with slot_insert by lineage, each family consistent).
// FOR pages reject keys outside the page's [base, base+max(u32)] with
// .Page_Full (no silent re-encoding; the caller splits and the halves
// re-derive their encodings). Full pages fail only on capacity.
@(private = "file", require_results)
dense_separator_insert :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	key: types.Row_ID,
	child: u32,
) -> Error {
	keys_off, children_off, count, use_for, base, g_err := dense_geometry(data, id)
	if g_err != .None {
		return g_err
	}
	if intrinsics.unlikely(idx < 0 || idx > count) {
		return .Invalid_Bounds
	}

	bk := rowid_bias_encode(key)
	if use_for && intrinsics.unlikely(bk < base || bk - base > u64(max(u32))) {
		return .Page_Full
	}

	kwidth := DENSE_DELTA_WIDTH if use_for else DENSE_FULL_KEY_WIDTH
	off := get_page_header_offset(u32(id))
	if off +
		   size_of(Dense_Interior_Header) +
		   (count + 1) * kwidth +
		   (count + 2) * DENSE_CHILD_WIDTH >
	   len(data) {
		return .Page_Full
	}

	// Relocation order matters: the children block's START depends on the
	// key count, so it moves first (whole block right by one key width),
	// then the keys tail shifts inside the fixed keys region, then the new
	// key+child land in the new geometry. All moves are rightward memmoves
	// into prechecked free space. (Shifting children in the OLD geometry
	// and bumping after — the naive order — strands them under the new
	// offset and returns stale children.)
	new_children_off := keys_off + (count + 1) * kwidth
	copy(
		data[new_children_off:],
		data[children_off:children_off + (count + 1) * DENSE_CHILD_WIDTH],
	)
	copy(
		data[keys_off + (idx + 1) * kwidth:],
		data[keys_off + idx * kwidth:keys_off + count * kwidth],
	)
	if use_for {
		if !endian.put_u32(data[keys_off + idx * kwidth:], .Little, u32(bk - base)) {
			return .Serialization_Failed
		}
	} else {
		if !endian.put_u64(data[keys_off + idx * kwidth:], .Little, bk) {
			return .Serialization_Failed
		}
	}
	if !endian.put_u32(data[new_children_off + idx * DENSE_CHILD_WIDTH:], .Little, child) {
		return .Serialization_Failed
	}

	h := get_dense_interior_header(data, u32(id))
	if h == nil {
		return .Invalid_Page_Header
	}

	h.cell_count += 1
	return .None
}

// shared_cell_count adapts the header reader to the vtable slot type.
@(private = "file")
shared_cell_count :: #force_inline proc "contextless" (data: []u8, id: Page_Id) -> int {
	return get_cell_count(data, u32(id))
}

@(private = "file")
dense_interior_table := Page_Layout_VTable {
	header_size       = page_header_size,
	cell_count        = shared_cell_count,
	key_at            = dense_key_at,
	lower_bound_rowid = dense_page_lower_bound,
	slot_insert       = v3_stub_insert,
	slot_delete       = v3_stub_delete,
	cell_ptr_at       = v3_stub_cell_ptr_at,
	slot_repoint      = v3_stub_repoint,
	child_at          = dense_child_at,
	separator_insert  = dense_separator_insert,
	validate          = validate_dense_interior,
}

// slot_leaf_* are the LEAF_SLOTDIR mechanics: same shift/write shapes as
// the slot ops, Slot-typed. They check the page type:
// the tables route by discriminant, but a direct call on V2 bytes must fail
// never reinterpret a Cell_Entry as a Slot.
@(private = "file", require_results)
slot_leaf_key_at :: #force_inline proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	types.Row_ID,
	Error,
) {
	k, _, k_err := slot_at(data, id, i)
	if k_err != .None {
		return 0, k_err
	}
	return k, .None
}

@(private = "file", require_results)
slot_leaf_cell_ptr_at :: #force_inline proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	u16,
	Error,
) {
	_, off, o_err := slot_at(data, id, i)
	if o_err != .None {
		return 0, o_err
	}
	return off, .None
}

@(private = "file", require_results)
slot_leaf_insert :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_SLOTDIR) {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(idx < 0 || idx > int(hdr.cell_count)) {
		return .Invalid_Bounds
	}

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	cell_count := int(hdr.cell_count)
	if intrinsics.unlikely(start + (cell_count + 1) * size_of(Slot) > len(data)) {
		return .Invalid_Bounds
	}
	if idx < cell_count {
		src := data[start + idx * size_of(Slot):start + cell_count * size_of(Slot)]
		dst := data[start + (idx + 1) * size_of(Slot):]
		copy(dst, src)
	}

	entry := (^Slot)(raw_data(data[start + idx * size_of(Slot):]))
	entry^ = Slot {
		rowid = u64le(rowid_bias_encode(rowid)),
		off   = u16le(u16(off)),
	}
	return .None
}

@(private = "file", require_results)
slot_leaf_delete :: proc "contextless" (data: []u8, id: Page_Id, idx: int) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_SLOTDIR) {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(idx < 0 || idx >= int(hdr.cell_count)) {
		return .Invalid_Bounds
	}

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	cell_count := int(hdr.cell_count)
	if intrinsics.unlikely(start + cell_count * size_of(Slot) > len(data)) {
		return .Invalid_Bounds
	}
	if idx < cell_count - 1 {
		src := data[start + (idx + 1) * size_of(Slot):start + cell_count * size_of(Slot)]
		dst := data[start + idx * size_of(Slot):]
		copy(dst, src)
	}
	return .None
}

@(private = "file", require_results)
slot_leaf_repoint :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_SLOTDIR) {
		return .Invalid_Page_Header
	}
	if intrinsics.unlikely(idx < 0 || idx >= int(hdr.cell_count)) {
		return .Invalid_Bounds
	}

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	if intrinsics.unlikely(start + (idx + 1) * size_of(Slot) > len(data)) {
		return .Invalid_Bounds
	}

	entry := (^Slot)(raw_data(data[start + idx * size_of(Slot):]))
	entry^ = Slot {
		rowid = u64le(rowid_bias_encode(rowid)),
		off   = u16le(u16(off)),
	}
	return .None
}

@(private = "file")
slot_leaf_table := Page_Layout_VTable {
	header_size       = page_header_size,
	cell_count        = shared_cell_count,
	key_at            = slot_leaf_key_at,
	lower_bound_rowid = slot_lower_bound,
	slot_insert       = slot_leaf_insert,
	slot_delete       = slot_leaf_delete,
	cell_ptr_at       = slot_leaf_cell_ptr_at,
	slot_repoint      = slot_leaf_repoint,
	child_at          = v3_stub_child,
	separator_insert  = v3_stub_separator,
	validate          = validate_slot_leaf,
}

@(private = "file", cold)
v3_stub_int :: proc "contextless" (pt: Page_Type) -> int {
	_ = pt
	return 0
}

@(private = "file", cold)
v3_stub_count :: proc "contextless" (data: []u8, id: Page_Id) -> int {
	_ = data
	_ = id
	return 0
}

@(private = "file", cold, require_results)
v3_stub_key_at :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (types.Row_ID, Error) {
	_ = data
	_ = id
	_ = i
	return 0, .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_lower_bound :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	key: types.Row_ID,
) -> (
	int,
	Error,
) {
	_ = data
	_ = id
	_ = key
	return 0, .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_insert :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	_ = data
	_ = id
	_ = idx
	_ = rowid
	_ = off
	return .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_delete :: proc "contextless" (data: []u8, id: Page_Id, idx: int) -> Error {
	_ = data
	_ = id
	_ = idx
	return .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_validate :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	_ = data
	_ = id
	return .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_cell_ptr_at :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u16, Error) {
	_ = data
	_ = id
	_ = i
	return 0, .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_repoint :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	_ = data
	_ = id
	_ = idx
	_ = rowid
	_ = off
	return .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_child :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u32, Error) {
	_ = data
	_ = id
	_ = i
	return 0, .Unsupported_Format
}

@(private = "file", cold, require_results)
v3_stub_separator :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	key: types.Row_ID,
	child: u32,
) -> Error {
	_ = data
	_ = id
	_ = idx
	_ = key
	_ = child
	return .Unsupported_Format
}

@(private = "file")
v3_stub_table := Page_Layout_VTable {
	header_size       = v3_stub_int,
	cell_count        = v3_stub_count,
	key_at            = v3_stub_key_at,
	lower_bound_rowid = v3_stub_lower_bound,
	slot_insert       = v3_stub_insert,
	slot_delete       = v3_stub_delete,
	cell_ptr_at       = v3_stub_cell_ptr_at,
	slot_repoint      = v3_stub_repoint,
	child_at          = v3_stub_child,
	separator_insert  = v3_stub_separator,
	validate          = v3_stub_validate,
}

@(private = "file")
text_interior_table := Page_Layout_VTable {
	header_size       = page_header_size,
	cell_count        = shared_cell_count,
	key_at            = v3_stub_key_at,
	lower_bound_rowid = v3_stub_lower_bound,
	slot_insert       = v3_stub_insert,
	slot_delete       = v3_stub_delete,
	cell_ptr_at       = v3_stub_cell_ptr_at,
	slot_repoint      = v3_stub_repoint,
	child_at          = text_interior_child_at,
	separator_insert  = v3_stub_separator,
	validate          = text_validate_interior,
}

@(private = "file")
text_leaf_table := Page_Layout_VTable {
	header_size       = page_header_size,
	cell_count        = shared_cell_count,
	key_at            = v3_stub_key_at,
	lower_bound_rowid = v3_stub_lower_bound,
	slot_insert       = v3_stub_insert,
	slot_delete       = v3_stub_delete,
	cell_ptr_at       = v3_stub_cell_ptr_at,
	slot_repoint      = v3_stub_repoint,
	child_at          = v3_stub_child,
	separator_insert  = v3_stub_separator,
	validate          = text_validate_leaf,
}

dense_u64_interior_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &dense_interior_table}
}

slot_dir_leaf_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &slot_leaf_table}
}

prefix_leaf_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &text_leaf_table}
}

prefix_interior_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &text_interior_table}
}

// layout_for_page resolves the (Page_Layout, Key_Kind) pair for the page in
// front of the caller. One dynamic hop per page visit; key comparisons run
// through the static key_* dispatch (zero further hops). Unknown or
// headerless pages fail closed — never a nil vtable.
// require_results: an unresolved layout used as zero-value would nil-deref
// the vtable — always check.
@(require_results)
layout_for_page :: proc(data: []u8, id: Page_Id) -> (Page_Layout, Key_Kind, Error) {
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) {
		return {}, {}, .Invalid_Page_Header
	}

	switch hdr.page_type {
	case .INTERIOR_DENSE:
		return dense_u64_interior_layout(), .Rowid, .None
	case .LEAF_SLOTDIR:
		return slot_dir_leaf_layout(), .Rowid, .None
	case .LEAF_TEXT:
		return prefix_leaf_layout(), .Text, .None
	case .TEXT_INTERIOR:
		return prefix_interior_layout(), .Text, .None
	case:
		return {}, {}, .Invalid_Page_Header
	}
}
