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
	// Assigned, not returned, inside the cases: keeps the switch exhaustive
	// (adding a Key_Kind is a compile error here) without a trailing
	// unreachable, which require_results rejects in value-returning procs.
	res := 0
	switch kind {
	case .Rowid:
		// Core memcmp + shorter-first tiebreak, direct (a one-line
		// forwarder here would be pure noise).
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
	return u64(i64(v) ~ min(i64))
}

@(private = "file")
rowid_shared_prefix_len :: #force_inline proc "contextless" (
	a: []u8,
	b: []u8,
	max_cap: int,
) -> int {
	cap := max_cap
	if cap > len(a) { cap = len(a) }
	if cap > len(b) { cap = len(b) }
	if cap <= 0 { return 0 }

	n := 0
	// Proven: cap <= both lengths, so every index below is in range.
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
	if intrinsics.unlikely(!ok) { return 0, false }
	if intrinsics.unlikely(len(buf) < ROWID_INDEX_ENCODED_LEN) { return 0, false }

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

@(private = "file")
compat_header_size :: #force_inline proc "contextless" (pt: Page_Type) -> int {
	return page_header_size(pt)
}

@(private = "file")
compat_cell_count :: #force_inline proc "contextless" (data: []u8, id: Page_Id) -> int {
	return get_cell_count(data, u32(id))
}

@(private = "file", require_results)
compat_key_at :: #force_inline proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	types.Row_ID,
	Error,
) {
	off := get_page_header_offset(u32(id))
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return 0, .Invalid_Page_Header }
	if intrinsics.unlikely(i < 0 || i >= int(hdr.cell_count)) { return 0, .Cell_Not_Found }

	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	entry := (^Cell_Entry)(raw_data(data[start + i * CELL_ENTRY_STRIDE:]))
	return entry.key, .None
}

@(private = "file", require_results)
compat_lower_bound_rowid :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	key: types.Row_ID,
) -> (
	int,
	Error,
) {
	count := get_cell_count(data, u32(id))
	left, right := 0, count
	for left < right {
		mid := left + (right - left) / 2
		k, k_err := compat_key_at(data, id, mid)
		if k_err != .None { return 0, k_err }
		if k < key { left = mid + 1 } else { right = mid }
	}
	return left, .None
}

@(private = "file", require_results)
compat_slot_insert :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return .Invalid_Page_Header }
	if intrinsics.unlikely(idx < 0 || idx > int(hdr.cell_count)) { return .Invalid_Bounds }

	// Former insert_cell_at body, inlined so the free function can die:
	// shift entries [idx..count) right by one, then write the new entry.
	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	cell_count := int(hdr.cell_count)
	if idx < cell_count {
		src := data[start + idx * CELL_ENTRY_STRIDE:start + cell_count * CELL_ENTRY_STRIDE]
		dst := data[start + (idx + 1) * CELL_ENTRY_STRIDE:]
		copy(dst, src)
	}

	entry := (^Cell_Entry)(raw_data(data[start + idx * CELL_ENTRY_STRIDE:]))
	entry^ = Cell_Entry {
		ptr = Cell_Pointer(u16(off)),
		key = rowid,
	}
	return .None
}

@(private = "file", require_results)
compat_slot_delete :: proc "contextless" (data: []u8, id: Page_Id, idx: int) -> Error {
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return .Invalid_Page_Header }
	if intrinsics.unlikely(idx < 0 || idx >= int(hdr.cell_count)) { return .Invalid_Bounds }

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	cell_count := int(hdr.cell_count)
	if idx < cell_count - 1 {
		src := data[start + (idx + 1) * CELL_ENTRY_STRIDE:start + cell_count * CELL_ENTRY_STRIDE]
		dst := data[start + idx * CELL_ENTRY_STRIDE:]
		copy(dst, src)
	}
	return .None
}

@(private = "file", require_results)
compat_cell_ptr_at :: #force_inline proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	u16,
	Error,
) {
	off := get_page_header_offset(u32(id))
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return 0, .Invalid_Page_Header }
	if intrinsics.unlikely(i < 0 || i >= int(hdr.cell_count)) { return 0, .Cell_Not_Found }

	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	return u16((^u16le)(raw_data(data[start + i * CELL_ENTRY_STRIDE:]))^), .None
}

@(private = "file", require_results)
compat_slot_repoint :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	idx: int,
	rowid: types.Row_ID,
	off: Cell_Off,
) -> Error {
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return .Invalid_Page_Header }
	if intrinsics.unlikely(idx < 0 || idx >= int(hdr.cell_count)) { return .Invalid_Bounds }

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	entry := (^Cell_Entry)(raw_data(data[start + idx * CELL_ENTRY_STRIDE:]))
	entry^ = Cell_Entry {
		ptr = Cell_Pointer(u16(off)),
		key = rowid,
	}
	return .None
}

@(private = "file", require_results)
compat_validate :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	hdr := get_header(data, u32(id))
	if intrinsics.unlikely(hdr == nil) { return .Invalid_Page_Header }
	if hdr.page_type == .LEAF_TABLE_COLUMNAR { return .None }

	count := int(hdr.cell_count)
	prev: types.Row_ID = min(types.Row_ID)
	for i in 0 ..< count {
		k, k_err := compat_key_at(data, id, i)
		if k_err != .None { return k_err }
		if i > 0 && k < prev { return .Cell_Deserialize_Failed }
		prev = k
	}
	return .None
}

@(private = "file")
compat_page_table := Page_Layout_VTable {
	header_size       = compat_header_size,
	cell_count        = compat_cell_count,
	key_at            = compat_key_at,
	lower_bound_rowid = compat_lower_bound_rowid,
	slot_insert       = compat_slot_insert,
	slot_delete       = compat_slot_delete,
	cell_ptr_at       = compat_cell_ptr_at,
	slot_repoint      = compat_slot_repoint,
	validate          = compat_validate,
}

@(private)
compat_page_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &compat_page_table}
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
	validate          = v3_stub_validate,
}

dense_u64_interior_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &v3_stub_table}
}

slot_dir_leaf_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &v3_stub_table}
}

prefix_leaf_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &v3_stub_table}
}

prefix_interior_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &v3_stub_table}
}

@(private = "file")
columnar_readonly_count :: proc "contextless" (data: []u8, id: Page_Id) -> int {
	return get_cell_count(data, u32(id))
}

@(private = "file")
columnar_readonly_table := Page_Layout_VTable {
	header_size       = compat_header_size,
	cell_count        = columnar_readonly_count,
	key_at            = v3_stub_key_at,
	lower_bound_rowid = v3_stub_lower_bound,
	slot_insert       = v3_stub_insert,
	slot_delete       = v3_stub_delete,
	cell_ptr_at       = v3_stub_cell_ptr_at,
	slot_repoint      = v3_stub_repoint,
	validate          = compat_validate,
}

// columnar_readonly_layout serves test-only columnar pages: reads resolve,
// every write fails closed with .Unsupported_Format (never silent).
columnar_readonly_layout :: proc() -> Page_Layout {
	return Page_Layout{vtable = &columnar_readonly_table}
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
	if intrinsics.unlikely(hdr == nil) { return {}, {}, .Invalid_Page_Header }
	switch hdr.page_type {
	case .INTERIOR_TABLE:
		return compat_page_layout(), .Rowid, .None
	case .LEAF_TABLE:
		return compat_page_layout(), .Rowid, .None
	case .LEAF_TABLE_COLUMNAR:
		return columnar_readonly_layout(), .Rowid, .None
	case:
		return {}, {}, .Invalid_Page_Header
	}
}

// layout_for_version resolves the compat layout for a pager format version.
// V3 page kinds dispatch per-page via layout_for_page once their phases
// land; this stays the version-level entry until then.
// require_results: same nil-vtable hazard as layout_for_page.
@(require_results)
layout_for_version :: proc(version: u32) -> (Page_Layout, Error) {
	if version == 2 { return compat_page_layout(), .None }
	return {}, .Unsupported_Format
}
