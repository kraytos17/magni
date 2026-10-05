// Package btree — dense page codec (INTERIOR_DENSE, LEAF_SLOTDIR slots).
//
// All dense arrays are little-endian (native loads, SIMD-friendly). Every
// proc operates on caller-supplied buffers and is contextless; fallible
// procs carry require_results; page indexing stays bounds-checked because
// counts come from on-disk bytes (corrupt counts must trap, never read out
// of bounds).
package btree

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:types"

// DENSE_FLAG_FOR selects u32le delta keys (base + delta) over full u64le
// keys. Decided per page at split time; unknown flag bits fail closed.
DENSE_FLAG_FOR :: 0x01

DENSE_FULL_KEY_WIDTH :: 8
DENSE_DELTA_WIDTH    :: 4
DENSE_CHILD_WIDTH    :: 4

// Dense_Interior_Header is the 24-byte header of an INTERIOR_DENSE page:
// dense sorted keys, then (count+1) children. cell_count with the usual
// meaning (separator count). cell_content_offset is fixed at PAGE_SIZE
// (no content area); first_freeblock/fragmented_bytes are fixed at 0.
Dense_Interior_Header :: struct #packed {
	using common : Page_Header, // 8: type=6, count, content_off=PAGE_SIZE
	rightmost_ptr: u32le, // 4: child holding keys >= last separator
	flags        : u16le, // 2: DENSE_FLAG_FOR bit; rest must be zero
	base         : u64le, // 8: FOR base (== min key); zeroed when !FOR
	reserved     : u16le, // 2: zeroed; validated (fail closed on reuse)
}
#assert(size_of(Dense_Interior_Header) == 24)

// Slot is the 10-byte entry of a LEAF_SLOTDIR page: rowid-first order for
// dense comparison. rowid is sign-biased (rowid_bias_encode) so raw u64
// order == numeric order.
Slot :: struct #packed {
	rowid: u64le, // 8: biased Row_ID
	off  : u16le, // 2: cell-area offset
}
#assert(size_of(Slot) == 10) // slot capacity math assumes 10-byte entries

// rowid_bias_encode/decode is the canonical order-preserving bias:
// encoded u64 = u64(i64 ^ MIN_I64), so unsigned byte order == signed
// numeric order (negatives sort first). layout_iface's rowid codec
// delegates here (single source of truth).
rowid_bias_encode :: #force_inline proc "contextless" (v: types.Row_ID) -> u64 {
	return u64(i64(v) ~ min(i64))
}

rowid_bias_decode :: #force_inline proc "contextless" (w: u64) -> types.Row_ID {
	return types.Row_ID(i64(w) ~ min(i64))
}

// get_dense_interior_header reads the dense header with a length check.
// Public like get_leaf_header (tests assert header fields directly).
get_dense_interior_header :: proc "contextless" (
	data: []u8,
	page_id: u32,
) -> ^Dense_Interior_Header {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Dense_Interior_Header) {
		return nil
	}
	return (^Dense_Interior_Header)(raw_data(data[off:]))
}

// dense_geometry validates a dense interior page once and reports its
// section geometry: the single validation point every accessor uses.
// keys_off/children_off are byte offsets into data (page-relative).
@(private, require_results)
dense_geometry :: proc "contextless" (
	data: []u8,
	id: Page_Id,
) -> (
	keys_off: int,
	children_off: int,
	count: int,
	use_for: bool,
	base: u64,
	err: Error,
) {
	h := get_dense_interior_header(data, u32(id))
	if h == nil {
		return 0, 0, 0, false, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(h.page_type != .INTERIOR_DENSE) {
		return 0, 0, 0, false, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(u16(h.flags) & ~u16(DENSE_FLAG_FOR) != 0) {
		return 0, 0, 0, false, 0, .Unsupported_Format
	}
	if intrinsics.unlikely(h.reserved != 0) {
		return 0, 0, 0, false, 0, .Cell_Deserialize_Failed
	}

	n := int(h.cell_count)
	kwidth :=
		DENSE_DELTA_WIDTH if u16(h.flags) & u16(DENSE_FLAG_FOR) != 0 else DENSE_FULL_KEY_WIDTH

	keys_end := size_of(Dense_Interior_Header) + n * kwidth
	children_end := keys_end + (n + 1) * DENSE_CHILD_WIDTH
	off := get_page_header_offset(u32(id))
	if intrinsics.unlikely(keys_end > PAGE_SIZE || children_end > PAGE_SIZE) {
		return 0, 0, 0, false, 0, .Cell_Deserialize_Failed
	}
	if intrinsics.unlikely(off + children_end > len(data)) {
		return 0, 0, 0, false, 0, .Invalid_Page_Header
	}
	return off + size_of(Dense_Interior_Header),
		off + keys_end,
		n,
		u16(h.flags) & u16(DENSE_FLAG_FOR) != 0,
		u64(h.base),
		.None
}

// dense_key_at returns the separator key at slot i (unbiased Row_ID).
// OOB index or short buffer fails.
@(require_results)
dense_key_at :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (types.Row_ID, Error) {
	keys_off, _, count, use_for, base, g_err := dense_geometry(data, id)
	if g_err != .None {
		return 0, g_err
	}
	if intrinsics.unlikely(i < 0 || i >= count) {
		return 0, .Cell_Not_Found
	}
	if use_for {
		delta, ok := endian.get_u32(data[keys_off + i * DENSE_DELTA_WIDTH:], .Little)
		if !ok {
			return 0, .Cell_Deserialize_Failed
		}
		return rowid_bias_decode(base + u64(delta)), .None
	}

	w, ok := endian.get_u64(data[keys_off + i * DENSE_FULL_KEY_WIDTH:], .Little)
	if !ok {
		return 0, .Cell_Deserialize_Failed
	}
	return rowid_bias_decode(w), .None
}

// dense_child_at returns child page i. Valid range is 0..=count (slot
// count holds the rightmost child) — note the asymmetry with key_at.
@(require_results)
dense_child_at :: proc "contextless" (data: []u8, id: Page_Id, i: int) -> (u32, Error) {
	_, children_off, count, _, _, g_err := dense_geometry(data, id)
	if g_err != .None {
		return 0, g_err
	}
	if intrinsics.unlikely(i < 0 || i > count) {
		return 0, .Cell_Not_Found
	}

	child, ok := endian.get_u32(data[children_off + i * DENSE_CHILD_WIDTH:], .Little)
	if !ok {
		return 0, .Cell_Deserialize_Failed
	}
	return child, .None
}

// dense_lower_bound_u64 is the branchless lower bound over an in-memory
// biased-key slice: no data-dependent early exit, cmov-friendly shape
// (backend lowers the if/else to predicated moves at -o:aggressive).
// Pure/static so tests hammer it without page scaffolding (correctness
// pinned against a linear oracle in btree_v3_test); the page search below
// implements the same lower-bound contract over page bytes.
// force_inline: melts into the probe loop at each call site.
dense_lower_bound_u64 :: #force_inline proc "contextless" (keys: []u64, target: u64) -> int {
	pos := 0
	n := len(keys)
	for n > 0 {
		half := n >> 1
		mid := pos + half
		if keys[mid] < target {
			pos = mid + 1
			n -= half + 1
		} else {
			n = half
		}
	}
	return pos
}

// dense_page_lower_bound routes key to its child slot on a dense interior
// page: first separator >= key (rightmost child when past the end).
// Checked word loads per probe.
@(require_results)
dense_page_lower_bound :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	key: types.Row_ID,
) -> (
	int,
	Error,
) {
	keys_off, _, count, use_for, base, g_err := dense_geometry(data, id)
	if g_err != .None {
		return 0, g_err
	}

	target := rowid_bias_encode(key)
	left, right := 0, count
	for left < right {
		mid := left + (right - left) / 2
		probe: u64
		if use_for {
			delta, ok := endian.get_u32(data[keys_off + mid * DENSE_DELTA_WIDTH:], .Little)
			if !ok {
				return left, .Cell_Deserialize_Failed
			}
			probe = base + u64(delta)
		} else {
			w, ok := endian.get_u64(data[keys_off + mid * DENSE_FULL_KEY_WIDTH:], .Little)
			if !ok {
				return left, .Cell_Deserialize_Failed
			}
			probe = w
		}

		if probe < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, .None
}

// dense_choose_encoding applies the FOR rule: deltas iff max-min fits u32.
// Returns (use_for, base). Unordered input (max < min) reports no-FOR.
dense_choose_encoding :: #force_inline proc "contextless" (
	min_v: types.Row_ID,
	max_v: types.Row_ID,
) -> (
	use_for: bool,
	base: u64,
) {
	if max_v < min_v {
		return false, 0
	}

	diff := u64(i64(max_v) - i64(min_v))
	if diff <= u64(max(u32)) {
		return true, rowid_bias_encode(min_v)
	}
	return false, 0
}

// dense_split_mid is the split point convention for fixed-width int
// keys: the median.
dense_split_mid :: #force_inline proc "contextless" (n: int) -> int {
	return n / 2
}

// dense_build_from_sorted writes a complete dense interior page from sorted
// keys with children[0..n] (last = rightmost): init + encoding choice +
// fill, self-checked by validate at the end. Used by splits and bulk loads.
// Empty keys are valid (single-child page). Unsorted input fails
// (sortedness is the caller's contract; per-key range checks make
// corruption loud, never silently truncated).
@(require_results)
dense_build_from_sorted :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	keys: []types.Row_ID,
	children: []u32,
) -> Error {
	n := len(keys)
	if len(children) != n + 1 {
		return .Invalid_Bounds
	}
	if n > int(max(u16)) {
		return .Page_Full
	}

	use_for, base := false, u64(0)
	if n > 0 {
		use_for, base = dense_choose_encoding(keys[0], keys[n - 1])
	}

	kwidth := DENSE_DELTA_WIDTH if use_for else DENSE_FULL_KEY_WIDTH
	off := get_page_header_offset(u32(id))
	if off + size_of(Dense_Interior_Header) + n * kwidth + (n + 1) * DENSE_CHILD_WIDTH >
	   len(data) {
		return .Page_Full
	}
	if !init_dense_interior_page(data, u32(id)) {
		return .Invalid_Page_Header
	}

	h := get_dense_interior_header(data, u32(id))
	if h == nil {
		return .Invalid_Page_Header
	}

	h.cell_count = u16le(n)
	if use_for {
		h.flags = u16le(DENSE_FLAG_FOR)
		h.base = u64le(base)
	}

	keys_off := off + size_of(Dense_Interior_Header)
	for k, i in keys {
		biased := rowid_bias_encode(k)
		if use_for {
			// Per-key guard: endpoints chose the encoding, but only this
			// proves every key fits (unsorted input must fail, not wrap).
			if biased < base || biased - base > u64(max(u32)) {
				return .Cell_Deserialize_Failed
			}
			if !endian.put_u32(data[keys_off + i * kwidth:], .Little, u32(biased - base)) {
				return .Serialization_Failed
			}
		} else {
			if !endian.put_u64(data[keys_off + i * kwidth:], .Little, biased) {
				return .Serialization_Failed
			}
		}
	}

	children_off := keys_off + n * kwidth
	for c, i in children {
		if !endian.put_u32(data[children_off + i * DENSE_CHILD_WIDTH:], .Little, c) {
			return .Serialization_Failed
		}
	}

	h.rightmost_ptr = u32le(children[n])
	return validate_dense_interior(data, id)
}

// slot_at reads slot i of a slotdir leaf: (rowid, cell offset).
@(require_results)
slot_at :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	i: int,
) -> (
	rowid: types.Row_ID,
	off: u16,
	err: Error,
) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return 0, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(hdr.page_type != .LEAF_SLOTDIR) {
		return 0, 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(i < 0 || i >= int(hdr.cell_count)) {
		return 0, 0, .Cell_Not_Found
	}

	off0 := get_page_header_offset(u32(id))
	hdr_sz := page_header_size(hdr.page_type)
	start := off0 + hdr_sz
	// Span check: count comes from on-disk bytes — corrupt counts fail here,
	// never slice out of bounds (release builds elide slice checks).
	if intrinsics.unlikely(start + (i + 1) * size_of(Slot) > len(data)) {
		return 0, 0, .Cell_Deserialize_Failed
	}

	entry := (^Slot)(raw_data(data[start + i * size_of(Slot):]))
	return rowid_bias_decode(u64(entry.rowid)), u16(entry.off), .None
}

// slot_lower_bound is the scalar-obviously-correct binary search over slot
// rowids.
@(require_results)
slot_lower_bound :: proc "contextless" (
	data: []u8,
	id: Page_Id,
	target: types.Row_ID,
) -> (
	int,
	Error,
) {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return 0, .Invalid_Page_Header
	}
	if hdr.page_type != .LEAF_SLOTDIR {
		return 0, .Invalid_Page_Header
	}

	count := int(hdr.cell_count)
	left, right := 0, count
	for left < right {
		mid := left + (right - left) / 2
		k, _, k_err := slot_at(data, id, mid)
		if k_err != .None {
			return left, k_err
		}
		if k < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, .None
}

// init_dense_interior_page zeroes and headers a fresh INTERIOR_DENSE page.
// Public like init_slot_leaf_page (tests build pages through it). Returns false
// on a short buffer instead of writing out of bounds.
@(require_results)
init_dense_interior_page :: proc "contextless" (data: []u8, page_id: u32) -> bool {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Dense_Interior_Header) {
		return false
	}

	mem.zero_slice(data[off:])
	header := (^Dense_Interior_Header)(raw_data(data[off:]))
	header.page_type = .INTERIOR_DENSE
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
	header.rightmost_ptr = u32le(0)
	header.flags = 0
	header.base = u64le(0)
	header.reserved = 0
	return true
}

// init_slot_leaf_page zeroes and headers a fresh LEAF_SLOTDIR page.
// Same short-buffer contract as init_dense_interior_page.
@(require_results)
init_slot_leaf_page :: proc "contextless" (data: []u8, page_id: u32) -> bool {
	off := get_page_header_offset(page_id)
	if len(data) < off + size_of(Leaf_Header) {
		return false
	}

	mem.zero_slice(data[off:])
	header := (^Leaf_Header)(raw_data(data[off:]))
	header.page_type = .LEAF_SLOTDIR
	header.first_freeblock = 0
	header.cell_count = 0
	header.cell_content_offset = PAGE_SIZE
	header.fragmented_bytes = 0
	return true
}

// validate_dense_interior checks sortedness, bounds, FOR range, and fixed
// fields. It never rejects a page the writers can validly produce:
// nonzero-FOR-range with the !FOR flag is the encoder's choice, not an
// error; count == 0 is tolerated (transient empty interiors exist).
@(require_results)
validate_dense_interior :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	h := get_dense_interior_header(data, u32(id))
	if h == nil {
		return .Invalid_Page_Header
	}
	if h.page_type != .INTERIOR_DENSE {
		return .Invalid_Page_Header
	}
	if u16(h.flags) & ~u16(DENSE_FLAG_FOR) != 0 {
		return .Unsupported_Format
	}
	if h.reserved != 0 {
		return .Cell_Deserialize_Failed
	}
	if h.first_freeblock != 0 || h.cell_content_offset != u16le(PAGE_SIZE) {
		return .Cell_Deserialize_Failed
	}

	n := int(h.cell_count)
	kwidth :=
		DENSE_DELTA_WIDTH if u16(h.flags) & u16(DENSE_FLAG_FOR) != 0 else DENSE_FULL_KEY_WIDTH

	if size_of(Dense_Interior_Header) + n * kwidth + (n + 1) * DENSE_CHILD_WIDTH > PAGE_SIZE {
		return .Cell_Deserialize_Failed
	}

	prev_biased: u64 = 0
	for i in 0 ..< n {
		k, k_err := dense_key_at(data, id, i)
		if k_err != .None {
			return k_err
		}

		biased := rowid_bias_encode(k)
		if i > 0 && biased < prev_biased {
			return .Cell_Deserialize_Failed
		}
		if u16(h.flags) & u16(DENSE_FLAG_FOR) != 0 {
			if biased < u64(h.base) || biased - u64(h.base) > u64(max(u32)) {
				return .Cell_Deserialize_Failed
			}
		}

		prev_biased = biased
		if _, c_err := dense_child_at(data, id, i); c_err != .None {
			return c_err
		}
	}
	if _, c_err := dense_child_at(data, id, n); c_err != .None {
		return c_err
	}
	return .None
}

// validate_slot_leaf checks slot sortedness and offset bounds. Offsets must
// land in the cell area [entry_end, PAGE_SIZE); cell bodies themselves are
// not decoded (layout stays cell-agnostic).
@(require_results)
validate_slot_leaf :: proc "contextless" (data: []u8, id: Page_Id) -> Error {
	hdr := get_header(data, u32(id))
	if hdr == nil {
		return .Invalid_Page_Header
	}
	if hdr.page_type != .LEAF_SLOTDIR {
		return .Invalid_Page_Header
	}

	off0 := get_page_header_offset(u32(id))
	entry_end := off0 + size_of(Leaf_Header) + int(hdr.cell_count) * size_of(Slot)
	if int(hdr.cell_content_offset) < entry_end || int(hdr.cell_content_offset) > PAGE_SIZE {
		return .Cell_Deserialize_Failed
	}

	prev: types.Row_ID = min(types.Row_ID)
	count := int(hdr.cell_count)
	for i in 0 ..< count {
		k, off, k_err := slot_at(data, id, i)
		if k_err != .None {
			return k_err
		}
		if i > 0 && k < prev {
			return .Cell_Deserialize_Failed
		}

		prev = k
		if int(off) < entry_end || int(off) >= PAGE_SIZE {
			return .Cell_Deserialize_Failed
		}
	}
	return .None
}
