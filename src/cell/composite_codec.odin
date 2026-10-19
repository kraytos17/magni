// Package cell — composite secondary-index key codec.
//
// Keys are multi-column tuples over indexable storage classes (TEXT,
// INTEGER, REAL, in column order): every component must encode or the
// whole entry skips (a NULL anywhere makes the tuple unsearchable —
// NULL never equals — mirroring the single-column skip policy).
//
// Wire format (all multi-byte ints big-endian):
//   [0x43 'C'][component × N][rowid:u64be sign-biased]
// where each component is [kind:u8][len:u32be][payload] and payload is
// the codec value word (text bytes, biased int word, ordered real word).
// Length-prefix framing makes mem.compare order-correct: the first
// differing byte decides, and equal prefixes fall through to the next
// component, then the rowid tiebreak (deterministic for non-UNIQUE
// duplicates). Component kind bytes reuse the single-column codec tags,
// so the framing is self-describing.
//
// Order across kinds never arises: one index has fixed column types, so
// every key shares the component-kind sequence (the comparator still
// parses structure first and fails closed on malformed input, never
// partial).
//
// All procs borrow (encode writes into caller buf, no borrows escape); no
// allocations and no context use ("contextless").
package cell

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:types"

COMPOSITE_INDEX_TAG    :: u8(0x43)
COMPOSITE_COMP_HDR_LEN :: 1 + 4 // kind + len
COMPOSITE_ROWID_LEN    :: 8

// composite_comp_len returns the framed length of one component value, or
// 0 when the value is not indexable under its declared type (NULL,
// mismatched class, NaN real — caller skips the whole tuple).
composite_comp_len :: #force_inline proc "contextless" (
	val: types.Value,
	type: types.Column_Type,
) -> int {
	switch type {
	case .TEXT:
		if s, ok := val.(string); ok {
			return COMPOSITE_COMP_HDR_LEN + len(s)
		}
	case .INTEGER:
		if _, ok := val.(i64); ok {
			return COMPOSITE_COMP_HDR_LEN + 8
		}
	case .REAL:
		if v, ok := val.(f64); ok && real_index_encodable(v) {
			return COMPOSITE_COMP_HDR_LEN + 8
		}
	case .BLOB:
	}
	return 0
}

// composite_encoded_len returns the full wire length for a tuple
// (tag + framed components + rowid), or 0 when any component is not
// indexable (caller skips the entry, not an error).
composite_encoded_len :: proc "contextless" (
	vals: []types.Value,
	col_types: []types.Column_Type,
) -> int {
	if len(vals) != len(col_types) || len(vals) == 0 {
		return 0
	}

	total := 1 + COMPOSITE_ROWID_LEN
	for v, i in vals {
		cl := composite_comp_len(v, col_types[i])
		if cl == 0 {
			return 0
		}
		total += cl
	}
	return total
}

// composite_encode_raw writes the framed components (no tag, no rowid)
// for (vals, col_types) into buf and returns bytes written. ok=false when
// the arity mismatches, any component is unindexable, or buf is too small.
// Shared by composite_encode (full keys) and tree-layer raw builders,
// which frame values before the tree wraps tag + rowid.
@(require_results)
composite_encode_raw :: proc "contextless" (
	vals: []types.Value,
	col_types: []types.Column_Type,
	buf: []u8,
) -> (
	int,
	bool,
) {
	n := 0
	for v, i in vals {
		if i >= len(col_types) {
			return 0, false
		}

		cl := composite_comp_len(v, col_types[i])
		if cl == 0 {
			return 0, false
		}
		n += cl
	}
	if len(vals) != len(col_types) || len(vals) == 0 {
		return 0, false
	}
	if intrinsics.unlikely(len(buf) < n) {
		return 0, false
	}

	pos := 0
	for v, i in vals {
		payload: []u8
		tag := TEXT_INDEX_TAG
		switch col_types[i] {
		case .TEXT:
			tag = TEXT_INDEX_TAG
			payload = transmute([]u8)v.(string)
		case .INTEGER:
			tag = INT_INDEX_TAG
			w := u64(v.(i64) ~ min(i64))
			endian.unchecked_put_u64be(
				buf[pos + COMPOSITE_COMP_HDR_LEN:pos + COMPOSITE_COMP_HDR_LEN + 8],
				w,
			)
			payload = buf[pos + COMPOSITE_COMP_HDR_LEN:pos + COMPOSITE_COMP_HDR_LEN + 8]
		case .REAL:
			tag = REAL_INDEX_TAG
			endian.unchecked_put_u64be(
				buf[pos + COMPOSITE_COMP_HDR_LEN:pos + COMPOSITE_COMP_HDR_LEN + 8],
				real_sort_word(v.(f64)),
			)
			payload = buf[pos + COMPOSITE_COMP_HDR_LEN:pos + COMPOSITE_COMP_HDR_LEN + 8]
		case .BLOB:
			return 0, false
		}

		buf[pos] = tag
		endian.unchecked_put_u32be(buf[pos + 1:pos + 5], u32(len(payload)))
		if col_types[i] == .TEXT {
			copy(buf[pos + COMPOSITE_COMP_HDR_LEN:], payload)
		}
		pos += COMPOSITE_COMP_HDR_LEN + len(payload)
	}
	return n, true
}

// composite_encode writes the index key for (vals, rowid) into buf and
// returns bytes written. ok=false when the arity mismatches, any
// component is unindexable, or buf is too small. require_results is
// load-bearing: an unhandled false silently drops an index entry.
@(require_results)
composite_encode :: proc "contextless" (
	vals: []types.Value,
	col_types: []types.Column_Type,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	n := composite_encoded_len(vals, col_types)
	if n == 0 || intrinsics.unlikely(len(buf) < n) {
		return 0, false
	}

	buf[0] = COMPOSITE_INDEX_TAG
	raw_n, raw_ok := composite_encode_raw(vals, col_types, buf[1:n - COMPOSITE_ROWID_LEN])
	if !raw_ok {
		return 0, false
	}

	endian.unchecked_put_u64be(buf[1 + raw_n:1 + raw_n + 8], u64(i64(rowid) ~ min(i64)))
	return n, true
}

// composite_wellformed reports whether src parses as a complete composite
// key: tag, exactly-framed components, and the trailing rowid with no
// trailing garbage. Separator validation flows through this so corrupt
// bytes fail closed instead of misordering.
composite_wellformed :: proc "contextless" (src: []u8) -> bool {
	if len(src) < 1 + COMPOSITE_COMP_HDR_LEN + COMPOSITE_ROWID_LEN {
		return false
	}
	if src[0] != COMPOSITE_INDEX_TAG {
		return false
	}

	pos := 1
	for pos < len(src) {
		// Rowid tail: exactly 8 bytes must remain and the loop ends.
		if len(src) - pos == COMPOSITE_ROWID_LEN {
			return true
		}
		if len(src) - pos < COMPOSITE_COMP_HDR_LEN + COMPOSITE_ROWID_LEN {
			return false
		}

		kind := src[pos]
		if kind != TEXT_INDEX_TAG && kind != INT_INDEX_TAG && kind != REAL_INDEX_TAG {
			return false
		}

		elen := int(endian.unchecked_get_u32be(src[pos + 1:pos + 5]))
		if kind != TEXT_INDEX_TAG && elen != 8 {
			return false
		}
		pos += COMPOSITE_COMP_HDR_LEN + elen
	}
	return false
}

// composite_compare orders full composite keys: framing order first (which
// is component order by construction), rowid numeric last. -1/0/+1.
// Malformed inputs order before valid keys (never equal to one) so
// corruption sorts deterministically instead of reading out of bounds —
// same policy as the single-column comparators.
// force_inline: called per probe inside binary search; inlining
// removes call overhead from the log-n comparison chain.
composite_compare :: #force_inline proc "contextless" (a: []u8, b: []u8) -> int {
	oka := composite_wellformed(a)
	okb := composite_wellformed(b)
	if intrinsics.unlikely(!oka || !okb) {
		if oka == okb {
			return mem.compare(a, b)
		}
		return 1 if oka else -1
	}
	return mem.compare(a, b)
}

// composite_shared_prefix returns the common byte-prefix length of a and b,
// clamped to min(max_cap, len(a), len(b)); negative cap yields 0. Byte loop:
// composite keys are variable-length, and every index below is in range
// because cap is clamped to both lengths first.
composite_shared_prefix :: #force_inline proc "contextless" (
	a: []u8,
	b: []u8,
	max_cap: int,
) -> int {
	cap := min(max_cap, len(a), len(b))
	if cap <= 0 {
		return 0
	}

	n := 0
	#no_bounds_check for n < cap && a[n] == b[n] {
		n += 1
	}
	return n
}
