// Package cell — secondary-int index key codec.
//
// Keys are single-column, numerically-ordered INTEGER: only i64 values
// encode; every other storage class (string, real, blob, Null) reports
// ok=false so the caller skips the entry instead of failing the row.
//
// Wire format (all multi-byte ints big-endian):
//   [0x49 'I'][value:u64be sign-biased][rowid:u64be sign-biased]
// Sign bias (XOR sign bit) makes unsigned word order equal signed numeric
// order, so keys sort by value first and rowid second deterministically
// (tiebreak for non-UNIQUE indexes; UNIQUE compares the value region only).
// Fixed 17-byte width: no length prefix, so raw memcmp of valid keys would
// order correctly — but compare stays structured (tag-checked, fail-closed)
// like the text codec instead of relying on it.
//
// All procs borrow (encode writes into caller buf, decode returns values,
// no borrows escape); no allocations and no context use ("contextless").
// Malformed input fails closed (false), never partial.
package cell

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:types"

INT_INDEX_TAG :: u8(0x49)
INT_INDEX_LEN :: 1 + 8 + 8 // tag + biased value + biased rowid
#assert(INT_INDEX_LEN == 17)

// int_index_encoded_len returns the wire length for an INTEGER value (always
// 17), or 0 for any non-i64 value (not indexed — caller skips).
int_index_encoded_len :: #force_inline proc "contextless" (val: types.Value) -> int {
	_, ok := val.(i64)
	return INT_INDEX_LEN if ok else 0
}

// int_index_encode writes the index key for (val, rowid) into buf and
// returns bytes written. ok=false when val is not i64 (skip the entry, not
// an error) or buf is too small. require_results is load-bearing: an
// unhandled false silently drops an index entry.
@(require_results)
int_index_encode :: proc "contextless" (
	val: types.Value,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	v, ok := val.(i64)
	if !ok {
		return 0, false
	}
	if intrinsics.unlikely(len(buf) < INT_INDEX_LEN) {
		return 0, false
	}

	buf[0] = INT_INDEX_TAG
	endian.unchecked_put_u64be(buf[1:9], u64(v ~ min(i64)))
	endian.unchecked_put_u64be(buf[9:17], u64(i64(rowid) ~ min(i64)))
	return INT_INDEX_LEN, true
}

// int_index_decode parses key bytes back into (value, rowid).
// Exact-length match: truncation or trailing garbage fails.
@(require_results)
int_index_decode :: proc "contextless" (src: []u8) -> (val: i64, rowid: types.Row_ID, ok: bool) {
	if intrinsics.unlikely(len(src) != INT_INDEX_LEN) {
		return 0, 0, false
	}
	if intrinsics.unlikely(src[0] != INT_INDEX_TAG) {
		return 0, 0, false
	}

	w := endian.unchecked_get_u64be(src[1:9])
	r := endian.unchecked_get_u64be(src[9:17])
	return i64(w) ~ min(i64), types.Row_ID(i64(r) ~ min(i64)), true
}

// int_index_split parses one index key at the head of src into the biased
// value and rowid words. Requires 17 <= len(src); trailing bytes are
// ignored so arena windows compare correctly. Fails closed on bad tag or
// short buffer.
int_index_split :: #force_inline proc "contextless" (
	src: []u8,
) -> (
	val_enc: u64,
	rowid_enc: u64,
	ok: bool,
) {
	if intrinsics.unlikely(len(src) < INT_INDEX_LEN) {
		return 0, 0, false
	}
	if intrinsics.unlikely(src[0] != INT_INDEX_TAG) {
		return 0, 0, false
	}
	return endian.unchecked_get_u64be(src[1:9]), endian.unchecked_get_u64be(src[9:17]), true
}

// int_index_compare orders full index keys: (value numeric, rowid numeric).
// -1/0/+1. Invalid inputs order before valid keys (never equal to one) so
// corruption sorts deterministically instead of reading out of bounds —
// same policy as text_index_compare.
// force_inline: called per probe inside binary search; inlining
// removes call overhead from the log-n comparison chain.
int_index_compare :: #force_inline proc "contextless" (a: []u8, b: []u8) -> int {
	va, ra, oka := int_index_split(a)
	vb, rb, okb := int_index_split(b)
	if intrinsics.unlikely(!oka || !okb) {
		if oka == okb {
			return mem.compare(a, b)
		}
		return 1 if oka else -1
	}
	if va != vb {
		return -1 if va < vb else 1
	}
	if ra != rb {
		return -1 if ra < rb else 1
	}
	return 0
}

// int_index_shared_prefix returns the common byte-prefix length of a and b,
// clamped to min(max_cap, len(a), len(b)); negative cap yields 0. Byte loop:
// 17-byte keys never justify the word fast path, and every index below is
// in range because cap is clamped to both lengths first.
int_index_shared_prefix :: #force_inline proc "contextless" (
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
