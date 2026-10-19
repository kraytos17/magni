// Package cell — secondary-real index key codec.
//
// Keys are single-column, numerically-ordered REAL: only f64 values
// encode; every other storage class (string, int, blob, Null) reports
// ok=false so the caller skips the entry instead of failing the row.
// NaN reports ok=false too (unreachable via literals, which admit no NaN
// form — skipping keeps the index total-ordered by construction).
//
// Wire format (all multi-byte ints big-endian):
//   [0x46 'F'][value:u64be order-preserving][rowid:u64be sign-biased]
// The value word is the order-preserving transform (negatives bitwise-not,
// positives sign-bit-flipped), so unsigned word order equals cmp_f64 order
// and keys sort by value first and rowid second deterministically
// (tiebreak for non-UNIQUE indexes; UNIQUE compares the value region only).
// -0.0 normalizes to +0.0 before transform (cmp_f64 equates them, so the
// index must too — otherwise Eq(-0.0) would miss +0.0 rows the filter
// matches). Fixed 17-byte width, same geometry as the int codec, so the
// shared prefix-compressed pages serve it unchanged.
//
// All procs borrow (encode writes into caller buf, decode returns values,
// no borrows escape); no allocations and no context use ("contextless").
// Malformed input fails closed (false), never partial.
package cell

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:types"

REAL_INDEX_TAG :: u8(0x46)
REAL_INDEX_LEN :: 1 + 8 + 8 // tag + ordered value + biased rowid
#assert(REAL_INDEX_LEN == 17)

// REAL_NEG_ZERO is the only input real_sort_word rewrites (-0.0 → +0.0).
@(private = "file")
REAL_NEG_ZERO :: u64(0x8000000000000000)

// REAL_SIGN is the IEEE-754 binary64 sign bit, the pivot of the transform.
@(private = "file")
REAL_SIGN :: u64(0x8000000000000000)

// real_sort_word maps v to its order-preserving word: bitwise-not for
// negatives (reverses magnitude order into ascending), sign-bit-flip for
// positives and zeros. ±Inf land at the extremes by bit pattern; NaN is
// the caller's to exclude (real_index_encodable), never normalized here.
real_sort_word :: #force_inline proc "contextless" (v: f64) -> u64 {
	u := transmute(u64)v
	if u == REAL_NEG_ZERO {
		u = 0
	}
	if u & REAL_SIGN != 0 {
		return ~u
	}
	return u ~ REAL_SIGN
}

// real_index_encodable reports whether v may enter a real index: any f64
// except NaN (unordered under every comparison the recheck can express,
// so no bound could delimit it — skipping mirrors the NULL policy).
real_index_encodable :: #force_inline proc "contextless" (v: f64) -> bool {
	u := transmute(u64)v
	return (u & 0x7ff0000000000000) != 0x7ff0000000000000 || (u & 0x000fffffffffffff) == 0
}

// real_index_encoded_len returns the wire length for a REAL value (always
// 17), or 0 for any non-f64 value (not indexed — caller skips).
real_index_encoded_len :: #force_inline proc "contextless" (val: types.Value) -> int {
	v, ok := val.(f64)
	if !ok || !real_index_encodable(v) {
		return 0
	}
	return REAL_INDEX_LEN
}

// real_index_encode writes the index key for (val, rowid) into buf and
// returns bytes written. ok=false when val is not an encodable f64 (skip
// the entry, not an error) or buf is too small. require_results is
// load-bearing: an unhandled false silently drops an index entry.
@(require_results)
real_index_encode :: proc "contextless" (
	val: types.Value,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	v, ok := val.(f64)
	if !ok || !real_index_encodable(v) {
		return 0, false
	}
	if intrinsics.unlikely(len(buf) < REAL_INDEX_LEN) {
		return 0, false
	}

	buf[0] = REAL_INDEX_TAG
	endian.unchecked_put_u64be(buf[1:9], real_sort_word(v))
	endian.unchecked_put_u64be(buf[9:17], u64(i64(rowid) ~ min(i64)))
	return REAL_INDEX_LEN, true
}

// real_index_decode parses key bytes back into (value, rowid).
// Exact-length match: truncation or trailing garbage fails.
@(require_results)
real_index_decode :: proc "contextless" (src: []u8) -> (val: f64, rowid: types.Row_ID, ok: bool) {
	if intrinsics.unlikely(len(src) != REAL_INDEX_LEN) {
		return 0, 0, false
	}
	if intrinsics.unlikely(src[0] != REAL_INDEX_TAG) {
		return 0, 0, false
	}

	r := endian.unchecked_get_u64be(src[9:17])
	return real_index_decode_value(src[1:9]), types.Row_ID(i64(r) ~ min(i64)), true
}

// real_index_decode_value maps one 8-byte ordered value word back to its
// f64: the word-half inverse of real_index_encode (caller guarantees the
// length — make_index_key checks before calling).
real_index_decode_value :: #force_inline proc "contextless" (raw: []u8) -> f64 {
	w := endian.unchecked_get_u64be(raw)
	if w & REAL_SIGN == 0 {
		w = ~w
	} else {
		w ~= REAL_SIGN
	}
	return transmute(f64)w
}

// real_index_split parses one index key at the head of src into the ordered
// value and rowid words. Requires 17 <= len(src); trailing bytes are
// ignored so arena windows compare correctly. Fails closed on bad tag or
// short buffer.
real_index_split :: #force_inline proc "contextless" (
	src: []u8,
) -> (
	val_enc: u64,
	rowid_enc: u64,
	ok: bool,
) {
	if intrinsics.unlikely(len(src) < REAL_INDEX_LEN) {
		return 0, 0, false
	}
	if intrinsics.unlikely(src[0] != REAL_INDEX_TAG) {
		return 0, 0, false
	}
	return endian.unchecked_get_u64be(src[1:9]), endian.unchecked_get_u64be(src[9:17]), true
}

// real_index_compare orders full index keys: (value numeric, rowid numeric).
// -1/0/+1. Invalid inputs order before valid keys (never equal to one) so
// corruption sorts deterministically instead of reading out of bounds —
// same policy as int_index_compare.
// force_inline: called per probe inside binary search; inlining
// removes call overhead from the log-n comparison chain.
real_index_compare :: #force_inline proc "contextless" (a: []u8, b: []u8) -> int {
	va, ra, oka := real_index_split(a)
	vb, rb, okb := real_index_split(b)
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

// real_index_shared_prefix returns the common byte-prefix length of a and b,
// clamped to min(max_cap, len(a), len(b)); negative cap yields 0. Byte loop:
// 17-byte keys never justify the word fast path, and every index below is
// in range because cap is clamped to both lengths first.
real_index_shared_prefix :: #force_inline proc "contextless" (
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
