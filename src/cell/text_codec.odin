// Package cell — secondary-text index key codec.
//
// Keys are single-column, BINARY-collated TEXT: only string values encode;
// every other storage class (int, real, blob, Null) reports ok=false so the
// caller skips the entry instead of failing the row. The tag byte reserves
// space for future composite keys.
//
// Wire format (all multi-byte ints big-endian):
//   [0x54 'T'][len:u32be][text bytes][rowid:u64be sign-biased]
// Sign bias (XOR sign bit) makes unsigned word order equal signed numeric
// order, so same-text keys sort by rowid deterministically (tiebreak for
// non-UNIQUE indexes; UNIQUE compares the text region only).
//
// Key order is defined by text_index_compare (text bytes, then rowid), NOT
// by raw memcmp of the encoding: the length prefix would otherwise impose
// length-first order.
//
// All procs borrow (encode writes into caller buf, decode borrows src); no
// allocations and no context use ("contextless"). Malformed input fails
// closed (false), never partial.
package cell

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:types"

TEXT_INDEX_TAG        :: u8(0x54)
TEXT_INDEX_PREFIX_LEN :: 1 + 4 // tag + len
TEXT_INDEX_ROWID_LEN  :: 8
TEXT_INDEX_MIN_LEN    :: TEXT_INDEX_PREFIX_LEN + TEXT_INDEX_ROWID_LEN
#assert(TEXT_INDEX_MIN_LEN == 13)

// text_index_encoded_len returns the wire length for a TEXT value, or 0 for
// any non-TEXT value (not indexed — caller skips). Pure observer.
text_index_encoded_len :: #force_inline proc "contextless" (val: types.Value) -> int {
	s, ok := val.(string)
	if !ok {
		return 0
	}
	return TEXT_INDEX_PREFIX_LEN + len(s) + TEXT_INDEX_ROWID_LEN
}

// text_index_encode writes the index key for (val, rowid) into buf and
// returns bytes written. ok=false when val is not TEXT (skip the entry, not
// an error) or buf is too small. require_results is load-bearing: an
// unhandled false silently drops an index entry.
@(require_results)
text_index_encode :: proc "contextless" (
	val: types.Value,
	rowid: types.Row_ID,
	buf: []u8,
) -> (
	int,
	bool,
) {
	s, ok := val.(string)
	if !ok {
		return 0, false
	}

	need := TEXT_INDEX_PREFIX_LEN + len(s) + TEXT_INDEX_ROWID_LEN
	if intrinsics.unlikely(len(buf) < need) {
		return 0, false
	}

	buf[0] = TEXT_INDEX_TAG
	n := len(s)
	buf[1] = u8(n >> 24)
	buf[2] = u8(n >> 16)
	buf[3] = u8(n >> 8)
	buf[4] = u8(n)
	copy(buf[5:], s)

	w := u64(i64(rowid) ~ min(i64))
	off := 5 + n
	endian.unchecked_put_u64be(buf[off:off + 8], w)
	return need, true
}

// text_index_decode parses key bytes back into (text borrow, rowid).
// The text slice borrows src — valid while the page is pinned only.
// Exact-length match: truncation or trailing garbage fails.
@(require_results)
text_index_decode :: proc "contextless" (
	src: []u8,
) -> (
	text: string,
	rowid: types.Row_ID,
	ok: bool,
) {
	if intrinsics.unlikely(len(src) < TEXT_INDEX_MIN_LEN) {
		return "", 0, false
	}
	if intrinsics.unlikely(src[0] != TEXT_INDEX_TAG) {
		return "", 0, false
	}

	n := int(endian.unchecked_get_u32be(src[1:5]))
	if intrinsics.unlikely(n < 0 || TEXT_INDEX_PREFIX_LEN + n + TEXT_INDEX_ROWID_LEN != len(src)) {
		return "", 0, false
	}

	off := TEXT_INDEX_PREFIX_LEN + n
	w := endian.unchecked_get_u64be(src[off:off + 8])
	text = string(src[TEXT_INDEX_PREFIX_LEN:TEXT_INDEX_PREFIX_LEN + n])
	rowid = types.Row_ID(i64(w) ~ min(i64))
	ok = true
	return
}

// text_index_split parses one index key at the head of src: the text slice
// (borrowed) and the biased rowid word. Requires 5+n+8 <= len(src); trailing
// bytes are ignored so arena windows compare correctly. Fails closed on
// bad tag, short buffer, or absurd length.
text_index_split :: #force_inline proc "contextless" (
	src: []u8,
) -> (
	text: []u8,
	rowid_enc: u64,
	ok: bool,
) {
	if intrinsics.unlikely(len(src) < TEXT_INDEX_MIN_LEN) {
		return nil, 0, false
	}
	if intrinsics.unlikely(src[0] != TEXT_INDEX_TAG) {
		return nil, 0, false
	}

	n := int(endian.unchecked_get_u32be(src[1:5]))
	if intrinsics.unlikely(n < 0 || TEXT_INDEX_PREFIX_LEN + n + TEXT_INDEX_ROWID_LEN > len(src)) {
		return nil, 0, false
	}

	off := TEXT_INDEX_PREFIX_LEN + n
	w := endian.unchecked_get_u64be(src[off:off + 8])
	return src[TEXT_INDEX_PREFIX_LEN:TEXT_INDEX_PREFIX_LEN + n], w, true
}

// text_index_compare orders full index keys: (text BINARY, rowid numeric).
// -1/0/+1. Structured, not raw memcmp: the u32be length prefix would
// otherwise impose length-first order ("b" < "aa" wrongly). Invalid inputs
// order before valid keys (never equal to one) so corruption sorts
// deterministically instead of reading out of bounds.
// force_inline: called per probe inside binary search; inlining
// removes call overhead from the log-n comparison chain.
text_index_compare :: #force_inline proc "contextless" (a: []u8, b: []u8) -> int {
	ta, wa, oka := text_index_split(a)
	tb, wb, okb := text_index_split(b)
	if intrinsics.unlikely(!oka || !okb) {
		if oka == okb {
			return mem.compare(a, b)
		}
		return 1 if oka else -1
	}
	if r := mem.compare(ta, tb); r != 0 {
		return r
	}
	if wa != wb {
		return -1 if wa < wb else 1
	}
	return 0
}

// text_index_shared_prefix returns the common byte-prefix length of a and b,
// clamped to min(max_cap, len(a), len(b)); negative cap yields 0. Word-at-a-
// time fast path with a byte tail (same result as a naive loop, fewer
// iterations on long prefixes). Every index below is in range because cap is
// clamped to both lengths first.
text_index_shared_prefix :: #force_inline proc "contextless" (
	a: []u8,
	b: []u8,
	max_cap: int,
) -> int {
	cap := min(max_cap, len(a), len(b))
	if cap <= 0 {
		return 0
	}

	n := 0
	#no_bounds_check {
		for n + 8 <= cap {
			av :=
				u64(a[n]) << 56 |
				u64(a[n + 1]) << 48 |
				u64(a[n + 2]) << 40 |
				u64(a[n + 3]) << 32 |
				u64(a[n + 4]) << 24 |
				u64(a[n + 5]) << 16 |
				u64(a[n + 6]) << 8 |
				u64(a[n + 7])
			bv :=
				u64(b[n]) << 56 |
				u64(b[n + 1]) << 48 |
				u64(b[n + 2]) << 40 |
				u64(b[n + 3]) << 32 |
				u64(b[n + 4]) << 24 |
				u64(b[n + 5]) << 16 |
				u64(b[n + 6]) << 8 |
				u64(b[n + 7])
			if av != bv {
				break
			}
			n += 8
		}
		for n < cap && a[n] == b[n] {
			n += 1
		}
	}
	return n
}

// text_index_has_prefix reports whether text starts with prefix (BINARY).
// Feeds LIKE 'abc%' prefix-range planning; leading-wildcard patterns never
// reach here (the caller checks the first char).
text_index_has_prefix :: #force_inline proc "contextless" (text: []u8, prefix: []u8) -> bool {
	if len(prefix) > len(text) {
		return false
	}
	return mem.compare(text[:len(prefix)], prefix) == 0
}
