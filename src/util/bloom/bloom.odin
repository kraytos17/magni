// Package bloom provides a fixed-capacity counting Bloom filter for
// cache-membership gating. Counters are 4-bit, packed two per byte; the filter
// is a value type (no allocation) sized entirely at compile time.
//
// Contract: add saturates and remove floors — counters never wrap. A wrap would
// turn into a false negative, so saturation is a correctness property, not a
// perf one. might_contain may report false positives, never false negatives.
package bloom

import "core:hash"

// Counting Bloom with COUNTERS cells and HASHES probes. At n = 256 live keys
// (the pager cache capacity) this is m/n = 16, k = 11 -> ~0.05% false-positive
// rate. COUNTERS must be a power of two so probe positions mask, not modulo.
COUNTER_BITS :: 4
COUNTERS     :: 4096
MAX_COUNT    :: (1 << COUNTER_BITS) - 1
HASHES       :: 11
BYTES        :: COUNTERS * COUNTER_BITS / 8

#assert(COUNTERS & (COUNTERS - 1) == 0)
#assert(BYTES * 8 == COUNTERS * COUNTER_BITS)

// Filter is a value type: fixed storage, no allocator. Holding it by pointer
// avoids copying 2 KiB; the filter owns nothing to free.
Filter :: struct {
	counters: [BYTES]u8, // two COUNTER_BITS counters per byte, low nibble first
}

// reset clears the filter to the empty set.
reset :: proc(f: ^Filter) {
	f.counters = {}
}

// add records key. Counters saturate at MAX_COUNT and never wrap.
add :: proc(f: ^Filter, key: u32) {
	h1, h2 := probe_seeds(key)
	for i in 0 ..< HASHES {
		pos := (h1 + u32(i) * h2) & (COUNTERS - 1)
		counter_add(f, pos, +1)
	}
}

// remove un-records key. Callers must pair every remove with a prior add
// (a remove for a key never added corrupts shared counters into false
// negatives); counters floor at zero otherwise.
remove :: proc(f: ^Filter, key: u32) {
	h1, h2 := probe_seeds(key)
	for i in 0 ..< HASHES {
		pos := (h1 + u32(i) * h2) & (COUNTERS - 1)
		counter_add(f, pos, -1)
	}
}

// might_contain reports whether key is possibly present. false is definitive
// (no false negatives); true may be a false positive. Early-outs on the first
// zero counter, so the common miss path touches few bytes.
might_contain :: proc(f: ^Filter, key: u32) -> bool {
	h1, h2 := probe_seeds(key)
	for i in 0 ..< HASHES {
		pos := (h1 + u32(i) * h2) & (COUNTERS - 1)
		if counter_get(f, pos) == 0 {
			return false
		}
	}
	return true
}

// probe_seeds derives the two double-hashing seeds (Kirsch-Mitzenmacher) from
// one 64-bit mix of the key. h2 is forced odd so that, with a power-of-two
// COUNTERS, the k probe positions never degenerate to a short cycle (the
// RocksDB fix).
@(private = "file")
probe_seeds :: proc(key: u32) -> (h1, h2: u32) {
	bytes := transmute([4]u8)key
	h := hash.fnv64a(bytes[:])
	return u32(h), u32(h >> 32) | 1
}

@(private = "file")
counter_get :: #force_inline proc(f: ^Filter, pos: u32) -> u8 {
	idx := pos >> 1
	b: u8
	#no_bounds_check { b = f.counters[idx] }
	return u8(b & 0x0F) if (pos & 1) == 0 else u8(b >> 4)
}

@(private = "file")
counter_add :: #force_inline proc(f: ^Filter, pos: u32, delta: int) {
	idx := pos >> 1
	lo := (pos & 1) == 0
	#no_bounds_check {
		b := f.counters[idx]
		nib := b & 0x0F if lo else b >> 4
		nib = u8(clamp(int(nib) + delta, 0, MAX_COUNT))
		f.counters[idx] = (b & 0xF0) | nib if lo else (b & 0x0F) | (nib << 4)
	}
}
