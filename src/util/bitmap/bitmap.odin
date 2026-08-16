// Package bitmap provides a generic growable bitset over a []u64 backing
// store. It has no dependency on the database engine; the pager uses it for
// free-page tracking.
package bitmap

// set marks bit pn as set (1).
set :: proc(bm: []u64, pn: u32) {
	idx := int(pn) / 64
	if idx < len(bm) { bm[idx] |= u64(1) << uint(pn % 64) }
}

// clear marks bit pn as clear (0).
clear :: proc(bm: []u64, pn: u32) {
	idx := int(pn) / 64
	if idx < len(bm) {
		mask := ~(u64(1) << uint(pn % 64))
		bm[idx] &= mask
	}
}

// test reports whether bit pn is set.
test :: proc(bm: []u64, pn: u32) -> bool {
	idx := int(pn) / 64
	return idx < len(bm) && (bm[idx] & (u64(1) << uint(pn % 64))) != 0
}

// grow returns a copy of old sized to hold at least max_pn bits, with any new
// words initialized to all-ones (allocated). The caller owns the returned slice.
grow :: proc(old: []u64, max_pn: u32, allocator := context.allocator) -> []u64 {
	needed := int(max_pn) / 64 + 1
	if needed > len(old) {
		if len(old) > 0 {
			needed = max(needed, len(old) * 2)
		}
		bm := make([]u64, needed, allocator)
		copy(bm, old)
		for i := len(old); i < needed; i += 1 { bm[i] = ~u64(0) }
		delete(old)
		return bm
	}
	return old
}
