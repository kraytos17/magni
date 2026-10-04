package tests

import "core:testing"
import "src:util/bloom"

// Counting-bloom unit tests: exact add/remove round-trips, saturation safety
// (a counter wrap would be a false negative), the refresh-no-leak property the
// pager relies on, and a measured false-positive rate close to theory.

@(test)
test_bloom_empty_and_roundtrip :: proc(t: ^testing.T) {
	f: bloom.Filter
	for k in u32(1) ..= 300 {
		testing.expect(t, !bloom.might_contain(&f, k), "empty filter must be negative")
	}
	for k in u32(1) ..= 256 {
		bloom.add(&f, k)
	}
	for k in u32(1) ..= 256 {
		testing.expect(t, bloom.might_contain(&f, k), "added key must be present")
	}
	for k in u32(1) ..= 256 {
		bloom.remove(&f, k)
	}
	for k in u32(1) ..= 256 {
		testing.expect(t, !bloom.might_contain(&f, k), "removed key must be absent")
	}
}

@(test)
test_bloom_refresh_does_not_leak :: proc(t: ^testing.T) {
	// The pager calls cache_insert repeatedly for the same page; only the first
	// (new-bucket) insert may touch the bloom. Re-adding here would leak
	// counters upward toward saturation. Adding once then reading counters must
	// stay at the single-add value.
	f: bloom.Filter
	bloom.add(&f, 42)
	before := f.counters
	bloom.remove(&f, 42)
	bloom.add(&f, 42) // net effect of add/remove/add == one add
	testing.expect(t, f.counters == before, "single add must be idempotent-ish")

	// Many adds of the same key must saturate, not wrap.
	bloom.reset(&f)
	for _ in 0 ..< 100 {
		bloom.add(&f, 7)
	}

	bloom.remove(&f, 7)
	testing.expect(t, bloom.might_contain(&f, 7), "saturated counter must not wrap to zero")
}

@(test)
test_bloom_remove_floors_at_zero :: proc(t: ^testing.T) {
	// A remove for a key never added must not underflow into a wrapped high
	// counter (that would corrupt the shared cells for other keys).
	f: bloom.Filter
	bloom.remove(&f, 999) // never added
	for k in u32(1) ..= 256 {
		bloom.add(&f, k)
	}
	for k in u32(1) ..= 256 {
		testing.expect(t, bloom.might_contain(&f, k), "post-underflow add must still register")
	}
}

@(test)
test_bloom_measured_false_positive_rate :: proc(t: ^testing.T) {
	// Theory at n=256, m=4096, k=11 is ~0.05%. Allow generous headroom (<=1%)
	// for hash quality on this key pattern; the point is "small", and the test
	// guards against a hash bug that would make it O(1).
	f: bloom.Filter
	for k in u32(1) ..= 256 {
		bloom.add(&f, k)
	}

	fp := 0
	trials := 20000
	for k in u32(100000) ..< u32(100000 + trials) {
		if bloom.might_contain(&f, k) {
			fp += 1
		}
	}

	rate := f64(fp) / f64(trials)
	testing.expect(t, rate <= 0.01, "false-positive rate must stay near theory")
}
