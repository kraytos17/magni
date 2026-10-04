// Package varint — unsigned LEB128 variable-length integers over byte
// slices. No allocations; callers own dest/src. decode fails closed.
package varint

// encode writes value as a LEB128 varint into dest. Returns bytes written,
// or 0 when dest is too small.
encode :: proc(dest: []u8, value: u64) -> int {
	v := value
	i := 0
	for {
		if i >= len(dest) {
			return 0
		}

		b := u8(v & 0x7F)
		v >>= 7
		if v != 0 {
			#no_bounds_check { dest[i] = b | 0x80 }
			i += 1
		} else {
			#no_bounds_check { dest[i] = b }
			i += 1
			break
		}
	}
	return i
}

// decode reads a LEB128 varint from src at offset. Returns the value, bytes
// consumed, and ok=false on truncated input or an overlong encoding (more
// than 9 bytes). Reads never pass the returned count.
decode :: proc(src: []u8, offset: int = 0) -> (value: u64, bytes_read: int, ok: bool) {
	if offset >= len(src) {
		return 0, 0, false
	}

	shift: u32
	pos := offset
	for shift < 64 {
		if pos >= len(src) {
			return 0, 0, false
		}

		b: u64
		#no_bounds_check {
			b = u64(src[pos])
		}

		pos += 1
		bytes_read += 1
		value |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return value, bytes_read, true
		}

		shift += 7
		if bytes_read >= 9 {
			return 0, 0, false
		}
	}
	return 0, 0, false
}

// size returns the LEB128 byte width for v: 1–9 bytes.
size :: proc(v: u64) -> int {
	switch {
	case v < (1 << 7):
		return 1
	case v < (1 << 14):
		return 2
	case v < (1 << 21):
		return 3
	case v < (1 << 28):
		return 4
	case v < (1 << 35):
		return 5
	case v < (1 << 42):
		return 6
	case v < (1 << 49):
		return 7
	case v < (1 << 56):
		return 8
	case:
		return 9
	}
}
