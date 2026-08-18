// Package varint provides LEB128-style variable-length integer encoding.
// It operates purely on []u8 <-> u64
package varint

// encode writes value as a LEB128 varint into dest, returning the number of
// bytes written (0 if dest is too small).
encode :: proc(dest: []u8, value: u64) -> int {
	v := value
	i := 0
	for {
		if i >= len(dest) { return 0 }
		b := u8(v & 0x7F)
		v >>= 7
		if v != 0 { dest[i] = b | 0x80; i += 1 } else { dest[i] = b; i += 1; break }
	}
	return i
}

// decode reads a LEB128 varint from src starting at offset. Returns the value,
// the number of bytes consumed, and ok=false on truncated or malformed input.
decode :: proc(src: []u8, offset: int = 0) -> (value: u64, bytes_read: int, ok: bool) {
	if offset >= len(src) { return 0, 0, false }
	shift: u32; pos := offset
	for shift < 64 {
		if pos >= len(src) { return 0, 0, false }

		b := u64(src[pos]); pos += 1; bytes_read += 1
		value |= (b & 0x7F) << shift
		if (b & 0x80) == 0 { return value, bytes_read, true }

		shift += 7
		if bytes_read >= 9 { return 0, 0, false }
	}
	return 0, 0, false
}

// size returns the number of bytes encode will write for value.
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
