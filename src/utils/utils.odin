package utils

import "core:encoding/endian"
import "core:fmt"
import "src:types"

// Encode a u64 into varint format
// Returns the number of bytes written
varint_encode :: proc(dest: []u8, value: u64) -> int {
	v := value
	i := 0
	for {
		if i >= len(dest) {
			return 0
		}

		b := u8(v & 0x7F)
		v >>= 7
		if v != 0 {
			dest[i] = b | 0x80
			i += 1
		} else {
			dest[i] = b
			i += 1
			break
		}
	}
	return i
}

// Decode a varint from src, starting at offset
// Returns the value, number of bytes consumed, and success flag
// Fails if varint is malformed (> 9 bytes) or buffer is too small
varint_decode :: proc(src: []u8, offset: int = 0) -> (value: u64, bytes_read: int, ok: bool) {
	if offset >= len(src) {
		return 0, 0, false
	}

	value = 0
	shift: u32 = 0
	pos := offset
	for shift < 64 {
		if pos >= len(src) {
			return 0, 0, false
		}

		b := u64(src[pos])
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

// Calculate varint size for a given value
varint_size :: proc(v: u64) -> int {
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

read_u16_le :: proc(data: []u8, offset: int) -> (val: u16, ok: bool) {
	if offset >= len(data) do return 0, false
	return endian.get_u16(data[offset:], .Little)
}

read_u32_le :: proc(data: []u8, offset: int) -> (val: u32, ok: bool) {
	if offset >= len(data) do return 0, false
	return endian.get_u32(data[offset:], .Little)
}

read_u64_le :: proc(data: []u8, offset: int) -> (val: u64, ok: bool) {
	if offset >= len(data) do return 0, false
	return endian.get_u64(data[offset:], .Little)
}

read_f64_be :: proc(data: []u8, offset: int) -> (val: f64, ok: bool) {
	if offset >= len(data) do return 0, false
	return endian.get_f64(data[offset:], .Big)
}

write_u16_le :: proc(dest: []u8, offset: int, value: u16) -> bool {
	if offset + 2 > len(dest) do return false
	endian.put_u16(dest[offset:], .Little, value)
	return true
}

write_u32_le :: proc(dest: []u8, offset: int, value: u32) -> bool {
	if offset + 4 > len(dest) do return false
	endian.put_u32(dest[offset:], .Little, value)
	return true
}

write_u64_le :: proc(dest: []u8, offset: int, value: u64) -> bool {
	if offset + 8 > len(dest) do return false
	endian.put_u64(dest[offset:], .Little, value)
	return true
}

write_f64_be :: proc(dest: []u8, offset: int, value: f64) -> bool {
	if offset + 8 > len(dest) do return false
	endian.put_f64(dest[offset:], .Big, value)
	return true
}

// Read integer based on serial type size
read_int_by_size :: proc(data: []u8, offset: int, size: int) -> (val: i64, ok: bool) {
	if offset + size > len(data) {
		return 0, false
	}

	switch size {
	case 1:
		return i64(i8(data[offset])), true
	case 2:
		v := endian.get_u16(data[offset:], .Little) or_return
		return i64(i16(v)), true
	case 3:
		b0 := i64(data[offset])
		b1 := i64(data[offset + 1])
		b2 := i64(data[offset + 2])
		v := b0 | (b1 << 8) | (b2 << 16)
		if v & 0x800000 != 0 {
			v |= ~i64(0xFFFFFF)
		}
		return v, true
	case 4:
		v := endian.get_u32(data[offset:], .Little) or_return
		return i64(i32(v)), true
	case 6:
		lo := endian.get_u32(data[offset:], .Little) or_return
		hi := endian.get_u16(data[offset + 4:], .Little) or_return
		v := i64(lo) | (i64(hi) << 32)
		if v & 0x8000_0000_0000 != 0 {
			v |= ~i64(0xFFFF_FFFF_FFFF)
		}
		return v, true
	case 8:
		v := endian.get_u64(data[offset:], .Little) or_return
		return i64(v), true
	}
	return 0, false
}

// Write integer based on size
write_int_by_size :: proc(dest: []u8, offset: int, value: i64, size: int) -> bool {
	if offset + size > len(dest) {
		return false
	}

	switch size {
	case 1:
		dest[offset] = u8(value)
		return true
	case 2:
		return write_u16_le(dest, offset, u16(value))
	case 3:
		write_u16_le(dest, offset, u16(value))
		dest[offset + 2] = u8(value >> 16)
		return true
	case 4:
		return write_u32_le(dest, offset, u32(value))
	case 6:
		write_u32_le(dest, offset, u32(value))
		write_u16_le(dest, offset + 4, u16(value >> 32))
		return true
	case 8:
		return write_u64_le(dest, offset, u64(value))
	}
	return false
}

// Determine serial type for a value
serial_type_for_value :: proc(v: types.Value) -> u64 {
	switch val in v {
	case types.Null:
		return u64(types.Serial_Type.NULL)
	case i64:
		switch {
		case val == 0:
			return u64(types.Serial_Type.ZERO)
		case val == 1:
			return u64(types.Serial_Type.ONE)
		}

		abs_val := abs(val)
		switch {
		case abs_val < (1 << 7):
			return u64(types.Serial_Type.INT8)
		case abs_val < (1 << 15):
			return u64(types.Serial_Type.INT16)
		case abs_val < (1 << 23):
			return u64(types.Serial_Type.INT24)
		case abs_val < (1 << 31):
			return u64(types.Serial_Type.INT32)
		case abs_val < (1 << 47):
			return u64(types.Serial_Type.INT48)
		case:
			return u64(types.Serial_Type.INT64)
		}
	case f64:
		return u64(types.Serial_Type.FLOAT64)
	case string:
		// TEXT: 13 + 2*N
		return u64(len(val) * 2 + 13)
	case []u8:
		// BLOB: 12 + 2*N
		return u64(len(val) * 2 + 12)
	}
	unreachable()
}

// Get content length from serial type
content_length_from_serial :: proc(serial: u64) -> int {
	if serial >= 12 {
		// BLOB (even) or TEXT (odd)
		// BLOB: (N-12)/2, TEXT: (N-13)/2
		sub := u64(12) if serial % 2 == 0 else u64(13)
		return int((serial - sub) / 2)
	}
	return 0
}

is_text_serial :: proc(serial: u64) -> bool {
	return serial >= 13 && (serial % 2 != 0)
}

is_blob_serial :: proc(serial: u64) -> bool {
	return serial >= 12 && (serial % 2 == 0)
}

// Debug helper to print bytes
debug_print_bytes :: proc(label: string, data: []u8, max_len: int = 64) {
	fmt.printf("%s (%d bytes): ", label, len(data))
	limit := min(max_len, len(data))
	for i in 0 ..< limit {
		fmt.printf("%02X ", data[i])
	}

	if len(data) > max_len {
		fmt.print("...")
	}
	fmt.println()
}
