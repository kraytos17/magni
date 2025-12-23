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
	bytes_read = 0
	for shift < 64 && bytes_read < 9 {
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
	}
	return 0, 0, false
}

// Calculate varint size for a given value
varint_size :: proc(v: u64) -> int {
	if v < (1 << 7) do return 1
	if v < (1 << 14) do return 2
	if v < (1 << 21) do return 3
	if v < (1 << 28) do return 4
	if v < (1 << 35) do return 5
	if v < (1 << 42) do return 6
	if v < (1 << 49) do return 7
	if v < (1 << 56) do return 8
	return 9
}

read_u16_le :: proc(data: []u8, offset: int) -> (u16, bool) {
	if offset + 2 > len(data) {
		return 0, false
	}
	val, _ := endian.get_u16(data[offset:], .Little)
	return val, true
}

read_u32_le :: proc(data: []u8, offset: int) -> (u32, bool) {
	if offset + 4 > len(data) {
		return 0, false
	}
	val, _ := endian.get_u32(data[offset:], .Little)
	return val, true
}

read_u64_le :: proc(data: []u8, offset: int) -> (u64, bool) {
	if offset + 8 > len(data) {
		return 0, false
	}
	val, _ := endian.get_u64(data[offset:], .Little)
	return val, true
}

write_u16_le :: proc(dest: []u8, offset: int, value: u16) -> bool {
	if offset + 2 > len(dest) {
		return false
	}
	endian.put_u16(dest[offset:], .Little, value)
	return true
}

write_u32_le :: proc(dest: []u8, offset: int, value: u32) -> bool {
	if offset + 4 > len(dest) {
		return false
	}
	endian.put_u32(dest[offset:], .Little, value)
	return true
}

write_u64_le :: proc(dest: []u8, offset: int, value: u64) -> bool {
	if offset + 8 > len(dest) {
		return false
	}
	endian.put_u64(dest[offset:], .Little, value)
	return true
}

write_f64_be :: proc(dest: []u8, offset: int, value: f64) -> bool {
	if offset + 8 > len(dest) {
		return false
	}
	endian.put_f64(dest[offset:], .Big, value)
	return true
}

read_f64_be :: proc(data: []u8, offset: int) -> (f64, bool) {
	if offset + 8 > len(data) {
		return 0, false
	}
	val, _ := endian.get_f64(data[offset:], .Big)
	return val, true
}

// Read integer based on serial type size
read_int_by_size :: proc(data: []u8, offset: int, size: int) -> (i64, bool) {
	if offset + size > len(data) {
		return 0, false
	}

	switch size {
	case 1:
		val := i8(data[offset])
		return i64(val), true
	case 2:
		raw, ok := read_u16_le(data, offset)
		if !ok do return 0, false
		return i64(i16(raw)), true
	case 3:
		// 24-bit signed integer
		if offset + 3 > len(data) {
			return 0, false
		}

		low_raw, ok1 := read_u16_le(data, offset)
		if !ok1 do return 0, false

		low := u32(low_raw)
		high := u32(data[offset + 2])
		val := low | (high << 16)

		// Sign extend from 24-bit to 32-bit, then to 64-bit
		if val & 0x800000 != 0 {
			val |= 0xFF000000
		}
		return i64(i32(val)), true
	case 4:
		raw, ok := read_u32_le(data, offset)
		if !ok do return 0, false
		return i64(i32(raw)), true
	case 6:
		// 48-bit signed integer
		if offset + 6 > len(data) {
			return 0, false
		}
		lo_raw, ok1 := read_u32_le(data, offset)
		hi_raw, ok2 := read_u16_le(data, offset + 4)
		if !ok1 || !ok2 do return 0, false

		lo := u64(lo_raw)
		hi := u64(hi_raw)
		val := lo | (hi << 32)

		// Sign extend from 48-bit to 64-bit
		if val & 0x800000000000 != 0 {
			val |= 0xFFFF000000000000
		}
		return i64(val), true
	case 8:
		raw, ok := read_u64_le(data, offset)
		if !ok do return 0, false
		return i64(raw), true
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
		// Write 24-bit integer as 16-bit + 8-bit
		ok := write_u16_le(dest, offset, u16(value))
		if !ok do return false
		dest[offset + 2] = u8(value >> 16)
		return true
	case 4:
		return write_u32_le(dest, offset, u32(value))
	case 6:
		// Write 48-bit integer as 32-bit + 16-bit
		ok1 := write_u32_le(dest, offset, u32(value))
		ok2 := write_u16_le(dest, offset + 4, u16(value >> 32))
		return ok1 && ok2
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
		if val == 0 {
			return u64(types.Serial_Type.ZERO)
		}
		if val == 1 {
			return u64(types.Serial_Type.ONE)
		}

		abs_val := abs(val)
		if abs_val < (1 << 7) {
			return u64(types.Serial_Type.INT8)
		}
		if abs_val < (1 << 15) {
			return u64(types.Serial_Type.INT16)
		}
		if abs_val < (1 << 23) {
			return u64(types.Serial_Type.INT24)
		}
		if abs_val < (1 << 31) {
			return u64(types.Serial_Type.INT32)
		}
		if abs_val < (1 << 47) {
			return u64(types.Serial_Type.INT48)
		}
		return u64(types.Serial_Type.INT64)
	case f64:
		return u64(types.Serial_Type.FLOAT64)
	case string:
		// TEXT: serial type = 13 + 2*N where N is string length
		n := len(val) * 2 + 13
		return u64(n)
	case []u8:
		// BLOB: serial type = 12 + 2*N where N is blob length
		n := len(val) * 2 + 12
		return u64(n)
	}
	unreachable()
}

// Get content length from serial type
content_length_from_serial :: proc(serial: u64) -> int {
	if serial >= 12 {
		if serial % 2 == 0 {
			// BLOB (even): length = (serial - 12) / 2
			return int((serial - 12) / 2)
		} else {
			// TEXT (odd): length = (serial - 13) / 2
			return int((serial - 13) / 2)
		}
	}
	return 0
}

// Check if serial type represents TEXT
is_text_serial :: proc(serial: u64) -> bool {
	return serial >= 13 && serial & 1 == 1
}

// Check if serial type represents BLOB
is_blob_serial :: proc(serial: u64) -> bool {
	return serial >= 12 && serial & 1 == 0
}

// Debug helper to print bytes
debug_print_bytes :: proc(label: string, data: []u8, max: int = 64) {
	fmt.printf("%s (%d bytes): ", label, len(data))
	limit := min(max, len(data))
	for i in 0 ..< limit {
		fmt.printf("%02X ", data[i])
	}
	if len(data) > max {
		fmt.print("...")
	}
	fmt.println()
}
