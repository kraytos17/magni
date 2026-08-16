package cell

import "core:encoding/endian"
import "core:strings"
import "src:util/varint"
import "src:types"

Serialization_Info :: struct {
	serial_types_size: int,
	payload_size:      int,
	total_size:        int,
}

compute_info :: proc(rowid: types.Row_ID, values: []types.Value) -> Serialization_Info {
	info: Serialization_Info
	for val in values {
		serial := serial_type_for_value(val)
		info.serial_types_size += varint.size(serial)
		content_size, _ := types.serial_type_content_size(serial)
		info.payload_size += content_size
	}

	header_bytes :=
		varint.size(u64(rowid)) + varint.size(u64(info.serial_types_size)) + info.serial_types_size
	total_payload := header_bytes + info.payload_size
	info.total_size = varint.size(u64(total_payload)) + total_payload
	return info
}

// Serialize a row into the binary cell format at dest. Info must come from compute_info.
// Returns bytes written and ok=false if dest is too small.
serialize :: proc(
	dest: []u8,
	rowid: types.Row_ID,
	values: []types.Value,
	info: Serialization_Info,
) -> (
	bytes_written: int,
	ok: bool,
) {
	if len(dest) < info.total_size { return 0, false }

	offset := 0
	header_bytes :=
		varint.size(u64(rowid)) + varint.size(u64(info.serial_types_size)) + info.serial_types_size

	total_payload := header_bytes + info.payload_size
	offset += varint.encode(dest[offset:], u64(total_payload))
	offset += varint.encode(dest[offset:], u64(rowid))
	offset += varint.encode(dest[offset:], u64(info.serial_types_size))
	serial_types: [types.MAX_COLS]u64
	i := 0
	for val in values {
		serial := serial_type_for_value(val)
		serial_types[i] = serial; i += 1
		offset += varint.encode(dest[offset:], serial)
	}

	i = 0
	for val in values {
		serial := serial_types[i]
		i += 1
		switch v in val {
		case types.Null:
		case i64:
			if serial != u64(types.Serial_Type.ZERO) && serial != u64(types.Serial_Type.ONE) {
				size, _ := types.serial_type_content_size(serial)
				write_int_by_size(dest, offset, v, size)
				offset += size
			}
		case f64:
			endian.put_f64(dest[offset:], .Big, v)
			offset += 8
		case string:
			copy(dest[offset:], v)
			offset += len(v)
		case []u8:
			copy(dest[offset:], v)
			offset += len(v)
		}
	}
	return offset, true
}

// Deserialize a binary cell from src at offset. Config controls allocator and zero-copy mode.
// Returns the Cell + bytes consumed. ok=false on invalid input.
deserialize :: proc(
	src: []u8,
	offset := 0,
	config := Config{},
) -> (
	cell: Cell,
	bytes_consumed: int,
	ok: bool,
) {
	if offset >= len(src) {
		return {}, 0, false
	}

	alloc := config.allocator
	if alloc.procedure == nil {
		alloc = context.allocator
	}

	pos := offset
	_, n, ok_payload := varint.decode(src, pos)
	if !ok_payload { return {}, 0, false }
	pos += n

	rowid_val, n2, ok_rowid := varint.decode(src, pos)
	if !ok_rowid { return {}, 0, false }
	pos += n2

	header_size, n3, ok_header := varint.decode(src, pos)
	if !ok_header { return {}, 0, false }
	pos += n3

	header_start := pos
	serial_types: [types.MAX_COLS]u64
	serial_count := 0

	for pos < header_start + int(header_size) && serial_count < types.MAX_COLS {
		st, n4, ok_st := varint.decode(src, pos)
		if !ok_st { return {}, 0, false }
		serial_types[serial_count] = st
		serial_count += 1
		pos += n4
	}

	result_values := make([]types.Value, serial_count, alloc)

	success := false
	defer if !success && !config.zero_copy {
		types.values_delete(result_values, alloc)
	}

	for st_idx in 0 ..< serial_count {
		st := serial_types[st_idx]
		content_size, _ := types.serial_type_content_size(st)
		type_code := types.Serial_Type(st)
		if pos + content_size > len(src) {
			return {}, 0, false
		}
		if type_code == .ZERO {
			result_values[st_idx] = types.value_int(0)
		} else if type_code == .ONE {
			result_values[st_idx] = types.value_int(1)
		} else if st == u64(types.Serial_Type.NULL) {
			result_values[st_idx] = types.value_null()
		} else if st >= u64(types.Serial_Type.INT8) && st <= u64(types.Serial_Type.INT64) {
			int_val, _ := read_int_by_size(src, pos, content_size)
			result_values[st_idx] = types.value_int(int_val)
			pos += content_size
		} else if type_code == .FLOAT64 {
			float_val, _ := endian.get_f64(src[pos:], .Big)
			result_values[st_idx] = types.value_real(float_val)
			pos += 8
		} else if is_text_serial(st) {
			text_bytes := src[pos:pos + content_size]
			if config.zero_copy {
				result_values[st_idx] = types.value_text(string(text_bytes))
			} else {
				str := strings.clone_from(text_bytes, alloc)
				result_values[st_idx] = types.value_text(str)
			}
			pos += content_size
		} else if is_blob_serial(st) {
			blob_bytes := src[pos:pos + content_size]
			if config.zero_copy {
				result_values[st_idx] = types.value_blob(blob_bytes)
			} else {
				blob_copy := make([]u8, content_size, alloc)
				copy(blob_copy, blob_bytes)
				result_values[st_idx] = types.value_blob(blob_copy)
			}
			pos += content_size
		} else {
			return {}, 0, false
		}
	}

	success = true
	cell = Cell {
		rowid     = types.Row_ID(rowid_val),
		values    = result_values,
		owns_data = !config.zero_copy,
	}
	return cell, pos - offset, true
}

read_int_by_size :: proc(data: []u8, offset: int, size: int) -> (val: i64, ok: bool) {
	if offset + size > len(data) { return 0, false }
	switch size {
	case 1:
		return i64(i8(data[offset])), true
	case 2:
		return i64(i16(endian.get_u16(data[offset:], .Little) or_return)), true
	case 3:
		v := i64(data[offset]) | (i64(data[offset + 1]) << 8) | (i64(data[offset + 2]) << 16)
		if v & 0x800000 != 0 { v |= ~i64(0xFFFFFF) }
		return v, true
	case 4:
		return i64(i32(endian.get_u32(data[offset:], .Little) or_return)), true
	case 6:
		lo := endian.get_u32(data[offset:], .Little) or_return
		hi := endian.get_u16(data[offset + 4:], .Little) or_return
		v := i64(lo) | (i64(hi) << 32)
		if v & 0x8000_0000_0000 != 0 { v |= ~i64(0xFFFF_FFFF_FFFF) }
		return v, true
	case 8:
		return i64(endian.get_u64(data[offset:], .Little) or_return), true
	}
	return 0, false
}

write_int_by_size :: proc(dest: []u8, offset: int, value: i64, size: int) -> bool {
	if offset + size > len(dest) { return false }
	switch size {
	case 1:
		dest[offset] = u8(value); return true
	case 2:
		return endian.put_u16(dest[offset:], .Little, u16(value))
	case 3:
		endian.put_u16(dest[offset:], .Little, u16(value))
		dest[offset + 2] = u8(value >> 16)
		return true
	case 4:
		return endian.put_u32(dest[offset:], .Little, u32(value))
	case 6:
		endian.put_u32(dest[offset:], .Little, u32(value))
		endian.put_u16(dest[offset + 4:], .Little, u16(value >> 32))
		return true
	case 8:
		return endian.put_u64(dest[offset:], .Little, u64(value))
	}
	return false
}

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
		return u64(len(val) * 2 + 13)
	case []u8:
		return u64(len(val) * 2 + 12)
	case:
		return u64(types.Serial_Type.NULL)
	}
}

is_text_serial :: proc(serial: u64) -> bool { return serial >= 13 && (serial % 2 != 0) }
is_blob_serial :: proc(serial: u64) -> bool { return serial >= 12 && (serial % 2 == 0) }
