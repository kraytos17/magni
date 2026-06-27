// Package cell handles serialization/deserialization of rows (cells) to/from binary format.
package cell

import "core:encoding/endian"
import "core:fmt"
import "core:mem"
import "core:strings"
import "src:types"

// Cell represents a serialized row (Record).
//
// MEMORY MODEL:
// 1. `values` slice: Always allocated (owned by Cell).
// 2. `string/[]u8` content: Owned if `owns_data` is true. Unowned (pointers to Page) if false.
Cell :: struct {
	rowid:     types.Row_ID,
	values:    []types.Value,
	owns_data: bool,
}

Config :: types.Storage_Config

create :: proc(
	rowid: types.Row_ID,
	values: []types.Value,
	allocator := context.allocator,
) -> (
	Cell,
	mem.Allocator_Error,
) {
	values_copy := make([]types.Value, len(values), allocator)
	if values_copy == nil && len(values) > 0 { return {}, .Out_Of_Memory }
	for val, i in values {
		cloned, err := types.value_clone(val, allocator)
		if err != nil {
			for j in 0 ..< i {
				types.value_delete(values_copy[j])
			}
			delete(values_copy, allocator)
			return {}, err
		}
		values_copy[i] = cloned
	}
	return Cell{rowid = rowid, values = values_copy, owns_data = true}, nil
}

destroy :: proc(c: ^Cell, allocator := context.allocator) {
	if c.values == nil {
		return
	}
	if c.owns_data {
		for val in c.values {
			types.value_delete(val, allocator)
		}
	}
	delete(c.values, allocator)
	c.values = nil
}

Serialization_Info :: struct {
	serial_types_size: int,
	payload_size:      int,
	total_size:        int,
}

compute_info :: proc(rowid: types.Row_ID, values: []types.Value) -> Serialization_Info {
	info: Serialization_Info
	for val in values {
		serial := serial_type_for_value(val)
		info.serial_types_size += varint_size(serial)
		content_size, _ := types.serial_type_content_size(serial)
		info.payload_size += content_size
	}

	header_bytes :=
		varint_size(u64(rowid)) + varint_size(u64(info.serial_types_size)) + info.serial_types_size
	total_payload := header_bytes + info.payload_size
	info.total_size = varint_size(u64(total_payload)) + total_payload
	return info
}

calculate_size :: proc(rowid: types.Row_ID, values: []types.Value) -> int {
	return compute_info(rowid, values).total_size
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
		varint_size(u64(rowid)) + varint_size(u64(info.serial_types_size)) + info.serial_types_size

	total_payload := header_bytes + info.payload_size
	offset += varint_encode(dest[offset:], u64(total_payload))
	offset += varint_encode(dest[offset:], u64(rowid))
	offset += varint_encode(dest[offset:], u64(info.serial_types_size))
	for val in values {
		serial := serial_type_for_value(val)
		offset += varint_encode(dest[offset:], serial)
	}
	for val in values {
		serial := serial_type_for_value(val)
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
	_, n, ok_payload := varint_decode(src, pos)
	if !ok_payload { return {}, 0, false }
	pos += n

	rowid_val, n2, ok_rowid := varint_decode(src, pos)
	if !ok_rowid { return {}, 0, false }
	pos += n2

	header_size, n3, ok_header := varint_decode(src, pos)
	if !ok_header { return {}, 0, false }
	pos += n3

	header_start := pos
	serial_types: [types.MAX_COLS]u64
	serial_count := 0

	for pos < header_start + int(header_size) && serial_count < types.MAX_COLS {
		st, n4, ok_st := varint_decode(src, pos)
		if !ok_st { return {}, 0, false }
		serial_types[serial_count] = st
		serial_count += 1
		pos += n4
	}

	result_values := make([]types.Value, serial_count, alloc)
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

	cell = Cell {
		rowid     = types.Row_ID(rowid_val),
		values    = result_values,
		owns_data = !config.zero_copy,
	}
	return cell, pos - offset, true
}

get_rowid :: proc(src: []u8, offset := 0) -> (types.Row_ID, bool) {
	if offset >= len(src) { return 0, false }
	pos := offset
	_, n, ok := varint_decode(src, pos)
	if !ok { return 0, false }

	pos += n
	rowid, _, ok2 := varint_decode(src, pos)
	if !ok2 { return 0, false }
	return types.Row_ID(rowid), true
}

get_size :: proc(src: []u8, offset := 0) -> (int, bool) {
	if offset >= len(src) { return 0, false }
	payload_size, n, ok := varint_decode(src, offset)
	if !ok { return 0, false }
	return n + int(payload_size), true
}

debug_print :: proc(c: Cell) {
	fmt.printf("Cell(rowid=%d, owned=%t, values=[", c.rowid, c.owns_data)
	for val, i in c.values {
		if i > 0 { fmt.print(", ") }
		fmt.print(types.value_to_string(val))
	}
	fmt.println("])")
}

validate :: proc(values: []types.Value, columns: []types.Column) -> bool {
	if len(values) != len(columns) { return false }
	for val, i in values {
		col := columns[i]
		if col.not_null && types.is_null(val) { return false }
		if types.is_null(val) { continue }
		switch col.type {
		case .INTEGER:
			if _, ok := val.(i64); !ok { return false }
		case .REAL:
			_, is_real := val.(f64)
			_, is_int := val.(i64)
			if !is_real && !is_int { return false }
		case .TEXT:
			_, is_text := val.(string)
			_, is_blob := val.([]u8)
			if !is_text && !is_blob { return false }
		case .BLOB:
			_, is_blob := val.([]u8)
			_, is_text := val.(string)
			if !is_blob && !is_text { return false }
		}
	}
	return true
}

varint_encode :: proc(dest: []u8, value: u64) -> int {
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

varint_decode :: proc(src: []u8, offset: int = 0) -> (value: u64, bytes_read: int, ok: bool) {
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

content_length_from_serial :: proc(serial: u64) -> int {
	if serial >= 12 {
		sub := u64(12) if serial % 2 == 0 else u64(13)
		return int((serial - sub) / 2)
	}
	return 0
}

is_text_serial :: proc(serial: u64) -> bool { return serial >= 13 && (serial % 2 != 0) }
is_blob_serial :: proc(serial: u64) -> bool { return serial >= 12 && (serial % 2 == 0) }
