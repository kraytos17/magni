package cell

import "core:fmt"
import "core:mem"
import "core:strings"
import "src:types"
import "src:utils"

// Cell represents a serialized row (Record).
//
// MEMORY MODEL:
// 1. `values` slice: Always allocated (owned by Cell).
// 2. `string/[]u8` content: Owned if `owns_data` is true. Unowned (pointers to Page) if false.
Cell :: struct {
	rowid:     types.Row_ID,
	values:    []types.Value,
	allocator: mem.Allocator,
	owns_data: bool,
}

Config :: struct {
	allocator: mem.Allocator,
	zero_copy: bool,
}

create :: proc(
	rowid: types.Row_ID,
	values: []types.Value,
	allocator := context.allocator,
) -> (
	Cell,
	mem.Allocator_Error,
) {
	values_copy := make([]types.Value, len(values), allocator)
	if values_copy == nil { return {}, .Out_Of_Memory }

	for val, i in values {
		#partial switch v in val {
		case string:
			str_copy, err := strings.clone(v, allocator)
			if err != nil {
				delete(values_copy, allocator)
				return {}, err
			}
			values_copy[i] = types.value_text(str_copy)
		case []u8:
			blob_copy, err := make([]u8, len(v), allocator)
			if err != nil {
				delete(values_copy, allocator)
				return {}, err
			}
			copy(blob_copy, v)
			values_copy[i] = types.value_blob(blob_copy)
		case:
			values_copy[i] = val
		}
	}
	return Cell{rowid = rowid, values = values_copy, allocator = allocator, owns_data = true}, nil
}

clone :: proc(c: Cell, allocator := context.allocator) -> (Cell, mem.Allocator_Error) {
	return create(c.rowid, c.values, allocator)
}

destroy :: proc(c: ^Cell) {
	if c.values == nil {
		return
	}
	if c.owns_data {
		for val in c.values {
			#partial switch v in val {
			case string:
				delete(v, c.allocator)
			case []u8:
				delete(v, c.allocator)
			}
		}
	}
	delete(c.values, c.allocator)
	c.values = nil
}

calculate_size :: proc(rowid: types.Row_ID, values: []types.Value) -> int {
	payload_size := 0
	serial_types_size := 0
	for val in values {
		serial := utils.serial_type_for_value(val)
		serial_types_size += utils.varint_size(serial)
		content_size, _ := types.serial_type_content_size(serial)
		payload_size += content_size
	}

	header_bytes :=
		utils.varint_size(u64(rowid)) + utils.varint_size(u64(serial_types_size)) + serial_types_size

	total_payload := header_bytes + payload_size
	return utils.varint_size(u64(total_payload)) + total_payload
}

serialize :: proc(
	dest: []u8,
	rowid: types.Row_ID,
	values: []types.Value,
	temp_allocator := context.temp_allocator,
) -> (
	bytes_written: int,
	ok: bool,
) {
	if len(dest) == 0 { return 0, false }

	serial_types := make([dynamic]u64, 0, len(values), temp_allocator)
	defer delete(serial_types)

	payload_size := 0
	serial_types_size := 0
	for val in values {
		serial := utils.serial_type_for_value(val)
		append(&serial_types, serial)

		size, _ := types.serial_type_content_size(serial)
		payload_size += size
		serial_types_size += utils.varint_size(serial)
	}

	header_size_bytes :=
		utils.varint_size(u64(rowid)) + utils.varint_size(u64(serial_types_size)) + serial_types_size

	total_payload := header_size_bytes + payload_size
	total_size_needed := utils.varint_size(u64(total_payload)) + total_payload
	if len(dest) < total_size_needed { return 0, false }

	offset := 0
	offset += utils.varint_encode(dest[offset:], u64(total_payload))
	offset += utils.varint_encode(dest[offset:], u64(rowid))
	offset += utils.varint_encode(dest[offset:], u64(serial_types_size))
	
	for st in serial_types {
		offset += utils.varint_encode(dest[offset:], st)
	}
	for val, i in values {
		serial := serial_types[i]
		switch v in val {
		case types.Null:
		case i64:
			if serial != u64(types.Serial_Type.ZERO) && serial != u64(types.Serial_Type.ONE) {
				size, _ := types.serial_type_content_size(serial)
				utils.write_int_by_size(dest, offset, v, size)
				offset += size
			}
		case f64:
			utils.write_f64_be(dest, offset, v)
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
	_, n, ok_payload := utils.varint_decode(src, pos)
	if !ok_payload { return {}, 0, false }
	pos += n

	rowid_val, n2, ok_rowid := utils.varint_decode(src, pos)
	if !ok_rowid { return {}, 0, false }
	pos += n2

	header_size, n3, ok_header := utils.varint_decode(src, pos)
	if !ok_header { return {}, 0, false }
	pos += n3

	header_start := pos
	serial_types := make([dynamic]u64, context.temp_allocator)
	defer delete(serial_types)

	for pos < header_start + int(header_size) {
		st, n4, ok_st := utils.varint_decode(src, pos)
		if !ok_st { return {}, 0, false }
		append(&serial_types, st)
		pos += n4
	}

	values := make([dynamic]types.Value, 0, len(serial_types), alloc)
	defer if !ok { delete(values) }

	for st in serial_types {
		content_size, _ := types.serial_type_content_size(st)
		type_code := types.Serial_Type(st)
		if pos + content_size > len(src) {
			return {}, 0, false
		}
		if type_code == .ZERO {
			append(&values, types.value_int(0))
		} else if type_code == .ONE {
			append(&values, types.value_int(1))
		} else if st == u64(types.Serial_Type.NULL) {
			append(&values, types.value_null())
		} else if st >= u64(types.Serial_Type.INT8) && st <= u64(types.Serial_Type.INT64) {
			int_val, _ := utils.read_int_by_size(src, pos, content_size)
			append(&values, types.value_int(int_val))
			pos += content_size
		} else if type_code == .FLOAT64 {
			float_val, _ := utils.read_f64_be(src, pos)
			append(&values, types.value_real(float_val))
			pos += 8
		} else if utils.is_text_serial(st) {
			text_bytes := src[pos:pos + content_size]
			if config.zero_copy {
				append(&values, types.value_text(string(text_bytes)))
			} else {
				str := strings.clone_from(text_bytes, alloc)
				append(&values, types.value_text(str))
			}
			pos += content_size
		} else if utils.is_blob_serial(st) {
			blob_bytes := src[pos:pos + content_size]
			if config.zero_copy {
				append(&values, types.value_blob(blob_bytes))
			} else {
				blob_copy := make([]u8, content_size, alloc)
				copy(blob_copy, blob_bytes)
				append(&values, types.value_blob(blob_copy))
			}
			pos += content_size
		} else {
			return {}, 0, false
		}
	}

	cell = Cell {
		rowid     = types.Row_ID(rowid_val),
		values    = values[:],
		allocator = alloc,
		owns_data = !config.zero_copy,
	}
	return cell, pos - offset, true
}

get_rowid :: proc(src: []u8, offset := 0) -> (types.Row_ID, bool) {
	if offset >= len(src) { return 0, false }
	pos := offset
	_, n, ok := utils.varint_decode(src, pos)
	if !ok { return 0, false }
	pos += n

	rowid, _, ok2 := utils.varint_decode(src, pos)
	if !ok2 { return 0, false }
	return types.Row_ID(rowid), true
}

get_size :: proc(src: []u8, offset := 0) -> (int, bool) {
	if offset >= len(src) { return 0, false }
	payload_size, n, ok := utils.varint_decode(src, offset)
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
