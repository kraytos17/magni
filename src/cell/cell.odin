package cell

import "core:fmt"
import "core:mem"
import "core:strings"
import "src:types"
import "src:utils"

// Cell represents a serialized row in a B-tree leaf node
// Cell owns all memory in its values array
Cell :: struct {
	rowid:     types.Row_ID,
	values:    []types.Value,
	allocator: mem.Allocator,
}

// Cell_Builder is used for constructing cells with explicit allocator control
Cell_Builder :: struct {
	rowid:     types.Row_ID,
	values:    [dynamic]types.Value,
	allocator: mem.Allocator,
}

// Serialization options for fine-grained control
Serialize_Options :: struct {
	temp_allocator: mem.Allocator,
}

// Deserialization options
Deserialize_Options :: struct {
	allocator: mem.Allocator,
	zero_copy: bool,
}

cell_builder_init :: proc(allocator := context.allocator) -> Cell_Builder {
	return Cell_Builder{values = make([dynamic]types.Value, allocator), allocator = allocator}
}

cell_builder_set_rowid :: proc(builder: ^Cell_Builder, rowid: types.Row_ID) {
	builder.rowid = rowid
}

cell_builder_add_value :: proc(builder: ^Cell_Builder, value: types.Value) {
	append(&builder.values, value)
}

cell_builder_build :: proc(builder: ^Cell_Builder) -> Cell {
	cell := Cell {
		rowid     = builder.rowid,
		values    = builder.values[:],
		allocator = builder.allocator,
	}
	builder.values = nil
	return cell
}

cell_builder_destroy :: proc(builder: ^Cell_Builder) {
	if builder.values != nil {
		delete(builder.values)
	}
}

cell_create :: proc(
	rowid: types.Row_ID,
	values: []types.Value,
	allocator := context.allocator,
) -> (
	Cell,
	mem.Allocator_Error,
) {
	values_copy := make([]types.Value, len(values), allocator)
	for val, i in values {
		#partial switch v in val {
		case string:
			str_copy, _ := strings.clone(v, allocator)
			values_copy[i] = types.value_text(str_copy)
		case []u8:
			blob_copy, _ := make([]u8, len(v), allocator)
			copy(blob_copy, v)
			values_copy[i] = types.value_blob(blob_copy)
		case:
			values_copy[i] = val
		}
	}
	return Cell{rowid = rowid, values = values_copy, allocator = allocator}, nil
}

cell_destroy :: proc(cell: ^Cell) {
	if cell.values == nil {
		return
	}

	for val in cell.values {
		#partial switch v in val {
		case string:
			delete(v, cell.allocator)
		case []u8:
			delete(v, cell.allocator)
		}
	}
	delete(cell.values, cell.allocator)
	cell.values = nil
}

cell_calculate_size :: proc(rowid: types.Row_ID, values: []types.Value) -> int {
	context.allocator = context.temp_allocator
	serial_types := make([dynamic]u64, 0, len(values))
	// defer delete(serial_types)

	payload_size := 0
	for val in values {
		serial := utils.serial_type_for_value(val)
		append(&serial_types, serial)
		x, _ := types.serial_type_content_size(serial)
		payload_size += x
	}

	serial_types_size := 0
	for st in serial_types {
		serial_types_size += utils.varint_size(st)
	}

	header_len_val := serial_types_size
	header_bytes := 0
	header_bytes += utils.varint_size(u64(rowid))
	header_bytes += utils.varint_size(u64(header_len_val))
	header_bytes += header_len_val
	total_payload := header_bytes + payload_size
	total_size := utils.varint_size(u64(total_payload)) + total_payload

	return total_size
}

cell_serialize :: proc(
	dest: []u8,
	rowid: types.Row_ID,
	values: []types.Value,
	options := Serialize_Options{},
) -> (
	bytes_written: int,
	ok: bool,
) {
	if len(dest) == 0 {
		return 0, false
	}

	temp_alloc :=
		options.temp_allocator if options.temp_allocator.procedure != nil else context.temp_allocator

	context.allocator = temp_alloc
	serial_types := make([dynamic]u64, 0, len(values))
	// defer delete(serial_types)

	payload_size := 0
	for val in values {
		serial := utils.serial_type_for_value(val)
		append(&serial_types, serial)
		x, _ := types.serial_type_content_size(serial)
		payload_size += x
	}

	serial_types_size := 0
	for st in serial_types {
		serial_types_size += utils.varint_size(st)
	}

	stored_header_len := serial_types_size
	header_size_bytes :=
		utils.varint_size(u64(rowid)) + utils.varint_size(u64(stored_header_len)) + stored_header_len

	total_payload := header_size_bytes + payload_size
	total_size_needed := utils.varint_size(u64(total_payload)) + total_payload
	if len(dest) < total_size_needed {
		return 0, false
	}

	offset := 0
	n := utils.varint_encode(dest[offset:], u64(total_payload))
	offset += n

	n = utils.varint_encode(dest[offset:], u64(rowid))
	offset += n

	n = utils.varint_encode(dest[offset:], u64(stored_header_len))
	offset += n

	for st in serial_types {
		n = utils.varint_encode(dest[offset:], st)
		offset += n
	}
	for val, i in values {
		serial := serial_types[i]
		switch v in val {
		case types.Null:
		case i64:
			if serial == u64(types.Serial_Type.ZERO) || serial == u64(types.Serial_Type.ONE) {
			} else {
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

cell_deserialize :: proc(
	src: []u8,
	offset := 0,
	options := Deserialize_Options{},
) -> (
	cell: Cell,
	bytes_consumed: int,
	ok: bool,
) {
	if offset >= len(src) {
		return {}, 0, false
	}

	result_allocator := options.allocator
	if result_allocator.procedure == nil {
		result_allocator = context.allocator
	}

	pos := offset
	_, n, ok_payload := utils.varint_decode(src, pos)
	if !ok_payload {
		return {}, 0, false
	}

	pos += n
	rowid, n2, ok_rowid := utils.varint_decode(src, pos)
	if !ok_rowid {
		return {}, 0, false
	}

	pos += n2
	header_size, n3, ok_header := utils.varint_decode(src, pos)
	if !ok_header {
		return {}, 0, false
	}

	pos += n3
	header_start := pos
	serial_types := make([dynamic]u64, context.temp_allocator)
	for pos < header_start + int(header_size) {
		st, n4, ok_st := utils.varint_decode(src, pos)
		if !ok_st {
			return {}, 0, false
		}
		append(&serial_types, st)
		pos += n4
	}

	values := make([dynamic]types.Value, 0, len(serial_types), result_allocator)
	success := false
	defer if !success {
		delete(values)
	}

	for st in serial_types {
		content_size, _ := types.serial_type_content_size(st)
		if st == u64(types.Serial_Type.NULL) {
			append(&values, types.value_null())
			continue
		}
		if st == u64(types.Serial_Type.ZERO) {
			append(&values, types.value_int(0))
			continue
		}
		if st == u64(types.Serial_Type.ONE) {
			append(&values, types.value_int(1))
			continue
		}
		if st >= u64(types.Serial_Type.INT8) && st <= u64(types.Serial_Type.INT64) {
			int_val, _ := utils.read_int_by_size(src, pos, content_size)
			append(&values, types.value_int(int_val))
			pos += content_size
			continue
		}
		if st == u64(types.Serial_Type.FLOAT64) {
			float_val, _ := utils.read_f64_be(src, pos)
			append(&values, types.value_real(float_val))
			pos += 8
			continue
		}
		if utils.is_text_serial(st) {
			text_len := utils.content_length_from_serial(st)
			if pos + text_len > len(src) {
				fmt.eprintfln(
					"Error: TEXT data extends beyond buffer (pos=%d, len=%d, buffer=%d)",
					pos,
					text_len,
					len(src),
				)
				return {}, 0, false
			}

			text_bytes := src[pos:pos + text_len]
			if options.zero_copy {
				text_str := string(text_bytes)
				append(&values, types.value_text(text_str))
			} else {
				text_str := strings.clone_from_bytes(text_bytes, result_allocator)
				append(&values, types.value_text(text_str))
			}
			pos += text_len
			continue
		}
		if utils.is_blob_serial(st) {
			blob_len := utils.content_length_from_serial(st)
			if options.zero_copy {
				blob_bytes := src[pos:pos + blob_len]
				append(&values, types.value_blob(blob_bytes))
			} else {
				blob_bytes := make([]u8, blob_len, result_allocator)
				copy(blob_bytes, src[pos:pos + blob_len])
				append(&values, types.value_blob(blob_bytes))
			}
			pos += blob_len
			continue
		}
		return {}, 0, false
	}

	success = true
	cell = Cell {
		rowid     = types.Row_ID(rowid),
		values    = values[:],
		allocator = result_allocator,
	}
	bytes_consumed = pos - offset
	return cell, bytes_consumed, true
}

cell_get_rowid :: proc(src: []u8, offset := 0) -> (types.Row_ID, bool) {
	if offset >= len(src) {
		return 0, false
	}

	pos := offset
	_, n, ok := utils.varint_decode(src, pos)
	if !ok {
		return 0, false
	}

	pos += n
	rowid, _, ok2 := utils.varint_decode(src, pos)
	if !ok2 {
		return 0, false
	}
	return types.Row_ID(rowid), true
}

cell_get_size :: proc(src: []u8, offset := 0) -> (int, bool) {
	if offset >= len(src) {
		return 0, false
	}

	payload_size, n, ok := utils.varint_decode(src, offset)
	if !ok {
		return 0, false
	}
	total := n + int(payload_size)
	return total, true
}

cell_debug_print :: proc(cell: Cell) {
	fmt.printf("Cell(rowid=%d, values=[", cell.rowid)
	for val, i in cell.values {
		if i > 0 {
			fmt.print(", ")
		}
		fmt.print(types.value_to_string(val))
	}
	fmt.println("])")
}

cell_validate_types :: proc(values: []types.Value, columns: []types.Column) -> bool {
	if len(values) != len(columns) {
		return false
	}

	for val, i in values {
		col := columns[i]
		if col.not_null && types.is_null(val) {
			return false
		}

		switch col.type {
		case .INTEGER:
			if _, ok := val.(i64); !ok && !types.is_null(val) {
				return false
			}
		case .REAL:
			if _, ok := val.(f64); !ok && !types.is_null(val) {
				return false
			}
		case .TEXT:
			if _, ok := val.(string); !ok && !types.is_null(val) {
				return false
			}
		case .BLOB:
			if _, ok := val.([]u8); !ok && !types.is_null(val) {
				return false
			}
		}
	}
	return true
}

cell_clone :: proc(cell: Cell, allocator := context.allocator) -> (Cell, mem.Allocator_Error) {
	return cell_create(cell.rowid, cell.values, allocator)
}
