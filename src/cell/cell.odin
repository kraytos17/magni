package cell

import "core:fmt"
import "core:mem"
import "core:strings"
import "src:types"
import "src:utils"

// Cell represents a serialized row (Record) in a B-tree leaf node.
//
// MEMORY OWNERSHIP:
// The Cell struct OWNS the memory for the `values` slice and the data within it
// (unless zero_copy was used during deserialization).
//
// IMPORTANT: You MUST call `cell_destroy()` to free this memory when the Cell
// is no longer needed, EXCEPT when the Cell was created with zero_copy=true
Cell :: struct {
	rowid:     types.Row_ID, // The unique 64-bit integer key for this row
	values:    []types.Value, // Array of column values (dynamically allocated)
	allocator: mem.Allocator, // The allocator used to create `values`
}

// Configuration options for serialization.
Serialize_Options :: struct {
	// Temporary allocator used for intermediate arrays (e.g., serial types).
	// Defaults to context.temp_allocator if nil.
	temp_allocator: mem.Allocator,
}

// Configuration options for deserialization.
Deserialize_Options :: struct {
	// The allocator used for the `values` array and any deep-copied strings/blobs.
	// Defaults to context.allocator if nil.
	allocator: mem.Allocator,

	// If true, Text and Blob values will point directly into the source buffer.
	//
	// Note for zero_copy=true:
	// 1. The Cell CANNOT outlive the source buffer - accessing the Cell after
	//    the source buffer is freed/modified results in undefined behavior.
	// 2. You MUST NOT call cell_destroy() on zero-copy Cells, as the strings
	//    and blobs were never allocated and cannot be freed. Calling cell_destroy()
	//    will cause a "bad free" instance.
	// 3. Only the `values` slice itself should be freed (via `delete(cell.values)`).
	//
	// If false (default), Text and Blob values are deeply copied/cloned (safer).
	// This is the recommended default for most use cases.
	zero_copy: bool,
}

// Creates a new Cell from raw values, performing a DEEP COPY of all data.
//
// Arguments:
// - rowid: The unique key.
// - values: The values to store.
// - allocator: Allocator for the new Cell's internal storage.
//
// Returns:
// - Cell: The new cell (Must be freed via cell_destroy).
// - Allocator_Error: If memory allocation fails.
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

// Frees all heap memory associated with a Cell.
//
// Behavior:
// 1. Iterates through `cell.values` and frees every string/blob allocated.
// 2. Frees the `cell.values` slice itself.
// 3. Resets `cell.values` to nil to prevent double-free bugs.
//
// DO NOT call this on Cells deserialized with zero_copy=true!
// Zero-copy Cells do not own their string/blob data, so freeing them causes
// a "bad free" error. For zero-copy Cells, only free the values array:
//     delete(cell.values)
// `Note`: Safe to call on a zero-initialized Cell.
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

// Calculates exactly how many bytes are needed to store this Cell on disk.
// Includes overhead for Varints, Headers, and Serial Types.
//
// Optimization: Uses context.temp_allocator for intermediate calculations.
cell_calculate_size :: proc(rowid: types.Row_ID, values: []types.Value) -> int {
	context.allocator = context.temp_allocator
	serial_types := make([dynamic]u64, 0, len(values))
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

// Writes the Cell data into a byte buffer.
//
// Inputs:
// - dest: The target buffer (Must be large enough, see cell_calculate_size).
// - rowid: Key.
// - values: Data.
//
// Returns:
// - bytes_written: Actual number of bytes written.
// - ok: False if dest is too small.
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

// Reads a Cell from a byte buffer.
//
// Inputs:
// - src: Buffer containing the serialized cell.
// - offset: Where to start reading in the buffer.
// - options: Allocator choice and Zero-Copy preference.
//
// MEMORY BEHAVIOR:
// 1. The `cell.values` array is ALWAYS allocated using `options.allocator`.
//    You MUST free it later (usually via cell_destroy).
// 2. If `options.zero_copy` is TRUE:
//    - Strings and Blobs in the cell point directly to memory in `src`.
//    - The Cell becomes invalid if `src` is freed or modified.
// 3. If `options.zero_copy` is FALSE (default):
//    - Strings and Blobs are allocated and copied into `options.allocator`.
//    - The Cell is independent of `src`.
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
				text_str := strings.clone_from(text_bytes, result_allocator)
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

// Reads only the RowID from a serialized cell.
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

// Returns the total size in bytes of the cell at the given offset.
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

// Prints the cell content to stdout for debugging purposes.
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

// Validates that the cell's values match the expected table schema (Columns).
// Checks for type mismatches and Not-Null constraints.
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

// Creates a deep copy of an existing Cell.
cell_clone :: proc(cell: Cell, allocator := context.allocator) -> (Cell, mem.Allocator_Error) {
	return cell_create(cell.rowid, cell.values, allocator)
}
