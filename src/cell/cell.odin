// Package cell serializes/deserializes rows (cells) to/from the on-disk format.
package cell

import "core:fmt"
import "core:mem"
import "src:util/varint"
import "src:types"

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

// destroy frees the cell's values.
// allocator MUST match the allocator used when the cell was created.
// A mismatch causes memory corruption (bad free on string/blob values).
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

get_rowid :: proc(src: []u8, offset := 0) -> (types.Row_ID, bool) {
	if offset >= len(src) { return 0, false }
	pos := offset
	_, n, ok := varint.decode(src, pos)
	if !ok { return 0, false }

	pos += n
	rowid, _, ok2 := varint.decode(src, pos)
	if !ok2 { return 0, false }
	return types.Row_ID(rowid), true
}

get_size :: proc(src: []u8, offset := 0) -> (int, bool) {
	if offset >= len(src) { return 0, false }
	payload_size, n, ok := varint.decode(src, offset)
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
