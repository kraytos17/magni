// Package cell — row (cell) serialization for the on-disk page format.
//
// A cell is one row: [rowid][serial-type header][payload]. Two codecs live
// here: row-major cells (row_codec.odin) and secondary-text index keys
// (text_codec.odin). All decode paths fail closed on corrupt input.
package cell

import "core:fmt"
import "core:mem"
import "src:types"
import "src:util/varint"

// Cell is one row materialized in memory. values are owned when owns_data
// (cloned by create/deserialize); otherwise they borrow page memory and
// stay valid only while the source page is pinned.
Cell :: struct {
	rowid    : types.Row_ID,
	values   : []types.Value,
	owns_data: bool,
}

// Config re-exports the shared storage configuration used by all codecs.
Config :: types.Storage_Config

// create returns a Cell with every string/blob value cloned into allocator.
// On failure, partially cloned values are freed before returning.
create :: proc(
	rowid: types.Row_ID,
	values: []types.Value,
	allocator := context.allocator,
) -> (
	Cell,
	mem.Allocator_Error,
) {
	values_copy := make([]types.Value, len(values), allocator)
	if values_copy == nil && len(values) > 0 {
		return {}, .Out_Of_Memory
	}
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

// destroy frees the cell's values. allocator MUST match the allocator used
// when the cell was created: a mismatch corrupts string/blob payloads.
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

// get_rowid reads the rowid from a serialized cell at offset, skipping the
// leading payload-size varint. ok=false on truncated input.
@(require_results)
get_rowid :: proc(src: []u8, offset := 0) -> (types.Row_ID, bool) {
	if offset >= len(src) {
		return 0, false
	}

	pos := offset
	_, n, ok := varint.decode(src, pos)
	if !ok {
		return 0, false
	}

	pos += n
	rowid, _, ok2 := varint.decode(src, pos)
	if !ok2 {
		return 0, false
	}
	return types.Row_ID(rowid), true
}

// get_size returns the serialized cell's total byte length (size varint +
// payload). ok=false on truncated input.
@(require_results)
get_size :: proc(src: []u8, offset := 0) -> (int, bool) {
	if offset >= len(src) {
		return 0, false
	}

	payload_size, n, ok := varint.decode(src, offset)
	if !ok {
		return 0, false
	}
	return n + int(payload_size), true
}

// debug_print writes a human-readable cell dump to stdout.
debug_print :: proc(c: Cell) {
	fmt.printf("Cell(rowid=%d, owned=%t, values=[", c.rowid, c.owns_data)
	for val, i in c.values {
		if i > 0 {
			fmt.print(", ")
		}
		fmt.print(types.value_to_string(val))
	}
	fmt.println("])")
}

// validate checks value count against the schema and per-column rules:
// NOT NULL rejects nulls; INTEGER requires i64; REAL accepts i64/f64;
// TEXT and BLOB accept string or []u8 (the two text forms interchange).
validate :: proc(values: []types.Value, columns: []types.Column) -> bool {
	if len(values) != len(columns) {
		return false
	}
	for val, i in values {
		col := columns[i]
		if col.not_null && types.is_null(val) {
			return false
		}
		if types.is_null(val) {
			continue
		}

		switch col.type {
		case .INTEGER:
			if _, ok := val.(i64); !ok {
				return false
			}
		case .REAL:
			_, is_real := val.(f64)
			_, is_int := val.(i64)
			if !is_real && !is_int {
				return false
			}
		case .TEXT:
			_, is_text := val.(string)
			_, is_blob := val.([]u8)
			if !is_text && !is_blob {
				return false
			}
		case .BLOB:
			_, is_blob := val.([]u8)
			_, is_text := val.(string)
			if !is_blob && !is_text {
				return false
			}
		}
	}
	return true
}
