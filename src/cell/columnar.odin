package cell

import "core:encoding/endian"
import "src:types"

ENCODING_RAW :: 0
ENCODING_DELTA :: 1

Col_Header :: struct #packed {
	col_index:   u8,
	encoding:    u8,
	row_count:   u16,
	byte_offset: u32,
	byte_size:   u32,
}

COLUMNAR_DIR_OFFSET :: 8

serialize_columnar :: proc(
	dest: []u8,
	rowids: []types.Row_ID,
	rows: [][]types.Value,
	columns: []types.Column,
) -> bool {
	if len(rows) == 0 || len(columns) == 0 { return false }
	if len(rowids) != len(rows) { return false }

	dir_start := COLUMNAR_DIR_OFFSET
	data_pos := dir_start + len(columns) * size_of(Col_Header)
	for col_i := 0; col_i < len(columns); col_i += 1 {
		off := dir_start + col_i * size_of(Col_Header)
		h := (^Col_Header)(raw_data(dest[off:]))
		h.col_index = u8(col_i)
		h.encoding = ENCODING_DELTA if columns[col_i].type == .INTEGER else ENCODING_RAW
		h.row_count = u16(len(rows))
		h.byte_offset = 0
		h.byte_size = 0
	}
	for i in 0 ..< len(rowids) {
		id := u64(rowids[i])
		if i > 0 {
			prev := u64(rowids[i - 1])
			data_pos += varint_encode(dest[data_pos:], id - prev)
		} else {
			data_pos += varint_encode(dest[data_pos:], id)
		}
	}
	for col_i := 0; col_i < len(columns); col_i += 1 {
		off := dir_start + col_i * size_of(Col_Header)
		h := (^Col_Header)(raw_data(dest[off:]))
		h.byte_offset = u32(data_pos)
		col_start := data_pos

		if h.encoding == ENCODING_DELTA {
			min := i64(max(i64))
			for ri := 0; ri < len(rows); ri += 1 {
				if v, ok := rows[ri][col_i].(i64); ok && v < min { min = v }
			}

			data_pos += varint_encode(dest[data_pos:], u64(min))
			for ri := 0; ri < len(rows); ri += 1 {
				if v, ok := rows[ri][col_i].(i64); ok {
					data_pos += varint_encode(dest[data_pos:], u64(v - min))
				}
			}
		} else {
			for ri := 0; ri < len(rows); ri += 1 {
				val := rows[ri][col_i]
				switch v in val {
				case types.Null:
				case i64:
					data_pos += varint_encode(dest[data_pos:], u64(v))
				case f64:
					endian.put_f64(dest[data_pos:], .Big, v)
					data_pos += 8
				case string:
					copy(dest[data_pos:], transmute([]u8)v)
					data_pos += len(v)
				case []u8:
					copy(dest[data_pos:], v)
					data_pos += len(v)
				}
			}
		}
		h.byte_size = u32(data_pos - col_start)
	}
	return true
}

read_col_header :: proc(data: []u8, col_index: int, base_offset: int = 0) -> (Col_Header, bool) {
	off := base_offset + COLUMNAR_DIR_OFFSET + col_index * size_of(Col_Header)
	if off + size_of(Col_Header) > len(data) { return {}, false }
	return (^Col_Header)(raw_data(data[off:]))^, true
}

decode_column :: proc(
	data: []u8,
	num_cols: int,
	col_index: int,
	base_offset: int = 0,
	allocator := context.temp_allocator,
) -> []types.Value {
	h, ok := read_col_header(data, col_index, base_offset)
	if !ok { return nil }

	pos := base_offset + int(h.byte_offset)
	count := int(h.row_count)
	result := make([]types.Value, count, allocator)
	if h.encoding == ENCODING_RAW {
		for i in 0 ..< count {
			val, _ := endian.get_f64(data[pos + i * 8:], .Big)
			result[i] = types.value_real(val)
		}
	} else {
		min, n1, ok1 := varint_decode(data, pos)
		if !ok1 { return nil }

		pos += n1
		for i in 0 ..< count {
			delta, n2, ok2 := varint_decode(data, pos)
			if !ok2 { return nil }
			pos += n2
			result[i] = types.value_int(i64(i64(min) + i64(delta)))
		}
	}
	return result
}

read_columnar_rowid :: proc(
	data: []u8,
	num_cols: int,
	row_index: int,
	base_offset: int = 0,
) -> (
	types.Row_ID,
	bool,
) {
	pos := base_offset + COLUMNAR_DIR_OFFSET + num_cols * size_of(Col_Header)
	total: u64 = 0
	for i := 0; i <= row_index; i += 1 {
		delta, n, ok := varint_decode(data, pos)
		if !ok { return 0, false }
		total += delta
		if i == row_index { return types.Row_ID(total), true }
		pos += n
	}
	return 0, false
}

read_columnar_cell :: proc(
	data: []u8,
	num_cols: int,
	row_index: int,
	config: Config,
	base_offset: int = 0,
) -> (
	Cell,
	bool,
) {
	rowid, ok := read_columnar_rowid(data, num_cols, row_index, base_offset)
	if !ok { return {}, false }

	alloc := config.allocator
	if alloc.procedure == nil { alloc = context.allocator }

	scratch: [dynamic; types.MAX_COLS]types.Value
	for col_i in 0 ..< num_cols {
		h, h_ok := read_col_header(data, col_i, base_offset)
		if !h_ok { return {}, false }
		if row_index >= int(h.row_count) { return {}, false }

		val: types.Value
		if h.encoding == ENCODING_DELTA {
			pos := base_offset + int(h.byte_offset)
			min, n1, ok1 := varint_decode(data, pos)
			if !ok1 { return {}, false }
			pos += n1
			for i := 0; i <= row_index; i += 1 {
				delta, n2, ok2 := varint_decode(data, pos)
				if !ok2 { return {}, false }
				pos += n2
				if i == row_index {
					val = types.value_int(i64(i64(min) + i64(delta)))
				}
			}
		} else {
			pos := base_offset + int(h.byte_offset)
			for i := 0; i <= row_index; i += 1 {
				if i == row_index {
					if pos + 8 <= base_offset + int(h.byte_offset) + int(h.byte_size) {
						fv, fv_ok := endian.get_f64(data[pos:], .Big)
						if fv_ok {
							val = types.value_real(fv)
						}
					}
					break
				}
				_, n, _ := varint_decode(data, pos)
				if n > 0 {
					pos += n
				} else {
					pos += 8
				}
			}
		}
		append(&scratch, val)
	}

	if len(scratch) != num_cols { return {}, false }
	result_values := make([]types.Value, len(scratch), alloc)
	copy(result_values, scratch[:])
	return Cell{rowid = rowid, values = result_values, owns_data = !config.zero_copy}, true
}
