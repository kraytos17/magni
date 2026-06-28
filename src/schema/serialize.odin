package schema

import "core:encoding/endian"
import "core:strings"
import "src:cell"
import "src:types"

COL_BLOB_MARKER :: 0xFE
COL_BLOB_VERSION :: 1

serialize_columns_to_blob :: proc(
	columns: []types.Column,
	allocator := context.allocator,
) -> []u8 {
	// Estimate size: header (2) + count varint + per-column data
	size := 2
	size += cell.varint_size(u64(len(columns)))
	for col in columns {
		size += cell.varint_size(u64(len(col.name))) + len(col.name) + 1
		if def, ok := col.default_value.?; ok {
			#partial switch v in def {
			case types.Null:
				size += 1
			case i64:
				size += 1 + 8
			case f64:
				size += 1 + 8
			case string:
				size += 1 + 4 + len(v)
			case []u8:
				size += 1 + 4 + len(v)
			}
		}
		if chk, has_chk := col.check_expr.?; has_chk {
			size += cell.varint_size(u64(len(chk))) + len(chk)
		}
	}

	blob := make([]u8, size, allocator)
	offset := 0
	blob[offset] = COL_BLOB_MARKER; offset += 1
	blob[offset] = COL_BLOB_VERSION; offset += 1
	offset += cell.varint_encode(blob[offset:], u64(len(columns)))

	for col in columns {
		offset += cell.varint_encode(blob[offset:], u64(len(col.name)))
		copy(blob[offset:], col.name); offset += len(col.name)

		packed: u8 = u8(col.type)
		if col.not_null { packed |= 0x08 }
		if col.pk { packed |= 0x10 }
		if _, has := col.check_expr.?; has { packed |= 0x20 }
		if _, has := col.default_value.?; has { packed |= 0x40 }
		blob[offset] = packed; offset += 1

		if def, ok := col.default_value.?; ok {
			serialize_value_to_blob(blob, &offset, def)
		}
		if chk, has := col.check_expr.?; has {
			offset += cell.varint_encode(blob[offset:], u64(len(chk)))
			copy(blob[offset:], chk); offset += len(chk)
		}
	}
	return blob
}

deserialize_columns :: proc(blob: []u8, allocator := context.allocator) -> []types.Column {
	if len(blob) < 2 || blob[0] != COL_BLOB_MARKER { return nil }
	if blob[1] != COL_BLOB_VERSION { return nil }
	offset := 2
	count, _, cnt_ok := cell.varint_decode(blob, offset)
	if !cnt_ok || count == 0 { return nil }

	offset += cell.varint_size(count)
	cols := make([dynamic]types.Column, 0, int(count), allocator)
	for _ in 0 ..< count {
		name_len, _, name_ok := cell.varint_decode(blob, offset)
		if !name_ok || name_len == 0 { return nil }
		offset += cell.varint_size(name_len)
		if offset + int(name_len) + 1 > len(blob) { return nil }

		name_str := string(blob[offset:offset + int(name_len)])
		offset += int(name_len)
		packed := blob[offset]
		offset += 1
		col := types.Column {
			name     = strings.clone(name_str, allocator),
			type     = types.Column_Type(packed & 0x07),
			not_null = (packed & 0x08) != 0,
			pk       = (packed & 0x10) != 0,
		}

		if (packed & 0x40) != 0 {
			def_val, def_ok := deserialize_value_from_blob(blob, &offset, allocator)
			if !def_ok { return nil }
			col.default_value = def_val
		}
		if (packed & 0x20) != 0 {
			chk_len, _, chk_ok := cell.varint_decode(blob, offset)
			if !chk_ok || chk_len == 0 { return nil }

			offset += cell.varint_size(chk_len)
			if offset + int(chk_len) > len(blob) { return nil }
			col.check_expr = strings.clone(string(blob[offset:offset + int(chk_len)]), allocator)
			offset += int(chk_len)
		}
		append(&cols, col)
	}
	return cols[:]
}

// Write a Value in a simple binary format:
//	[type_byte(1)] + [payload]
//	type_byte: 0=null, 1=i64(8LE), 2=f64(8BE), 3=string(4LE+data), 4=blob(4LE+data)
serialize_value_to_blob :: proc(dest: []u8, offset: ^int, val: types.Value) {
	v := val
	#partial switch vv in v {
	case types.Null:
		dest[offset^] = 0
		offset^ += 1
	case i64:
		dest[offset^] = 1
		offset^ += 1
		endian.put_u64(dest[offset^:], .Little, u64(vv))
		offset^ += 8
	case f64:
		dest[offset^] = 2
		offset^ += 1
		endian.put_f64(dest[offset^:], .Big, vv)
		offset^ += 8
	case string:
		dest[offset^] = 3
		offset^ += 1
		endian.put_u32(dest[offset^:], .Little, u32(len(vv)))
		offset^ += 4
		copy(dest[offset^:], vv)
		offset^ += len(vv)
	case []u8:
		dest[offset^] = 4
		offset^ += 1
		endian.put_u32(dest[offset^:], .Little, u32(len(vv)))
		offset^ += 4
		copy(dest[offset^:], vv)
		offset^ += len(vv)
	}
}

deserialize_value_from_blob :: proc(
	src: []u8,
	offset: ^int,
	allocator := context.allocator,
) -> (
	types.Value,
	bool,
) {
	if offset^ >= len(src) { return {}, false }

	type_byte := src[offset^]
	offset^ += 1
	switch type_byte {
	case 0:
		return types.value_null(), true
	case 1:
		if offset^ + 8 > len(src) { return {}, false }

		val, _ := endian.get_u64(src[offset^:], .Little)
		offset^ += 8
		return types.value_int(i64(val)), true
	case 2:
		if offset^ + 8 > len(src) { return {}, false }
		val, _ := endian.get_f64(src[offset^:], .Big)
		offset^ += 8
		return types.value_real(val), true
	case 3:
		if offset^ + 4 > len(src) { return {}, false }
		len_val, _ := endian.get_u32(src[offset^:], .Little)
		offset^ += 4
		if offset^ + int(len_val) > len(src) { return {}, false }

		str_val := string(src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value_text(strings.clone(str_val, allocator)), true
	case 4:
		if offset^ + 4 > len(src) { return {}, false }
		len_val, _ := endian.get_u32(src[offset^:], .Little)
		offset^ += 4
		if offset^ + int(len_val) > len(src) { return {}, false }
		blob := make([]u8, int(len_val), allocator)

		copy(blob, src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value_blob(blob), true
	}
	return {}, false
}
