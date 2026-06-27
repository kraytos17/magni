package schema

import "core:encoding/endian"
import "core:strings"
import "src:types"

// Format: [Count(4b)] -> [NameLen(4b) + NameBytes + Type(1b) + Flags(1b) + DefaultMarker(1b) + DefaultValue... + CheckLen(4b) + CheckBytes?]
serialize_columns_to_blob :: proc(
	columns: []types.Column,
	allocator := context.allocator,
) -> []u8 {
	size := 4
	for col in columns { size += 4 + len(col.name) + 1 + 1 + 1 }
	for col in columns {
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
		if chk, has_chk := col.check_expr.?; has_chk { size += 4 + len(chk) }
	}

	blob := make([]u8, size, allocator)
	offset := 0
	endian.put_u32(blob[offset:], .Little, u32(len(columns))); offset += 4
	for col in columns {
		endian.put_u32(blob[offset:], .Little, u32(len(col.name))); offset += 4
		copy(blob[offset:], col.name); offset += len(col.name)
		blob[offset] = u8(col.type); offset += 1
		flags: u8
		if col.not_null do flags |= 1
		if col.pk do flags |= 2
		if _, has_chk := col.check_expr.?; has_chk do flags |= 4

		blob[offset] = flags; offset += 1
		if def, ok := col.default_value.?; ok {
			blob[offset] = 1; offset += 1
			serialize_value_to_blob(blob, &offset, def)
		} else { blob[offset] = 0; offset += 1 }
		if chk, has_chk := col.check_expr.?; has_chk {
			endian.put_u32(blob[offset:], .Little, u32(len(chk))); offset += 4
			copy(blob[offset:], chk); offset += len(chk)
		}
	}
	return blob
}

deserialize_columns :: proc(blob: []u8, allocator := context.allocator) -> []types.Column {
	if len(blob) < 4 { return nil }
	offset := 0
	count, ok := endian.get_u32(blob[offset:], .Little)
	if !ok { return nil }

	offset += 4
	cols := make([dynamic]types.Column, 0, count, allocator)
	for _ in 0 ..< count {
		name_len, ok_len := endian.get_u32(blob[offset:], .Little)
		if !ok_len { return nil }

		offset += 4
		if offset + int(name_len) + 3 > len(blob) { return nil }

		name_str := string(blob[offset:offset + int(name_len)]); offset += int(name_len)
		type_byte := blob[offset]; offset += 1
		flags_byte := blob[offset]; offset += 1
		default_marker := blob[offset]; offset += 1
		col := types.Column {
			name     = strings.clone(name_str, allocator),
			type     = types.Column_Type(type_byte),
			not_null = (flags_byte & 1) != 0,
			pk       = (flags_byte & 2) != 0,
		}

		if default_marker == 1 {
			def_val, def_ok := deserialize_value_from_blob(blob, &offset, allocator)
			if !def_ok { return nil }
			col.default_value = def_val
		}
		if (flags_byte & 4) != 0 {
			if offset + 4 > len(blob) { return nil }
			chk_len := (^u32le)(raw_data(blob[offset:]))^
			offset += 4
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
