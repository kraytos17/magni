// Column-list persistence: encodes []types.Column into the versioned blob
// stored in a schema row (columns_blob) and decodes it back.
//
// Layout: [0xFE marker][version] then a varint column count, then per
// column: varint name len, name bytes, one packed flags byte (type in the
// low 3 bits, not-null/pk/check-present/default-present/unique as bits),
// then the optional default value followed by the optional check expression
// when their bits are set. Decoding validates marker, version, and every
// length against the blob, returning nil/false on any mismatch rather than
// a partial list. Unknown flag bits are ignored, so a bit addition like
// unique stays wire-compatible both ways without a version bump.
package schema

import "core:encoding/endian"
import "core:strings"
import "src:types"
import "src:util/varint"

COL_BLOB_MARKER  :: 0xFE
COL_BLOB_VERSION :: 1
// Groups section version: blobs carrying composite unique groups. Version-1
// output is byte-identical to before (no groups ever encoded), so only
// grouped tables use this version.
COL_BLOB_GROUPS_VERSION :: 2

// Packed column-flag bits in the per-column header byte (low 3 bits are
// the Column_Type tag).
COL_TYPE_MASK     :: 0x07
COL_FLAG_NOT_NULL :: 0x08
COL_FLAG_PK       :: 0x10
COL_FLAG_CHECK    :: 0x20
COL_FLAG_DEFAULT  :: 0x40
COL_FLAG_UNIQUE   :: 0x80

// serialize_columns_to_blob encodes a column list per the layout above.
// The size estimate and the writes must agree (no length prefix for the
// blob itself) — caller stores the returned slice into a schema row.
// unique_groups appends a version-2 section ([varint group count], then per
// group [varint member count, varint member indices]); empty groups keep
// byte-identical version-1 output, so only grouped tables change versions.
serialize_columns_to_blob :: proc(
	columns: []types.Column,
	allocator := context.allocator,
	unique_groups: [][]int = nil,
) -> []u8 {
	// Estimate size: header (2) + count varint + per-column data
	size := 2
	size += varint.size(u64(len(columns)))
	for col in columns {
		size += varint.size(u64(len(col.name))) + len(col.name) + 1
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
			size += varint.size(u64(len(chk))) + len(chk)
		}
	}
	if len(unique_groups) > 0 {
		size += varint.size(u64(len(unique_groups)))
		for g in unique_groups {
			size += varint.size(u64(len(g)))
			for idx in g {
				size += varint.size(u64(idx))
			}
		}
	}

	blob := make([]u8, size, allocator)
	offset := 0
	blob[offset] = COL_BLOB_MARKER; offset += 1
	version := u8(COL_BLOB_GROUPS_VERSION) if len(unique_groups) > 0 else u8(COL_BLOB_VERSION)
	blob[offset] = version; offset += 1
	offset += varint.encode(blob[offset:], u64(len(columns)))
	for col in columns {
		offset += varint.encode(blob[offset:], u64(len(col.name)))
		copy(blob[offset:], col.name); offset += len(col.name)
		packed := u8(col.type)
		if col.not_null {
			packed |= COL_FLAG_NOT_NULL
		}
		if col.pk {
			packed |= COL_FLAG_PK
		}
		if _, has := col.check_expr.?; has {
			packed |= COL_FLAG_CHECK
		}
		if _, has := col.default_value.?; has {
			packed |= COL_FLAG_DEFAULT
		}
		if col.unique {
			packed |= COL_FLAG_UNIQUE
		}

		blob[offset] = packed; offset += 1
		if def, ok := col.default_value.?; ok {
			serialize_value_to_blob(blob, &offset, def)
		}
		if chk, has := col.check_expr.?; has {
			offset += varint.encode(blob[offset:], u64(len(chk)))
			copy(blob[offset:], chk); offset += len(chk)
		}
	}
	if len(unique_groups) > 0 {
		offset += varint.encode(blob[offset:], u64(len(unique_groups)))
		for g in unique_groups {
			offset += varint.encode(blob[offset:], u64(len(g)))
			for idx in g {
				offset += varint.encode(blob[offset:], u64(idx))
			}
		}
	}
	return blob
}

// deserialize_columns decodes a blob produced by serialize_columns_to_blob
// into allocator-owned columns (names/check expressions cloned) plus the
// composite unique groups (nil when the blob carries none). ok=false on any
// malformed input: wrong marker/version, zero or truncated count, bad
// name/check length, an undecodable default, or a malformed groups section
// (stale member index, empty group) — never partially initialized outputs.
// Version-1 blobs decode with nil groups.
deserialize_columns :: proc(
	blob: []u8,
	allocator := context.allocator,
) -> (
	cols: []types.Column,
	groups: [][]int,
	ok: bool,
) {
	if len(blob) < 2 || blob[0] != COL_BLOB_MARKER {
		return nil, nil, false
	}

	version := blob[1]
	if version != COL_BLOB_VERSION && version != COL_BLOB_GROUPS_VERSION {
		return nil, nil, false
	}

	offset := 2
	count, _, cnt_ok := varint.decode(blob, offset)
	if !cnt_ok || count == 0 {
		return nil, nil, false
	}

	offset += varint.size(count)
	col_list := make([dynamic]types.Column, 0, int(count), allocator)
	for _ in 0 ..< count {
		name_len, _, name_ok := varint.decode(blob, offset)
		if !name_ok || name_len == 0 {
			return nil, nil, false
		}

		offset += varint.size(name_len)
		if offset + int(name_len) + 1 > len(blob) {
			return nil, nil, false
		}

		name_str := string(blob[offset:offset + int(name_len)])
		offset += int(name_len)
		packed := blob[offset]
		offset += 1
		col := types.Column {
			name     = strings.clone(name_str, allocator),
			type     = types.Column_Type(packed & COL_TYPE_MASK),
			not_null = (packed & COL_FLAG_NOT_NULL) != 0,
			pk       = (packed & COL_FLAG_PK) != 0,
			unique   = (packed & COL_FLAG_UNIQUE) != 0,
		}
		if (packed & COL_FLAG_DEFAULT) != 0 {
			def_val, def_ok := deserialize_value_from_blob(blob, &offset, allocator)
			if !def_ok {
				return nil, nil, false
			}
			col.default_value = def_val
		}
		if (packed & COL_FLAG_CHECK) != 0 {
			chk_len, _, chk_ok := varint.decode(blob, offset)
			if !chk_ok || chk_len == 0 {
				return nil, nil, false
			}

			offset += varint.size(chk_len)
			if offset + int(chk_len) > len(blob) {
				return nil, nil, false
			}

			col.check_expr = strings.clone(string(blob[offset:offset + int(chk_len)]), allocator)
			offset += int(chk_len)
		}
		append(&col_list, col)
	}
	if version != COL_BLOB_GROUPS_VERSION {
		return col_list[:], nil, true
	}

	gc, _, gc_ok := varint.decode(blob, offset)
	if !gc_ok {
		return nil, nil, false
	}

	offset += varint.size(gc)
	group_list := make([dynamic][]int, 0, int(gc), allocator)
	for _ in 0 ..< gc {
		cc, _, cc_ok := varint.decode(blob, offset)
		if !cc_ok || cc == 0 {
			return nil, nil, false
		}

		offset += varint.size(cc)
		members := make([]int, cc, allocator)
		for j in 0 ..< cc {
			mv, _, mv_ok := varint.decode(blob, offset)
			if !mv_ok || mv >= count {
				return nil, nil, false
			}

			offset += varint.size(mv)
			members[j] = int(mv)
		}
		append(&group_list, members)
	}
	return col_list[:], group_list[:], true
}

// Write a Value in a simple binary format:
//	[type_byte(1)] + [payload]
//	type_byte: 0=null, 1=i64(8LE), 2=f64(8BE), 3=string(4LE+data), 4=blob(4LE+data)
@(private = "file")
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

// deserialize_value_from_blob reads one value written by
// serialize_value_to_blob (same wire format: [type_byte][payload]), checks
// every length against src, advances offset, and clones TEXT/BLOB payloads
// into allocator. ok=false on a truncated buffer or unknown type byte.
@(private = "file")
deserialize_value_from_blob :: proc(
	src: []u8,
	offset: ^int,
	allocator := context.allocator,
) -> (
	types.Value,
	bool,
) {
	if offset^ >= len(src) {
		return {}, false
	}

	type_byte := src[offset^]
	offset^ += 1
	switch type_byte {
	case 0:
		return types.value(), true
	case 1:
		if offset^ + 8 > len(src) {
			return {}, false
		}

		val, _ := endian.get_u64(src[offset^:], .Little)
		offset^ += 8
		return types.value(i64(val)), true
	case 2:
		if offset^ + 8 > len(src) {
			return {}, false
		}

		val, _ := endian.get_f64(src[offset^:], .Big)
		offset^ += 8
		return types.value(val), true
	case 3:
		if offset^ + 4 > len(src) {
			return {}, false
		}

		len_val, _ := endian.get_u32(src[offset^:], .Little)
		offset^ += 4
		if offset^ + int(len_val) > len(src) {
			return {}, false
		}

		str_val := string(src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value(strings.clone(str_val, allocator)), true
	case 4:
		if offset^ + 4 > len(src) {
			return {}, false
		}

		len_val, _ := endian.get_u32(src[offset^:], .Little)
		offset^ += 4
		if offset^ + int(len_val) > len(src) {
			return {}, false
		}

		blob := make([]u8, int(len_val), allocator)
		copy(blob, src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value(blob), true
	}
	return {}, false
}
