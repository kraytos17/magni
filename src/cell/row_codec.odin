package cell

import "core:encoding/endian"
import "core:mem"
import "core:strings"
import "src:types"
import "src:util/varint"

Serialization_Info :: struct {
	serial_types_size: int,
	payload_size     : int,
	total_size       : int,
}

compute_info :: proc(rowid: types.Row_ID, values: []types.Value) -> Serialization_Info {
	info: Serialization_Info
	for val in values {
		serial := serial_type_for_value(val)
		info.serial_types_size += varint.size(serial)
		content_size, _ := types.serial_type_content_size(serial)
		info.payload_size += content_size
	}

	header_bytes :=
		varint.size(u64(rowid)) + varint.size(u64(info.serial_types_size)) + info.serial_types_size

	total_payload := header_bytes + info.payload_size
	info.total_size = varint.size(u64(total_payload)) + total_payload
	return info
}

// Serialize a row into the binary cell format at dest. Info must come from compute_info.
// Returns bytes written and ok=false if dest is too small.
@(require_results)
serialize :: proc(
	dest: []u8,
	rowid: types.Row_ID,
	values: []types.Value,
	info: Serialization_Info,
) -> (
	bytes_written: int,
	ok: bool,
) {
	if len(dest) < info.total_size {
		return 0, false
	}

	offset := 0
	header_bytes :=
		varint.size(u64(rowid)) + varint.size(u64(info.serial_types_size)) + info.serial_types_size

	total_payload := header_bytes + info.payload_size
	offset += varint.encode(dest[offset:], u64(total_payload))
	offset += varint.encode(dest[offset:], u64(rowid))
	offset += varint.encode(dest[offset:], u64(info.serial_types_size))

	// Stack scratch covers every user row (MAX_COLS cap); wider rows
	// (schema catalog rows past one index) spill to temp. The spill
	// branch never fires on the DML hot path.
	serial_stack: [types.MAX_COLS]u64
	serial_wide: []u64
	serials := serial_stack[:]
	if len(values) > types.MAX_COLS {
		serial_wide = make([]u64, len(values), context.temp_allocator)
		serials = serial_wide
	}

	i := 0
	for val in values {
		serial := serial_type_for_value(val)
		serials[i] = serial; i += 1
		offset += varint.encode(dest[offset:], serial)
	}

	i = 0
	for val in values {
		serial := serials[i]
		i += 1
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

// decode_row_prefix reads the payload-size, rowid, and header-size varints
// at offset, returning the rowid, header size, and the position of the
// serial-type header. ok=false on short/truncated input (varint.decode
// fails closed; same call order as the inline code it replaces).
@(private = "file")
decode_row_prefix :: proc(
	src: []u8,
	offset: int,
) -> (
	rowid: u64,
	header_size: u64,
	pos: int,
	ok: bool,
) {
	if offset >= len(src) {
		return 0, 0, offset, false
	}

	pos = offset
	_, n, ok_payload := varint.decode(src, pos)
	if !ok_payload {
		return 0, 0, pos, false
	}

	pos += n
	rowid_val, n2, ok_rowid := varint.decode(src, pos)
	if !ok_rowid {
		return 0, 0, pos, false
	}

	pos += n2
	hdr, n3, ok_header := varint.decode(src, pos)
	if !ok_header {
		return 0, 0, pos, false
	}
	return rowid_val, hdr, pos + n3, true
}

// decode_row_values materializes one value per serial type into the
// caller-owned result_values (len == serial_count), advancing past each
// payload. TEXT/BLOB honor zero_copy (borrow vs clone). Bounds: every
// payload is range-checked before read (untrusted page bytes); the loop
// keeps its proven-index annotation. Returns the first unconsumed position.
@(private = "file")
decode_row_values :: proc(
	src: []u8,
	serials: []u64,
	serial_count: int,
	result_values: []types.Value,
	pos: int,
	config: Config,
	alloc: mem.Allocator,
) -> (
	next_pos: int,
	ok: bool,
) {
	next_pos = pos
	#no_bounds_check for st_idx in 0 ..< serial_count {
		st := serials[st_idx]
		content_size, _ := types.serial_type_content_size(st)
		type_code := types.Serial_Type(st)
		if next_pos + content_size > len(src) {
			return next_pos, false
		}
		if type_code == .ZERO {
			result_values[st_idx] = types.value_int(0)
		} else if type_code == .ONE {
			result_values[st_idx] = types.value_int(1)
		} else if st == u64(types.Serial_Type.NULL) {
			result_values[st_idx] = types.value_null()
		} else if st >= u64(types.Serial_Type.INT8) && st <= u64(types.Serial_Type.INT64) {
			int_val, _ := read_int_by_size(src, next_pos, content_size)
			result_values[st_idx] = types.value_int(int_val)
			next_pos += content_size
		} else if type_code == .FLOAT64 {
			float_val, _ := endian.get_f64(src[next_pos:], .Big)
			result_values[st_idx] = types.value_real(float_val)
			next_pos += 8
		} else if is_text_serial(st) {
			text_bytes := src[next_pos:next_pos + content_size]
			if config.zero_copy {
				result_values[st_idx] = types.value_text(string(text_bytes))
			} else {
				str := strings.clone_from(text_bytes, alloc)
				result_values[st_idx] = types.value_text(str)
			}
			next_pos += content_size
		} else if is_blob_serial(st) {
			blob_bytes := src[next_pos:next_pos + content_size]
			if config.zero_copy {
				result_values[st_idx] = types.value_blob(blob_bytes)
			} else {
				blob_copy := make([]u8, content_size, alloc)
				copy(blob_copy, blob_bytes)
				result_values[st_idx] = types.value_blob(blob_copy)
			}
			next_pos += content_size
		} else {
			return next_pos, false
		}
	}
	return next_pos, true
}

// Returns the Cell + bytes consumed. ok=false on invalid input.
@(require_results)
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

	rowid_val, header_size, pos, prefix_ok := decode_row_prefix(src, offset)
	if !prefix_ok {
		return {}, 0, false
	}

	header_start := pos
	// Stack scratch covers every user row; wider rows (schema catalog
	// rows past one index) spill the whole header to temp. The header
	// loop stays bounds-checked (untrusted page bytes); consumption
	// loops below keep their proven-index annotations.
	serial_stack: [types.MAX_COLS]u64
	serial_spill: [dynamic]u64
	serial_count := 0
	for pos < header_start + int(header_size) {
		st, n4, ok_st := varint.decode(src, pos)
		if !ok_st {
			return {}, 0, false
		}
		if serial_count < types.MAX_COLS {
			serial_stack[serial_count] = st
		} else {
			if serial_spill == nil {
				serial_spill = make([dynamic]u64, 0, 16, context.temp_allocator)
				append(&serial_spill, ..serial_stack[:])
			}
			append(&serial_spill, st)
		}

		serial_count += 1
		pos += n4
	}

	serials := serial_stack[:] if serial_spill == nil else serial_spill[:]
	result_values := make([]types.Value, serial_count, alloc)
	success := false
	defer if !success && !config.zero_copy {
		types.values_delete(result_values, alloc)
	}

	end_pos, values_ok := decode_row_values(
		src,
		serials,
		serial_count,
		result_values,
		pos,
		config,
		alloc,
	)
	if !values_ok {
		return {}, 0, false
	}

	success = true
	cell = Cell {
		rowid     = types.Row_ID(rowid_val),
		values    = result_values,
		owns_data = !config.zero_copy,
	}
	return cell, end_pos - offset, true
}

// deserialize_needed decodes one row-major cell but materializes only the
// columns flagged in `needed` (index = serial position). Unneeded columns are
// still walked (payload skipped via content size) so bytes_consumed matches
// deserialize exactly, but no value is produced for them: the slot is set to
// Null and no allocation or clone happens.
//
// TEXT/BLOB values for needed columns are ALWAYS borrowed from `src`:
// the caller must clone survivors before the page is unpinned/evicted. The
// cursor pins only the current page (load_cached_page unpins on page move;
// eviction reuses the slot buffer), so borrowed strings are valid only
// while the cursor stays on the page. Non-survivor rows cost zero
// allocations by construction: ints/reals/Null are by value, text/blob are
// borrows, and `out_values` is caller storage (stack or reused batch buffer).
//
// `out_values` must have len >= serial count; `needed` shorter than the
// serial count treats trailing positions as not needed. Malformed input
// fails exactly where deserialize fails.
// Returns the rowid, bytes consumed, and ok=false on invalid input.
@(require_results)
deserialize_needed :: proc(
	src: []u8,
	offset: int,
	needed: []bool,
	out_values: []types.Value,
) -> (
	rowid: types.Row_ID,
	bytes_consumed: int,
	ok: bool,
) {
	if offset >= len(src) {
		return 0, 0, false
	}

	pos := offset
	_, n, ok_payload := varint.decode(src, pos)
	if !ok_payload {
		return 0, 0, false
	}

	pos += n
	rowid_val, n2, ok_rowid := varint.decode(src, pos)
	if !ok_rowid {
		return 0, 0, false
	}

	pos += n2
	header_size, n3, ok_header := varint.decode(src, pos)
	if !ok_header {
		return 0, 0, false
	}

	pos += n3
	header_start := pos
	// Stack scratch covers every user row; wider rows (schema catalog
	// rows past one index) spill the whole header to temp. The header
	// loop stays bounds-checked (untrusted page bytes); consumption
	// loops below keep their proven-index annotations.
	serial_stack: [types.MAX_COLS]u64
	serial_spill: [dynamic]u64
	serial_count := 0
	for pos < header_start + int(header_size) {
		st, n4, ok_st := varint.decode(src, pos)
		if !ok_st {
			return 0, 0, false
		}
		if serial_count < types.MAX_COLS {
			serial_stack[serial_count] = st
		} else {
			if serial_spill == nil {
				serial_spill = make([dynamic]u64, 0, 16, context.temp_allocator)
				append(&serial_spill, ..serial_stack[:])
			}
			append(&serial_spill, st)
		}

		serial_count += 1
		pos += n4
	}

	serials := serial_stack[:] if serial_spill == nil else serial_spill[:]
	if len(out_values) < serial_count {
		return 0, 0, false
	}
	#no_bounds_check for st_idx in 0 ..< serial_count {
		st := serials[st_idx]
		content_size, _ := types.serial_type_content_size(st)
		type_code := types.Serial_Type(st)
		if pos + content_size > len(src) {
			return 0, 0, false
		}

		want := st_idx < len(needed) && needed[st_idx]
		if !want {
			pos += content_size
			out_values[st_idx] = types.value_null()
			continue
		}
		if type_code == .ZERO {
			out_values[st_idx] = types.value_int(0)
		} else if type_code == .ONE {
			out_values[st_idx] = types.value_int(1)
		} else if st == u64(types.Serial_Type.NULL) {
			out_values[st_idx] = types.value_null()
		} else if st >= u64(types.Serial_Type.INT8) && st <= u64(types.Serial_Type.INT64) {
			int_val, _ := read_int_by_size(src, pos, content_size)
			out_values[st_idx] = types.value_int(int_val)
			pos += content_size
		} else if type_code == .FLOAT64 {
			float_val, _ := endian.get_f64(src[pos:], .Big)
			out_values[st_idx] = types.value_real(float_val)
			pos += 8
		} else if is_text_serial(st) {
			text_bytes := src[pos:pos + content_size]
			out_values[st_idx] = types.value_text(string(text_bytes))
			pos += content_size
		} else if is_blob_serial(st) {
			blob_bytes := src[pos:pos + content_size]
			out_values[st_idx] = types.value_blob(blob_bytes)
			pos += content_size
		} else {
			return 0, 0, false
		}
	}
	return types.Row_ID(rowid_val), pos - offset, true
}

@(private = "file")
read_int_by_size :: proc(data: []u8, offset: int, size: int) -> (val: i64, ok: bool) {
	if offset + size > len(data) {
		return 0, false
	}

	switch size {
	case 1:
		return i64(i8(data[offset])), true
	case 2:
		return i64(i16(endian.get_u16(data[offset:], .Little) or_return)), true
	case 3:
		v := i64(data[offset]) | (i64(data[offset + 1]) << 8) | (i64(data[offset + 2]) << 16)
		if v & 0x800000 != 0 {
			v |= ~i64(0xFFFFFF)
		}
		return v, true
	case 4:
		return i64(i32(endian.get_u32(data[offset:], .Little) or_return)), true
	case 6:
		lo := endian.get_u32(data[offset:], .Little) or_return
		hi := endian.get_u16(data[offset + 4:], .Little) or_return
		v := i64(lo) | (i64(hi) << 32)
		if v & 0x8000_0000_0000 != 0 {
			v |= ~i64(0xFFFF_FFFF_FFFF)
		}
		return v, true
	case 8:
		return i64(endian.get_u64(data[offset:], .Little) or_return), true
	}
	return 0, false
}

@(private = "file")
write_int_by_size :: proc(dest: []u8, offset: int, value: i64, size: int) -> bool {
	if offset + size > len(dest) {
		return false
	}

	switch size {
	case 1:
		dest[offset] = u8(value)
		return true
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

@(private = "file")
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

@(private = "file")
is_text_serial :: proc(serial: u64) -> bool { return serial >= 13 && (serial % 2 != 0) }

@(private = "file")
is_blob_serial :: proc(serial: u64) -> bool { return serial >= 12 && (serial % 2 == 0) }
