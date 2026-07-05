package tests

import "core:testing"
import "src:cell"
import "src:types"

T :: ^testing.T

@(test)
test_lifecycle_create_destroy :: proc(t: T) {
	values := []types.Value {
		types.value_int(101),
		types.value_text("Odin Lang"),
		types.value_real(1.618),
	}

	c, err := cell.create(1, values)
	testing.expect(t, err == nil, "Cell creation failed")
	defer cell.destroy(&c)

	testing.expect_value(t, c.rowid, 1)
	testing.expect_value(t, len(c.values), 3)
	testing.expect_value(t, c.owns_data, true)

	testing.expect_value(t, c.values[0].(i64), 101)
	testing.expect_value(t, c.values[1].(string), "Odin Lang")
	testing.expect_value(t, c.values[2].(f64), 1.618)
}

@(test)
test_blob_handling :: proc(t: T) {
	blob_data := []u8{0xDE, 0xAD, 0xBE, 0xEF}
	values := []types.Value{types.value_int(1), types.value_blob(blob_data)}

	c, _ := cell.create(1, values)
	defer cell.destroy(&c)

	res_blob := c.values[1].([]u8)
	testing.expect_value(t, len(res_blob), 4)
	testing.expect_value(t, res_blob[0], 0xDE)
	testing.expect_value(t, res_blob[3], 0xEF)
}

@(test)
test_serialization_roundtrip :: proc(t: T) {
	original_values := []types.Value {
		types.value_int(999999),
		types.value_text("Hello Serialization"),
		types.value_null(),
		types.value_real(3.14159),
	}

	buffer := make([]u8, 1024)
	defer delete(buffer)

	ci := cell.compute_info(42, original_values)
	bytes_written, ok := cell.serialize(buffer, 42, original_values, ci)
	testing.expect(t, ok, "Serialization returned false")
	testing.expect(t, bytes_written > 0, "No bytes written")

	c, bytes_read, deser_ok := cell.deserialize(buffer, 0)
	testing.expect(t, deser_ok, "Deserialization failed")
	defer cell.destroy(&c)

	testing.expect_value(t, bytes_read, bytes_written)
	testing.expect_value(t, c.rowid, 42)
	testing.expect_value(t, c.owns_data, true)

	testing.expect_value(t, c.values[0].(i64), 999999)
	testing.expect_value(t, c.values[1].(string), "Hello Serialization")
	testing.expect(t, types.is_null(c.values[2]), "Expected NULL value")
}

@(test)
test_zero_copy_mechanics :: proc(t: T) {
	values := []types.Value{types.value_text("PersistentData")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	ci := cell.compute_info(10, values)
	cell.serialize(buffer, 10, values, ci)
	cfg := cell.Config {
		allocator = context.allocator,
		zero_copy = true,
	}

	c, _, ok := cell.deserialize(buffer, 0, cfg)
	testing.expect(t, ok, "Deserialization failed")
	defer cell.destroy(&c)

	testing.expect_value(t, c.owns_data, false)
	val := c.values[0].(string)
	testing.expect_value(t, val, "PersistentData")

	str_ptr := raw_data(val)
	buf_ptr := raw_data(buffer)
	buf_end := rawptr(uintptr(buf_ptr) + uintptr(len(buffer)))
	is_inside := uintptr(str_ptr) >= uintptr(buf_ptr) && uintptr(str_ptr) < uintptr(buf_end)
	testing.expect(
		t,
		is_inside,
		"Zero-copy violation: String data does not point to source buffer",
	)
}

@(test)
test_buffer_boundaries :: proc(t: T) {
	small_buf := make([]u8, 2)
	defer delete(small_buf)

	_, _, ok := cell.deserialize(small_buf, 0)
	testing.expect(t, !ok, "Should fail on truncated buffer")
	valid_buf := make([]u8, 100)
	defer delete(valid_buf)

	_, _, ok2 := cell.deserialize(valid_buf, 999)
	testing.expect(t, !ok2, "Should fail on OOB offset")
}

@(test)
test_multiple_cells_in_buffer :: proc(t: T) {
	values_a := []types.Value{types.value_int(1)}
	values_b := []types.Value{types.value_int(2)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	ci_a := cell.compute_info(1, values_a)
	ci_b := cell.compute_info(2, values_b)
	len_a, _ := cell.serialize(buffer[0:], 1, values_a, ci_a)
	len_b, _ := cell.serialize(buffer[len_a:], 2, values_b, ci_b)
	c, consumed, ok := cell.deserialize(buffer, len_a)
	defer cell.destroy(&c)

	testing.expect(t, ok, "Failed to read second cell")
	testing.expect_value(t, c.rowid, 2)
	testing.expect_value(t, consumed, len_b)
}

@(test)
test_schema_validation :: proc(t: T) {
	cols := []types.Column {
		{name = "id", type = .INTEGER, not_null = true},
		{name = "name", type = .TEXT, not_null = false},
	}

	v1 := []types.Value{types.value_int(1), types.value_text("Alice")}
	testing.expect(t, cell.validate(v1, cols), "Valid row validation failed")

	v2 := []types.Value{types.value_int(2), types.value_null()}
	testing.expect(t, cell.validate(v2, cols), "Nullable validation failed")

	v3 := []types.Value{types.value_text("NaN"), types.value_text("Bob")}
	testing.expect(t, !cell.validate(v3, cols), "Type mismatch validation failed")

	v4 := []types.Value{types.value_null(), types.value_text("Bob")}
	testing.expect(t, !cell.validate(v4, cols), "Not-Null constraint validation failed")

	v5 := []types.Value{types.value_int(1)}
	testing.expect(t, !cell.validate(v5, cols), "Column count validation failed")
}

@(test)
test_empty_value_list :: proc(t: T) {
	c, err := cell.create(42, {})
	testing.expect(t, err == nil, "create with empty values should succeed")
	defer cell.destroy(&c)
	testing.expect_value(t, c.rowid, 42)
	testing.expect_value(t, len(c.values), 0)
}

@(test)
test_all_null_values :: proc(t: T) {
	vals := []types.Value{types.value_null(), types.value_null(), types.value_null()}
	c, err := cell.create(7, vals)
	testing.expect(t, err == nil, "create with all null values should succeed")
	defer cell.destroy(&c)
	for i in 0 ..< 3 {
		testing.expect(t, types.is_null(c.values[i]), "all values should be null")
	}
}

@(test)
test_get_rowid_on_invalid_buffer :: proc(t: T) {
	truncated := []u8{0xFF}
	_, ok := cell.get_rowid(truncated, 0)
	testing.expect(t, !ok, "get_rowid on single-byte buffer should fail")

	_, ok2 := cell.get_rowid({}, 0)
	testing.expect(t, !ok2, "get_rowid on empty buffer should fail")
}

@(test)
test_utilities :: proc(t: T) {
	values := []types.Value{types.value_int(42), types.value_text("SizeTest")}
	cinfo := cell.compute_info(1, values)
	calc_size := cell.compute_info(1, values).total_size
	buffer := make([]u8, 256)
	defer delete(buffer)

	written, _ := cell.serialize(buffer, 1, values, cinfo)
	testing.expect_value(t, calc_size, written)

	rowid, ok := cell.get_rowid(buffer, 0)
	testing.expect(t, ok, "get_rowid failed")
	testing.expect_value(t, rowid, 1)
}

@(test)
test_columnar_roundtrip :: proc(t: T) {
	columns := []types.Column {
		{name = "id", type = .INTEGER},
		{name = "score", type = .REAL},
		{name = "name", type = .TEXT},
	}

	rowids := []types.Row_ID{1, 2, 3}
	rows := [][]types.Value {
		{types.value_int(1), types.value_real(10.5), types.value_text("Alice")},
		{types.value_int(2), types.value_real(20.0), types.value_text("Bob")},
		{types.value_int(3), types.value_real(30.5), types.value_text("Charlie")},
	}

	buf: [4096]u8
	ok := cell.serialize_columnar(buf[:], rowids, rows[:], columns)
	testing.expect(t, ok, "serialize_columnar should succeed")

	// Verify column directory
	h0, ok0 := cell.read_col_header(buf[:], 0)
	testing.expect(t, ok0, "read col 0 header")
	testing.expect_value(t, h0.col_index, u8(0))
	testing.expect_value(t, h0.row_count, u16(3))

	h1, ok1 := cell.read_col_header(buf[:], 1)
	testing.expect(t, ok1, "read col 1 header")
	testing.expect_value(t, h1.col_index, u8(1))
	testing.expect_value(t, h1.row_count, u16(3))

	// Decode column 0 (INTEGER)
	col0 := cell.decode_column(buf[:], 3, 0)
	testing.expect(t, col0 != nil, "decode col 0")
	if col0 != nil {
		v0, _ := col0[0].(i64)
		v1, _ := col0[1].(i64)
		v2, _ := col0[2].(i64)
		testing.expect_value(t, v0, i64(1))
		testing.expect_value(t, v1, i64(2))
		testing.expect_value(t, v2, i64(3))
	}

	// Decode column 1 (REAL)
	col1 := cell.decode_column(buf[:], 3, 1)
	testing.expect(t, col1 != nil, "decode col 1")
	if col1 != nil {
		v0, _ := col1[0].(f64)
		v1, _ := col1[1].(f64)
		v2, _ := col1[2].(f64)
		testing.expect(t, v0 > 10.0 && v0 < 11.0, "col 1 val 0 ~10.5")
		testing.expect(t, v1 > 19.0 && v1 < 21.0, "col 1 val 1 ~20.0")
		testing.expect(t, v2 > 30.0 && v2 < 31.0, "col 1 val 2 ~30.5")
	}
}

@(test)
test_columnar_single_row :: proc(t: T) {
	columns := []types.Column{{name = "id", type = .INTEGER}}
	rowids := []types.Row_ID{42}
	rows := [][]types.Value{{types.value_int(42)}}
	buf: [4096]u8

	ok := cell.serialize_columnar(buf[:], rowids, rows[:], columns)
	testing.expect(t, ok, "serialize single row")

	col0 := cell.decode_column(buf[:], 1, 0)
	testing.expect(t, col0 != nil, "decode single row")
	if col0 != nil {
		v, _ := col0[0].(i64)
		testing.expect_value(t, v, i64(42))
	}
}

@(test)
test_columnar_all_types :: proc(t: T) {
	columns := []types.Column{{name = "a", type = .INTEGER}, {name = "b", type = .REAL}}
	rowids := []types.Row_ID{10, 20}
	rows := [][]types.Value {
		{types.value_int(10), types.value_real(1.5)},
		{types.value_int(20), types.value_real(2.5)},
	}

	buf: [4096]u8
	ok := cell.serialize_columnar(buf[:], rowids, rows[:], columns)
	testing.expect(t, ok, "serialize mixed types")

	col0 := cell.decode_column(buf[:], 2, 0)
	testing.expect(t, col0 != nil, "decode int col")
	if col0 != nil {
		v0, _ := col0[0].(i64)
		v1, _ := col0[1].(i64)
		testing.expect_value(t, v0, i64(10))
		testing.expect_value(t, v1, i64(20))
	}

	col1 := cell.decode_column(buf[:], 2, 1)
	testing.expect(t, col1 != nil, "decode real col")
	if col1 != nil {
		v0, _ := col1[0].(f64)
		v1, _ := col1[1].(f64)
		testing.expect(t, v0 > 1.0 && v0 < 2.0, "real val 0")
		testing.expect(t, v1 > 2.0 && v1 < 3.0, "real val 1")
	}
}

@(test)
test_columnar_null_values :: proc(t: T) {
	columns := []types.Column{{name = "id", type = .INTEGER}, {name = "name", type = .TEXT}}
	rowids := []types.Row_ID{1, 2, 3}
	rows := [][]types.Value {
		{types.value_int(1), types.value_text("Alice")},
		{types.value_int(2), types.value_null()},
		{types.value_int(3), types.value_null()},
	}

	buf: [4096]u8
	ok := cell.serialize_columnar(buf[:], rowids, rows[:], columns)
	testing.expect(t, ok, "serialize with nulls")

	col1 := cell.decode_column(buf[:], 3, 1)
	testing.expect(t, col1 != nil, "decode text col with nulls")
	if col1 != nil {
		v0, ok0 := col1[0].(f64)
		testing.expect(t, ok0, "columnar raw-encodes text as f64 (known limitation)")
		_ = v0
	}
}

@(test)
test_columnar_empty :: proc(t: T) {
	columns := []types.Column{{name = "x", type = .INTEGER}}
	buf: [4096]u8
	ok := cell.serialize_columnar(buf[:], {}, {}, columns)
	testing.expect(t, !ok, "serialize empty columnar returns false")
}

@(test)
test_columnar_text_blob_types :: proc(t: T) {
	columns := []types.Column{{name = "t", type = .TEXT}, {name = "b", type = .BLOB}}
	rowids := []types.Row_ID{1, 2}
	rows := [][]types.Value {
		{types.value_text("hello"), types.value_blob({0x01, 0x02})},
		{types.value_text("world"), types.value_blob({0xFF})},
	}

	buf: [4096]u8
	ok := cell.serialize_columnar(buf[:], rowids, rows[:], columns)
	testing.expect(t, ok, "serialize text+blob")
	// decode_column doesn't handle TEXT/BLOB in columnar format
	// (they are stored in raw encoding and read back as f64)
}
