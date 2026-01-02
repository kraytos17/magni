package tests

import "core:testing"
import "src:cell"
import "src:types"

T :: ^testing.T

@(test)
test_lifecycle_create_destroy :: proc(t: T) {
	values := []types.Value{types.value_int(101), types.value_text("Odin Lang"), types.value_real(1.618)}

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

	bytes_written, ok := cell.serialize(buffer, 42, original_values)
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

	cell.serialize(buffer, 10, values)
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
	testing.expect(t, is_inside, "Zero-copy violation: String data does not point to source buffer")
}

@(test)
test_buffer_boundaries :: proc(t: T) {
	values := []types.Value{types.value_int(123)}
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

	len_a, _ := cell.serialize(buffer[0:], 1, values_a)
	len_b, _ := cell.serialize(buffer[len_a:], 2, values_b)
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
test_utilities :: proc(t: T) {
	values := []types.Value{types.value_int(42), types.value_text("SizeTest")}
	calc_size := cell.calculate_size(1, values)
	buffer := make([]u8, 256)
	defer delete(buffer)

	written, _ := cell.serialize(buffer, 1, values)
	testing.expect_value(t, calc_size, written)

	rowid, ok := cell.get_rowid(buffer, 0)
	testing.expect(t, ok, "get_rowid failed")
	testing.expect_value(t, rowid, 1)
}
