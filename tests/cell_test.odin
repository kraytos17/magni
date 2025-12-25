package tests

import "core:testing"
import "src:cell"
import "src:types"

@(test)
test_cell_create_and_destroy :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42), types.value_text("hello"), types.value_real(3.14)}
	c, err := cell.cell_create(1, values)
	defer cell.cell_destroy(&c)

	testing.expect(t, err == nil, "Failed to create cell")
	testing.expect(t, c.rowid == 1, "Rowid mismatch")
	testing.expect(t, len(c.values) == 3, "Value count mismatch")

	val0 := c.values[0].(i64)
	testing.expect(t, val0 == 42, "Integer value mismatch")

	val1 := c.values[1].(string)
	testing.expect(t, val1 == "hello", "String value mismatch")

	val2 := c.values[2].(f64)
	testing.expect(t, val2 == 3.14, "Float value mismatch")
}

@(test)
test_cell_create_with_blob :: proc(t: ^testing.T) {
	blob_data := []u8{0x01, 0x02, 0x03, 0x04, 0x05}
	values := []types.Value{types.value_int(1), types.value_blob(blob_data)}

	c, err := cell.cell_create(1, values)
	defer cell.cell_destroy(&c)

	testing.expect(t, err == nil, "Failed to create cell with blob")
	blob := c.values[1].([]u8)
	testing.expect(t, len(blob) == 5, "Blob length mismatch")
	testing.expect(t, blob[0] == 0x01, "Blob data mismatch")
	testing.expect(t, blob[4] == 0x05, "Blob data mismatch")
}

@(test)
test_cell_calculate_size :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42), types.value_text("hello")}
	size := cell.cell_calculate_size(1, values)
	testing.expect(t, size > 0, "Cell size should be positive")
	testing.expect(t, size < 1024, "Cell size should be reasonable")
}

@(test)
test_cell_calculate_size_various_types :: proc(t: ^testing.T) {
	values1 := []types.Value{types.value_int(1)}
	size1 := cell.cell_calculate_size(1, values1)
	values2 := []types.Value{types.value_int(1), types.value_text("test")}
	size2 := cell.cell_calculate_size(1, values2)

	testing.expect(t, size2 > size1, "Adding text should increase size")
	values3 := []types.Value{types.value_int(1), types.value_text("this is a much longer text string")}
	size3 := cell.cell_calculate_size(1, values3)
	testing.expect(t, size3 > size2, "Longer text should increase size")
}

@(test)
test_cell_serialize_deserialize_simple :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")
	testing.expect(t, bytes_written > 0, "Should write some bytes")

	c, bytes_read, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	testing.expect(t, bytes_read == bytes_written, "Bytes read/written mismatch")
	testing.expect(t, c.rowid == 1, "Rowid mismatch")
	testing.expect(t, len(c.values) == 1, "Value count mismatch")

	val := c.values[0].(i64)
	testing.expect(t, val == 42, "Value mismatch")
}

@(test)
test_cell_serialize_deserialize_multiple_values :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(100), types.value_text("world"), types.value_real(2.718)}
	buffer := make([]u8, 512)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 5, values)
	testing.expect(t, ok, "Serialization failed")

	c, bytes_read, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	testing.expect(t, bytes_read == bytes_written, "Bytes mismatch")
	testing.expect(t, c.rowid == 5, "Rowid mismatch")
	testing.expect(t, len(c.values) == 3, "Value count mismatch")

	val0 := c.values[0].(i64)
	testing.expect(t, val0 == 100, "Integer mismatch")

	val1 := c.values[1].(string)
	testing.expect(t, val1 == "world", "String mismatch")

	val2 := c.values[2].(f64)
	testing.expect(t, val2 == 2.718, "Float mismatch")
}

@(test)
test_cell_serialize_deserialize_with_null :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(1), types.value_null(), types.value_text("test")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	testing.expect(t, len(c.values) == 3, "Value count mismatch")
	testing.expect(t, types.is_null(c.values[1]), "Second value should be NULL")
}

@(test)
test_cell_serialize_deserialize_blob :: proc(t: ^testing.T) {
	blob_data := []u8{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
	values := []types.Value{types.value_int(1), types.value_blob(blob_data)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 10, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	blob := c.values[1].([]u8)
	testing.expect(t, len(blob) == 6, "Blob length mismatch")
	testing.expect(t, blob[0] == 0xDE, "Blob data mismatch")
	testing.expect(t, blob[5] == 0xFE, "Blob data mismatch")
}

@(test)
test_cell_serialize_buffer_too_small :: proc(t: ^testing.T) {
	values := []types.Value{types.value_text("this is a very long string that won't fit")}
	small_buffer := make([]u8, 5)
	defer delete(small_buffer)

	_, ok := cell.cell_serialize(small_buffer, 1, values)
	testing.expect(t, !ok, "Should fail with small buffer")
}

@(test)
test_cell_serialize_empty_buffer :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(1)}
	empty_buffer := []u8{}
	_, ok := cell.cell_serialize(empty_buffer, 1, values)
	testing.expect(t, !ok, "Should fail with empty buffer")
}

@(test)
test_cell_deserialize_invalid_offset :: proc(t: ^testing.T) {
	buffer := make([]u8, 10)
	defer delete(buffer)

	_, _, ok := cell.cell_deserialize(buffer, 100)
	testing.expect(t, !ok, "Should fail with invalid offset")
}

@(test)
test_cell_deserialize_zero_copy :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(1), types.value_text("shared")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	cell.cell_serialize(buffer, 1, values)
	opts := cell.Deserialize_Options {
		allocator = context.allocator,
		zero_copy = true,
	}

	c, _, ok := cell.cell_deserialize(buffer, 0, opts)
	defer delete(c.values)
	
	testing.expect(t, ok, "Deserialization failed")
	text := c.values[1].(string)
	testing.expect(t, text == "shared", "Text mismatch")
}

@(test)
test_cell_get_rowid :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	cell.cell_serialize(buffer, 123, values)
	rowid, ok := cell.cell_get_rowid(buffer, 0)
	testing.expect(t, ok, "Failed to get rowid")
	testing.expect(t, rowid == 123, "Rowid mismatch")
}

@(test)
test_cell_get_rowid_invalid :: proc(t: ^testing.T) {
	buffer := make([]u8, 2)
	defer delete(buffer)

	_, ok := cell.cell_get_rowid(buffer, 10)
	testing.expect(t, !ok, "Should fail with invalid offset")
}

@(test)
test_cell_get_size :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(1), types.value_text("test")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, _ := cell.cell_serialize(buffer, 1, values)
	size, ok := cell.cell_get_size(buffer, 0)
	testing.expect(t, ok, "Failed to get size")
	testing.expect(t, size == bytes_written, "Size mismatch")
}

@(test)
test_cell_get_size_invalid :: proc(t: ^testing.T) {
	buffer := make([]u8, 5)
	defer delete(buffer)

	_, ok := cell.cell_get_size(buffer, 10)
	testing.expect(t, !ok, "Should fail with invalid offset")
}

@(test)
test_cell_validate_types_valid :: proc(t: ^testing.T) {
	columns := []types.Column {
		{name = "id", type = .INTEGER, not_null = true, pk = true},
		{name = "name", type = .TEXT, not_null = true, pk = false},
		{name = "score", type = .REAL, not_null = false, pk = false},
	}

	values := []types.Value{types.value_int(1), types.value_text("test"), types.value_real(99.5)}
	valid := cell.cell_validate_types(values, columns)
	testing.expect(t, valid, "Validation should pass")
}

@(test)
test_cell_validate_types_wrong_count :: proc(t: ^testing.T) {
	columns := []types.Column {
		{name = "id", type = .INTEGER, not_null = true, pk = true},
		{name = "name", type = .TEXT, not_null = true, pk = false},
	}

	values := []types.Value{types.value_int(1)}
	valid := cell.cell_validate_types(values, columns)
	testing.expect(t, !valid, "Should fail with wrong value count")
}

@(test)
test_cell_validate_types_null_violation :: proc(t: ^testing.T) {
	columns := []types.Column{{name = "id", type = .INTEGER, not_null = true, pk = true}}
	values := []types.Value{types.value_null()}
	valid := cell.cell_validate_types(values, columns)
	testing.expect(t, !valid, "Should fail with NULL in NOT NULL column")
}

@(test)
test_cell_validate_types_type_mismatch :: proc(t: ^testing.T) {
	columns := []types.Column{{name = "id", type = .INTEGER, not_null = true, pk = true}}
	values := []types.Value{types.value_text("not an integer")}
	valid := cell.cell_validate_types(values, columns)
	testing.expect(t, !valid, "Should fail with type mismatch")
}

@(test)
test_cell_validate_types_nullable_with_null :: proc(t: ^testing.T) {
	columns := []types.Column {
		{name = "id", type = .INTEGER, not_null = true, pk = true},
		{name = "optional", type = .TEXT, not_null = false, pk = false},
	}

	values := []types.Value{types.value_int(1), types.value_null()}
	valid := cell.cell_validate_types(values, columns)
	testing.expect(t, valid, "Should allow NULL in nullable column")
}

@(test)
test_cell_clone :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42), types.value_text("original")}
	original, _ := cell.cell_create(1, values)
	defer cell.cell_destroy(&original)

	cloned, err := cell.cell_clone(original)
	defer cell.cell_destroy(&cloned)

	testing.expect(t, err == nil, "Clone failed")
	testing.expect(t, cloned.rowid == original.rowid, "Rowid mismatch")
	testing.expect(t, len(cloned.values) == len(original.values), "Value count mismatch")

	val0 := cloned.values[0].(i64)
	testing.expect(t, val0 == 42, "Integer mismatch")

	val1 := cloned.values[1].(string)
	testing.expect(t, val1 == "original", "String mismatch")
}

@(test)
test_cell_serialize_special_integers :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(0), types.value_int(1), types.value_int(2)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	val0 := c.values[0].(i64)
	val1 := c.values[1].(i64)
	val2 := c.values[2].(i64)

	testing.expect(t, val0 == 0, "Zero mismatch")
	testing.expect(t, val1 == 1, "One mismatch")
	testing.expect(t, val2 == 2, "Two mismatch")
}

@(test)
test_cell_serialize_large_integers :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(i64(max(i32))), types.value_int(i64(min(i32)))}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	val0 := c.values[0].(i64)
	val1 := c.values[1].(i64)

	testing.expect(t, val0 == i64(max(i32)), "Max i32 mismatch")
	testing.expect(t, val1 == i64(min(i32)), "Min i32 mismatch")
}

@(test)
test_cell_serialize_empty_string :: proc(t: ^testing.T) {
	values := []types.Value{types.value_text("")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	text := c.values[0].(string)
	testing.expect(t, text == "", "Empty string mismatch")
}

@(test)
test_cell_serialize_empty_blob :: proc(t: ^testing.T) {
	empty_blob := []u8{}
	values := []types.Value{types.value_blob(empty_blob)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 1, values)
	testing.expect(t, ok, "Serialization failed")

	c, _, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")

	blob := c.values[0].([]u8)
	testing.expect(t, len(blob) == 0, "Empty blob mismatch")
}

@(test)
test_cell_roundtrip_complex :: proc(t: ^testing.T) {
	blob := []u8{1, 2, 3, 4, 5}
	values := []types.Value {
		types.value_int(12345),
		types.value_text("Hello, World!"),
		types.value_real(3.14159),
		types.value_null(),
		types.value_blob(blob),
		types.value_int(0),
		types.value_int(1),
	}

	buffer := make([]u8, 1024)
	defer delete(buffer)

	bytes_written, ok := cell.cell_serialize(buffer, 999, values)
	testing.expect(t, ok, "Serialization failed")

	c, bytes_read, deser_ok := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c)

	testing.expect(t, deser_ok, "Deserialization failed")
	testing.expect(t, bytes_read == bytes_written, "Bytes mismatch")
	testing.expect(t, c.rowid == 999, "Rowid mismatch")
	testing.expect(t, len(c.values) == 7, "Value count mismatch")

	testing.expect(t, c.values[0].(i64) == 12345, "Int mismatch")
	testing.expect(t, c.values[1].(string) == "Hello, World!", "String mismatch")
	testing.expect(t, c.values[2].(f64) == 3.14159, "Float mismatch")
	testing.expect(t, types.is_null(c.values[3]), "NULL mismatch")

	result_blob := c.values[4].([]u8)
	testing.expect(t, len(result_blob) == 5, "Blob length mismatch")

	testing.expect(t, c.values[5].(i64) == 0, "Zero mismatch")
	testing.expect(t, c.values[6].(i64) == 1, "One mismatch")
}

@(test)
test_cell_serialize_at_offset :: proc(t: ^testing.T) {
	values := []types.Value{types.value_int(42)}
	buffer := make([]u8, 256)
	defer delete(buffer)

	bytes1, _ := cell.cell_serialize(buffer[0:], 1, values)
	bytes2, _ := cell.cell_serialize(buffer[bytes1:], 2, values)
	c1, _, ok1 := cell.cell_deserialize(buffer, 0)
	defer cell.cell_destroy(&c1)

	c2, _, ok2 := cell.cell_deserialize(buffer, bytes1)
	defer cell.cell_destroy(&c2)

	testing.expect(t, ok1 && ok2, "Deserialization failed")
	testing.expect(t, c1.rowid == 1, "First rowid mismatch")
	testing.expect(t, c2.rowid == 2, "Second rowid mismatch")
}
