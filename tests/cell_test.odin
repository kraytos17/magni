// Cell codec tests: create/destroy lifecycle, serialization round-trips,
// zero-copy borrow mechanics, and malformed-input fail-closed behavior.
// Buffer-level only (no DB).
package tests

import "core:testing"
import "src:cell"
import "src:types"

T :: ^testing.T

@(test)
test_lifecycle_create_destroy :: proc(t: T) {
	values := []types.Value{types.value(101), types.value("Odin Lang"), types.value(1.618)}

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
	values := []types.Value{types.value(1), types.value(blob_data)}

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
		types.value(999999),
		types.value("Hello Serialization"),
		types.value(),
		types.value(3.14159),
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
	values := []types.Value{types.value("PersistentData")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	ci := cell.compute_info(10, values)
	_, ser_ok := cell.serialize(buffer, 10, values, ci)
	testing.expect(t, ser_ok, "serialize succeeds")
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
	values_a := []types.Value{types.value(1)}
	values_b := []types.Value{types.value(2)}
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

	v1 := []types.Value{types.value(1), types.value("Alice")}
	testing.expect(t, cell.validate(v1, cols), "Valid row validation failed")

	v2 := []types.Value{types.value(2), types.value()}
	testing.expect(t, cell.validate(v2, cols), "Nullable validation failed")

	v3 := []types.Value{types.value("NaN"), types.value("Bob")}
	testing.expect(t, !cell.validate(v3, cols), "Type mismatch validation failed")

	v4 := []types.Value{types.value(), types.value("Bob")}
	testing.expect(t, !cell.validate(v4, cols), "Not-Null constraint validation failed")

	v5 := []types.Value{types.value(1)}
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
	vals := []types.Value{types.value(), types.value(), types.value()}
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
	values := []types.Value{types.value(42), types.value("SizeTest")}
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
test_deserialize_needed_all_parity :: proc(t: T) {
	original_values := []types.Value {
		types.value(0),
		types.value(1),
		types.value(-999999),
		types.value(2.5),
		types.value("Hello Needed"),
		types.value([]u8{0x01, 0x02, 0x03}),
		types.value(),
	}
	buffer := make([]u8, 1024)
	defer delete(buffer)

	ci := cell.compute_info(7, original_values)
	_, wok := cell.serialize(buffer, 7, original_values, ci)
	testing.expect(t, wok, "serialize failed")

	full, consumed_full, fok := cell.deserialize(buffer, 0)
	testing.expect(t, fok, "baseline deserialize failed")
	defer cell.destroy(&full)

	needed := []bool{true, true, true, true, true, true, true}
	out_vals := make([]types.Value, 7)
	defer delete(out_vals)
	rowid, consumed, ok := cell.deserialize_needed(buffer, 0, needed, out_vals)
	testing.expect(t, ok, "deserialize_needed failed")
	testing.expect_value(t, rowid, full.rowid)
	testing.expect_value(t, consumed, consumed_full)
	testing.expect_value(t, len(out_vals), len(full.values))
	for i in 0 ..< len(full.values) {
		testing.expect(
			t,
			types.value_compare(out_vals[i], full.values[i]),
			"all-needed parity mismatch",
		)
	}
	// Borrowed text/blob point into the source buffer (no clone).
	if s, is_str := out_vals[4].(string); is_str {
		testing.expect(t, raw_data(s) != nil, "borrowed string must be non-nil")
	}
}

@(test)
test_deserialize_needed_subset :: proc(t: T) {
	original_values := []types.Value {
		types.value(42),
		types.value("SkipMe"),
		types.value(1.5),
		types.value("KeepMe"),
	}
	buffer := make([]u8, 1024)
	defer delete(buffer)

	ci := cell.compute_info(9, original_values)
	written, wok := cell.serialize(buffer, 9, original_values, ci)
	testing.expect(t, wok, "serialize failed")

	needed := []bool{true, false, false, true}
	out_vals := make([]types.Value, 4)
	defer delete(out_vals)
	rowid, consumed, ok := cell.deserialize_needed(buffer, 0, needed, out_vals)
	testing.expect(t, ok, "subset decode failed")
	testing.expect_value(t, rowid, 9)
	testing.expect_value(t, consumed, written)
	testing.expect_value(t, out_vals[0].(i64), 42)
	testing.expect(t, types.is_null(out_vals[1]), "skipped col must be Null")
	testing.expect(t, types.is_null(out_vals[2]), "skipped col must be Null")
	testing.expect_value(t, out_vals[3].(string), "KeepMe")
	// Borrow check: no clone happened, pointer inside buffer.
	s := out_vals[3].(string)
	is_inside :=
		uintptr(raw_data(s)) >= uintptr(raw_data(buffer)) &&
		uintptr(raw_data(s)) < uintptr(raw_data(buffer)) + uintptr(len(buffer))
	testing.expect(t, is_inside, "subset text must borrow from source buffer")
}

@(test)
test_deserialize_needed_none_and_malformed :: proc(t: T) {
	original_values := []types.Value{types.value(5), types.value("x")}
	buffer := make([]u8, 256)
	defer delete(buffer)

	ci := cell.compute_info(3, original_values)
	written, wok := cell.serialize(buffer, 3, original_values, ci)
	testing.expect(t, wok, "serialize failed")

	// All-false: still consumes identical bytes, all Null.
	needed := []bool{false, false}
	out_vals := make([]types.Value, 2)
	defer delete(out_vals)
	rowid, consumed, ok := cell.deserialize_needed(buffer, 0, needed, out_vals)
	testing.expect(t, ok, "all-skipped decode failed")
	testing.expect_value(t, rowid, 3)
	testing.expect_value(t, consumed, written)
	testing.expect(t, types.is_null(out_vals[0]), "unwanted col must be Null")
	testing.expect(t, types.is_null(out_vals[1]), "unwanted col must be Null")

	// Undersized output buffer fails loudly.
	small := make([]types.Value, 1)
	defer delete(small)
	_, _, ok2 := cell.deserialize_needed(buffer, 0, needed, small)
	testing.expect(t, !ok2, "undersized out_values must fail")

	// Truncated input fails like deserialize.
	trunc := make([]u8, 2)
	defer delete(trunc)
	big_out := make([]types.Value, 2)
	defer delete(big_out)
	_, _, ok3 := cell.deserialize_needed(trunc, 0, needed, big_out)
	testing.expect(t, !ok3, "truncated buffer must fail")

	_, _, ok4 := cell.deserialize_needed(buffer, 999, needed, big_out)
	testing.expect(t, !ok4, "OOB offset must fail")
}

@(test)
test_composite_codec_order_and_failures :: proc(t: T) {
	// Framing orders by component (first differing byte decides), then
	// rowid; malformed keys order before valid ones, never equal.
	enc := proc(vals: []types.Value, r: i64) -> []u8 {
		b := make([]u8, 64, context.temp_allocator)
		n, ok := cell.composite_encode(vals, {.INTEGER, .TEXT}, types.Row_ID(r), b)
		assert(ok)
		return b[:n]
	}
	mk := proc(v1: i64, v2: string) -> []types.Value {
		out := make([]types.Value, 2, context.temp_allocator)
		out[0] = types.value(v1)
		out[1] = types.value(v2)
		return out
	}
	testing.expect(
		t,
		cell.composite_compare(enc(mk(1, "b"), 0), enc(mk(2, "a"), 0)) < 0,
		"first component decides",
	)
	testing.expect(
		t,
		cell.composite_compare(enc(mk(1, "a"), 0), enc(mk(1, "b"), 0)) < 0,
		"second component breaks ties",
	)
	testing.expect(
		t,
		cell.composite_compare(enc(mk(1, "a"), 1), enc(mk(1, "a"), 2)) < 0,
		"rowid tiebreak",
	)
	testing.expect_value(t, cell.composite_compare(enc(mk(1, "a"), 3), enc(mk(1, "a"), 3)), 0)
	// Variable-length framing is unambiguous: ("ab","c") != ("a","bc").
	testing.expect(
		t,
		cell.composite_compare(enc(mk(1, "ab"), 0), enc(mk(1, "a"), 0)) > 0,
		"framing disambiguates",
	)

	// Unindexable components skip the whole tuple (mismatch, NaN). NULL
	// frames as a marker (leading skip lives in the extractor).
	nv := make([]types.Value, 2, context.temp_allocator)
	nv[0] = types.value()
	nv[1] = types.value("x")
	testing.expect(t, cell.composite_encoded_len(nv, {.INTEGER, .TEXT}) > 0, "NULL frames")
	mv := make([]types.Value, 2, context.temp_allocator)
	mv[0] = types.value("x")
	mv[1] = types.value("y")
	testing.expect_value(t, cell.composite_encoded_len(mv, {.INTEGER, .TEXT}), 0)
	nanv := make([]types.Value, 1, context.temp_allocator)
	nanv[0] = types.value(transmute(f64)u64(0x7ff8000000000000))
	testing.expect_value(t, cell.composite_encoded_len(nanv, {.REAL}), 0)
	testing.expect_value(t, cell.composite_encoded_len(mk(1, "a"), {.INTEGER}), 0)

	// Malformed keys fail closed, never equal to valid ones.
	good := enc(mk(1, "a"), 0)
	testing.expect(t, cell.composite_wellformed(good), "valid key parses")
	testing.expect(t, !cell.composite_wellformed(good[:len(good) - 1]), "truncation fails")
	long := make([]u8, len(good) + 1, context.temp_allocator)
	copy(long, good)
	long[len(good)] = 0x00
	testing.expect(t, !cell.composite_wellformed(long), "trailing garbage fails")
	badtag := make([]u8, len(good), context.temp_allocator)
	copy(badtag, good)
	badtag[0] = 0x00
	testing.expect(t, !cell.composite_wellformed(badtag), "bad tag fails")
	testing.expect(t, cell.composite_compare(badtag, good) < 0, "invalid orders first")
	testing.expect(t, cell.composite_compare(good, badtag) > 0, "valid orders last")

	// NULL markers: NULL frames anywhere (leading skip is the extractor's
	// call, not the codec's); markers sort before valued frames.
	nulltup := make([]types.Value, 2, context.temp_allocator)
	nulltup[0] = types.value(5)
	nulltup[1] = types.value()
	nlen := cell.composite_encoded_len(nulltup, {.INTEGER, .TEXT})
	testing.expect(t, nlen > 0, "trailing NULL frames")
	nbuf := make([]u8, 64, context.temp_allocator)
	nn, nok := cell.composite_encode_raw(nulltup, {.INTEGER, .TEXT}, nbuf)
	testing.expect(t, nok, "NULL tuple frames")
	nfull := make([]u8, 64, context.temp_allocator)
	fn, fok := cell.composite_encode(nulltup, {.INTEGER, .TEXT}, 9, nfull)
	testing.expect(t, fok && cell.composite_wellformed(nfull[:fn]), "marked key parses")
	nv2, nok2 := cell.composite_decode_col(nbuf[:nn], 1)
	testing.expect(t, nok2, "marker decodes")
	if nok2 {
		testing.expect(t, types.is_null(nv2), "marker decodes to NULL")
	}

	// Blobs frame in TEXT positions after every text frame (rank order).
	bv := make([]types.Value, 2, context.temp_allocator)
	bv[0] = types.value(5)
	bv[1] = types.value([]u8{0xAB})
	blen := cell.composite_encoded_len(bv, {.INTEGER, .TEXT})
	testing.expect(t, blen > 0, "blob frames")
	bbuf := make([]u8, 64, context.temp_allocator)
	bn, bok := cell.composite_encode_raw(bv, {.INTEGER, .TEXT}, bbuf)
	testing.expect(t, bok, "blob tuple frames")
	bfull := make([]u8, 64, context.temp_allocator)
	fbn, fbok := cell.composite_encode(bv, {.INTEGER, .TEXT}, 9, bfull)
	testing.expect(t, fbok && cell.composite_wellformed(bfull[:fbn]), "blob key parses")
	bdec, bok2 := cell.composite_decode_col(bbuf[:bn], 1)
	testing.expect(t, bok2, "blob decodes")
	if bok2 {
		bb, is_blob := bdec.([]u8)
		testing.expect(t, is_blob && len(bb) == 1 && bb[0] == 0xAB, "blob bytes survive")
	}
	testing.expect(
		t,
		cell.composite_compare(bfull[:fbn], enc(mk(5, "z"), 9)) > 0,
		"blob sorts after text",
	)
	testing.expect(
		t,
		cell.composite_compare(nfull[:fn], enc(mk(5, "a"), 9)) < 0,
		"NULL marker sorts first",
	)
}
