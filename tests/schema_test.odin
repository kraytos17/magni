package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:pager"
import "src:schema"
import "src:types"

setup_schema_env :: proc(t: ^testing.T, test_name: string) -> (btree.Tree, string) {
	filename := fmt.tprintf("test_schema_%s.db", test_name)
	safe_filename, _ := strings.clone(filename, context.allocator)
	os.remove(safe_filename)

	p, err := pager.open(safe_filename)
	testing.expect(t, err == nil, "Failed to open pager")

	schema_page, aerr := pager.allocate_page(p)
	testing.expect(t, aerr == .None, "Failed to allocate schema page")
	btree.init_leaf_page(schema_page.data, schema_page.page_num)
	pager.mark_dirty(p, schema_page.page_num)
	pager.unpin_page(p, schema_page.page_num)

	tree := btree.init(p, schema_page.page_num)
	ok := schema.init(&tree)
	testing.expect(t, ok, "Failed to init schema B-Tree")
	return tree, safe_filename
}

teardown_schema_env :: proc(tree: btree.Tree, filename: string) {
	_ = pager.close(tree.pager)
	os.remove(filename)
	wal_name := fmt.tprintf("%s-wal", filename)
	os.remove(wal_name)
	delete(filename, context.allocator)
}

@(test)
test_column_blob_roundtrip :: proc(t: ^testing.T) {
	cols := []types.Column {
		{name = "id", type = .INTEGER, pk = true, not_null = true},
		{name = "username", type = .TEXT, pk = false, not_null = true},
		{name = "score", type = .REAL, pk = false, not_null = false},
	}

	blob := schema.serialize_columns_to_blob(cols, context.temp_allocator)
	testing.expect(t, len(blob) > 4, "Blob too small")

	restored := schema.deserialize_columns(blob, context.temp_allocator)
	testing.expect_value(t, len(restored), 3)

	testing.expect_value(t, restored[0].name, "id")
	testing.expect_value(t, restored[0].pk, true)

	testing.expect_value(t, restored[1].name, "username")
	testing.expect_value(t, restored[1].type, types.Column_Type.TEXT)

	testing.expect_value(t, restored[2].name, "score")
	testing.expect_value(t, restored[2].not_null, false)
}

@(test)
test_add_and_find_table :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "basic_ops")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	root_page := u32(2)
	sql := "CREATE TABLE users (id INT)"

	added := schema.add_table(&tree, "users", cols, root_page, sql)
	testing.expect(t, added, "schema.add_table failed")

	tbl, found := schema.find_table(&tree, "users", context.temp_allocator)
	testing.expect(t, found, "Table 'users' not found after insertion")

	testing.expect_value(t, tbl.name, "users")
	testing.expect_value(t, tbl.root_page, root_page)
	testing.expect_value(t, tbl.sql, sql)
	testing.expect_value(t, len(tbl.columns), 1)
}

@(test)
test_table_persistence :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "persistence")
	schema_root := tree.root
	cols := []types.Column{{name = "x", type = .INTEGER}}

	ok := schema.add_table(&tree, "persistent", cols, 99, "")
	testing.expect(t, ok, "add_table failed in persistence test")
	_ = pager.close(tree.pager)

	p2, _ := pager.open(file)
	tree2 := btree.init(p2, schema_root)
	defer teardown_schema_env(tree2, file)

	exists := schema.table_exists(&tree2, "persistent")
	testing.expect(t, exists, "Table lost after reload")
}

@(test)
test_list_tables :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "list")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "a", type = .INTEGER}}
	schema.add_table(&tree, "t1", cols, 2, "")
	schema.add_table(&tree, "t2", cols, 3, "")
	schema.add_table(&tree, "t3", cols, 4, "")

	tables := schema.list_tables(&tree, context.temp_allocator)
	testing.expect_value(t, len(tables), 3)
	found_count := 0
	for tbl in tables {
		if tbl.name == "t1" || tbl.name == "t2" || tbl.name == "t3" {
			found_count += 1
		}
	}
	testing.expect_value(t, found_count, 3)
}

@(test)
test_drop_table :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "drop")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	schema.add_table(&tree, "to_delete", cols, 10, "")
	testing.expect(t, schema.table_exists(&tree, "to_delete"), "Pre-condition failed")

	dropped := schema.drop_table(&tree, "to_delete")
	testing.expect(t, dropped, "drop_table returned false")
	testing.expect(t, !schema.table_exists(&tree, "to_delete"), "Table still exists after drop")
}

@(test)
test_column_validation :: proc(t: ^testing.T) {
	c1 := []types.Column{{name = "ok", type = .INTEGER}}
	ok1, _ := schema.validate_columns(c1)
	testing.expect(t, ok1, "Valid column failed")

	c2 := []types.Column{}
	ok2, msg2 := schema.validate_columns(c2)
	testing.expect(t, !ok2, "Empty columns allowed")
	testing.expect(t, strings.contains(msg2, "at least one"), "Wrong error message")

	c3 := []types.Column{{name = "dup", type = .INTEGER}, {name = "dup", type = .TEXT}}
	ok3, msg3 := schema.validate_columns(c3)
	testing.expect(t, !ok3, "Duplicate columns allowed")
	testing.expect(t, strings.contains(msg3, "Duplicate"), "Wrong error message")
}

@(test)
test_get_table_deep_copy :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "deep_copy")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "data", type = .BLOB}}
	schema.add_table(&tree, "deep", cols, 50, "")

	tbl, found := schema.get_table(&tree, "deep", context.allocator)
	testing.expect(t, found, "Table not found")
	defer schema.table_free(tbl, context.allocator)

	free_all(context.temp_allocator)
	testing.expect_value(t, tbl.name, "deep")
	testing.expect_value(t, tbl.columns[0].name, "data")
}

@(test)
test_find_nonexistent_table :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "find_nonexist")
	defer teardown_schema_env(tree, file)

	_, found := schema.find_table(&tree, "ghost", context.temp_allocator)
	testing.expect(t, !found, "find_table should return false for non-existent table")
}

@(test)
test_drop_nonexistent_table :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "drop_nonexist")
	defer teardown_schema_env(tree, file)

	dropped := schema.drop_table(&tree, "ghost")
	testing.expect(t, !dropped, "drop_table should return false for non-existent table")
}

@(test)
test_duplicate_table_name :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "dup_name")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	ok1 := schema.add_table(&tree, "dup", cols, 2, "")
	testing.expect(t, ok1, "First add should succeed")

	ok2 := schema.add_table(&tree, "dup", cols, 3, "")
	testing.expect(t, !ok2, "Duplicate add should fail")
}

@(test)
test_schema_hash_collision :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "hashcol")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}

	// Normal add succeeds
	ok1 := schema.add_table(&tree, "mytable", cols, 42, "")
	testing.expect(t, ok1, "First add succeeds")

	// Same name again fails (duplicate detection)
	ok_dup := schema.add_table(&tree, "mytable", cols, 99, "")
	testing.expect(t, !ok_dup, "Duplicate name rejected")

	// Force a hash collision: manually insert a row at "mytable"'s hash with a different name.
	// This simulates what happens if two different table names hash to the same value.
	target_hash := types.Row_ID(types.hash_string("mytable"))
	collision_vals := []types.Value {
		types.value_int(0),
		types.value_text("intruder"),
		types.value_int(999),
		types.value_text(""),
		types.value_blob({}),
		types.value_int(0),
	}

	btree.tree_delete(&tree, target_hash)
	btree.tree_insert(&tree, target_hash, collision_vals)
	// The original "mytable" row was overwritten by the collision row (same hash key).
	// get_table("mytable") should fail because the row at that hash now has name "intruder".
	_, found_mytable := schema.get_table(&tree, "mytable")
	testing.expect(t, !found_mytable, "mytable overwritten by collision row")

	// The collision row can be found by reading the schema tree at the hash key directly.
	// It has name "intruder" but lives at hash("mytable"), so get_table("intruder") won't find it.
	_, found_intruder := schema.get_table(&tree, "intruder")
	testing.expect(t, !found_intruder, "intruder not accessible by name (lives at different hash)")

	// Verify the collision row is physically present at the hash key
	c, find_err := btree.tree_find(&tree, target_hash, context.temp_allocator)
	testing.expect(t, find_err == .None, "row exists at target hash")
	if find_err == .None {
		stored_name, _ := c.values[1].(string)
		testing.expect_value(t, stored_name, "intruder")
	}
}

@(test)
test_schema_row_roundtrip :: proc(t: ^testing.T) {
	r := schema.Schema_Row {
		kind         = "table",
		name         = "test_tbl",
		root_page    = 42,
		sql          = "CREATE TABLE test_tbl (id INT)",
		columns_blob = []u8{1, 2, 3, 4},
		skip_root    = 7,
	}

	// New format with skip_root: produces 6 values (kind byte + 5 fields)
	values := schema.schema_row_to_values(r)
	testing.expect(t, len(values) == 6, "skip_root>0 produces 6 values")

	r2, ok := schema.schema_row_from_values(values)
	testing.expect(t, ok, "new format skip>0 round-trip")
	testing.expect_value(t, r2.kind, r.kind)
	testing.expect_value(t, r2.name, r.name)
	testing.expect_value(t, r2.root_page, r.root_page)
	testing.expect_value(t, r2.sql, r.sql)
	testing.expect_value(t, r2.skip_root, r.skip_root)

	// New format without skip_root: produces 5 values
	r0 := r
	r0.skip_root = 0
	values5 := schema.schema_row_to_values(r0)
	testing.expect(t, len(values5) == 5, "skip_root=0 produces 5 values")

	r5, ok5 := schema.schema_row_from_values(values5)
	testing.expect(t, ok5, "5-value format accepted")
	testing.expect_value(t, r5.skip_root, u32(0))
	testing.expect_value(t, r5.name, "test_tbl")
	testing.expect_value(t, r5.root_page, u32(42))

	// values[0] is i64(0) for kind=table
	v0, is_int := values[0].(i64)
	testing.expect(t, is_int && v0 == 0, "values[0] is i64(0)")

	// Invalid: too few values
	_, bad := schema.schema_row_from_values(
		[]types.Value{types.value_int(0), types.value_text("x")},
	)
	testing.expect(t, !bad, "<5 values rejected")

	// Invalid: wrong type at values[0]
	_, bad2 := schema.schema_row_from_values(
		[]types.Value {
			types.value_text("table"),
			types.value_text("x"),
			types.value_int(1),
			types.value_text(""),
			types.value_blob({}),
		},
	)
	testing.expect(t, !bad2, "string at values[0] rejected")
}

@(test)
test_column_blob_version :: proc(t: ^testing.T) {
	// A blob with a marker byte but wrong version should be rejected
	bad_blob := []u8{0xFE, 0xFF, 0x01} // marker=0xFE, version=0xFF, count=1
	result := schema.deserialize_columns(bad_blob, context.temp_allocator)
	testing.expect(t, result == nil, "wrong column blob version rejected")
}

@(test)
test_list_tables_empty :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "list_empty")
	defer teardown_schema_env(tree, file)

	tables := schema.list_tables(&tree, context.temp_allocator)
	testing.expect_value(t, len(tables), 0)
}

@(test)
test_schema_unknown_kind :: proc(t: ^testing.T) {
	// A row with kind=5 (unknown, not 0=table) should be rejected
	vals := []types.Value {
		types.value_int(5),
		types.value_text("weird"),
		types.value_int(1),
		types.value_text(""),
		types.value_blob({}),
	}
	_, ok := schema.schema_row_from_values(vals)
	testing.expect(t, !ok, "unknown kind rejected")
}

@(test)
test_schema_special_char_names :: proc(t: ^testing.T) {
	tree, file := setup_schema_env(t, "spec_names")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "col one", type = .INTEGER}}
	ok := schema.add_table(&tree, "my table", cols, 2, "")
	testing.expect(t, ok, "table name with space added")

	tbl, found := schema.find_table(&tree, "my table", context.temp_allocator)
	testing.expect(t, found, "table with space found")
	testing.expect_value(t, tbl.name, "my table")
	testing.expect_value(t, tbl.columns[0].name, "col one")
}

@(test)
test_schema_row_kind_as_string :: proc(t: ^testing.T) {
	// kind must be an int, not a string
	vals := []types.Value {
		types.value_text("table"),
		types.value_text("x"),
		types.value_int(1),
		types.value_text(""),
		types.value_blob({}),
	}
	_, ok := schema.schema_row_from_values(vals)
	testing.expect(t, !ok, "string kind rejected")
}
