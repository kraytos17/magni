package tests

import "core:fmt"
import os "core:os/os2"
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
	tree := btree.init(p, schema.SCHEMA_PAGE_ID)
	ok := schema.init(&tree)
	testing.expect(t, ok, "Failed to init schema B-Tree on Page 1")
	return tree, safe_filename
}

teardown_schema_env :: proc(tree: btree.Tree, filename: string) {
	pager.close(tree.pager)
	os.remove(filename)
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
	cols := []types.Column{{name = "x", type = .INTEGER}}

	ok := schema.add_table(&tree, "persistent", cols, 99, "")
	testing.expect(t, ok, "add_table failed in persistence test")
	pager.close(tree.pager)

	p2, _ := pager.open(file)
	tree2 := btree.init(p2, schema.SCHEMA_PAGE_ID)
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
