package tests

import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:pager"
import "src:schema"
import "src:types"

setup_schema_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	filename := fmt.tprintf("test_schema_%s.db", test_name)
	safe_filename, _ := strings.clone(filename, context.allocator)
	os.remove(safe_filename)

	p, err := pager.open(safe_filename)
	testing.expect(t, err == nil, "Failed to open pager")

	pager.allocate_page(p)
	ok := schema.init(p)
	testing.expect(t, ok, "Failed to initialize schema page")

	return p, safe_filename
}

teardown_schema_env :: proc(p: ^pager.Pager, filename: string) {
	pager.close(p)
	os.remove(filename)
	delete(filename, context.allocator)
}

@(test)
test_column_serialization_roundtrip :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	original_cols := make([dynamic]types.Column, context.temp_allocator)
	append(&original_cols, types.Column{name = "id", type = .INTEGER, pk = true, not_null = true})
	append(&original_cols, types.Column{name = "username", type = .TEXT, pk = false, not_null = true})
	append(&original_cols, types.Column{name = "avatar", type = .BLOB, pk = false, not_null = false})

	blob := schema.serialize_columns_to_blob(original_cols[:], context.temp_allocator)
	testing.expect(t, len(blob) > 0, "Blob should not be empty")

	restored_cols := schema.deserialize_columns_from_blob(blob, context.temp_allocator)
	testing.expect(t, len(restored_cols) == 3, "Column count mismatch")

	testing.expect(t, restored_cols[0].name == "id", "Col 0 name mismatch")
	testing.expect(t, restored_cols[0].type == .INTEGER, "Col 0 type mismatch")
	testing.expect(t, restored_cols[0].pk == true, "Col 0 PK mismatch")

	testing.expect(t, restored_cols[1].name == "username", "Col 1 name mismatch")
	testing.expect(t, restored_cols[1].not_null == true, "Col 1 Not Null mismatch")
	testing.expect(t, restored_cols[2].type == .BLOB, "Col 2 type mismatch")
}

@(test)
test_schema_init_correctness :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file := setup_schema_env(t, "init")
	defer teardown_schema_env(p, file)

	page, err := pager.get_page(p, schema.SCHEMA_PAGE)
	testing.expect(t, err == nil, "Failed to get schema page")

	header := btree.get_header(page.data)
	testing.expect(t, header.page_type == .LEAF_TABLE, "Schema page should be a B-Tree Leaf")
	testing.expect(t, header.cell_count == 0, "New schema should be empty")
}

@(test)
test_add_and_find_table :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file := setup_schema_env(t, "add_find")
	defer teardown_schema_env(p, file)

	cols := []types.Column{{name = "col1", type = .INTEGER}}
	root_page := u32(2)
	sql_stmt := "CREATE TABLE t1 (col1 INTEGER);"
	ok := schema.add_table(p, "t1", cols, root_page, sql_stmt)
	testing.expect(t, ok, "Failed to add table")

	table, found := schema.find_table(p, "t1", context.temp_allocator)
	testing.expect(t, found, "Table not found")
	testing.expect(t, table.name == "t1", "Name mismatch")
	testing.expect(t, table.root_page == root_page, "Root page mismatch")
	testing.expect(t, table.sql == sql_stmt, "SQL mismatch")
	testing.expect(t, len(table.columns) == 1, "Column count mismatch")
}

@(test)
test_table_exists :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file := setup_schema_env(t, "exists")
	defer teardown_schema_env(p, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	schema.add_table(p, "exists_test", cols, 5, "")

	testing.expect(t, schema.table_exists(p, "exists_test"), "Should return true for existing table")
	testing.expect(t, !schema.table_exists(p, "ghost_table"), "Should return false for missing table")
}

@(test)
test_list_tables :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file := setup_schema_env(t, "list")
	defer teardown_schema_env(p, file)

	cols := []types.Column{{name = "a", type = .INTEGER}}
	schema.add_table(p, "alpha", cols, 2, "")
	schema.add_table(p, "beta", cols, 3, "")
	schema.add_table(p, "gamma", cols, 4, "")

	tables := schema.list_tables(p, context.temp_allocator)
	testing.expect(t, len(tables) == 3, "Should list 3 tables")

	found_alpha := false
	found_beta := false
	for tbl in tables {
		if tbl.name == "alpha" do found_alpha = true
		if tbl.name == "beta" do found_beta = true
	}
	testing.expect(t, found_alpha && found_beta, "List missing tables")
}

@(test)
test_drop_table :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)
	p, file := setup_schema_env(t, "drop")
	defer teardown_schema_env(p, file)

	cols := []types.Column{{name = "x", type = .INTEGER}}
	schema.add_table(p, "temp_table", cols, 2, "")
	testing.expect(t, schema.table_exists(p, "temp_table"), "Table should exist before drop")

	ok := schema.drop_table(p, "temp_table")
	testing.expect(t, ok, "Drop table failed")
	testing.expect(t, !schema.table_exists(p, "temp_table"), "Table should not exist after drop")
}

@(test)
test_validate_columns_valid :: proc(t: ^testing.T) {
	cols := []types.Column{{name = "id", type = .INTEGER, pk = true}, {name = "name", type = .TEXT}}
	ok, msg := schema.validate_columns(cols)
	testing.expect(t, ok, fmt.tprintf("Valid columns rejected: %s", msg))
}

@(test)
test_validate_columns_empty :: proc(t: ^testing.T) {
	cols := []types.Column{}
	ok, _ := schema.validate_columns(cols)
	testing.expect(t, !ok, "Should fail on 0 columns")
}

@(test)
test_validate_columns_duplicate :: proc(t: ^testing.T) {
	cols := []types.Column {
		{name = "age", type = .INTEGER},
		{name = "age", type = .TEXT},
	}
	
	ok, msg := schema.validate_columns(cols)
	testing.expect(t, !ok, "Should fail on duplicate names")
	testing.expect(t, strings.contains(msg, "Duplicate"), "Error message mismatch")
}

@(test)
test_validate_columns_multi_pk :: proc(t: ^testing.T) {
	cols := []types.Column {
		{name = "id1", type = .INTEGER, pk = true},
		{name = "id2", type = .INTEGER, pk = true},
	}
	
	ok, msg := schema.validate_columns(cols)
	testing.expect(t, !ok, "Should fail on multiple PKs")
	testing.expect(t, strings.contains(msg, "Multiple primary keys"), "Error message mismatch")
}

@(test)
test_get_table_memory_safety :: proc(t: ^testing.T) {
	p, file := setup_schema_env(t, "memory")
	defer teardown_schema_env(p, file)

	cols := []types.Column{{name = "persist", type = .TEXT}}
	schema.add_table(p, "mem_test", cols, 2, "")
	table, found := schema.get_table(p, "mem_test", context.allocator)
	testing.expect(t, found, "Table not found")

	free_all(context.temp_allocator)
	testing.expect(t, table.name == "mem_test", "Name corrupted after temp free")
	testing.expect(t, table.columns[0].name == "persist", "Column name corrupted")

	schema.table_free(table)
}
