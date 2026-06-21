package schema

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:types"

init :: proc(t: ^btree.Tree) -> bool { _, err := btree.load_node(t, t.root); return err == .None }

add_table :: proc(t: ^btree.Tree, table_name: string, columns: []types.Column, root_page: u32, sql_stmt: string) -> bool {
	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	values := []types.Value{types.value_text("table"), types.value_text(table_name), types.value_text(table_name), types.value_int(i64(root_page)), types.value_text(sql_stmt), types.value_blob(col_blob)}
	rowid := types.Row_ID(types.hash_string(table_name))
	err := btree.tree_insert(t, rowid, values)
	if err != .None { fmt.eprintln("[Schema] add_table failed:", err); return false }
	return true
}

add_table_cow :: proc(t: ^btree.Tree, table_name: string, columns: []types.Column, root_page: u32, sql_stmt: string) -> (u32, bool) {
	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	values := []types.Value{types.value_text("table"), types.value_text(table_name), types.value_text(table_name), types.value_int(i64(root_page)), types.value_text(sql_stmt), types.value_blob(col_blob)}
	rowid := types.Row_ID(types.hash_string(table_name))
	new_root, err := btree.tree_insert_cow(t, rowid, values)
	if err != .None { fmt.eprintln("[Schema] add_table_cow failed:", err); return t.root, false }
	return new_root, true
}

find_table :: proc(t: ^btree.Tree, table_name: string, allocator := context.allocator) -> (types.Table, bool) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None { return {}, false }
	table, ok := table_from_values(c.values, allocator)
	if !ok { return {}, false }
	if table.name != table_name { table_free(table, allocator); return {}, false }
	return table, true
}

get_table :: proc(t: ^btree.Tree, table_name: string, allocator := context.allocator) -> (types.Table, bool) { return find_table(t, table_name, allocator) }

list_tables :: proc(t: ^btree.Tree, allocator := context.allocator) -> []types.Table {
	tables := make([dynamic]types.Table, allocator)
	cursor, err := btree.cursor_start(t, context.temp_allocator)
	if err != .None { return nil }
	defer btree.cursor_destroy(&cursor)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err == .None {
			if tbl, ok := table_from_values(c.values, allocator); ok { append(&tables, tbl) }
		}
		btree.cursor_advance(&cursor)
	}
	return tables[:]
}

drop_table :: proc(t: ^btree.Tree, table_name: string) -> bool {
	return btree.tree_delete(t, types.Row_ID(types.hash_string(table_name))) == .None
}

drop_table_cow :: proc(t: ^btree.Tree, table_name: string) -> (u32, bool) {
	new_root, err := btree.tree_delete_cow(t, types.Row_ID(types.hash_string(table_name)))
	if err != .None { fmt.eprintln("[Schema] drop_table_cow failed:", err); return t.root, false }
	return new_root, true
}

table_exists :: proc(t: ^btree.Tree, table_name: string) -> bool {
	_, err := btree.tree_find(t, types.Row_ID(types.hash_string(table_name)), context.temp_allocator)
	return err == .None
}

table_from_values :: proc(values: []types.Value, allocator := context.allocator) -> (types.Table, bool) {
	if len(values) < 6 { return {}, false }
	name_str, ok1 := values[1].(string); root_page, ok2 := values[3].(i64); sql_stmt, ok3 := values[4].(string); blob, ok4 := values[5].([]u8)
	if !ok1 || !ok2 || !ok3 || !ok4 { return {}, false }
	table: types.Table
	table.name = strings.clone(name_str, allocator); table.root_page = u32(root_page); table.sql = strings.clone(sql_stmt, allocator)
	cols := deserialize_columns(blob, allocator)
	if cols == nil { delete(table.name, allocator); delete(table.sql, allocator); return {}, false }
	table.columns = cols
	return table, true
}

table_free :: proc(table: types.Table, allocator := context.allocator) {
	delete(table.name, allocator); delete(table.sql, allocator)
	for col in table.columns {
		delete(col.name, allocator)
		if def, ok := col.default_value.?; ok { types.value_delete(def, allocator) }
		if chk, has := col.check_expr.?; has { delete(chk, allocator) }
	}
	delete(table.columns, allocator)
}

update_root_page_cow :: proc(t: ^btree.Tree, table_name: string, new_root_page: u32) -> (new_schema_root: u32, ok: bool) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None { fmt.eprintf("[schema] update_root_page_cow: tree_find failed for '%s' rowid=%v root=%d\n", table_name, rowid, t.root); return t.root, false }
	old_sql, sql_ok := c.values[4].(string); old_blob, blob_ok := c.values[5].([]u8)
	if !sql_ok || !blob_ok { fmt.eprintf("[schema] update_root_page_cow: type assertion failed sql=%v blob=%v for '%s'\n", sql_ok, blob_ok, table_name); return t.root, false }
	values := []types.Value{types.value_text("table"), types.value_text(table_name), types.value_text(table_name), types.value_int(i64(new_root_page)), types.value_text(old_sql), types.value_blob(old_blob)}
	new_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None { fmt.eprintf("[schema] update_root_page_cow: tree_update_cow failed for '%s': %v\n", table_name, upd_err); return t.root, false }
	return new_root, true
}

validate_columns :: proc(columns: []types.Column) -> (bool, string) {
	if len(columns) == 0 { return false, "Table must have at least one column" }
	if len(columns) > types.MAX_COLS { return false, fmt.tprintf("Too many columns (max %d)", types.MAX_COLS) }
	pk_count := 0
	for col, i in columns {
		if len(col.name) == 0 { return false, "Column name cannot be empty" }
		if col.pk { pk_count += 1 }
		for j in i + 1 ..< len(columns) { if columns[i].name == columns[j].name { return false, fmt.tprintf("Duplicate column name: %s", columns[i].name) } }
	}
	if pk_count > 1 { return false, "Multiple primary keys not supported right now" }
	return true, ""
}

find_column_index :: proc(columns: []types.Column, name: string) -> (int, bool) {
	for col, i in columns { if col.name == name { return i, true } }; return -1, false
}

get_pk_column :: proc(columns: []types.Column) -> (int, bool) {
	for col, i in columns { if col.pk { return i, true } }; return -1, false
}

debug_print_entry :: proc(table: types.Table) {
	fmt.printf("Table: %s (Root: %d)\n", table.name, table.root_page)
	fmt.printf("SQL:   %s\n", table.sql)
	fmt.println("Columns:")
	for col, i in table.columns {
		flags := make([dynamic]string, context.temp_allocator)
		if col.pk do append(&flags, "PK")
		if col.not_null do append(&flags, "NN")
		flags_str := strings.join(flags[:], ", ", context.temp_allocator)
		type_str: string
		switch col.type {
		case .INTEGER:
			type_str = "INT"
		case .TEXT:
			type_str = "TXT"
		case .REAL:
			type_str = "REAL"
		case .BLOB:
			type_str = "BLOB"
		}
		if len(flags) > 0 { fmt.printf("  %d. %-10s %-5s [%s]\n", i + 1, col.name, type_str, flags_str) }
		else { fmt.printf("  %d. %-10s %-5s\n", i + 1, col.name, type_str) }
	}
}

debug_print_all :: proc(t: ^btree.Tree) {
	fmt.println("=== Database Schema ===")
	tables := list_tables(t, context.temp_allocator)
	if len(tables) == 0 { fmt.println("No tables found."); return }
	for table, i in tables { if i > 0 do fmt.println("-----------------------"); debug_print_entry(table) }
	fmt.println("=======================")
}

print_ddl :: proc(t: ^btree.Tree) {
	tables := list_tables(t, context.temp_allocator)
	for table in tables { fmt.println(table.sql); fmt.print(";"); fmt.println() }
}
