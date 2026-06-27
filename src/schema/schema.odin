// Package schema manages database metadata (tables, columns) stored in the schema b-tree.
package schema

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:types"

init :: proc(t: ^btree.Tree) -> bool {
	_, err := btree.load_node(t, t.root)
	return err == .None
}

// Schema_Row is the canonical representation of a schema b-tree entry.
Schema_Row :: struct {
	kind:         string, // always "table"
	name:         string,
	root_page:    u32,
	sql:          string,
	columns_blob: []u8,
	skip_root:    u32,
}

schema_row_to_values :: proc(r: Schema_Row, allocator := context.temp_allocator) -> []types.Value {
	n := 5 // kind + name + root + sql + blob
	if r.skip_root > 0 { n += 1 }
	result := make([]types.Value, n, allocator)
	result[0] = types.value_int(0) // 0 = table
	result[1] = types.value_text(r.name)
	result[2] = types.value_int(i64(r.root_page))
	result[3] = types.value_text(r.sql)
	result[4] = types.value_blob(r.columns_blob)
	if r.skip_root > 0 {
		result[5] = types.value_int(i64(r.skip_root))
	}
	return result
}

schema_row_from_values :: proc(values: []types.Value) -> (Schema_Row, bool) {
	if len(values) < 5 { return {}, false }
	name, ok1 := values[1].(string)
	if !ok1 { return {}, false }
	_, kind_ok := values[0].(i64)
	if !kind_ok { return {}, false }

	sr := Schema_Row {
		kind = "table",
		name = name,
	}
	root, ok2 := values[2].(i64)
	sql, ok3 := values[3].(string)
	blob, ok4 := values[4].([]u8)
	if !ok2 || !ok3 || !ok4 { return {}, false }

	sr.root_page = u32(root)
	sr.sql = sql
	sr.columns_blob = blob
	if len(values) >= 6 {
		if skip, ok5 := values[5].(i64); ok5 { sr.skip_root = u32(skip) }
	}
	return sr, true
}

add_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
	sql_stmt: string,
) -> bool {
	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	r := Schema_Row {
		kind         = "table",
		name         = table_name,
		root_page    = root_page,
		sql          = sql_stmt,
		columns_blob = col_blob,
	}

	values := schema_row_to_values(r)
	rowid := types.Row_ID(types.hash_string(table_name))
	err := btree.tree_insert(t, rowid, values)
	if err != .None {
		fmt.eprintln("[Schema] add_table failed:", err)
		return false
	}
	return true
}

add_table_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
	sql_stmt: string,
) -> (
	u32,
	bool,
) {
	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	r := Schema_Row {
		kind         = "table",
		name         = table_name,
		root_page    = root_page,
		sql          = sql_stmt,
		columns_blob = col_blob,
	}

	values := schema_row_to_values(r)
	rowid := types.Row_ID(types.hash_string(table_name))
	new_root, err := btree.tree_insert_cow(t, rowid, values)
	if err != .None {
		fmt.eprintln("[Schema] add_table_cow failed:", err)
		return t.root, false
	}
	return new_root, true
}

find_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None { return {}, false }

	table, ok := table_from_values(c.values, allocator)
	if !ok { return {}, false }
	if table.name != table_name {
		table_free(table, allocator)
		return {}, false
	}
	return table, true
}

get_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) { return find_table(t, table_name, allocator) }

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
	if err != .None {
		fmt.eprintln("[Schema] drop_table_cow failed:", err)
		return t.root, false
	}
	return new_root, true
}

table_exists :: proc(t: ^btree.Tree, table_name: string) -> bool {
	_, err := btree.tree_find(
		t,
		types.Row_ID(types.hash_string(table_name)),
		context.temp_allocator,
	)
	return err == .None
}

table_from_values :: proc(
	values: []types.Value,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	sr, ok := schema_row_from_values(values)
	if !ok { return {}, false }

	table: types.Table
	table.name = strings.clone(sr.name, allocator)
	table.root_page = sr.root_page
	table.sql = strings.clone(sr.sql, allocator)
	cols := deserialize_columns(sr.columns_blob, allocator)
	if cols == nil {
		delete(table.name, allocator)
		delete(table.sql, allocator)
		return {}, false
	}

	table.columns = cols
	table.skip_root = sr.skip_root
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

update_root_page_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_root_page: u32,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		fmt.eprintf(
			"[schema] update_root_page_cow: tree_find failed for '%s' rowid=%v root=%d\n",
			table_name,
			rowid,
			t.root,
		)
		return t.root, false
	}

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		fmt.eprintf(
			"[schema] update_root_page_cow: schema_row_from_values failed for '%s'\n",
			table_name,
		)
		return t.root, false
	}

	sr.root_page = new_root_page
	values := schema_row_to_values(sr)
	new_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		fmt.eprintf(
			"[schema] update_root_page_cow: tree_update_cow failed for '%s': %v\n",
			table_name,
			upd_err,
		)
		return t.root, false
	}
	return new_root, true
}

update_skip_root_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_skip_root: u32,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		fmt.eprintf("[schema] update_skip_root_cow: tree_find failed for '%s'\n", table_name)
		return t.root, false
	}

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		fmt.eprintf(
			"[schema] update_skip_root_cow: schema_row_from_values failed for '%s'\n",
			table_name,
		)
		return t.root, false
	}

	sr.skip_root = new_skip_root
	values := schema_row_to_values(sr)
	new_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		fmt.eprintf(
			"[schema] update_skip_root_cow: tree_update_cow failed for '%s': %v\n",
			table_name,
			upd_err,
		)
		return t.root, false
	}
	return new_root, true
}

validate_columns :: proc(columns: []types.Column) -> (bool, string) {
	if len(columns) == 0 { return false, "Table must have at least one column" }
	if len(columns) > types.MAX_COLS {
		return false, fmt.tprintf("Too many columns (max %d)", types.MAX_COLS)
	}

	pk_count := 0
	for col, i in columns {
		if len(col.name) == 0 { return false, "Column name cannot be empty" }
		if col.pk { pk_count += 1 }
		for j in i + 1 ..< len(columns) {
			if columns[i].name == columns[j].name {
				return false, fmt.tprintf("Duplicate column name: %s", columns[i].name)
			}
		}
	}
	if pk_count > 1 {
		return false, "Multiple primary keys not supported right now"
	}
	return true, ""
}

find_column_index :: proc(columns: []types.Column, name: string) -> (int, bool) {
	for col, i in columns {
		if col.name == name {
			return i, true
		}
	}
	return -1, false
}

get_pk_column :: proc(columns: []types.Column) -> (int, bool) {
	for col, i in columns {
		if col.pk {
			return i, true
		}
	}
	return -1, false
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
		if len(flags) > 0 {
			fmt.printf("  %d. %-10s %-5s [%s]\n", i + 1, col.name, type_str, flags_str)
		} else {
			fmt.printf("  %d. %-10s %-5s\n", i + 1, col.name, type_str)
		}
	}
}

debug_print_all :: proc(t: ^btree.Tree) {
	fmt.println("=== Database Schema ===")
	tables := list_tables(t, context.temp_allocator)
	if len(tables) == 0 {
		fmt.println("No tables found.")
		return
	}
	for table, i in tables {
		if i > 0 do fmt.println("-----------------------")
		debug_print_entry(table)
	}
	fmt.println("=======================")
}

print_ddl :: proc(t: ^btree.Tree) {
	tables := list_tables(t, context.temp_allocator)
	for table in tables {
		fmt.println(table.sql)
		fmt.print(";")
		fmt.println()
	}
}
