package schema

import "core:fmt"
import "core:hash"
import "core:strings"
import "src:btree"
import "src:pager"
import "src:types"
import "src:utils"

SCHEMA_PAGE_ID :: 1

init :: proc(t: ^btree.Tree) -> bool {
	_, err := btree.load_node(t, SCHEMA_PAGE_ID)
	if err == .None {
		return true
	}

	page, e := pager.get_or_allocate_page(t.pager, SCHEMA_PAGE_ID)
	if e != .None { return false }

	btree.init_leaf_page(page.data, SCHEMA_PAGE_ID)
	pager.mark_dirty(t.pager, SCHEMA_PAGE_ID)
	_, reload_err := btree.load_node(t, SCHEMA_PAGE_ID)
	return reload_err == .None
}

add_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
	sql_stmt: string,
) -> bool {
	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	values := []types.Value {
		types.value_text("table"), // type
		types.value_text(table_name), // name
		types.value_text(table_name), // tbl_name
		types.value_int(i64(root_page)), // rootpage
		types.value_text(sql_stmt), // sql
		types.value_blob(col_blob), // custom binary metadata
	}

	rowid := types.Row_ID(hash_string(table_name))
	err := btree.tree_insert(t, rowid, values)
	return err == .None
}

find_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	rowid := types.Row_ID(hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		return {}, false
	}

	table, ok := table_from_values(c.values, allocator)
	if !ok {
		return {}, false
	}
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
) {
	temp_table, found := find_table(t, table_name, context.temp_allocator)
	if !found {
		return {}, false
	}

	table := types.Table {
		name      = strings.clone(temp_table.name, allocator),
		root_page = temp_table.root_page,
		sql       = strings.clone(temp_table.sql, allocator),
		columns   = make([]types.Column, len(temp_table.columns), allocator),
	}

	for col, i in temp_table.columns {
		table.columns[i] = types.Column {
			name     = strings.clone(col.name, allocator),
			type     = col.type,
			not_null = col.not_null,
			pk       = col.pk,
		}
	}
	return table, true
}

list_tables :: proc(t: ^btree.Tree, allocator := context.allocator) -> []types.Table {
	tables := make([dynamic]types.Table, allocator)
	cursor, err := btree.cursor_start(t, context.temp_allocator)
	if err != .None { return nil }
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err == .None {
			if tbl, ok := table_from_values(c.values, allocator); ok {
				append(&tables, tbl)
			}
		}
		btree.cursor_advance(&cursor)
	}
	return tables[:]
}

drop_table :: proc(t: ^btree.Tree, table_name: string) -> bool {
	rowid := types.Row_ID(hash_string(table_name))
	return btree.tree_delete(t, rowid) == .None
}

table_exists :: proc(t: ^btree.Tree, table_name: string) -> bool {
	rowid := types.Row_ID(hash_string(table_name))
	_, err := btree.tree_find(t, rowid, context.temp_allocator)
	return err == .None
}

// Deserialize values back into a Table struct
table_from_values :: proc(values: []types.Value, allocator := context.allocator) -> (types.Table, bool) {
	if len(values) < 6 { return {}, false }

	name_str, ok1 := values[1].(string)
	root_page, ok2 := values[3].(i64)
	sql_stmt, ok3 := values[4].(string)
	blob, ok4 := values[5].([]u8)
	if !ok1 || !ok2 || !ok3 || !ok4 { return {}, false }

	table: types.Table
	table.name = strings.clone(name_str, allocator)
	table.root_page = u32(root_page)
	table.sql = strings.clone(sql_stmt, allocator)
	cols := deserialize_columns(blob, allocator)
	if cols == nil {
		delete(table.name, allocator)
		delete(table.sql, allocator)
		return {}, false
	}
	table.columns = cols
	return table, true
}

// Format: [Count(4b)] -> [NameLen(4b) + NameBytes + Type(1b) + Flags(1b)]...
serialize_columns_to_blob :: proc(columns: []types.Column, allocator := context.allocator) -> []u8 {
	size := 4
	for col in columns {
		size += 4 + len(col.name) + 1 + 1
	}

	blob := make([]u8, size, allocator)
	offset := 0
	utils.write_u32_le(blob, offset, u32(len(columns)))
	offset += 4
	for col in columns {
		utils.write_u32_le(blob, offset, u32(len(col.name)))
		offset += 4
		copy(blob[offset:], col.name)

		offset += len(col.name)
		blob[offset] = u8(col.type)
		offset += 1
		flags: u8 = 0
		if col.not_null do flags |= 1
		if col.pk do flags |= 2
		blob[offset] = flags
		offset += 1
	}
	return blob
}

deserialize_columns :: proc(blob: []u8, allocator := context.allocator) -> []types.Column {
	if len(blob) < 4 { return nil }

	offset := 0
	count, ok := utils.read_u32_le(blob, offset)
	if !ok { return nil }

	offset += 4
	cols := make([dynamic]types.Column, 0, count, allocator)
	for _ in 0 ..< count {
		name_len, ok_len := utils.read_u32_le(blob, offset)
		if !ok_len { return nil }

		offset += 4
		if offset + int(name_len) + 2 > len(blob) { return nil }

		name_str := string(blob[offset:offset + int(name_len)])
		offset += int(name_len)
		type_byte := blob[offset]
		offset += 1
		flags_byte := blob[offset]
		offset += 1

		append(
			&cols,
			types.Column {
				name = strings.clone(name_str, allocator),
				type = types.Column_Type(type_byte),
				not_null = (flags_byte & 1) != 0,
				pk = (flags_byte & 2) != 0,
			},
		)
	}
	return cols[:]
}

table_free :: proc(table: types.Table, allocator := context.allocator) {
	delete(table.name, allocator)
	delete(table.sql, allocator)
	for col in table.columns {
		delete(col.name, allocator)
	}
	delete(table.columns, allocator)
}

hash_string :: proc(s: string) -> u64 {
	return hash.fnv64(transmute([]u8)s) & 0x7FFFFFFFFFFFFFFF
}

// Simple text dump of schema
debug_print_schema :: proc(t: ^btree.Tree) {
	fmt.println("=== Schema Dump ===")
	tables := list_tables(t, context.temp_allocator)
	for table in tables {
		fmt.printf("TABLE %s (Root: %d)\n", table.name, table.root_page)
		for col in table.columns {
			fmt.printf("  - %s %v\n", col.name, col.type)
		}
	}
	fmt.println("===================")
}

validate_columns :: proc(columns: []types.Column) -> (bool, string) {
	if len(columns) == 0 {
		return false, "Table must have at least one column"
	}
	if len(columns) > types.MAX_COLS {
		return false, fmt.tprintf("Too many columns (max %d)", types.MAX_COLS)
	}

	pk_count := 0
	for col, i in columns {
		if len(col.name) == 0 {
			return false, "Column name cannot be empty"
		}
		if col.pk {
			pk_count += 1
		}
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
