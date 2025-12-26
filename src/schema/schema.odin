package schema

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:pager"
import "src:types"

SCHEMA_PAGE :: 1

// Initialize schema page
schema_init :: proc(p: ^pager.Pager) -> bool {
	page, err := pager.pager_allocate_page(p)
	if err != nil {
		return false
	}
	if page.page_num != SCHEMA_PAGE {
		pager.page_destroy(page)
		fmt.println("Critical Error: Schema page allocated at wrong index:", page.page_num)
		return false
	}

	init_err := btree.btree_init_leaf_page(page.data)
	if init_err != .None {
		return false
	}

	flush_err := pager.pager_flush_page(p, page.page_num)
	if flush_err != nil {
		fmt.println("Critical Error: Failed to flush schema page to disk")
		return false
	}
	return true
}

// Serialize a table definition into values for storage
// Format: [name (TEXT), root_page (INTEGER), column_count (INTEGER),
//          col1_name (TEXT), col1_type (INTEGER), col1_flags (INTEGER), ...]
schema_table_to_values :: proc(table: types.Table, allocator := context.allocator) -> []types.Value {
	values := make([dynamic]types.Value, allocator)
	append(&values, types.value_text(strings.clone(table.name, allocator)))
	append(&values, types.value_int(i64(table.root_page)))
	append(&values, types.value_int(i64(len(table.columns))))

	for col in table.columns {
		append(&values, types.value_text(strings.clone(col.name, allocator)))
		append(&values, types.value_int(i64(col.type)))

		flags: i64 = 0
		if col.not_null do flags |= 1
		if col.pk do flags |= 2
		append(&values, types.value_int(flags))
	}
	return values[:]
}

// Deserialize values back into a Table struct
schema_table_from_values :: proc(
	values: []types.Value,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	if len(values) < 3 {
		return types.Table{}, false
	}

	t_name_str, ok1 := values[0].(string)
	root_page, ok2 := values[1].(i64)
	col_cnt, ok3 := values[2].(i64)
	if !ok1 || !ok2 || !ok3 {
		return types.Table{}, false
	}

	col_count := int(col_cnt)
	expected_len := 3 + col_count * 3
	if len(values) != expected_len {
		return types.Table{}, false
	}

	table: types.Table
	table.name = strings.clone(t_name_str, allocator)
	table.root_page = u32(root_page)
	columns := make([dynamic]types.Column, 0, col_count, allocator)
	success := false
	defer if !success {
		delete(table.name, allocator)
		for col in columns {
			delete(col.name, allocator)
		}
		delete(columns)
	}

	idx := 3
	for _ in 0 ..< col_count {
		col_name, ok4 := values[idx].(string)
		col_type, ok5 := values[idx + 1].(i64)
		col_flags, ok6 := values[idx + 2].(i64)
		if !ok4 || !ok5 || !ok6 {
			return types.Table{}, false
		}

		col := types.Column {
			name     = strings.clone(col_name, allocator),
			type     = types.Column_Type(col_type),
			not_null = (col_flags & 1) != 0,
			pk       = (col_flags & 2) != 0,
		}

		append(&columns, col)
		idx += 3
	}

	table.columns = columns[:]
	success = true
	return table, true
}

// Add a table to the schema
schema_add_table :: proc(
	p: ^pager.Pager,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
) -> bool {
	table := types.Table {
		name      = table_name,
		root_page = root_page,
		columns   = columns,
	}

	values := schema_table_to_values(table, context.temp_allocator)
	rowid := types.Row_ID(hash_string(table_name))
	err := btree.btree_insert_cell(p, SCHEMA_PAGE, rowid, values)
	if err != .None {
		return false
	}

	pager.pager_flush_page(p, SCHEMA_PAGE)
	return true
}

// Find a table in the schema by name
schema_find_table :: proc(
	p: ^pager.Pager,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	rowid := types.Row_ID(hash_string(table_name))
	config := btree.BTree_Config {
		allocator = context.temp_allocator,
		zero_copy = false,
	}

	cell_ref, err := btree.btree_find_by_rowid(p, SCHEMA_PAGE, rowid, config)
	if err != .None {
		return types.Table{}, false
	}

	defer btree.btree_cell_ref_destroy(&cell_ref)
	table, ok := schema_table_from_values(cell_ref.cell.values, allocator)
	if !ok {
		return types.Table{}, false
	}
	if table.name != table_name {
		schema_table_free(table)
		return types.Table{}, false
	}
	return table, true
}

// List all tables in the schema
schema_list_tables :: proc(p: ^pager.Pager, allocator := context.allocator) -> []types.Table {
	tables := make([dynamic]types.Table, allocator)
	cursor := btree.btree_cursor_start(SCHEMA_PAGE, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.BTree_Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.btree_cursor_get_cell(p, &cursor, config)
		if err != .None {
			advance_err := btree.btree_cursor_advance(p, &cursor)
			if advance_err != .None {
				break
			}
			continue
		}

		table, ok := schema_table_from_values(cell_ref.cell.values, allocator)
		btree.btree_cell_ref_destroy(&cell_ref)
		if ok {
			append(&tables, table)
		}

		advance_err := btree.btree_cursor_advance(p, &cursor)
		if advance_err != .None {
			break
		}
	}
	return tables[:]
}

// Drop a table from the schema
schema_drop_table :: proc(p: ^pager.Pager, table_name: string) -> bool {
	rowid := types.Row_ID(hash_string(table_name))
	err := btree.btree_delete_cell(p, SCHEMA_PAGE, rowid)
	if err != .None {
		return false
	}
	pager.pager_flush_page(p, SCHEMA_PAGE)
	return true
}

// Get table metadata (columns, root page)
// Returns a deep copy of the table in the provided allocator
schema_get_table :: proc(
	p: ^pager.Pager,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	temp_table, found := schema_find_table(p, table_name, context.temp_allocator)
	if !found {
		return types.Table{}, false
	}

	table := types.Table {
		name      = strings.clone(temp_table.name, allocator),
		root_page = temp_table.root_page,
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

// Check if a table exists
schema_table_exists :: proc(p: ^pager.Pager, table_name: string) -> bool {
	table, found := schema_find_table(p, table_name, context.temp_allocator)
	return found
}

// Free table memory
schema_table_free :: proc(table: types.Table) {
	delete(table.name)
	for col in table.columns {
		delete(col.name)
	}
	delete(table.columns)
}

// Simple hash function for table names
hash_string :: proc(s: string) -> u64 {
	h: u64 = 5381
	for c in s {
		h = ((h << 5) + h) + u64(c)
	}
	return h & 0x7FFFFFFFFFFFFFFF
}

// Validate column definitions
schema_validate_columns :: proc(columns: []types.Column) -> (bool, string) {
	if len(columns) == 0 {
		return false, "Table must have at least one column"
	}
	if len(columns) > types.MAX_COLS {
		return false, fmt.tprintf("Too many columns (max %d)", types.MAX_COLS)
	}

	pk_count := 0
	for col in columns {
		if len(col.name) == 0 {
			return false, "Column name cannot be empty"
		}
		if col.pk {
			pk_count += 1
		}
	}
	for i in 0 ..< len(columns) {
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

// Find column index by name
schema_find_column_index :: proc(columns: []types.Column, name: string) -> (int, bool) {
	for col, i in columns {
		if col.name == name {
			return i, true
		}
	}
	return -1, false
}

// Get primary key column index
schema_get_pk_column :: proc(columns: []types.Column) -> (int, bool) {
	for col, i in columns {
		if col.pk {
			return i, true
		}
	}
	return -1, false
}

// Debug: Print schema entry
schema_debug_print_entry :: proc(table: types.Table) {
	fmt.printf("Table: %s (root_page=%d)\n", table.name, table.root_page)
	fmt.println("Columns:")
	for col, i in table.columns {
		flags := make([dynamic]string, context.temp_allocator)
		if col.pk do append(&flags, "PRIMARY KEY")
		if col.not_null do append(&flags, "NOT NULL")

		flags_str := strings.join(flags[:], ", ", context.temp_allocator)
		type_str: string
		switch col.type {
		case .INTEGER:
			type_str = "INTEGER"
		case .TEXT:
			type_str = "TEXT"
		case .REAL:
			type_str = "REAL"
		case .BLOB:
			type_str = "BLOB"
		}

		if len(flags) > 0 {
			fmt.printf("  %d. %s %s (%s)\n", i + 1, col.name, type_str, flags_str)
		} else {
			fmt.printf("  %d. %s %s\n", i + 1, col.name, type_str)
		}
	}
}

// Debug: Print all tables
schema_debug_print_all :: proc(p: ^pager.Pager) {
	fmt.println("=== Database Schema ===")
	tables := schema_list_tables(p, context.temp_allocator)
	if len(tables) == 0 {
		fmt.println("No tables found.")
		return
	}

	for table, i in tables {
		if i > 0 do fmt.println()
		schema_debug_print_entry(table)
	}
	fmt.println("======================")
}
