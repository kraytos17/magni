package schema

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:pager"
import "src:types"
import "src:utils"

SCHEMA_PAGE :: 0

// Initialize schema page
init :: proc(p: ^pager.Pager) -> bool {
	page, err := pager.get_page(p, SCHEMA_PAGE)
	if err != nil {
		return false
	}
	if btree.init_leaf_page(page.data, SCHEMA_PAGE) != .None {
		return false
	}
	pager.mark_dirty(p, SCHEMA_PAGE)
	return true
}

// Deserialize values back into a Table struct
table_from_values :: proc(values: []types.Value, allocator := context.allocator) -> (types.Table, bool) {
	if len(values) != 6 {
		return types.Table{}, false
	}

	name_str, ok1 := values[1].(string)
	root_page_i64, ok2 := values[3].(i64)
	sql_stmt, ok3 := values[4].(string)
	blob, ok4 := values[5].([]u8)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return types.Table{}, false
	}

	table: types.Table
	table.name = strings.clone(name_str, allocator)
	table.root_page = u32(root_page_i64)
	table.sql = strings.clone(sql_stmt, allocator)
	cols := deserialize_columns_from_blob(blob, allocator)
	if cols == nil {
		delete(table.name, allocator)
		delete(table.sql, allocator)
		return types.Table{}, false
	}

	table.columns = cols
	return table, true
}

// Format: [Count(4b)] -> [NameLen(4b) + NameBytes + Type(1b) + Flags(1b)]...
serialize_columns_to_blob :: proc(columns: []types.Column, allocator := context.allocator) -> []u8 {
	total_size := 4
	for col in columns {
		total_size += 4 + len(col.name) + 1 + 1
	}

	blob := make([]u8, total_size, allocator)
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

deserialize_columns_from_blob :: proc(blob: []u8, allocator := context.allocator) -> []types.Column {
	if len(blob) < 4 do return nil

	offset := 0
	count, ok_count := utils.read_u32_le(blob, offset)
	if !ok_count do return nil

	offset += 4
	columns := make([dynamic]types.Column, 0, count, allocator)
	success := false
	defer if !success do delete(columns)

	for _ in 0 ..< count {
		name_len, ok_len := utils.read_u32_le(blob, offset)
		if !ok_len do return nil

		offset += 4
		if offset + int(name_len) > len(blob) {
			return nil
		}

		name_str := string(blob[offset:offset + int(name_len)])
		offset += int(name_len)
		if offset + 2 > len(blob) {
			return nil
		}

		type_byte := blob[offset]
		offset += 1
		flags_byte := blob[offset]
		offset += 1
		append(
			&columns,
			types.Column {
				name = strings.clone(name_str, allocator),
				type = types.Column_Type(type_byte),
				not_null = (flags_byte & 1) != 0,
				pk = (flags_byte & 2) != 0,
			},
		)
	}
	success = true
	return columns[:]
}

// Add a table to the schema
add_table :: proc(
	p: ^pager.Pager,
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
		types.value_blob(col_blob), // hidden blob
	}

	rowid := types.Row_ID(hash_string(table_name))
	if btree.insert_cell(p, SCHEMA_PAGE, rowid, values) != .None {
		return false
	}

	pager.flush_page(p, SCHEMA_PAGE)
	return true
}

// Find a table in the schema by name
find_table :: proc(
	p: ^pager.Pager,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	rowid := types.Row_ID(hash_string(table_name))
	config := btree.Config {
		allocator = context.temp_allocator,
		zero_copy = false,
	}

	cell_ref, err := btree.find_by_rowid(p, SCHEMA_PAGE, rowid, config)
	if err != .None {
		return types.Table{}, false
	}

	defer btree.cell_ref_destroy(&cell_ref)
	table, ok := table_from_values(cell_ref.cell.values, allocator)
	if !ok {
		return types.Table{}, false
	}
	if table.name != table_name {
		table_free(table)
		return types.Table{}, false
	}
	return table, true
}

// List all tables in the schema
list_tables :: proc(p: ^pager.Pager, allocator := context.allocator) -> []types.Table {
	tables := make([dynamic]types.Table, allocator)
	cursor, _ := btree.cursor_start(p, SCHEMA_PAGE, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.cursor_get_cell(p, &cursor, config)
		if err != .None {
			if btree.cursor_advance(p, &cursor) != .None {
				break
			}
			continue
		}

		table, ok := table_from_values(cell_ref.cell.values, allocator)
		btree.cell_ref_destroy(&cell_ref)
		if ok {
			append(&tables, table)
		}
		if btree.cursor_advance(p, &cursor) != .None {
			break
		}
	}
	return tables[:]
}

// Drop a table from the schema
drop_table :: proc(p: ^pager.Pager, table_name: string) -> bool {
	rowid := types.Row_ID(hash_string(table_name))
	if btree.delete_cell(p, SCHEMA_PAGE, rowid) != .None {
		return false
	}
	pager.flush_page(p, SCHEMA_PAGE)
	return true
}

// Get table metadata (columns, root page)
// Returns a deep copy of the table in the provided allocator
get_table :: proc(
	p: ^pager.Pager,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	temp_table, found := find_table(p, table_name, context.temp_allocator)
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
table_exists :: proc(p: ^pager.Pager, table_name: string) -> bool {
	_, found := find_table(p, table_name, context.temp_allocator)
	return found
}

// Free table memory
table_free :: proc(table: types.Table) {
	delete(table.name)
	delete(table.sql)
	for col in table.columns {
		delete(col.name)
	}
	delete(table.columns)
}

// FNV-1a string hashing for table names
hash_string :: proc(s: string) -> u64 {
	OFFSET_BASIS :: 0xcbf29ce484222325
	PRIME :: 0x100000001b3

	h: u64 = OFFSET_BASIS
	for c in s {
		h = h ~ u64(c)
		h = h * PRIME
	}
	return h & 0x7FFFFFFFFFFFFFFF
}

// Validate column definitions
validate_columns :: proc(columns: []types.Column) -> (bool, string) {
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
find_column_index :: proc(columns: []types.Column, name: string) -> (int, bool) {
	for col, i in columns {
		if col.name == name {
			return i, true
		}
	}
	return -1, false
}

// Get primary key column index
get_pk_column :: proc(columns: []types.Column) -> (int, bool) {
	for col, i in columns {
		if col.pk {
			return i, true
		}
	}
	return -1, false
}

// Debug: Print schema entry
debug_print_entry :: proc(table: types.Table) {
	fmt.printf("Table: %s (root_page=%d)\n", table.name, table.root_page)
	fmt.printf("SQL:   %s\n", table.sql)
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
debug_print_all :: proc(p: ^pager.Pager) {
	fmt.println("=== Database Schema ===")
	tables := list_tables(p, context.temp_allocator)
	if len(tables) == 0 {
		fmt.println("No tables found.")
		return
	}

	for table, i in tables {
		if i > 0 do fmt.println()
		debug_print_entry(table)
	}
	fmt.println("======================")
}

// Print the CREATE TABLE statements for all tables
print_ddl :: proc(p: ^pager.Pager) {
	tables := list_tables(p, context.temp_allocator)
	for table in tables {
		fmt.println(table.sql)
	}
}
