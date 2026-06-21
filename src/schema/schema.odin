package schema

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:types"
import "src:utils"

init :: proc(t: ^btree.Tree) -> bool {
	_, err := btree.load_node(t, t.root)
	return err == .None
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
	values := []types.Value {
		types.value_text("table"),
		types.value_text(table_name),
		types.value_text(table_name),
		types.value_int(i64(root_page)),
		types.value_text(sql_stmt),
		types.value_blob(col_blob),
	}

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
	return find_table(t, table_name, allocator)
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
	rowid := types.Row_ID(types.hash_string(table_name))
	return btree.tree_delete(t, rowid) == .None
}

drop_table_cow :: proc(t: ^btree.Tree, table_name: string) -> (u32, bool) {
	rowid := types.Row_ID(types.hash_string(table_name))
	new_root, err := btree.tree_delete_cow(t, rowid)
	if err != .None {
		fmt.eprintln("[Schema] drop_table_cow failed:", err)
		return t.root, false
	}
	return new_root, true
}

table_exists :: proc(t: ^btree.Tree, table_name: string) -> bool {
	rowid := types.Row_ID(types.hash_string(table_name))
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

// Write a Value in a simple binary format:
//	[type_byte(1)] + [payload]
//	type_byte: 0=null, 1=i64(8LE), 2=f64(8BE), 3=string(4LE+data), 4=blob(4LE+data)
serialize_value_to_blob :: proc(dest: []u8, offset: ^int, val: types.Value) {
	v := val
	#partial switch vv in v {
	case types.Null:
		dest[offset^] = 0; offset^ += 1
	case i64:
		dest[offset^] = 1; offset^ += 1
		utils.write_u64_le(dest, offset^, u64(vv)); offset^ += 8
	case f64:
		dest[offset^] = 2; offset^ += 1
		utils.write_f64_be(dest, offset^, vv); offset^ += 8
	case string:
		dest[offset^] = 3; offset^ += 1
		utils.write_u32_le(dest, offset^, u32(len(vv))); offset^ += 4
		copy(dest[offset^:], vv)
		offset^ += len(vv)
	case []u8:
		dest[offset^] = 4; offset^ += 1
		utils.write_u32_le(dest, offset^, u32(len(vv))); offset^ += 4
		copy(dest[offset^:], vv)
		offset^ += len(vv)
	}
}

deserialize_value_from_blob :: proc(
	src: []u8,
	offset: ^int,
	allocator := context.allocator,
) -> (
	types.Value,
	bool,
) {
	if offset^ >= len(src) { return {}, false }

	type_byte := src[offset^]; offset^ += 1
	switch type_byte {
	case 0:
		return types.value_null(), true
	case 1:
		if offset^ + 8 > len(src) { return {}, false }
		val, _ := utils.read_u64_le(src, offset^); offset^ += 8
		return types.value_int(i64(val)), true
	case 2:
		if offset^ + 8 > len(src) { return {}, false }
		val, _ := utils.read_f64_be(src, offset^); offset^ += 8
		return types.value_real(val), true
	case 3:
		if offset^ + 4 > len(src) { return {}, false }
		len_val, _ := utils.read_u32_le(src, offset^); offset^ += 4
		if offset^ + int(len_val) > len(src) { return {}, false }

		str_val := string(src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value_text(strings.clone(str_val, allocator)), true
	case 4:
		if offset^ + 4 > len(src) { return {}, false }
		len_val, _ := utils.read_u32_le(src, offset^); offset^ += 4
		if offset^ + int(len_val) > len(src) { return {}, false }

		blob := make([]u8, int(len_val), allocator)
		copy(blob, src[offset^:offset^ + int(len_val)])
		offset^ += int(len_val)
		return types.value_blob(blob), true
	case:
		return {}, false
	}
}

// Format: [Count(4b)] -> [NameLen(4b) + NameBytes + Type(1b) + Flags(1b) + DefaultMarker(1b) + DefaultValue... + CheckLen(4b) + CheckBytes?]
serialize_columns_to_blob :: proc(columns: []types.Column, allocator := context.allocator) -> []u8 {
	size := 4
	for col in columns {
		size += 4 + len(col.name) + 1 + 1 + 1
	}
	for col in columns {
		if def, ok := col.default_value.?; ok {
			#partial switch v in def {
			case types.Null:
				size += 1
			case i64:
				size += 1 + 8
			case f64:
				size += 1 + 8
			case string:
				size += 1 + 4 + len(v)
			case []u8:
				size += 1 + 4 + len(v)
			}
		}
		if chk, has_chk := col.check_expr.?; has_chk {
			size += 4 + len(chk)
		}
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
		if _, has_chk := col.check_expr.?; has_chk do flags |= 4

		blob[offset] = flags
		offset += 1
		if def, ok := col.default_value.?; ok {
			blob[offset] = 1; offset += 1
			serialize_value_to_blob(blob, &offset, def)
		} else {
			blob[offset] = 0; offset += 1
		}
		if chk, has_chk := col.check_expr.?; has_chk {
			utils.write_u32_le(blob, offset, u32(len(chk)))
			offset += 4
			copy(blob[offset:], chk)
			offset += len(chk)
		}
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
		if offset + int(name_len) + 3 > len(blob) { return nil }

		name_str := string(blob[offset:offset + int(name_len)])
		offset += int(name_len)
		type_byte := blob[offset]
		offset += 1
		flags_byte := blob[offset]
		offset += 1
		default_marker := blob[offset]
		offset += 1

		col := types.Column {
			name     = strings.clone(name_str, allocator),
			type     = types.Column_Type(type_byte),
			not_null = (flags_byte & 1) != 0,
			pk       = (flags_byte & 2) != 0,
		}
		if default_marker == 1 {
			def_val, def_ok := deserialize_value_from_blob(blob, &offset, allocator)
			if !def_ok { return nil }
			col.default_value = def_val
		}
		if (flags_byte & 4) != 0 {
			if offset + 4 > len(blob) { return nil }
			chk_len := (^u32le)(raw_data(blob[offset:]))^
			offset += 4
			if offset + int(chk_len) > len(blob) { return nil }
			col.check_expr = strings.clone(string(blob[offset:offset + int(chk_len)]), allocator)
			offset += int(chk_len)
		}
		append(&cols, col)
	}
	return cols[:]
}

table_free :: proc(table: types.Table, allocator := context.allocator) {
	delete(table.name, allocator)
	delete(table.sql, allocator)
	for col in table.columns {
		delete(col.name, allocator)
		if def, ok := col.default_value.?; ok {
			types.value_delete(def, allocator)
		}
		if chk, has := col.check_expr.?; has {
			delete(chk, allocator)
		}
	}
	delete(table.columns, allocator)
}

// Updates a table's root_page in the schema tree using COW.
// Returns the new schema tree root page.
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

	old_sql, sql_ok := c.values[4].(string)
	old_blob, blob_ok := c.values[5].([]u8)
	if !sql_ok || !blob_ok {
		fmt.eprintf(
			"[schema] update_root_page_cow: type assertion failed sql=%v blob=%v for '%s'\n",
			sql_ok,
			blob_ok,
			table_name,
		)
		return t.root, false
	}

	values := []types.Value {
		types.value_text("table"),
		types.value_text(table_name),
		types.value_text(table_name),
		types.value_int(i64(new_root_page)),
		types.value_text(old_sql),
		types.value_blob(old_blob),
	}

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
