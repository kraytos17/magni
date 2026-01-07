package executor

import "core:fmt"
import "core:slice"
import "core:strings"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

// Deep copy values to ensure they survive beyond the source cell's lifetime
//
// Parameters:
// values: The source slice of values (usually from a B-Tree cell).
//
// Returns:
// A new slice of Values with deep-copied strings and blobs.
deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		#partial switch val in v {
		case string:
			new_values[i] = types.value_text(strings.clone(val, context.temp_allocator))
		case []u8:
			blob, _ := slice.clone(val, context.temp_allocator)
			new_values[i] = types.value_blob(blob)
		case:
			new_values[i] = val
		}
	}
	return new_values
}

execute :: proc(schema_tree: ^btree.Tree, stmt: parser.Statement) -> bool {
	switch s in stmt.type {
	case parser.Create_Stmt:
		return exec_create(schema_tree, s, stmt.sql)
	case parser.Insert_Stmt:
		return exec_insert(schema_tree, s)
	case parser.Select_Stmt:
		return exec_select(schema_tree, s)
	case parser.Update_Stmt:
		return exec_update(schema_tree, s)
	case parser.Delete_Stmt:
		return exec_delete(schema_tree, s)
	case parser.Drop_Stmt:
		return exec_drop(schema_tree, s)
	}
	return false
}

exec_create :: proc(t: ^btree.Tree, stmt: parser.Create_Stmt, sql: string) -> bool {
	if ok, msg := schema.validate_columns(stmt.columns); !ok {
		fmt.eprintln("Schema Error:", msg)
		return false
	}
	if schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table already exists:", stmt.table_name)
		return false
	}

	root_page, err := pager.allocate_page(t.pager)
	for err == .None && root_page.page_num <= schema.SCHEMA_PAGE_ID {
		pager.mark_dirty(t.pager, root_page.page_num)
		root_page, err = pager.allocate_page(t.pager)
	}

	if err != .None {
		fmt.eprintln("Error: Failed to allocate table root page")
		return false
	}

	btree.init_leaf_page(root_page.data, root_page.page_num)
	pager.mark_dirty(t.pager, root_page.page_num)
	if !schema.add_table(t, stmt.table_name, stmt.columns, root_page.page_num, sql) {
		fmt.eprintln("Error: Failed to register table in schema")
		return false
	}
	fmt.printf("Created table '%s' at Page %d\n", stmt.table_name, root_page.page_num)
	return true
}

exec_insert :: proc(t: ^btree.Tree, stmt: parser.Insert_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	if len(stmt.values) != len(table.columns) {
		fmt.eprintfln(
			"Error: Column count mismatch. Expected %d, got %d",
			len(table.columns),
			len(stmt.values),
		)
		return false
	}
	if !cell.validate(stmt.values, table.columns) {
		fmt.eprintln("Error: Data type validation failed")
		return false
	}

	table_tree := btree.init(t.pager, table.root_page)
	next_rowid: types.Row_ID = 0
	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if val, is_int := stmt.values[pk_idx].(i64); is_int {
			next_rowid = types.Row_ID(val)
		} else {
			next_rowid, _ = btree.tree_next_rowid(&table_tree)
		}
	} else {
		id, err := btree.tree_next_rowid(&table_tree)
		if err != .None {
			next_rowid = 1
		} else {
			next_rowid = id
		}
	}

	err := btree.tree_insert(&table_tree, next_rowid, stmt.values)
	if err != .None {
		fmt.eprintln("Error inserting row:", err)
		return false
	}
	fmt.println("Inserted row", next_rowid)
	return true
}

exec_select :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	display_indices := make([dynamic]int, context.temp_allocator)
	if len(stmt.columns) == 0 {
		for i in 0 ..< len(table.columns) {
			append(&display_indices, i)
		}
	} else {
		for req_col in stmt.columns {
			idx, ok := schema.find_column_index(table.columns, req_col)
			if !ok {
				fmt.eprintln("Error: Unknown column:", req_col)
				return false
			}
			append(&display_indices, idx)
		}
	}

	print_header(table.columns, display_indices[:])
	table_tree := btree.init(t.pager, table.root_page)
	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		return true
	}
	defer btree.cursor_destroy(&cursor)

	row_count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if where_clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where(where_clause, c.values, table.columns) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		print_row(c.values, display_indices[:])
		row_count += 1
		btree.cursor_advance(&cursor)
	}
	fmt.printf("(%d rows)\n", row_count)
	return true
}

exec_update :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	update_map := make(map[int]types.Value, context.temp_allocator)
	if len(stmt.update_columns) != len(stmt.update_values) {
		fmt.eprintln("Error: Column/Value count mismatch in UPDATE")
		return false
	}

	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, ok := schema.find_column_index(table.columns, col_name)
		if !ok {
			fmt.eprintln("Error: Unknown column:", col_name)
			return false
		}
		update_map[idx] = stmt.update_values[i]
	}

	Update_Op :: struct {
		rowid:      types.Row_ID,
		new_values: []types.Value,
	}

	ops := make([dynamic]Update_Op, context.temp_allocator)
	table_tree := btree.init(t.pager, table.root_page)
	cursor, _ := btree.cursor_start(&table_tree, context.temp_allocator)
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, _ := btree.cursor_get_cell(&cursor, context.temp_allocator)
		should_update := true
		if where_clause, has_where := stmt.where_clause.?; has_where {
			should_update = evaluate_where(where_clause, c.values, table.columns)
		}
		if should_update {
			new_row := deep_copy_values(c.values)
			for idx, val in update_map {
				new_row[idx] = val
			}
			append(&ops, Update_Op{c.rowid, new_row})
		}
		btree.cursor_advance(&cursor)
	}

	count := 0
	for op in ops {
		if btree.tree_delete(&table_tree, op.rowid) == .None {
			btree.tree_insert(&table_tree, op.rowid, op.new_values)
			count += 1
		}
	}
	fmt.printf("Updated %d rows.\n", count)
	return true
}

exec_delete :: proc(t: ^btree.Tree, stmt: parser.Delete_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	targets := make([dynamic]types.Row_ID, context.temp_allocator)
	table_tree := btree.init(t.pager, table.root_page)
	cursor, _ := btree.cursor_start(&table_tree, context.temp_allocator)
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, _ := btree.cursor_get_cell(&cursor, context.temp_allocator)
		should_delete := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_delete = evaluate_where(where_cl, c.values, table.columns)
		}
		if should_delete {
			append(&targets, c.rowid)
		}
		btree.cursor_advance(&cursor)
	}

	count := 0
	for rowid in targets {
		if btree.tree_delete(&table_tree, rowid) == .None {
			count += 1
		}
	}
	fmt.printf("Deleted %d rows.\n", count)
	return true
}

exec_drop :: proc(t: ^btree.Tree, stmt: parser.Drop_Stmt) -> bool {
	if !schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if schema.drop_table(t, stmt.table_name) {
		fmt.println("Dropped table:", stmt.table_name)
		return true
	}
	return false
}

// Evaluate WHERE clause against a row
evaluate_where :: proc(clause: parser.Where_Clause, row: []types.Value, cols: []types.Column) -> bool {
	if len(clause.conditions) == 0 {
		return true
	}

	match := true
	if !clause.is_and {
		match = false
	}

	for cond in clause.conditions {
		idx, found := schema.find_column_index(cols, cond.column)
		if !found {
			return false
		}

		val := row[idx]
		cond_result := compare_condition(val, cond.operator, cond.value)
		if clause.is_and {
			match = match && cond_result
			if !match do return false
		} else {
			match = match || cond_result
			if match do return true
		}
	}
	return match
}

compare_condition :: proc(val: types.Value, op: parser.Token_Type, target: types.Value) -> bool {
	cmp := compare_values(val, target)
	#partial switch op {
	case .EQUALS:
		return cmp == 0
	case .NOT_EQUALS:
		return cmp != 0
	case .LESS_THAN:
		return cmp < 0
	case .GREATER_THAN:
		return cmp > 0
	case .LESS_EQUAL:
		return cmp <= 0
	case .GREATER_EQUAL:
		return cmp >= 0
	}
	return false
}

compare_values :: proc(a: types.Value, b: types.Value) -> int {
	if types.is_null(a) && types.is_null(b) do return 0
	if types.is_null(a) do return -1
	if types.is_null(b) do return 1

	#partial switch va in a {
	case i64:
		if vb, ok := b.(i64); ok {
			if va < vb do return -1
			if va > vb do return 1
			return 0
		}
		if vb, ok := b.(f64); ok {
			if f64(va) < vb do return -1
			if f64(va) > vb do return 1
			return 0
		}
	case f64:
		if vb, ok := b.(f64); ok {
			if va < vb do return -1
			if va > vb do return 1
			return 0
		}
		if vb, ok := b.(i64); ok {
			if va < f64(vb) do return -1
			if va > f64(vb) do return 1
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.compare(va, vb)
		}
	}
	return 0
}

print_header :: proc(cols: []types.Column, indices: []int) {
	for idx, i in indices {
		if i > 0 do fmt.print(" | ")
		fmt.print(cols[idx].name)
	}
	fmt.println()
	for _, i in indices {
		if i > 0 do fmt.print("-+-")
		for _ in 0 ..< len(cols[indices[i]].name) {
			fmt.print("-")
		}
	}
	fmt.println()
}

print_row :: proc(values: []types.Value, indices: []int) {
	for idx, i in indices {
		if i > 0 do fmt.print(" | ")
		fmt.print(types.value_to_string(values[idx]))
	}
	fmt.println()
}
