package executor

import "core:fmt"
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
			blob := make([]u8, len(val), context.temp_allocator)
			copy(blob, val)
			new_values[i] = types.value_blob(blob)
		case:
			new_values[i] = val
		}
	}
	return new_values
}

// Execute a parsed statement
execute_statement :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	switch stmt.type {
	case .CREATE_TABLE:
		return execute_create_table(p, stmt)
	case .INSERT:
		return execute_insert(p, stmt)
	case .SELECT:
		return execute_select(p, stmt)
	case .UPDATE:
		return execute_update(p, stmt)
	case .DELETE:
		return execute_delete(p, stmt)
	case .DROP_TABLE:
		return execute_drop_table(p, stmt)
	}
	return false
}

// Execute CREATE TABLE
execute_create_table :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	ok, err_msg := schema.schema_validate_columns(stmt.columns)
	if !ok {
		fmt.eprintln("Error:", err_msg)
		return false
	}
	if schema.schema_table_exists(p, stmt.table_name) {
		fmt.eprintln("Error: Table already exists:", stmt.table_name)
		return false
	}

	root_page, err := pager.pager_allocate_page(p)
	if err != nil {
		fmt.eprintln("Error: Failed to allocate page for table")
		return false
	}

	for root_page.page_num <= schema.SCHEMA_PAGE {
		pager.pager_mark_dirty(p, root_page.page_num)
		pager.pager_flush_page(p, root_page.page_num)
		root_page, err = pager.pager_allocate_page(p)
		if err != nil {
			fmt.eprintln("Error: Failed to allocate valid page")
			return false
		}
	}

	init_err := btree.btree_init_leaf_page(root_page.data)
	if init_err != nil {
		fmt.eprintln("Error: Failed to initialize leaf page")
		return false
	}
	if !schema.schema_add_table(p, stmt.table_name, stmt.columns, root_page.page_num) {
		fmt.eprintln("Error: Failed to add table to schema")
		return false
	}

	pager.pager_flush_page(p, root_page.page_num)
	fmt.printf("Created table: %s\n", stmt.table_name)
	return true
}

// Execute INSERT
execute_insert :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.schema_get_table(p, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if len(stmt.insert_values) != len(table.columns) {
		fmt.eprintln("Error: Column count mismatch")
		fmt.printfln("Expected %d columns, got %d", len(table.columns), len(stmt.insert_values))
		return false
	}
	if !cell.cell_validate_types(stmt.insert_values, table.columns) {
		fmt.eprintln("Error: Type or constraint validation failed")
		return false
	}

	next_rowid, rowid_err := btree.btree_get_next_rowid(p, table.root_page)
	if rowid_err != nil {
		fmt.eprintln("Error: Failed to get next rowid")
		return false
	}

	pk_col_idx, has_pk := schema.schema_get_pk_column(table.columns)
	if has_pk {
		if pk_val, ok := stmt.insert_values[pk_col_idx].(i64); ok {
			next_rowid = types.Row_ID(pk_val)
		}
	}

	insert_err := btree.btree_insert_cell(p, table.root_page, next_rowid, stmt.insert_values)
	if insert_err != nil {
		fmt.eprintln("Error: Failed to insert row:", insert_err)
		return false
	}

	pager.pager_flush_page(p, table.root_page)
	fmt.printf("Inserted 1 row (rowid=%d)\n", next_rowid)
	return true
}

// Execute SELECT
execute_select :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.schema_get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	display_all := len(stmt.select_columns) == 0
	col_indices := make([dynamic]int, context.temp_allocator)
	if !display_all {
		for col_name in stmt.select_columns {
			idx, ok := schema.schema_find_column_index(table.columns, col_name)
			if !ok {
				fmt.eprintln("Error: Column not found:", col_name)
				return false
			}
			append(&col_indices, idx)
		}
	} else {
		for i in 0 ..< len(table.columns) {
			append(&col_indices, i)
		}
	}
	if display_all {
		for col, i in table.columns {
			if i > 0 do fmt.print(" | ")
			fmt.print(col.name)
		}
	} else {
		for col_name, i in stmt.select_columns {
			if i > 0 do fmt.print(" | ")
			fmt.print(col_name)
		}
	}

	fmt.println()
	for i in 0 ..< len(col_indices) {
		if i > 0 do fmt.print("-+-")
		fmt.print("----------")
	}

	fmt.println()
	cursor := btree.btree_cursor_start(table.root_page, context.temp_allocator)
	row_count := 0
	for !cursor.end_of_table {
		config := btree.BTree_Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.btree_cursor_get_cell(p, &cursor, config)
		if err != nil {
			btree.btree_cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.btree_cell_ref_destroy(&cell_ref)
				btree.btree_cursor_advance(p, &cursor)
				continue
			}
		}

		for idx, i in col_indices {
			if i > 0 do fmt.print(" | ")
			if idx < len(row_values) {
				fmt.print(types.value_to_string(row_values[idx]))
			} else {
				fmt.print("NULL")
			}
		}

		fmt.println()
		row_count += 1
		btree.btree_cell_ref_destroy(&cell_ref)
		btree.btree_cursor_advance(p, &cursor)
	}
	fmt.printf("\n%d row(s) returned\n", row_count)
	return true
}

// Execute UPDATE
execute_update :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.schema_get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	update_indices := make([dynamic]int, context.temp_allocator)
	for col_name in stmt.update_columns {
		idx, ok := schema.schema_find_column_index(table.columns, col_name)
		if !ok {
			fmt.eprintln("Error: Column not found:", col_name)
			return false
		}
		append(&update_indices, idx)
	}

	rows_to_update := make([dynamic]struct {
			rowid:  types.Row_ID,
			values: []types.Value,
		}, context.temp_allocator)

	cursor := btree.btree_cursor_start(table.root_page, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.BTree_Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.btree_cursor_get_cell(p, &cursor, config)
		if err != nil {
			btree.btree_cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		rowid := cell_ref.cell.rowid
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.btree_cell_ref_destroy(&cell_ref)
				btree.btree_cursor_advance(p, &cursor)
				continue
			}
		}

		new_values := make([]types.Value, len(row_values), context.temp_allocator)
		copy(new_values, row_values)
		for i in 0 ..< len(update_indices) {
			new_values[update_indices[i]] = stmt.update_values[i]
		}

		if !cell.cell_validate_types(new_values, table.columns) {
			fmt.eprintln("Error: Type or constraint validation failed")
			btree.btree_cell_ref_destroy(&cell_ref)
			return false
		}

		append(&rows_to_update, struct {
			rowid:  types.Row_ID,
			values: []types.Value,
		}{rowid, new_values})

		btree.btree_cell_ref_destroy(&cell_ref)
		btree.btree_cursor_advance(p, &cursor)
	}

	update_count := 0
	for row in rows_to_update {
		del_err := btree.btree_delete_cell(p, table.root_page, row.rowid)
		if del_err != nil {
			fmt.eprintln("Warning: Failed to delete row", row.rowid)
			continue
		}

		ins_err := btree.btree_insert_cell(p, table.root_page, row.rowid, row.values)
		if ins_err != nil {
			fmt.eprintln("Warning: Failed to insert updated row", row.rowid)
			continue
		}
		update_count += 1
	}

	pager.pager_flush_page(p, table.root_page)
	fmt.printf("Updated %d row(s)\n", update_count)
	return true
}

// Execute DELETE
execute_delete :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.schema_get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	rowids_to_delete := make([dynamic]types.Row_ID, context.temp_allocator)
	cursor := btree.btree_cursor_start(table.root_page, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.BTree_Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.btree_cursor_get_cell(p, &cursor, config)
		if err != nil {
			btree.btree_cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		rowid := cell_ref.cell.rowid
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.btree_cell_ref_destroy(&cell_ref)
				btree.btree_cursor_advance(p, &cursor)
				continue
			}
		}

		append(&rowids_to_delete, rowid)
		btree.btree_cell_ref_destroy(&cell_ref)
		btree.btree_cursor_advance(p, &cursor)
	}

	delete_count := 0
	for rowid in rowids_to_delete {
		del_err := btree.btree_delete_cell(p, table.root_page, rowid)
		if del_err == nil {
			delete_count += 1
		}
	}

	pager.pager_flush_page(p, table.root_page)
	fmt.printf("Deleted %d row(s)\n", delete_count)
	return true
}

// Execute DROP TABLE
execute_drop_table :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	if !schema.schema_table_exists(p, stmt.table_name) {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if !schema.schema_drop_table(p, stmt.table_name) {
		fmt.eprintln("Error: Failed to drop table")
		return false
	}
	fmt.printf("Dropped table: %s\n", stmt.table_name)
	return true
}

// Evaluate WHERE clause against a row
evaluate_where_clause :: proc(
	clause: parser.Where_Clause,
	values: []types.Value,
	columns: []types.Column,
) -> bool {
	if len(clause.conditions) == 0 {
		return true
	}

	results := make([dynamic]bool, context.temp_allocator)
	for cond in clause.conditions {
		col_idx, ok := schema.schema_find_column_index(columns, cond.column)
		if !ok {
			append(&results, false)
			continue
		}
		if col_idx >= len(values) {
			append(&results, false)
			continue
		}

		result := evaluate_condition(cond, values[col_idx])
		append(&results, result)
	}

	if len(results) == 0 {
		return true
	}

	final_result := results[0]
	if clause.is_and {
		for r in results[1:] {
			final_result = final_result && r
		}
	} else {
		for r in results[1:] {
			final_result = final_result || r
		}
	}
	return final_result
}

// Evaluate a single condition
evaluate_condition :: proc(cond: parser.Condition, value: types.Value) -> bool {
	#partial switch cond.operator {
	case .EQUALS:
		return compare_values(value, cond.value) == 0
	case .NOT_EQUALS:
		return compare_values(value, cond.value) != 0
	case .LESS_THAN:
		return compare_values(value, cond.value) < 0
	case .GREATER_THAN:
		return compare_values(value, cond.value) > 0
	case .LESS_EQUAL:
		return compare_values(value, cond.value) <= 0
	case .GREATER_EQUAL:
		return compare_values(value, cond.value) >= 0
	}
	return false
}

// Compare two values (-1: left < right, 0: equal, 1: left > right)
compare_values :: proc(left: types.Value, right: types.Value) -> int {
	left_null := types.is_null(left)
	right_null := types.is_null(right)
	if left_null && right_null do return 0
	if left_null do return -1
	if right_null do return 1

	#partial switch l in left {
	case i64:
		if r, ok := right.(i64); ok {
			if l < r do return -1
			if l > r do return 1
			return 0
		}
		if r, ok := right.(f64); ok {
			lf := f64(l)
			if lf < r do return -1
			if lf > r do return 1
			return 0
		}
	case f64:
		if r, ok := right.(f64); ok {
			if l < r do return -1
			if l > r do return 1
			return 0
		}
		if r, ok := right.(i64); ok {
			rf := f64(r)
			if l < rf do return -1
			if l > rf do return 1
			return 0
		}
	case string:
		if r, ok := right.(string); ok {
			return strings.compare(l, r)
		}
	case []u8:
		if r, ok := right.([]u8); ok {
			min_len := min(len(l), len(r))
			for i in 0 ..< min_len {
				if l[i] < r[i] do return -1
				if l[i] > r[i] do return 1
			}

			if len(l) < len(r) do return -1
			if len(l) > len(r) do return 1
			return 0
		}
	}
	return 0
}
