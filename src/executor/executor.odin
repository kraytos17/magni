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

// Execute a parsed statement
execute_statement :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	switch stmt.type {
	case .CREATE_TABLE:
		return exec_create_table(p, stmt)
	case .INSERT:
		return exec_insert(p, stmt)
	case .SELECT:
		return exec_select(p, stmt)
	case .UPDATE:
		return exec_update(p, stmt)
	case .DELETE:
		return exec_delete(p, stmt)
	case .DROP_TABLE:
		return exec_drop_table(p, stmt)
	}
	return false
}

exec_create_table :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	ok, err_msg := schema.validate_columns(stmt.columns)
	if !ok {
		fmt.eprintln("Error:", err_msg)
		return false
	}
	if schema.table_exists(p, stmt.table_name) {
		fmt.eprintln("Error: Table already exists:", stmt.table_name)
		return false
	}

	root_page, err := pager.allocate_page(p)
	if err != nil {
		fmt.eprintln("Error: Failed to allocate page for table")
		return false
	}

	for root_page.page_num <= schema.SCHEMA_PAGE {
		pager.mark_dirty(p, root_page.page_num)
		pager.flush_page(p, root_page.page_num)
		root_page, err = pager.allocate_page(p)
		if err != nil {
			fmt.eprintln("Error: Failed to allocate valid page")
			return false
		}
	}

	init_err := btree.init_leaf_page(root_page.data, root_page.page_num)
	if init_err != nil {
		fmt.eprintln("Error: Failed to initialize leaf page")
		return false
	}
	if !schema.add_table(p, stmt.table_name, stmt.columns, root_page.page_num, stmt.original_sql) {
		fmt.eprintln("Error: Failed to add table to schema")
		return false
	}

	pager.flush_page(p, root_page.page_num)
	fmt.printf("Created table: %s\n", stmt.table_name)
	return true
}

exec_insert :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.get_table(p, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if len(stmt.insert_values) != len(table.columns) {
		fmt.eprintln("Error: Column count mismatch")
		fmt.printfln("Expected %d columns, got %d", len(table.columns), len(stmt.insert_values))
		return false
	}
	if !cell.validate(stmt.insert_values, table.columns) {
		fmt.eprintln("Error: Type or constraint validation failed")
		return false
	}

	next_rowid, rowid_err := btree.get_next_rowid(p, table.root_page)
	if rowid_err != .None {
		next_rowid = 1
	}

	pk_col_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if pk_val, ok := stmt.insert_values[pk_col_idx].(i64); ok {
			next_rowid = types.Row_ID(pk_val)
		}
	}

	insert_err := btree.insert_cell(p, table.root_page, next_rowid, stmt.insert_values)
	if insert_err == .Page_Full {
		page, _ := pager.get_page(p, table.root_page)
		header := btree.get_header(page.data, table.root_page)
		if header.page_type == .LEAF_TABLE {
			fmt.println("Root Leaf full! Splitting root...")
			split_err := btree.split_leaf_root(p, table.root_page)
			if split_err != .None {
				fmt.eprintln("Critical Error: Failed to split root page:", split_err)
				return false
			}

			// Retry insertion into the new structure (Root is now Interior)
			insert_err = btree.insert_cell(p, table.root_page, next_rowid, stmt.insert_values)

		} else {
			fmt.eprintln(
				"Error: Root Interior Node is full. Tree height increase not implemented for Interior Roots.",
			)
			return false
		}
	}

	if insert_err != .None {
		fmt.eprintln("Error: Failed to insert row:", insert_err)
		return false
	}

	pager.flush_page(p, table.root_page)
	fmt.printf("Inserted 1 row (rowid=%d)\n", next_rowid)
	return true
}

exec_select :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	display_all := len(stmt.select_columns) == 0
	col_indices := make([dynamic]int, context.temp_allocator)
	if !display_all {
		for col_name in stmt.select_columns {
			idx, ok := schema.find_column_index(table.columns, col_name)
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
	cursor, _ := btree.cursor_start(p, table.root_page, context.temp_allocator)
	row_count := 0
	for !cursor.end_of_table {
		config := btree.Config {
			allocator        = context.temp_allocator,
			zero_copy        = false,
			check_duplicates = false,
		}

		cell_ref, err := btree.cursor_get_cell(p, &cursor, config)
		if err != nil {
			btree.cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.cell_ref_destroy(&cell_ref)
				btree.cursor_advance(p, &cursor)
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
		btree.cell_ref_destroy(&cell_ref)
		btree.cursor_advance(p, &cursor)
	}
	fmt.printf("\n%d row(s) returned\n", row_count)
	return true
}

exec_update :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	update_indices := make([dynamic]int, context.temp_allocator)
	for col_name in stmt.update_columns {
		idx, ok := schema.find_column_index(table.columns, col_name)
		if !ok {
			fmt.eprintln("Error: Column not found:", col_name)
			return false
		}
		append(&update_indices, idx)
	}

	Row_Update :: struct {
		rowid:  types.Row_ID,
		values: []types.Value,
	}

	rows_to_update := make([dynamic]Row_Update, context.temp_allocator)
	cursor, _ := btree.cursor_start(p, table.root_page, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.Config {
			allocator = context.temp_allocator,
			zero_copy = false,
		}

		cell_ref, err := btree.cursor_get_cell(p, &cursor, config)
		if err != .None {
			btree.cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		rowid := cell_ref.cell.rowid
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.cell_ref_destroy(&cell_ref)
				btree.cursor_advance(p, &cursor)
				continue
			}
		}

		new_values := make([]types.Value, len(row_values), context.temp_allocator)
		copy(new_values, row_values)
		for i in 0 ..< len(update_indices) {
			new_values[update_indices[i]] = stmt.update_values[i]
		}

		if !cell.validate(new_values, table.columns) {
			fmt.eprintln("Error: Type or constraint validation failed")
			btree.cell_ref_destroy(&cell_ref)
			return false
		}

		append(&rows_to_update, Row_Update{rowid, new_values})
		btree.cell_ref_destroy(&cell_ref)
		btree.cursor_advance(p, &cursor)
	}

	update_count := 0
	for row in rows_to_update {
		leaf_page_num, find_err := btree.find_leaf_page(p, table.root_page, row.rowid)
		if find_err != .None {
			fmt.eprintln("Warning: Could not locate row for update:", row.rowid)
			continue
		}
		if err := btree.delete_cell(p, leaf_page_num, row.rowid); err != .None {
			fmt.eprintln("Warning: Failed to delete old row", row.rowid)
			continue
		}
		if err := btree.insert_cell(p, table.root_page, row.rowid, row.values); err != .None {
			fmt.eprintln("Warning: Failed to insert updated row", row.rowid)
			continue
		}
		update_count += 1
	}

	pager.flush_page(p, table.root_page)
	fmt.printf("Updated %d row(s)\n", update_count)
	return true
}

exec_delete :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	table, found := schema.get_table(p, stmt.from_table, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.from_table)
		return false
	}

	rowids_to_delete := make([dynamic]types.Row_ID, context.temp_allocator)
	cursor, _ := btree.cursor_start(p, table.root_page, context.temp_allocator)
	for !cursor.end_of_table {
		config := btree.Config {
			allocator = context.temp_allocator,
			zero_copy = false,
		}
		
		cell_ref, err := btree.cursor_get_cell(p, &cursor, config)
		if err != .None {
			btree.cursor_advance(p, &cursor)
			continue
		}

		row_values := deep_copy_values(cell_ref.cell.values)
		rowid := cell_ref.cell.rowid
		if clause, has_where := stmt.where_clause.?; has_where {
			if !evaluate_where_clause(clause, row_values, table.columns) {
				btree.cell_ref_destroy(&cell_ref)
				btree.cursor_advance(p, &cursor)
				continue
			}
		}

		append(&rowids_to_delete, rowid)
		btree.cell_ref_destroy(&cell_ref)
		btree.cursor_advance(p, &cursor)
	}

	delete_count := 0
	for rowid in rowids_to_delete {
		leaf_page_num, find_err := btree.find_leaf_page(p, table.root_page, rowid)
		if find_err != .None {
			continue
		}
		if btree.delete_cell(p, leaf_page_num, rowid) == .None {
			delete_count += 1
		}
	}

	pager.flush_page(p, table.root_page)
	fmt.printf("Deleted %d row(s)\n", delete_count)
	return true
}

exec_drop_table :: proc(p: ^pager.Pager, stmt: parser.Statement) -> bool {
	if !schema.table_exists(p, stmt.table_name) {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if !schema.drop_table(p, stmt.table_name) {
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
		col_idx, ok := schema.find_column_index(columns, cond.column)
		if !ok || col_idx >= len(values) {
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
	cmp := compare_values(value, cond.value)
	#partial switch cond.operator {
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

// Compare two values (-1: left < right, 0: equal, 1: left > right)
compare_values :: proc(left: types.Value, right: types.Value) -> int {
	if types.is_null(left) && types.is_null(right) do return 0
	if types.is_null(left) do return -1
	if types.is_null(right) do return 1

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
