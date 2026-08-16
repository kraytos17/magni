package executor

import "core:log"
import "core:strings"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

exec_subquery :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> ([]Row_Entry, []types.Column) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil }

	table, found := schema.find_table_cached(t, tbl_name, cache)
	if !found {
		log.errorf("Error: Subquery table not found: %s", tbl_name)
		return nil, nil
	}

	table_tree := btree.init(t.pager, table.root_page)
	rows, scan_err := scan_table(&table_tree, table, nil, nil, t, context.temp_allocator, cache)
	if scan_err { return nil, nil }
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(
			rows,
			&where_clause,
			table.columns,
			[]Table_Col_Range {
				{
					table_name = stmt.from_alias if stmt.from_alias != "" else tbl_name,
					start_col = 0,
					col_count = len(table.columns),
				},
			},
		)
	}
	if len(stmt.columns) > 0 {
		col_indices := make([]int, len(stmt.columns), context.temp_allocator)
		for req_col, i in stmt.columns {
			idx, ok := schema.find_column_index(table.columns, req_col)
			if !ok { return nil, nil }
			col_indices[i] = idx
		}

		projected := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
		for entry in rows {
			new_vals := make([]types.Value, len(col_indices), context.temp_allocator)
			for ci, idx in col_indices {
				cloned, _ := types.value_clone(entry.values[idx], context.temp_allocator)
				new_vals[ci] = cloned
			}
			append(&projected, Row_Entry{entry.rowid, new_vals})
		}

		proj_cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
		for col_name, i in stmt.columns {
			proj_cols[i] = types.Column {
				name = strings.clone(col_name, context.temp_allocator),
				type = .TEXT,
			}
		}
		return projected[:], proj_cols
	}
	return rows, table.columns
}

// exec_select_literals materializes a FROM-less SELECT (e.g. `SELECT 1, 'a'`)
// as a single row of literal values. Returns the row and synthesized columns.
exec_select_subquery :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	subq, subq_ok := stmt.from.(^parser.Select_Stmt)
	if !subq_ok { return false }

	inner_rows, virtual_cols := exec_subquery(t, subq^)
	if inner_rows == nil { return false }

	alias := stmt.from_alias
	rows := make([dynamic]Row_Entry, context.temp_allocator)
	for entry in inner_rows {
		cloned := deep_copy_values(entry.values)
		append(&rows, Row_Entry{entry.rowid, cloned})
	}

	single_range := []Table_Col_Range {
		{table_name = alias, start_col = 0, col_count = len(virtual_cols)},
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		filtered := filter_rows(rows[:], &where_clause, virtual_cols, single_range)
		clear(&rows)
		append(&rows, ..filtered)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		virtual_cols,
		single_range,
		len(virtual_cols),
	)

	if !ok { return false }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows[:], order_clause, virtual_cols, single_range) {
			return false
		}
	}

	display_results(rows[:], virtual_cols, display_indices[:], stmt.limit, stmt.offset, stmt.aliases)
	return true
}

// exec_subquery_data evaluates a SELECT whose FROM is a subquery and returns
// the projected rows/columns without printing. Mirror of exec_select_subquery.
exec_subquery_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	subq, subq_ok := stmt.from.(^parser.Select_Stmt)
	if !subq_ok { return nil, nil, false }

	inner_rows, virtual_cols := exec_subquery(t, subq^, cache)
	if inner_rows == nil { return nil, nil, false }

	alias := stmt.from_alias
	rows := make([dynamic]Row_Entry, context.temp_allocator)
	for entry in inner_rows {
		cloned := deep_copy_values(entry.values)
		append(&rows, Row_Entry{entry.rowid, cloned})
	}

	single_range := []Table_Col_Range {
		{table_name = alias, start_col = 0, col_count = len(virtual_cols)},
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		filtered := filter_rows(rows[:], &where_clause, virtual_cols, single_range)
		clear(&rows)
		append(&rows, ..filtered)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		virtual_cols,
		single_range,
		len(virtual_cols),
	)
	if !ok { return nil, nil, false }

	// Project to the requested columns.
	proj_rows := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	for entry in rows {
		proj_vals := make([]types.Value, len(display_indices), context.temp_allocator)
		for idx, i in display_indices { proj_vals[i] = entry.values[idx] }
		append(&proj_rows, Row_Entry{entry.rowid, proj_vals})
	}

	proj_cols := make([]types.Column, len(display_indices), context.temp_allocator)
	for idx, i in display_indices { proj_cols[i] = virtual_cols[idx] }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		range0 := []Table_Col_Range {{table_name = "", start_col = 0, col_count = len(proj_cols)}}
		if !sort_rows(proj_rows[:], order_clause, proj_cols, range0) {
			return nil, nil, false
		}
	}

	out := proj_rows[:]
	if stmt.is_distinct { out = dedup_rows(out) }
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off { off = o }

		start := int(min(off, u64(len(out))))
		end := int(min(off + limit, u64(len(out))))
		out = out[start:end]
	}
	return out, proj_cols, true
}

