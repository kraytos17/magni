package executor

import "core:fmt"
import "core:log"
import "core:strings"
import "src:btree"
import "src:cell"
import "src:parser"
import "src:schema"
import "src:types"

// hash_join_key returns a string suitable for use as a hash map key.
// For string values, returns the original string directly (no allocation).
// For other types, converts via value_to_string.
hash_join_key :: proc(v: types.Value) -> string {
	if s, ok := v.(string); ok { return s }
	return types.value_to_string(v)
}

join_emit_combined :: proc(outer: Row_Entry, inner: []types.Value, new_rows: ^[dynamic]Row_Entry) {
	combined := make([]types.Value, len(outer.values) + len(inner), context.temp_allocator)
	copy(combined[:len(outer.values)], outer.values)
	copy(combined[len(outer.values):], inner)
	append(new_rows, Row_Entry{0, combined})
}

join_emit_null_row :: proc(outer: Row_Entry, right_col_count: int, new_rows: ^[dynamic]Row_Entry) {
	null_row := make([]types.Value, len(outer.values) + right_col_count, context.temp_allocator)
	copy(null_row[:len(outer.values)], outer.values)
	for k in len(outer.values) ..< len(null_row) {
		null_row[k] = types.value_null()
	}
	append(new_rows, Row_Entry{0, null_row})
}

join_hash_i64 :: proc(
	rows: []Row_Entry,
	right_rows: []Row_Entry,
	left_col: int,
	right_col: int,
	right_col_count: int,
	is_left: bool,
	new_rows: ^[dynamic]Row_Entry,
) {
	build_left := len(rows) <= len(right_rows)
	build_cap := len(rows) if build_left else len(right_rows)
	ht := make(map[i64][dynamic]int, build_cap, context.temp_allocator)
	if build_left {
		for row, ri in rows {
			key, key_ok := row.values[left_col].(i64)
			if !key_ok { continue }

			bucket := ht[key]
			append(&bucket, ri)
			ht[key] = bucket
		}
	} else {
		for r_row, ri in right_rows {
			key, key_ok := r_row.values[right_col].(i64)
			if !key_ok { continue }

			bucket := ht[key]
			append(&bucket, ri)
			ht[key] = bucket
		}
	}

	matched_left := make(map[int]bool, len(rows), context.temp_allocator)
	if build_left {
		for r_row in right_rows {
			key, key_ok := r_row.values[right_col].(i64)
			if !key_ok { continue }
			if matches, has := ht[key]; has {
				for ri in matches {
					matched_left[ri] = true
					join_emit_combined(rows[ri], r_row.values, new_rows)
				}
			}
		}
	} else {
		for l_row, li in rows {
			key, key_ok := l_row.values[left_col].(i64)
			if !key_ok { continue }
			if matches, has := ht[key]; has {
				for ri in matches {
					matched_left[li] = true
					join_emit_combined(l_row, right_rows[ri].values, new_rows)
				}
			}
		}
	}

	for _, bucket in ht do delete(bucket)
	delete(ht)
	if is_left {
		for li in 0 ..< len(rows) {
			if li in matched_left { continue }
			join_emit_null_row(rows[li], right_col_count, new_rows)
		}
	}
	delete(matched_left)
}

join_hash_string :: proc(
	rows: []Row_Entry,
	right_rows: []Row_Entry,
	left_col: int,
	right_col: int,
	right_col_count: int,
	is_left: bool,
	new_rows: ^[dynamic]Row_Entry,
) {
	build_left := len(rows) <= len(right_rows)
	build_cap := len(rows) if build_left else len(right_rows)
	ht := make(map[string][dynamic]int, build_cap, context.temp_allocator)
	if build_left {
		for row, ri in rows {
			key := hash_join_key(row.values[left_col])
			bucket := ht[key]
			append(&bucket, ri)
			ht[key] = bucket
		}
	} else {
		for r_row, ri in right_rows {
			key := hash_join_key(r_row.values[right_col])
			bucket := ht[key]
			append(&bucket, ri)
			ht[key] = bucket
		}
	}

	matched_left := make(map[int]bool, len(rows), context.temp_allocator)
	if build_left {
		for r_row in right_rows {
			key := hash_join_key(r_row.values[right_col])
			if matches, has := ht[key]; has {
				for ri in matches {
					matched_left[ri] = true
					join_emit_combined(rows[ri], r_row.values, new_rows)
				}
			}
		}
	} else {
		for l_row, li in rows {
			key := hash_join_key(l_row.values[left_col])
			if matches, has := ht[key]; has {
				for ri in matches {
					matched_left[li] = true
					join_emit_combined(l_row, right_rows[ri].values, new_rows)
				}
			}
		}
	}

	for _, bucket in ht do delete(bucket)
	delete(ht)
	if is_left {
		for li in 0 ..< len(rows) {
			if li in matched_left { continue }
			join_emit_null_row(rows[li], right_col_count, new_rows)
		}
	}
	delete(matched_left)
}

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
exec_select_literals :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	row := Row_Entry{rowid = 1, values = stmt.literal_values}
	cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
	for name, i in stmt.columns {
		cols[i] = types.Column {
			name = name,
			type = .INTEGER,
		}
	}

	rows := make([]Row_Entry, 1, context.temp_allocator)
	rows[0] = row
	return rows, cols, true
}

exec_select :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	if _, is_nf := stmt.from.(parser.No_From); is_nf {
		rows, cols, ok := exec_select_literals(t, stmt)
		if !ok { return false }

		indices := make([]int, len(cols), context.temp_allocator)
		for i in 0 ..< len(cols) { indices[i] = i }

		display_results(rows, cols, indices, nil, nil, stmt.aliases)
		return true
	}

	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq { return exec_select_subquery(t, stmt) }
	if len(stmt.joins) == 0 { return exec_select_single(t, stmt) }

	rows, combined_cols, table_ranges, total_cols, ok := build_join_result(t, stmt)
	if !ok { return false }
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_combined(stmt, rows, combined_cols, table_ranges)
	}

	display_indices, d_ok := build_display_indices(
		stmt.columns,
		combined_cols,
		table_ranges,
		total_cols,
	)

	if !d_ok { return false }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) {
			return false
		}
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }

	display_results(rows, combined_cols, display_indices[:], stmt.limit, stmt.offset, stmt.aliases)
	return true
}

// single_range_for returns a single flat table-range covering all columns —
// used when a result has no per-table column ranges (compound results, joins).
single_range_for :: proc(col_count: int) -> []Table_Col_Range {
	range0 := []Table_Col_Range {
		{table_name = "", start_col = 0, col_count = col_count},
	}
	return range0
}

// select_header_names returns the display names for a SELECT's columns,
// substituting AS aliases where present.
select_header_names :: proc(stmt: parser.Select_Stmt) -> []string {
	names := make([dynamic]string, 0, len(stmt.columns), context.temp_allocator)
	for col, i in stmt.columns {
		if i < len(stmt.aliases) && stmt.aliases[i] != "" {
			append(&names, stmt.aliases[i])
		} else {
			append(&names, col)
		}
	}
	return names[:]
}

// build_join_result resolves a SELECT's table sources, executes its joins, and
// applies the WHERE filter, returning the combined rows and columns WITHOUT
// printing. Shared by the printing path and the set-operation data path.
build_join_result :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	int,
	bool,
) {
	table_count := 1 + len(stmt.joins)
	table_ctxs := make([]Table_Context, table_count, context.temp_allocator)
	table_ranges := make([]Table_Col_Range, table_count, context.temp_allocator)
	info: ^types.Table
	found: bool
	t0_alias: string
	col_count_0 := 0
	if tbl_name, is_table := stmt.from.(string); is_table {
		info, found = schema.find_table_cached(t, tbl_name, cache)
		if !found {
			log.errorf("Error: Table not found: %s", tbl_name)
			return nil, nil, nil, 0, false
		}

		table_ctxs[0] = Table_Context {
			info = {table = info^, tree = btree.init(t.pager, info.root_page)},
			range = {
				table_name = stmt.from_alias if stmt.from_alias != "" else tbl_name,
				start_col = 0,
				col_count = len(info.columns),
			},
		}

		table_ranges[0] = table_ctxs[0].range
		t0_alias = stmt.from_alias if stmt.from_alias != "" else tbl_name
		col_count_0 = len(info.columns)
	} else if vt, is_vt := stmt.from.(^parser.Select_Stmt); is_vt {
		inner_rows, inner_cols := exec_subquery(t, vt^, cache)
		if inner_rows == nil { return nil, nil, nil, 0, false }

		table_ctxs[0] = Table_Context {
			info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
			range = {table_name = stmt.from_alias, start_col = 0, col_count = len(inner_cols)},
		}

		table_ranges[0] = table_ctxs[0].range
		t0_alias = stmt.from_alias
		col_count_0 = len(inner_cols)
	}

	for join, i in stmt.joins {
		idx := i + 1
		if jt_name, is_table := join.source.(string); is_table {
			info, found = schema.find_table_cached(t, jt_name, cache)
			if !found {
				log.errorf("Error: Table not found: %s", jt_name)
				return nil, nil, nil, 0, false
			}

			prev := table_ctxs[idx - 1].range
			alias := join.alias if join.alias != "" else jt_name
			table_ctxs[idx] = Table_Context {
				info = {table = info^, tree = btree.init(t.pager, info.root_page)},
				range = {
					table_name = alias,
					start_col = prev.start_col + prev.col_count,
					col_count = len(info.columns),
				},
			}
			table_ranges[idx] = table_ctxs[idx].range
		} else if subq, is_subquery := join.source.(^parser.Select_Stmt); is_subquery {
			inner_rows, inner_cols := exec_subquery(t, subq^, cache)
			if inner_rows == nil { return nil, nil, nil, 0, false }

			alias := join.alias if join.alias != "" else ""
			prev := table_ctxs[idx - 1].range
			table_ctxs[idx] = Table_Context {
				info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
				range = {
					table_name = alias,
					start_col = prev.start_col + prev.col_count,
					col_count = len(inner_cols),
				},
			}
			table_ranges[idx] = table_ctxs[idx].range
		}
	}

	total_cols :=
		table_ctxs[table_count - 1].range.start_col + table_ctxs[table_count - 1].range.col_count

	combined_cols := make([]types.Column, total_cols, context.temp_allocator)
	for ti in 0 ..< table_count {
		tr := table_ctxs[ti].range
		if vt, is_virtual := table_ctxs[ti].info.virtual.?; is_virtual {
			for j in 0 ..< tr.col_count { combined_cols[tr.start_col + j] = vt.columns[j] }
		} else {
			for j in 0 ..< tr.col_count { combined_cols[tr.start_col + j] = table_ctxs[ti].info.table.columns[j] }
		}
	}

	rows: []Row_Entry
	if col_count_0 > 0 {
		r, scan_err := scan_table(
			&table_ctxs[0].info.tree,
			&table_ctxs[0].info.table,
			nil,
			nil,
			t,
			context.temp_allocator,
			cache,
		)
		if scan_err { return nil, nil, nil, 0, false }
		rows = r
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		rows = vt.rows
	}

	for j_idx in 0 ..< len(stmt.joins) {
		jc := stmt.joins[j_idx]
		info_idx := j_idx + 1
		is_left := jc.join_type == .LEFT
		right_col_count := table_ctxs[info_idx].range.col_count
		new_rows := make([dynamic]Row_Entry, context.temp_allocator)
		right_rows: []Row_Entry
		if vt, is_vt := table_ctxs[info_idx].info.virtual.?; is_vt {
			right_rows = vt.rows
		} else {
			// Materialize the right table once, then match against every left row
			right_rows, _ = scan_table(
				&table_ctxs[info_idx].info.tree,
				&table_ctxs[info_idx].info.table,
				nil,
				nil,
				t,
				context.temp_allocator,
				cache,
			)
		}

		hash_used := false
		if on_cl, has_on := jc.on_clause.?; has_on {
			if cond, has_cond := where_single_condition(on_cl); has_cond && cond.operator == .EQUALS {
				if rhs_str, is_col := cond.rhs.(string); is_col {
					left_idx, left_ok := resolve_qualified_column(
						combined_cols,
						table_ranges,
						cond.column,
					)
					right_idx, right_ok := resolve_qualified_column(
						combined_cols,
						table_ranges,
						rhs_str,
					)
					if left_ok && right_ok {
						hash_used = true
						right_adjust := table_ctxs[info_idx].range.start_col
						key_is_int := false
						if len(rows) > 0 && len(right_rows) > 0 {
							if _, ok := rows[0].values[left_idx].(i64);
							   ok { key_is_int = true }
						}
						if key_is_int {
							join_hash_i64(
								rows,
								right_rows,
								left_idx,
								right_idx - right_adjust,
								right_col_count,
								is_left,
								&new_rows,
							)
						} else {
							join_hash_string(
								rows,
								right_rows,
								left_idx,
								right_idx - right_adjust,
								right_col_count,
								is_left,
								&new_rows,
							)
						}
					}
				}
			}
		}
		if !hash_used {
			if is_left || len(right_rows) >= len(rows) {
				for outer_row in rows {
					matched := false
					for right_row in right_rows {
						try_join_match(
							outer_row,
							right_row.values,
							jc,
							combined_cols,
							table_ranges,
							&new_rows,
							&matched,
						)
					}
					if is_left && !matched {
						join_emit_null_row(outer_row, right_col_count, &new_rows)
					}
				}
			} else {
				for r_row in right_rows {
					for l_row in rows {
						_dummy := false
						try_join_match(
							l_row,
							r_row.values,
							jc,
							combined_cols,
							table_ranges,
							&new_rows,
							&_dummy,
						)
					}
				}
			}
		}
		rows = new_rows[:]
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(rows, &where_clause, combined_cols, table_ranges)
	}
	return rows, combined_cols, table_ranges, total_cols, true
}

// exec_select_join_data evaluates a SELECT with JOINs and returns projected
// rows/columns without printing.
exec_select_join_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	rows, combined_cols, table_ranges, total_cols, ok := build_join_result(t, stmt, cache)
	if !ok { return nil, nil, false }
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows, combined_cols, table_ranges)
	}

	// Sort on the full combined rows (so qualified ORDER BY names resolve),
	// then project to the requested columns.
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) {
			return nil, nil, false
		}
	}

	display_indices, d_ok := build_display_indices(
		stmt.columns,
		combined_cols,
		table_ranges,
		total_cols,
	)
	if !d_ok { return nil, nil, false }

	proj_rows := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	for entry in rows {
		proj_vals := make([]types.Value, len(display_indices), context.temp_allocator)
		for idx, i in display_indices { proj_vals[i] = entry.values[idx] }
		append(&proj_rows, Row_Entry{entry.rowid, proj_vals})
	}

	proj_cols := make([]types.Column, len(display_indices), context.temp_allocator)
	for idx, i in display_indices {
		proj_cols[i] = combined_cols[idx]
		if i < len(stmt.aliases) && stmt.aliases[i] != "" {
			proj_cols[i].name = stmt.aliases[i]
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

exec_select_single :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found { log.errorf("Error: Table not found: %s", tbl_name); return false }
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 { has_order = true }
	// Fast path: SELECT COUNT(*) FROM table (no WHERE, GROUP BY, DISTINCT, ORDER BY, LIMIT)
	if len(stmt.aggregates) == 1 &&
	   stmt.aggregates[0].func == .COUNT &&
	   stmt.aggregates[0].column == "" &&
	   stmt.where_clause == nil &&
	   len(stmt.group_by) == 0 &&
	   stmt.having == nil &&
	   !stmt.is_distinct &&
	   !has_order &&
	   stmt.limit == nil &&
	   stmt.offset == nil {
		count, count_err := btree.tree_count_rows(&table_tree)
		if count_err != .None {
			log.error("Error: Failed to count rows")
			return false
		}

		rows_mat := make([][]string, 1, context.temp_allocator)
		row_strs := make([]string, len(stmt.columns), context.temp_allocator)
		for i in 0 ..< len(stmt.columns) {
			row_strs[i] = fmt.aprintf("%d", i64(count), allocator = context.temp_allocator)
		}

		rows_mat[0] = row_strs
		render_table(select_header_names(stmt), rows_mat)
		fmt.printf("(%d rows)\n", 1)
		return true
	}

	_, has_lim := stmt.limit.?
	max_rows := stmt.limit if has_lim && !has_order else nil
	rows, scan_err := scan_table(
		&table_tree,
		&table,
		stmt.where_clause,
		max_rows,
		t,
		context.temp_allocator,
	)
	if scan_err { return false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
	}
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_combined(stmt, rows, table.columns, single_range)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		table.columns,
		single_range,
		len(table.columns),
	)

	if !ok { return false }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, table.columns, single_range) { return false }
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }
	display_results(rows, table.columns, display_indices, stmt.limit, stmt.offset, stmt.aliases)
	return true
}

exec_select_single_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil, false }

	table, found := schema.find_table_cached(t, tbl_name, cache)
	if !found {
		log.errorf("Error: Table not found: %s", tbl_name)
		return nil, nil, false
	}

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 { has_order = true }

	_, has_lim := stmt.limit.?
	max_rows := stmt.limit if has_lim && !has_order else nil
	rows, scan_err := scan_table(
		&table_tree,
		table,
		stmt.where_clause,
		max_rows,
		t,
		context.temp_allocator,
		cache,
	)
	if scan_err { return nil, nil, false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
	}
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows, table.columns, single_range)
	}
	if order_clause, has_o2 := stmt.order_by.?; has_o2 && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, table.columns, single_range) {
			return nil, nil, false
		}
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off { off = o }

		start := int(min(off, u64(len(rows))))
		end := int(min(off + limit, u64(len(rows))))
		rows = rows[start:end]
	}

	// Project to the requested columns (e.g. `SELECT c FROM u` on a multi-column
	// table returns only column c). Full projection matches exec_select_single.
	if len(stmt.columns) > 0 {
		indices, i_ok := build_display_indices(
			stmt.columns,
			table.columns,
			single_range,
			len(table.columns),
		)
		if !i_ok { return nil, nil, false }

		proj := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
		for entry in rows {
			vals := make([]types.Value, len(indices), context.temp_allocator)
			for idx, i in indices { vals[i] = entry.values[idx] }
			append(&proj, Row_Entry{entry.rowid, vals})
		}

		proj_cols := make([]types.Column, len(indices), context.temp_allocator)
		for idx, i in indices {
			proj_cols[i] = table.columns[idx]
			if i < len(stmt.aliases) && stmt.aliases[i] != "" {
				proj_cols[i].name = stmt.aliases[i]
			}
		}
		return proj[:], proj_cols, true
	}
	return rows, table.columns, true
}

exec_query :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	if _, is_nf := stmt.from.(parser.No_From); is_nf {
		return exec_select_literals(t, stmt)
	}

	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq {
		return exec_subquery_data(t, stmt, cache)
	}
	if len(stmt.joins) == 0 {
		return exec_select_single_data(t, stmt, cache)
	}
	return exec_select_join_data(t, stmt, cache)
}

// find_existing_group locates the group whose GROUP BY key equals row_entry's.
// group_map chains candidate group indices per hash (different keys can collide
// on the same hash), so every candidate is verified before deciding a new group
// is needed. Returns (-1, false) when no matching group exists.
find_existing_group :: proc(
	group_map: map[u64][dynamic]int,
	groups: []Group,
	hash: u64,
	row_entry: Row_Entry,
	group_by_indices: []int,
) -> (
	int,
	bool,
) {
	if bucket, ok := group_map[hash]; ok {
		for gi in bucket {
			if values_equal_by_indices(
				row_entry.values,
				groups[gi].key_values,
				group_by_indices,
			) {
				return gi, true
			}
		}
	}
	return -1, false
}

exec_select_aggregate_combined :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	group_by_indices := make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	group_map := make(map[u64][dynamic]int, context.temp_allocator)
	defer {
		for _, bucket in group_map { delete(bucket) }
		delete(group_map)
	}
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := find_existing_group(
				group_map,
				groups[:],
				hash,
				row_entry,
				group_by_indices,
			)
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for col_idx, pos in group_by_indices {
					key_vals[pos] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				bucket := group_map[hash]
				append(&bucket, len(groups))
				group_map[hash] = bucket
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}

	rows_mat := make([dynamic][]string, context.temp_allocator)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows { group_rows[ri] = row_entry.values }

		agg_vals := compute_aggregates(
			group_rows,
			stmt.aggregates,
			combined_cols,
			context.temp_allocator,
		)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) { continue }
		}

		row_strs := make([]string, len(stmt.columns), context.temp_allocator)
		val_idx := 0
		for _, i in stmt.columns {
			if val_idx < len(group_by_indices) {
				row_strs[i] = value_string(groups[gi].key_values[val_idx])
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				row_strs[i] = value_string(agg_vals[agg_idx])
				val_idx += 1
			}
		}
		append(&rows_mat, row_strs)
	}

	render_table(select_header_names(stmt), rows_mat[:])
	fmt.printf("(%d rows)\n", len(rows_mat))
	return true
}

// exec_select_aggregate_data evaluates a SELECT with aggregates/GROUP BY and returns
// the result rows as data (group key values followed by aggregate values), without
// printing. `cols` are synthesized from stmt.columns.
exec_select_aggregate_data :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	group_by_indices := make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return nil, nil, false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	group_map := make(map[u64][dynamic]int, context.temp_allocator)
	defer {
		for _, bucket in group_map { delete(bucket) }
		delete(group_map)
	}
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := find_existing_group(
				group_map,
				groups[:],
				hash,
				row_entry,
				group_by_indices,
			)
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for col_idx, pos in group_by_indices {
					key_vals[pos] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				bucket := group_map[hash]
				append(&bucket, len(groups))
				group_map[hash] = bucket
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}

	result := make([dynamic]Row_Entry, context.temp_allocator)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows { group_rows[ri] = row_entry.values }

		agg_vals := compute_aggregates(
			group_rows,
			stmt.aggregates,
			combined_cols,
			context.temp_allocator,
		)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) { continue }
		}

		out := make([]types.Value, len(stmt.columns), context.temp_allocator)
		val_idx := 0
		for i in 0 ..< len(stmt.columns) {
			if val_idx < len(group_by_indices) {
				out[i] = groups[gi].key_values[val_idx]
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				out[i] = agg_vals[agg_idx]
				val_idx += 1
			}
		}
		append(&result, Row_Entry{rowid = types.Row_ID(gi), values = out})
	}

	cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
	for name, i in stmt.columns {
		display := name
		if i < len(stmt.aliases) && stmt.aliases[i] != "" { display = stmt.aliases[i] }
		cols[i] = types.Column {name = display, type = .INTEGER}
	}
	return result[:], cols, true
}

// skip_op_from_token maps a comparison token to the skip-index operator used to
// derive a conservative page window from a zone map. Returns false for operators
// that cannot be answered from min/max ranges (e.g. NOT_EQUALS, IN, LIKE).
skip_op_from_token :: proc(op: parser.Token_Type) -> (btree.Skip_Op, bool) {
	#partial switch op {
	case .EQUALS:
		return .EQ, true
	case .LESS_THAN:
		return .LT, true
	case .LESS_EQUAL:
		return .LTE, true
	case .GREATER_THAN:
		return .GT, true
	case .GREATER_EQUAL:
		return .GTE, true
	}
	return .EQ, false
}

scan_table :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	where_clause: Maybe(parser.Where_Clause),
	max_rows: Maybe(u64),
	schema_tree: ^btree.Tree = nil,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	rows: []Row_Entry,
	err: bool,
) {
	r := make([dynamic]Row_Entry, allocator)
	where_ctx: Maybe(Where_Eval_Ctx)
	if wc, has_wc := where_clause.?; has_wc {
		where_ctx = init_where_ctx(&wc, table.columns, nil, schema_tree, allocator, cache)
	}

	use_where := false
	where_ctx_val: Where_Eval_Ctx
	if ctx, ok := where_ctx.?; ok && ctx.root != nil {
		use_where = true
		where_ctx_val = ctx
	}

	// Skip-index bounds are only safe for a top-level AND chain of single-column
	// integer comparisons on the indexed column. OR subtrees (or nested boolean
	// groups) disable skipping; the operator decides which side of the page
	// window a condition can bound (e.g. `>` only gives a lower bound, `<` only
	// an upper bound).
	skip_conds: []Resolved_Condition
	if use_where {
		skip_conds = skip_chain_conditions(where_ctx_val.root)
	}

	skip_start, skip_end: u32
	if use_where && table.skip_root > 0 {
		for rc in skip_conds {
			if rc.has_right_col || rc.has_in { continue }
			if val, is_int := rc.rhs.(i64); is_int {
				op, op_ok := skip_op_from_token(rc.operator)
				if !op_ok { continue }
				start, end, found := btree.query_skip_index_range(
					tree.pager,
					table.skip_root,
					rc.col_idx,
					op,
					val,
				)
				if found {
					if start > skip_start { skip_start = start }
					if end > 0 && (skip_end == 0 || end < skip_end) { skip_end = end }
				}
			}
		}
	}

	cursor, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None { return nil, true }
	if skip_start > 0 {
		if seek_err := btree.cursor_seek_to_page(&cursor, skip_start); seek_err != .None {
			// Stale/unreachable lower bound: fall back to a full scan (results stay correct).
			btree.cursor_destroy(&cursor)
			cursor, c_err = btree.cursor_start(tree, allocator)
			if c_err != .None { return nil, true }
		}
	}
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		if skip_end > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > skip_end { break }
		}

		c, get_err := btree.cursor_get_cell(&cursor, allocator)
		defer cell.destroy(&c, allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if use_where {
			if !evaluate_where_ctx(where_ctx_val, c.values) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		if limit, has_limit := max_rows.?; has_limit && u64(len(r)) >= limit { break }
		btree.cursor_advance(&cursor)
	}
	if use_where && table.skip_root == 0 && schema_tree != nil {
		for rc in skip_conds {
			if rc.has_right_col || rc.has_in { continue }
			if _, is_int := rc.rhs.(i64); !is_int { continue }
			if _, op_ok := skip_op_from_token(rc.operator); !op_ok { continue }

			col_idx := rc.col_idx
			skip_idx, build_err := btree.build_skip_index(tree, col_idx)
			if build_err == .None {
				new_schema_root, ok := schema.update_skip_root_cow(
					schema_tree,
					table.name,
					skip_idx.root,
				)
				if ok {
					schema_tree.root = new_schema_root
				}
			}
			break // only build for the first qualifying column
		}
	}
	return r[:], false
}

// skip_chain_conditions collects the leaf conditions of a top-level AND chain.
// Skipping is only safe when the predicate is a flat conjunction of comparisons;
// OR subtrees or nested boolean groups disable the optimization (empty result).
skip_chain_conditions :: proc(root: ^Resolved_Node) -> []Resolved_Condition {
	chain := make([dynamic]Resolved_Condition, context.temp_allocator)
	collect_skip_chain(root, &chain)
	return chain[:]
}

collect_skip_chain :: proc(node: ^Resolved_Node, out: ^[dynamic]Resolved_Condition) {
	if node == nil { return }
	switch node.kind {
	case .COND:
		append(out, node.cond)
	case .AND:
		for child in node.children {
			if child.kind != .COND {
				clear(out)
				return
			}
			append(out, child.cond)
		}
	case .OR:
		clear(out)
	case .NOT:
		clear(out)
	}
}
