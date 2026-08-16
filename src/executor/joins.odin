package executor

import "core:log"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

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



try_join_match :: proc(
	outer_row: Row_Entry,
	inner_values: []types.Value,
	jc: parser.Join_Clause,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	new_rows: ^[dynamic]Row_Entry,
	matched: ^bool,
) {
	if on_cl, has_on := jc.on_clause.?; has_on {
		tmp := make(
			[]types.Value,
			len(outer_row.values) + len(inner_values),
			context.temp_allocator,
		)

		copy(tmp[:len(outer_row.values)], outer_row.values)
		copy(tmp[len(outer_row.values):], inner_values)
		if !evaluate_where(&on_cl, tmp, combined_cols, table_ranges) { return }
	}

	matched^ = true
	join_emit_combined(outer_row, inner_values, new_rows)
}

