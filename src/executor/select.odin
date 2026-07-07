package executor

import "core:fmt"
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

exec_subquery :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> ([]Row_Entry, []types.Column) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Subquery table not found:", tbl_name)
		return nil, nil
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	rows, scan_err := scan_table(&table_tree, &table, nil, nil, t, context.temp_allocator)
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

exec_select :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq { return exec_select_subquery(t, stmt) }
	if len(stmt.joins) == 0 { return exec_select_single(t, stmt) }

	// Phase 1: Resolve all table sources and build column ranges
	table_count := 1 + len(stmt.joins)
	table_ctxs := make([]Table_Context, table_count, context.temp_allocator)
	table_ranges := make([]Table_Col_Range, table_count, context.temp_allocator)
	info: types.Table
	found: bool
	t0_alias: string
	col_count_0 := 0
	if tbl_name, is_table := stmt.from.(string); is_table {
		info, found = schema.get_table(t, tbl_name, context.temp_allocator)
		if !found { fmt.eprintln("Error: Table not found:", tbl_name); return false }

		table_ctxs[0] = Table_Context {
			info = {table = info, tree = btree.init(t.pager, info.root_page)},
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
		inner_rows, inner_cols := exec_subquery(t, vt^)
		if inner_rows == nil { return false }

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
			info, found = schema.get_table(t, jt_name, context.temp_allocator)
			if !found {
				fmt.eprintln("Error: Table not found:", jt_name)
				return false
			}

			prev := table_ctxs[idx - 1].range
			alias := join.alias if join.alias != "" else jt_name
			table_ctxs[idx] = Table_Context {
				info = {table = info, tree = btree.init(t.pager, info.root_page)},
				range = {
					table_name = alias,
					start_col = prev.start_col + prev.col_count,
					col_count = len(info.columns),
				},
			}

			table_ranges[idx] = table_ctxs[idx].range
		} else if subq, is_subquery := join.source.(^parser.Select_Stmt); is_subquery {
			inner_rows, inner_cols := exec_subquery(t, subq^)
			if inner_rows == nil { return false }

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

	// Phase 2: Scan first table (or materialize subquery)
	rows: []Row_Entry
	if col_count_0 > 0 {
		r, scan_err := scan_table(
			&table_ctxs[0].info.tree,
			&table_ctxs[0].info.table,
			nil,
			nil,
			t,
			context.temp_allocator,
		)
		if scan_err { return false }
		rows = r
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		rows = vt.rows
	}

	// Phase 3: Process JOINs
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
			)
		}

		// Phase 4: Hash join (single equi-condition) or nested-loop fallback
		hash_used := false
		if on_cl, has_on := jc.on_clause.?; has_on && len(on_cl.conditions) == 1 {
			cond := on_cl.conditions[0]
			if cond.operator == .EQUALS {
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
						left_adjust := 0
						right_adjust := table_ctxs[info_idx].range.start_col
						key_is_int := false
						if len(rows) > 0 && len(right_rows) > 0 {
							if _, ok := rows[0].values[left_idx - left_adjust].(i64);
							   ok { key_is_int = true }
						}
						if key_is_int {
							if len(rows) <= len(right_rows) {
								ht := make(map[i64][dynamic]int, len(rows), context.temp_allocator)
								for row, ri in rows {
									key, key_ok := row.values[left_idx - left_adjust].(i64)
									if !key_ok { continue }

									bucket := ht[key]
									append(&bucket, ri)
									ht[key] = bucket
								}

								matched_left := make(
									map[int]bool,
									len(rows),
									context.temp_allocator,
								)
								for r_row in right_rows {
									key, key_ok := r_row.values[right_idx - right_adjust].(i64)
									if !key_ok { continue }
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[ri] = true
											combined := make(
												[]types.Value,
												len(rows[ri].values) + len(r_row.values),
												context.temp_allocator,
											)

											copy(combined[:len(rows[ri].values)], rows[ri].values)
											copy(combined[len(rows[ri].values):], r_row.values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}

								for _, bucket in ht { delete(bucket) }
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make(
											[]types.Value,
											len(rows[li].values) + right_col_count,
											context.temp_allocator,
										)

										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 {
											null_row[k] = types.value_null()
										}
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							} else {
								ht := make(
									map[i64][dynamic]int,
									len(right_rows),
									context.temp_allocator,
								)
								for r_row, ri in right_rows {
									key, key_ok := r_row.values[right_idx - right_adjust].(i64)
									if !key_ok { continue }

									bucket := ht[key]
									append(&bucket, ri)
									ht[key] = bucket
								}

								matched_left := make(
									map[int]bool,
									len(rows),
									context.temp_allocator,
								)
								for l_row, li in rows {
									key, key_ok := l_row.values[left_idx - left_adjust].(i64)
									if !key_ok { continue }
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[li] = true
											combined := make(
												[]types.Value,
												len(l_row.values) + len(right_rows[ri].values),
												context.temp_allocator,
											)

											copy(combined[:len(l_row.values)], l_row.values)
											copy(
												combined[len(l_row.values):],
												right_rows[ri].values,
											)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}

								for _, bucket in ht { delete(bucket) }
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make(
											[]types.Value,
											len(rows[li].values) + right_col_count,
											context.temp_allocator,
										)

										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 {
											null_row[k] = types.value_null()
										}
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							}
						} else {
							if len(rows) <= len(right_rows) {
								ht := make(
									map[string][dynamic]int,
									len(rows),
									context.temp_allocator,
								)
								for row, ri in rows {
									key := hash_join_key(row.values[left_idx - left_adjust])
									bucket := ht[key]
									append(&bucket, ri)
									ht[key] = bucket
								}

								matched_left := make(
									map[int]bool,
									len(rows),
									context.temp_allocator,
								)
								for r_row in right_rows {
									key := hash_join_key(r_row.values[right_idx - right_adjust])
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[ri] = true
											combined := make(
												[]types.Value,
												len(rows[ri].values) + len(r_row.values),
												context.temp_allocator,
											)

											copy(combined[:len(rows[ri].values)], rows[ri].values)
											copy(combined[len(rows[ri].values):], r_row.values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}

								for _, bucket in ht { delete(bucket) }
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make(
											[]types.Value,
											len(rows[li].values) + right_col_count,
											context.temp_allocator,
										)

										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values);
										    k < len(null_row);
										    k += 1 { null_row[k] = types.value_null() }
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							} else {
								ht := make(
									map[string][dynamic]int,
									len(right_rows),
									context.temp_allocator,
								)
								for r_row, ri in right_rows {
									key := hash_join_key(r_row.values[right_idx - right_adjust])
									bucket := ht[key]
									append(&bucket, ri)
									ht[key] = bucket
								}

								matched_left := make(
									map[int]bool,
									len(rows),
									context.temp_allocator,
								)
								for l_row, li in rows {
									key := hash_join_key(l_row.values[left_idx - left_adjust])
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[li] = true
											combined := make(
												[]types.Value,
												len(l_row.values) + len(right_rows[ri].values),
												context.temp_allocator,
											)

											copy(combined[:len(l_row.values)], l_row.values)
											copy(
												combined[len(l_row.values):],
												right_rows[ri].values,
											)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}

								for _, bucket in ht { delete(bucket) }
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left {
											continue
										}

										null_row := make(
											[]types.Value,
											len(rows[li].values) + right_col_count,
											context.temp_allocator,
										)

										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 {
											null_row[k] = types.value_null()
										}
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							}
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
						null_row := make(
							[]types.Value,
							len(outer_row.values) + right_col_count,
							context.temp_allocator,
						)

						copy(null_row[:len(outer_row.values)], outer_row.values)
						for k in len(outer_row.values) ..< len(null_row) {
							null_row[k] = types.value_null()
						}
						append(&new_rows, Row_Entry{0, null_row})
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
	if len(stmt.aggregates) > 0 {
		return exec_select_aggregate_combined(stmt, rows, combined_cols, table_ranges)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		combined_cols,
		table_ranges,
		total_cols,
	)

	if !ok { return false }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) { return false }
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }
	display_results(rows, combined_cols, display_indices[:], stmt.limit, stmt.offset)
	return true
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
		if !sort_rows(rows[:], order_clause, virtual_cols, single_range) { return false }
	}
	display_results(rows[:], virtual_cols, display_indices[:], stmt.limit, stmt.offset)
	return true
}

exec_select_single :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found { fmt.eprintln("Error: Table not found:", tbl_name); return false }
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
	   !stmt.is_distinct &&
	   !has_order &&
	   stmt.limit == nil &&
	   stmt.offset == nil {
		count, count_err := btree.tree_count_rows(&table_tree)
		if count_err != .None {
			fmt.eprintln("Error: Failed to count rows")
			return false
		}

		print_agg_header(stmt.columns)
		for _, i in stmt.columns {
			if i > 0 do fmt.print(" | ")
			fmt.print(i64(count))
		}

		fmt.println()
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
	if len(stmt.aggregates) > 0 {
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
	display_results(rows, table.columns, display_indices, stmt.limit, stmt.offset)
	return true
}

exec_select_single_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil, false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", tbl_name)
		return nil, nil, false
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 { has_order = true }

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
	if scan_err { return nil, nil, false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
	}

	if len(stmt.aggregates) > 0 {
		exec_select_aggregate_combined(stmt, rows, table.columns, single_range)
		return nil, nil, true
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
	return rows, table.columns, true
}

exec_query :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq {
		inner_rows, inner_cols := exec_subquery(t, stmt)
		return inner_rows, inner_cols, inner_rows != nil
	}
	if len(stmt.joins) == 0 {
		return exec_select_single_data(t, stmt)
	}
	ok := exec_select(t, stmt)
	return nil, nil, ok
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
			fmt.eprintln("Error: Unknown column in GROUP BY:", col)
			return false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	group_map := make(map[u64]int, context.temp_allocator)
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := group_map[hash]
			if exists {
				if !values_equal_by_indices(
					row_entry.values,
					groups[gi].key_values,
					group_by_indices,
				) {
					exists = false
				}
			}
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for i, col_idx in group_by_indices {
					key_vals[i] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				group_map[hash] = len(groups)
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}

	print_agg_header(stmt.columns)
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

		val_idx := 0
		for _, i in stmt.columns {
			if i > 0 do fmt.print(" | ")
			if val_idx < len(group_by_indices) {
				fmt.print(types.value_to_string(groups[gi].key_values[val_idx]))
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				fmt.print(types.value_to_string(agg_vals[agg_idx]))
				val_idx += 1
			}
		}
		fmt.println()
	}
	fmt.printf("(%d rows)\n", len(groups))
	return true
}

scan_table :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	where_clause: Maybe(parser.Where_Clause),
	max_rows: Maybe(u64),
	schema_tree: ^btree.Tree = nil,
	allocator := context.allocator,
) -> (
	rows: []Row_Entry,
	err: bool,
) {
	r := make([dynamic]Row_Entry, allocator)
	cursor, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None { return nil, true }
	defer btree.cursor_destroy(&cursor)

	where_ctx: Maybe(Where_Eval_Ctx)
	if wc, has_wc := where_clause.?; has_wc {
		where_ctx = init_where_ctx(&wc, table.columns, nil, schema_tree, allocator)
	}
	// Populate skip index bounds from the table's skip index
	if ctx, ctx_ok := where_ctx.?; ctx_ok && table.skip_root > 0 {
		for i in 0 ..< len(ctx.conditions) {
			rc := &ctx.conditions[i]
			if rc.has_right_col || rc.has_in { continue }
			if val, is_int := rc.rhs.(i64); is_int {
				pmin, pmax, found := btree.query_skip_index(tree.pager, table.skip_root, val)
				if found {
					rc.skip_page_min = pmin; rc.skip_page_max = pmax
				}
			}
		}
	}

	use_where := false
	where_ctx_val: Where_Eval_Ctx
	if ctx, ok := where_ctx.?; ok && len(ctx.conditions) > 0 {
		use_where = true
		where_ctx_val = ctx
	}

	skip_min, skip_max: u32
	if use_where {
		for rc in where_ctx_val.conditions {
			if rc.skip_page_min > 0 {
				if skip_min == 0 || rc.skip_page_min < skip_min { skip_min = rc.skip_page_min }
			}
			if rc.skip_page_max > 0 {
				if rc.skip_page_max > skip_max { skip_max = rc.skip_page_max }
			}
		}
	}
	for cursor.is_valid {
		if skip_max > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > skip_max { break }
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
		for rc in where_ctx_val.conditions {
			if rc.has_right_col || rc.has_in { continue }
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
					table.skip_root = skip_idx.root
				}
			}
			break // only build for the first qualifying column
		}
	}
	return r[:], false
}
