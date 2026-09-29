package executor

import "core:log"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

@(private)
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

// build_groups resolves GROUP BY column indices and partitions rows into
// groups (a single implicit group when there is no GROUP BY). Shared by the
// printing and data aggregate evaluators.
@(private)
build_groups :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	groups: [dynamic]Group,
	group_by_indices: []int,
	ok: bool,
) {
	group_by_indices = make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return nil, nil, false
		}
		group_by_indices[i] = idx
	}
	// Unknown aggregate arguments (e.g. SUM(nosuchcol)) are a clean error,
	// like unknown WHERE/GROUP BY columns — never silent NULLs. COUNT(*)
	// takes no column and always resolves.
	for agg in stmt.aggregates {
		if agg.column == "" { continue }
		if _, col_ok := resolve_qualified_column(combined_cols, table_ranges, agg.column); !col_ok {
			log.errorf("Error: Unknown column in aggregate: %s", agg.column)
			return nil, nil, false
		}
	}

	groups = make([dynamic]Group, context.temp_allocator)
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
	if len(groups) == 0 && len(group_by_indices) == 0 {
		append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
	}
	return groups, group_by_indices, true
}

// exec_select_aggregate_data evaluates a SELECT with aggregates/GROUP BY and returns
// the result rows as data (group key values followed by aggregate values), without
// printing. `cols` are synthesized from stmt.columns.
@(private)
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
	groups, group_by_indices, g_ok := build_groups(stmt, rows, combined_cols, table_ranges)
	if !g_ok { return nil, nil, false }

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
				// A bare literal (e.g. SELECT 0, COUNT(*) ...) occupies a
				// column slot with no corresponding aggregate value — that
				// is a clean error, not an out-of-range index.
				agg_idx := val_idx - len(group_by_indices)
				if agg_idx < 0 || agg_idx >= len(agg_vals) {
					log.errorf("Error: Cannot mix non-aggregate column '%s' with aggregates", stmt.columns[i])
					return nil, nil, false
				}

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

// group_key_hash computes a hash over GROUP BY key columns. Implemented via
// hash_values (single FNV-1a with per-type tags); collisions fall back to
// value_compare at the call site.
group_key_hash :: proc(values: []types.Value, indices: []int) -> u64 {
	return hash_values(values, indices)
}

@(fast_math = {.No_NaNs, .No_Infs, .No_Signed_Zeros})
@(private)
compare_values :: proc(a: types.Value, b: types.Value) -> int {
	if types.is_null(a) && types.is_null(b) do return 0
	if types.is_null(a) do return -1
	if types.is_null(b) do return 1
	#partial switch va in a {
	case i64:
		#partial switch vb in b {
		case i64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case f64:
			if f64(va) < vb do return -1
			if f64(va) > vb do return 1
			return 0
		}
	case f64:
		#partial switch vb in b {
		case f64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case i64:
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

@(private)
value_string :: proc(v: types.Value) -> string {
	return types.value_to_string(v)
}

@(private)
build_display_indices :: proc(
	columns: []string,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	total_cols: int,
) -> (
	[]int,
	bool,
) {
	indices := make([dynamic]int, context.temp_allocator)
	if len(columns) == 0 {
		for i in 0 ..< total_cols {
			append(&indices, i)
		}
	} else {
		for req_col in columns {
			idx, ok := resolve_qualified_column(cols, table_ranges, req_col)
			if !ok {
				log.errorf("Error: Unknown column: %s", req_col)
				return nil, false
			}
			append(&indices, idx)
		}
	}
	return indices[:], true
}

@(fast_math = {
	.Allow_Reassoc,
	.No_NaNs,
	.No_Infs,
	.No_Signed_Zeros,
	.Allow_Reciprocal,
	.Allow_Contract,
	.Approx_Func,
})
@(private)
compute_aggregates :: proc(
	rows: [][]types.Value,
	aggregates: []parser.Aggregate_Expr,
	columns: []types.Column,
	allocator := context.temp_allocator,
) -> []types.Value {
	results := make([]types.Value, len(aggregates), allocator)
	for agg, i in aggregates {
		col_idx := -1
		if agg.column != "" {
			found: bool
			col_idx, found = schema.find_column_index(columns, agg.column)
			if !found { col_idx = -1 }
		}

		switch agg.func {
		case .COUNT:
			if agg.column == "" {
				results[i] = types.value_int(i64(len(rows)))
			} else {
				count := 0
				for row_vals in rows {
					if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
						count += 1
					}
				}
				results[i] = types.value_int(i64(count))
			}
		case .SUM:
			sum: f64
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					#partial switch v in row_vals[col_idx] {
					case i64:
						sum += f64(v)
					case f64:
						sum += v
					}
				}
			}
			results[i] = types.value_real(sum)
		case .AVG:
			sum: f64
			count := 0
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					#partial switch v in row_vals[col_idx] {
					case i64:
						sum += f64(v); count += 1
					case f64:
						sum += v; count += 1
					}
				}
			}
			if count > 0 {
				results[i] = types.value_real(sum / f64(count))
			} else {
				results[i] = types.value_null()
			}
		case .MIN:
			if len(rows) == 0 || col_idx < 0 {
				results[i] = types.value_null()
				break
			}

			min := rows[0][col_idx]
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					if compare_values(row_vals[col_idx], min) < 0 {
						min = row_vals[col_idx]
					}
				}
			}
			results[i] = min
		case .MAX:
			if len(rows) == 0 || col_idx < 0 {
				results[i] = types.value_null()
				break
			}

			max := rows[0][col_idx]
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					if compare_values(row_vals[col_idx], max) > 0 {
						max = row_vals[col_idx]
					}
				}
			}
			results[i] = max
		}
	}
	return results
}

@(private)
evaluate_where_having :: proc(
	clause: parser.Where_Clause,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	if clause.root == nil { return true }
	return evaluate_having_node(clause.root, group_keys, agg_values, group_cols, aggregates)
}

// resolve_having_value finds the group-key or aggregate value a HAVING
// condition names, without requiring an rhs. Shared lookup for the IS NULL
// branch; the comparison path keeps its inline form.
@(private="file")
resolve_having_value :: proc(
	cond: parser.Condition,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> (types.Value, bool) {
	for col, i in group_cols {
		if col == cond.column {
			return group_keys[i], true
		}
	}
	for agg, i in aggregates {
		name := ""
		switch agg.func {
		case .COUNT:
			name = "count"
		case .SUM:
			name = "sum"
		case .AVG:
			name = "avg"
		case .MIN:
			name = "min"
		case .MAX:
			name = "max"
		}
		// Compare case-insensitively: HAVING count and HAVING COUNT both
		// reference the COUNT aggregate (cond.column is stored as written).
		if strings.to_lower(cond.column, context.temp_allocator) == name {
			return agg_values[i], true
		}
	}
	return {}, false
}

@(private="file")
evaluate_having_node :: proc(
	node: ^parser.Where_Node,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	switch node.kind {
	case .COND:
		return evaluate_having_condition(node.cond, group_keys, agg_values, group_cols, aggregates)
	case .AND:
		for child in node.children {
			if !evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates) {
				return false
			}
		}
		return true
	case .OR:
		for child in node.children {
			if evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates) {
				return true
			}
		}
		return false
	case .NOT:
		for child in node.children {
			return !evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates)
		}
		return false
	}
	return false
}

@(private="file")
evaluate_having_condition :: proc(
	cond: parser.Condition,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	// IS [NOT] NULL tests nullness of the resolved value instead of
	// comparing against an rhs (which IS conditions never carry).
	if cond.operator == .IS {
		cond_result := false
		if v, ok := resolve_having_value(cond, group_keys, agg_values, group_cols, aggregates); ok {
			cond_result = types.is_null(v)
		}
		if cond.negated { cond_result = !cond_result }
		return cond_result
	}

	cond_result := false
	rhs_val, rhs_is_val := cond.rhs.(types.Value)
	for col, i in group_cols {
		if col == cond.column {
			if rhs_is_val {
				cond_result = compare_condition(group_keys[i], cond.operator, rhs_val)
			}
			break
		}
	}
	if !cond_result {
		for agg, i in aggregates {
			name := ""
			switch agg.func {
			case .COUNT:
				name = "count"
			case .SUM:
				name = "sum"
			case .AVG:
				name = "avg"
			case .MIN:
				name = "min"
			case .MAX:
				name = "max"
			}
			// Compare case-insensitively: HAVING count > 1 and HAVING COUNT > 1
			// both reference the COUNT aggregate (cond.column is stored as written).
			if strings.to_lower(cond.column, context.temp_allocator) == name && rhs_is_val {
				cond_result = compare_condition(agg_values[i], cond.operator, rhs_val)
				break
			}
		}
	}
	if cond.negated { cond_result = !cond_result }
	return cond_result
}
