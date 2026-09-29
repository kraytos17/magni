package executor

import "core:log"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

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
			if len(rows) == 0 { results[i] = types.value_null(); break }
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
			if len(rows) == 0 { results[i] = types.value_null(); break }
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
