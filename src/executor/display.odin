package executor

import "core:fmt"
import "core:log"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

print_agg_header :: proc(cols: []string) {
	b := strings.builder_make(context.temp_allocator)
	for col, i in cols {
		if i > 0 do strings.write_string(&b, " | ")
		strings.write_string(&b, col)
	}

	strings.write_byte(&b, '\n')
	for col, i in cols {
		if i > 0 do strings.write_string(&b, "-+-")
		for _ in 0 ..< len(col) do strings.write_byte(&b, '-')
	}
	fmt.println(strings.to_string(b))
}

display_results :: proc(
	rows: []Row_Entry,
	cols: []types.Column,
	display_indices: []int,
	limit, offset: Maybe(u64),
) {
	skip_count := u64(0)
	if off, has_off := offset.?; has_off { skip_count = off }

	limit_count := u64(0)
	has_limit := false
	if lim, has_lim := limit.?; has_lim {
		limit_count = lim
		has_limit = true
	}

	print_header(cols, display_indices)
	row_count := 0
	for entry in rows {
		if skip_count > 0 {
			skip_count -= 1
			continue
		}

		print_row(entry.values, display_indices)
		row_count += 1
		if has_limit && u64(row_count) >= limit_count { break }
	}
	fmt.printf("(%d rows)\n", row_count)
}

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

evaluate_where_having :: proc(
	clause: parser.Where_Clause,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	if len(clause.conditions) == 0 { return true }
	match := clause.is_and
	for cond in clause.conditions {
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
					name = "COUNT"
				case .SUM:
					name = "SUM"
				case .AVG:
					name = "AVG"
				case .MIN:
					name = "MIN"
				case .MAX:
					name = "MAX"
				}
				if cond.column == name && rhs_is_val {
					cond_result = compare_condition(agg_values[i], cond.operator, rhs_val)
					break
				}
			}
		}
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

print_header :: proc(cols: []types.Column, indices: []int) {
	b := strings.builder_make(context.temp_allocator)
	for idx, i in indices {
		if i > 0 do strings.write_string(&b, " | ")
		strings.write_string(&b, cols[idx].name)
	}

	strings.write_byte(&b, '\n')
	for _, i in indices {
		if i > 0 do strings.write_string(&b, "-+-")
		for _ in 0 ..< len(cols[indices[i]].name) {
			strings.write_byte(&b, '-')
		}
	}
	fmt.println(strings.to_string(b))
}

write_value_to_builder :: proc(b: ^strings.Builder, v: types.Value) {
	switch val in v {
	case types.Null:
		strings.write_string(b, "NULL")
	case i64:
		strings.write_i64(b, val)
	case f64:
		strings.write_f64(b, val, 'f')
	case string:
		strings.write_string(b, val)
	case []u8:
		strings.write_string(b, "<BLOB ")
		strings.write_int(b, len(val))
		strings.write_string(b, " bytes>")
	case:
		strings.write_string(b, "<?>")
	}
}

print_row :: proc(values: []types.Value, indices: []int) {
	b := strings.builder_make(context.temp_allocator)
	for idx, i in indices {
		if i > 0 do strings.write_string(&b, " | ")
		write_value_to_builder(&b, values[idx])
	}
	fmt.println(strings.to_string(b))
}
