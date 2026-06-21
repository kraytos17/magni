package executor

import "src:parser"
import "src:types"

filter_rows :: proc(
	rows: []Row_Entry,
	where_clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> []Row_Entry {
	filtered := make([dynamic]Row_Entry, context.temp_allocator)
	ctx, ctx_ok := init_where_ctx(where_clause, cols, table_ranges, context.temp_allocator).?
	if !ctx_ok { return filtered[:] }
	if len(ctx.conditions) == 0 { return rows }
	for entry in rows {
		if evaluate_where_ctx(ctx, entry.values) {
			append(&filtered, entry)
		}
	}
	return filtered[:]
}

init_where_ctx :: proc(
	clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	allocator := context.allocator,
) -> Maybe(Where_Eval_Ctx) {
	if len(clause.conditions) == 0 {
		return Where_Eval_Ctx{}
	}
	resolved := make([]Resolved_Condition, len(clause.conditions), allocator)
	for cond, i in clause.conditions {
		idx, found := resolve_qualified_column(cols, table_ranges, cond.column)
		if !found { return nil }
		rc := Resolved_Condition{
			col_idx = idx,
			operator = cond.operator,
			has_in = cond.operator == .IN,
		}
		if rhs_str, is_col := cond.rhs.(string); is_col {
			right_idx, rc_found := resolve_qualified_column(cols, table_ranges, rhs_str)
			if !rc_found { return nil }
			rc.has_right_col = true
			rc.right_idx = right_idx
		} else if val, is_val := cond.rhs.(types.Value); is_val {
			rc.rhs = val
		}
		if cond.in_values != nil {
			rc.in_values = cond.in_values
		}
		if cond.in_subquery != nil {
			rc.in_subquery = cond.in_subquery
		}
		resolved[i] = rc
	}
	return Where_Eval_Ctx{conditions = resolved, is_and = clause.is_and}
}

evaluate_where_ctx :: proc(ctx: Where_Eval_Ctx, row: []types.Value) -> bool {
	if len(ctx.conditions) == 0 { return true }
	match_res := ctx.is_and
	for rc in ctx.conditions {
		left_val := row[rc.col_idx]
		cond_result: bool
		if rc.has_right_col {
			cond_result = compare_condition(left_val, rc.operator, row[rc.right_idx])
		} else {
			cond_result = compare_condition(left_val, rc.operator, rc.rhs)
		}
		if rc.has_in && rc.in_values != nil {
			cond_result = false
			for v in rc.in_values {
				if compare_values(left_val, v) == 0 {
					cond_result = true; break
				}
			}
		} else if rc.has_in && rc.in_subquery != nil {
			cond_result = false
			subq_rows, _ := exec_subquery(nil, rc.in_subquery^)
			for sr in subq_rows {
				if len(sr.values) > 0 && compare_values(left_val, sr.values[0]) == 0 {
					cond_result = true; break
				}
			}
		}
		if ctx.is_and {
			match_res = match_res && cond_result
			if !match_res { return false }
		} else {
			match_res = match_res || cond_result
			if match_res { return true }
		}
	}
	return match_res
}

evaluate_where :: proc(
	clause: ^parser.Where_Clause,
	row: []types.Value,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	ctx, ok := init_where_ctx(clause, cols, table_ranges, context.temp_allocator).?
	if !ok { return false }
	return evaluate_where_ctx(ctx, row)
}

compare_condition :: proc(val: types.Value, op: parser.Token_Type, target: types.Value) -> bool {
	if op == .LIKE {
		text, text_ok := val.(string)
		pattern, pat_ok := target.(string)
		if !text_ok || !pat_ok { return false }
		return like_match(pattern, text)
	}
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

like_match :: proc(pattern: string, text: string) -> bool {
	pi := 0
	ti := 0
	star_pi := -1
	star_ti := -1
	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == text[ti] || pattern[pi] == '_') {
			pi += 1
			ti += 1
		} else if pi < len(pattern) && pattern[pi] == '%' {
			star_pi = pi
			star_ti = ti
			pi += 1
		} else if star_pi != -1 {
			pi = star_pi + 1
			star_ti += 1
			ti = star_ti
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi += 1
	}
	return pi == len(pattern)
}
