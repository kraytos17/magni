package executor

import "core:log"
import "core:strconv"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

resolve_qualified_column :: proc(
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	name: string,
) -> (
	int,
	bool,
) {
	if len(table_ranges) > 0 {
		if dot_pos := strings.last_index_byte(name, '.'); dot_pos >= 0 {
			table_part := name[:dot_pos]
			col_part := name[dot_pos + 1:]
			for tr in table_ranges {
				if tr.table_name == table_part {
					end := tr.start_col + tr.col_count
					for i in tr.start_col ..< end {
						if combined_cols[i].name == col_part {
							return i, true
						}
					}
				}
			}
			return -1, false
		}
	}
	return schema.find_column_index(combined_cols, name)
}

// where_single_condition returns the lone leaf condition when the clause tree is
// exactly one comparison (used by the PK fast-path and hash-join optimization).

where_single_condition :: proc(clause: parser.Where_Clause) -> (parser.Condition, bool) {
	root := clause.root
	if root == nil || root.kind != .COND { return {}, false }
	return root.cond, true
}


try_pk_lookup :: proc(
	table: types.Table,
	clause: parser.Where_Clause,
) -> (
	rowid: types.Row_ID,
	ok: bool,
) {
	cond, has_cond := where_single_condition(clause)
	if !has_cond { return }
	if cond.operator != .EQUALS { return }

	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if !has_pk { return }
	if table.columns[pk_idx].name != cond.column { return }

	val, is_int := cond.rhs.(types.Value).(i64)
	if !is_int { return }
	return types.Row_ID(val), true
}


values_equal :: proc(a, b: []types.Value) -> bool {
	if len(a) != len(b) { return false }
	for v, i in a {
		if !types.value_compare(v, b[i]) { return false }
	}
	return true
}

// values_equal_by_indices compares the key values (extracted at `indices`) of two rows.

values_equal_by_indices :: proc(
	values: []types.Value,
	key: []types.Value,
	indices: []int,
) -> bool {
	if len(key) != len(indices) { return false }
	for col_idx, pos in indices {
		if !types.value_compare(key[pos], values[col_idx]) { return false }
	}
	return true
}


deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		new_values[i] = types.value_clone(v, context.temp_allocator) or_else types.Null{}
	}
	return new_values
}


check_constraints :: proc(values: []types.Value, table: types.Table) -> bool {
	for col in table.columns {
		if chk, has_chk := col.check_expr.?; has_chk {
			parts := strings.split(chk, " ", context.temp_allocator)
			if len(parts) < 3 {
				log.errorf("Error: CHECK constraint too complex: %s", chk)
				return false
			}

			col_idx, col_ok := resolve_qualified_column(table.columns, nil, parts[0])
			if !col_ok {
				log.errorf("Error: CHECK references unknown column: %s", parts[0])
				return false
			}

			left_val := values[col_idx]
			op_token := parts[1]
			val_num, parse_num := strconv.parse_i64(parts[2])
			if !parse_num {
				log.errorf("Error: CHECK constraint non-integer comparison: %s", chk)
				return false
			}

			left_i64, is_int := left_val.(i64)
			if !is_int {
				log.errorf("Error: CHECK column value is not an integer: %s", chk)
				return false
			}

			result := false
			if op_token == ">" {
				result = left_i64 > val_num
			} else if op_token == "<" {
				result = left_i64 < val_num
			} else if op_token == ">=" {
				result = left_i64 >= val_num
			} else if op_token == "<=" {
				result = left_i64 <= val_num
			} else if op_token == "=" {
				result = left_i64 == val_num
			} else if op_token == "!=" || op_token == "<>" {
				result = left_i64 != val_num
			} else {
				log.errorf("CHECK uses unsupported operator: %s", op_token)
				return false
			}
			if !result {
				log.errorf("CHECK constraint violation: %s", chk)
				return false
			}
		}
	}
	return true
}

@(fast_math = {.No_NaNs, .No_Infs, .No_Signed_Zeros})

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

