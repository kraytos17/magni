package executor

import "core:hash"
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

try_pk_lookup :: proc(
	table: types.Table,
	clause: parser.Where_Clause,
) -> (
	rowid: types.Row_ID,
	ok: bool,
) {
	if len(clause.conditions) != 1 { return }
	if !clause.is_and { return }

	cond := clause.conditions[0]
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

values_equal_by_indices :: proc(
	values: []types.Value,
	key: []types.Value,
	indices: []int,
) -> bool {
	if len(key) != len(indices) { return false }
	for i, col_idx in indices {
		if !types.value_compare(key[i], values[col_idx]) { return false }
	}
	return true
}

group_key_hash :: proc(values: []types.Value, indices: []int) -> u64 {
	h: u64 = 0xcbf29ce484222325
	FNV_PRIME :: 0x100000001b3
	for col_idx in indices {
		v := values[col_idx]
		switch val in v {
		case types.Null:
			h = h ~ 0
			h *= FNV_PRIME
		case i64:
			h = h ~ u64(val)
			h *= FNV_PRIME
		case f64:
			h = h ~ transmute(u64)val
			h *= FNV_PRIME
		case string:
			hv := hash.fnv64a(transmute([]u8)val)
			h = h ~ hv
			h *= FNV_PRIME
		case []u8:
			hv := hash.fnv64a(val)
			h = h ~ hv
			h *= FNV_PRIME
		}
	}
	return h
}

deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		new_values[i] = types.value_clone(v, context.temp_allocator) or_else types.Null{}
	}
	return new_values
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

	combined := make(
		[]types.Value,
		len(outer_row.values) + len(inner_values),
		context.temp_allocator,
	)

	copy(combined[:len(outer_row.values)], outer_row.values)
	copy(combined[len(outer_row.values):], inner_values)

	matched^ = true
	append(new_rows, Row_Entry{0, combined})
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
