package executor

import "core:hash"
import "core:log"
import "core:strconv"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

// FNV-1a constants shared by hash_values.
@(private="file")
FNV_OFFSET_BASIS :: u64(0xcbf29ce484222325)

@(private="file")
FNV_PRIME :: u64(0x100000001b3)

// hash_values computes a single FNV-1a hash over a row's values (or a subset
// via indices; nil indices hashes all values). Each value is prefixed with a
// fixed tag byte so that e.g. integer 1 and string "1" hash differently and
// column boundaries are unambiguous. Used for DISTINCT dedup, set-operation
// membership, GROUP BY keys, and hash-join keys. Collisions fall back to
// types.value_compare at every call site — this is a hash-map key, not a digest.
@(private)
hash_values :: proc(values: []types.Value, indices: []int = nil) -> u64 {
	h := FNV_OFFSET_BASIS
	if indices == nil {
		for v in values {
			switch val in v {
			case types.Null:
				h = fnv_mix(h, 0)
			case i64:
				h = fnv_mix(h, 1)
				h = fnv_mix(h, u64(val))
			case f64:
				h = fnv_mix(h, 2)
				h = fnv_mix(h, transmute(u64)val)
			case string:
				h = fnv_mix(h, 3)
				h = hash.fnv64a(transmute([]u8)val, h)
			case []u8:
				h = fnv_mix(h, 4)
				h = hash.fnv64a(val, h)
			}
		}
	} else {
		for col_idx in indices {
			v := values[col_idx]
			switch val in v {
			case types.Null:
				h = fnv_mix(h, 0)
			case i64:
				h = fnv_mix(h, 1)
				h = fnv_mix(h, u64(val))
			case f64:
				h = fnv_mix(h, 2)
				h = fnv_mix(h, transmute(u64)val)
			case string:
				h = fnv_mix(h, 3)
				h = hash.fnv64a(transmute([]u8)val, h)
			case []u8:
				h = fnv_mix(h, 4)
				h = hash.fnv64a(val, h)
			}
		}
	}
	return h
}

@(private="file")
fnv_mix :: proc(h, w: u64) -> u64 {
	return (h ~ w) * FNV_PRIME
}

// hash_value computes the FNV-1a hash of a single value, using the same
// per-type tags as hash_values. Used for hash-join keys.
@(private)
hash_value :: proc(v: types.Value) -> u64 {
	h := FNV_OFFSET_BASIS
	switch val in v {
	case types.Null:
		h = fnv_mix(h, 0)
	case i64:
		h = fnv_mix(h, 1)
		h = fnv_mix(h, u64(val))
	case f64:
		h = fnv_mix(h, 2)
		h = fnv_mix(h, transmute(u64)val)
	case string:
		h = fnv_mix(h, 3)
		h = hash.fnv64a(transmute([]u8)val, h)
	case []u8:
		h = fnv_mix(h, 4)
		h = hash.fnv64a(val, h)
	}
	return h
}

@(private)
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
@(private)
where_single_condition :: proc(clause: parser.Where_Clause) -> (parser.Condition, bool) {
	root := clause.root
	if root == nil || root.kind != .COND { return {}, false }
	return root.cond, true
}

@(private)
try_pk_lookup :: proc(
	table: types.Table,
	clause: parser.Where_Clause,
	table_name: string = "",
	table_alias: string = "",
) -> (
	rowid: types.Row_ID,
	ok: bool,
) {
	cond, has_cond := where_single_condition(clause)
	if !has_cond { return }
	if cond.operator != .EQUALS { return }

	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if !has_pk { return }

	pk_name := table.columns[pk_idx].name
	if cond.column != pk_name {
		qual, col, has_qual := split_qualifier(cond.column)
		if !has_qual || col != pk_name { return }

		matches := qual == table_name || (table_alias != "" && qual == table_alias)
		if !matches { return }
	}

	val, is_int := cond.rhs.(types.Value).(i64)
	if !is_int { return }
	return types.Row_ID(val), true
}

// split_qualifier splits "t.col" into ("t", "col"); has=false when unqualified.
@(private="file")
split_qualifier :: proc(name: string) -> (qual: string, col: string, has: bool) {
	if i := strings.last_index_byte(name, '.'); i >= 0 {
		return name[:i], name[i + 1:], true
	}
	return "", name, false
}

@(private)
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

@(private)
deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		new_values[i] = types.value_clone(v, context.temp_allocator) or_else types.Null{}
	}
	return new_values
}

// Parsed_Check is a CHECK expression decomposed into its parts:
// `col <op> int_literal`. Parsing a raw check_expr string is split out so the
// evaluator is a clean comparison instead of an inline string split.
Parsed_Check :: struct {
	col_name: string, // resolved against the table's columns
	op:       Check_Op,
	val:      i64,
}

Check_Op :: enum u8 {
	GT,
	LT,
	GTE,
	LTE,
	EQ,
	NE,
}

// parse_check_expr decomposes a raw `col <op> int` CHECK string. Returns
// ok=false (already logged) on malformed input or an unsupported operator.
@(private)
parse_check_expr :: proc(chk: string) -> (Parsed_Check, bool) {
	parts := strings.split(chk, " ", context.temp_allocator)
	if len(parts) < 3 {
		log.errorf("Error: CHECK constraint too complex: %s", chk)
		return {}, false
	}

	op, op_ok := check_op_from_token(parts[1])
	if !op_ok {
		log.errorf("CHECK uses unsupported operator: %s", parts[1])
		return {}, false
	}

	val, parse_num := strconv.parse_i64(parts[2])
	if !parse_num {
		log.errorf("Error: CHECK constraint non-integer comparison: %s", chk)
		return {}, false
	}
	return Parsed_Check{col_name = parts[0], op = op, val = val}, true
}

@(private="file")
check_op_from_token :: proc(tok: string) -> (Check_Op, bool) {
	switch tok {
	case ">":
		return .GT, true
	case "<":
		return .LT, true
	case ">=":
		return .GTE, true
	case "<=":
		return .LTE, true
	case "=":
		return .EQ, true
	case "!=", "<>":
		return .NE, true
	}
	return .EQ, false
}

@(private="file")
check_op_eval :: proc(op: Check_Op, left, right: i64) -> bool {
	switch op {
	case .GT:
		return left > right
	case .LT:
		return left < right
	case .GTE:
		return left >= right
	case .LTE:
		return left <= right
	case .EQ:
		return left == right
	case .NE:
		return left != right
	}
	return false
}

@(private)
check_constraints :: proc(values: []types.Value, table: types.Table) -> bool {
	for col in table.columns {
		if chk, has_chk := col.check_expr.?; has_chk {
			parsed, p_ok := parse_check_expr(chk)
			if !p_ok { return false }

			col_idx, col_ok := resolve_qualified_column(table.columns, nil, parsed.col_name)
			if !col_ok {
				log.errorf("Error: CHECK references unknown column: %s", parsed.col_name)
				return false
			}

			left_i64, is_int := values[col_idx].(i64)
			if !is_int {
				log.errorf("Error: CHECK column value is not an integer: %s", chk)
				return false
			}
			if !check_op_eval(parsed.op, left_i64, parsed.val) {
				log.errorf("CHECK constraint violation: %s", chk)
				return false
			}
		}
	}
	return true
}
