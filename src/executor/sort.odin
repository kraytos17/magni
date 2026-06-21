package executor

import "core:fmt"
import "core:hash"
import "core:slice"
import "core:strings"
import "src:parser"
import "src:types"

dedup_rows :: proc(rows: []Row_Entry) -> []Row_Entry {
	if len(rows) <= 1 { return rows }
	seen := make(map[u64]bool, len(rows), context.temp_allocator)
	result := make([dynamic]Row_Entry, context.temp_allocator)
	for r in rows {
		key_b := strings.builder_make(context.temp_allocator)
		for v, i in r.values {
			if i > 0 { strings.write_byte(&key_b, '\x00') }
			strings.write_string(&key_b, types.value_to_string(v))
		}
		fp := hash.fnv64(transmute([]u8)strings.to_string(key_b))
		if fp not_in seen {
			seen[fp] = true
			append(&result, r)
		} else {
			is_dup := false
			for row in result {
				if len(row.values) != len(r.values) { continue }
				all_eq := true
				for j in 0 ..< len(r.values) {
					if !types.value_compare(r.values[j], row.values[j]) {
						all_eq = false
						break
					}
				}
				if all_eq {
					is_dup = true
					break
				}
			}
			if !is_dup {
				append(&result, r)
			}
		}
	}
	return result[:]
}

sort_rows :: proc(
	rows: []Row_Entry,
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	sort_indices := make([]int, len(order_clause), context.temp_allocator)
	for o, i in order_clause {
		idx, col_ok := resolve_qualified_column(cols, table_ranges, o.column)
		if !col_ok {
			fmt.eprintln("Error: Unknown column in ORDER BY:", o.column)
			return false
		}
		sort_indices[i] = idx
	}
	if len(order_clause) == 1 && len(rows) > 1 {
		sort_idx := sort_indices[0]
		all_int := true
		keys := make([]i64, len(rows), context.temp_allocator)
		for row, i in rows {
			if iv, ok := row.values[sort_idx].(i64); ok {
				keys[i] = iv
			} else {
				all_int = false
				break
			}
		}
		if all_int {
			desc := order_clause[0].desc
			nulls_first := order_clause[0].nulls_first
			if !nulls_first { nulls_first = desc }
			idx := make([]int, len(rows), context.temp_allocator)
			for i in 0 ..< len(rows) { idx[i] = i }
			slice.sort_by_with_data(idx, proc(a, b: int, data: rawptr) -> bool {
				k := (^[]i64)(data)
				return k[a] < k[b]
			}, &keys)
			sorted := make([]Row_Entry, len(rows), context.temp_allocator)
			if desc || nulls_first {
				for pi, i in idx {
					sorted[len(rows) - 1 - i] = rows[pi]
				}
			} else {
				for pi, i in idx {
					sorted[i] = rows[pi]
				}
			}
			copy(rows, sorted)
			return true
		}
	}
	sort_ctx := Sort_Ctx{order_clause, sort_indices}
	slice.sort_by_with_data(
		rows,
		proc(a, b: Row_Entry, data: rawptr) -> bool {
			ctx := (^Sort_Ctx)(data)
			for sort_idx, i in ctx.sort_indices {
				a_null := types.is_null(a.values[sort_idx])
				b_null := types.is_null(b.values[sort_idx])
				if a_null != b_null {
					nulls_first := ctx.order_clause[i].nulls_first
					if !nulls_first {
						nulls_first = ctx.order_clause[i].desc
					}
					return a_null == nulls_first
				}
				cmp := compare_values(a.values[sort_idx], b.values[sort_idx])
				if cmp != 0 {
					if ctx.order_clause[i].desc { return cmp > 0 }
					return cmp < 0
				}
			}
			return false
		},
		&sort_ctx,
	)
	return true
}
