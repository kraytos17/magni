package executor

import "core:log"
import "core:slice"
import "src:parser"
import "src:types"

// row_fingerprint computes a hash over a row's values. Used for DISTINCT
// dedup and set-operation membership. Implemented via hash_values (single
// FNV-1a with per-type tags); collisions fall back to value_compare.
@(private)
row_fingerprint :: proc(values: []types.Value) -> u64 {
	return hash_values(values)
}

dedup_rows :: proc(rows: []Row_Entry) -> []Row_Entry {
	if len(rows) <= 1 { return rows }
	seen := make(map[u64][dynamic]int, len(rows), context.temp_allocator)
	result := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	defer {
		for _, bucket in seen { delete(bucket) }
		delete(seen)
	}

	for r in rows {
		fp := row_fingerprint(r.values)
		is_dup := false
		if bucket, ok := seen[fp]; ok {
			for idx in bucket {
				existing := result[idx]
				all_eq := true
				for j in 0 ..< len(r.values) {
					if !types.value_compare(r.values[j], existing.values[j]) {
						all_eq = false
						break
					}
				}
				if all_eq {
					is_dup = true
					break
				}
			}
		}
		if !is_dup {
			append(&result, r)
			bucket := seen[fp]
			append(&bucket, len(result) - 1)
			seen[fp] = bucket
		}
	}
	return result[:]
}

@(private)
sort_rows :: proc(
	rows: []Row_Entry,
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	sort_indices, r_ok := resolve_sort_indices(order_clause, cols, table_ranges)
	if !r_ok { return false }
	if len(order_clause) == 1 && len(rows) > 1 {
		if sort_rows_int_fast(rows, order_clause[0], sort_indices[0]) { return true }
	}

	sort_ctx := Sort_Ctx{order_clause, sort_indices}
	slice.sort_by_with_data(rows, proc(a, b: Row_Entry, data: rawptr) -> bool {
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
		}, &sort_ctx)
	return true
}

// resolve_sort_indices maps each ORDER BY column to its absolute index in the
// row. Logs and returns false on an unknown column.
@(private="file")
resolve_sort_indices :: proc(
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	[]int,
	bool,
) {
	sort_indices := make([]int, len(order_clause), context.temp_allocator)
	for o, i in order_clause {
		idx, col_ok := resolve_qualified_column(cols, table_ranges, o.column)
		if !col_ok {
			log.errorf("Error: Unknown column in ORDER BY: %s", o.column)
			return nil, false
		}
		sort_indices[i] = idx
	}
	return sort_indices, true
}

// sort_rows_int_fast sorts by a single integer column via a precomputed key
// array (no per-comparison union dispatch). Returns false when any value is a
// non-int (or there are no rows), letting the general comparator handle it.
@(private="file")
sort_rows_int_fast :: proc(rows: []Row_Entry, order: parser.Order_By_Column, sort_idx: int) -> bool {
	keys := make([]i64, len(rows), context.temp_allocator)
	for row, i in rows {
		iv, ok := row.values[sort_idx].(i64)
		if !ok { return false }
		keys[i] = iv
	}

	desc := order.desc
	nulls_first := order.nulls_first
	if !nulls_first { nulls_first = desc }

	idx := make([]int, len(rows), context.temp_allocator)
	for i in 0 ..< len(rows) { idx[i] = i }

	slice.sort_by_with_data(idx, proc(a, b: int, data: rawptr) -> bool {
			k := (^[]i64)(data)
			return k[a] < k[b]
		}, &keys)

	sorted := make([]Row_Entry, len(rows), context.temp_allocator)
	for pi, i in idx {
		sorted[i] = rows[pi]
	}
	if desc || nulls_first { slice.reverse(sorted) }

	copy(rows, sorted)
	return true
}
