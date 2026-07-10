package executor

import "core:encoding/endian"
import "core:hash"
import "core:log"
import "core:slice"
import "src:parser"
import "src:types"

dedup_rows :: proc(rows: []Row_Entry) -> []Row_Entry {
	if len(rows) <= 1 { return rows }
	seen := make(map[u64][dynamic]int, len(rows), context.temp_allocator)
	result := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	defer {
		for _, bucket in seen { delete(bucket) }
		delete(seen)
	}
	for r in rows {
		h := u64(0)
		for v, i in r.values {
			if i > 0 {
				h = hash.fnv64a([]byte{0}, h)
			}

			switch val in v {
			case types.Null:
				h = hash.fnv64a(transmute([]byte)string("NULL"), h)
			case i64:
				buf: [8]u8
				endian.put_u64(buf[:], .Little, u64(val))
				h = hash.fnv64a(buf[:], h)
			case f64:
				buf: [8]u8
				endian.put_u64(buf[:], .Little, transmute(u64)val)
				h = hash.fnv64a(buf[:], h)
			case string:
				h = hash.fnv64a(transmute([]byte)val, h)
			case []u8:
				h = hash.fnv64a(val, h)
			}
		}

		fp := h
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
			log.errorf("Error: Unknown column in ORDER BY: %s", o.column)
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
			// SQL default: ASC → NULLS LAST, DESC → NULLS FIRST.
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
