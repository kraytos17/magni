package executor

import "core:log"
import "core:slice"
import "src:parser"
import "src:types"

// compare_key_values three-way-compares two rows' group keys in index
// order (NULLs sort last). The single comparison primitive behind the key
// sorter, the ordered-input check, and (via ==0) the boundary test, so the
// three can never disagree on key order.
@(private)
compare_key_values :: proc(a, b: []types.Value, indices: []int) -> int {
	for idx in indices {
		a_null := types.is_null(a[idx])
		b_null := types.is_null(b[idx])
		if a_null != b_null {
			return 1 if a_null else -1
		}
		if a_null {
			continue
		}
		if cmp := compare_values(a[idx], b[idx]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

// rows_key_ordered reports whether rows already ascend by key (an O(n) scan
// with early exit — unsorted input typically exits within a few pairs, so
// the check is only fully paid when it pays off by skipping the sort).
@(private)
rows_key_ordered :: proc(rows: []Row_Entry, indices: []int) -> bool {
	for i in 1 ..< len(rows) {
		if compare_key_values(rows[i - 1].values, rows[i].values, indices) > 0 {
			return false
		}
	}
	return true
}

// group_boundary_at returns the end index of the group starting at start
// (rows [start, end) share one key). Runs the shared equality primitive
// rather than a re-derivation so boundary semantics cannot drift from the
// sorter.
@(private)
group_boundary_at :: proc(rows: []Row_Entry, start: int, indices: []int) -> int {
	end := start + 1
	for end < len(rows) && compare_key_values(rows[start].values, rows[end].values, indices) == 0 {
		end += 1
	}
	return end
}

// sort_rows_by_key_indices sorts rows ascending by key via
// compare_key_values. Used by streaming GROUP BY, which needs key order
// rather than ORDER BY display semantics.
@(private)
sort_rows_by_key_indices :: proc(rows: []Row_Entry, key_indices: []int) -> bool {
	if len(key_indices) == 0 || len(rows) < 2 {
		return true
	}

	idxs := key_indices
	slice.sort_by_with_data(rows, proc(a, b: Row_Entry, data: rawptr) -> bool {
			keys := (^[]int)(data)
			return compare_key_values(a.values, b.values, keys^) < 0
		}, &idxs)
	return true
}

// row_fingerprint computes a hash over a row's values. Used for DISTINCT
// dedup and set-operation membership. Implemented via hash_values (single
// FNV-1a with per-type tags); collisions fall back to value_compare.
@(private)
row_fingerprint :: proc(values: []types.Value) -> u64 {
	return hash_values(values)
}

// build_fp_index returns the sorted fingerprints of vals for binary-search
// membership prefiltering (empty input yields an empty index = scan all).
// Callers verify index hits exactly: fingerprints can collide.
@(private)
build_fp_index :: proc(vals: []types.Value, allocator := context.temp_allocator) -> []u64 {
	if len(vals) == 0 {
		return nil
	}

	fps := make([]u64, len(vals), allocator)
	for v, i in vals {
		fps[i] = hash_value(v)
	}

	slice.sort(fps)
	return fps
}

// fp_index_hit binary-searches the sorted fingerprint index. An empty index
// (e.g. hand-built nodes) hits everything, falling back to the linear scan.
@(private)
fp_index_hit :: proc(fps: []u64, fp: u64) -> bool {
	if len(fps) == 0 {
		return true
	}

	_, found := slice.binary_search(fps, fp)
	return found
}

// dedup_rows removes duplicate rows in first-seen order (fingerprint
// buckets prefilter, values_equal verifies — hash collisions never merge).
// Temp-allocated output; zero/one rows return the input slice as-is.
dedup_rows :: proc(rows: []Row_Entry) -> []Row_Entry {
	if len(rows) <= 1 {
		return rows
	}

	seen := fp_buckets_make(len(rows), context.temp_allocator)
	result := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	defer fp_buckets_destroy(&seen)
	for r in rows {
		fp := row_fingerprint(r.values)
		is_dup := false
		if h, ok := fp_buckets_probe(&seen, fp); ok {
			for n := h; n != -1; n = seen.next[n] {
				if values_equal(r.values, result[seen.rows[n]].values) {
					is_dup = true
					break
				}
			}
		}
		if !is_dup {
			append(&result, r)
			fp_buckets_add(&seen, fp, len(result) - 1)
		}
	}
	return result[:]
}

// sort_rows orders rows by the ORDER BY clause (stable, NULL placement per
// Order_By_Column with the DESC default). Single-integer-key sorts take
// the sort_rows_int_fast path; anything it rejects (non-int values)
// falls through to the general comparator. False when a sort key won't
// resolve.
@(private)
sort_rows :: proc(
	rows: []Row_Entry,
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	resolver := build_column_resolver(cols, table_ranges)
	sort_indices, r_ok := resolve_sort_indices(order_clause, resolver)
	if !r_ok {
		return false
	}
	if len(order_clause) == 1 && len(rows) > 1 {
		if sort_rows_int_fast(rows, order_clause[0], sort_indices[0]) {
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
					if ctx.order_clause[i].desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		}, &sort_ctx)
	return true
}

// resolve_sort_indices maps each ORDER BY column to its absolute index in the
// row. Logs and returns false on an unknown column. Package-visible (not
// file-private): the vector scan path needs sort keys for its needed-column
// mask (sort runs on full rows before projection, so sort keys must decode).
@(private)
resolve_sort_indices :: proc(
	order_clause: []parser.Order_By_Column,
	resolver: Column_Resolver,
) -> (
	[]int,
	bool,
) {
	sort_indices := make([]int, len(order_clause), context.temp_allocator)
	for o, i in order_clause {
		idx, col_ok := resolve(resolver, o.column)
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
@(private = "file")
sort_rows_int_fast :: proc(
	rows: []Row_Entry,
	order: parser.Order_By_Column,
	sort_idx: int,
) -> bool {
	keys := make([]i64, len(rows), context.temp_allocator)
	for row, i in rows {
		iv, ok := row.values[sort_idx].(i64)
		if !ok {
			return false
		}
		keys[i] = iv
	}

	desc := order.desc
	nulls_first := order.nulls_first
	if !nulls_first {
		nulls_first = desc
	}

	idx := make([]int, len(rows), context.temp_allocator)
	for i in 0 ..< len(rows) {
		idx[i] = i
	}

	slice.sort_by_with_data(idx, proc(a, b: int, data: rawptr) -> bool {
			k := (^[]i64)(data)
			return k[a] < k[b]
		}, &keys)

	sorted := make([]Row_Entry, len(rows), context.temp_allocator)
	for pi, i in idx {
		sorted[i] = rows[pi]
	}
	if desc || nulls_first {
		slice.reverse(sorted)
	}

	copy(rows, sorted)
	return true
}
