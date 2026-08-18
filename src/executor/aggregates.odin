package executor

import "core:fmt"
import "core:hash"
import "core:log"
import "src:parser"
import "src:types"

@(private)
find_existing_group :: proc(
	group_map: map[u64][dynamic]int,
	groups: []Group,
	hash: u64,
	row_entry: Row_Entry,
	group_by_indices: []int,
) -> (
	int,
	bool,
) {
	if bucket, ok := group_map[hash]; ok {
		for gi in bucket {
			if values_equal_by_indices(
				row_entry.values,
				groups[gi].key_values,
				group_by_indices,
			) {
				return gi, true
			}
		}
	}
	return -1, false
}

@(private)
exec_select_aggregate_combined :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	group_by_indices := make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	group_map := make(map[u64][dynamic]int, context.temp_allocator)
	defer {
		for _, bucket in group_map { delete(bucket) }
		delete(group_map)
	}
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := find_existing_group(
				group_map,
				groups[:],
				hash,
				row_entry,
				group_by_indices,
			)
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for col_idx, pos in group_by_indices {
					key_vals[pos] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				bucket := group_map[hash]
				append(&bucket, len(groups))
				group_map[hash] = bucket
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}

	if len(groups) == 0 && len(group_by_indices) == 0 {
		append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
	}

	rows_mat := make([dynamic][]string, context.temp_allocator)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows { group_rows[ri] = row_entry.values }

		agg_vals := compute_aggregates(
			group_rows,
			stmt.aggregates,
			combined_cols,
			context.temp_allocator,
		)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) { continue }
		}

		row_strs := make([]string, len(stmt.columns), context.temp_allocator)
		val_idx := 0
		for _, i in stmt.columns {
			if val_idx < len(group_by_indices) {
				row_strs[i] = value_string(groups[gi].key_values[val_idx])
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				row_strs[i] = value_string(agg_vals[agg_idx])
				val_idx += 1
			}
		}
		append(&rows_mat, row_strs)
	}

	render_table(select_header_names(stmt), rows_mat[:])
	fmt.printf("(%d rows)\n", len(rows_mat))
	return true
}

// exec_select_aggregate_data evaluates a SELECT with aggregates/GROUP BY and returns
// the result rows as data (group key values followed by aggregate values), without
// printing. `cols` are synthesized from stmt.columns.
@(private)
exec_select_aggregate_data :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	group_by_indices := make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return nil, nil, false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	group_map := make(map[u64][dynamic]int, context.temp_allocator)
	defer {
		for _, bucket in group_map { delete(bucket) }
		delete(group_map)
	}
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := find_existing_group(
				group_map,
				groups[:],
				hash,
				row_entry,
				group_by_indices,
			)
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for col_idx, pos in group_by_indices {
					key_vals[pos] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				bucket := group_map[hash]
				append(&bucket, len(groups))
				group_map[hash] = bucket
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}
	if len(groups) == 0 && len(group_by_indices) == 0 {
		append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
	}

	result := make([dynamic]Row_Entry, context.temp_allocator)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows { group_rows[ri] = row_entry.values }

		agg_vals := compute_aggregates(
			group_rows,
			stmt.aggregates,
			combined_cols,
			context.temp_allocator,
		)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) { continue }
		}

		out := make([]types.Value, len(stmt.columns), context.temp_allocator)
		val_idx := 0
		for i in 0 ..< len(stmt.columns) {
			if val_idx < len(group_by_indices) {
				out[i] = groups[gi].key_values[val_idx]
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				out[i] = agg_vals[agg_idx]
				val_idx += 1
			}
		}
		append(&result, Row_Entry{rowid = types.Row_ID(gi), values = out})
	}

	cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
	for name, i in stmt.columns {
		display := name
		if i < len(stmt.aliases) && stmt.aliases[i] != "" { display = stmt.aliases[i] }
		cols[i] = types.Column {name = display, type = .INTEGER}
	}
	return result[:], cols, true
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
