package executor

import "core:log"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

@(private="file")
join_emit_combined :: proc(outer: Row_Entry, inner: []types.Value, new_rows: ^[dynamic]Row_Entry) {
	combined := make([]types.Value, len(outer.values) + len(inner), context.temp_allocator)
	copy(combined[:len(outer.values)], outer.values)
	copy(combined[len(outer.values):], inner)
	append(new_rows, Row_Entry{0, combined})
}

@(private="file")
join_emit_null_row :: proc(outer: Row_Entry, right_col_count: int, new_rows: ^[dynamic]Row_Entry) {
	null_row := make([]types.Value, len(outer.values) + right_col_count, context.temp_allocator)
	copy(null_row[:len(outer.values)], outer.values)
	for k in len(outer.values) ..< len(null_row) {
		null_row[k] = types.value_null()
	}
	append(new_rows, Row_Entry{0, null_row})
}

@(private="file")
join_emit_null_left_row :: proc(right_row: Row_Entry, left_col_count: int, new_rows: ^[dynamic]Row_Entry) {
	null_row := make([]types.Value, left_col_count + len(right_row.values), context.temp_allocator)
	for k in 0 ..< left_col_count {
		null_row[k] = types.value_null()
	}

	copy(null_row[left_col_count:], right_row.values)
	append(new_rows, Row_Entry{0, null_row})
}

// emit_unmatched_outer null-extends every row of an outer join side when the
// ON clause is unresolvable (matches nothing): LEFT pads each left row with
// nulls, RIGHT pads each right row.
@(private="file")
emit_unmatched_outer :: proc(
	is_left, is_right: bool,
	rows, right_rows: []Row_Entry,
	left_col_count, right_col_count: int,
	new_rows: ^[dynamic]Row_Entry,
) {
	if is_left {
		for outer_row in rows {
			join_emit_null_row(outer_row, right_col_count, new_rows)
		}
	}
	if is_right {
		for ri in 0 ..< len(right_rows) {
			join_emit_null_left_row(right_rows[ri], left_col_count, new_rows)
		}
	}
}

// Join_Side bundles one side of a hash join: its rows, the join column
// (absolute index into the combined row), and the side's full row width
// (for the OOB guard and null-extended outer rows).
Join_Side :: struct {
	rows:  []Row_Entry,
	col:   int,
	width: int,
}

// Join_Outer marks which unmatched sides emit null-extended rows.
Join_Outer :: struct {
	left:  bool,
	right: bool,
}

// join_key_i64 fingerprints an integer join key (bijective, so hits need no
// verification). Non-i64 values — including NULLs — report false and never
// match, mirroring the old dedicated i64 path.
@(private="file")
join_key_i64 :: proc(v: types.Value) -> (u64, bool) {
	key, ok := v.(i64)
	if !ok { return 0, false }
	return u64(key), true
}

// join_key_fingerprint hashes any non-NULL key (no per-row string
// allocation). Collisions fall back to value_compare at the call site.
@(private="file")
join_key_fingerprint :: proc(v: types.Value) -> (u64, bool) {
	if types.is_null(v) { return 0, false }
	return hash_value(v), true
}

@(private="file")
join_match_any :: proc(a, b: types.Value) -> bool { return true }

@(private="file")
join_match_compare :: proc(a, b: types.Value) -> bool {
	return types.value_compare(a, b)
}

// join_hash_probe is the shared hash-join engine behind the former
// join_hash_i64 / join_hash_string twins. key_of fingerprints one key value
// (false = skip); verify confirms a fingerprint hit. The smaller side is built
// into the bucket table; the other probes it.
@(private="file")
join_hash_probe :: proc(
	left, right: Join_Side,
	outer: Join_Outer,
	key_of: proc(v: types.Value) -> (u64, bool),
	verify: proc(a, b: types.Value) -> bool,
	new_rows: ^[dynamic]Row_Entry,
) {
	// Defensive: avoid OOB if a future caller bypasses the try_hash_join guard.
	if left.col < 0 || left.col >= left.width || right.col < 0 || right.col >= right.width {
		return
	}

	build_left := len(left.rows) <= len(right.rows)
	build := left if build_left else right
	probe := right if build_left else left
	build_cap := len(build.rows)
	ht := make(map[u64][dynamic]int, build_cap, context.temp_allocator)
	for row, ri in build.rows {
		key, ok := key_of(row.values[build.col])
		if !ok { continue }

		bucket := ht[key]
		append(&bucket, ri)
		ht[key] = bucket
	}

	matched_build := make(map[int]bool, len(build.rows), context.temp_allocator)
	matched_probe := make(map[int]bool, len(probe.rows), context.temp_allocator)
	for p_row, pi in probe.rows {
		pv := p_row.values[probe.col]
		key, ok := key_of(pv)
		if !ok { continue }

		matches, has := ht[key]
		if !has { continue }
		for bi in matches {
			if !verify(build.rows[bi].values[build.col], pv) { continue }

			matched_build[bi] = true
			matched_probe[pi] = true
			if build_left {
				join_emit_combined(build.rows[bi], p_row.values, new_rows)
			} else {
				join_emit_combined(p_row, build.rows[bi].values, new_rows)
			}
		}
	}

	for _, bucket in ht do delete(bucket)
	delete(ht)

	// Null-extend unmatched rows of either side; `outer.left/right` name the
	// logical sides, so map the matched sets back accordingly.
	matched_left := matched_build if build_left else matched_probe
	matched_right := matched_probe if build_left else matched_build
	if outer.left {
		for li in 0 ..< len(left.rows) {
			if li in matched_left { continue }
			join_emit_null_row(left.rows[li], right.width, new_rows)
		}
	}
	if outer.right {
		for ri in 0 ..< len(right.rows) {
			if ri in matched_right { continue }
			join_emit_null_left_row(right.rows[ri], left.width, new_rows)
		}
	}

	delete(matched_build)
	delete(matched_probe)
}

@(private="file")
resolve_from_source :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	ctx: ^Table_Context,
	cache: ^schema.Table_Cache = nil,
) -> bool {
	if tbl_name, is_table := stmt.from.(string); is_table {
		info, found := schema.find_table_cached(t, tbl_name, cache)
		if !found {
			log.errorf("Error: Table not found: %s", tbl_name)
			return false
		}

		ctx^ = Table_Context {
			info = {table = info^, tree = btree.init(t.pager, info.root_page)},
			range = {
				table_name = stmt.from_alias if stmt.from_alias != "" else tbl_name,
				start_col = 0,
				col_count = len(info.columns),
			},
		}
		return true
	} else if vt, is_vt := stmt.from.(^parser.Select_Stmt); is_vt {
		inner_rows, inner_cols := exec_subquery(t, vt^, cache)
		if inner_rows == nil { return false }
		ctx^ = Table_Context {
			info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
			range = {table_name = stmt.from_alias, start_col = 0, col_count = len(inner_cols)},
		}
		return true
	}
	return false
}

@(private="file")
resolve_join_source :: proc(
	t: ^btree.Tree,
	join: parser.Join_Clause,
	prev_range: Table_Col_Range,
	ctx: ^Table_Context,
	cache: ^schema.Table_Cache = nil,
) -> bool {
	if jt_name, is_table := join.source.(string); is_table {
		info, found := schema.find_table_cached(t, jt_name, cache)
		if !found {
			log.errorf("Error: Table not found: %s", jt_name)
			return false
		}

		alias := join.alias if join.alias != "" else jt_name
		ctx^ = Table_Context {
			info = {table = info^, tree = btree.init(t.pager, info.root_page)},
			range = {
				table_name = alias,
				start_col = prev_range.start_col + prev_range.col_count,
				col_count = len(info.columns),
			},
		}
		return true
	} else if subq, is_subquery := join.source.(^parser.Select_Stmt); is_subquery {
		inner_rows, inner_cols := exec_subquery(t, subq^, cache)
		if inner_rows == nil { return false }

		alias := join.alias if join.alias != "" else ""
		ctx^ = Table_Context {
			info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
			range = {
				table_name = alias,
				start_col = prev_range.start_col + prev_range.col_count,
				col_count = len(inner_cols),
			},
		}
		return true
	}
	return false
}

// execute_single_join runs one JOIN clause: materializes the right side,
// picks hash vs nested-loop, and returns the combined rows.
@(private="file")
execute_single_join :: proc(
	t: ^btree.Tree,
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	info_idx: int,
	rows: []Row_Entry,
	filter: Maybe(parser.Where_Clause),
	cache: ^schema.Table_Cache = nil,
) -> []Row_Entry {
	new_rows := make([dynamic]Row_Entry, context.temp_allocator)
	right_rows: []Row_Entry
	if vt, is_vt := jb.ctxs[info_idx].info.virtual.?; is_vt {
		right_rows = vt.rows
	} else {
		// Materialize the right table once, then match against every left row
		right_rows, _ = scan_table(
			&jb.ctxs[info_idx].info.tree,
			&jb.ctxs[info_idx].info.table,
			filter,
			nil,
			t,
			context.temp_allocator,
			cache,
		)
	}

	hash_used := try_hash_join(jb, jc, info_idx, rows, right_rows, &new_rows)
	if !hash_used {
		nested_loop_join(jb, jc, info_idx, rows, right_rows, &new_rows)
	}
	return new_rows[:]
}

// try_hash_join attempts the hash-join fast path for single-COND equi-joins
// with a column RHS. Returns false to fall back to nested loop.
@(private="file")
try_hash_join :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	info_idx: int,
	rows: []Row_Entry,
	right_rows: []Row_Entry,
	new_rows: ^[dynamic]Row_Entry,
) -> bool {
	is_left := jc.join_type == .LEFT
	is_right := jc.join_type == .RIGHT
	// Accumulated left width: rows hold all tables joined so far, not just
	// the previous table (chained RIGHT JOINs emitted narrow rows otherwise).
	left_col_count := jb.ctxs[info_idx - 1].range.start_col + jb.ctxs[info_idx - 1].range.col_count
	right_col_count := jb.ctxs[info_idx].range.col_count
	on_cl, has_on := jc.on_clause.?
	if !has_on { return false }

	cond, has_cond := where_single_condition(on_cl)
	if !has_cond || cond.operator != .EQUALS { return false }

	rhs_str, is_col := cond.rhs.(string)
	if !is_col { return false }

	left_idx, left_ok := resolve_qualified_column(
		jb.cols,
		jb.ranges,
		cond.column,
	)
	right_idx, right_ok := resolve_qualified_column(
		jb.cols,
		jb.ranges,
		rhs_str,
	)
	if !left_ok || !right_ok { return false }

	right_adjust := jb.ctxs[info_idx].range.start_col
	lcc := jb.ctxs[info_idx - 1].range.col_count
	rcc := jb.ctxs[info_idx].range.col_count

	// Verify both ON columns belong to their respective sides, accounting
	// for the current table's start_col. Multi-JOIN or junk ON clauses can
	// resolve to indices outside the table's range; the nested-loop path
	// resolves per-table correctly, so fall through here instead of OOB-ing.
	if left_idx < 0 || left_idx >= lcc ||
	   right_idx < right_adjust || right_idx >= right_adjust + rcc {
		return false
	}

	key_is_int := false
	if len(rows) > 0 && len(right_rows) > 0 {
		if _, ok := rows[0].values[left_idx].(i64); ok {
			key_is_int = true
		}
	}
	if key_is_int {
		join_hash_probe(
			{rows, left_idx, left_col_count},
			{right_rows, right_idx - right_adjust, right_col_count},
			{is_left, is_right},
			join_key_i64,
			join_match_any,
			new_rows,
		)
	} else {
		join_hash_probe(
			{rows, left_idx, left_col_count},
			{right_rows, right_idx - right_adjust, right_col_count},
			{is_left, is_right},
			join_key_fingerprint,
			join_match_compare,
			new_rows,
		)
	}
	return true
}

// nested_loop_join is the fallback for non-equi and multi-conjunct joins:
// pair-wise ON evaluation with null extension for outer joins.
@(private="file")
nested_loop_join :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	info_idx: int,
	rows: []Row_Entry,
	right_rows: []Row_Entry,
	new_rows: ^[dynamic]Row_Entry,
) {
	is_left := jc.join_type == .LEFT
	is_right := jc.join_type == .RIGHT
	// Accumulated left width: rows hold all tables joined so far, not just
	// the previous table (chained RIGHT JOINs emitted narrow rows otherwise).
	left_col_count := jb.ctxs[info_idx - 1].range.start_col + jb.ctxs[info_idx - 1].range.col_count
	right_col_count := jb.ctxs[info_idx].range.col_count
	// Resolve the ON filter once, not per pair. Unresolvable ON matches
	// nothing (mirrors evaluate_where); absent ON matches everything.
	filter: Maybe(Where_Eval_Ctx)
	if on_cl, has := jc.on_clause.?; has {
		filter = init_where_ctx(&on_cl, jb.cols, jb.ranges, nil, context.temp_allocator)
		if _, ok := filter.?; !ok {
			// Unresolvable ON: still emit null-extended rows for outer joins.
			emit_unmatched_outer(is_left, is_right, rows, right_rows, left_col_count, right_col_count, new_rows)
			return
		}
	}

	matched_right := make(map[int]bool, len(right_rows), context.temp_allocator)
	if is_left || len(right_rows) >= len(rows) {
		for outer_row in rows {
			matched := false
			for right_row, ri in right_rows {
				try_join_match(
					outer_row,
					right_row.values,
					filter,
					new_rows,
					&matched,
				)
				if matched { matched_right[ri] = true }
			}
			if is_left && !matched {
				join_emit_null_row(outer_row, right_col_count, new_rows)
			}
		}
	} else {
		for r_row, r_idx in right_rows {
			for l_row in rows {
				dummy := false
				try_join_match(
					l_row,
					r_row.values,
					filter,
					new_rows,
					&dummy,
				)
				if dummy { matched_right[r_idx] = true }
			}
		}
	}
	if is_right {
		for ri in 0 ..< len(right_rows) {
			if ri in matched_right { continue }
			join_emit_null_left_row(right_rows[ri], left_col_count, new_rows)
		}
	}
	delete(matched_right)
}

@(private)
build_join_result :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> Join_Build {
	table_count := 1 + len(stmt.joins)
	table_ctxs := make([]Table_Context, table_count, context.temp_allocator)
	table_ranges := make([]Table_Col_Range, table_count, context.temp_allocator)
	if !resolve_from_source(t, stmt, &table_ctxs[0], cache) {
		return {}
	}

	table_ranges[0] = table_ctxs[0].range
	col_count_0 := table_ctxs[0].range.col_count
	for join, i in stmt.joins {
		idx := i + 1
		if !resolve_join_source(t, join, table_ctxs[idx - 1].range, &table_ctxs[idx], cache) {
			return {}
		}
		table_ranges[idx] = table_ctxs[idx].range
	}

	total_cols :=
		table_ctxs[table_count - 1].range.start_col + table_ctxs[table_count - 1].range.col_count

	combined_cols := make([]types.Column, total_cols, context.temp_allocator)
	for ti in 0 ..< table_count {
		tr := table_ctxs[ti].range
		if vt, is_virtual := table_ctxs[ti].info.virtual.?; is_virtual {
			for j in 0 ..< tr.col_count { combined_cols[tr.start_col + j] = vt.columns[j] }
		} else {
			for j in 0 ..< tr.col_count { combined_cols[tr.start_col + j] = table_ctxs[ti].info.table.columns[j] }
		}
	}

	// Predicate pushdown: partition the WHERE conjuncts per table so each table
	// scan filters early (and can use skip-index pruning). Conjuncts that are
	// qualified/ambiguous/cross-table stay for the post-join filter_rows below.
	join_filters := make([dynamic]Maybe(parser.Where_Clause), 0, context.temp_allocator)
	if wc, has_wc := stmt.where_clause.?; has_wc {
		join_filters = split_where_for_join(wc, combined_cols, table_ranges)
	}

	rows: []Row_Entry
	if col_count_0 > 0 {
		r, scan_err := scan_table(
			&table_ctxs[0].info.tree,
			&table_ctxs[0].info.table,
			join_filters[0] if 0 < len(join_filters) else nil,
			nil,
			t,
			context.temp_allocator,
			cache,
		)
		if scan_err { return {} }
		rows = r
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		rows = vt.rows
	}

	jb := Join_Build {
		ctxs = table_ctxs,
		ranges = table_ranges,
		cols = combined_cols,
	}
	for j_idx in 0 ..< len(stmt.joins) {
		jc := stmt.joins[j_idx]
		info_idx := j_idx + 1
		filter := join_filters[info_idx] if info_idx < len(join_filters) else nil
		rows = execute_single_join(t, &jb, jc, info_idx, rows, filter, cache)
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(rows, &where_clause, combined_cols, table_ranges)
	}

	jb.rows = rows
	jb.total_cols = total_cols
	jb.ok = true
	return jb
}

// exec_select_join_data evaluates a SELECT with JOINs and returns projected
// rows/columns without printing.
@(private)
exec_select_join_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	jb := build_join_result(t, stmt, cache)
	if !jb.ok { return nil, nil, false }

	rows, combined_cols, table_ranges, total_cols := jb.rows, jb.cols, jb.ranges, jb.total_cols
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows, combined_cols, table_ranges)
	}
	// Sort on the full combined rows (so qualified ORDER BY names resolve),
	// then project to the requested columns.
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) {
			return nil, nil, false
		}
	}

	display_indices, d_ok := build_display_indices(
		stmt.columns,
		combined_cols,
		table_ranges,
		total_cols,
	)
	if !d_ok { return nil, nil, false }

	proj_rows := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	for entry in rows {
		proj_vals := make([]types.Value, len(display_indices), context.temp_allocator)
		for idx, i in display_indices { proj_vals[i] = entry.values[idx] }
		append(&proj_rows, Row_Entry{entry.rowid, proj_vals})
	}

	proj_cols := make([]types.Column, len(display_indices), context.temp_allocator)
	for idx, i in display_indices {
		proj_cols[i] = combined_cols[idx]
		if i < len(stmt.aliases) && stmt.aliases[i] != "" {
			proj_cols[i].name = stmt.aliases[i]
		}
	}

	out := proj_rows[:]
	if stmt.is_distinct { out = dedup_rows(out) }
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off { off = o }

		start := int(min(off, u64(len(out))))
		end := int(min(off + limit, u64(len(out))))
		out = out[start:end]
	}
	return out, proj_cols, true
}

@(private="file")
try_join_match :: proc(
	outer_row: Row_Entry,
	inner_values: []types.Value,
	filter: Maybe(Where_Eval_Ctx),
	new_rows: ^[dynamic]Row_Entry,
	matched: ^bool,
) {
	if f, has_f := filter.?; has_f {
		tmp := make(
			[]types.Value,
			len(outer_row.values) + len(inner_values),
			context.temp_allocator,
		)

		copy(tmp[:len(outer_row.values)], outer_row.values)
		copy(tmp[len(outer_row.values):], inner_values)
		if !evaluate_where_ctx(f, tmp) { return }
	}

	matched^ = true
	join_emit_combined(outer_row, inner_values, new_rows)
}
