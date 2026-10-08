package executor

import "core:log"
import "core:slice"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// join_emit_combined appends one joined row (left values + right values,
// temp-allocated, rowid 0 — joined rows have no source rowid).
@(private = "file")
join_emit_combined :: proc(outer: Row_Entry, inner: []types.Value, new_rows: ^[dynamic]Row_Entry) {
	combined := make([]types.Value, len(outer.values) + len(inner), context.temp_allocator)
	copy(combined[:len(outer.values)], outer.values)
	copy(combined[len(outer.values):], inner)
	append(new_rows, Row_Entry{0, combined})
}

// join_emit_null_row appends one LEFT-outer row: left values + right_width
// NULLs (unmatched left side survives the join).
@(private = "file")
join_emit_null_row :: proc(outer: Row_Entry, right_col_count: int, new_rows: ^[dynamic]Row_Entry) {
	null_row := make([]types.Value, len(outer.values) + right_col_count, context.temp_allocator)
	copy(null_row[:len(outer.values)], outer.values)
	slice.fill(null_row[len(outer.values):], types.value())
	append(new_rows, Row_Entry{0, null_row})
}

// join_emit_null_left_row appends one RIGHT-outer row: left_width NULLs +
// right values (unmatched right side survives the join).
@(private = "file")
join_emit_null_left_row :: proc(
	right_row: Row_Entry,
	left_col_count: int,
	new_rows: ^[dynamic]Row_Entry,
) {
	null_row := make([]types.Value, left_col_count + len(right_row.values), context.temp_allocator)

	slice.fill(null_row[:left_col_count], types.value())
	copy(null_row[left_col_count:], right_row.values)
	append(new_rows, Row_Entry{0, null_row})
}

// emit_unmatched_outer null-extends every row of an outer join side when the
// ON clause is unresolvable (matches nothing): LEFT pads each left row with
// nulls, RIGHT pads each right row.
@(private = "file")
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
	rows : []Row_Entry,
	col  : int,
	width: int,
}

// Join_Outer marks which unmatched sides emit null-extended rows.
Join_Outer :: struct {
	left : bool,
	right: bool,
}

// join_key_i64 fingerprints an integer join key with the identity map
// (bijective, so hits need no verification). Non-i64 values — including
// NULLs — report false and never match (NULL never joins).
@(private = "file")
join_key_i64 :: proc(v: types.Value) -> (u64, bool) {
	key, ok := v.(i64)
	if !ok {
		return 0, false
	}
	return u64(key), true
}

// join_key_fingerprint hashes any non-NULL key (no per-row string
// allocation). Collisions fall back to value_compare at the call site.
@(private = "file")
join_key_fingerprint :: proc(v: types.Value) -> (u64, bool) {
	if types.is_null(v) {
		return 0, false
	}
	return types.hash(v), true
}

// join_match_any accepts every pair (CROSS JOIN and ON-less arms —
// filtering, if any, happens in a later WHERE). Paired with the identity
// fingerprint so hash probing still partitions by key.
@(private = "file")
join_match_any :: proc(a, b: types.Value) -> bool { return true }

// join_match_compare accepts pairs the executor's value order calls equal
// (equi-join verification after a fingerprint hit).
@(private = "file")
join_match_compare :: proc(a, b: types.Value) -> bool {
	return types.value_compare(a, b)
}

// join_hash_probe is the shared hash-join engine: key_of fingerprints one
// key value (false = skip this row); verify confirms a fingerprint hit.
// The smaller side is built into the bucket table; the other probes it.
// Integer keys use the identity fingerprint (no verification); all other
// types hash with value_compare verification on hits.
@(private = "file")
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
		if !ok {
			continue
		}

		bucket := ht[key]
		append(&bucket, ri)
		ht[key] = bucket
	}

	matched_build := make(map[int]bool, len(build.rows), context.temp_allocator)
	matched_probe := make(map[int]bool, len(probe.rows), context.temp_allocator)
	for p_row, pi in probe.rows {
		pv := p_row.values[probe.col]
		key, ok := key_of(pv)
		if !ok {
			continue
		}

		matches, has := ht[key]
		if !has {
			continue
		}
		for bi in matches {
			if !verify(build.rows[bi].values[build.col], pv) {
				continue
			}

			matched_build[bi] = true
			matched_probe[pi] = true
			if build_left {
				join_emit_combined(build.rows[bi], p_row.values, new_rows)
			} else {
				join_emit_combined(p_row, build.rows[bi].values, new_rows)
			}
		}
	}
	for _, bucket in ht {
		delete(bucket)
	}

	delete(ht)
	matched_left := matched_build if build_left else matched_probe
	matched_right := matched_probe if build_left else matched_build
	if outer.left {
		for li in 0 ..< len(left.rows) {
			if li in matched_left {
				continue
			}
			join_emit_null_row(left.rows[li], right.width, new_rows)
		}
	}
	if outer.right {
		for ri in 0 ..< len(right.rows) {
			if ri in matched_right {
				continue
			}
			join_emit_null_left_row(right.rows[ri], left.width, new_rows)
		}
	}

	delete(matched_build)
	delete(matched_probe)
}

// resolve_from_source resolves the FROM arm into a Table_Context: physical
// tables via the catalog (tree opened on their root), subqueries by
// executing them into a Virtual_Table. FROM-less statements never reach
// here (literal path). False on unknown tables or failed subqueries.
@(private = "file")
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
		inner_rows, inner_cols, inner_ok := exec_subquery(t, vt^, cache)
		if !inner_ok {
			return false
		}

		ctx^ = Table_Context {
			info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
			range = {table_name = stmt.from_alias, start_col = 0, col_count = len(inner_cols)},
		}
		return true
	}
	return false
}

// resolve_join_source resolves one JOIN arm like resolve_from_source, with
// its column range placed after prev_range (combined-row layout). Same
// false conditions.
@(private = "file")
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
		inner_rows, inner_cols, inner_ok := exec_subquery(t, subq^, cache)
		if !inner_ok {
			return false
		}

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
@(private = "file")
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
@(private = "file")
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
	if !has_on {
		return false
	}

	cond, has_cond := where_single_condition(on_cl)
	if !has_cond || cond.operator != .EQUALS {
		return false
	}

	rhs_str, is_col := cond.rhs.(string)
	if !is_col {
		return false
	}

	left_idx, left_ok := resolve(jb.resolver, cond.column)
	right_idx, right_ok := resolve(jb.resolver, rhs_str)
	if !left_ok || !right_ok {
		return false
	}

	right_adjust := jb.ctxs[info_idx].range.start_col
	lcc := jb.ctxs[info_idx - 1].range.col_count
	rcc := jb.ctxs[info_idx].range.col_count

	// Verify both ON columns belong to their respective sides, accounting
	// for the current table's start_col. Multi-JOIN or junk ON clauses can
	// resolve to indices outside the table's range; the nested-loop path
	// resolves per-table correctly, so fall through here instead of OOB-ing.
	if left_idx < 0 ||
	   left_idx >= lcc ||
	   right_idx < right_adjust ||
	   right_idx >= right_adjust + rcc {
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

// init_join_filter resolves the ON clause once (not per pair). Absent ON
// matches everything (nil filter); unresolvable ON matches nothing —
// signalled by ok=false so the caller can null-extend every outer row
// (same fail-closed convention as evaluate_where).
@(private = "file")
init_join_filter :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
) -> (
	filter: Maybe(Where_Eval_Ctx),
	ok: bool,
) {
	if on_cl, has := jc.on_clause.?; has {
		filter = init_where_ctx(&on_cl, jb.cols, jb.ranges, nil, context.temp_allocator)
		if _, fok := filter.?; !fok {
			return nil, false
		}
	}
	return filter, true
}

// probe_left_driven pairs each left row against all right rows, null-
// extending unmatched left rows for LEFT JOIN. Records right-side hits in
// matched_right for the RIGHT JOIN tail.
@(private = "file")
probe_left_driven :: proc(
	rows, right_rows: []Row_Entry,
	filter: Maybe(Where_Eval_Ctx),
	is_left: bool,
	right_col_count: int,
	matched_right: ^map[int]bool,
	new_rows: ^[dynamic]Row_Entry,
) {
	for outer_row in rows {
		matched := false
		for right_row, ri in right_rows {
			try_join_match(outer_row, right_row.values, filter, new_rows, &matched)
			if matched {
				matched_right[ri] = true
			}
		}
		if is_left && !matched {
			join_emit_null_row(outer_row, right_col_count, new_rows)
		}
	}
}

// probe_right_driven pairs each right row against all left rows (the small-
// side-first orientation). No null extension here: misses are handled by
// the LEFT/RIGHT tails from matched_right.
@(private = "file")
probe_right_driven :: proc(
	rows, right_rows: []Row_Entry,
	filter: Maybe(Where_Eval_Ctx),
	matched_right: ^map[int]bool,
	new_rows: ^[dynamic]Row_Entry,
) {
	for r_row, r_idx in right_rows {
		for l_row in rows {
			dummy := false
			try_join_match(l_row, r_row.values, filter, new_rows, &dummy)
			if dummy {
				matched_right[r_idx] = true
			}
		}
	}
}

// emit_unmatched_right null-extends every right row with no match (the
// RIGHT JOIN tail).
@(private = "file")
emit_unmatched_right :: proc(
	right_rows: []Row_Entry,
	matched_right: ^map[int]bool,
	left_col_count: int,
	new_rows: ^[dynamic]Row_Entry,
) {
	for ri in 0 ..< len(right_rows) {
		if ri in matched_right {
			continue
		}
		join_emit_null_left_row(right_rows[ri], left_col_count, new_rows)
	}
}

// nested_loop_join is the fallback for non-equi and multi-conjunct joins:
// pair-wise ON evaluation with null extension for outer joins.
@(private = "file")
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
	// nothing (same fail-closed convention as evaluate_where); absent ON
	// matches everything.
	filter, filter_ok := init_join_filter(jb, jc)
	if !filter_ok {
		emit_unmatched_outer(
			is_left,
			is_right,
			rows,
			right_rows,
			left_col_count,
			right_col_count,
			new_rows,
		)
		return
	}

	matched_right := make(map[int]bool, len(right_rows), context.temp_allocator)
	if is_left || len(right_rows) >= len(rows) {
		probe_left_driven(
			rows,
			right_rows,
			filter,
			is_left,
			right_col_count,
			&matched_right,
			new_rows,
		)
	} else {
		probe_right_driven(rows, right_rows, filter, &matched_right, new_rows)
	}

	if is_right {
		emit_unmatched_right(right_rows, &matched_right, left_col_count, new_rows)
	}
	delete(matched_right)
}

// resolve_join_tables resolves the FROM source and every JOIN source into
// parallel context/range arrays. Returns false (already logged) on failure.
@(private = "file")
resolve_join_tables :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	table_ctxs: []Table_Context,
	table_ranges: []Table_Col_Range,
	cache: ^schema.Table_Cache = nil,
) -> bool {
	if !resolve_from_source(t, stmt, &table_ctxs[0], cache) {
		return false
	}

	table_ranges[0] = table_ctxs[0].range
	for join, i in stmt.joins {
		idx := i + 1
		if !resolve_join_source(t, join, table_ctxs[idx - 1].range, &table_ctxs[idx], cache) {
			return false
		}
		table_ranges[idx] = table_ctxs[idx].range
	}
	return true
}

// assemble_combined_cols lays every table's columns into one array at their
// range offsets. Returns the array and the total column count.
@(private = "file")
assemble_combined_cols :: proc(
	table_ctxs: []Table_Context,
	table_count: int,
) -> (
	[]types.Column,
	int,
) {
	total_cols :=
		table_ctxs[table_count - 1].range.start_col + table_ctxs[table_count - 1].range.col_count

	combined_cols := make([]types.Column, total_cols, context.temp_allocator)
	for ti in 0 ..< table_count {
		tr := table_ctxs[ti].range
		if vt, is_virtual := table_ctxs[ti].info.virtual.?; is_virtual {
			for j in 0 ..< tr.col_count {
				combined_cols[tr.start_col + j] = vt.columns[j]
			}
		} else {
			for j in 0 ..< tr.col_count {
				combined_cols[tr.start_col + j] = table_ctxs[ti].info.table.columns[j]
			}
		}
	}
	return combined_cols, total_cols
}

// scan_first_table materializes the FROM side: a btree scan with its
// pushdown filter, or the virtual rows for a subquery source. Returns
// (nil, true) when the first source yields no rows without error.
@(private = "file")
scan_first_table :: proc(
	t: ^btree.Tree,
	table_ctxs: []Table_Context,
	join_filters: [dynamic]Maybe(parser.Where_Clause),
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	bool,
) {
	if table_ctxs[0].range.col_count > 0 {
		r, scan_err := scan_table(
			&table_ctxs[0].info.tree,
			&table_ctxs[0].info.table,
			join_filters[0] if 0 < len(join_filters) else nil,
			nil,
			t,
			context.temp_allocator,
			cache,
		)
		if scan_err {
			return nil, false
		}
		return r, true
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		return vt.rows, true
	}
	return nil, true
}

// run_join_chain executes each JOIN clause in order, then applies the
// residual post-join WHERE filter over the combined rows.
@(private = "file")
run_join_chain :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	jb: ^Join_Build,
	rows: []Row_Entry,
	join_filters: [dynamic]Maybe(parser.Where_Clause),
	cache: ^schema.Table_Cache = nil,
) -> []Row_Entry {
	out := rows
	for j_idx in 0 ..< len(stmt.joins) {
		jc := stmt.joins[j_idx]
		info_idx := j_idx + 1
		filter := join_filters[info_idx] if info_idx < len(join_filters) else nil
		out = execute_single_join(t, jb, jc, info_idx, out, filter, cache)
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		out = filter_rows(out, &where_clause, jb.cols, jb.ranges)
	}
	return out
}

// build_join_result assembles and runs a FROM+JOINs query: resolve every
// source, push single-table WHERE conjuncts down to the scans
// (split_where_for_join), scan the first table, then chain the joins arm
// by arm. The post-join WHERE (unpushed conjuncts) applies to the combined
// rows. ok=false when any source or scan fails.
@(private)
build_join_result :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> Join_Build {
	table_count := 1 + len(stmt.joins)
	table_ctxs := make([]Table_Context, table_count, context.temp_allocator)
	table_ranges := make([]Table_Col_Range, table_count, context.temp_allocator)
	if !resolve_join_tables(t, stmt, table_ctxs, table_ranges, cache) {
		return {}
	}

	combined_cols, total_cols := assemble_combined_cols(table_ctxs, table_count)
	join_filters := make([dynamic]Maybe(parser.Where_Clause), 0, context.temp_allocator)
	if wc, has_wc := stmt.where_clause.?; has_wc {
		join_filters = split_where_for_join(wc, combined_cols, table_ranges)
	}

	rows, scan_ok := scan_first_table(t, table_ctxs, join_filters, cache)
	if !scan_ok {
		return {}
	}

	jb := Join_Build {
		ctxs     = table_ctxs,
		ranges   = table_ranges,
		cols     = combined_cols,
		resolver = build_column_resolver(combined_cols, table_ranges),
	}

	jb.rows = run_join_chain(t, stmt, &jb, rows, join_filters, cache)
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
	if !jb.ok {
		return nil, nil, false
	}

	rows, combined_cols, table_ranges := jb.rows, jb.cols, jb.ranges
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows, combined_cols, table_ranges)
	}
	return finish_select(stmt, rows, combined_cols, table_ranges)
}

// try_join_match tests one pair against the arm's ON filter (nil filter =
// match) and emits the combined row on success, setting matched (the
// outer-join tail uses it to decide null-extension). Non-matches emit
// nothing.
@(private = "file")
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
		if !evaluate_where_ctx(f, tmp) {
			return
		}
	}

	matched^ = true
	join_emit_combined(outer_row, inner_values, new_rows)
}
