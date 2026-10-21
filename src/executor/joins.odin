package executor

import "core:slice"
import "src:btree"
import "src:cell"
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

// join_emit_unmatched emits the NULL-extended rows for outer joins: left
// rows (or right rows) with no hash match during the probe. The matched
// sets arrive already oriented to left/right by the caller.
@(private = "file")
join_emit_unmatched :: proc(
	left, right: Join_Side,
	matched_left, matched_right: map[int]bool,
	outer: Join_Outer,
	new_rows: ^[dynamic]Row_Entry,
) {
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
}

// join_hash_probe is the shared hash-join engine: key_of fingerprints one
// key value (false = skip this row); verify confirms a fingerprint hit.
// The smaller side is built into the bucket table; the other probes it.
// Integer keys use the identity fingerprint (no verification); all other
// types hash with value_compare verification on hits.
join_hash_probe :: proc(
	left, right: Join_Side,
	outer: Join_Outer,
	key_of: proc(v: types.Value) -> (u64, bool),
	verify: proc(a, b: types.Value) -> bool,
	new_rows: ^[dynamic]Row_Entry,
) {
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
	join_emit_unmatched(left, right, matched_left, matched_right, outer, new_rows)

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
			log_table_not_found(tbl_name)
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
			log_table_not_found(jt_name)
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

// PROBE_MAX_OUTER_ROWS caps probe-first routing by driver size, and
// PROBE_MAX_EST_MATCHES by expected output: per-row index lookups beat
// hash only for small outers with few matches (fit matrix, 30k inner:
// probe wins to 512 outer at low fan-out, loses past 1k outer or past
// ~6k matches at any size; misses cost per outer regardless). matches
// estimate outer*inner/ndv when NDV is known, unknown counts as small
// (today's behavior). Both orders are row-identical by construction
// (probe verifies every pair), so this is plan choice only. The outer
// cap also bounds per-row temp growth on huge drivers.
PROBE_MAX_OUTER_ROWS  :: 512
PROBE_MAX_EST_MATCHES :: 6000

// execute_single_join runs one JOIN clause: tries the index nested-loop
// probe first for small outers (no inner materialization), otherwise
// materializes the right side and picks hash vs nested-loop. Returns the
// combined rows.
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
	if try_index_nlj(jb, jc, t, info_idx, rows, &new_rows) {
		return new_rows[:]
	}

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
		nested_loop_join(jb, jc, t, info_idx, rows, right_rows, &new_rows)
	}
	return new_rows[:]
}

// try_index_nlj attempts the index nested-loop path for equi-joins whose
// inner columns carry a usable secondary index: one index probe per outer
// row instead of materializing the whole inner side. Single COND uses a
// single-column index (or a composite led by the column); an AND of
// EQUALS conjuncts uses a composite index covering the leading run, full
// tuple preferred. Every probed pair is verified with the same ON filter the nested-loop path uses, so the probe is an index-narrowed NLJ
// by construction — it can only skip pairs NLJ would reject, never change
// which pairs match. NULL keys never probe (same as both key
// fingerprints). Returns false to fall through to the materializing
// paths. RIGHT JOIN stays out: unmatched inner rows need a full inner
// enumeration the probe never performs.
@(private = "file")
try_index_nlj :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	t: ^btree.Tree,
	info_idx: int,
	rows: []Row_Entry,
	new_rows: ^[dynamic]Row_Entry,
) -> bool {
	if jc.join_type == .RIGHT {
		return false
	}

	is_left := jc.join_type == .LEFT
	right_col_count := jb.ctxs[info_idx].range.col_count
	pairs, keys_ok := collect_join_keys(jb, jc, info_idx)
	if !keys_ok {
		return false
	}

	defer delete(pairs)
	if _, is_vt := jb.ctxs[info_idx].info.virtual.?; is_vt {
		return false
	}

	inner_table := jb.ctxs[info_idx].info.table
	probe, has_probe := make_join_probe(inner_table, pairs[:])
	if !has_probe {
		return false
	}

	defer delete(probe.outer_idx)
	inner_tree := &jb.ctxs[info_idx].info.tree
	if !probe_cost_ok(inner_tree, probe.def, len(rows)) {
		return false
	}
	// Strict key-type gate over every outer key up front: a mid-loop
	// mismatch could not fall back (rows already emitted), so any
	// heterogeneous-or-foreign key declines the whole probe.
	for outer_row in rows {
		for comp, i in probe.outer_idx {
			key := outer_row.values[comp]
			if types.is_null(key) {
				continue
			}
			if !join_key_matches_index(key, probe.def.col_types[i]) {
				return false
			}
		}
	}

	filter, filter_ok := init_join_filter(jb, jc, t)
	if !filter_ok {
		return false
	}

	idx_tree := btree.init(inner_tree.pager, probe.def.root)
	for outer_row in rows {
		if probe_row_has_null(outer_row, probe.outer_idx[:]) {
			if is_left {
				join_emit_null_row(outer_row, right_col_count, new_rows)
			}
			continue
		}

		found, probe_ok := probe_candidates(&idx_tree, probe, outer_row.values)
		if !probe_ok {
			log_index_search_fail(inner_table.name)
			clear(new_rows)
			return false
		}

		matched := false
		for rid in found {
			c, find_row_err := btree.tree_find(inner_tree, rid, context.temp_allocator)
			if find_row_err == .Cell_Not_Found {
				continue
			}
			if find_row_err != .None {
				log_index_search_fail(inner_table.name)
				clear(new_rows)
				return false
			}

			try_join_match(outer_row, c.values, filter, new_rows, &matched)
			cell.destroy(&c, context.temp_allocator)
		}
		if is_left && !matched {
			join_emit_null_row(outer_row, right_col_count, new_rows)
		}
	}
	return true
}

// probe_cost_ok applies the fitted routing rule: probe only for small
// drivers with few expected matches. est_matches = outer*inner/ndv when
// NDV is known; unknown NDV skips the matches test (today's behavior —
// the outer cap alone decides). Count errors fail open (the probe loop
// fails closed on unreadable pages, so no correctness depends on this).
@(private = "file")
probe_cost_ok :: proc(inner_tree: ^btree.Tree, def: types.Index_Def, outer: int) -> bool {
	if outer > PROBE_MAX_OUTER_ROWS {
		return false
	}

	ndv, ndv_ok := btree.index_ndv_get(inner_tree.pager, def.root)
	if !ndv_ok {
		return true
	}

	inner_count, count_err := btree.tree_count_rows(inner_tree)
	if count_err != .None {
		return true
	}

	est := u64(outer) * u64(inner_count) / u64(max(ndv, 1))
	return est <= PROBE_MAX_EST_MATCHES
}

// Join_Key_Pair is one equi-conjunct oriented outer→inner: the absolute
// outer index into the combined row and the inner-relative table position.
Join_Key_Pair :: struct {
	outer_idx: int,
	inner_pos: int,
}

// collect_join_keys splits the ON clause into equi-conjuncts (one COND,
// or an AND of CONDs) and orients each outer→inner, resolving both sides
// through the join resolver. Per-conjunct orientation may differ
// (a.x = b.x AND b.y = a.y). Anything else — OR/NOT, non-EQUALS,
// negated, literal RHS, unresolvable, or out-of-range sides (same
// multi-JOIN/junk guard as the hash path) — declines the probe.
@(private = "file")
collect_join_keys :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	info_idx: int,
) -> (
	pairs: [dynamic]Join_Key_Pair,
	ok: bool,
) {
	on_cl, has_on := jc.on_clause.?
	if !has_on || on_cl.root == nil {
		return nil, false
	}

	nodes: []^parser.Where_Node
	single := make([]^parser.Where_Node, 1, context.temp_allocator)
	multi := make([dynamic]^parser.Where_Node, 0, 4, context.temp_allocator)
	#partial switch on_cl.root.kind {
	case .COND:
		single[0] = on_cl.root
		nodes = single
	case .AND:
		for child in on_cl.root.children {
			if child.kind != .COND {
				return nil, false
			}
			append(&multi, child)
		}
		if len(multi) == 0 {
			return nil, false
		}
		nodes = multi[:]
	case:
		return nil, false
	}

	right_adjust := jb.ctxs[info_idx].range.start_col
	lcc := jb.ctxs[info_idx - 1].range.col_count
	rcc := jb.ctxs[info_idx].range.col_count
	in_left := proc(idx, lcc: int) -> bool {
		return idx >= 0 && idx < lcc
	}
	in_inner := proc(idx, adjust, rcc: int) -> bool {
		return idx >= adjust && idx < adjust + rcc
	}

	pairs = make([dynamic]Join_Key_Pair, 0, len(nodes), context.temp_allocator)
	for node in nodes {
		cond := node.cond
		if cond.negated || cond.operator != .EQUALS {
			return nil, false
		}

		rhs_str, is_col := cond.rhs.(string)
		if !is_col {
			return nil, false
		}

		a_idx, a_ok := resolve(jb.resolver, cond.column)
		b_idx, b_ok := resolve(jb.resolver, rhs_str)
		if !a_ok || !b_ok {
			return nil, false
		}
		switch {
		case in_left(a_idx, lcc) && in_inner(b_idx, right_adjust, rcc):
			append(&pairs, Join_Key_Pair{a_idx, b_idx - right_adjust})
		case in_left(b_idx, lcc) && in_inner(a_idx, right_adjust, rcc):
			append(&pairs, Join_Key_Pair{b_idx, a_idx - right_adjust})
		case:
			return nil, false
		}
	}
	return pairs, true
}

// Join_Probe is one executable probe: the index def plus the absolute
// outer indexes in def column order (length 1 for single-column).
Join_Probe :: struct {
	def      : types.Index_Def,
	outer_idx: [dynamic]int,
}

// make_join_probe maps key pairs onto a usable index: a single pair takes
// a rooted single-column index on its column, else a composite index led
// by it (prefix probe); several pairs take a rooted composite index whose
// leading columns they cover — full coverage preferred, else the longest
// leading prefix (first catalog match wins ties). Unbound trailing
// columns ride the pair verification, exactly like the fetch recheck.
@(private = "file")
make_join_probe :: proc(
	inner_table: types.Table,
	pairs: []Join_Key_Pair,
) -> (
	probe: Join_Probe,
	ok: bool,
) {
	if len(pairs) == 1 {
		col_name := inner_table.columns[pairs[0].inner_pos].name
		for d in inner_table.indexes {
			if d.root != 0 &&
			   len(d.columns) == 1 &&
			   len(d.col_types) == 1 &&
			   d.columns[0] == col_name {
				outer_idx := make([dynamic]int, 1, context.temp_allocator)
				outer_idx[0] = pairs[0].outer_idx
				return Join_Probe{d, outer_idx}, true
			}
		}
		for d in inner_table.indexes {
			if d.root != 0 &&
			   len(d.columns) >= 2 &&
			   len(d.col_types) == len(d.columns) &&
			   d.columns[0] == col_name {
				outer_idx := make([dynamic]int, 1, context.temp_allocator)
				outer_idx[0] = pairs[0].outer_idx
				return Join_Probe{d, outer_idx}, true
			}
		}
		return {}, false
	}

	by_name := make(map[string]int, len(pairs), context.temp_allocator)
	for p, i in pairs {
		by_name[inner_table.columns[p.inner_pos].name] = i
	}

	best: Join_Probe
	best_k := 0
	for d in inner_table.indexes {
		if d.root == 0 || len(d.columns) < 2 || len(d.col_types) != len(d.columns) {
			continue
		}
		// Leading run bound by the pairs; stop at the first gap (only a
		// leading prefix frames).
		k := 0
		for col in d.columns {
			if _, bound := by_name[col]; !bound {
				break
			}
			k += 1
		}
		if k == 0 {
			continue
		}
		if k == len(d.columns) {
			outer_idx := make([dynamic]int, k, context.temp_allocator)
			for col, i in d.columns {
				outer_idx[i] = pairs[by_name[col]].outer_idx
			}
			return Join_Probe{d, outer_idx}, true
		}
		if k > best_k {
			best_k = k
			outer_idx := make([dynamic]int, k, context.temp_allocator)
			for col, i in d.columns[:k] {
				outer_idx[i] = pairs[by_name[col]].outer_idx
			}
			if best.outer_idx != nil {
				delete(best.outer_idx)
			}
			best = Join_Probe{d, outer_idx}
		}
	}
	if best_k > 0 {
		return best, true
	}
	return {}, false
}

// probe_row_has_null reports whether any probe component is NULL (NULL
// never joins, so the row skips probing and null-extends on LEFT).
@(private = "file")
probe_row_has_null :: proc(outer_row: Row_Entry, outer_idx: []int) -> bool {
	for comp in outer_idx {
		if types.is_null(outer_row.values[comp]) {
			return true
		}
	}
	return false
}

// probe_candidates returns every indexed rowid matching one outer row's
// key tuple: the scalar overload for single-column probes, a framed
// full-tuple composite lookup when every index column is bound, else a
// framed leading-prefix scan (unbound trailing columns ride the pair
// verification). False on any lookup or framing failure (caller fails
// closed); the caller gates types up front so the assertions below are
// provably safe.
@(private = "file")
probe_candidates :: proc(
	idx_tree: ^btree.Tree,
	probe: Join_Probe,
	values: []types.Value,
) -> (
	found: []types.Row_ID,
	ok: bool,
) {
	if len(probe.outer_idx) == 1 && len(probe.def.columns) == 1 {
		key := values[probe.outer_idx[0]]
		hits, find_err := probe_index_key(idx_tree, probe.def, key)
		if find_err != .None {
			return nil, false
		}
		return hits, true
	}

	k := len(probe.outer_idx)
	tuple := make([]types.Value, k, context.temp_allocator)
	for comp, i in probe.outer_idx {
		tuple[i] = values[comp]
	}

	need := cell.composite_encoded_len(tuple, probe.def.col_types[:k])
	if need == 0 {
		return nil, false
	}

	buf := make([]u8, need, context.temp_allocator)
	wn, wok := cell.composite_encode_raw(tuple, probe.def.col_types[:k], buf)
	if !wok {
		return nil, false
	}
	if k == len(probe.def.columns) {
		hits, find_err := btree.composite_find_rowids(idx_tree, probe.def.root, buf[:wn])
		if find_err != .None {
			return nil, false
		}
		return hits, true
	}

	hits, scan_err := btree.composite_prefix_rowids(idx_tree, probe.def.root, buf[:wn])
	if scan_err != .None {
		return nil, false
	}
	return hits, true
}

// join_key_matches_index reports whether a non-NULL join key probes the
// index's codec exactly: the runtime storage class must equal the index
// column type. Anything else (widened numerics, foreign classes) declines
// the probe so the hash/NLJ paths decide with their own semantics.
@(private = "file")
join_key_matches_index :: proc(key: types.Value, col_type: types.Column_Type) -> bool {
	switch col_type {
	case .INTEGER:
		_, ok := key.(i64)
		return ok
	case .REAL:
		_, ok := key.(f64)
		return ok
	case .TEXT:
		_, ok := key.(string)
		return ok
	case .BLOB:
		_, ok := key.([]u8)
		return ok
	}
	return false
}

// probe_index_key returns every indexed rowid for one join key, dispatching
// the find_rowids overload by the index column type. The caller gates key
// types up front, so the assertions below are provably safe.
@(private = "file")
probe_index_key :: proc(
	idx_tree: ^btree.Tree,
	def: types.Index_Def,
	key: types.Value,
) -> (
	found: []types.Row_ID,
	err: btree.Error,
) {
	switch def.col_type {
	case .INTEGER:
		return btree.find_rowids(idx_tree, def.root, key.(i64))
	case .REAL:
		return btree.find_rowids(idx_tree, def.root, key.(f64))
	case .TEXT:
		s, _ := key.(string)
		return btree.find_rowids(idx_tree, def.root, transmute([]u8)s)
	case .BLOB:
		b, _ := key.([]u8)
		return btree.find_rowids(idx_tree, def.root, b)
	}
	return nil, .None
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
// (same fail-closed convention as evaluate_where). The schema tree lets
// IN-subqueries materialize once here instead of per pair (nil tree keeps
// the old per-row fallback — only callers without a tree may pass nil).
@(private = "file")
init_join_filter :: proc(
	jb: ^Join_Build,
	jc: parser.Join_Clause,
	t: ^btree.Tree = nil,
) -> (
	filter: Maybe(Where_Eval_Ctx),
	ok: bool,
) {
	if on_cl, has := jc.on_clause.?; has {
		filter = init_where_ctx(&on_cl, jb.cols, jb.ranges, t, context.temp_allocator)
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
	t: ^btree.Tree,
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
	filter, filter_ok := init_join_filter(jb, jc, t)
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

// order_inner_joins sorts an all-INNER join chain's arms by ascending
// table row count (FROM stays first — only JOIN arms move), so small
// drivers build first and the probe meets small outers. Exact counts via
// tree_count_rows (cached or single walk each). Any non-INNER arm,
// non-table source, or count failure keeps written order (fail-closed:
// reordering is plan choice only). Stable on ties (deterministic), and a
// no-op when already ordered.
@(private = "file")
order_inner_joins :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	ordered: []parser.Join_Clause,
	ok: bool,
) {
	if len(stmt.joins) < 2 {
		return nil, false
	}

	from_names := make([]string, 2, context.temp_allocator)
	if from_tbl, is_table := stmt.from.(string); is_table {
		from_names[0] = from_tbl
	}

	from_names[1] = stmt.from_alias
	counts := make([]int, len(stmt.joins), context.temp_allocator)
	for jc, i in stmt.joins {
		if jc.join_type != .INNER {
			return nil, false
		}

		name, is_table := jc.source.(string)
		if !is_table {
			return nil, false
		}

		alias := jc.alias
		if !arm_orderable(from_names, name, alias, jc) {
			return nil, false
		}

		info, found := schema.find_table_cached(t, name, cache)
		if !found {
			return nil, false
		}

		tree := btree.init(t.pager, info.root_page)
		n, count_err := btree.tree_count_rows(&tree)
		if count_err != .None {
			return nil, false
		}
		counts[i] = n
	}

	perm := make([]int, len(stmt.joins), context.temp_allocator)
	for i in 0 ..< len(perm) {
		perm[i] = i
	}
	for i in 1 ..< len(perm) {
		j := i
		for j > 0 && counts[perm[j]] < counts[perm[j - 1]] {
			perm[j], perm[j - 1] = perm[j - 1], perm[j]
			j -= 1
		}
	}

	moved := false
	for p, i in perm {
		if p != i {
			moved = true
			break
		}
	}
	if !moved {
		return nil, false
	}

	ordered = make([]parser.Join_Clause, len(stmt.joins), context.temp_allocator)
	for p, i in perm {
		ordered[i] = stmt.joins[p]
	}
	return ordered, true
}

// arm_orderable reports whether an INNER arm may move anywhere among
// reordered arms: every ON column reference must name FROM or the arm's
// own table/alias, so pairwise evaluation never reads a table not yet in
// the pair (out-of-range reads fail closed to "no match" — reordering
// past an unjoined table would silently drop matches). Bare columns and
// non-COND structure veto (unprovable statically); CROSS (no ON) always
// qualifies. Operators don't matter here, only references.
@(private = "file")
arm_orderable :: proc(
	from_names: []string,
	arm_table: string,
	arm_alias: string,
	jc: parser.Join_Clause,
) -> bool {
	on_cl, has_on := jc.on_clause.?
	if !has_on {
		return true
	}
	if on_cl.root == nil {
		return false
	}

	nodes: []^parser.Where_Node
	single := make([]^parser.Where_Node, 1, context.temp_allocator)
	if on_cl.root.kind == .COND {
		single[0] = on_cl.root
		nodes = single
	} else if on_cl.root.kind == .AND {
		multi := make([dynamic]^parser.Where_Node, 0, 4, context.temp_allocator)
		for child in on_cl.root.children {
			if child.kind != .COND {
				return false
			}
			append(&multi, child)
		}
		nodes = multi[:]
	} else {
		return false
	}

	known := make(map[string]bool, 4, context.temp_allocator)
	for n in from_names {
		if n != "" {
			known[n] = true
		}
	}
	if arm_table != "" {
		known[arm_table] = true
	}
	if arm_alias != "" {
		known[arm_alias] = true
	}
	for node in nodes {
		sides := make([]string, 2, context.temp_allocator)
		sides[0] = node.cond.column
		rhs_str, is_col := node.cond.rhs.(string)
		if !is_col {
			continue
		}

		sides[1] = rhs_str
		for s in sides {
			qual, _, has_qual := split_qualifier(s)
			if !has_qual || !known[qual] {
				return false
			}
		}
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
	ordered_stmt := stmt
	if ordered, reordered := order_inner_joins(t, stmt, cache); reordered {
		ordered_stmt.joins = ordered
	}
	if !resolve_join_tables(t, ordered_stmt, table_ctxs, table_ranges, cache) {
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

	jb.rows = run_join_chain(t, ordered_stmt, &jb, rows, join_filters, cache)
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
