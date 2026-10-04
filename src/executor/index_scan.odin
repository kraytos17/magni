package executor

import "core:fmt"
import "core:log"
import "core:slice"
import "core:strings"
import "src:btree"
import "src:cell"
import "src:parser"
import "src:schema"
import "src:types"

// Index_Use names which predicate shape an Index_Plan answers.
Index_Use :: enum u8 {
	None,
	Eq, // col = 'text'
	Prefix, // col LIKE 'stem%' (canonical: single trailing %, no %/_ in stem)
	In, // col IN ('a', ...) over literal TEXT members
	Or, // flat OR of usable same-column CONDs (union, exact)
	And, // flat AND with 2+ usable conjuncts (intersection, needs recheck)
}

// Index_Plan is one resolved index access: the shape plus its operands.
// Strings borrow the parser's arena (statement-scoped); candidates are
// consumed synchronously, same as the dense path's temp arrays.
// Or/And carry their member plans in subs (all on the indexed column by
// construction — each member passed index_use_of_cond).
Index_Plan :: struct {
	use     : Index_Use,
	root    : u32, // index root this plan reads (per-sub for Or/And)
	column  : string, // indexed column (arena-borrowed; Or/And: first sub's)
	eq_text : string, // Eq
	prefix  : string, // Prefix stem
	in_texts: []string, // In TEXT members
	subs    : []Index_Plan, // Or/And members (each with its own root)
}

// MAX_INDEX_IN_MEMBERS caps literal IN routing: each TEXT member is one
// point lookup, so an unbounded list turns the index into a slower scan.
// Above the cap the full scan wins back (identical results, one code
// path). Tunable; 128 sits far past the measured wins (3-member IN ≥20×)
// and far below any plausible crossover.
MAX_INDEX_IN_MEMBERS :: 128

// canonical_like_prefix extracts the stem of `LIKE 'abc%'`: non-empty,
// single trailing %, no % or _ elsewhere. Exactly the shapes where
// like_match reduces to a byte-prefix test, so index candidates and the
// recheck filter agree by construction. Interior wildcards fall back to the
// full scan (like_match's general path, behavior untouched).
@(private)
canonical_like_prefix :: proc(pattern: string) -> (stem: string, ok: bool) {
	if len(pattern) < 2 || pattern[len(pattern) - 1] != '%' {
		return "", false
	}

	stem = pattern[:len(pattern) - 1]
	if len(stem) == 0 {
		return "", false
	}
	for i in 0 ..< len(stem) {
		if stem[i] == '%' || stem[i] == '_' {
			return "", false
		}
	}
	return stem, true
}

// index_cond_column_ok matches one raw condition's column against the
// indexed column: bare name or table/alias-qualified (same rule as
// try_pk_lookup).
@(private)
index_cond_column_ok :: proc(
	index_column: string,
	column: string,
	tbl_name: string,
	tbl_alias: string,
) -> bool {
	if column == index_column {
		return true
	}

	qual, col, has_qual := split_qualifier(column)
	if !has_qual || col != index_column {
		return false
	}
	return qual == tbl_name || (tbl_alias != "" && qual == tbl_alias)
}

// index_use_of_cond classifies one raw COND against the indexed column.
// Negated, column-comparison (rhs is a column ref, not a Value), NULL and
// non-TEXT literals all fall through (same result via the full scan, one
// code path). IN over a literal list routes on its TEXT members: cross-class
// members provably never match a TEXT row (compare_values ranks storage
// classes apart) and NULL members never match (membership_test guards the
// row side), so the member lookups are complete. Subquery IN stays on the
// scan path (its membership materializes per resolve, not per lookup).
@(private)
index_use_of_cond :: proc(
	index_root: u32,
	index_column: string,
	cond: parser.Condition,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if index_root == 0 || len(index_column) == 0 {
		return {}, false
	}
	if cond.negated {
		return {}, false
	}
	if !index_cond_column_ok(index_column, cond.column, tbl_name, tbl_alias) {
		return {}, false
	}
	if cond.operator == .EQUALS {
		val, is_val := cond.rhs.(types.Value)
		if !is_val {
			return {}, false
		}
		if s, is_text := val.(string); is_text {
			return Index_Plan{use = .Eq, root = index_root, column = index_column, eq_text = s},
				true
		}
		return {}, false
	}
	if cond.operator == .LIKE {
		val, is_val := cond.rhs.(types.Value)
		if !is_val {
			return {}, false
		}

		pat, is_text := val.(string)
		if !is_text {
			return {}, false
		}
		if stem, stem_ok := canonical_like_prefix(pat); stem_ok {
			return Index_Plan {
					use = .Prefix,
					root = index_root,
					column = index_column,
					prefix = stem,
				},
				true
		}
		return {}, false
	}
	if cond.operator == .IN {
		if cond.in_subquery != nil || cond.in_values == nil {
			return {}, false
		}

		texts := make([dynamic]string, 0, len(cond.in_values), context.temp_allocator)
		for v in cond.in_values {
			if s, is_text := v.(string); is_text {
				append(&texts, s)
			}
		}
		if len(texts) == 0 {
			return {}, false
		}
		if len(texts) > MAX_INDEX_IN_MEMBERS {
			return {}, false
		}
		return Index_Plan {
				use = .In,
				root = index_root,
				column = index_column,
				in_texts = texts[:],
			},
			true
	}
	return {}, false
}

// resolve_index_covering routes when the filter is exactly one usable
// COND — or a flat OR of usable CONDs (union of exact sets is exact) —
// on the indexed column: index output is then exact (no recheck possible —
// covering rows carry no column values). AND chains never qualify here:
// a conjunct on another column can't be verified without the row.
@(private)
resolve_index_covering :: proc(
	table: types.Table,
	wc: parser.Where_Clause,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	root := wc.root
	if root == nil {
		return {}, false
	}
	if root.kind == .COND {
		cond, has_cond := where_single_condition(wc)
		if !has_cond {
			return {}, false
		}
		for def in table.indexes {
			if cand, usable := index_use_of_cond(def.root, def.column, cond, tbl_name, tbl_alias);
			   usable {
				return cand, true
			}
		}
		return {}, false
	}
	if root.kind == .OR {
		if or_plan, or_ok := resolve_index_or(table, root.children, tbl_name, tbl_alias); or_ok {
			return or_plan, true
		}
	}
	return {}, false
}

// resolve_index_or routes a flat OR whose every disjunct is a usable COND
// on the indexed column (union of exact sets is exact — covering-safe).
// One unusable disjunct (other column, negated, nested) falls back: its
// rows are unscannable by the index, so the union would be incomplete.
@(private)
resolve_index_or :: proc(
	table: types.Table,
	children: [dynamic]^parser.Where_Node,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if len(children) < 2 {
		return {}, false
	}

	subs := make([dynamic]Index_Plan, 0, len(children), context.temp_allocator)
	for child in children {
		if child.kind != .COND {
			return {}, false
		}

		resolved := false
		for def in table.indexes {
			if sub, usable := index_use_of_cond(
				def.root,
				def.column,
				child.cond,
				tbl_name,
				tbl_alias,
			); usable {
				append(&subs, sub)
				resolved = true
				break
			}
		}
		if !resolved {
			return {}, false
		}
	}
	return Index_Plan{use = .Or, column = subs[0].column, subs = subs[:]}, true
}

// resolve_index_fetch routes a flat AND-chain (or a lone COND) with at
// least one usable conjunct on the indexed column. Two or more usable
// conjuncts intersect (narrower candidates, fewer fetches); one behaves
// exactly as before. OR, NOT, and nested groups disable routing (same
// rule as skip_chain_conditions) — see resolve_index_or for the flat-OR
// exception, which both covering and fetch accept. The full filter
// rechecks every candidate, so extra conjuncts only narrow — never
// widen — the answer.
@(private)
resolve_index_fetch :: proc(
	table: types.Table,
	wc: parser.Where_Clause,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	root := wc.root
	if root == nil {
		return {}, false
	}
	if root.kind == .COND {
		for def in table.indexes {
			if cand, usable := index_use_of_cond(
				def.root,
				def.column,
				root.cond,
				tbl_name,
				tbl_alias,
			); usable {
				return cand, true
			}
		}
		return {}, false
	}
	if root.kind == .OR {
		return resolve_index_or(table, root.children, tbl_name, tbl_alias)
	}
	if root.kind != .AND {
		return {}, false
	}

	subs := make([dynamic]Index_Plan, 0, len(root.children), context.temp_allocator)
	for child in root.children {
		if child.kind != .COND {
			return {}, false
		}
		for def in table.indexes {
			if cand, usable := index_use_of_cond(
				def.root,
				def.column,
				child.cond,
				tbl_name,
				tbl_alias,
			); usable {
				append(&subs, cand)
				break
			}
		}
	}
	if len(subs) == 0 {
		return {}, false
	}
	if len(subs) == 1 {
		return subs[0], true
	}
	return Index_Plan{use = .And, column = subs[0].column, subs = subs[:]}, true
}

// index_candidate_rowids runs a plan's btree lookups and returns sorted,
// deduplicated rowids. Rowid order is the full-scan emission order, so LIMIT
// without ORDER BY behaves identically on both paths. Or unions member sets
// (all members usable by construction); And intersects them. A btree error
// fails loudly, mirroring scan errors.
@(private)
index_candidate_rowids :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	if plan.use == .Or || plan.use == .And {
		return index_candidate_multi(t, table, plan, allocator)
	}

	out, ok := index_candidate_single(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	sort_dedup_rowids(&out)
	return out, true
}

// sort_dedup_rowids sorts rowids in place and drops duplicates.
@(private = "file")
sort_dedup_rowids :: proc(out: ^[dynamic]types.Row_ID) {
	slice.sort(out[:])
	w := 0
	for r in out {
		if w == 0 || out[w - 1] != r {
			out[w] = r
			w += 1
		}
	}
	resize(out, w)
}

// index_candidate_multi unions (Or) or intersects (And) member candidate
// sets. Each member arrives sorted+deduped, so union reuses the single
// sort tail and intersection is a linear two-pointer walk with one owned
// accumulator (frees each consumed set as it merges).
@(private)
index_candidate_multi :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	if len(plan.subs) == 0 {
		return nil, false
	}
	if plan.use == .Or {
		out := make([dynamic]types.Row_ID, 0, 8, allocator)
		for sub in plan.subs {
			s, ok := index_candidate_single(t, table, sub, allocator)
			if !ok {
				delete(out)
				return nil, false
			}

			append(&out, ..s[:])
			delete(s)
		}

		sort_dedup_rowids(&out)
		return out, true
	}

	// And.
	acc, ok := index_candidate_single(t, table, plan.subs[0], allocator)
	if !ok {
		return nil, false
	}

	sort_dedup_rowids(&acc)
	for i in 1 ..< len(plan.subs) {
		s, s_ok := index_candidate_single(t, table, plan.subs[i], allocator)
		if !s_ok {
			delete(acc)
			return nil, false
		}

		sort_dedup_rowids(&s)
		merged := make([dynamic]types.Row_ID, 0, min(len(acc), len(s)), allocator)
		a, b := 0, 0
		for a < len(acc) && b < len(s) {
			if acc[a] == s[b] {
				append(&merged, acc[a])
				a += 1
				b += 1
			} else if acc[a] < s[b] {
				a += 1
			} else {
				b += 1
			}
		}

		delete(acc)
		delete(s)
		acc = merged
		if len(acc) == 0 {
			break
		}
	}
	return acc, true
}

// index_candidate_single runs one Eq/Prefix/In plan's btree lookups.
// Unsorted, may contain duplicates (IN overlap); the caller sorts.
@(private = "file")
index_candidate_single :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	idx_tree := btree.init(t.pager, plan.root)
	out := make([dynamic]types.Row_ID, 0, 8, allocator)
	lookup_eq :: proc(
		idx_tree: ^btree.Tree,
		root: u32,
		text: string,
		out: ^[dynamic]types.Row_ID,
		table_name: string,
	) -> bool {
		found, find_err := btree.text_find_rowids(idx_tree, root, transmute([]u8)text)
		if find_err != .None {
			log.errorf("Error: Failed to search index for '%s'", table_name)
			return false
		}

		append(out, ..found)
		return true
	}

	#partial switch plan.use {
	case .Eq:
		if !lookup_eq(&idx_tree, plan.root, plan.eq_text, &out, table.name) {
			delete(out)
			return nil, false
		}
	case .Prefix:
		found, find_err := btree.text_find_prefix(
			&idx_tree,
			plan.root,
			transmute([]u8)plan.prefix,
		)
		if find_err != .None {
			log.errorf("Error: Failed to search index for '%s'", table.name)
			delete(out)
			return nil, false
		}
		append(&out, ..found)
	case .In:
		for s in plan.in_texts {
			if !lookup_eq(&idx_tree, plan.root, s, &out, table.name) {
				delete(out)
				return nil, false
			}
		}
	case:
		delete(out)
		return nil, false
	}
	return out, true
}

// plan_index_columns renders the USING column list: the single column,
// or distinct sub columns joined for Or/And (temp-owned).
plan_index_columns :: proc(plan: Index_Plan, allocator := context.allocator) -> string {
	if plan.use != .Or && plan.use != .And {
		return plan.column
	}

	seen := make([dynamic]string, 0, len(plan.subs), context.temp_allocator)
	for sub in plan.subs {
		dup := false
		for s in seen {
			if s == sub.column {
				dup = true
				break
			}
		}
		if !dup {
			append(&seen, sub.column)
		}
	}
	return strings.join(seen[:], ", ", allocator)
}

// index_use_name renders a plan shape for EXPLAIN output.
@(private)
index_use_name :: proc(use: Index_Use) -> string {
	#partial switch use {
	case .Eq:
		return "eq"
	case .Prefix:
		return "prefix"
	case .In:
		return "in"
	case .Or:
		return "or"
	case .And:
		return "and"
	}
	return "unknown"
}

// explain_plan_text renders the access decision for EXPLAIN <select>: PK
// SEEK, INDEX SCAN (shape + covering/fetch), or FULL SCAN — mirroring the
// fetch procs' decision order (PK, covering, fetch, scan) so the output can
// never disagree with execution. Anything that isn't a single-table SELECT
// (joins, other statements, unparseable text, unknown tables) keeps the
// legacy echo of the inner SQL: no value to add, no behavior to change.
@(private)
explain_plan_text :: proc(
	schema_tree: ^btree.Tree,
	stmt: parser.Explain_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> string {
	echo := strings.trim_space(stmt.sql)
	inner, parse_ok, _ := parser.parse(stmt.sql, context.temp_allocator)
	if !parse_ok {
		return echo
	}

	sel, is_sel := inner.type.(parser.Select_Stmt)
	if !is_sel {
		return echo
	}
	if plan := plan_select(sel); !plan.single_table {
		return echo
	}

	tbl_name, name_ok := sel.from.(string)
	if !name_ok {
		return echo
	}

	table, found := schema.find_table_cached(schema_tree, tbl_name, cache)
	if !found {
		return echo
	}
	if wc, has_wc := sel.where_clause.?; has_wc {
		if _, seek_ok := try_pk_lookup(table^, wc, tbl_name, sel.from_alias); seek_ok {
			return fmt.tprintf("PK SEEK ON %s", tbl_name)
		}
		if is_covering_rowid_select(sel, table.columns) {
			if plan, idx_ok := resolve_index_covering(table^, wc, tbl_name, sel.from_alias);
			   idx_ok {
				return fmt.tprintf(
					"INDEX SCAN ON %s USING %s (%s, covering)",
					tbl_name,
					plan_index_columns(plan, context.temp_allocator),
					index_use_name(plan.use),
				)
			}
		}
		if is_covering_col_select(sel, table^) {
			if plan, idx_ok := resolve_index_covering(table^, wc, tbl_name, sel.from_alias);
			   idx_ok && covering_known_values(plan) {
				return fmt.tprintf(
					"INDEX SCAN ON %s USING %s (%s, covering)",
					tbl_name,
					plan_index_columns(plan, context.temp_allocator),
					index_use_name(plan.use),
				)
			}
		}
		if plan, idx_ok := resolve_index_fetch(table^, wc, tbl_name, sel.from_alias); idx_ok {
			return fmt.tprintf(
				"INDEX SCAN ON %s USING %s (%s, fetch)",
				tbl_name,
				plan_index_columns(plan, context.temp_allocator),
				index_use_name(plan.use),
			)
		}
	}
	return fmt.tprintf("FULL SCAN ON %s", tbl_name)
}

// is_covering_rowid_select reports whether the statement is literally
// `SELECT rowid` (one projected column, no aggregates/grouping) on a table
// with no user column named "rowid" — which keeps today's meaning there
// (the projection would resolve to the user column, so index routing must
// not hijack it).
@(private)
is_covering_rowid_select :: proc(stmt: parser.Select_Stmt, cols: []types.Column) -> bool {
	if len(stmt.columns) != 1 || stmt.columns[0] != "rowid" {
		return false
	}
	if len(stmt.aggregates) != 0 || len(stmt.group_by) != 0 || stmt.having != nil {
		return false
	}

	_, has_user_rowid := schema.find_column_index(cols, "rowid")
	if has_user_rowid {
		return false
	}
	return true
}

// fetch_covering_index answers a covering `SELECT rowid ... WHERE <routed>`
// straight from the text index. Rows carry synthetic single-INTEGER-column
// values named "rowid"; the caller's finish_select tail (projection,
// ORDER BY, DISTINCT, LIMIT) runs unchanged.
@(private)
fetch_covering_index :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	from_name: string,
	allocator := context.allocator,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	bool,
) {
	cands, ok := index_candidate_rowids(t, table, plan, allocator)
	if !ok {
		return nil, nil, nil, false
	}

	defer delete(cands)
	rows := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		vals := make([]types.Value, 1, allocator)
		vals[0] = types.value_int(i64(rid))
		append(&rows, Row_Entry{rowid = rid, values = vals})
	}

	cols := make([]types.Column, 1, allocator)
	cols[0] = types.Column {
		name = "rowid",
		type = .INTEGER,
	}

	ranges := make([]Table_Col_Range, 1, allocator)
	ranges[0] = Table_Col_Range {
		table_name = from_name,
		start_col  = 0,
		col_count  = 1,
	}
	return rows[:], cols, ranges, true
}

// is_covering_col_select reports whether the statement projects exactly
// the indexed TEXT column (bare name, no aggregates/grouping). Like the
// rowid rule, one projected column only — wider projections take the
// fetch path with its full-filter recheck.
@(private)
is_covering_col_select :: proc(stmt: parser.Select_Stmt, table: types.Table) -> bool {
	if len(stmt.columns) != 1 {
		return false
	}
	if len(stmt.aggregates) != 0 || len(stmt.group_by) != 0 || stmt.having != nil {
		return false
	}
	for def in table.indexes {
		if def.root != 0 && stmt.columns[0] == def.column {
			return true
		}
	}
	return false
}

// covering_known_values reports whether a covering plan's output values
// are known without touching data pages: Eq/In carry their keys; Or of
// Eq/In unions known keys. Prefix (and And, which never reaches covering)
// needs the row. Shared with both fetch paths and EXPLAIN so the three
// can never disagree.
@(private)
covering_known_values :: proc(plan: Index_Plan) -> bool {
	#partial switch plan.use {
	case .Eq, .In:
		return true
	case .Or:
		for sub in plan.subs {
			if sub.use != .Eq && sub.use != .In {
				return false
			}
		}
		return len(plan.subs) > 0
	}
	return false
}

// Covering_Pair is one covering row: its rowid plus the known TEXT value.
@(private = "file")
Covering_Pair :: struct {
	rid: types.Row_ID,
	val: string,
}

// covering_pairs resolves a known-values covering plan to (rowid, text)
// pairs, one per matching row. Sorted by rowid (scan emission order),
// deduplicated (duplicate IN members).
@(private = "file")
covering_pairs :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]Covering_Pair,
	bool,
) {
	out := make([dynamic]Covering_Pair, 0, 8, allocator)
	emit :: proc(
		t: ^btree.Tree,
		table: ^types.Table,
		root: u32,
		text: string,
		out: ^[dynamic]Covering_Pair,
		allocator := context.allocator,
	) -> bool {
		idx_tree := btree.init(t.pager, root)
		found, find_err := btree.text_find_rowids(&idx_tree, root, transmute([]u8)text)
		if find_err != .None {
			log.errorf("Error: Failed to search index for '%s'", table.name)
			return false
		}
		for rid in found {
			append(out, Covering_Pair{rid = rid, val = text})
		}
		return true
	}

	#partial switch plan.use {
	case .Eq:
		if !emit(t, table, plan.root, plan.eq_text, &out, allocator) {
			delete(out)
			return nil, false
		}
	case .In:
		for s in plan.in_texts {
			if !emit(t, table, plan.root, s, &out, allocator) {
				delete(out)
				return nil, false
			}
		}
	case .Or:
		for sub in plan.subs {
			#partial switch sub.use {
			case .Eq:
				if !emit(t, table, sub.root, sub.eq_text, &out, allocator) {
					delete(out)
					return nil, false
				}
			case .In:
				for s in sub.in_texts {
					if !emit(t, table, sub.root, s, &out, allocator) {
						delete(out)
						return nil, false
					}
				}
			case:
				delete(out)
				return nil, false
			}
		}
	case:
		delete(out)
		return nil, false
	}

	slice.sort_by(out[:], proc(a, b: Covering_Pair) -> bool { return a.rid < b.rid })
	w := 0
	for i in 0 ..< len(out) {
		if w == 0 || out[w - 1].rid != out[i].rid {
			out[w] = out[i]
			w += 1
		}
	}

	resize(&out, w)
	return out, true
}

// fetch_covering_col answers `SELECT <indexed-col> ... WHERE <routed>`
// straight from the text index (Eq/In keys, or Or thereof). Same tail
// contract as fetch_covering_index: single TEXT column, rowid-ordered
// rows, caller slices LIMIT.
@(private)
fetch_covering_col :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	col_name: string,
	from_name: string,
	allocator := context.allocator,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	bool,
) {
	pairs, ok := covering_pairs(t, table, plan, allocator)
	if !ok {
		return nil, nil, nil, false
	}

	defer delete(pairs)
	rows := make([dynamic]Row_Entry, 0, len(pairs), allocator)
	for p in pairs {
		vals := make([]types.Value, 1, allocator)
		// Cloned per row: plan strings borrow the parser arena and
		// downstream tails (sort/distinct) reorder freely.
		vals[0] = types.value_text(strings.clone(p.val, allocator))
		append(&rows, Row_Entry{rowid = p.rid, values = vals})
	}

	cols := make([]types.Column, 1, allocator)
	cols[0] = types.Column {
		name = strings.clone(col_name, allocator),
		type = .TEXT,
	}

	ranges := make([]Table_Col_Range, 1, allocator)
	ranges[0] = Table_Col_Range {
		table_name = from_name,
		start_col  = 0,
		col_count  = 1,
	}
	return rows[:], cols, ranges, true
}

// fetch_index_rows resolves a fetch plan's candidates through the data tree
// and rechecks the FULL WHERE filter per row (mandatory: the index only
// promises the usable conjunct). Misses are tolerated-and-skipped (mirrors
// delete's stale-entry stance); btree errors fail loudly. Ownership mirrors
// the scan loop: values transfer to the entry, the deferred destroy is
// disarmed. No LIMIT pushdown: all candidates materialize and the caller's
// tails slice — candidates arrive in rowid order, so the slice matches the
// scan path exactly.
@(private)
fetch_index_rows :: proc(
	t: ^btree.Tree,
	table_tree: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	wc: ^parser.Where_Clause,
	single_range: []Table_Col_Range,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	bool,
) {
	cands, ok := index_candidate_rowids(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	defer delete(cands)
	ctx, ctx_ok := init_where_ctx(wc, table.columns, single_range, t, allocator, cache).?
	if !ctx_ok {
		log.error("Error: Could not resolve WHERE clause")
		return nil, false
	}

	r := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		c, find_err := btree.tree_find(table_tree, rid, allocator)
		if find_err == .Cell_Not_Found {
			continue
		}
		if find_err != .None {
			log.errorf("Error: Failed to fetch indexed row %d", i64(rid))
			return nil, false
		}
		if !evaluate_where_ctx(ctx, c.values) {
			cell.destroy(&c, allocator)
			continue
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		cell.destroy(&c, allocator)
	}
	return r[:], true
}
