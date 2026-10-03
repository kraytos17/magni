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
}

// Index_Plan is one resolved index access: the shape plus its operands.
// Strings borrow the parser's arena (statement-scoped); candidates are
// consumed synchronously, same as the dense path's temp arrays.
Index_Plan :: struct {
	use     : Index_Use,
	eq_text : string, // Eq
	prefix  : string, // Prefix stem
	in_texts: []string, // In TEXT members
}

// canonical_like_prefix extracts the stem of `LIKE 'abc%'`: non-empty,
// single trailing %, no % or _ elsewhere. Exactly the shapes where
// like_match reduces to a byte-prefix test, so index candidates and the
// recheck filter agree by construction. Interior wildcards fall back to the
// full scan (like_match's general path, behavior untouched).
@(private)
canonical_like_prefix :: proc(pattern: string) -> (stem: string, ok: bool) {
	if len(pattern) < 2 || pattern[len(pattern) - 1] != '%' { return "", false }

	stem = pattern[:len(pattern) - 1]
	if len(stem) == 0 { return "", false }
	for i in 0 ..< len(stem) {
		if stem[i] == '%' || stem[i] == '_' { return "", false }
	}
	return stem, true
}

// index_cond_column_ok matches one raw condition's column against the
// indexed column: bare name or table/alias-qualified (same rule as
// try_pk_lookup).
@(private)
index_cond_column_ok :: proc(
	table: types.Table,
	column: string,
	tbl_name: string,
	tbl_alias: string,
) -> bool {
	if column == table.index_column { return true }

	qual, col, has_qual := split_qualifier(column)
	if !has_qual || col != table.index_column { return false }
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
	table: types.Table,
	cond: parser.Condition,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if table.index_root == 0 || len(table.index_column) == 0 { return {}, false }
	if cond.negated { return {}, false }
	if !index_cond_column_ok(table, cond.column, tbl_name, tbl_alias) { return {}, false }
	if cond.operator == .EQUALS {
		val, is_val := cond.rhs.(types.Value)
		if !is_val { return {}, false }
		if s, is_text := val.(string); is_text {
			return Index_Plan{use = .Eq, eq_text = s}, true
		}
		return {}, false
	}
	if cond.operator == .LIKE {
		val, is_val := cond.rhs.(types.Value)
		if !is_val { return {}, false }

		pat, is_text := val.(string)
		if !is_text { return {}, false }
		if stem, stem_ok := canonical_like_prefix(pat); stem_ok {
			return Index_Plan{use = .Prefix, prefix = stem}, true
		}
		return {}, false
	}
	if cond.operator == .IN {
		if cond.in_subquery != nil || cond.in_values == nil { return {}, false }

		texts := make([dynamic]string, 0, len(cond.in_values), context.temp_allocator)
		for v in cond.in_values {
			if s, is_text := v.(string); is_text { append(&texts, s) }
		}
		if len(texts) == 0 { return {}, false }
		return Index_Plan{use = .In, in_texts = texts[:]}, true
	}
	return {}, false
}

// resolve_index_covering routes when the whole filter is exactly one usable
// COND on the indexed column: index output is then exact (no recheck
// possible — covering rows carry no column values), so AND chains never
// qualify here even when one conjunct is usable.
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
	cond, has_cond := where_single_condition(wc)
	if !has_cond { return {}, false }
	return index_use_of_cond(table, cond, tbl_name, tbl_alias)
}

// resolve_index_fetch routes a flat AND-chain (or a lone COND) with at
// least one usable conjunct on the indexed column, returning the first
// usable candidate plan. OR, NOT, and nested groups disable routing (same
// rule as skip_chain_conditions). The full filter rechecks every candidate,
// so extra conjuncts only narrow — never widen — the answer.
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
	if root == nil { return {}, false }
	if root.kind == .COND {
		return index_use_of_cond(table, root.cond, tbl_name, tbl_alias)
	}
	if root.kind != .AND { return {}, false }
	for child in root.children {
		if child.kind != .COND { return {}, false }
		if cand, usable := index_use_of_cond(table, child.cond, tbl_name, tbl_alias); usable {
			return cand, true
		}
	}
	return {}, false
}

// index_candidate_rowids runs a plan's btree lookups and returns sorted,
// deduplicated rowids. Rowid order is the full-scan emission order, so LIMIT
// without ORDER BY behaves identically on both paths. A btree error fails
// loudly, mirroring scan errors.
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
	idx_tree := btree.init(t.pager, table.index_root)
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
		if !lookup_eq(&idx_tree, table.index_root, plan.eq_text, &out, table.name) {
			delete(out)
			return nil, false
		}
	case .Prefix:
		found, find_err := btree.text_find_prefix(
			&idx_tree,
			table.index_root,
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
			if !lookup_eq(&idx_tree, table.index_root, s, &out, table.name) {
				delete(out)
				return nil, false
			}
		}
	case:
		delete(out)
		return nil, false
	}

	slice.sort(out[:])
	w := 0
	for r in out {
		if w == 0 || out[w - 1] != r {
			out[w] = r
			w += 1
		}
	}

	resize(&out, w)
	return out, true
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
	if !parse_ok { return echo }

	sel, is_sel := inner.type.(parser.Select_Stmt)
	if !is_sel { return echo }
	if plan := plan_select(sel); !plan.single_table { return echo }

	tbl_name, name_ok := sel.from.(string)
	if !name_ok { return echo }

	table, found := schema.find_table_cached(schema_tree, tbl_name, cache)
	if !found { return echo }
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
					table.index_column,
					index_use_name(plan.use),
				)
			}
		}
		if plan, idx_ok := resolve_index_fetch(table^, wc, tbl_name, sel.from_alias); idx_ok {
			return fmt.tprintf(
				"INDEX SCAN ON %s USING %s (%s, fetch)",
				tbl_name,
				table.index_column,
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
	if len(stmt.columns) != 1 || stmt.columns[0] != "rowid" { return false }
	if len(stmt.aggregates) != 0 || len(stmt.group_by) != 0 || stmt.having != nil {
		return false
	}

	_, has_user_rowid := schema.find_column_index(cols, "rowid")
	if has_user_rowid { return false }
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
	if !ok { return nil, nil, nil, false }
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
	if !ok { return nil, false }
	defer delete(cands)

	ctx, ctx_ok := init_where_ctx(wc, table.columns, single_range, t, allocator, cache).?
	if !ctx_ok {
		log.error("Error: Could not resolve WHERE clause")
		return nil, false
	}

	r := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		c, find_err := btree.tree_find(table_tree, rid, allocator)
		if find_err == .Cell_Not_Found { continue }
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
