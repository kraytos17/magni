package executor

import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// exec_subquery runs a FROM-subquery into materialized rows for the
// enclosing query by delegating to the full SELECT dispatcher (exec_query):
// single-table, join, aggregate, ORDER BY/LIMIT, and DISTINCT semantics
// inside the subquery all apply as written. ok=false when the inner query
// fails (nil rows with ok=true is a legitimate empty result — callers must
// check ok, not nilness).
@(private)
exec_subquery :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	rows: []Row_Entry,
	cols: []types.Column,
	ok: bool,
) {
	rows, cols, ok = exec_query(t, stmt, cache)
	if !ok {
		return nil, nil, false
	}
	return rows, cols, true
}

// materialize_subquery_rows applies the outer WHERE filter to the inner
// subquery result rows. Values are borrowed (all temp-allocated with the same
// lifetime) — no deep copy. Returns the rows and the virtual-column
// table-range descriptor used for resolution/display.
@(private)
materialize_subquery_rows :: proc(
	inner_rows: []Row_Entry,
	virtual_cols: []types.Column,
	stmt: parser.Select_Stmt,
) -> (
	rows: [dynamic]Row_Entry,
	single_range: []Table_Col_Range,
) {
	rows = make([dynamic]Row_Entry, 0, len(inner_rows), context.temp_allocator)
	append(&rows, ..inner_rows)
	alias := stmt.from_alias
	// NOTE: built with explicit make, not a []Table_Col_Range{...} literal:
	// slice-of-struct literals with runtime fields miscompile on odin
	// dev-2026-09-nightly at -o:none (verified segfault; struct literal + make
	// is exact and stable). Don't "simplify" this back to a literal.
	tr := Table_Col_Range {
		table_name = alias,
		start_col  = 0,
		col_count  = len(virtual_cols),
	}

	sr := make([]Table_Col_Range, 1, context.temp_allocator)
	sr[0] = tr
	if where_clause, has_where := stmt.where_clause.?; has_where {
		filtered := filter_rows(rows[:], &where_clause, virtual_cols, sr)
		clear(&rows)
		append(&rows, ..filtered)
	}
	return rows, sr
}

// exec_subquery_data evaluates a SELECT whose FROM is a subquery and returns
// the projected rows/columns without printing.
@(private)
exec_subquery_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	subq, subq_ok := stmt.from.(^parser.Select_Stmt)
	if !subq_ok {
		return nil, nil, false
	}

	inner_rows, virtual_cols, inner_ok := exec_subquery(t, subq^, cache)
	if !inner_ok {
		return nil, nil, false
	}

	rows, single_range := materialize_subquery_rows(inner_rows, virtual_cols, stmt)
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows[:], virtual_cols, single_range)
	}
	display_indices, ok := build_display_indices(
		stmt.columns,
		build_column_resolver(virtual_cols, single_range),
		len(virtual_cols),
	)
	if !ok {
		return nil, nil, false
	}
	// Sort on the full rows (so ORDER BY names outside the projected
	// columns resolve), then project — same order as exec_select_join_data.
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows[:], order_clause, virtual_cols, single_range) {
			return nil, nil, false
		}
	}

	// Project to the requested columns.
	proj_rows := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	for entry in rows {
		proj_vals := make([]types.Value, len(display_indices), context.temp_allocator)
		for idx, i in display_indices {
			proj_vals[i] = entry.values[idx]
		}
		append(&proj_rows, Row_Entry{entry.rowid, proj_vals})
	}

	proj_cols := make([]types.Column, len(display_indices), context.temp_allocator)
	for idx, i in display_indices {
		proj_cols[i] = virtual_cols[idx]
	}

	out := proj_rows[:]
	if stmt.is_distinct {
		out = dedup_rows(out)
	}
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off {
			off = o
		}

		start := int(min(off, u64(len(out))))
		end := int(min(off + limit, u64(len(out))))
		out = out[start:end]
	}
	return out, proj_cols, true
}
