package executor

import "core:log"
import "src:btree"
import "src:cell"
import "src:parser"
import "src:schema"
import "src:types"

// Select_Plan describes which stages a given SELECT execution needs, decided
// once by the top-level dispatch instead of being re-derived inside every
// exec_select_* variant.
Select_Plan :: struct {
	has_join:       bool,
	has_aggregate:  bool,
	has_group_by:   bool,
	has_subquery:   bool,
	is_literal_only: bool,
	single_table:   bool,
}

plan_select :: proc(stmt: parser.Select_Stmt) -> Select_Plan {
	if _, is_nf := stmt.from.(parser.No_From); is_nf {
		return Select_Plan{is_literal_only = true}
	}
	if _, is_subq := stmt.from.(^parser.Select_Stmt); is_subq {
		return Select_Plan{has_subquery = true}
	}

	has_join := len(stmt.joins) > 0
	has_agg := len(stmt.aggregates) > 0
	has_group := len(stmt.group_by) > 0
	return Select_Plan{
		has_join       = has_join,
		has_aggregate  = has_agg,
		has_group_by   = has_group,
		has_subquery   = false,
		is_literal_only = false,
		single_table   = !has_join,
	}
}

@(private)
exec_select_literals :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	// A bare SELECT without FROM resolves every projection to a literal at
	// parse time. A column reference (e.g. `SELECT k`) yields a column name
	// with no corresponding literal value — that is a clean error, not a
	// short row (which would panic the renderers indexing values[i]).
	if len(stmt.literal_values) != len(stmt.columns) {
		if len(stmt.literal_values) < len(stmt.columns) {
			log.errorf("Error: Unknown column: %s", stmt.columns[len(stmt.literal_values)])
		} else {
			log.error("Error: Column/value count mismatch in SELECT without FROM")
		}
		return nil, nil, false
	}

	row := Row_Entry{rowid = 1, values = stmt.literal_values}
	cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
	for name, i in stmt.columns {
		cols[i] = types.Column {
			name = name,
			type = .INTEGER,
		}
	}

	rows := make([]Row_Entry, 1, context.temp_allocator)
	rows[0] = row
	return rows, cols, true
}

@(private="file")
single_range_for :: proc(col_count: int) -> []Table_Col_Range {
	range0 := []Table_Col_Range {
		{table_name = "", start_col = 0, col_count = col_count},
	}
	return range0
}

@(private)
// fetch_single_rows scans a single-table SELECT (no joins), applying the WHERE
// filter with LIMIT pushdown, and returns the rows, the table's columns, and
// the single table-range descriptor. Aggregate routing, sort/dedup, projection,
// and display are left to the caller.
fetch_single_rows :: proc(
	t: ^btree.Tree,
	table: types.Table,
	tbl_name: string,
	stmt: parser.Select_Stmt,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	bool,
) {
	tbl := table
	table_tree := btree.init(t.pager, tbl.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
	}

	_, has_lim := stmt.limit.?
	// LIMIT pushdown is only valid for plain row-returning scans. ORDER BY
	// and DISTINCT need the full row set, and aggregates/GROUP BY/HAVING
	// change cardinality — pushing LIMIT into those scans silently truncates
	// the aggregation input.
	pushable := has_lim && !has_order && !stmt.is_distinct &&
		len(stmt.aggregates) == 0 && len(stmt.group_by) == 0 && stmt.having == nil

	max_rows := stmt.limit if pushable else nil
	rows, scan_err := scan_table(
		&table_tree,
		&tbl,
		stmt.where_clause,
		max_rows,
		t,
		allocator,
		cache,
	)
	if scan_err { return nil, nil, nil, false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(tbl.columns)},
	}
	return rows, tbl.columns, single_range, true
}

exec_select_single_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil, false }

	table, found := schema.find_table_cached(t, tbl_name, cache)
	if !found {
		log.errorf("Error: Table not found: %s", tbl_name)
		return nil, nil, false
	}

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
	}
	// Fast path: SELECT COUNT(*) FROM table (exactly one projected column, no
	// WHERE, GROUP BY, DISTINCT, ORDER BY, LIMIT). The single-column check is
	// load-bearing: companion columns (e.g. SELECT 0, COUNT(*)) must take the
	// general aggregate path, never be silently dropped here.
	if len(stmt.aggregates) == 1 &&
	   len(stmt.columns) == 1 &&
	   stmt.aggregates[0].func == .COUNT &&
	   stmt.aggregates[0].column == "" &&
	   stmt.where_clause == nil &&
	   len(stmt.group_by) == 0 &&
	   stmt.having == nil &&
	   !stmt.is_distinct &&
	   !has_order &&
	   stmt.limit == nil &&
	   stmt.offset == nil {
		count, count_err := btree.tree_count_rows(&table_tree)
		if count_err != .None {
			log.error("Error: Failed to count rows")
			return nil, nil, false
		}

		vals := make([]types.Value, 1, context.temp_allocator)
		vals[0] = types.value_int(i64(count))
		rows_mat := make([]Row_Entry, 1, context.temp_allocator)
		rows_mat[0] = Row_Entry{rowid = 1, values = vals}

		name := "COUNT(*)"
		if len(stmt.columns) > 0 { name = stmt.columns[0] }
		if len(stmt.aliases) > 0 && stmt.aliases[0] != "" { name = stmt.aliases[0] }

		cols_mat := make([]types.Column, 1, context.temp_allocator)
		cols_mat[0] = types.Column{name = name, type = .INTEGER}
		return rows_mat, cols_mat, true
	}

	rows, cols, single_range, f_ok := fetch_single_rows(
		t,
		table^,
		tbl_name,
		stmt,
		context.temp_allocator,
		cache,
	)

	if !f_ok { return nil, nil, false }
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_data(stmt, rows, cols, single_range)
	}
	// Sort on the full rows (so ORDER BY names outside the projected columns
	// resolve), then project, dedup, and limit — same order as
	// exec_select_join_data.
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, cols, single_range) {
			return nil, nil, false
		}
	}
	if len(stmt.columns) == 0 {
		out := rows
		if stmt.is_distinct { out = dedup_rows(out) }
		if limit, has_limit := stmt.limit.?; has_limit {
			off := u64(0)
			if o, has_off := stmt.offset.?; has_off { off = o }

			start := int(min(off, u64(len(out))))
			end := int(min(off + limit, u64(len(out))))
			out = out[start:end]
		}
		return out, cols, true
	}
	// Project to the requested columns (e.g. `SELECT c FROM u` on a multi-column
	// table returns only column c).
	indices, i_ok := build_display_indices(
		stmt.columns,
		cols,
		single_range,
		len(cols),
	)
	if !i_ok { return nil, nil, false }

	proj := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	for entry in rows {
		vals := make([]types.Value, len(indices), context.temp_allocator)
		for idx, i in indices { vals[i] = entry.values[idx] }
		append(&proj, Row_Entry{entry.rowid, vals})
	}

	proj_cols := make([]types.Column, len(indices), context.temp_allocator)
	for idx, i in indices {
		proj_cols[i] = cols[idx]
		if i < len(stmt.aliases) && stmt.aliases[i] != "" {
			proj_cols[i].name = stmt.aliases[i]
		}
	}

	out := proj[:]
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

exec_query :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	plan := plan_select(stmt)
	if plan.is_literal_only {
		return exec_select_literals(t, stmt)
	}
	if plan.has_subquery {
		return exec_subquery_data(t, stmt, cache)
	}
	if plan.single_table {
		return exec_select_single_data(t, stmt, cache)
	}
	return exec_select_join_data(t, stmt, cache)
}

@(private)
skip_op_from_token :: proc(op: parser.Token_Type) -> (btree.Skip_Op, bool) {
	#partial switch op {
	case .EQUALS:
		return .EQ, true
	case .LESS_THAN:
		return .LT, true
	case .LESS_EQUAL:
		return .LTE, true
	case .GREATER_THAN:
		return .GT, true
	case .GREATER_EQUAL:
		return .GTE, true
	}
	return .EQ, false
}

@(private)
scan_table :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	where_clause: Maybe(parser.Where_Clause),
	max_rows: Maybe(u64),
	schema_tree: ^btree.Tree = nil,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	rows: []Row_Entry,
	err: bool,
) {
	plan := build_scan_plan(tree, table, where_clause, max_rows, schema_tree, allocator, cache)
	r := make([dynamic]Row_Entry, allocator)

	cursor, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None { return nil, true }
	if plan.skip_start > 0 {
		if seek_err := btree.cursor_seek_to_page(&cursor, plan.skip_start); seek_err != .None {
			btree.cursor_destroy(&cursor)
			cursor, c_err = btree.cursor_start(tree, allocator)
			if c_err != .None { return nil, true }
		}
	}
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		if plan.skip_end > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > plan.skip_end { break }
		}

		c, get_err := btree.cursor_get_cell(&cursor, allocator)
		defer cell.destroy(&c, allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if f, has_f := plan.filter.?; has_f {
			if !evaluate_where_ctx(f, c.values) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		if limit, has_limit := plan.max_rows.?; has_limit && u64(len(r)) >= limit { break }
		btree.cursor_advance(&cursor)
	}

	maybe_build_skip_index(&plan, tree, table, schema_tree)
	return r[:], false
}

// build_scan_plan resolves the WHERE clause once and computes skip-index
// page bounds. A filter with nil root normalizes to nil (no filtering).
@(private="file")
build_scan_plan :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	where_clause: Maybe(parser.Where_Clause),
	max_rows: Maybe(u64),
	schema_tree: ^btree.Tree,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> Scan_Plan {
	plan := Scan_Plan{max_rows = max_rows}
	if wc, has_wc := where_clause.?; has_wc {
		if ctx, ok := init_where_ctx(&wc, table.columns, nil, schema_tree, allocator, cache).?; ok && ctx.root != nil {
			plan.filter = ctx
		}
	}
	if _, has_f := plan.filter.?; !has_f { return plan }

	// Skip-index bounds are only safe for a top-level AND chain of single-column
	// integer comparisons on the indexed column. OR subtrees (or nested boolean
	// groups) disable skipping; the operator decides which side of the page
	// window a condition can bound (e.g. `>` only gives a lower bound, `<` only
	// an upper bound).
	plan.skip_conds = skip_chain_conditions(plan.filter.?.root)
	if table.skip_root == 0 { return plan }
	for rc in plan.skip_conds {
		if rc.has_right_col || rc.has_in { continue }
		if val, is_int := rc.rhs.(i64); is_int {
			op, op_ok := skip_op_from_token(rc.operator)
			if !op_ok { continue }

			start, end, found := btree.query_skip_index_range(
				tree.pager,
				table.skip_root,
				rc.col_idx,
				op,
				val,
			)
			if found {
				if start > plan.skip_start { plan.skip_start = start }
				if end > 0 && (plan.skip_end == 0 || end < plan.skip_end) { plan.skip_end = end }
			}
		}
	}
	return plan
}

// maybe_build_skip_index auto-builds a skip index for the first qualifying
// integer column when the table has none yet.
@(private="file")
maybe_build_skip_index :: proc(
	plan: ^Scan_Plan,
	tree: ^btree.Tree,
	table: ^types.Table,
	schema_tree: ^btree.Tree,
) {
	if _, has_f := plan.filter.?; !has_f { return }
	if table.skip_root != 0 || schema_tree == nil { return }
	for rc in plan.skip_conds {
		if rc.has_right_col || rc.has_in { continue }
		if _, is_int := rc.rhs.(i64); !is_int { continue }
		if _, op_ok := skip_op_from_token(rc.operator); !op_ok { continue }

		skip_idx, build_err := btree.build_skip_index(tree, rc.col_idx)
		if build_err == .None {
			new_schema_root, ok := schema.update_skip_root_cow(
				schema_tree,
				table.name,
				skip_idx.root,
			)
			if ok {
				schema_tree.root = new_schema_root
			}
		}
		break // only build for the first qualifying column
	}
}

// skip_chain_conditions collects the leaf conditions of a top-level AND chain.
// Skipping is only safe when the predicate is a flat conjunction of comparisons;
// OR subtrees or nested boolean groups disable the optimization (empty result).
@(private="file")
skip_chain_conditions :: proc(root: ^Resolved_Node) -> []Resolved_Condition {
	chain := make([dynamic]Resolved_Condition, context.temp_allocator)
	collect_skip_chain(root, &chain)
	return chain[:]
}

@(private="file")
collect_skip_chain :: proc(node: ^Resolved_Node, out: ^[dynamic]Resolved_Condition) {
	if node == nil { return }
	switch node.kind {
	case .COND:
		append(out, node.cond)
	case .AND:
		for child in node.children {
			if child.kind != .COND {
				clear(out)
				return
			}
			append(out, child.cond)
		}
	case .OR:
		clear(out)
	case .NOT:
		clear(out)
	}
}
