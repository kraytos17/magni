package executor

import "core:fmt"
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

exec_select :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	plan := plan_select(stmt)
	if plan.is_literal_only {
		rows, cols, ok := exec_select_literals(t, stmt)
		if !ok { return false }

		indices := make([]int, len(cols), context.temp_allocator)
		for i in 0 ..< len(cols) { indices[i] = i }

		display_results(rows, cols, indices, nil, nil, stmt.aliases)
		return true
	}

	if plan.has_subquery { return exec_select_subquery(t, stmt) }
	if plan.single_table { return exec_select_single(t, stmt) }

	rows, combined_cols, table_ranges, total_cols, ok := build_join_result(t, stmt)
	if !ok { return false }
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_combined(stmt, rows, combined_cols, table_ranges)
	}

	display_indices, d_ok := build_display_indices(
		stmt.columns,
		combined_cols,
		table_ranges,
		total_cols,
	)

	if !d_ok { return false }
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) {
			return false
		}
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }

	display_results(rows, combined_cols, display_indices[:], stmt.limit, stmt.offset, stmt.aliases)
	return true
}

// single_range_for returns a single flat table-range covering all columns —
// used when a result has no per-table column ranges (compound results, joins).
@(private="file")
single_range_for :: proc(col_count: int) -> []Table_Col_Range {
	range0 := []Table_Col_Range {
		{table_name = "", start_col = 0, col_count = col_count},
	}
	return range0
}

// select_header_names returns the display names for a SELECT's columns,
// substituting AS aliases where present.
@(private)
select_header_names :: proc(stmt: parser.Select_Stmt) -> []string {
	names := make([dynamic]string, 0, len(stmt.columns), context.temp_allocator)
	for col, i in stmt.columns {
		if i < len(stmt.aliases) && stmt.aliases[i] != "" {
			append(&names, stmt.aliases[i])
		} else {
			append(&names, col)
		}
	}
	return names[:]
}

@(private)
// fetch_single_rows scans a single-table SELECT (no joins), applying the WHERE
// filter with LIMIT pushdown, and returns the rows, the table's columns, and
// the single table-range descriptor. Aggregate routing, sort/dedup, projection,
// and display are left to the caller (printing vs data variants differ there).
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
	max_rows := stmt.limit if has_lim && !has_order else nil
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

@(private)
// apply_sort_dedup applies ORDER BY and DISTINCT to a result set, in that
// order (matching the standard pipeline). Returns false on sort failure.
apply_sort_dedup :: proc(
	rows: []Row_Entry,
	stmt: parser.Select_Stmt,
	cols: []types.Column,
	single_range: []Table_Col_Range,
	allocator := context.allocator,
) -> (out: []Row_Entry, ok: bool) {
	out = rows
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(out, order_clause, cols, single_range) { return nil, false }
	}
	if stmt.is_distinct { out = dedup_rows(out) }
	return out, true
}

@(private="file")
exec_select_single :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found { log.errorf("Error: Table not found: %s", tbl_name); return false }
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
	}
	// Fast path: SELECT COUNT(*) FROM table (no WHERE, GROUP BY, DISTINCT, ORDER BY, LIMIT)
	if len(stmt.aggregates) == 1 &&
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
			return false
		}

		rows_mat := make([][]string, 1, context.temp_allocator)
		row_strs := make([]string, len(stmt.columns), context.temp_allocator)
		for i in 0 ..< len(stmt.columns) {
			row_strs[i] = fmt.aprintf("%d", i64(count), allocator = context.temp_allocator)
		}

		rows_mat[0] = row_strs
		render_table(select_header_names(stmt), rows_mat)
		fmt.printf("(%d rows)\n", 1)
		return true
	}

	rows, cols, single_range, f_ok := fetch_single_rows(
		t,
		table,
		tbl_name,
		stmt,
		context.temp_allocator,
	)
	if !f_ok { return false }
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		return exec_select_aggregate_combined(stmt, rows, cols, single_range)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		cols,
		single_range,
		len(cols),
	)
	if !ok { return false }

	rows, ok = apply_sort_dedup(rows, stmt, cols, single_range)
	if !ok { return false }
	display_results(rows, cols, display_indices, stmt.limit, stmt.offset, stmt.aliases)
	return true
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

	sorted_rows, ok := apply_sort_dedup(rows, stmt, cols, single_range)
	if !ok { return nil, nil, false }

	rows = sorted_rows
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off { off = o }

		start := int(min(off, u64(len(rows))))
		end := int(min(off + limit, u64(len(rows))))
		rows = rows[start:end]
	}
	// Project to the requested columns (e.g. `SELECT c FROM u` on a multi-column
	// table returns only column c). Full projection matches exec_select_single.
	if len(stmt.columns) > 0 {
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
		return proj[:], proj_cols, true
	}
	return rows, cols, true
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
	r := make([dynamic]Row_Entry, allocator)
	where_ctx: Maybe(Where_Eval_Ctx)
	if wc, has_wc := where_clause.?; has_wc {
		where_ctx = init_where_ctx(&wc, table.columns, nil, schema_tree, allocator, cache)
	}

	use_where := false
	where_ctx_val: Where_Eval_Ctx
	if ctx, ok := where_ctx.?; ok && ctx.root != nil {
		use_where = true
		where_ctx_val = ctx
	}

	// Skip-index bounds are only safe for a top-level AND chain of single-column
	// integer comparisons on the indexed column. OR subtrees (or nested boolean
	// groups) disable skipping; the operator decides which side of the page
	// window a condition can bound (e.g. `>` only gives a lower bound, `<` only
	// an upper bound).
	skip_conds: []Resolved_Condition
	if use_where {
		skip_conds = skip_chain_conditions(where_ctx_val.root)
	}

	skip_start, skip_end: u32
	if use_where && table.skip_root > 0 {
		for rc in skip_conds {
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
					if start > skip_start { skip_start = start }
					if end > 0 && (skip_end == 0 || end < skip_end) { skip_end = end }
				}
			}
		}
	}

	cursor, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None { return nil, true }
	if skip_start > 0 {
		if seek_err := btree.cursor_seek_to_page(&cursor, skip_start); seek_err != .None {
			btree.cursor_destroy(&cursor)
			cursor, c_err = btree.cursor_start(tree, allocator)
			if c_err != .None { return nil, true }
		}
	}
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		if skip_end > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > skip_end { break }
		}

		c, get_err := btree.cursor_get_cell(&cursor, allocator)
		defer cell.destroy(&c, allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if use_where {
			if !evaluate_where_ctx(where_ctx_val, c.values) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		if limit, has_limit := max_rows.?; has_limit && u64(len(r)) >= limit { break }
		btree.cursor_advance(&cursor)
	}
	if use_where && table.skip_root == 0 && schema_tree != nil {
		for rc in skip_conds {
			if rc.has_right_col || rc.has_in { continue }
			if _, is_int := rc.rhs.(i64); !is_int { continue }
			if _, op_ok := skip_op_from_token(rc.operator); !op_ok { continue }

			col_idx := rc.col_idx
			skip_idx, build_err := btree.build_skip_index(tree, col_idx)
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
	return r[:], false
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
