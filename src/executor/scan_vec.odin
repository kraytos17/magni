// Package executor — vector scan path.
//
// Design: late materialization with a row-materialized boundary. Each row
// decodes needed columns only into a reused stack buffer (zero
// allocations), the existing scalar filter evaluators run on that buffer to
// decide survival, and only survivors are materialized into owned
// Row_Entry values — with text/blob cloned at materialize time. Downstream
// (sort/dedup/group/join/setops/render) keeps consuming []Row_Entry
// unchanged.
//
// Why row-at-a-time, not column batches: the Row_Entry boundary makes one
// owned slice per survivor mandatory, so column buffers cannot reduce the
// survivor alloc count — they would only add machinery. The census targets
// (non-survivors → 0 allocs, no projection copy, skipped-col bytes,
// survivor-only clones) are all met by filter-early + fused allocation.
// Column batches stay deferred until a consumer can use them directly.
//
// O1 — page/borrow lifetime (proven, not assumed): the cursor pins exactly
// one page (Cursor.cached_page_id; load_cached_page unpins on page move,
// cursor.odin:203-231) and pager eviction reuses the slot buffer in place
// (evict_slot, pager.odin:342-358). A zero-copy borrow is therefore valid
// only while the cursor stays on the page. Consequence: filter buffers may
// borrow, but survivor materialization MUST clone text/blob
// (types.value_clone) before cursor_advance. Non-survivors cost zero
// allocations by construction. Never retain a borrow across pages.
//
// Rollout: gated by vec_scan_enabled (MAGNI_VECTOR=1). Default off until
// the benchmark gate passes; the scalar scan_table stays the default path.
package executor

import "core:os"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// vec_scan_enabled reports whether the vector scan path may be used.
// Default off (empty/unset); set MAGNI_VECTOR=1 to enable for A/B runs.
// Mirrors the MAGNI_PAGER_STATS opt-in pattern.
vec_scan_enabled :: proc() -> bool {
	return os.get_env("MAGNI_VECTOR", context.temp_allocator) == "1"
}

// collect_needed_cols returns a length-total_cols mask of the columns a
// scan must decode: every column referenced by the filter tree (col_idx
// plus right_idx for column-column comparisons) unioned with the
// projection and sort indices. Out-of-range indices are ignored
// defensively. A nil filter root contributes nothing. Unreferenced
// trailing positions stay false; callers size decode buffers from this.
collect_needed_cols :: proc(
	root: ^Resolved_Node,
	proj_indices: []int,
	sort_indices: []int,
	total_cols: int,
	allocator := context.temp_allocator,
) -> []bool {
	if total_cols <= 0 { return nil }
	needed := make([]bool, total_cols, allocator)
	if root != nil {
		collect_node_cols(root, needed)
	}
	for idx in proj_indices {
		if idx >= 0 && idx < total_cols { needed[idx] = true }
	}
	for idx in sort_indices {
		if idx >= 0 && idx < total_cols { needed[idx] = true }
	}
	return needed
}

// collect_node_cols marks every column index referenced anywhere in the
// filter tree, including under OR/NOT (any reachable comparison may need
// its column). IN-list/subquery memberships need no extra columns beyond
// col_idx: their candidate values are constants, not row references.
@(private="file")
collect_node_cols :: proc(node: ^Resolved_Node, needed: []bool) {
	if node == nil { return }
	switch node.kind {
	case .COND:
		if node.cond.col_idx >= 0 && node.cond.col_idx < len(needed) {
			needed[node.cond.col_idx] = true
		}
		if node.cond.has_right_col &&
		   node.cond.right_idx >= 0 && node.cond.right_idx < len(needed) {
			needed[node.cond.right_idx] = true
		}
	case .AND, .OR, .NOT:
		for child in node.children {
			collect_node_cols(child, needed)
		}
	}
}

// scan_table_vec is the vector counterpart of scan_table: same plan
// construction (shared build_scan_plan, so skip bounds, LIMIT pushdown
// counting, and filter resolution cannot diverge), same row order, same
// error shape — but each row decodes needed columns only into a reused
// stack buffer and only survivors allocate. The filter runs on the
// full-width buffer via the existing scalar evaluators (unneeded slots are
// Null, and every referenced column is decoded by construction of the
// needed mask), so filter semantics are identical by reuse, not by
// reimplementation. Survivors materialize full-width with text/blob
// cloned; finish_select (projection/sort/distinct/limit) runs unchanged
// downstream, exactly as on scalar rows.
scan_table_vec :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	stmt: parser.Select_Stmt,
	max_rows: Maybe(u64),
	schema_tree: ^btree.Tree = nil,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
	table_ranges: []Table_Col_Range = nil,
) -> (
	rows: []Row_Entry,
	err: bool,
) {
	plan, plan_ok := build_scan_plan(
		tree,
		table,
		stmt.where_clause,
		max_rows,
		schema_tree,
		allocator,
		cache,
		table_ranges,
	)
	if !plan_ok {
		return nil, true
	}

	// Needed mask: filter columns from the resolved plan plus sort keys
	// (ORDER BY sorts full rows before projection, so sort keys must
	// decode even when unprojected) plus the projection itself (T2a
	// materializes full-width survivors for the shared finish_select tail,
	// so every column finish_select might read must decode).
	total_cols := len(table.columns)
	resolver := build_column_resolver(table.columns, table_ranges)
	sort_indices: []int
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		si, si_ok := resolve_sort_indices(order_clause, resolver)
		if !si_ok {
			// Logged canonically inside resolve_sort_indices, same outcome
			// as the scalar path failing later in sort_rows.
			return nil, true
		}
		sort_indices = si
	}
	proj_indices: []int
	if len(stmt.columns) > 0 {
		pi, pi_ok := build_display_indices(stmt.columns, resolver, len(table.columns))
		if !pi_ok {
			// Logged canonically inside build_display_indices, same
			// outcome as the scalar finish_select failing on projection.
			return nil, true
		}
		proj_indices = pi
	} else {
		// SELECT *: every column is projected; mark all needed. (The
		// empty-columns case must NOT yield an all-false mask, which
		// would decode every row to Null.)
		proj_indices = make([]int, total_cols, allocator)
		for i in 0 ..< total_cols {
			proj_indices[i] = i
		}
	}
	filter_root: ^Resolved_Node
	if f, has_f := plan.filter.?; has_f {
		filter_root = f.root
	}
	needed := collect_needed_cols(filter_root, proj_indices, sort_indices, total_cols, allocator)
	// Extend to MAX_COLS with true: a row whose serial count exceeds the
	// table width (schema drift, impossible mid-statement) then decodes
	// exactly like the scalar path instead of Null-filling the excess.
	ext := make([]bool, types.MAX_COLS, allocator)
	for i in 0 ..< total_cols {
		ext[i] = needed[i]
	}
	for i in total_cols ..< types.MAX_COLS {
		ext[i] = true
	}

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

	row_buf: [types.MAX_COLS]types.Value
	for cursor.is_valid {
		if plan.skip_end > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > plan.skip_end { break }
		}

		// Full-width clear: deserialize_needed writes only 0..<serial_count,
		// so without this a short row could inherit a previous row's tail
		// into a filter position. Ten stores per row; negligible next to
		// varint decode, kills the stale-slot bug class entirely.
		for i in 0 ..< len(row_buf) {
			row_buf[i] = types.value_null()
		}
		rowid, get_err := btree.cursor_get_cell_needed(&cursor, ext, row_buf[:], allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if f, has_f := plan.filter.?; has_f {
			if !evaluate_where_ctx(f, row_buf[:]) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		// Survivor: one owned slice, text/blob cloned before the cursor
		// moves (borrows die on page change). Matches scalar ownership:
		// values live in `allocator`, same as scan_table entries.
		vals := make([]types.Value, total_cols, allocator)
		for i in 0 ..< total_cols {
			#partial switch _ in row_buf[i] {
			case string, []u8:
				cloned, c_err := types.value_clone(row_buf[i], allocator)
				if c_err != nil {
					delete(vals, allocator)
					return nil, true
				}
				vals[i] = cloned
			case:
				vals[i] = row_buf[i]
			}
		}
		append(&r, Row_Entry{rowid, vals})
		if limit, has_limit := plan.max_rows.?; has_limit && u64(len(r)) >= limit { break }
		btree.cursor_advance(&cursor)
	}

	return r[:], false
}

// fetch_single_rows_vec mirrors fetch_single_rows (PK-seek bypass, shared
// limit_pushable rule, same range descriptor) but scans through
// scan_table_vec. Same returns, same error shape.
fetch_single_rows_vec :: proc(
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

	pushable := limit_pushable(stmt, has_order)

	max_rows := stmt.limit if pushable else nil
	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(tbl.columns)},
	}

	if wc, has_wc := stmt.where_clause.?; has_wc {
		if rowid, seek_ok := try_pk_lookup(tbl, wc, tbl_name, stmt.from_alias); seek_ok {
			return seek_single_row(&table_tree, rowid, tbl.columns, single_range, allocator)
		}
	}

	rows, scan_err := scan_table_vec(
		&table_tree,
		&tbl,
		stmt,
		max_rows,
		t,
		allocator,
		cache,
		single_range,
	)
	if scan_err { return nil, nil, nil, false }
	return rows, tbl.columns, single_range, true
}
