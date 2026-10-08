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
// Rollout: gated by vec_scan_enabled (MAGNI_VECTOR=0 forces the scalar
// path). Default on; the scalar scan_table stays as the fallback route.
package executor

import "core:mem"
import "core:os"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// vec_scan_enabled reports whether the vector scan path may be used.
// Default on; set MAGNI_VECTOR=0 to force the scalar path.
vec_scan_enabled :: proc() -> bool {
	return os.get_env("MAGNI_VECTOR", context.temp_allocator) != "0"
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
	if total_cols <= 0 {
		return nil
	}

	needed := make([]bool, total_cols, allocator)
	if root != nil {
		collect_node_cols(root, needed)
	}
	for idx in proj_indices {
		if idx >= 0 && idx < total_cols {
			needed[idx] = true
		}
	}
	for idx in sort_indices {
		if idx >= 0 && idx < total_cols {
			needed[idx] = true
		}
	}
	return needed
}

// collect_node_cols marks every column index referenced anywhere in the
// filter tree, including under OR/NOT (any reachable comparison may need
// its column). IN-list/subquery memberships need no extra columns beyond
// col_idx: their candidate values are constants, not row references.
@(private = "file")
collect_node_cols :: proc(node: ^Resolved_Node, needed: []bool) {
	if node == nil {
		return
	}

	switch node.kind {
	case .COND:
		if node.cond.col_idx >= 0 && node.cond.col_idx < len(needed) {
			needed[node.cond.col_idx] = true
		}
		if node.cond.has_right_col &&
		   node.cond.right_idx >= 0 &&
		   node.cond.right_idx < len(needed) {
			needed[node.cond.right_idx] = true
		}
	case .AND, .OR, .NOT:
		for child in node.children {
			collect_node_cols(child, needed)
		}
	}
}

// resolve_vec_widths resolves ORDER BY sort keys + the display projection for
// the vector scan. Sort keys must decode even when unprojected (ORDER BY
// sorts full rows before projection); the projection itself must decode
// everything finish_select might read.
@(private = "file")
resolve_vec_widths :: proc(
	stmt: parser.Select_Stmt,
	resolver: Column_Resolver,
	total_cols: int,
	allocator: mem.Allocator,
) -> (
	sort_indices, proj_indices: []int,
	has_order: bool,
	ok: bool,
) {
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
		si, si_ok := resolve_sort_indices(order_clause, resolver)
		if !si_ok {
			return nil, nil, false, false
		}
		sort_indices = si
	}
	if len(stmt.columns) > 0 {
		pi, pi_ok := build_display_indices(stmt.columns, resolver, total_cols)
		if !pi_ok {
			return nil, nil, false, false
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
	return sort_indices, proj_indices, has_order, true
}

// vec_seek_cursor opens a scan cursor and applies the plan's skip-start
// bound, restarting cleanly when the seek target is unreachable. The
// returned cursor stays live: the caller must defer cursor_destroy (defer
// cannot cross the helper boundary).
@(private = "file")
vec_seek_cursor :: proc(
	tree: ^btree.Tree,
	plan: Scan_Plan,
	allocator: mem.Allocator,
) -> (
	cursor: btree.Cursor,
	ok: bool,
) {
	start, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None {
		return {}, false
	}

	cursor = start
	if plan.skip_start > 0 {
		if seek_err := btree.cursor_seek_to_page(&cursor, plan.skip_start); seek_err != .None {
			btree.cursor_destroy(&cursor)
			cursor, c_err = btree.cursor_start(tree, allocator)
			if c_err != .None {
				return {}, false
			}
		}
	}
	return cursor, true
}

// clone_fused_row materializes one survivor directly at projected width:
// display position i from table column proj_indices[i]. Text/blob clones
// once per column and shares the header on repeats (SELECT a, a), exactly
// like finish_select's copy. Mutates row_buf (clone-once sharing), same as
// the inline code it replaces.
@(private = "file")
clone_fused_row :: proc(
	row_buf: []types.Value,
	proj_indices: []int,
	out_width: int,
	allocator: mem.Allocator,
) -> (
	vals: []types.Value,
	ok: bool,
) {
	vals = make([]types.Value, out_width, allocator)
	cloned_cols: [types.MAX_COLS]bool
	for idx, i in proj_indices {
		src := row_buf[idx]
		#partial switch _ in src {
		case string, []u8:
			if !cloned_cols[idx] {
				c, c_err := types.value_clone(src, allocator)
				if c_err != nil {
					delete(vals, allocator)
					return nil, false
				}

				row_buf[idx] = c
				cloned_cols[idx] = true
			}
			vals[i] = row_buf[idx]
		case:
			vals[i] = src
		}
	}
	return vals, true
}

// clone_full_row clones one full-width survivor (text/blob owned, scalars
// shared), matching scalar scan_table ownership: values live in allocator.
@(private = "file")
clone_full_row :: proc(
	row_buf: []types.Value,
	total_cols: int,
	allocator: mem.Allocator,
) -> (
	vals: []types.Value,
	ok: bool,
) {
	vals = make([]types.Value, total_cols, allocator)
	for i in 0 ..< total_cols {
		#partial switch _ in row_buf[i] {
		case string, []u8:
			cloned, c_err := types.value_clone(row_buf[i], allocator)
			if c_err != nil {
				delete(vals, allocator)
				return nil, false
			}
			vals[i] = cloned
		case:
			vals[i] = row_buf[i]
		}
	}
	return vals, true
}

// scan_table_vec is the vector counterpart of scan_table: same plan
// construction (shared build_scan_plan, so skip bounds, LIMIT pushdown
// counting, and filter resolution cannot diverge), same row order, same
// error shape — but each row decodes needed columns only into a reused
// stack buffer and only survivors allocate. The filter runs on the
// full-width buffer via the existing scalar evaluators (unneeded slots are
// Null, and every referenced column is decoded by construction of the
// needed mask), so filter semantics are identical by reuse, not by
// reimplementation.
//
// Projection fusion: when the statement has no ORDER BY and projects
// explicit columns, survivors materialize directly at projected width and
// `projected` returns true — the caller must use finish_projected (dedup +
// limit only), never finish_select (which would project twice). With
// ORDER BY, survivors stay full-width (sort runs on full rows before
// projection, so sort keys must survive) and `projected` is false.
// SELECT * is always full-width. Text/blob clone before the cursor moves
// in both modes; a display slot duplicated in the projection (SELECT a, a)
// clones once and shares the header, exactly like finish_select's copy.
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
	out_cols: []types.Column,
	projected: bool,
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
		return nil, nil, false, true
	}

	// Needed mask: filter columns from the resolved plan plus sort keys
	// (ORDER BY sorts full rows before projection, so sort keys must
	// decode even when unprojected) plus the projection itself (survivors
	// stay full-width for the shared finish_select tail, so every column
	// finish_select might read must decode).
	total_cols := len(table.columns)
	resolver := build_column_resolver(table.columns, table_ranges)
	sort_indices, proj_indices, has_order, widths_ok := resolve_vec_widths(
		stmt,
		resolver,
		total_cols,
		allocator,
	)
	if !widths_ok {
		return nil, nil, false, true
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
	cursor, cursor_ok := vec_seek_cursor(tree, plan, allocator)
	if !cursor_ok {
		return nil, nil, false, true
	}

	defer btree.cursor_destroy(&cursor)
	// Fused projection: valid only without ORDER BY. finish_select sorts
	// full rows before projecting (so out-of-projection sort keys
	// resolve); projecting first would silently drop them. With ORDER BY
	// the scan stays full-width and finish_select runs unchanged.
	fused := !has_order && len(stmt.columns) > 0
	out_width := total_cols if !fused else len(proj_indices)
	proj_cols: []types.Column
	if fused {
		proj_cols = make([]types.Column, len(proj_indices), allocator)
		for idx, i in proj_indices {
			proj_cols[i] = table.columns[idx]
			if i < len(stmt.aliases) && stmt.aliases[i] != "" {
				proj_cols[i].name = stmt.aliases[i]
			}
		}
	} else {
		proj_cols = table.columns
	}

	row_buf: [types.MAX_COLS]types.Value
	for cursor.is_valid {
		if plan.skip_end > 0 {
			cp := cursor.path[cursor.depth - 1].page_id
			if cp > plan.skip_end {
				break
			}
		}
		// Full-width clear: deserialize_needed writes only 0..<serial_count,
		// so without this a short row could inherit a previous row's tail
		// into a filter position. Ten stores per row; negligible next to
		// varint decode, kills the stale-slot bug class entirely.
		for i in 0 ..< len(row_buf) {
			row_buf[i] = types.value()
		}

		rowid, get_err := btree.cursor_get_cell_needed(&cursor, ext, row_buf[:])
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
		// values live in `allocator`, same as scan_table entries. Fused
		// mode writes display position i from table column proj_indices[i];
		// a column projected twice (SELECT a, a) clones once and shares
		// the header, exactly like finish_select's copy.
		vals: []types.Value
		vals_ok := true
		if fused {
			vals, vals_ok = clone_fused_row(row_buf[:], proj_indices, out_width, allocator)
		} else {
			vals, vals_ok = clone_full_row(row_buf[:], total_cols, allocator)
		}

		if !vals_ok {
			return nil, nil, false, true
		}

		append(&r, Row_Entry{rowid, vals})
		if limit, has_limit := plan.max_rows.?; has_limit && u64(len(r)) >= limit {
			break
		}
		btree.cursor_advance(&cursor)
	}
	return r[:], proj_cols, fused, false
}

// fetch_single_rows_vec is the vector twin of fetch_single_rows (PK-seek
// bypass, shared limit_pushable rule, same range descriptor) but scans
// through scan_table_vec. Same returns plus the projected flag: true when
// rows are already at projected width (caller must use finish_projected,
// never
// finish_select). The PK-seek bypass returns full-width rows exactly like
// the scalar path, so projected is false there.
fetch_single_rows_vec :: proc(
	t: ^btree.Tree,
	table: types.Table,
	tbl_name: string,
	stmt: parser.Select_Stmt,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	rows: []Row_Entry,
	cols: []types.Column,
	ranges: []Table_Col_Range,
	projected: bool,
	ok: bool,
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
			srows, scols, sranges, sok := seek_single_row(
				&table_tree,
				rowid,
				tbl.columns,
				single_range,
				allocator,
			)

			rows, cols, ranges, projected, ok = srows, scols, sranges, false, sok
			return
		}
		if is_covering_rowid_select(stmt, tbl.columns) {
			if plan, idx_ok := resolve_index_covering(tbl, wc, tbl_name, stmt.from_alias); idx_ok {
				crows, ccols, cranges, cok := fetch_covering_index(
					t,
					&tbl,
					plan,
					from_name,
					allocator,
				)

				rows, cols, ranges, projected, ok = crows, ccols, cranges, false, cok
				return
			}
		}
		if is_covering_col_select(stmt, tbl) {
			if plan, idx_ok := resolve_index_covering(tbl, wc, tbl_name, stmt.from_alias);
			   idx_ok && covering_known_values(plan) {
				crows, ccols, cranges, cok := fetch_covering_col(
					t,
					&tbl,
					plan,
					plan.column,
					from_name,
					allocator,
				)

				rows, cols, ranges, projected, ok = crows, ccols, cranges, false, cok
				return
			}
		}
		if plan, idx_ok := resolve_index_fetch(tbl, wc, tbl_name, stmt.from_alias); idx_ok {
			frows, f_ok := fetch_index_rows(
				t,
				&table_tree,
				&tbl,
				plan,
				&wc,
				single_range,
				allocator,
				cache,
			)

			rows, cols, ranges, projected, ok = frows, tbl.columns, single_range, false, f_ok
			return
		}
	}

	vrows, out_cols, vproj, scan_err := scan_table_vec(
		&table_tree,
		&tbl,
		stmt,
		max_rows,
		t,
		allocator,
		cache,
		single_range,
	)
	if scan_err {
		return nil, nil, nil, false, false
	}
	if vproj {
		// Rows are at projected width: redescribe the range so any
		// downstream resolver sees a consistent (cols, ranges) pair.
		// finish_projected itself resolves nothing (no sort/projection).
		single_range = []Table_Col_Range {
			{table_name = from_name, start_col = 0, col_count = len(out_cols)},
		}
	}

	rows, cols, ranges, projected, ok = vrows, out_cols, single_range, vproj, true
	return
}
