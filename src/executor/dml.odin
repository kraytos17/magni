package executor

import "core:log"
import "core:sync"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

@(private)
exec_create :: proc(
	t: ^btree.Tree,
	stmt: parser.Create_Stmt,
	sql: string,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	if ok, msg := schema.validate_columns(stmt.columns); !ok {
		log.errorf("Schema Error: %s", msg)
		return false, t.root, {}
	}
	if schema.table_exists(t, stmt.table_name) {
		log.errorf("Error: Table already exists: %s", stmt.table_name)
		return false, t.root, {}
	}

	root_page, err := pager.allocate_page(t.pager)
	for err == .None && (root_page.page_num == 1 || root_page.page_num == t.root) {
		pager.mark_dirty(t.pager, root_page.page_num)
		pager.unpin_page(t.pager, root_page.page_num)
		root_page, err = pager.allocate_page(t.pager)
	}
	if err != .None {
		log.error("Error: Failed to allocate table root page")
		return false, t.root, {}
	}

	defer pager.unpin_page(t.pager, root_page.page_num)
	if !btree.init_slot_leaf_page(root_page.data, root_page.page_num) {
		log.error("Error: Failed to initialize table root page")
		return false, t.root, {}
	}

	pager.mark_dirty(t.pager, root_page.page_num)
	for fk in stmt.foreign_keys {
		if !schema.table_exists(t, fk.ref_table) {
			log.errorf(
				"Error: Referenced table '%s' does not exist (FOREIGN KEY on '%s')",
				fk.ref_table,
				fk.col,
			)
			return false, t.root, {}
		}
	}

	new_root, ok := schema.add_table_cow(t, stmt.table_name, stmt.columns, root_page.page_num, sql)
	if !ok {
		log.error("Error: Failed to register table in schema")
		return false, t.root, {}
	}

	log.infof("Created table '%s' at Page %d", stmt.table_name, root_page.page_num)
	return true, new_root, Mutated_Table_Info{name = stmt.table_name, root = root_page.page_num}
}

// exec_create_index implements `CREATE INDEX name ON table (column)` —
// V3.0: single-column TEXT indexes only. Validates, allocates an empty
// text root, backfills existing rows one by one (loop, not bulk: CREATE
// INDEX is rare DDL — measure before optimizing), then publishes root +
// column atomically (readers never see a half index). DDL publishes
// immediately like exec_create (even in-txn, with reoverlay after).
@(private)
exec_create_index :: proc(
	t: ^btree.Tree,
	stmt: parser.Create_Index_Stmt,
	sql: string,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.find_table_cached(t, stmt.table_name, nil)
	if !found {
		log.errorf("Error: Table does not exist: %s", stmt.table_name)
		return false, t.root, {}
	}
	defer schema.table_free(table^, context.temp_allocator)

	col_idx, col_ok := schema.find_column_index(table.columns, stmt.column)
	if !col_ok {
		log.errorf("Error: Column does not exist: %s.%s", stmt.table_name, stmt.column)
		return false, t.root, {}
	}
	if table.columns[col_idx].type != .TEXT {
		log.errorf(
			"Error: Secondary index requires a TEXT column, got %s.%s",
			stmt.table_name,
			stmt.column,
		)
		return false, t.root, {}
	}
	if table.index_root > 0 {
		log.errorf("Error: Table already has an index: %s", stmt.table_name)
		return false, t.root, {}
	}

	root_page, err := pager.allocate_page(t.pager)
	for err == .None && (root_page.page_num == 1 || root_page.page_num == t.root) {
		pager.mark_dirty(t.pager, root_page.page_num)
		pager.unpin_page(t.pager, root_page.page_num)
		root_page, err = pager.allocate_page(t.pager)
	}
	if err != .None {
		log.error("Error: Failed to allocate index root page")
		return false, t.root, {}
	}

	defer pager.unpin_page(t.pager, root_page.page_num)
	if !btree.init_text_leaf_page(root_page.data, root_page.page_num) {
		log.error("Error: Failed to initialize index root page")
		return false, t.root, {}
	}

	pager.mark_dirty(t.pager, root_page.page_num)
	index_root := root_page.page_num
	data_tree := btree.init(t.pager, table.root_page)
	cursor, c_err := btree.cursor_start(&data_tree, context.temp_allocator)
	if c_err != .None {
		log.error("Error: Failed to scan table for index build")
		return false, t.root, {}
	}

	defer btree.cursor_destroy(&cursor)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			cell.destroy(&c, context.temp_allocator)
			btree.cursor_advance(&cursor)
			continue
		}
		if s, is_text := c.values[col_idx].(string); is_text {
			idx_tree := btree.init(t.pager, index_root)
			new_root, ins_err := btree.text_insert_cow(&idx_tree, transmute([]u8)s, c.rowid)
			if ins_err != .None {
				log.errorf("Error: Failed to index row %d", c.rowid)
				cell.destroy(&c, context.temp_allocator)
				return false, t.root, {}
			}
			index_root = new_root
		}

		cell.destroy(&c, context.temp_allocator)
		btree.cursor_advance(&cursor)
	}

	new_schema_root, ok := schema.update_index_def_cow(t, table.name, index_root, stmt.column)
	if !ok {
		log.error("Error: Failed to register index in schema")
		return false, t.root, {}
	}

	// NOTE: index_name is informational in V3.0 (single index per table,
	// no DROP INDEX): it rides the AST for logging/future DDL only, with
	// no catalog field. stmt.sql likewise has no index home (cf. tables).
	_ = sql
	log.infof(
		"Created index '%s' on %s(%s) at Page %d",
		stmt.index_name,
		stmt.table_name,
		stmt.column,
		index_root,
	)

	return true, new_schema_root, Mutated_Table_Info{name = stmt.table_name, root = index_root}
}

Mutation_Mode :: enum u8 {
	Direct,
	COW,
}

// Violation_Policy decides whether a constraint-violating row fails the
// statement (Fail) or is skipped with a warning (Skip, Direct scans where
// siblings still apply).
Violation_Policy :: enum u8 {
	Fail,
	Skip,
}

// Row_Check names which validation stage (if any) rejected a candidate row.
Row_Check :: enum u8 {
	Ok,
	Type_Error,
	Check_Error,
}

// check_row runs type validation then statement-resolved CHECK constraints
// for one candidate row. Logging stays with the caller, which owns the
// INSERT-vs-UPDATE message vocabulary; check_constraints_resolved logs its
// own specifics on failure.
@(private)
check_row :: proc(
	values: []types.Value,
	table: types.Table,
	checks: []Resolved_Check,
) -> Row_Check {
	if !cell.validate(values, table.columns) { return .Type_Error }
	if !check_constraints_resolved(values, checks) { return .Check_Error }
	return .Ok
}

// Insert_Row_Info holds the validated, column-ordered values and row ID for a
// single INSERT row. Shared by the direct and COW insert paths.
Insert_Row_Info :: struct {
	values    : []types.Value,
	row_id    : types.Row_ID,
	table_tree: btree.Tree,
}

// prepare_insert_row validates column list, reorders values to match the table
// schema, applies defaults, checks constraints, and assigns a row ID.
// root_page is the current data root to use for rowid lookup and insert.
// `checks` are the statement-resolved CHECK constraints (built once per
// INSERT, not per row). Returns (info, true) on success or ({}, false) on
// error (already logged).
@(private)
prepare_insert_row :: proc(
	table: types.Table,
	columns: []string,
	row_values: []types.Value,
	t: ^btree.Tree,
	root_page: u32,
	checks: []Resolved_Check,
) -> (
	Insert_Row_Info,
	bool,
) {
	values, v_ok := reorder_insert_values(table, columns, row_values)
	if !v_ok { return {}, false }
	if check := check_row(values, table, checks); check != .Ok {
		if check == .Type_Error {
			log.error("Error: Data type validation failed")
		}
		return {}, false
	}
	return Insert_Row_Info {
			values = values,
			row_id = assign_insert_rowid(table, values, t, root_page),
			table_tree = btree.init(t.pager, root_page),
		},
		true
}

// reorder_insert_values maps the INSERT's column list / values onto the table's
// full column order, filling omitted columns with their DEFAULT (cloned) or
// NULL. With an empty column list, row_values must already be full-width.
@(private = "file")
reorder_insert_values :: proc(
	table: types.Table,
	columns: []string,
	row_values: []types.Value,
) -> (
	values: []types.Value,
	ok: bool,
) {
	values = row_values
	if len(columns) > 0 {
		if len(columns) != len(row_values) {
			log.error("Error: Column list length does not match value count")
			return nil, false
		}
		if len(columns) > len(table.columns) {
			log.errorf(
				"Error: Too many columns in INSERT. Expected at most %d, got %d",
				len(table.columns),
				len(columns),
			)
			return nil, false
		}

		reordered := make([]types.Value, len(table.columns), context.temp_allocator)
		for i in 0 ..< len(reordered) {
			if def, has_def := table.columns[i].default_value.?; has_def {
				cloned, _ := types.value_clone(def, context.temp_allocator)
				reordered[i] = cloned
			} else {
				reordered[i] = types.value_null()
			}
		}
		for col_name, i in columns {
			idx, col_ok := schema.find_column_index(table.columns, col_name)
			if !col_ok {
				log.errorf("Error: Unknown column: %s", col_name)
				return nil, false
			}
			reordered[idx] = row_values[i]
		}
		values = reordered
	}
	if len(values) != len(table.columns) {
		log.errorf(
			"Error: Column count mismatch. Expected %d, got %d",
			len(table.columns),
			len(values),
		)
		return nil, false
	}
	return values, true
}

// assign_insert_rowid picks the row's Row_ID: an explicit integer PK value, else
// the next tree rowid (which fills an implicit/missing PK slot in place).
@(private = "file")
assign_insert_rowid :: proc(
	table: types.Table,
	values: []types.Value,
	t: ^btree.Tree,
	root_page: u32,
) -> types.Row_ID {
	table_tree := btree.init(t.pager, root_page)
	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if val, is_int := values[pk_idx].(i64); is_int {
			return types.Row_ID(val)
		}

		id, id_err := btree.tree_next_rowid(&table_tree)
		next := id if id_err == .None else 1
		values[pk_idx] = types.value_int(i64(next))
		return next
	}

	id, err := btree.tree_next_rowid(&table_tree)
	return id if err == .None else 1
}

@(private)
exec_insert_impl :: proc(
	t: ^btree.Tree,
	table: types.Table,
	stmt: parser.Insert_Stmt,
	mode: Mutation_Mode,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	is_direct := mode == .Direct
	checks: []Resolved_Check
	if len(stmt.values) > 0 {
		rc, rc_ok := resolve_table_checks(table)
		if !rc_ok { return false, t.root, {} }
		checks = rc
	}
	if is_direct {
		for row_values in stmt.values {
			info, ok := prepare_insert_row(
				table,
				stmt.columns,
				row_values,
				t,
				table.root_page,
				checks,
			)
			if !ok { return false, t.root, {} }

			err := btree.tree_insert(&info.table_tree, info.row_id, info.values)
			if err != .None {
				log.errorf("Error inserting row: %v", err)
				return false, t.root, {}
			}
			if _, ok1 := fanout_insert_row(
				t,
				table,
				table.index_root,
				info.row_id,
				info.values,
				false,
			); !ok1 {
				return false, t.root, {}
			}
			log.infof("Inserted row %d", info.row_id)
		}
		return true, t.root, {}
	} else {
		data_root := table.root_page
		index_root := table.index_root
		for row_values in stmt.values {
			info, ok := prepare_insert_row(table, stmt.columns, row_values, t, data_root, checks)
			if !ok { return false, t.root, {} }

			table_tree := btree.init(t.pager, data_root)
			new_data_root, ins_err := btree.tree_insert_cow(&table_tree, info.row_id, info.values)
			if ins_err != .None {
				log.errorf("Error inserting row: %v", ins_err)
				return false, t.root, {}
			}

			data_root = new_data_root
			index_root, ok = fanout_insert_row(
				t,
				table,
				index_root,
				info.row_id,
				info.values,
				true,
			)
			if !ok { return false, t.root, {} }
			log.infof("Inserted row %d", info.row_id)
		}

		new_schema_root, info, ok := commit_cow_root(t, stmt.table_name, data_root, pending, cache)
		if !ok {
			log.error("Error: Failed to update schema root page")
			return false, t.root, {}
		}
		if table.index_root > 0 {
			st := btree.init(t.pager, new_schema_root)
			final_root, _, iok := commit_index_cow_root(
				&st,
				stmt.table_name,
				index_root,
				pending,
				cache,
			)
			if !iok {
				log.error("Error: Failed to update index root page")
				return false, t.root, {}
			}
			new_schema_root = final_root
		}
		return true, new_schema_root, info
	}
}

@(private = "file")
exec_insert :: proc(t: ^btree.Tree, stmt: parser.Insert_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}

	defer schema.table_free(table, context.temp_allocator)
	ok, _, _ := exec_insert_impl(t, table, stmt, .Direct)
	return ok
}

// build_update_map resolves column names to indices and builds the
// index→value mapping for an UPDATE statement.
// Returns the map and true on success, or (nil, false) on error (already logged).
@(private)
build_update_map :: proc(
	table: ^types.Table,
	stmt: parser.Update_Stmt,
	allocator := context.allocator,
) -> (
	map[int]types.Value,
	bool,
) {
	if len(stmt.update_columns) != len(stmt.update_values) {
		log.error("Error: Column/Value count mismatch in UPDATE")
		return nil, false
	}

	update_map := make(map[int]types.Value, len(stmt.update_columns), allocator)
	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, ok := schema.find_column_index(table.columns, col_name)
		if !ok {
			log.errorf("Error: Unknown column: %s", col_name)
			return nil, false
		}
		update_map[idx] = stmt.update_values[i]
	}
	return update_map, true
}

// apply_update validates and applies column updates to a row.
// Returns the new row and true if valid and changed, or (nil, false) if unchanged,
// or (nil, true) if validation failed (error already logged or warned).
// CHECK constraints resolve lazily on the first updated row, so zero-match
// UPDATEs never surface resolution errors for rows they never touch.
@(private)
apply_update :: proc(
	c: ^cell.Cell,
	update_map: map[int]types.Value,
	plan: ^Update_Plan,
	policy: Violation_Policy,
) -> (
	[]types.Value,
	bool,
) {
	if !plan.checks_built {
		plan.checks_built = true
		if rc, rc_ok := resolve_table_checks(plan.tbl); rc_ok {
			plan.checks = rc
		} else {
			return nil, true
		}
	}

	new_row := deep_copy_values(c.values)
	for idx, val in update_map {
		new_row[idx] = val
	}
	if check := check_row(new_row, plan.tbl, plan.checks); check != .Ok {
		reason :=
			"violates column constraints" if check == .Type_Error else "violates CHECK constraint"
		if policy == .Skip {
			log.warn("Skipping UPDATE row", c.rowid, "—", reason)
		} else {
			log.error("Error: UPDATE", reason)
		}
		return nil, true // true = had an error
	}
	if values_equal(c.values, new_row) {
		return nil, false // false = no change, not an error
	}
	return new_row, false
}

@(private)
exec_update_impl :: proc(
	t: ^btree.Tree,
	table: types.Table,
	stmt: parser.Update_Stmt,
	mode: Mutation_Mode,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	tbl := table
	update_map, ok := build_update_map(&tbl, stmt, context.temp_allocator)
	if !ok { return false, t.root, {} }

	plan := Update_Plan {
		tbl = tbl,
		table_name = stmt.table_name,
		update_map = update_map,
		filt = {filter = stmt.where_clause},
		direct = mode == .Direct,
	}

	table_tree := btree.init(t.pager, tbl.root_page)
	if done, ok1, root, info := update_by_pk(t, &plan, &table_tree, pending, cache); done {
		return ok1, root, info
	}
	return update_by_scan(t, &plan, &table_tree, pending, cache)
}

// Update_Plan captures the resolved state for an UPDATE: target table,
// column→value map, optional filter, and write mode. Built once by
// exec_update_impl, consumed by the pk/scan procs below. `checks` holds the
// statement-resolved CHECK constraints, built lazily by apply_update on the
// first updated row (so zero-match UPDATEs never resolve them).
Update_Plan :: struct {
	tbl         : types.Table,
	table_name  : string,
	update_map  : map[int]types.Value,
	using filt  : Mutation_Filter,
	direct      : bool,
	checks      : []Resolved_Check,
	checks_built: bool,
}

// eval_mutation_filter evaluates a mutation plan's pre-resolved filter
// against one row. No filter → true; unresolvable filter → false (mirrors
// evaluate_where). Shared by UPDATE and DELETE.
@(private = "file")
eval_mutation_filter :: proc(f: ^Mutation_Filter, values: []types.Value) -> bool {
	if _, has_wc := f.filter.?; !has_wc { return true }
	if ctx, ok := f.filter_ctx.?; ok {
		return evaluate_where_ctx(ctx, values)
	}
	return false
}

// resolve_mutation_filter resolves the plan filter once per scan (not per
// row). Shared by update_by_scan and collect_delete_targets.
@(private = "file")
resolve_mutation_filter :: proc(f: ^Mutation_Filter, cols: []types.Column) {
	if wc, has_wc := f.filter.?; has_wc {
		f.filter_ctx = init_where_ctx(&wc, cols, nil, nil, context.temp_allocator)
	}
}

// pk_target_rowid extracts a PK rowid from an equality filter, if usable.
// Shared prologue for update_by_pk and delete_by_pk. table_name lets a
// qualified `t.pk` / alias filter resolve to the same seek.
@(private = "file")
pk_target_rowid :: proc(
	tbl: types.Table,
	table_name: string,
	filter: Maybe(parser.Where_Clause),
) -> (
	types.Row_ID,
	bool,
) {
	where_clause, has_where := filter.?
	if !has_where { return 0, false }
	return try_pk_lookup(tbl, where_clause, table_name)
}

// commit_cow_root publishes a COW-mutated table root to the schema.
// Shared tail for the COW write paths (pk + scan, update + delete);
// callers log their own row counts. When pending != nil (explicit txn) the
// root is staged instead of published: the map plus the table-cache overlay
// (so in-txn readers see it) replace the per-statement schema-leaf COW, and
// COMMIT flushes one COW per dirty table. Nil pending ⇒ immediate publish,
// exactly as before.
@(private = "file")
commit_cow_root :: proc(
	t: ^btree.Tree,
	table_name: string,
	nroot: u32,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	u32,
	Mutated_Table_Info,
	bool,
) {
	if pending != nil {
		pending_stage(pending, table_name, nroot)
		if cache != nil {
			if tbl, ok := pending_cache_entry(cache, table_name); ok {
				tbl.root_page = nroot
			}
		}
		return t.root, Mutated_Table_Info{name = table_name, root = nroot}, true
	}

	new_schema_root, ok := schema.update_root_page_cow(t, table_name, nroot)
	if !ok { return t.root, {}, false }
	return new_schema_root, Mutated_Table_Info{name = table_name, root = nroot}, true
}

// commit_index_cow_root publishes a new secondary-index root: staged into
// pending under txn (plus the cache overlay), immediate schema COW
// otherwise. Mirrors commit_cow_root (same two paths, index key space) —
// D4 fan-out calls both helpers per mutation.
@(private = "file")
commit_index_cow_root :: proc(
	t: ^btree.Tree,
	table_name: string,
	nroot: u32,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	u32,
	Mutated_Table_Info,
	bool,
) {
	if pending != nil {
		pending_stage_index(pending, table_name, nroot)
		if cache != nil {
			if tbl, ok := pending_cache_entry(cache, table_name); ok {
				tbl.index_root = nroot
			}
		}
		return t.root, Mutated_Table_Info{name = table_name, root = nroot}, true
	}

	new_schema_root, ok := schema.update_index_root_cow(t, table_name, nroot)
	if !ok { return t.root, {}, false }
	return new_schema_root, Mutated_Table_Info{name = table_name, root = nroot}, true
}

// pending_cache_entry finds the cached catalog entry for an overlay update.
// The entry must exist: the table was just resolved through the cache to
// compute the staged root. A miss means someone bypassed the cache — skip
// the overlay (the next root-bump clears it) rather than fabricate state.
@(private = "file")
pending_cache_entry :: proc(
	cache: ^schema.Table_Cache,
	table_name: string,
) -> (
	^types.Table,
	bool,
) {
	sync.rw_mutex_lock(&cache.mu)
	defer sync.rw_mutex_unlock(&cache.mu)
	if cache.tables == nil { return nil, false }

	tbl, ok := cache.tables[table_name]
	if !ok { return nil, false }
	return tbl, true
}

// pending_reoverlay re-applies staged roots onto a fresh cache generation.
// Called after immediate publishers (DDL) bump the schema root and wipe the
// overlays: each staged table re-resolves from the current tree, then takes
// its pending root back. Dropped tables no longer resolve — skipped.
@(private)
pending_reoverlay :: proc(t: ^btree.Tree, pending: ^Pending_Roots, cache: ^schema.Table_Cache) {
	if pending == nil || cache == nil { return }
	for name, root in pending.roots {
		if tbl, ok := schema.find_table_cached(t, name, cache); ok {
			tbl.root_page = root
		}
	}
	for name, root in pending.index_roots {
		if tbl, ok := schema.find_table_cached(t, name, cache); ok {
			tbl.index_root = root
		}
	}
}

// update_by_pk handles the PK fast path. Returns handled=false to fall
// through to the full scan when no usable PK lookup exists.
@(private = "file")
update_by_pk :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	done: bool,
	ok: bool,
	root: u32,
	info: Mutated_Table_Info,
) {
	target_rowid, pk_ok := pk_target_rowid(plan.tbl, plan.table_name, plan.filter)
	if !pk_ok { return false, false, 0, {} }

	c, find_err := btree.tree_find(table_tree, target_rowid, context.temp_allocator)
	if find_err != .None {
		log.info("Updated 0 rows.")
		return true, true, t.root, {}
	}
	defer cell.destroy(&c, context.temp_allocator)

	new_row, had_err := apply_update(&c, plan.update_map, plan, .Fail)
	if had_err && new_row == nil {
		return true, false, t.root, {}
	}
	if !had_err && new_row == nil {
		log.info("Updated 0 rows.")
		return true, true, t.root, {}
	}
	if plan.direct {
		if u_err := btree.tree_update(table_tree, target_rowid, new_row); u_err != .None {
			log.error("Error: Failed to update row")
			return true, false, t.root, {}
		}
		if _, ok1 := fanout_update_row(
			t,
			plan.tbl,
			plan.tbl.index_root,
			target_rowid,
			c.values,
			new_row,
			false,
		); !ok1 {
			return true, false, t.root, {}
		}

		log.info("Updated 1 row.")
		return true, true, t.root, {}
	}

	nroot, upd_err := btree.tree_update_cow(table_tree, target_rowid, new_row)
	if upd_err != .None {
		log.error("Error: Failed to update row")
		return true, false, t.root, {}
	}

	index_root, iok := fanout_update_row(
		t,
		plan.tbl,
		plan.tbl.index_root,
		target_rowid,
		c.values,
		new_row,
		true,
	)
	if !iok { return true, false, t.root, {} }

	new_schema_root, committed, ok1 := commit_cow_root(t, plan.table_name, nroot, pending, cache)
	if !ok1 { return true, false, t.root, {} }
	if plan.tbl.index_root > 0 {
		st := btree.init(t.pager, new_schema_root)
		final_root, _, iok2 := commit_index_cow_root(
			&st,
			plan.table_name,
			index_root,
			pending,
			cache,
		)
		if !iok2 { return true, false, t.root, {} }
		new_schema_root = final_root
	}

	log.info("Updated 1 row.")
	return true, true, new_schema_root, committed
}

// update_by_scan runs the cursor scan, dispatching on write mode.
@(private = "file")
update_by_scan :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	cursor, cursor_err := btree.cursor_start(table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root, {} }
	defer btree.cursor_destroy(&cursor)

	resolve_mutation_filter(&plan.filt, plan.tbl.columns)
	if plan.direct {
		return update_scan_direct(t, plan, table_tree, &cursor)
	}
	return update_scan_cow(t, plan, table_tree, &cursor, pending, cache)
}

// update_scan_direct collects matching ops, then applies them in place.
@(private = "file")
update_scan_direct :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
	cursor: ^btree.Cursor,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	ops := make([dynamic]Update_Op, context.temp_allocator)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(cursor, context.temp_allocator)
		if get_err != .None {
			cell.destroy(&c, context.temp_allocator)
			btree.cursor_advance(cursor)
			continue
		}

		should_update := eval_mutation_filter(&plan.filt, c.values)
		if should_update {
			new_row, had_err := apply_update(&c, plan.update_map, plan, .Skip)
			if !had_err && new_row != nil {
				append(&ops, Update_Op{c.rowid, new_row})
			}
		}

		cell.destroy(&c, context.temp_allocator)
		btree.cursor_advance(cursor)
	}

	count := 0
	for op in ops {
		if upd_err := btree.tree_update(table_tree, op.rowid, op.new_values); upd_err == .None {
			count += 1
			if _, iok := fanout_update_rowid(
				t,
				table_tree,
				plan.tbl,
				plan.tbl.index_root,
				op.rowid,
				op.new_values,
				false,
			); !iok {
				return false, t.root, {}
			}
		}
	}

	log.infof("Updated %d rows.", count)
	return true, t.root, {}
}

// update_scan_cow applies COW updates as the cursor advances.
@(private = "file")
update_scan_cow :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
	cursor: ^btree.Cursor,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	current_root := plan.tbl.root_page
	index_root := plan.tbl.index_root
	count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(cursor, context.temp_allocator)
		if get_err != .None {
			cell.destroy(&c, context.temp_allocator)
			btree.cursor_advance(cursor)
			continue
		}

		should_update := eval_mutation_filter(&plan.filt, c.values)
		if should_update {
			new_row, had_err := apply_update(&c, plan.update_map, plan, .Fail)
			if !had_err && new_row != nil {
				tree_at := btree.init(t.pager, current_root)
				nroot, upd_err := btree.tree_update_cow(&tree_at, c.rowid, new_row)
				if upd_err == .None {
					new_index_root, iok := fanout_update_row(
						t,
						plan.tbl,
						index_root,
						c.rowid,
						c.values,
						new_row,
						true,
					)
					if !iok {
						cell.destroy(&c, context.temp_allocator)
						return false, t.root, {}
					}

					index_root = new_index_root
					current_root = nroot
					count += 1
				}
			}
		}

		cell.destroy(&c, context.temp_allocator)
		btree.cursor_advance(cursor)
	}
	if count > 0 {
		new_schema_root, info, ok1 := commit_cow_root(
			t,
			plan.table_name,
			current_root,
			pending,
			cache,
		)
		if !ok1 { return false, t.root, {} }
		if plan.tbl.index_root > 0 {
			st := btree.init(t.pager, new_schema_root)
			final_root, _, iok := commit_index_cow_root(
				&st,
				plan.table_name,
				index_root,
				pending,
				cache,
			)
			if !iok { return false, t.root, {} }
			new_schema_root = final_root
		}

		log.infof("Updated %d rows.", count)
		return true, new_schema_root, info
	}

	log.info("Updated 0 rows.")
	return true, t.root, {}
}

@(private = "file")
exec_update :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}

	defer schema.table_free(table, context.temp_allocator)
	ok, _, _ := exec_update_impl(t, table, stmt, .Direct)
	return ok
}

@(private)
exec_delete_impl :: proc(
	t: ^btree.Tree,
	table: types.Table,
	stmt: parser.Delete_Stmt,
	mode: Mutation_Mode,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	plan := Delete_Plan {
		tbl = table,
		table_name = stmt.table_name,
		filt = {filter = stmt.where_clause},
		direct = mode == .Direct,
	}

	table_tree := btree.init(t.pager, table.root_page)
	if done, ok, root, info := delete_by_pk(t, &plan, &table_tree, pending, cache); done {
		return ok, root, info
	}

	targets := collect_delete_targets(&plan, &table_tree)
	return apply_deletes(t, &plan, &table_tree, targets[:], pending, cache)
}

// Delete_Plan captures the resolved state for a DELETE: target table,
// optional filter, and write mode. Built once by exec_delete_impl.
Delete_Plan :: struct {
	tbl       : types.Table,
	table_name: string,
	using filt: Mutation_Filter,
	direct    : bool,
}

// delete_by_pk handles the PK fast path. Returns handled=false to fall
// through to the full scan when no usable PK lookup exists.
@(private = "file")
delete_by_pk :: proc(
	t: ^btree.Tree,
	plan: ^Delete_Plan,
	table_tree: ^btree.Tree,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	done: bool,
	ok: bool,
	root: u32,
	info: Mutated_Table_Info,
) {
	target_rowid, pk_ok := pk_target_rowid(plan.tbl, plan.table_name, plan.filter)
	if !pk_ok { return false, false, 0, {} }
	if plan.direct {
		if btree.tree_delete(table_tree, target_rowid) == .None {
			log.info("Deleted 1 row.")
		} else {
			log.info("Deleted 0 rows.")
		}
		if _, ok1 := fanout_delete_rowid(
			t,
			table_tree,
			plan.tbl,
			plan.tbl.index_root,
			target_rowid,
			false,
		); !ok1 {
			return true, false, t.root, {}
		}
		return true, true, t.root, {}
	}

	nroot, del_err := btree.tree_delete_cow(table_tree, target_rowid)
	if del_err == .None {
		index_root, iok := fanout_delete_rowid(
			t,
			table_tree,
			plan.tbl,
			plan.tbl.index_root,
			target_rowid,
			true,
		)
		if !iok { return true, false, t.root, {} }

		new_schema_root, info1, ok1 := commit_cow_root(t, plan.table_name, nroot, pending, cache)
		if !ok1 { return true, false, t.root, {} }
		if plan.tbl.index_root > 0 {
			st := btree.init(t.pager, new_schema_root)
			final_root, _, iok2 := commit_index_cow_root(
				&st,
				plan.table_name,
				index_root,
				pending,
				cache,
			)
			if !iok2 { return true, false, t.root, {} }
			new_schema_root = final_root
		}

		log.info("Deleted 1 row.")
		return true, true, new_schema_root, info1
	}

	log.info("Deleted 0 rows.")
	return true, true, t.root, {}
}

// collect_delete_targets scans for rowids matching the filter.
@(private = "file")
collect_delete_targets :: proc(
	plan: ^Delete_Plan,
	table_tree: ^btree.Tree,
) -> [dynamic]types.Row_ID {
	targets := make([dynamic]types.Row_ID, context.temp_allocator)
	cursor, err := btree.cursor_start(table_tree, context.temp_allocator)
	if err != .None { return targets }
	defer btree.cursor_destroy(&cursor)

	// Resolve the filter once, not per row.
	resolve_mutation_filter(&plan.filt, plan.tbl.columns)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			cell.destroy(&c, context.temp_allocator)
			btree.cursor_advance(&cursor)
			continue
		}

		should_delete := eval_mutation_filter(&plan.filt, c.values)
		if should_delete {
			append(&targets, c.rowid)
		}

		cell.destroy(&c, context.temp_allocator)
		btree.cursor_advance(&cursor)
	}
	return targets
}

// apply_deletes removes the collected targets, direct or COW.
@(private = "file")
apply_deletes :: proc(
	t: ^btree.Tree,
	plan: ^Delete_Plan,
	table_tree: ^btree.Tree,
	targets: []types.Row_ID,
	pending: ^Pending_Roots = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	if plan.direct {
		count := 0
		for rowid in targets {
			if btree.tree_delete(table_tree, rowid) == .None {
				count += 1
			}
			if _, iok := fanout_delete_rowid(
				t,
				table_tree,
				plan.tbl,
				plan.tbl.index_root,
				rowid,
				false,
			); !iok {
				return false, t.root, {}
			}
		}

		log.infof("Deleted %d rows.", count)
		return true, t.root, {}
	}

	current_root := plan.tbl.root_page
	index_root := plan.tbl.index_root
	count := 0
	for rowid in targets {
		tree_at := btree.init(t.pager, current_root)
		nroot, del_err := btree.tree_delete_cow(&tree_at, rowid)
		if del_err == .None {
			current_root = nroot
			count += 1
		}

		new_index_root, iok := fanout_delete_rowid(
			t,
			table_tree,
			plan.tbl,
			index_root,
			rowid,
			true,
		)
		if !iok { return false, t.root, {} }
		index_root = new_index_root
	}
	if count > 0 {
		new_schema_root, info, ok := commit_cow_root(
			t,
			plan.table_name,
			current_root,
			pending,
			cache,
		)
		if !ok { return false, t.root, {} }
		if plan.tbl.index_root > 0 {
			st := btree.init(t.pager, new_schema_root)
			final_root, _, iok := commit_index_cow_root(
				&st,
				plan.table_name,
				index_root,
				pending,
				cache,
			)
			if !iok { return false, t.root, {} }
			new_schema_root = final_root
		}

		log.infof("Deleted %d rows.", count)
		return true, new_schema_root, info
	}

	log.info("Deleted 0 rows.")
	return true, t.root, {}
}

@(private = "file")
exec_delete :: proc(t: ^btree.Tree, stmt: parser.Delete_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}

	defer schema.table_free(table, context.temp_allocator)
	ok, _, _ := exec_delete_impl(t, table, stmt, .Direct)
	return ok
}

@(private)
exec_drop :: proc(t: ^btree.Tree, stmt: parser.Drop_Stmt) -> (bool, u32, Mutated_Table_Info) {
	if !schema.table_exists(t, stmt.table_name) {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}

	new_root, ok := schema.drop_table_cow(t, stmt.table_name)
	if ok {
		log.infof("Dropped table: %s", stmt.table_name)
		return true, new_root, Mutated_Table_Info{name = stmt.table_name, root = 0}
	}
	return false, t.root, {}
}

@(private)
exec_insert_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Insert_Stmt,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.find_table_cached(t, stmt.table_name, cache)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	return exec_insert_impl(t, table^, stmt, .COW, cache, pending)
}

@(private)
exec_update_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Update_Stmt,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.find_table_cached(t, stmt.table_name, cache)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	return exec_update_impl(t, table^, stmt, .COW, cache, pending)
}

@(private)
exec_delete_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Delete_Stmt,
	cache: ^schema.Table_Cache = nil,
	pending: ^Pending_Roots = nil,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.find_table_cached(t, stmt.table_name, cache)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	return exec_delete_impl(t, table^, stmt, .COW, cache, pending)
}

// One entry point per mutation kind, each threading the index root the
// same way the data root threads beside it (COW) or touching nothing
// (Direct — in-place writes never change page ids). Tables without an
// index return early on `index_root == 0`: one branch, negligible cost
// for the unindexed hot path. Non-TEXT values (incl. NULL) skip — the
// type assertion is the whole gate, same as backfill.
//
// Old values ride in explicitly (pk sites hold them; scan sites refetch
// before mutating — never after, since Direct mutates in place). All
// borrows (old row texts, new values) are consumed synchronously: COW
// never mutates the pages they borrow, and index writes never touch
// data pages. Missing index entries on delete tolerate-and-log (mirrors
// delete_by_pk's "Deleted 0 rows" stance); anything else fails loudly.

// index_col_text extracts the indexed TEXT value of a row: ("", false)
// when unindexed, column-missing, non-TEXT, or NULL. Callers treat false
// as skip (insert) — never an error.
@(private = "file")
index_col_text :: proc(table: types.Table, values: []types.Value) -> (string, bool) {
	if table.index_root == 0 || len(table.index_column) == 0 { return "", false }

	col_idx, col_ok := schema.find_column_index(table.columns, table.index_column)
	if !col_ok || col_idx < 0 || col_idx >= len(values) { return "", false }

	s, is_text := values[col_idx].(string)
	if !is_text { return "", false }
	return s, true
}

// fanout_insert_row indexes one inserted row. Returns the (possibly new)
// index root — same threading contract as the data root beside it.
@(private = "file")
fanout_insert_row :: proc(
	t: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	cow: bool,
) -> (
	u32,
	bool,
) {
	text, ok := index_col_text(table, values)
	if !ok { return index_root, true }
	return fanout_insert_text(t, table, index_root, text, rowid, cow)
}

// fanout_delete_text removes one entry, tolerating absence (mirrors
// delete_by_pk's "Deleted 0 rows" tolerance — a missing entry is stale
// state, not a statement failure).
@(private = "file")
fanout_delete_text :: proc(
	t: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	text: string,
	rowid: types.Row_ID,
	cow: bool,
) -> (
	u32,
	bool,
) {
	idx_tree := btree.init(t.pager, index_root)
	if cow {
		new_root, del_err := btree.text_delete_cow(&idx_tree, transmute([]u8)text, rowid)
		if del_err != .None {
			if del_err == .Cell_Not_Found {
				log.infof("Index entry already absent for row %d.", rowid)
				return index_root, true
			}

			log.errorf("Error: Failed to de-index row %d for '%s'", rowid, table.name)
			return index_root, false
		}
		return new_root, true
	}
	if del_err := btree.text_delete(&idx_tree, transmute([]u8)text, rowid); del_err != .None {
		if del_err == .Cell_Not_Found {
			log.infof("Index entry already absent for row %d.", rowid)
			return index_root, true
		}

		log.errorf("Error: Failed to de-index row %d for '%s'", rowid, table.name)
		return index_root, false
	}
	return index_root, true
}

// fanout_delete_rowid removes one row's index entry, looked up by rowid.
// A missing data row tolerates (nothing to de-index — same stance as the
// data delete it accompanies, which reports "0 rows" and succeeds).
@(private = "file")
fanout_delete_rowid :: proc(
	t: ^btree.Tree,
	data_tree: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	rowid: types.Row_ID,
	cow: bool,
) -> (
	u32,
	bool,
) {
	if table.index_root == 0 || len(table.index_column) == 0 { return index_root, true }
	old, find_err := btree.tree_find(data_tree, rowid, context.temp_allocator)
	if find_err != .None {
		log.infof("Row %d already absent; nothing to de-index.", rowid)
		return index_root, true
	}

	defer cell.destroy(&old, context.temp_allocator)
	text, ok := index_col_text(table, old.values)
	if !ok { return index_root, true }
	return fanout_delete_text(t, table, index_root, text, rowid, cow)
}

// fanout_update_row maintains the index across one UPDATE from explicit
// old/new values: unchanged text (or both sides unindexed) skips;
// NULL<->text transitions insert/delete alone; otherwise delete-old +
// insert-new.
@(private = "file")
fanout_update_row :: proc(
	t: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	rowid: types.Row_ID,
	old_values: []types.Value,
	new_values: []types.Value,
	cow: bool,
) -> (
	u32,
	bool,
) {
	if table.index_root == 0 || len(table.index_column) == 0 { return index_root, true }

	old_text, old_ok := index_col_text(table, old_values)
	new_text, new_ok := index_col_text(table, new_values)
	if old_ok == new_ok && (!old_ok || old_text == new_text) { return index_root, true }

	cur := index_root
	if old_ok {
		nr, ok := fanout_delete_text(t, table, cur, old_text, rowid, cow)
		if !ok { return index_root, false }
		cur = nr
	}
	if new_ok {
		nr, ok := fanout_insert_text(t, table, cur, new_text, rowid, cow)
		if !ok { return index_root, false }
		cur = nr
	}
	return cur, true
}

// fanout_update_rowid is the scan-path entry: re-fetches the old row
// (BEFORE the caller mutates — Direct writes in place), then delegates.
// A vanished old row fails loudly (the update just proved it present).
@(private = "file")
fanout_update_rowid :: proc(
	t: ^btree.Tree,
	data_tree: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	rowid: types.Row_ID,
	new_values: []types.Value,
	cow: bool,
) -> (
	u32,
	bool,
) {
	if table.index_root == 0 || len(table.index_column) == 0 { return index_root, true }
	old, find_err := btree.tree_find(data_tree, rowid, context.temp_allocator)
	if find_err != .None {
		log.errorf("Error: Failed to find row %d for re-indexing '%s'", rowid, table.name)
		return index_root, false
	}

	defer cell.destroy(&old, context.temp_allocator)
	return fanout_update_row(t, table, index_root, rowid, old.values, new_values, cow)
}

// fanout_insert_text inserts one index entry by explicit text (the update
// path's new side). Split from fanout_insert_row so both read paths share
// one insert implementation.
@(private = "file")
fanout_insert_text :: proc(
	t: ^btree.Tree,
	table: types.Table,
	index_root: u32,
	text: string,
	rowid: types.Row_ID,
	cow: bool,
) -> (
	u32,
	bool,
) {
	idx_tree := btree.init(t.pager, index_root)
	if cow {
		new_root, ins_err := btree.text_insert_cow(&idx_tree, transmute([]u8)text, rowid)
		if ins_err != .None {
			log.errorf("Error: Failed to index row %d for '%s'", rowid, table.name)
			return index_root, false
		}
		return new_root, true
	}
	if ins_err := btree.text_insert(&idx_tree, transmute([]u8)text, rowid); ins_err != .None {
		log.errorf("Error: Failed to index row %d for '%s'", rowid, table.name)
		return index_root, false
	}
	return index_root, true
}
