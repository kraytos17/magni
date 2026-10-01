package executor

import "core:log"
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

	btree.init_leaf_page(root_page.data, root_page.page_num)
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

Mutation_Mode :: enum {
	Direct,
	COW,
}

// Violation_Policy decides whether a constraint-violating row fails the
// statement (Fail) or is skipped with a warning (Skip, Direct scans where
// siblings still apply).
Violation_Policy :: enum {
	Fail,
	Skip,
}

// Row_Check names which validation stage (if any) rejected a candidate row.
Row_Check :: enum {
	Ok,
	Type_Error,
	Check_Error,
}

// check_row runs type validation then CHECK constraints for one candidate
// row. Logging stays with the caller, which owns the INSERT-vs-UPDATE
// message vocabulary; check_constraints logs its own specifics on failure.
@(private)
check_row :: proc(values: []types.Value, table: types.Table) -> Row_Check {
	if !cell.validate(values, table.columns) { return .Type_Error }
	if !check_constraints(values, table) { return .Check_Error }
	return .Ok
}

// Insert_Row_Info holds the validated, column-ordered values and row ID for a
// single INSERT row. Shared by the direct and COW insert paths.
Insert_Row_Info :: struct {
	values:     []types.Value,
	row_id:     types.Row_ID,
	table_tree: btree.Tree,
}

// prepare_insert_row validates column list, reorders values to match the table
// schema, applies defaults, checks constraints, and assigns a row ID.
// root_page is the current data root to use for rowid lookup and insert.
// Returns (info, true) on success or ({}, false) on error (already logged).
@(private)
prepare_insert_row :: proc(
	table: types.Table,
	columns: []string,
	row_values: []types.Value,
	t: ^btree.Tree,
	root_page: u32,
) -> (Insert_Row_Info, bool) {
	values := row_values
	if len(columns) > 0 {
		if len(columns) != len(row_values) {
			log.error("Error: Column list length does not match value count")
			return {}, false
		}
		if len(columns) > len(table.columns) {
			log.errorf(
				"Error: Too many columns in INSERT. Expected at most %d, got %d",
				len(table.columns),
				len(columns),
			)
			return {}, false
		}

		reordered := make([]types.Value, len(table.columns), context.temp_allocator)
		for i in 0 ..< len(reordered) {
			if def, ok := table.columns[i].default_value.?; ok {
				cloned, _ := types.value_clone(def, context.temp_allocator)
				reordered[i] = cloned
			} else {
				reordered[i] = types.value_null()
			}
		}
		for col_name, i in columns {
			idx, ok := schema.find_column_index(table.columns, col_name)
			if !ok {
				log.errorf("Error: Unknown column: %s", col_name)
				return {}, false
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
		return {}, false
	}
	if check := check_row(values, table); check != .Ok {
		if check == .Type_Error {
			log.error("Error: Data type validation failed")
		}
		return {}, false
	}

	table_tree := btree.init(t.pager, root_page)
	next_rowid: types.Row_ID
	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if val, is_int := values[pk_idx].(i64); is_int {
			next_rowid = types.Row_ID(val)
		} else {
			id, id_err := btree.tree_next_rowid(&table_tree)
			next_rowid = id if id_err == .None else 1
			values[pk_idx] = types.value_int(i64(next_rowid))
		}
	} else {
		id, err := btree.tree_next_rowid(&table_tree)
		if err != .None {
			next_rowid = 1
		} else {
			next_rowid = id
		}
	}
	return Insert_Row_Info{values = values, row_id = next_rowid, table_tree = table_tree}, true
}

@(private)
exec_insert_impl :: proc(
	t: ^btree.Tree,
	table: types.Table,
	stmt: parser.Insert_Stmt,
	mode: Mutation_Mode,
	cache: ^schema.Table_Cache = nil,
) -> (bool, u32, Mutated_Table_Info) {
	is_direct := mode == .Direct
	if is_direct {
		for row_values in stmt.values {
			info, ok := prepare_insert_row(table, stmt.columns, row_values, t, table.root_page)
			if !ok { return false, t.root, {} }

			err := btree.tree_insert(&info.table_tree, info.row_id, info.values)
			if err != .None {
				log.errorf("Error inserting row: %v", err)
				return false, t.root, {}
			}
			log.infof("Inserted row %d", info.row_id)
		}
		return true, t.root, {}
	} else {
		data_root := table.root_page
		for row_values in stmt.values {
			info, ok := prepare_insert_row(table, stmt.columns, row_values, t, data_root)
			if !ok { return false, t.root, {} }

			table_tree := btree.init(t.pager, data_root)
			new_data_root, ins_err := btree.tree_insert_cow(&table_tree, info.row_id, info.values)
			if ins_err != .None {
				log.errorf("Error inserting row: %v", ins_err)
				return false, t.root, {}
			}

			data_root = new_data_root
			log.infof("Inserted row %d", info.row_id)
		}

		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, data_root)
		if !ok {
			log.error("Error: Failed to update schema root page")
			return false, t.root, {}
		}
		return true, new_schema_root, Mutated_Table_Info{name = stmt.table_name, root = data_root}
	}
}

@(private="file")
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
) -> (map[int]types.Value, bool) {
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
@(private)
apply_update :: proc(
	c: ^cell.Cell,
	update_map: map[int]types.Value,
	table: ^types.Table,
	policy: Violation_Policy,
) -> ([]types.Value, bool) {
	new_row := deep_copy_values(c.values)
	for idx, val in update_map {
		new_row[idx] = val
	}
	if check := check_row(new_row, table^); check != .Ok {
		reason := "violates column constraints" if check == .Type_Error else "violates CHECK constraint"
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
) -> (bool, u32, Mutated_Table_Info) {
	tbl := table
	update_map, ok := build_update_map(&tbl, stmt, context.temp_allocator)
	if !ok { return false, t.root, {} }

	plan := Update_Plan {
		tbl        = tbl,
		table_name = stmt.table_name,
		update_map = update_map,
		filt       = {filter = stmt.where_clause},
		direct     = mode == .Direct,
	}

	table_tree := btree.init(t.pager, tbl.root_page)
	if done, ok1, root, info := update_by_pk(t, &plan, &table_tree); done {
		return ok1, root, info
	}
	return update_by_scan(t, &plan, &table_tree)
}

// Update_Plan captures the resolved state for an UPDATE: target table,
// column→value map, optional filter, and write mode. Built once by
// exec_update_impl, consumed by the pk/scan procs below.
Update_Plan :: struct {
	tbl:        types.Table,
	table_name: string,
	update_map: map[int]types.Value,
	using filt: Mutation_Filter,
	direct:     bool,
}

// eval_mutation_filter evaluates a mutation plan's pre-resolved filter
// against one row. No filter → true; unresolvable filter → false (mirrors
// evaluate_where). Shared by UPDATE and DELETE.
@(private="file")
eval_mutation_filter :: proc(f: ^Mutation_Filter, values: []types.Value) -> bool {
	if _, has_wc := f.filter.?; !has_wc { return true }
	if ctx, ok := f.filter_ctx.?; ok {
		return evaluate_where_ctx(ctx, values)
	}
	return false
}

// resolve_mutation_filter resolves the plan filter once per scan (not per
// row). Shared by update_by_scan and collect_delete_targets.
@(private="file")
resolve_mutation_filter :: proc(f: ^Mutation_Filter, cols: []types.Column) {
	if wc, has_wc := f.filter.?; has_wc {
		f.filter_ctx = init_where_ctx(&wc, cols, nil, nil, context.temp_allocator)
	}
}

// pk_target_rowid extracts a PK rowid from an equality filter, if usable.
// Shared prologue for update_by_pk and delete_by_pk.
@(private="file")
pk_target_rowid :: proc(tbl: types.Table, filter: Maybe(parser.Where_Clause)) -> (types.Row_ID, bool) {
	where_clause, has_where := filter.?
	if !has_where { return 0, false }
	return try_pk_lookup(tbl, where_clause)
}

// commit_cow_root publishes a COW-mutated table root to the schema.
// Shared tail for the COW write paths (pk + scan, update + delete);
// callers log their own row counts.
@(private="file")
commit_cow_root :: proc(t: ^btree.Tree, table_name: string, nroot: u32) -> (u32, Mutated_Table_Info, bool) {
	new_schema_root, ok := schema.update_root_page_cow(t, table_name, nroot)
	if !ok { return t.root, {}, false }
	return new_schema_root, Mutated_Table_Info{name = table_name, root = nroot}, true
}

// update_by_pk handles the PK fast path. Returns handled=false to fall
// through to the full scan when no usable PK lookup exists.
@(private="file")
update_by_pk :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
) -> (
	done: bool,
	ok: bool,
	root: u32,
	info: Mutated_Table_Info,
) {
	target_rowid, pk_ok := pk_target_rowid(plan.tbl, plan.filter)
	if !pk_ok { return false, false, 0, {} }

	c, find_err := btree.tree_find(table_tree, target_rowid, context.temp_allocator)
	if find_err != .None {
		log.info("Updated 0 rows.")
		return true, true, t.root, {}
	}
	defer cell.destroy(&c, context.temp_allocator)

	new_row, had_err := apply_update(&c, plan.update_map, &plan.tbl, .Fail)
	if had_err && new_row == nil {
		return true, false, t.root, {}
	}
	if !had_err && new_row == nil {
		log.info("Updated 0 rows.")
		return true, true, t.root, {}
	}
	if plan.direct {
		btree.tree_update(table_tree, target_rowid, new_row)
		log.info("Updated 1 row.")
		return true, true, t.root, {}
	}

	nroot, upd_err := btree.tree_update_cow(table_tree, target_rowid, new_row)
	if upd_err != .None {
		log.error("Error: Failed to update row")
		return true, false, t.root, {}
	}

	new_schema_root, committed, ok1 := commit_cow_root(t, plan.table_name, nroot)
	if !ok1 { return true, false, t.root, {} }

	log.info("Updated 1 row.")
	return true, true, new_schema_root, committed
}

// update_by_scan runs the cursor scan, dispatching on write mode.
@(private="file")
update_by_scan :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	cursor, cursor_err := btree.cursor_start(table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root, {} }
	defer btree.cursor_destroy(&cursor)

	// Resolve the filter once, not per row.
	resolve_mutation_filter(&plan.filt, plan.tbl.columns)
	if plan.direct {
		return update_scan_direct(t, plan, table_tree, &cursor)
	}
	return update_scan_cow(t, plan, table_tree, &cursor)
}

// update_scan_direct collects matching ops, then applies them in place.
@(private="file")
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
		defer cell.destroy(&c, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(cursor)
			continue
		}

		should_update := eval_mutation_filter(&plan.filt, c.values)
		if should_update {
			new_row, had_err := apply_update(&c, plan.update_map, &plan.tbl, .Skip)
			if !had_err && new_row != nil {
				append(&ops, Update_Op{c.rowid, new_row})
			}
		}
		btree.cursor_advance(cursor)
	}

	count := 0
	for op in ops {
		if upd_err := btree.tree_update(table_tree, op.rowid, op.new_values); upd_err == .None {
			count += 1
		}
	}

	log.infof("Updated %d rows.", count)
	return true, t.root, {}
}

// update_scan_cow applies COW updates as the cursor advances.
@(private="file")
update_scan_cow :: proc(
	t: ^btree.Tree,
	plan: ^Update_Plan,
	table_tree: ^btree.Tree,
	cursor: ^btree.Cursor,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	current_root := plan.tbl.root_page
	count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(cursor)
			continue
		}

		defer cell.destroy(&c, context.temp_allocator)
		should_update := eval_mutation_filter(&plan.filt, c.values)
		if should_update {
			new_row, had_err := apply_update(&c, plan.update_map, &plan.tbl, .Fail)
			if !had_err && new_row != nil {
				tree_at := btree.init(t.pager, current_root)
				nroot, upd_err := btree.tree_update_cow(&tree_at, c.rowid, new_row)
				if upd_err == .None {
					current_root = nroot
					count += 1
				}
			}
		}
		btree.cursor_advance(cursor)
	}
	if count > 0 {
		new_schema_root, info, ok1 := commit_cow_root(t, plan.table_name, current_root)
		if !ok1 { return false, t.root, {} }

		log.infof("Updated %d rows.", count)
		return true, new_schema_root, info
	}

	log.info("Updated 0 rows.")
	return true, t.root, {}
}

@(private="file")
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
) -> (bool, u32, Mutated_Table_Info) {
	plan := Delete_Plan {
		tbl        = table,
		table_name = stmt.table_name,
		filt       = {filter = stmt.where_clause},
		direct     = mode == .Direct,
	}

	table_tree := btree.init(t.pager, table.root_page)
	if done, ok, root, info := delete_by_pk(t, &plan, &table_tree); done {
		return ok, root, info
	}

	targets := collect_delete_targets(&plan, &table_tree)
	return apply_deletes(t, &plan, &table_tree, targets[:])
}

// Delete_Plan captures the resolved state for a DELETE: target table,
// optional filter, and write mode. Built once by exec_delete_impl.
Delete_Plan :: struct {
	tbl:        types.Table,
	table_name: string,
	using filt: Mutation_Filter,
	direct:     bool,
}

// delete_by_pk handles the PK fast path. Returns handled=false to fall
// through to the full scan when no usable PK lookup exists.
@(private="file")
delete_by_pk :: proc(
	t: ^btree.Tree,
	plan: ^Delete_Plan,
	table_tree: ^btree.Tree,
) -> (
	done: bool,
	ok: bool,
	root: u32,
	info: Mutated_Table_Info,
) {
	target_rowid, pk_ok := pk_target_rowid(plan.tbl, plan.filter)
	if !pk_ok { return false, false, 0, {} }
	if plan.direct {
		if btree.tree_delete(table_tree, target_rowid) == .None {
			log.info("Deleted 1 row.")
		} else {
			log.info("Deleted 0 rows.")
		}
		return true, true, t.root, {}
	}

	nroot, del_err := btree.tree_delete_cow(table_tree, target_rowid)
	if del_err == .None {
		new_schema_root, info1, ok1 := commit_cow_root(t, plan.table_name, nroot)
		if !ok1 { return true, false, t.root, {} }

		log.info("Deleted 1 row.")
		return true, true, new_schema_root, info1
	}

	log.info("Deleted 0 rows.")
	return true, true, t.root, {}
}

// collect_delete_targets scans for rowids matching the filter.
@(private="file")
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
			btree.cursor_advance(&cursor)
			continue
		}
		defer cell.destroy(&c, context.temp_allocator)

		should_delete := eval_mutation_filter(&plan.filt, c.values)
		if should_delete {
			append(&targets, c.rowid)
		}
		btree.cursor_advance(&cursor)
	}
	return targets
}

// apply_deletes removes the collected targets, direct or COW.
@(private="file")
apply_deletes :: proc(
	t: ^btree.Tree,
	plan: ^Delete_Plan,
	table_tree: ^btree.Tree,
	targets: []types.Row_ID,
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
		}

		log.infof("Deleted %d rows.", count)
		return true, t.root, {}
	}

	current_root := plan.tbl.root_page
	count := 0
	for rowid in targets {
		tree_at := btree.init(t.pager, current_root)
		nroot, del_err := btree.tree_delete_cow(&tree_at, rowid)
		if del_err == .None {
			current_root = nroot
			count += 1
		}
	}
	if count > 0 {
		new_schema_root, info, ok := commit_cow_root(t, plan.table_name, current_root)
		if !ok { return false, t.root, {} }

		log.infof("Deleted %d rows.", count)
		return true, new_schema_root, info
	}

	log.info("Deleted 0 rows.")
	return true, t.root, {}
}

@(private="file")
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
	return exec_insert_impl(t, table^, stmt, .COW, cache)
}

@(private)
exec_update_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Update_Stmt,
	cache: ^schema.Table_Cache = nil,
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
	return exec_update_impl(t, table^, stmt, .COW, cache)
}

@(private)
exec_delete_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Delete_Stmt,
	cache: ^schema.Table_Cache = nil,
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
	return exec_delete_impl(t, table^, stmt, .COW, cache)
}
