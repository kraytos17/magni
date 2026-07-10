package executor

import "core:log"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

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

exec_insert :: proc(t: ^btree.Tree, stmt: parser.Insert_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	values := stmt.values
	if len(stmt.columns) > 0 {
		if len(stmt.columns) != len(stmt.values) {
			log.error("Error: Column list length does not match value count")
			return false
		}
		if len(stmt.columns) > len(table.columns) {
			log.errorf(
				"Error: Too many columns in INSERT. Expected at most %d, got %d",
				len(table.columns),
				len(stmt.columns),
			)
			return false
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
		for col_name, i in stmt.columns {
			idx, ok := schema.find_column_index(table.columns, col_name)
			if !ok {
				log.errorf("Error: Unknown column: %s", col_name)
				return false
			}
			reordered[idx] = stmt.values[i]
		}
		values = reordered
	}
	if len(values) != len(table.columns) {
		log.errorf(
			"Error: Column count mismatch. Expected %d, got %d",
			len(table.columns),
			len(values),
		)
		return false
	}
	if !cell.validate(values, table.columns) {
		log.error("Error: Data type validation failed")
		return false
	}
	if !check_constraints(values, table) { return false }

	table_tree := btree.init(t.pager, table.root_page)
	// RowID: use PK value if provided, otherwise auto-increment from max rowid.
	next_rowid: types.Row_ID
	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if val, is_int := values[pk_idx].(i64); is_int {
			next_rowid = types.Row_ID(val)
		} else {
			next_rowid, _ = btree.tree_next_rowid(&table_tree)
		}
	} else {
		id, err := btree.tree_next_rowid(&table_tree)
		if err != .None {
			next_rowid = 1
		} else {
			next_rowid = id
		}
	}

	err := btree.tree_insert(&table_tree, next_rowid, values)
	if err != .None {
		log.errorf("Error inserting row: %v", err)
		return false
	}
	log.infof("Inserted row %d", next_rowid)
	return true
}

exec_update :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	update_map := make(map[int]types.Value, context.temp_allocator)
	if len(stmt.update_columns) != len(stmt.update_values) {
		log.error("Error: Column/Value count mismatch in UPDATE")
		return false
	}
	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, ok := schema.find_column_index(table.columns, col_name)
		if !ok {
			log.errorf("Error: Unknown column: %s", col_name)
			return false
		}
		update_map[idx] = stmt.update_values[i]
	}

	ops := make([dynamic]Update_Op, context.temp_allocator)
	table_tree := btree.init(t.pager, table.root_page)
	if where_clause, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_clause); pk_ok {
			c, find_err := btree.tree_find(&table_tree, target_rowid, context.temp_allocator)
			if find_err == .None {
				defer cell.destroy(&c, context.temp_allocator)
				new_row := deep_copy_values(c.values)
				for idx, val in update_map {
					new_row[idx] = val
				}
				if !cell.validate(new_row, table.columns) {
					log.error("Error: UPDATE violates column constraints")
					return false
				} else if values_equal(c.values, new_row) {
					log.info("Updated 0 rows.")
				} else {
					btree.tree_update(&table_tree, target_rowid, new_row)
					log.info("Updated 1 row.")
				}
			} else {
				log.info("Updated 0 rows.")
			}
			return true
		}
	}

	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None { return false }
	defer btree.cursor_destroy(&cursor)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		defer cell.destroy(&c, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}

		should_update := true
		if where_clause, has_where := stmt.where_clause.?; has_where {
			should_update = evaluate_where(&where_clause, c.values, table.columns, nil)
		}
		if should_update {
			new_row := deep_copy_values(c.values)
			for idx, val in update_map {
				new_row[idx] = val
			}
			if !cell.validate(new_row, table.columns) {
				log.warn(
					"Skipping UPDATE row",
					c.rowid,
					"— violates column constraints",
				)
			} else if !values_equal(c.values, new_row) {
				append(&ops, Update_Op{c.rowid, new_row})
			}
		}
		btree.cursor_advance(&cursor)
	}

	count := 0
	for op in ops {
		if upd_err := btree.tree_update(&table_tree, op.rowid, op.new_values); upd_err == .None {
			count += 1
		}
	}
	log.infof("Updated %d rows.", count)
	return true
}

exec_delete :: proc(t: ^btree.Tree, stmt: parser.Delete_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	targets := make([dynamic]types.Row_ID, context.temp_allocator)
	table_tree := btree.init(t.pager, table.root_page)
	if where_cl, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_cl); pk_ok {
			if btree.tree_delete(&table_tree, target_rowid) == .None {
				log.info("Deleted 1 row.")
			} else {
				log.info("Deleted 0 rows.")
			}
			return true
		}
	}

	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None { return false }
	defer btree.cursor_destroy(&cursor)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		defer cell.destroy(&c, context.temp_allocator)

		should_delete := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_delete = evaluate_where(&where_cl, c.values, table.columns, nil)
		}
		if should_delete {
			append(&targets, c.rowid)
		}
		btree.cursor_advance(&cursor)
	}

	count := 0
	for rowid in targets {
		if btree.tree_delete(&table_tree, rowid) == .None {
			count += 1
		}
	}
	log.infof("Deleted %d rows.", count)
	return true
}

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

exec_insert_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Insert_Stmt,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	defer schema.table_free(table, context.temp_allocator)

	values := stmt.values
	if len(stmt.columns) > 0 {
		if len(stmt.columns) != len(stmt.values) {
			log.error("Error: Column list length does not match value count")
			return false, t.root, {}
		}
		if len(stmt.columns) > len(table.columns) {
			log.errorf(
				"Error: Too many columns in INSERT. Expected at most %d, got %d",
				len(table.columns),
				len(stmt.columns),
			)
			return false, t.root, {}
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
		for col_name, i in stmt.columns {
			idx, col_ok := schema.find_column_index(table.columns, col_name)
			if !col_ok {
				log.errorf("Error: Unknown column: %s", col_name)
				return false, t.root, {}
			}
			reordered[idx] = stmt.values[i]
		}
		values = reordered
	}
	if len(values) != len(table.columns) {
		log.errorf(
			"Error: Column count mismatch. Expected %d, got %d",
			len(table.columns),
			len(values),
		)
		return false, t.root, {}
	}
	if !cell.validate(values, table.columns) {
		log.error("Error: Data type validation failed")
		return false, t.root, {}
	}

	table_tree := btree.init(t.pager, table.root_page)
	next_rowid: types.Row_ID
	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if has_pk {
		if val, is_int := values[pk_idx].(i64); is_int {
			next_rowid = types.Row_ID(val)
		} else {
			id, _ := btree.tree_next_rowid(&table_tree)
			next_rowid = id
		}
	} else {
		id, id_err := btree.tree_next_rowid(&table_tree)
		next_rowid = id if id_err == .None else 1
	}
	if !check_constraints(values, table) { return false, t.root, {} }

	new_data_root, ins_err := btree.tree_insert_cow(&table_tree, next_rowid, values)
	if ins_err != .None {
		log.errorf("Error inserting row: %v", ins_err)
		return false, t.root, {}
	}

	new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, new_data_root)
	if !ok {
		log.error("Error: Failed to update schema root page")
		return false, t.root, {}
	}

	log.infof("Inserted row %d", next_rowid)
	return true, new_schema_root, Mutated_Table_Info{name = stmt.table_name, root = new_data_root}
}

exec_update_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Update_Stmt,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	defer schema.table_free(table, context.temp_allocator)

	update_map := make(map[int]types.Value, context.temp_allocator)
	if len(stmt.update_columns) != len(stmt.update_values) {
		log.error("Error: Column/Value count mismatch in UPDATE")
		return false, t.root, {}
	}
	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, col_ok := schema.find_column_index(table.columns, col_name)
		if !col_ok {
			log.errorf("Error: Unknown column: %s", col_name)
			return false, t.root, {}
		}
		update_map[idx] = stmt.update_values[i]
	}

	table_tree := btree.init(t.pager, table.root_page)
	if where_clause, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_clause); pk_ok {
			c, find_err := btree.tree_find(&table_tree, target_rowid, context.temp_allocator)
			if find_err == .None {
				defer cell.destroy(&c, context.temp_allocator)
				new_row := deep_copy_values(c.values)
				for idx, val in update_map {
					new_row[idx] = val
				}
				if !cell.validate(new_row, table.columns) {
					log.error("Error: UPDATE violates column constraints")
					return false, t.root, {}
				}
				if values_equal(c.values, new_row) {
					log.info("Updated 0 rows.")
					return true, t.root, {}
				}

				nroot, upd_err := btree.tree_update_cow(&table_tree, target_rowid, new_row)
				if upd_err != .None {
					log.error("Error: Failed to update row")
					return false, t.root, {}
				}

				new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, nroot)
				if !ok { return false, t.root, {} }

				log.info("Updated 1 row.")
				return true, new_schema_root, Mutated_Table_Info {
					name = stmt.table_name,
					root = nroot,
				}
			}
			log.info("Updated 0 rows.")
			return true, t.root, {}
		}
	}

	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root, {} }
	defer btree.cursor_destroy(&cursor)

	current_root := table.root_page
	count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		defer cell.destroy(&c, context.temp_allocator)

		should_update := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_update = evaluate_where(&where_cl, c.values, table.columns, nil)
		}
		if should_update {
			new_row := deep_copy_values(c.values)
			for idx, val in update_map {
				new_row[idx] = val
			}
			if cell.validate(new_row, table.columns) && !values_equal(c.values, new_row) {
				tree_at := btree.init(t.pager, current_root)
				nroot, upd_err := btree.tree_update_cow(&tree_at, c.rowid, new_row)
				if upd_err == .None {
					current_root = nroot
					count += 1
				}
			}
		}
		btree.cursor_advance(&cursor)
	}
	if count > 0 {
		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, current_root)
		if !ok { return false, t.root, {} }

		log.infof("Updated %d rows.", count)
		return true, new_schema_root, Mutated_Table_Info {
			name = stmt.table_name,
			root = current_root,
		}
	}
	log.info("Updated 0 rows.")
	return true, t.root, {}
}

exec_delete_cow :: proc(
	t: ^btree.Tree,
	stmt: parser.Delete_Stmt,
) -> (
	bool,
	u32,
	Mutated_Table_Info,
) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table not found: %s", stmt.table_name)
		return false, t.root, {}
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	if where_cl, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_cl); pk_ok {
			nroot, del_err := btree.tree_delete_cow(&table_tree, target_rowid)
			if del_err == .None {
				new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, nroot)
				if !ok { return false, t.root, {} }
				log.info("Deleted 1 row.")
				return true, new_schema_root, Mutated_Table_Info {
					name = stmt.table_name,
					root = nroot,
				}
			}
			log.info("Deleted 0 rows.")
			return true, t.root, {}
		}
	}

	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root, {} }
	defer btree.cursor_destroy(&cursor)

	current_root := table.root_page
	count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		defer cell.destroy(&c, context.temp_allocator)

		should_delete := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_delete = evaluate_where(&where_cl, c.values, table.columns, nil)
		}
		if should_delete {
			tree_at := btree.init(t.pager, current_root)
			nroot, del_err := btree.tree_delete_cow(&tree_at, c.rowid)
			if del_err == .None {
				current_root = nroot
				count += 1
			}
		}
		btree.cursor_advance(&cursor)
	}
	if count > 0 {
		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, current_root)
		if !ok { return false, t.root, {} }

		log.infof("Deleted %d rows.", count)
		return true, new_schema_root, Mutated_Table_Info {
			name = stmt.table_name,
			root = current_root,
		}
	}
	log.info("Deleted 0 rows.")
	return true, t.root, {}
}
