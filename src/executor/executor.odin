package executor

import "core:fmt"
import "core:slice"
import "core:strings"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

Table_Info :: struct {
	table:   types.Table,
	tree:    btree.Tree,
	virtual: Maybe(Virtual_Table),
}

Table_Col_Range :: struct {
	table_name: string,
	start_col:  int,
	col_count:  int,
}

Table_Context :: struct {
	info:  Table_Info,
	range: Table_Col_Range,
}

Row_Entry :: struct {
	rowid:  types.Row_ID,
	values: []types.Value,
}

Virtual_Table :: struct {
	columns: []types.Column,
	rows:    []Row_Entry,
}

Sort_Ctx :: struct {
	order_clause: []parser.Order_By_Column,
	sort_indices: []int,
}

Group :: struct {
	key_values: []types.Value,
	rows:       [dynamic]Row_Entry,
}

Update_Op :: struct {
	rowid:      types.Row_ID,
	old_values: []types.Value,
	new_values: []types.Value,
}

@(private)
print_agg_header :: proc(cols: []string) {
	b := strings.builder_make(context.temp_allocator)
	for col, i in cols {
		if i > 0 do strings.write_string(&b, " | ")
		strings.write_string(&b, col)
	}

	strings.write_byte(&b, '\n')
	for col, i in cols {
		if i > 0 do strings.write_string(&b, "-+-")
		for _ in 0 ..< len(col) do strings.write_byte(&b, '-')
	}
	fmt.println(strings.to_string(b))
}

// Resolve a column name against combined columns from multiple tables.
// Supports qualified names like "table.column".
// If table_ranges is nil/empty, falls back to simple find_column_index (single-table).
@(private)
resolve_qualified_column :: proc(
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	name: string,
) -> (
	int,
	bool,
) {
	if len(table_ranges) > 0 {
		if dot_pos := strings.last_index_byte(name, '.'); dot_pos >= 0 {
			table_part := name[:dot_pos]
			col_part := name[dot_pos + 1:]
			for tr in table_ranges {
				if tr.table_name == table_part {
					end := tr.start_col + tr.col_count
					for i in tr.start_col ..< end {
						if combined_cols[i].name == col_part {
							return i, true
						}
					}
				}
			}
			return -1, false
		}
	}
	return schema.find_column_index(combined_cols, name)
}

// Checks if the WHERE clause is a simple "pk_column = literal_integer" match
// and returns the target rowid. This enables O(log n) tree_find instead of O(n) scan.
@(private)
try_pk_lookup :: proc(table: types.Table, clause: parser.Where_Clause) -> (rowid: types.Row_ID, ok: bool) {
	if len(clause.conditions) != 1 { return }
	if !clause.is_and { return }

	cond := clause.conditions[0]
	if cond.operator != .EQUALS { return }

	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if !has_pk { return }
	if table.columns[pk_idx].name != cond.column { return }

	val, is_int := cond.rhs.(types.Value).(i64)
	if !is_int { return }
	return types.Row_ID(val), true
}

@(private)
values_equal :: proc(a, b: []types.Value) -> bool {
	if len(a) != len(b) { return false }
	for v, i in a {
		if !types.value_compare(v, b[i]) { return false }
	}
	return true
}

@(private)
deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		new_values[i] = types.value_clone(v, context.temp_allocator) or_else types.Null{}
	}
	return new_values
}

@(private)
filter_rows :: proc(
	rows: []Row_Entry,
	where_clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> []Row_Entry {
	filtered := make([dynamic]Row_Entry, context.temp_allocator)
	for entry in rows {
		if evaluate_where(where_clause, entry.values, cols, table_ranges) {
			append(&filtered, entry)
		}
	}
	return filtered[:]
}

@(private)
sort_rows :: proc(
	rows: []Row_Entry,
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	sort_indices := make([]int, len(order_clause), context.temp_allocator)
	for o, i in order_clause {
		idx, col_ok := resolve_qualified_column(cols, table_ranges, o.column)
		if !col_ok {
			fmt.eprintln("Error: Unknown column in ORDER BY:", o.column)
			return false
		}
		sort_indices[i] = idx
	}

	sort_ctx := Sort_Ctx{order_clause, sort_indices}
	slice.sort_by_with_data(rows, proc(a, b: Row_Entry, data: rawptr) -> bool {
			ctx := (^Sort_Ctx)(data)
			for sort_idx, i in ctx.sort_indices {
				cmp := compare_values(a.values[sort_idx], b.values[sort_idx])
				if cmp != 0 {
					if ctx.order_clause[i].desc { return cmp > 0 }
					return cmp < 0
				}
			}
			return false
		}, &sort_ctx)
	return true
}

@(private)
display_results :: proc(
	rows: []Row_Entry,
	cols: []types.Column,
	display_indices: []int,
	limit, offset: Maybe(u64),
) {
	skip_count := u64(0)
	if off, has_off := offset.?; has_off { skip_count = off }
	limit_count := u64(0)
	has_limit := false
	if lim, has_lim := limit.?; has_lim { limit_count = lim; has_limit = true }

	print_header(cols, display_indices)
	row_count := 0
	for entry in rows {
		if skip_count > 0 { skip_count -= 1; continue }
		print_row(entry.values, display_indices)
		row_count += 1
		if has_limit && u64(row_count) >= limit_count { break }
	}
	fmt.printf("(%d rows)\n", row_count)
}

@(private)
build_display_indices :: proc(
	columns: []string,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	total_cols: int,
) -> (
	[]int,
	bool,
) {
	indices := make([dynamic]int, context.temp_allocator)
	if len(columns) == 0 {
		for i in 0 ..< total_cols {
			append(&indices, i)
		}
	} else {
		for req_col in columns {
			idx, ok := resolve_qualified_column(cols, table_ranges, req_col)
			if !ok {
				fmt.eprintln("Error: Unknown column:", req_col)
				return nil, false
			}
			append(&indices, idx)
		}
	}
	return indices[:], true
}

@(private)
try_join_match :: proc(
	outer_row: Row_Entry,
	inner_values: []types.Value,
	jc: parser.Join_Clause,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	new_rows: ^[dynamic]Row_Entry,
	matched: ^bool,
	allocator := context.allocator,
) {
	combined := make([]types.Value, len(outer_row.values) + len(inner_values), allocator)
	copy(combined[:len(outer_row.values)], outer_row.values)
	copy(combined[len(outer_row.values):], inner_values)

	on_pass := true
	if on_cl, has_on := jc.on_clause.?; has_on {
		on_pass = evaluate_where(&on_cl, combined, combined_cols, table_ranges)
	}
	if on_pass {
		matched^ = true
		append(new_rows, Row_Entry{0, combined})
	}
}

execute :: proc(schema_tree: ^btree.Tree, stmt: parser.Statement) -> (ok: bool, new_schema_root: u32) {
	switch s in stmt.type {
	case parser.Create_Stmt:
		ok := exec_create(schema_tree, s, stmt.sql)
		return ok, schema_tree.root
	case parser.Insert_Stmt:
		return exec_insert_cow(schema_tree, s)
	case parser.Select_Stmt:
		return exec_select(schema_tree, s), schema_tree.root
	case parser.Update_Stmt:
		return exec_update_cow(schema_tree, s)
	case parser.Delete_Stmt:
		return exec_delete_cow(schema_tree, s)
	case parser.Drop_Stmt:
		ok := exec_drop(schema_tree, s)
		return ok, schema_tree.root
	}
	return false, schema_tree.root
}

@(private)
exec_create :: proc(t: ^btree.Tree, stmt: parser.Create_Stmt, sql: string) -> bool {
	if ok, msg := schema.validate_columns(stmt.columns); !ok {
		fmt.eprintln("Schema Error:", msg)
		return false
	}
	if schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table already exists:", stmt.table_name)
		return false
	}

	root_page, err := pager.allocate_page(t.pager)
	for err == .None && (root_page.page_num == 1 || root_page.page_num == t.root) {
		pager.mark_dirty(t.pager, root_page.page_num)
		pager.unpin_page(t.pager, root_page.page_num)
		root_page, err = pager.allocate_page(t.pager)
	}
	if err != .None {
		fmt.eprintln("Error: Failed to allocate table root page")
		return false
	}
	defer pager.unpin_page(t.pager, root_page.page_num)

	btree.init_leaf_page(root_page.data, root_page.page_num)
	pager.mark_dirty(t.pager, root_page.page_num)
	if !schema.add_table(t, stmt.table_name, stmt.columns, root_page.page_num, sql) {
		fmt.eprintln("Error: Failed to register table in schema")
		return false
	}
	fmt.printf("Created table '%s' at Page %d\n", stmt.table_name, root_page.page_num)
	return true
}

@(private)
exec_insert :: proc(t: ^btree.Tree, stmt: parser.Insert_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	values := stmt.values
	if len(stmt.columns) > 0 {
		if len(stmt.columns) != len(stmt.values) {
			fmt.eprintln("Error: Column list length does not match value count")
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
				fmt.eprintln("Error: Unknown column:", col_name)
				return false
			}
			reordered[idx] = stmt.values[i]
		}
		values = reordered
	}
	if len(values) != len(table.columns) {
		fmt.eprintfln("Error: Column count mismatch. Expected %d, got %d", len(table.columns), len(values))
		return false
	}
	if !cell.validate(values, table.columns) {
		fmt.eprintln("Error: Data type validation failed")
		return false
	}

	table_tree := btree.init(t.pager, table.root_page)
	next_rowid: types.Row_ID = 0
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
		fmt.eprintln("Error inserting row:", err)
		return false
	}
	fmt.println("Inserted row", next_rowid)
	return true
}

// Execute a subquery and return its result rows and column names.
@(private)
exec_subquery :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> ([]Row_Entry, []types.Column) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil }
	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Subquery table not found:", tbl_name)
		return nil, nil
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	rows := scan_table(&table_tree, &table, nil, context.temp_allocator)
	if rows == nil { return nil, nil }
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(
			rows,
			&where_clause,
			table.columns,
			[]Table_Col_Range {
				{
					table_name = stmt.from_alias if stmt.from_alias != "" else tbl_name,
					start_col = 0,
					col_count = len(table.columns),
				},
			},
		)
	}

	if len(stmt.columns) > 0 {
		// Project only requested columns
		col_indices := make([]int, len(stmt.columns), context.temp_allocator)
		for req_col, i in stmt.columns {
			idx, ok := schema.find_column_index(table.columns, req_col)
			if !ok { return nil, nil }
			col_indices[i] = idx
		}

		projected := make([dynamic]Row_Entry, context.temp_allocator)
		for entry in rows {
			new_vals := make([]types.Value, len(col_indices), context.temp_allocator)
			for ci, idx in col_indices {
				cloned, _ := types.value_clone(entry.values[idx], context.temp_allocator)
				new_vals[ci] = cloned
			}
			append(&projected, Row_Entry{entry.rowid, new_vals})
		}

		// Build column defs from projected column names
		proj_cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
		for col_name, i in stmt.columns {
			proj_cols[i] = types.Column {
				name = strings.clone(col_name, context.temp_allocator),
				type = .TEXT,
			}
		}
		return rows, proj_cols
	}
	return rows, table.columns
}

@(private)
exec_select :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq {
		return exec_select_subquery(t, stmt)
	}
	if len(stmt.joins) == 0 {
		return exec_select_single(t, stmt)
	}

	table_count := 1 + len(stmt.joins)
	table_ctxs := make([]Table_Context, table_count, context.temp_allocator)
	table_ranges := make([]Table_Col_Range, table_count, context.temp_allocator)

	info: types.Table
	found: bool
	t0_alias: string
	col_count_0 := 0
	if tbl_name, is_table := stmt.from.(string); is_table {
		info, found = schema.get_table(t, tbl_name, context.temp_allocator)
		if !found {
			fmt.eprintln("Error: Table not found:", tbl_name)
			return false
		}

		table_ctxs[0] = Table_Context {
			info = {table = info, tree = btree.init(t.pager, info.root_page)},
			range = {
				table_name = stmt.from_alias if stmt.from_alias != "" else tbl_name,
				start_col = 0,
				col_count = len(info.columns),
			},
		}

		table_ranges[0] = table_ctxs[0].range
		t0_alias = stmt.from_alias if stmt.from_alias != "" else tbl_name
		col_count_0 = len(info.columns)
	} else if vt, is_vt := stmt.from.(^parser.Select_Stmt); is_vt {
		inner_rows, inner_cols := exec_subquery(t, vt^)
		if inner_rows == nil { return false }
		table_ctxs[0] = Table_Context {
			info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
			range = {table_name = stmt.from_alias, start_col = 0, col_count = len(inner_cols)},
		}

		table_ranges[0] = table_ctxs[0].range
		t0_alias = stmt.from_alias
		col_count_0 = len(inner_cols)
	}

	for join, i in stmt.joins {
		idx := i + 1
		if jt_name, is_table := join.source.(string); is_table {
			info, found = schema.get_table(t, jt_name, context.temp_allocator)
			if !found {
				fmt.eprintln("Error: Table not found:", jt_name)
				return false
			}

			prev := table_ctxs[idx - 1].range
			alias := join.alias if join.alias != "" else jt_name
			table_ctxs[idx] = Table_Context {
				info = {table = info, tree = btree.init(t.pager, info.root_page)},
				range = {
					table_name = alias,
					start_col = prev.start_col + prev.col_count,
					col_count = len(info.columns),
				},
			}
			table_ranges[idx] = table_ctxs[idx].range
		} else if subq, is_subquery := join.source.(^parser.Select_Stmt); is_subquery {
			inner_rows, inner_cols := exec_subquery(t, subq^)
			if inner_rows == nil { return false }

			alias := join.alias if join.alias != "" else ""
			prev := table_ctxs[idx - 1].range
			table_ctxs[idx] = Table_Context {
				info = {virtual = Virtual_Table{columns = inner_cols, rows = inner_rows}},
				range = {
					table_name = alias,
					start_col = prev.start_col + prev.col_count,
					col_count = len(inner_cols),
				},
			}
			table_ranges[idx] = table_ctxs[idx].range
		}
	}

	total_cols := table_ctxs[table_count - 1].range.start_col + table_ctxs[table_count - 1].range.col_count
	combined_cols := make([]types.Column, total_cols, context.temp_allocator)
	for ti in 0 ..< table_count {
		tr := table_ctxs[ti].range
		if vt, is_virtual := table_ctxs[ti].info.virtual.?; is_virtual {
			src := vt.columns
			for j in 0 ..< tr.col_count {
				combined_cols[tr.start_col + j] = src[j]
			}
		} else {
			src := table_ctxs[ti].info.table.columns
			for j in 0 ..< tr.col_count {
				combined_cols[tr.start_col + j] = src[j]
			}
		}
	}

	rows: []Row_Entry
	if col_count_0 > 0 {
		rows = scan_table(&table_ctxs[0].info.tree, &table_ctxs[0].info.table, nil, context.temp_allocator)
		if rows == nil { return false }
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		rows = vt.rows
	}

	for j_idx in 0 ..< len(stmt.joins) {
		jc := stmt.joins[j_idx]
		info_idx := j_idx + 1
		is_left := jc.join_type == .LEFT
		right_col_count := table_ctxs[info_idx].range.col_count
		new_rows := make([dynamic]Row_Entry, context.temp_allocator)
		for outer_row in rows {
			matched := false
			if vt, is_vt := table_ctxs[info_idx].info.virtual.?; is_vt {
				for c, _ in vt.rows {
					try_join_match(outer_row, c.values, jc, combined_cols, table_ranges, &new_rows, &matched)
				}
			} else {
				right_tree := &table_ctxs[info_idx].info.tree
				cursor, cursor_err := btree.cursor_start(right_tree, context.temp_allocator)
				if cursor_err != .None { return false }
				for cursor.is_valid {
					c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
					if get_err != .None {
						btree.cursor_advance(&cursor)
						continue
					}

					try_join_match(outer_row, c.values, jc, combined_cols, table_ranges, &new_rows, &matched)
					btree.cursor_advance(&cursor)
				}
				btree.cursor_destroy(&cursor)
			}

			if is_left && !matched {
				null_row := make(
					[]types.Value,
					len(outer_row.values) + right_col_count,
					context.temp_allocator,
				)

				copy(null_row[:len(outer_row.values)], outer_row.values)
				for k in len(outer_row.values) ..< len(null_row) {
					null_row[k] = types.value_null()
				}
				append(&new_rows, Row_Entry{0, null_row})
			}
		}
		rows = new_rows[:]
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(rows, &where_clause, combined_cols, table_ranges)
	}
	if len(stmt.aggregates) > 0 {
		return exec_select_aggregate_combined(stmt, rows, combined_cols, table_ranges)
	}

	display_indices, ok := build_display_indices(stmt.columns, combined_cols, table_ranges, total_cols)
	if !ok { return false }
	if order_clause, has_order := stmt.order_by.?; has_order && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) { return false }
	}

	display_results(rows, combined_cols, display_indices[:], stmt.limit, stmt.offset)
	return true
}

@(private)
exec_select_subquery :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	subq, subq_ok := stmt.from.(^parser.Select_Stmt)
	if !subq_ok { return false }
	inner_rows, virtual_cols := exec_subquery(t, subq^)
	if inner_rows == nil { return false }

	alias := stmt.from_alias
	rows := make([dynamic]Row_Entry, context.temp_allocator)
	for entry in inner_rows {
		cloned := deep_copy_values(entry.values)
		append(&rows, Row_Entry{entry.rowid, cloned})
	}

	single_range := []Table_Col_Range{{table_name = alias, start_col = 0, col_count = len(virtual_cols)}}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		filtered := filter_rows(rows[:], &where_clause, virtual_cols, single_range)
		clear(&rows); append(&rows, ..filtered)
	}

	display_indices, ok := build_display_indices(stmt.columns, virtual_cols, single_range, len(virtual_cols))
	if !ok { return false }
	if order_clause, has_order := stmt.order_by.?; has_order && len(order_clause) > 0 {
		if !sort_rows(rows[:], order_clause, virtual_cols, single_range) { return false }
	}

	display_results(rows[:], virtual_cols, display_indices[:], stmt.limit, stmt.offset)
	return true
}

@(private)
exec_select_single :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> bool {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", tbl_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	rows := scan_table(&table_tree, &table, nil, context.temp_allocator)
	if rows == nil { return false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
	}
	if where_clause, has_where := stmt.where_clause.?; has_where {
		rows = filter_rows(rows, &where_clause, table.columns, single_range)
	}
	if len(stmt.aggregates) > 0 {
		return exec_select_aggregate_combined(stmt, rows, table.columns, single_range)
	}

	display_indices, ok := build_display_indices(
		stmt.columns,
		table.columns,
		single_range,
		len(table.columns),
	)
	if !ok { return false }
	if order_clause, has_order := stmt.order_by.?; has_order && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, table.columns, single_range) { return false }
	}

	display_results(rows, table.columns, display_indices, stmt.limit, stmt.offset)
	return true
}

@(private)
exec_select_aggregate_combined :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	group_by_indices := make([]int, len(stmt.group_by), context.temp_allocator)
	for col, i in stmt.group_by {
		idx, col_ok := resolve_qualified_column(combined_cols, table_ranges, col)
		if !col_ok {
			fmt.eprintln("Error: Unknown column in GROUP BY:", col)
			return false
		}
		group_by_indices[i] = idx
	}

	groups := make([dynamic]Group, context.temp_allocator)
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			group_found := false
			for gi in 0 ..< len(groups) {
				match := true
				for ti, pi in group_by_indices {
					if compare_values(groups[gi].key_values[pi], row_entry.values[ti]) != 0 {
						match = false
						break
					}
				}
				if match {
					append(&groups[gi].rows, row_entry)
					group_found = true
					break
				}
			}
			if !group_found {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for ti, pi in group_by_indices {
					key_vals[pi] = row_entry.values[ti]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}

	print_agg_header(stmt.columns)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows {
			group_rows[ri] = row_entry.values
		}

		agg_vals := compute_aggregates(group_rows, stmt.aggregates, combined_cols, context.temp_allocator)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) {
				continue
			}
		}

		val_idx := 0
		for _, i in stmt.columns {
			if i > 0 do fmt.print(" | ")
			if val_idx < len(group_by_indices) {
				fmt.print(types.value_to_string(groups[gi].key_values[val_idx]))
				val_idx += 1
			} else {
				agg_idx := val_idx - len(group_by_indices)
				fmt.print(types.value_to_string(agg_vals[agg_idx]))
				val_idx += 1
			}
		}
		fmt.println()
	}
	fmt.printf("(%d rows)\n", len(groups))
	return true
}

@(private)
scan_table :: proc(
	tree: ^btree.Tree,
	table: ^types.Table,
	where_clause: Maybe(parser.Where_Clause),
	allocator := context.allocator,
) -> []Row_Entry {
	rows := make([dynamic]Row_Entry, allocator)
	cursor, err := btree.cursor_start(tree, allocator)
	if err != .None { return nil }
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if wc, has_wc := where_clause.?; has_wc {
			if !evaluate_where(&wc, c.values, table.columns, nil) {
				btree.cursor_advance(&cursor)
				continue
			}
		}

		cloned := deep_copy_values(c.values)
		append(&rows, Row_Entry{c.rowid, cloned})
		btree.cursor_advance(&cursor)
	}
	return rows[:]
}

@(private)
exec_update :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	update_map := make(map[int]types.Value, context.temp_allocator)
	if len(stmt.update_columns) != len(stmt.update_values) {
		fmt.eprintln("Error: Column/Value count mismatch in UPDATE")
		return false
	}
	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, ok := schema.find_column_index(table.columns, col_name)
		if !ok {
			fmt.eprintln("Error: Unknown column:", col_name)
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
				new_row := deep_copy_values(c.values)
				for idx, val in update_map {
					new_row[idx] = val
				}
				if !cell.validate(new_row, table.columns) {
					fmt.eprintln("Error: UPDATE violates column constraints")
					return false
				} else if values_equal(c.values, new_row) {
					fmt.printf("Updated 0 rows.\n")
				} else {
					btree.tree_delete(&table_tree, target_rowid)
					btree.tree_insert(&table_tree, target_rowid, new_row)
					fmt.printf("Updated 1 row.\n")
				}
			} else {
				fmt.printf("Updated 0 rows.\n")
			}
			return true
		}
	}

	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		return false
	}
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
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
				fmt.eprintln("Warning: Skipping UPDATE row", c.rowid, "— violates column constraints")
			} else if !values_equal(c.values, new_row) {
				old_row := deep_copy_values(c.values)
				append(&ops, Update_Op{c.rowid, old_row, new_row})
			}
		}
		btree.cursor_advance(&cursor)
	}

	count := 0
	for op in ops {
		if btree.tree_delete(&table_tree, op.rowid) == .None {
			if btree.tree_insert(&table_tree, op.rowid, op.new_values) != .None {
				btree.tree_insert(&table_tree, op.rowid, op.old_values)
				fmt.eprintln("Error: Failed to update row", op.rowid)
			} else {
				count += 1
			}
		}
	}
	fmt.printf("Updated %d rows.\n", count)
	return true
}

@(private)
exec_delete :: proc(t: ^btree.Tree, stmt: parser.Delete_Stmt) -> bool {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	defer schema.table_free(table, context.temp_allocator)

	targets := make([dynamic]types.Row_ID, context.temp_allocator)
	table_tree := btree.init(t.pager, table.root_page)
	if where_cl, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_cl); pk_ok {
			if btree.tree_delete(&table_tree, target_rowid) == .None {
				fmt.printf("Deleted 1 row.\n")
			} else {
				fmt.printf("Deleted 0 rows.\n")
			}
			return true
		}
	}

	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		return false
	}
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}

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
	fmt.printf("Deleted %d rows.\n", count)
	return true
}

@(private)
exec_drop :: proc(t: ^btree.Tree, stmt: parser.Drop_Stmt) -> bool {
	if !schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false
	}
	if schema.drop_table(t, stmt.table_name) {
		fmt.println("Dropped table:", stmt.table_name)
		return true
	}
	return false
}

// ─── COW DML Operations ───────────────────────────────────────────────────────

@(private)
exec_insert_cow :: proc(t: ^btree.Tree, stmt: parser.Insert_Stmt) -> (bool, u32) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false, t.root
	}
	defer schema.table_free(table, context.temp_allocator)

	values := stmt.values
	if len(stmt.columns) > 0 {
		if len(stmt.columns) != len(stmt.values) {
			fmt.eprintln("Error: Column list length does not match value count")
			return false, t.root
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
				fmt.eprintln("Error: Unknown column:", col_name)
				return false, t.root
			}
			reordered[idx] = stmt.values[i]
		}
		values = reordered
	}
	if len(values) != len(table.columns) {
		fmt.eprintfln("Error: Column count mismatch. Expected %d, got %d", len(table.columns), len(values))
		return false, t.root
	}
	if !cell.validate(values, table.columns) {
		fmt.eprintln("Error: Data type validation failed")
		return false, t.root
	}

	table_tree := btree.init(t.pager, table.root_page)
	next_rowid: types.Row_ID = 0
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

	new_data_root, ins_err := btree.tree_insert_cow(&table_tree, next_rowid, values)
	if ins_err != .None {
		fmt.eprintln("Error inserting row:", ins_err)
		return false, t.root
	}

	new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, new_data_root)
	if !ok {
		fmt.eprintln("Error: Failed to update schema root page")
		return false, t.root
	}

	fmt.println("Inserted row", next_rowid)
	return true, new_schema_root
}

@(private)
exec_update_cow :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> (bool, u32) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false, t.root
	}
	defer schema.table_free(table, context.temp_allocator)

	update_map := make(map[int]types.Value, context.temp_allocator)
	if len(stmt.update_columns) != len(stmt.update_values) {
		fmt.eprintln("Error: Column/Value count mismatch in UPDATE")
		return false, t.root
	}
	for i in 0 ..< len(stmt.update_columns) {
		col_name := stmt.update_columns[i]
		idx, col_ok := schema.find_column_index(table.columns, col_name)
		if !col_ok {
			fmt.eprintln("Error: Unknown column:", col_name)
			return false, t.root
		}
		update_map[idx] = stmt.update_values[i]
	}

	table_tree := btree.init(t.pager, table.root_page)

	// PK fast-path: use tree_find + tree_delete_cow + tree_insert_cow
	if where_clause, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_clause); pk_ok {
			c, find_err := btree.tree_find(&table_tree, target_rowid, context.temp_allocator)
			if find_err == .None {
				new_row := deep_copy_values(c.values)
				for idx, val in update_map {
					new_row[idx] = val
				}
				if !cell.validate(new_row, table.columns) {
					fmt.eprintln("Error: UPDATE violates column constraints")
					return false, t.root
				}
				if values_equal(c.values, new_row) {
					fmt.printf("Updated 0 rows.\n")
					return true, t.root
				}

				droot, del_err := btree.tree_delete_cow(&table_tree, target_rowid)
				if del_err != .None {
					fmt.eprintln("Error: Failed to delete old row in UPDATE")
					return false, t.root
				}
				cow_tree := btree.init(t.pager, droot)
				nroot, ins_err := btree.tree_insert_cow(&cow_tree, target_rowid, new_row)
				if ins_err != .None {
					fmt.eprintln("Error: Failed to insert updated row")
					return false, t.root
				}

				new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, nroot)
				if !ok { return false, t.root }
				fmt.printf("Updated 1 row.\n")
				return true, new_schema_root
			}
			fmt.printf("Updated 0 rows.\n")
			return true, t.root
		}
	}

	// Full scan for non-PK updates
	ops := make([dynamic]Update_Op, context.temp_allocator)
	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root }
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		should_update := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_update = evaluate_where(&where_cl, c.values, table.columns, nil)
		}
		if should_update {
			new_row := deep_copy_values(c.values)
			for idx, val in update_map {
				new_row[idx] = val
			}
			if !cell.validate(new_row, table.columns) {
				fmt.eprintln("Warning: Skipping UPDATE row", c.rowid, "— violates column constraints")
			} else if !values_equal(c.values, new_row) {
				old_row := deep_copy_values(c.values)
				append(&ops, Update_Op{c.rowid, old_row, new_row})
			}
		}
		btree.cursor_advance(&cursor)
	}

	current_root := table.root_page
	count := 0
	for op in ops {
		tree_at := btree.init(t.pager, current_root)
		droot, del_err := btree.tree_delete_cow(&tree_at, op.rowid)
		if del_err != .None { continue }

		cow_at := btree.init(t.pager, droot)
		nroot, ins_err := btree.tree_insert_cow(&cow_at, op.rowid, op.new_values)
		if ins_err != .None { continue }
		current_root = nroot
		count += 1
	}

	if count > 0 {
		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, current_root)
		if !ok { return false, t.root }
		fmt.printf("Updated %d rows.\n", count)
		return true, new_schema_root
	}
	fmt.printf("Updated 0 rows.\n")
	return true, t.root
}

@(private)
exec_delete_cow :: proc(t: ^btree.Tree, stmt: parser.Delete_Stmt) -> (bool, u32) {
	table, found := schema.get_table(t, stmt.table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false, t.root
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)

	// PK fast-path: single delete with COW
	if where_cl, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_cl); pk_ok {
			nroot, del_err := btree.tree_delete_cow(&table_tree, target_rowid)
			if del_err == .None {
				new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, nroot)
				if !ok { return false, t.root }
				fmt.printf("Deleted 1 row.\n")
				return true, new_schema_root
			}
			fmt.printf("Deleted 0 rows.\n")
			return true, t.root
		}
	}

	// Full scan
	targets := make([dynamic]types.Row_ID, context.temp_allocator)
	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root }
	defer btree.cursor_destroy(&cursor)

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		should_delete := true
		if where_cl, has_where := stmt.where_clause.?; has_where {
			should_delete = evaluate_where(&where_cl, c.values, table.columns, nil)
		}
		if should_delete {
			append(&targets, c.rowid)
		}
		btree.cursor_advance(&cursor)
	}

	current_root := table.root_page
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
		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, current_root)
		if !ok { return false, t.root }
		fmt.printf("Deleted %d rows.\n", count)
		return true, new_schema_root
	}
	fmt.printf("Deleted 0 rows.\n")
	return true, t.root
}

// Evaluate WHERE clause against a row.
// table_ranges is non-nil for multi-table (joined) queries to support qualified column names.
@(private)
evaluate_where :: proc(
	clause: ^parser.Where_Clause,
	row: []types.Value,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	if len(clause.conditions) == 0 {
		return true
	}

	match_res := true
	if !clause.is_and {
		match_res = false
	}
	for cond in clause.conditions {
		idx, found := resolve_qualified_column(cols, table_ranges, cond.column)
		if !found {
			return false
		}

		left_val := row[idx]
		cond_result: bool
		switch rhs in cond.rhs {
		case string:
			right_idx, rc_found1 := resolve_qualified_column(cols, table_ranges, rhs)
			if !rc_found1 { return false }
			cond_result = compare_condition(left_val, cond.operator, row[right_idx])
		case types.Value:
			cond_result = compare_condition(left_val, cond.operator, rhs)
		}
		if clause.is_and {
			match_res = match_res && cond_result
			if !match_res do return false
		} else {
			match_res = match_res || cond_result
			if match_res do return true
		}
	}
	return match_res
}

@(private)
compare_condition :: proc(val: types.Value, op: parser.Token_Type, target: types.Value) -> bool {
	if op == .LIKE {
		text, text_ok := val.(string)
		pattern, pat_ok := target.(string)
		if !text_ok || !pat_ok { return false }
		return like_match(pattern, text)
	}

	cmp := compare_values(val, target)
	#partial switch op {
	case .EQUALS:
		return cmp == 0
	case .NOT_EQUALS:
		return cmp != 0
	case .LESS_THAN:
		return cmp < 0
	case .GREATER_THAN:
		return cmp > 0
	case .LESS_EQUAL:
		return cmp <= 0
	case .GREATER_EQUAL:
		return cmp >= 0
	}
	return false
}

// Simple LIKE pattern matching.
// % matches any sequence of characters (including empty)
// _ matches any single character
like_match :: proc(pattern: string, text: string) -> bool {
	pi := 0
	ti := 0
	star_pi := -1
	star_ti := -1

	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == text[ti] || pattern[pi] == '_') {
			pi += 1
			ti += 1
		} else if pi < len(pattern) && pattern[pi] == '%' {
			star_pi = pi
			star_ti = ti
			pi += 1
		} else if star_pi != -1 {
			pi = star_pi + 1
			star_ti += 1
			ti = star_ti
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi += 1
	}
	return pi == len(pattern)
}

@(private)
compute_aggregates :: proc(
	rows: [][]types.Value,
	aggregates: []parser.Aggregate_Expr,
	columns: []types.Column,
	allocator := context.temp_allocator,
) -> []types.Value {
	results := make([]types.Value, len(aggregates), allocator)
	for agg, i in aggregates {
		col_idx := -1
		if agg.column != "" {
			found: bool
			col_idx, found = schema.find_column_index(columns, agg.column)
			if !found { col_idx = -1 }
		}

		switch agg.func {
		case .COUNT:
			if agg.column == "" {
				results[i] = types.value_int(i64(len(rows)))
			} else {
				count := 0
				for row_vals in rows {
					if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
						count += 1
					}
				}
				results[i] = types.value_int(i64(count))
			}
		case .SUM:
			sum: f64 = 0
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					#partial switch v in row_vals[col_idx] {
					case i64:
						sum += f64(v)
					case f64:
						sum += v
					}
				}
			}
			results[i] = types.value_real(sum)
		case .AVG:
			sum: f64 = 0
			count := 0
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					#partial switch v in row_vals[col_idx] {
					case i64:
						sum += f64(v); count += 1
					case f64:
						sum += v; count += 1
					}
				}
			}
			if count > 0 {
				results[i] = types.value_real(sum / f64(count))
			} else {
				results[i] = types.value_null()
			}
		case .MIN:
			if len(rows) == 0 { results[i] = types.value_null(); break }
			min := rows[0][col_idx]
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					if compare_values(row_vals[col_idx], min) < 0 {
						min = row_vals[col_idx]
					}
				}
			}
			results[i] = min
		case .MAX:
			if len(rows) == 0 { results[i] = types.value_null(); break }
			max := rows[0][col_idx]
			for row_vals in rows {
				if col_idx >= 0 && !types.is_null(row_vals[col_idx]) {
					if compare_values(row_vals[col_idx], max) > 0 {
						max = row_vals[col_idx]
					}
				}
			}
			results[i] = max
		}
	}
	return results
}

@(private)
evaluate_where_having :: proc(
	clause: parser.Where_Clause,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	if len(clause.conditions) == 0 { return true }

	match := clause.is_and
	for cond in clause.conditions {
		cond_result := false
		// Try group keys
		rhs_val, rhs_is_val := cond.rhs.(types.Value)
		for col, i in group_cols {
			if col == cond.column {
				if rhs_is_val {
					cond_result = compare_condition(group_keys[i], cond.operator, rhs_val)
				}
				break
			}
		}
		// Try aggregates
		if !cond_result {
			for agg, i in aggregates {
				name := ""
				switch agg.func {
				case .COUNT:
					name = "COUNT"
				case .SUM:
					name = "SUM"
				case .AVG:
					name = "AVG"
				case .MIN:
					name = "MIN"
				case .MAX:
					name = "MAX"
				}
				if cond.column == name && rhs_is_val {
					cond_result = compare_condition(agg_values[i], cond.operator, rhs_val)
					break
				}
			}
		}
		if clause.is_and {
			match = match && cond_result
			if !match do return false
		} else {
			match = match || cond_result
			if match do return true
		}
	}
	return match
}

@(private)
compare_values :: proc(a: types.Value, b: types.Value) -> int {
	if types.is_null(a) && types.is_null(b) do return 0
	if types.is_null(a) do return -1
	if types.is_null(b) do return 1

	#partial switch va in a {
	case i64:
		#partial switch vb in b {
		case i64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case f64:
			if f64(va) < vb do return -1
			if f64(va) > vb do return 1
			return 0
		}
	case f64:
		#partial switch vb in b {
		case f64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case i64:
			if va < f64(vb) do return -1
			if va > f64(vb) do return 1
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.compare(va, vb)
		}
	}
	return 0
}

@(private)
print_header :: proc(cols: []types.Column, indices: []int) {
	b := strings.builder_make(context.temp_allocator)
	for idx, i in indices {
		if i > 0 do strings.write_string(&b, " | ")
		strings.write_string(&b, cols[idx].name)
	}

	strings.write_byte(&b, '\n')
	for _, i in indices {
		if i > 0 do strings.write_string(&b, "-+-")
		for _ in 0 ..< len(cols[indices[i]].name) {
			strings.write_byte(&b, '-')
		}
	}
	fmt.println(strings.to_string(b))
}

@(private)
print_row :: proc(values: []types.Value, indices: []int) {
	b := strings.builder_make(context.temp_allocator)
	for idx, i in indices {
		if i > 0 do strings.write_string(&b, " | ")
		strings.write_string(&b, types.value_to_string(values[idx]))
	}
	fmt.println(strings.to_string(b))
}
