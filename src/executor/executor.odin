package executor

import "core:fmt"
import "core:slice"
import "core:strconv"
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
	new_values: []types.Value,
}

// Set by mutation procs so db.execute can update its table_roots cache.
// db.execute reads and clears these after executor.execute returns.
Mutated_Table_Info :: struct {
	name: string,
	root: u32,
}
mutated_table_info: Mutated_Table_Info

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
	ctx, ctx_ok := init_where_ctx(where_clause, cols, table_ranges, context.temp_allocator).?
	if !ctx_ok { return filtered[:] }
	if len(ctx.conditions) == 0 { return rows }
	for entry in rows {
		if evaluate_where_ctx(ctx, entry.values) {
			append(&filtered, entry)
		}
	}
	return filtered[:]
}

@(private)
dedup_rows :: proc(rows: []Row_Entry) -> []Row_Entry {
	if len(rows) <= 1 { return rows }
	seen := make(map[u64]bool, len(rows), context.temp_allocator)
	result := make([dynamic]Row_Entry, context.temp_allocator)

	for r in rows {
		key_b := strings.builder_make(context.temp_allocator)
		for v, i in r.values {
			if i > 0 { strings.write_byte(&key_b, '\x00') }
			strings.write_string(&key_b, types.value_to_string(v))
		}
		fp := u64(hash(strings.to_string(key_b)))
		if fp not_in seen {
			seen[fp] = true
			append(&result, r)
		} else {
			// Hash collision: check if this row is truly a duplicate
			is_dup := false
			for row in result {
				if len(row.values) != len(r.values) { continue }
				all_eq := true
				for j in 0 ..< len(r.values) {
					if !types.value_compare(r.values[j], row.values[j]) {
						all_eq = false
						break
					}
				}
				if all_eq {
					is_dup = true
					break
				}
			}
			if !is_dup {
				append(&result, r)
			}
		}
	}
	return result[:]
}

// FNV-1a hash for string fingerprints
@(private)
hash :: proc(s: string) -> u64 {
	h: u64 = 14695981039346656037
	for b in s {
		h ~= u64(b)
		h *= 1099511628211
	}
	return h
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

	// Fast path: single-column integer ORDER BY
	if len(order_clause) == 1 && len(rows) > 1 {
		sort_idx := sort_indices[0]
		all_int := true
		keys := make([]i64, len(rows), context.temp_allocator)
		for row, i in rows {
			if iv, ok := row.values[sort_idx].(i64); ok {
				keys[i] = iv
			} else {
				all_int = false
				break
			}
		}
		if all_int {
			desc := order_clause[0].desc
			nulls_first := order_clause[0].nulls_first
			if !nulls_first { nulls_first = desc }
			idx := make([]int, len(rows), context.temp_allocator)
			for i in 0 ..< len(rows) { idx[i] = i }
			slice.sort_by_with_data(idx, proc(a, b: int, data: rawptr) -> bool {
				k := (^[]i64)(data)
				return k[a] < k[b]
			}, &keys)
			sorted := make([]Row_Entry, len(rows), context.temp_allocator)
			if desc || nulls_first {
				for pi, i in idx {
					sorted[len(rows) - 1 - i] = rows[pi]
				}
			} else {
				for pi, i in idx {
					sorted[i] = rows[pi]
				}
			}
			copy(rows, sorted)
			return true
		}
	}

	sort_ctx := Sort_Ctx{order_clause, sort_indices}
	slice.sort_by_with_data(
		rows,
		proc(a, b: Row_Entry, data: rawptr) -> bool {
			ctx := (^Sort_Ctx)(data)
			for sort_idx, i in ctx.sort_indices {
				a_null := types.is_null(a.values[sort_idx])
				b_null := types.is_null(b.values[sort_idx])
				if a_null != b_null {
					nulls_first := ctx.order_clause[i].nulls_first
					if !nulls_first {
						nulls_first = ctx.order_clause[i].desc
					}
					return a_null == nulls_first
				}
				cmp := compare_values(a.values[sort_idx], b.values[sort_idx])
				if cmp != 0 {
					if ctx.order_clause[i].desc { return cmp > 0 }
					return cmp < 0
				}
			}
			return false
		},
		&sort_ctx,
	)
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
) {
	if on_cl, has_on := jc.on_clause.?; has_on {
		tmp := make([]types.Value, len(outer_row.values) + len(inner_values), context.temp_allocator)
		copy(tmp[:len(outer_row.values)], outer_row.values)
		copy(tmp[len(outer_row.values):], inner_values)
		if !evaluate_where(&on_cl, tmp, combined_cols, table_ranges) { return }
	}

	combined := make([]types.Value, len(outer_row.values) + len(inner_values), context.temp_allocator)
	copy(combined[:len(outer_row.values)], outer_row.values)
	copy(combined[len(outer_row.values):], inner_values)
	matched^ = true
	append(new_rows, Row_Entry{0, combined})
}

@(private)
check_constraints :: proc(values: []types.Value, table: types.Table) -> bool {
	for col in table.columns {
		if chk, has_chk := col.check_expr.?; has_chk {
			parts := strings.split(chk, " ", context.temp_allocator)
			if len(parts) < 3 {
				fmt.eprintln("Error: CHECK constraint too complex:", chk)
				return false
			}
			col_idx, col_ok := resolve_qualified_column(table.columns, nil, parts[0])
			if !col_ok {
				fmt.eprintln("Error: CHECK references unknown column:", parts[0])
				return false
			}
			left_val := values[col_idx]
			op_token := parts[1]
			val_num, parse_num := strconv.parse_i64(parts[2])
			if !parse_num {
				fmt.eprintln("Error: CHECK constraint non-integer comparison:", chk)
				return false
			}
			left_i64, is_int := left_val.(i64)
			if !is_int {
				fmt.eprintln("Error: CHECK column value is not an integer:", chk)
				return false
			}
			result := false
			if op_token == ">" { result = left_i64 > val_num }
			else if op_token == "<" { result = left_i64 < val_num }
			else if op_token == ">=" { result = left_i64 >= val_num }
			else if op_token == "<=" { result = left_i64 <= val_num }
			else if op_token == "=" { result = left_i64 == val_num }
			else if op_token == "!=" || op_token == "<>" { result = left_i64 != val_num }
			else {
				fmt.eprintln("Error: CHECK uses unsupported operator:", op_token)
				return false
			}
			if !result {
				fmt.eprintln("Error: CHECK constraint violation:", chk)
				return false
			}
		}
	}
	return true
}

execute :: proc(schema_tree: ^btree.Tree, stmt: parser.Statement) -> (ok: bool, new_schema_root: u32) {
	new_root: u32
	switch s in stmt.type {
	case parser.Create_Stmt:
		ok, new_root = exec_create(schema_tree, s, stmt.sql)
		schema_tree.root = new_root
		return ok, new_root
	case parser.Insert_Stmt:
		ok, new_root = exec_insert_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root
	case parser.Select_Stmt:
		return exec_select(schema_tree, s), schema_tree.root
	case parser.Update_Stmt:
		ok, new_root = exec_update_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root
	case parser.Delete_Stmt:
		ok, new_root = exec_delete_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root
	case parser.Drop_Stmt:
		ok, new_root = exec_drop(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root
	case parser.Explain_Stmt:
		fmt.printf("QUERY PLAN\n── %s\n", strings.trim_space(s.sql))
		inner, parse_ok := parser.parse(s.sql, context.temp_allocator)
		if !parse_ok { return false, schema_tree.root }
		return execute(schema_tree, inner)
	case parser.Txn_Stmt:
		return false, schema_tree.root
	}
	return false, schema_tree.root
}

@(private)
exec_create :: proc(t: ^btree.Tree, stmt: parser.Create_Stmt, sql: string) -> (bool, u32) {
	if ok, msg := schema.validate_columns(stmt.columns); !ok {
		fmt.eprintln("Schema Error:", msg)
		return false, t.root
	}
	if schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table already exists:", stmt.table_name)
		return false, t.root
	}

	root_page, err := pager.allocate_page(t.pager)
	for err == .None && (root_page.page_num == 1 || root_page.page_num == t.root) {
		pager.mark_dirty(t.pager, root_page.page_num)
		pager.unpin_page(t.pager, root_page.page_num)
		root_page, err = pager.allocate_page(t.pager)
	}
	if err != .None {
		fmt.eprintln("Error: Failed to allocate table root page")
		return false, t.root
	}
	defer pager.unpin_page(t.pager, root_page.page_num)

	btree.init_leaf_page(root_page.data, root_page.page_num)
	pager.mark_dirty(t.pager, root_page.page_num)
	// Verify FOREIGN KEY references
	for fk in stmt.foreign_keys {
		if !schema.table_exists(t, fk.ref_table) {
			fmt.eprintf("Error: Referenced table '%s' does not exist (FOREIGN KEY on '%s')\n", fk.ref_table, fk.col)
			return false, t.root
		}
	}
	new_root, ok := schema.add_table_cow(t, stmt.table_name, stmt.columns, root_page.page_num, sql)
	if !ok {
		fmt.eprintln("Error: Failed to register table in schema")
		return false, t.root
	}
	mutated_table_info.name = stmt.table_name
	mutated_table_info.root = root_page.page_num
	fmt.printf("Created table '%s' at Page %d\n", stmt.table_name, root_page.page_num)
	return true, new_root
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
	if !check_constraints(values, table) { return false }

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
	rows, scan_err := scan_table(&table_tree, &table, nil, nil, context.temp_allocator)
	if scan_err { return nil, nil }
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
		return projected[:], proj_cols
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
		r, scan_err := scan_table(
			&table_ctxs[0].info.tree,
			&table_ctxs[0].info.table,
			nil,
			nil,
			context.temp_allocator,
		)
		if scan_err { return false }
		rows = r
	} else if vt, is_virtual := table_ctxs[0].info.virtual.?; is_virtual {
		rows = vt.rows
	}

	for j_idx in 0 ..< len(stmt.joins) {
		jc := stmt.joins[j_idx]
		info_idx := j_idx + 1
		is_left := jc.join_type == .LEFT
		right_col_count := table_ctxs[info_idx].range.col_count
		new_rows := make([dynamic]Row_Entry, context.temp_allocator)

		right_rows: []Row_Entry
		if vt, is_vt := table_ctxs[info_idx].info.virtual.?; is_vt {
			right_rows = vt.rows
		} else {
			right_tree := &table_ctxs[info_idx].info.tree
			right_table := &table_ctxs[info_idx].info.table
			right_rows, _ = scan_table(right_tree, right_table, nil, nil, context.temp_allocator)
		}

		// Try hash join for equi-joins, fall back to nested-loop
		hash_used := false
		if on_cl, has_on := jc.on_clause.?; has_on && len(on_cl.conditions) == 1 {
			cond := on_cl.conditions[0]
			if cond.operator == .EQUALS {
				if rhs_str, is_col := cond.rhs.(string); is_col {
					left_idx, left_ok := resolve_qualified_column(combined_cols, table_ranges, cond.column)
					right_idx, right_ok := resolve_qualified_column(combined_cols, table_ranges, rhs_str)
					if left_ok && right_ok {
						hash_used = true
						left_adjust := 0
						right_adjust := table_ctxs[info_idx].range.start_col

						// Check if key type is integer for faster hash key
						key_is_int := false
						if len(rows) > 0 && len(right_rows) > 0 {
							if _, ok := rows[0].values[left_idx - left_adjust].(i64); ok {
								key_is_int = true
							}
						}

						if key_is_int {
							if len(rows) <= len(right_rows) {
								ht := make(map[i64][]int, len(rows), context.temp_allocator)
								for row, ri in rows {
									key := row.values[left_idx - left_adjust].(i64)
									if existing, ok := ht[key]; ok {
										n := make([]int, len(existing) + 1, context.temp_allocator)
										copy(n, existing)
										n[len(existing)] = ri
										ht[key] = n
									} else {
										ht[key] = {ri}
									}
								}
								matched_left := make(map[int]bool, len(rows), context.temp_allocator)
								for r_row in right_rows {
									key := r_row.values[right_idx - right_adjust].(i64)
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[ri] = true
											combined := make([]types.Value, len(rows[ri].values) + len(r_row.values), context.temp_allocator)
											copy(combined[:len(rows[ri].values)], rows[ri].values)
											copy(combined[len(rows[ri].values):], r_row.values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make([]types.Value, len(rows[li].values) + right_col_count, context.temp_allocator)
										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 { null_row[k] = types.value_null() }
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							} else {
								ht := make(map[i64][]int, len(right_rows), context.temp_allocator)
								for r_row, ri in right_rows {
									key := r_row.values[right_idx - right_adjust].(i64)
									if existing, ok := ht[key]; ok {
										n := make([]int, len(existing) + 1, context.temp_allocator)
										copy(n, existing)
										n[len(existing)] = ri
										ht[key] = n
									} else {
										ht[key] = {ri}
									}
								}
								matched_left := make(map[int]bool, len(rows), context.temp_allocator)
								for l_row, li in rows {
									key := l_row.values[left_idx - left_adjust].(i64)
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[li] = true
											combined := make([]types.Value, len(l_row.values) + len(right_rows[ri].values), context.temp_allocator)
											copy(combined[:len(l_row.values)], l_row.values)
											copy(combined[len(l_row.values):], right_rows[ri].values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make([]types.Value, len(rows[li].values) + right_col_count, context.temp_allocator)
										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 { null_row[k] = types.value_null() }
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							}
						} else {
							if len(rows) <= len(right_rows) {
								ht := make(map[string][]int, len(rows), context.temp_allocator)
								for row, ri in rows {
									key := types.value_to_string(row.values[left_idx - left_adjust])
									if existing, ok := ht[key]; ok {
										n := make([]int, len(existing) + 1, context.temp_allocator)
										copy(n, existing)
										n[len(existing)] = ri
										ht[key] = n
									} else {
										ht[key] = {ri}
									}
								}
								matched_left := make(map[int]bool, len(rows), context.temp_allocator)
								for r_row in right_rows {
									key := types.value_to_string(r_row.values[right_idx - right_adjust])
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[ri] = true
											combined := make([]types.Value, len(rows[ri].values) + len(r_row.values), context.temp_allocator)
											copy(combined[:len(rows[ri].values)], rows[ri].values)
											copy(combined[len(rows[ri].values):], r_row.values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make([]types.Value, len(rows[li].values) + right_col_count, context.temp_allocator)
										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 { null_row[k] = types.value_null() }
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							} else {
								ht := make(map[string][]int, len(right_rows), context.temp_allocator)
								for r_row, ri in right_rows {
									key := types.value_to_string(r_row.values[right_idx - right_adjust])
									if existing, ok := ht[key]; ok {
										n := make([]int, len(existing) + 1, context.temp_allocator)
										copy(n, existing)
										n[len(existing)] = ri
										ht[key] = n
									} else {
										ht[key] = {ri}
									}
								}
								matched_left := make(map[int]bool, len(rows), context.temp_allocator)
								for l_row, li in rows {
									key := types.value_to_string(l_row.values[left_idx - left_adjust])
									if matches, has := ht[key]; has {
										for ri in matches {
											matched_left[li] = true
											combined := make([]types.Value, len(l_row.values) + len(right_rows[ri].values), context.temp_allocator)
											copy(combined[:len(l_row.values)], l_row.values)
											copy(combined[len(l_row.values):], right_rows[ri].values)
											append(&new_rows, Row_Entry{0, combined})
										}
									}
								}
								delete(ht)
								if is_left {
									for li in 0 ..< len(rows) {
										if li in matched_left { continue }
										null_row := make([]types.Value, len(rows[li].values) + right_col_count, context.temp_allocator)
										copy(null_row[:len(rows[li].values)], rows[li].values)
										for k := len(rows[li].values); k < len(null_row); k += 1 { null_row[k] = types.value_null() }
										append(&new_rows, Row_Entry{0, null_row})
									}
								}
								delete(matched_left)
							}
						}
					}
				}
			}
		}

		if !hash_used {
			// Join reorder: if right is smaller, swap to minimize inner loop iterations
			if is_left || len(right_rows) >= len(rows) {
				for outer_row in rows {
					matched := false
					for right_row in right_rows {
						try_join_match(
							outer_row,
							right_row.values,
							jc,
							combined_cols,
							table_ranges,
							&new_rows,
							&matched,
						)
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
			} else {
				// Right table is smaller — swap: use right_rows as outer
				for r_row in right_rows {
					for l_row in rows {
						_dummy := false
						try_join_match(
							l_row,
							r_row.values,
							jc,
							combined_cols,
							table_ranges,
							&new_rows,
							&_dummy,
						)
					}
				}
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
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, combined_cols, table_ranges) { return false }
	}
	if stmt.is_distinct {
		rows = dedup_rows(rows)
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
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
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
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
	}

	_, has_lim := stmt.limit.?
	max_rows := stmt.limit if has_lim && !has_order else nil
	rows, scan_err := scan_table(&table_tree, &table, stmt.where_clause, max_rows, context.temp_allocator)
	if scan_err { return false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
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
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, table.columns, single_range) { return false }
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }

	display_results(rows, table.columns, display_indices, stmt.limit, stmt.offset)
	return true
}

// Like exec_select_single but returns the row data instead of displaying.
exec_select_single_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	tbl_name, name_ok := stmt.from.(string)
	if !name_ok { return nil, nil, false }

	table, found := schema.get_table(t, tbl_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", tbl_name)
		return nil, nil, false
	}
	defer schema.table_free(table, context.temp_allocator)

	table_tree := btree.init(t.pager, table.root_page)
	has_order := false
	if order_clause, has_o := stmt.order_by.?; has_o && len(order_clause) > 0 {
		has_order = true
	}

	_, has_lim := stmt.limit.?
	max_rows := stmt.limit if has_lim && !has_order else nil
	rows, scan_err := scan_table(&table_tree, &table, stmt.where_clause, max_rows, context.temp_allocator)
	if scan_err { return nil, nil, false }

	from_name := stmt.from_alias if stmt.from_alias != "" else tbl_name
	single_range := []Table_Col_Range {
		{table_name = from_name, start_col = 0, col_count = len(table.columns)},
	}

	if len(stmt.aggregates) > 0 {
		return nil, nil, false
	}
	if order_clause, has_o2 := stmt.order_by.?; has_o2 && len(order_clause) > 0 {
		if !sort_rows(rows, order_clause, table.columns, single_range) { return nil, nil, false }
	}
	if stmt.is_distinct { rows = dedup_rows(rows) }
	if limit, has_limit := stmt.limit.?; has_limit {
		off := u64(0)
		if o, has_off := stmt.offset.?; has_off { off = o }
		start := int(min(off, u64(len(rows))))
		end := int(min(off + limit, u64(len(rows))))
		rows = rows[start:end]
	}
	return rows, table.columns, true
}

// Runs a SELECT query and returns the result data without displaying.
// Returns (rows, column_defs, ok).
exec_query :: proc(t: ^btree.Tree, stmt: parser.Select_Stmt) -> ([]Row_Entry, []types.Column, bool) {
	_, is_subq := stmt.from.(^parser.Select_Stmt)
	if is_subq {
		inner_rows, inner_cols := exec_subquery(t, stmt)
		return inner_rows, inner_cols, inner_rows != nil
	}
	if len(stmt.joins) == 0 {
		return exec_select_single_data(t, stmt)
	}
	ok := exec_select(t, stmt)
	return nil, nil, ok
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
	group_map := make(map[string]int, context.temp_allocator)
	defer delete(group_map)

	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			key_b := strings.builder_make(context.temp_allocator)
			for ti, _ in group_by_indices {
				if ti > 0 { strings.write_string(&key_b, "\x00") }
				strings.write_string(&key_b, types.value_to_string(row_entry.values[ti]))
			}

			key := strings.to_string(key_b)
			if gi, exists := group_map[key]; exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for ti, pi in group_by_indices {
					key_vals[pi] = row_entry.values[ti]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				group_map[key] = len(groups)
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
	max_rows: Maybe(u64),
	allocator := context.allocator,
) -> (
	rows: []Row_Entry,
	err: bool,
) {
	r := make([dynamic]Row_Entry, allocator)
	cursor, c_err := btree.cursor_start(tree, allocator)
	if c_err != .None { return nil, true }
	defer btree.cursor_destroy(&cursor)

	where_ctx: Maybe(Where_Eval_Ctx)
	if wc, has_wc := where_clause.?; has_wc {
		where_ctx = init_where_ctx(&wc, table.columns, nil, allocator)
	}
	use_where := false
	where_ctx_val: Where_Eval_Ctx
	if ctx, ok := where_ctx.?; ok && len(ctx.conditions) > 0 {
		use_where = true
		where_ctx_val = ctx
	}

	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		if use_where {
			if !evaluate_where_ctx(where_ctx_val, c.values) {
				cell.destroy(&c)
				btree.cursor_advance(&cursor)
				continue
			}
		}

		append(&r, Row_Entry{c.rowid, c.values})
		if limit, has_limit := max_rows.?; has_limit && u64(len(r)) >= limit {
			break
		}
		btree.cursor_advance(&cursor)
	}
	return r[:], false
}

@(private)
exec_update :: proc(t: ^btree.Tree, stmt: parser.Update_Stmt) -> bool {
	// NOTE: this function assumes context.temp_allocator is valid for the
	// entire call. old_row values captured during the scan are read during
	// the rollback loop. The caller (db.execute) must not call free_all
	// between these two phases.
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
					btree.tree_update(&table_tree, target_rowid, new_row)
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
exec_drop :: proc(t: ^btree.Tree, stmt: parser.Drop_Stmt) -> (bool, u32) {
	if !schema.table_exists(t, stmt.table_name) {
		fmt.eprintln("Error: Table not found:", stmt.table_name)
		return false, t.root
	}

	new_root, ok := schema.drop_table_cow(t, stmt.table_name)
	if ok {
		mutated_table_info.name = stmt.table_name
		mutated_table_info.root = 0
		fmt.println("Dropped table:", stmt.table_name)
		return true, new_root
	}
	return false, t.root
}

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

	if !check_constraints(values, table) { return false, t.root }

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
	mutated_table_info.name = stmt.table_name
	mutated_table_info.root = new_data_root

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

				nroot, upd_err := btree.tree_update_cow(&table_tree, target_rowid, new_row)
				if upd_err != .None {
					fmt.eprintln("Error: Failed to update row")
					return false, t.root
				}

				mutated_table_info.name = stmt.table_name
				mutated_table_info.root = nroot
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
	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root }
	defer btree.cursor_destroy(&cursor)

	current_root := table.root_page
	count := 0

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
		mutated_table_info.name = stmt.table_name
		mutated_table_info.root = current_root
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
	if where_cl, has_where := stmt.where_clause.?; has_where {
		if target_rowid, pk_ok := try_pk_lookup(table, where_cl); pk_ok {
			nroot, del_err := btree.tree_delete_cow(&table_tree, target_rowid)
			if del_err == .None {
				mutated_table_info.name = stmt.table_name
				mutated_table_info.root = nroot
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
	cursor, cursor_err := btree.cursor_start(&table_tree, context.temp_allocator)
	if cursor_err != .None { return false, t.root }
	defer btree.cursor_destroy(&cursor)

	current_root := table.root_page
	count := 0

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
		mutated_table_info.name = stmt.table_name
		mutated_table_info.root = current_root
		new_schema_root, ok := schema.update_root_page_cow(t, stmt.table_name, current_root)
		if !ok { return false, t.root }
		fmt.printf("Deleted %d rows.\n", count)
		return true, new_schema_root
	}
	fmt.printf("Deleted 0 rows.\n")
	return true, t.root
}

Resolved_Condition :: struct {
	col_idx:      int,
	operator:     parser.Token_Type,
	rhs:          types.Value,
	has_right_col: bool,
	right_idx:    int,
	has_in:       bool,
	in_values:    []types.Value,
	in_subquery:  ^parser.Select_Stmt,
}

Where_Eval_Ctx :: struct {
	conditions:  []Resolved_Condition,
	is_and:      bool,
}

// Build a Where_Eval_Ctx from a WHERE clause, resolving column names to indices once.
// Returns nil if resolution fails.
@(private)
init_where_ctx :: proc(
	clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	allocator := context.allocator,
) -> Maybe(Where_Eval_Ctx) {
	if len(clause.conditions) == 0 {
		return Where_Eval_Ctx{}
	}
	resolved := make([]Resolved_Condition, len(clause.conditions), allocator)
	for cond, i in clause.conditions {
		idx, found := resolve_qualified_column(cols, table_ranges, cond.column)
		if !found { return nil }
		rc := Resolved_Condition{
			col_idx = idx,
			operator = cond.operator,
			has_in = cond.operator == .IN,
		}
		if rhs_str, is_col := cond.rhs.(string); is_col {
			right_idx, rc_found := resolve_qualified_column(cols, table_ranges, rhs_str)
			if !rc_found { return nil }
			rc.has_right_col = true
			rc.right_idx = right_idx
		} else if val, is_val := cond.rhs.(types.Value); is_val {
			rc.rhs = val
		}
		if cond.in_values != nil {
			rc.in_values = cond.in_values
		}
		if cond.in_subquery != nil {
			rc.in_subquery = cond.in_subquery
		}
		resolved[i] = rc
	}
	return Where_Eval_Ctx{conditions = resolved, is_and = clause.is_and}
}

// Evaluate a pre-resolved WHERE context against a row. No string lookups.
@(private)
evaluate_where_ctx :: proc(ctx: Where_Eval_Ctx, row: []types.Value) -> bool {
	if len(ctx.conditions) == 0 { return true }
	match_res := ctx.is_and
	for rc in ctx.conditions {
		left_val := row[rc.col_idx]
		cond_result: bool
		if rc.has_right_col {
			cond_result = compare_condition(left_val, rc.operator, row[rc.right_idx])
		} else {
			cond_result = compare_condition(left_val, rc.operator, rc.rhs)
		}
		if rc.has_in && rc.in_values != nil {
			cond_result = false
			for v in rc.in_values {
				if compare_values(left_val, v) == 0 {
					cond_result = true; break
				}
			}
		} else if rc.has_in && rc.in_subquery != nil {
			cond_result = false
			subq_rows, _ := exec_subquery(nil, rc.in_subquery^)
			for sr in subq_rows {
				if len(sr.values) > 0 && compare_values(left_val, sr.values[0]) == 0 {
					cond_result = true; break
				}
			}
		}
		if ctx.is_and {
			match_res = match_res && cond_result
			if !match_res { return false }
		} else {
			match_res = match_res || cond_result
			if match_res { return true }
		}
	}
	return match_res
}

// Evaluate WHERE clause against a row (convenience wrapper — resolves per call).
@(private)
evaluate_where :: proc(
	clause: ^parser.Where_Clause,
	row: []types.Value,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	ctx, ok := init_where_ctx(clause, cols, table_ranges, context.temp_allocator).?
	if !ok { return false }
	return evaluate_where_ctx(ctx, row)
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
write_value_to_builder :: proc(b: ^strings.Builder, v: types.Value) {
	switch val in v {
	case types.Null:
		strings.write_string(b, "NULL")
	case i64:
		strings.write_i64(b, val)
	case f64:
		strings.write_f64(b, val, 'f')
	case string:
		strings.write_string(b, val)
	case []u8:
		strings.write_string(b, "<BLOB ")
		strings.write_int(b, len(val))
		strings.write_string(b, " bytes>")
	case:
		strings.write_string(b, "<?>")
	}
}

print_row :: proc(values: []types.Value, indices: []int) {
	b := strings.builder_make(context.temp_allocator)
	for idx, i in indices {
		if i > 0 do strings.write_string(&b, " | ")
		write_value_to_builder(&b, values[idx])
	}
	fmt.println(strings.to_string(b))
}
