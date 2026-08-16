// Package executor dispatches parsed SQL statements to the appropriate execution engine.
//
// execute is the core executor: it is pure (no printing, no direct I/O) and
// returns a data-only Result plus an Exec_Error. Rendering is the caller's
// responsibility via render_result (used by the CLI's db.execute).
package executor

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

execute :: proc(
	schema_tree: ^btree.Tree,
	stmt: parser.Statement,
	out: ^Result = nil,
	cache: ^schema.Table_Cache = nil,
) -> (
	ok: bool,
	new_schema_root: u32,
	mutated: Mutated_Table_Info,
) {
	new_root: u32
	switch s in stmt.type {
	case parser.Create_Stmt:
		ok, new_root, mutated = exec_create(schema_tree, s, stmt.sql)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Insert_Stmt:
		ok, new_root, mutated = exec_insert_cow(schema_tree, s, cache)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Select_Stmt:
		rows, cols, q_ok := exec_query(schema_tree, s, cache)
		if out != nil {
			out.rows = rows
			out.cols = cols
			out.is_select = true
			out.new_root = schema_tree.root
		}
		return q_ok, schema_tree.root, {}
	case parser.Compound_Stmt:
		rows, cols, q_ok := exec_compound_data(schema_tree, s, cache)
		if out != nil {
			out.rows = rows
			out.cols = cols
			out.is_select = true
			out.new_root = schema_tree.root
		}
		return q_ok, schema_tree.root, {}
	case parser.Update_Stmt:
		ok, new_root, mutated = exec_update_cow(schema_tree, s, cache)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Delete_Stmt:
		ok, new_root, mutated = exec_delete_cow(schema_tree, s, cache)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Drop_Stmt:
		ok, new_root, mutated = exec_drop(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Explain_Stmt:
		if out != nil {
			sql := strings.trim_space(s.sql)
			vals := make([]types.Value, 1, context.temp_allocator)
			vals[0] = types.value_text(sql)
			rows := make([]Row_Entry, 1, context.temp_allocator)
			rows[0] = Row_Entry{rowid = 1, values = vals}
			out.rows = rows
			cols := make([]types.Column, 1, context.temp_allocator)
			cols[0] = types.Column{name = "QUERY PLAN", type = .TEXT}
			out.cols = cols
			out.is_select = true
			out.new_root = schema_tree.root
		}
		return true, schema_tree.root, {}
	case parser.Txn_Stmt:
		return false, schema_tree.root, {}
	}
	return false, schema_tree.root, {}
}

// render_result prints a SELECT/Compound result (or EXPLAIN description) as a
// markdown table. The core executor never calls this; the CLI layer does.
render_result :: proc(out: Result) {
	if !out.is_select { return }
	cols := out.cols
	if len(cols) == 0 { return }

	indices := make([]int, len(cols), context.temp_allocator)
	for i in 0 ..< len(cols) { indices[i] = i }

	header := make([]string, len(cols), context.temp_allocator)
	for c, i in cols { header[i] = c.name }

	table_rows := make([dynamic][]string, context.temp_allocator)
	row_count := 0
	for entry in out.rows {
		row_strs := make([]string, len(cols), context.temp_allocator)
		for i in 0 ..< len(cols) {
			row_strs[i] = value_string(entry.values[i])
		}
		append(&table_rows, row_strs)
		row_count += 1
	}
	render_table(header, table_rows[:])
	fmt.printf("(%d rows)\n", row_count)
}
