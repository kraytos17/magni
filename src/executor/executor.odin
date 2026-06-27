// Package executor dispatches parsed SQL statements to the appropriate execution engine.
package executor

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:parser"

execute :: proc(
	schema_tree: ^btree.Tree,
	stmt: parser.Statement,
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
		ok, new_root, mutated = exec_insert_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Select_Stmt:
		return exec_select(schema_tree, s), schema_tree.root, {}
	case parser.Update_Stmt:
		ok, new_root, mutated = exec_update_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Delete_Stmt:
		ok, new_root, mutated = exec_delete_cow(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Drop_Stmt:
		ok, new_root, mutated = exec_drop(schema_tree, s)
		schema_tree.root = new_root
		return ok, new_root, mutated
	case parser.Explain_Stmt:
		fmt.printf("QUERY PLAN\n── %s\n", strings.trim_space(s.sql))
		inner, parse_ok, _ := parser.parse(s.sql, context.temp_allocator)
		if !parse_ok { return false, schema_tree.root, {} }
		return execute(schema_tree, inner)
	case parser.Txn_Stmt:
		return false, schema_tree.root, {}
	}
	return false, schema_tree.root, {}
}
