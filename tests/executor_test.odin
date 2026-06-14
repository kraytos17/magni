package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

setup_executor_env :: proc(t: ^testing.T, test_name: string) -> (btree.Tree, string) {
	temp_name := fmt.tprintf("test_exec_%s.db", test_name)
	filename := strings.clone(temp_name, context.allocator)
	if os.exists(filename) {
		os.remove(filename)
	}

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "Failed to open pager")

	schema_page, alloc_err := pager.allocate_page(p)
	testing.expect(t, alloc_err == .None, "Failed to allocate schema page")
	btree.init_leaf_page(schema_page.data, schema_page.page_num)
	pager.mark_dirty(p, schema_page.page_num)
	pager.unpin_page(p, schema_page.page_num)

	tree := btree.init(p, schema_page.page_num)
	ok := schema.init(&tree)
	testing.expect(t, ok, "Failed to init schema")
	return tree, filename
}

teardown_executor_env :: proc(tree: btree.Tree, filename: string) {
	pager.close(tree.pager)
	if os.exists(filename) {
		os.remove(filename)
	}
	delete(filename)
}

make_create_stmt :: proc(name: string) -> parser.Statement {
	cols := make([dynamic]types.Column, context.temp_allocator)
	append(&cols, types.Column{name = "id", type = .INTEGER, pk = true, not_null = true})
	append(&cols, types.Column{name = "name", type = .TEXT})
	append(&cols, types.Column{name = "score", type = .REAL})

	variant := parser.Create_Stmt {
		table_name = name,
		columns    = cols[:],
	}
	return parser.Statement{type = variant, sql = "CREATE TABLE ... (MOCKED)"}
}

make_insert_stmt :: proc(table: string, id: i64, name: string, score: f64) -> parser.Statement {
	vals := make([dynamic]types.Value, context.temp_allocator)
	append(&vals, types.value_int(id))
	append(&vals, types.value_text(name))
	append(&vals, types.value_real(score))

	variant := parser.Insert_Stmt {
		table_name = table,
		values     = vals[:],
	}
	return parser.Statement{type = variant, sql = ""}
}

@(test)
test_exec_create_table :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "create")
	defer teardown_executor_env(tree, file)

	stmt := make_create_stmt("users")
	success, _ := executor.execute(&tree, stmt)

	testing.expect(t, success, "CREATE TABLE should succeed")
	testing.expect(t, schema.table_exists(&tree, "users"), "Table should exist in schema")

	success_dup, _ := executor.execute(&tree, stmt)
	testing.expect(t, !success_dup, "Duplicate CREATE TABLE should fail")
}

@(test)
test_exec_insert_select :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "insert")
	defer teardown_executor_env(tree, file)

	create_stmt := make_create_stmt("players")
	executor.execute(&tree, create_stmt)

	insert_stmt := make_insert_stmt("players", 100, "Alice", 99.5)
	success, _ := executor.execute(&tree, insert_stmt)
	testing.expect(t, success, "INSERT should succeed")

	table, _ := schema.get_table(&tree, "players", context.temp_allocator)
	table_tree := btree.init(tree.pager, table.root_page)

	count, _ := btree.tree_count_rows(&table_tree)
	testing.expect_value(t, count, 1)

	cell, err := btree.tree_find(&table_tree, 100, context.temp_allocator)
	testing.expect(t, err == .None, "Should find inserted row by PK")
	testing.expect_value(t, cell.values[1].(string), "Alice")
}

@(test)
test_exec_insert_validation_failure :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "insert_fail")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("strict_table"))
	vals := make([dynamic]types.Value, context.temp_allocator)
	append(&vals, types.value_int(1))
	append(&vals, types.value_text("A"))
	append(&vals, types.value_real(1.0))
	append(&vals, types.value_int(999))

	variant := parser.Insert_Stmt {
		table_name = "strict_table",
		values     = vals[:],
	}
	stmt := parser.Statement {
		type = variant,
		sql  = "",
	}

	success, _ := executor.execute(&tree, stmt)
	testing.expect(t, !success, "INSERT with wrong column count should fail")
}

@(test)
test_exec_update :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "update")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("inventory"))
	executor.execute(&tree, make_insert_stmt("inventory", 1, "Apple", 1.50))
	executor.execute(&tree, make_insert_stmt("inventory", 2, "Banana", 0.80))

	cond := parser.Condition {
		column   = "id",
		operator = .EQUALS,
		rhs      = types.value_int(1),
	}
	where_clause := parser.Where_Clause {
		conditions = []parser.Condition{cond},
		is_and     = true,
	}
	variant := parser.Update_Stmt {
		table_name     = "inventory",
		update_columns = []string{"score"},
		update_values  = []types.Value{types.value_real(2.00)},
		where_clause   = where_clause,
	}
	stmt := parser.Statement {
		type = variant,
		sql  = "UPDATE ...",
	}

	success, _ := executor.execute(&tree, stmt)
	testing.expect(t, success, "UPDATE should succeed")

	table, _ := schema.get_table(&tree, "inventory", context.temp_allocator)
	table_tree := btree.init(tree.pager, table.root_page)

	cell, _ := btree.tree_find(&table_tree, 1, context.temp_allocator)
	new_price := cell.values[2].(f64)
	testing.expect_value(t, new_price, 2.00)
}

@(test)
test_exec_delete :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "delete")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("logs"))
	executor.execute(&tree, make_insert_stmt("logs", 1, "Log A", 0))
	executor.execute(&tree, make_insert_stmt("logs", 2, "Log B", 0))

	cond := parser.Condition {
		column   = "name",
		operator = .EQUALS,
		rhs      = types.value_text("Log A"),
	}
	variant := parser.Delete_Stmt {
		table_name = "logs",
		where_clause = parser.Where_Clause{conditions = []parser.Condition{cond}, is_and = true},
	}
	stmt := parser.Statement {
		type = variant,
		sql  = "DELETE ...",
	}

	success, _ := executor.execute(&tree, stmt)
	testing.expect(t, success, "DELETE should succeed")

	table, _ := schema.get_table(&tree, "logs", context.temp_allocator)
	table_tree := btree.init(tree.pager, table.root_page)
	count, _ := btree.tree_count_rows(&table_tree)
	testing.expect_value(t, count, 1)
}

@(test)
test_page_splitting_stress :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "stress_split")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("stress"))
	fmt.println("--- Starting Stress Insert (70 rows) ---")
	for i in 1 ..= 70 {
		free_all(context.temp_allocator)
		name := fmt.tprintf("Row Number %d with padding to force split.............................", i)
		stmt := make_insert_stmt("stress", i64(i), name, 10.5)
		success, _ := executor.execute(&tree, stmt)
		if !success {
			fmt.printf(" [FAIL] Insert failed at row ID %d\n", i)
			testing.fail(t)
			return
		}
	}

	fmt.println("--- Finished Stress Insert ---")
	table, _ := schema.get_table(&tree, "stress", context.temp_allocator)
	root_page, _ := pager.get_page(tree.pager, table.root_page)
	header := btree.get_header(root_page.data, table.root_page)
	is_interior := header.page_type == .INTERIOR_TABLE
	testing.expect(t, is_interior, "Root page did not split! It is still a Leaf Node.")
	if is_interior {
		fmt.printf(" [PASS] Root Page %d is now Interior (Cells: %d)\n", table.root_page, header.cell_count)
	}

	table_tree := btree.init(tree.pager, table.root_page)
	cell, err := btree.tree_find(&table_tree, 60, context.temp_allocator)
	testing.expect(t, err == .None, "Could not find row 60 after split")
	testing.expect_value(t, cell.rowid, 60)
}

@(test)
test_exec_select_empty_table :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "empty_sel")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("empty_tbl"))
	sel_sql := "SELECT * FROM empty_tbl;"
	sel_stmt, _ := parser.parse(sel_sql, context.temp_allocator)
	ok, _ := executor.execute(&tree, sel_stmt)
	testing.expect(t, ok, "SELECT on empty table should succeed (not crash)")
}

@(test)
test_exec_time_travel_via_schema_root :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "time_travel")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("tt_tbl"))
	executor.execute(&tree, make_insert_stmt("tt_tbl", 1, "First", 1.0))
	executor.execute(&tree, make_insert_stmt("tt_tbl", 2, "Second", 2.0))
	schema_root_after_batch1 := tree.root

	executor.execute(&tree, make_insert_stmt("tt_tbl", 3, "Third", 3.0))
	executor.execute(&tree, make_insert_stmt("tt_tbl", 4, "Fourth", 4.0))

	tt_table, _ := schema.get_table(&tree, "tt_tbl", context.temp_allocator)
	tt_tree := btree.init(tree.pager, tt_table.root_page)
	count_all, _ := btree.tree_count_rows(&tt_tree)
	testing.expect_value(t, count_all, 4)

	tree.root = schema_root_after_batch1
	tt_table_old, found_old := schema.get_table(&tree, "tt_tbl", context.temp_allocator)
	testing.expect(t, found_old, "Table should exist in historical schema")

	tt_tree_old := btree.init(tree.pager, tt_table_old.root_page)
	count_old, _ := btree.tree_count_rows(&tt_tree_old)
	testing.expect_value(t, count_old, 2)

	cell, err := btree.tree_find(&tt_tree_old, 1, context.temp_allocator)
	testing.expect(t, err == .None, "Row 1 should exist in historical data")
	testing.expect_value(t, cell.values[1].(string), "First")

	_, err3 := btree.tree_find(&tt_tree_old, 3, context.temp_allocator)
	testing.expect(t, err3 != .None, "Row 3 should NOT exist in historical data")
}

@(test)
test_exec_as_of_snapshot_parse_and_exec :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "as_of")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("snap_tbl"))
	executor.execute(&tree, make_insert_stmt("snap_tbl", 10, "X", 1.0))
	executor.execute(&tree, make_insert_stmt("snap_tbl", 20, "Y", 2.0))

	sql := "SELECT * FROM snap_tbl AS OF SNAPSHOT 42;"
	stmt, parse_ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, parse_ok, "AS OF SNAPSHOT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	snap_id, has_snap := sel.as_of_snapshot.?
	testing.expect(t, has_snap, "Expected as_of_snapshot field")
	testing.expect_value(t, snap_id, u64(42))

	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "SELECT AS OF SNAPSHOT should execute")
}
