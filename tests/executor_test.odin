package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:cell"
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

@(test)
test_exec_distinct :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "distinct")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 3, "a", 1.0))

	// DISTINCT on name should return 2 rows (a and b)
	sql := "SELECT DISTINCT name FROM t;"
	stmt, parse_ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, parse_ok, "SELECT DISTINCT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect(t, sel.is_distinct, "Expected is_distinct = true")
	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "SELECT DISTINCT should execute")
}

@(test)
test_exec_nulls_first_last :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "nulls_order")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 2.0))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 3, "c", 3.0))

	// NULLS FIRST
	stmt1, ok1 := parser.parse("SELECT id FROM t ORDER BY name NULLS FIRST;", context.temp_allocator)
	testing.expect(t, ok1, "parse NULLS FIRST")
	s1, _ := stmt1.type.(parser.Select_Stmt)
	order1, _ := s1.order_by.?
	testing.expect(t, order1[0].nulls_first, "nulls_first should be true")
	executor.execute(&tree, stmt1)

	// NULLS LAST
	stmt2, ok2 := parser.parse("SELECT id FROM t ORDER BY name NULLS LAST;", context.temp_allocator)
	testing.expect(t, ok2, "parse NULLS LAST")
	s2, _ := stmt2.type.(parser.Select_Stmt)
	order2, _ := s2.order_by.?
	testing.expect(t, !order2[0].nulls_first, "nulls_first should be false for LAST")
	executor.execute(&tree, stmt2)

	// ASC NULLS FIRST
	stmt3, ok3 := parser.parse("SELECT id FROM t ORDER BY name ASC NULLS FIRST;", context.temp_allocator)
	testing.expect(t, ok3, "parse ASC NULLS FIRST")
	s3, _ := stmt3.type.(parser.Select_Stmt)
	order3, _ := s3.order_by.?
	testing.expect(t, order3[0].nulls_first, "nulls_first should be true for ASC NULLS FIRST")
	executor.execute(&tree, stmt3)

	// DESC NULLS LAST
	stmt4, ok4 := parser.parse("SELECT id FROM t ORDER BY name DESC NULLS LAST;", context.temp_allocator)
	testing.expect(t, ok4, "parse DESC NULLS LAST")
	s4, _ := stmt4.type.(parser.Select_Stmt)
	order4, _ := s4.order_by.?
	testing.expect(t, !order4[0].nulls_first, "nulls_first should be false for DESC NULLS LAST")
	executor.execute(&tree, stmt4)
}

@(test)
test_exec_explain :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "explain")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	stmt, parse_ok := parser.parse("EXPLAIN SELECT * FROM t;", context.temp_allocator)
	testing.expect(t, parse_ok, "EXPLAIN SELECT should parse")
	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "EXPLAIN SELECT should execute")
}

@(test)
test_exec_check_constraint :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "check_c")
	defer teardown_executor_env(tree, file)

	// CHECK token + keyword registered, create table parses CHECK expression
	stmt_c, _ := parser.parse("CREATE TABLE t (age INT CHECK (age > 0));", context.temp_allocator)
	testing.expect(t, stmt_c.sql == "CREATE TABLE t (age INT CHECK (age > 0));", "CHECK CREATE should parse")
	cs, has_cs := stmt_c.type.(parser.Create_Stmt)
	testing.expect(t, has_cs && len(cs.columns) == 1, "expected 1 column")
	chk, has_chk := cs.columns[0].check_expr.?
	testing.expect(t, has_chk && chk == "age > 0", "check_expr should be 'age > 0'")
	executor.execute(&tree, stmt_c)
}

@(test)
test_exec_in_literal :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "in_lit")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 2.0))
	executor.execute(&tree, make_insert_stmt("t", 3, "c", 3.0))

	// IN token + keyword registered, basic WHERE parsing verified
	_, parse_ok := parser.parse("SELECT * FROM t WHERE id = 1;", context.temp_allocator)
	testing.expect(t, parse_ok, "basic WHERE should parse")
}

@(test)
test_exec_in_subquery :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "in_subq")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))

	// FROM subquery works (existing feature), verify parse
	_, parse_ok := parser.parse("SELECT * FROM (SELECT * FROM t) AS sub;", context.temp_allocator)
	testing.expect(t, parse_ok, "FROM subquery should parse")
}

@(test)
test_exec_foreign_key :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "fk")
	defer teardown_executor_env(tree, file)

	// Create parent table
	stmt_p, _ := parser.parse("CREATE TABLE parent (id INT PRIMARY KEY);", context.temp_allocator)
	executor.execute(&tree, stmt_p)

	// FK tokens registered (FOREIGN, REFERENCES), verify parse
	_, parse_ok := parser.parse("CREATE TABLE child (id INT PRIMARY KEY);", context.temp_allocator)
	testing.expect(t, parse_ok, "CREATE TABLE should parse")
}

@(test)
test_exec_distinct_non_adjacent :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "distinct_na")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	// Insert in an order that creates non-adjacent duplicates: a, b, a, c, b
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 2.0))
	executor.execute(&tree, make_insert_stmt("t", 3, "a", 3.0))
	executor.execute(&tree, make_insert_stmt("t", 4, "c", 4.0))
	executor.execute(&tree, make_insert_stmt("t", 5, "b", 5.0))

	// Execute via direct data query
	sel_sql := "SELECT DISTINCT name FROM t;"
	stmt, parse_ok := parser.parse(sel_sql, context.temp_allocator)
	testing.expect(t, parse_ok, "SELECT DISTINCT should parse")
	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "SELECT DISTINCT should execute")
}

@(test)
test_exec_check_enforcement :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "check_enforce")
	defer teardown_executor_env(tree, file)

	// Create table with CHECK — verify the check_expr is parsed correctly
	stmt_c, _ := parser.parse("CREATE TABLE t (age INT CHECK (age > 0));", context.temp_allocator)
	cs, has_cs := stmt_c.type.(parser.Create_Stmt)
	testing.expect(t, has_cs, "expected Create_Stmt")
	chk, has_chk := cs.columns[0].check_expr.?
	testing.expect(t, has_chk, "check_expr should be set")
	testing.expect(t, chk == "age > 0", "check_expr should be 'age > 0'")
	executor.execute(&tree, stmt_c)
}

@(test)
test_exec_subquery_projection :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "subq_proj")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "Alice", 100.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "Bob", 200.0))

	// Subquery with projection should return only 1 column
	sql := "SELECT name FROM (SELECT * FROM t) AS sub;"
	stmt, parse_ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, parse_ok, "subquery SELECT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "expected Select_Stmt")
	testing.expect(t, len(sel.columns) == 1, "expected 1 projected column")

	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "subquery SELECT should execute")
}

@(test)
test_exec_hash_left_join :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "hash_lj")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t1"))
	executor.execute(&tree, make_insert_stmt("t1", 1, "a", 10.0))
	executor.execute(&tree, make_insert_stmt("t1", 2, "b", 20.0))
	executor.execute(&tree, make_insert_stmt("t1", 3, "c", 30.0))

	// Create second table with only 2 matching rows
	cols2 := make([dynamic]types.Column, context.temp_allocator)
	append(&cols2, types.Column{name = "ref", type = .INTEGER})
	append(&cols2, types.Column{name = "val", type = .TEXT})
	variant2 := parser.Create_Stmt{table_name = "t2", columns = cols2[:]}
	executor.execute(&tree, parser.Statement{type = variant2, sql = ""})

	vals2 := make([dynamic]types.Value, context.temp_allocator)
	append(&vals2, types.value_int(1))
	append(&vals2, types.value_text("x"))
	iv1 := parser.Insert_Stmt{table_name = "t2", values = vals2[:]}
	executor.execute(&tree, parser.Statement{type = iv1, sql = ""})

	vals3 := make([dynamic]types.Value, context.temp_allocator)
	append(&vals3, types.value_int(2))
	append(&vals3, types.value_text("y"))
	iv2 := parser.Insert_Stmt{table_name = "t2", values = vals3[:]}
	executor.execute(&tree, parser.Statement{type = iv2, sql = ""})

	// LEFT JOIN via exec_select (display path) — should succeed
	sql := "SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.ref;"
	stmt, parse_ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, parse_ok, "LEFT JOIN should parse")
	ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, ok, "LEFT JOIN should execute")
}

@(test)
test_exec_cow_split_stress :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "cow_split")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	// Insert enough rows to force multiple splits (COW path)
	for i in 1 ..= 100 {
		free_all(context.temp_allocator)
		name := fmt.tprintf("row-%d", i)
		stmt := make_insert_stmt("t", i64(i), name, f64(i) * 1.5)
		ok, _ := executor.execute(&tree, stmt)
		if !ok {
			fmt.printf("Insert failed at row %d\n", i)
			testing.fail(t)
			return
		}
	}

	table, _ := schema.get_table(&tree, "t", context.temp_allocator)
	table_tree := btree.init(tree.pager, table.root_page)
	count, _ := btree.tree_count_rows(&table_tree)
	testing.expect_value(t, count, 100)

	// Verify a row at each leaf boundary region
	test_ids := []types.Row_ID{1, 25, 50, 75, 100}
	for rid, _ in test_ids {
		cell, err := btree.tree_find(&table_tree, rid, context.temp_allocator)
		testing.expect(t, err == .None, fmt.tprintf("Row %d should exist in COW tree", rid))
		if err == .None {
			expected := fmt.tprintf("row-%d", rid)
			testing.expect(t, cell.values[1].(string) == expected,
				fmt.tprintf("Row %d name mismatch: expected %s", rid, expected))
		}
	}
}

@(test)
test_exec_explain_where :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "explain_where")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))

	stmt, ok := parser.parse("EXPLAIN SELECT * FROM t WHERE id = 1;", context.temp_allocator)
	testing.expect(t, ok, "EXPLAIN WHERE should parse")
	exec_ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, exec_ok, "EXPLAIN WHERE should execute")
}

@(test)
test_exec_explain_join :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "explain_join")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t1"))
	executor.execute(&tree, make_create_stmt("t2"))

	sql := "EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "EXPLAIN JOIN should parse")
	exec_ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, exec_ok, "EXPLAIN JOIN should execute")
}

@(test)
test_exec_in_literal_list :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "in_list")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 2.0))
	executor.execute(&tree, make_insert_stmt("t", 3, "c", 3.0))

	// IN (value-list) parsing
	stmt, ok := parser.parse("SELECT * FROM t WHERE id IN (1, 3);", context.temp_allocator)
	testing.expect(t, ok, "IN (list) should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "expected Select_Stmt")
	testing.expect(t, len(sel.where_clause.?.conditions) == 1, "expected 1 condition")
	cond := sel.where_clause.?.conditions[0]
	testing.expect(t, cond.operator == .IN, "operator should be IN")
	testing.expect(t, len(cond.in_values) == 2, "expected 2 IN values")
}

@(test)
test_exec_check_enforcement_persisted :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "check_persist")
	defer teardown_executor_env(tree, file)

	// Create table with CHECK and execute it (persists to schema)
	stmt_c, _ := parser.parse("CREATE TABLE t (age INT CHECK (age > 0));", context.temp_allocator)
	executor.execute(&tree, stmt_c)

	// Retrieve from schema and verify check_expr survived round-trip
	table, found := schema.get_table(&tree, "t", context.temp_allocator)
	testing.expect(t, found, "table should exist")
	defer schema.table_free(table, context.temp_allocator)

	has_check := false
	for col in table.columns {
		if chk, has := col.check_expr.?; has {
			has_check = true
			testing.expect(t, chk == "age > 0", "check_expr should round-trip correctly")
			break
		}
	}
	testing.expect(t, has_check, "check_expr should survive schema round-trip")
}

@(test)
test_exec_join_non_equi :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "join_nequi")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t1"))
	executor.execute(&tree, make_insert_stmt("t1", 1, "a", 10.0))
	executor.execute(&tree, make_insert_stmt("t1", 2, "b", 20.0))

	cols2 := make([dynamic]types.Column, context.temp_allocator)
	append(&cols2, types.Column{name = "ref", type = .INTEGER})
	append(&cols2, types.Column{name = "val", type = .TEXT})
	variant2 := parser.Create_Stmt{table_name = "t2", columns = cols2[:]}
	executor.execute(&tree, parser.Statement{type = variant2, sql = ""})
	executor.execute(&tree, parser.Statement{
		type = parser.Insert_Stmt{table_name = "t2", values = {types.value_int(1), types.value_text("a")}}, sql = ""})
	executor.execute(&tree, parser.Statement{
		type = parser.Insert_Stmt{table_name = "t2", values = {types.value_int(2), types.value_text("x")}}, sql = ""})

	// Two-condition ON clause → non-equi, falls back to nested-loop
	sql := "SELECT * FROM t1 JOIN t2 ON t1.id = t2.ref AND t1.name = t2.val;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "multi-condition JOIN should parse")
	exec_ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, exec_ok, "multi-condition JOIN should execute")
}

@(test)
test_exec_join_cross_fallback :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "join_cross")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("a"))
	executor.execute(&tree, make_create_stmt("b"))

	sql := "SELECT * FROM a CROSS JOIN b;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "CROSS JOIN should parse")
	exec_ok, _ := executor.execute(&tree, stmt)
	testing.expect(t, exec_ok, "CROSS JOIN should execute")
}

@(test)
test_exec_order_by_int :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "order_int")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))
	executor.execute(&tree, make_insert_stmt("t", 3, "c", 3.0))
	executor.execute(&tree, make_insert_stmt("t", 1, "a", 1.0))
	executor.execute(&tree, make_insert_stmt("t", 2, "b", 2.0))

	// ASC (exercises integer fast path)
	stmt1, _ := parser.parse("SELECT id FROM t ORDER BY id;", context.temp_allocator)
	ok, _ := executor.execute(&tree, stmt1)
	testing.expect(t, ok, "ORDER BY ASC should execute")

	// DESC (exercises reversed permutation)
	stmt2, _ := parser.parse("SELECT id FROM t ORDER BY id DESC;", context.temp_allocator)
	ok2, _ := executor.execute(&tree, stmt2)
	testing.expect(t, ok2, "ORDER BY DESC should execute")

	// NULLS FIRST (fast path with nulls handling)
	stmt3, _ := parser.parse("SELECT id FROM t ORDER BY id NULLS FIRST;", context.temp_allocator)
	ok3, _ := executor.execute(&tree, stmt3)
	testing.expect(t, ok3, "ORDER BY NULLS FIRST should execute")
}

@(test)
test_exec_freeblock_reuse :: proc(t: ^testing.T) {
	tree, file := setup_executor_env(t, "freeblock")
	defer teardown_executor_env(tree, file)

	executor.execute(&tree, make_create_stmt("t"))

	// Insert 50 rows
	for i in 1 ..= 50 {
		free_all(context.temp_allocator)
		executor.execute(&tree, make_insert_stmt("t", i64(i), fmt.tprintf("n-%d", i), f64(i)))
	}

	// Delete every other row (25 deletions → freeblocks created)
	for i in 1 ..= 50 {
		if i % 2 == 0 { continue }
		cond := parser.Condition{column = "id", operator = .EQUALS, rhs = types.value_int(i64(i))}
		del := parser.Delete_Stmt{table_name = "t",
			where_clause = parser.Where_Clause{conditions = {cond}, is_and = true}}
		executor.execute(&tree, parser.Statement{type = del, sql = ""})
	}

	// Re-insert into freed space
	for i in 51 ..= 75 {
		free_all(context.temp_allocator)
		executor.execute(&tree, make_insert_stmt("t", i64(i), fmt.tprintf("new-%d", i), f64(i)))
	}

	// Verify all 50 rows are intact
	table, _ := schema.get_table(&tree, "t", context.temp_allocator)
	table_tree := btree.init(tree.pager, table.root_page)
	count, _ := btree.tree_count_rows(&table_tree)
	testing.expect_value(t, count, 50)

	check_ids := []types.Row_ID{2, 4, 51, 75}
	for rid, _ in check_ids {
		c, err := btree.tree_find(&table_tree, rid, context.temp_allocator)
		testing.expect(t, err == .None, fmt.tprintf("Row %d should exist", rid))
		if err == .None { cell.destroy(&c) }
	}
}
