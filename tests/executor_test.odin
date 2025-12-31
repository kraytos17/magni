package tests

import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"

setup_executor_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	temp_name := fmt.tprintf("test_exec_%s.db", test_name)
	filename, _ := strings.clone(temp_name, context.allocator)

	os.remove(filename)
	p, err := pager.open(filename)
	testing.expect(t, err == nil, "Failed to open pager")

	pager.allocate_page(p) // Page 0
	ok := schema.init(p)
	testing.expect(t, ok, "Failed to init schema")

	return p, filename
}

teardown_executor_env :: proc(p: ^pager.Pager, filename: string) {
	pager.close(p)
	os.remove(filename)
	delete(filename, context.allocator)
}

make_create_stmt :: proc(name: string) -> parser.Statement {
	context.allocator = context.temp_allocator
	cols := make([dynamic]types.Column)
	append(&cols, types.Column{name = "id", type = .INTEGER, pk = true, not_null = true})
	append(&cols, types.Column{name = "name", type = .TEXT})
	append(&cols, types.Column{name = "score", type = .REAL})

	return parser.Statement {
		type = .CREATE_TABLE,
		table_name = name,
		columns = cols[:],
		original_sql = "CREATE TABLE ...",
	}
}

make_insert_stmt :: proc(table: string, id: i64, name: string, score: f64) -> parser.Statement {
	context.allocator = context.temp_allocator
	vals := make([dynamic]types.Value)
	append(&vals, types.value_int(id))
	append(&vals, types.value_text(name))
	append(&vals, types.value_real(score))

	return parser.Statement{type = .INSERT, table_name = table, insert_values = vals[:]}
}

@(test)
test_exec_create_table :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "create")
	defer teardown_executor_env(p, file)

	stmt := make_create_stmt("users")
	success := executor.execute_statement(p, stmt)

	testing.expect(t, success, "CREATE TABLE should succeed")
	testing.expect(t, schema.table_exists(p, "users"), "Table should exist")

	success_dup := executor.execute_statement(p, stmt)
	testing.expect(t, !success_dup, "Duplicate CREATE TABLE should fail")
}

@(test)
test_exec_insert :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "insert")
	defer teardown_executor_env(p, file)

	create_stmt := make_create_stmt("players")
	executor.execute_statement(p, create_stmt)

	insert_stmt := make_insert_stmt("players", 100, "Alice", 99.5)
	success := executor.execute_statement(p, insert_stmt)
	testing.expect(t, success, "INSERT should succeed")

	table, _ := schema.get_table(p, "players", context.temp_allocator)
	count, _ := btree.count_rows(p, table.root_page)
	testing.expect(t, count == 1, "Row count should be 1")

	ref, err := btree.find_by_rowid(p, table.root_page, 100)
	defer btree.cell_ref_destroy(&ref)

	testing.expect(t, err == .None, "Should find inserted row")
	testing.expect(t, ref.cell.values[1].(string) == "Alice", "Data mismatch")
}

@(test)
test_exec_insert_validation_failure :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "insert_fail")
	defer teardown_executor_env(p, file)

	executor.execute_statement(p, make_create_stmt("strict_table"))

	vals := make([dynamic]types.Value, context.temp_allocator)
	append(&vals, types.value_int(1))
	append(&vals, types.value_text("A"))
	append(&vals, types.value_real(1.0))
	append(&vals, types.value_int(999))

	stmt := parser.Statement {
		type          = .INSERT,
		table_name    = "strict_table",
		insert_values = vals[:],
	}

	success := executor.execute_statement(p, stmt)
	testing.expect(t, !success, "INSERT with wrong column count should fail")
}

@(test)
test_exec_update :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "update")
	defer teardown_executor_env(p, file)

	executor.execute_statement(p, make_create_stmt("inventory"))
	executor.execute_statement(p, make_insert_stmt("inventory", 1, "Apple", 1.50))
	executor.execute_statement(p, make_insert_stmt("inventory", 2, "Banana", 0.80))

	context.allocator = context.temp_allocator
	cond := parser.Condition {
		column   = "id",
		operator = .EQUALS,
		value    = types.value_int(1),
	}
	where_clause := parser.Where_Clause {
		conditions = []parser.Condition{cond},
		is_and     = true,
	}

	stmt := parser.Statement {
		type           = .UPDATE,
		from_table     = "inventory",
		update_columns = []string{"score"},
		update_values  = []types.Value{types.value_real(2.00)},
		where_clause   = where_clause,
	}

	context.allocator = context.allocator
	success := executor.execute_statement(p, stmt)
	testing.expect(t, success, "UPDATE should succeed")

	table, _ := schema.get_table(p, "inventory", context.temp_allocator)
	ref, _ := btree.find_by_rowid(p, table.root_page, 1)
	defer btree.cell_ref_destroy(&ref)

	new_price := ref.cell.values[2].(f64)
	testing.expect(t, new_price == 2.00, "Value should be updated")
}

@(test)
test_exec_delete :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "delete")
	defer teardown_executor_env(p, file)

	executor.execute_statement(p, make_create_stmt("logs"))
	executor.execute_statement(p, make_insert_stmt("logs", 1, "Log A", 0))
	executor.execute_statement(p, make_insert_stmt("logs", 2, "Log B", 0))

	context.allocator = context.temp_allocator
	cond := parser.Condition {
		column   = "name",
		operator = .EQUALS,
		value    = types.value_text("Log A"),
	}
	stmt := parser.Statement {
		type = .DELETE,
		from_table = "logs",
		where_clause = parser.Where_Clause{conditions = []parser.Condition{cond}, is_and = true},
	}

	context.allocator = context.allocator
	success := executor.execute_statement(p, stmt)
	testing.expect(t, success, "DELETE should succeed")

	table, _ := schema.get_table(p, "logs", context.temp_allocator)
	count, _ := btree.count_rows(p, table.root_page)
	testing.expect(t, count == 1, "Should have 1 row left")
}

@(test)
test_evaluate_where_clause :: proc(t: ^testing.T) {
	cols := []types.Column {
		{name = "id", type = .INTEGER},
		{name = "name", type = .TEXT},
		{name = "age", type = .INTEGER},
	}

	row := []types.Value{types.value_int(1), types.value_text("Bob"), types.value_int(30)}
	context.allocator = context.temp_allocator
	c1 := parser.Condition {
		column   = "age",
		operator = .GREATER_THAN,
		value    = types.value_int(20),
	}
	w1 := parser.Where_Clause {
		conditions = []parser.Condition{c1},
		is_and     = true,
	}
	testing.expect(t, executor.evaluate_where_clause(w1, row, cols), "Age > 20 should pass")
}

@(test)
test_compare_values :: proc(t: ^testing.T) {
	testing.expect(t, executor.compare_values(types.value_int(10), types.value_int(20)) == -1, "10 < 20")
	testing.expect(t, executor.compare_values(types.value_text("a"), types.value_text("b")) == -1, "a < b")
}

@(test)
test_page_splitting :: proc(t: ^testing.T) {
	p, file := setup_executor_env(t, "split_test")
	defer teardown_executor_env(p, file)

	create_stmt := make_create_stmt("stress_test")
	if !executor.execute_statement(p, create_stmt) {

	}

	fmt.println("--- Starting Stress Insert (70 rows) ---")
	for i in 1 ..= 70 {
		free_all(context.temp_allocator)
		name := fmt.tprintf("Row Number %d with padding to force split.............................", i)
		stmt := make_insert_stmt("stress_test", i64(i), name, 10.5)
		success := executor.execute_statement(p, stmt)
		if !success {
			fmt.printf(" [FAIL] Insert failed at row ID %d\n", i)
		}
	}

	fmt.println("--- Finished Stress Insert ---")
	table, _ := schema.get_table(p, "stress_test", context.temp_allocator)

	fmt.println("\n--- VERIFYING BTREE STRUCTURE ---")
	ok := btree.verify_page(p, table.root_page, 1, 1_000_000)
	testing.expect(t, ok, "B-tree invariant violation")

	ref, err := btree.find_by_rowid(p, table.root_page, 60)
	defer btree.cell_ref_destroy(&ref)

	testing.expect_value(t, err, btree.Error.None)
	if err == .None {
		testing.expect(t, ref.cell.rowid == 60, "Row ID mismatch for Row 60")
	}

	root_page, pg_err := pager.get_page(p, table.root_page)
	testing.expect(t, pg_err == nil, "Could not load root page for verification")
	if pg_err == nil {
		header := btree.get_header(root_page.data, table.root_page)
		is_interior := header.page_type == .INTERIOR_TABLE
		testing.expect(t, is_interior, "Root page did not split! It is still a Leaf Node.")
		if is_interior {
			fmt.printf(
				" [PASS] Root Page %d is now Interior (Cells: %d)\n",
				table.root_page,
				header.cell_count,
			)
		}
	}
}
