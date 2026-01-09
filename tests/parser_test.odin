package tests

import "core:fmt"
import "core:testing"
import "src:parser"
import "src:types"

@(test)
test_tokenize_basic :: proc(t: ^testing.T) {
	sql := "SELECT * FROM users WHERE id = 1;"
	tokens, ok := parser.tokenize(sql, context.temp_allocator)
	testing.expect(t, ok, "Tokenization failed")
	testing.expect(t, len(tokens) == 10, fmt.tprintf("Expected 10 tokens, got %d", len(tokens)))

	testing.expect(t, tokens[0].type == .SELECT, "Expected SELECT")
	testing.expect(t, tokens[1].type == .ASTERISK, "Expected *")
	testing.expect(t, tokens[2].type == .FROM, "Expected FROM")
	testing.expect(t, tokens[3].type == .IDENTIFIER, "Expected IDENTIFIER")
	testing.expect(t, tokens[3].lexeme == "users", "Expected 'users'")

	testing.expect(t, tokens[8].type == .SEMICOLON, "Expected SEMICOLON")
	testing.expect(t, tokens[9].type == .EOF, "Expected EOF")
}

@(test)
test_tokenize_literals :: proc(t: ^testing.T) {
	sql := "VALUES ('hello', 123, -45.67)"
	tokens, ok := parser.tokenize(sql, context.temp_allocator)
	testing.expect(t, ok, "Tokenization failed")

	testing.expect(t, tokens[2].type == .STRING, "Expected STRING")
	testing.expect(t, tokens[2].lexeme == "hello", "Expected raw string lexeme (no quotes)")

	testing.expect(t, tokens[4].type == .NUMBER, "Expected NUMBER (int)")
	testing.expect(t, tokens[4].lexeme == "123", "Expected 123")

	testing.expect(t, tokens[6].type == .NUMBER, "Expected NUMBER (float/neg)")
	testing.expect(t, tokens[6].lexeme == "-45.67", "Expected -45.67")
}

@(test)
test_parse_create_table :: proc(t: ^testing.T) {
	sql := "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	create_stmt, is_create := stmt.type.(parser.Create_Stmt)
	testing.expect(t, is_create, "Expected Create_Stmt variant")

	testing.expect(t, create_stmt.table_name == "products", "Wrong table name")
	testing.expect(t, len(create_stmt.columns) == 3, "Wrong column count")

	c1 := create_stmt.columns[0]
	testing.expect(t, c1.name == "id", "Col 1 name mismatch")
	testing.expect(t, c1.type == .INTEGER, "Col 1 type mismatch")
	testing.expect(t, c1.pk == true, "Col 1 should be PK")

	c2 := create_stmt.columns[1]
	testing.expect(t, c2.name == "name", "Col 2 name mismatch")
	testing.expect(t, c2.type == .TEXT, "Col 2 type mismatch")
	testing.expect(t, c2.not_null == true, "Col 2 should be NOT NULL")
	testing.expect(t, c2.pk == false, "Col 2 should not be PK")

	c3 := create_stmt.columns[2]
	testing.expect(t, c3.type == .REAL, "Col 3 type mismatch")
}

@(test)
test_parse_insert :: proc(t: ^testing.T) {
	sql := "INSERT INTO users VALUES (1, 'Alice', NULL, 99.9);"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	insert_stmt, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt variant")

	testing.expect(t, insert_stmt.table_name == "users", "Wrong table")
	testing.expect(t, len(insert_stmt.values) == 4, "Value count mismatch")

	v0 := insert_stmt.values[0].(i64)
	testing.expect(t, v0 == 1, "Val 0 mismatch")

	v1 := insert_stmt.values[1].(string)
	testing.expect(t, v1 == "Alice", "Val 1 mismatch")

	_, is_null := insert_stmt.values[2].(types.Null)
	testing.expect(t, is_null, "Val 2 should be NULL")

	v3 := insert_stmt.values[3].(f64)
	testing.expect(t, v3 == 99.9, "Val 3 mismatch")
}

@(test)
test_parse_select_star :: proc(t: ^testing.T) {
	sql := "SELECT * FROM data;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")

	testing.expect(t, sel.table_name == "data", "Wrong table")
	testing.expect(t, len(sel.columns) == 0, "Star should result in empty column list")
	testing.expect(t, sel.where_clause == nil, "Should have no where clause")
}

@(test)
test_parse_select_specific :: proc(t: ^testing.T) {
	sql := "SELECT id, name, age FROM users;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")

	testing.expect(t, len(sel.columns) == 3, "Column count mismatch")
	testing.expect(t, sel.columns[0] == "id", "Col 0 mismatch")
	testing.expect(t, sel.columns[1] == "name", "Col 1 mismatch")
}

@(test)
test_parse_select_where :: proc(t: ^testing.T) {
	sql := "SELECT * FROM users WHERE age >= 18 AND status = 'active';"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")

	clause, has_clause := sel.where_clause.?
	testing.expect(t, has_clause, "Missing WHERE clause")
	testing.expect(t, clause.is_and == true, "Should be AND logic")
	testing.expect(t, len(clause.conditions) == 2, "Should have 2 conditions")

	c1 := clause.conditions[0]
	testing.expect(t, c1.column == "age", "C1 column mismatch")
	testing.expect(t, c1.operator == .GREATER_EQUAL, "C1 op mismatch")
	testing.expect(t, c1.value.(i64) == 18, "C1 val mismatch")

	c2 := clause.conditions[1]
	testing.expect(t, c2.column == "status", "C2 column mismatch")
	testing.expect(t, c2.operator == .EQUALS, "C2 op mismatch")
	testing.expect(t, c2.value.(string) == "active", "C2 val mismatch")
}

@(test)
test_parse_update :: proc(t: ^testing.T) {
	sql := "UPDATE employees SET salary = 50000, rank = 2 WHERE id = 10;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	upd, is_update := stmt.type.(parser.Update_Stmt)
	testing.expect(t, is_update, "Expected Update_Stmt")

	testing.expect(t, upd.table_name == "employees", "Wrong table")

	testing.expect(t, len(upd.update_columns) == 2, "Update col count mismatch")
	testing.expect(t, upd.update_columns[0] == "salary", "Col 0 mismatch")
	testing.expect(t, upd.update_values[0].(i64) == 50000, "Val 0 mismatch")

	testing.expect(t, upd.update_columns[1] == "rank", "Col 1 mismatch")

	_, has_where := upd.where_clause.?
	testing.expect(t, has_where, "Missing WHERE clause")
}

@(test)
test_parse_delete :: proc(t: ^testing.T) {
	sql := "DELETE FROM logs WHERE date < '2023-01-01';"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	del, is_delete := stmt.type.(parser.Delete_Stmt)
	testing.expect(t, is_delete, "Expected Delete_Stmt")
	testing.expect(t, del.table_name == "logs", "Wrong table")

	clause, _ := del.where_clause.?
	testing.expect(t, len(clause.conditions) == 1, "Cond count mismatch")
	testing.expect(t, clause.conditions[0].operator == .LESS_THAN, "Op mismatch")
}

@(test)
test_parse_drop :: proc(t: ^testing.T) {
	sql := "DROP TABLE old_data;"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	drop, is_drop := stmt.type.(parser.Drop_Stmt)
	testing.expect(t, is_drop, "Expected Drop_Stmt")
	testing.expect(t, drop.table_name == "old_data", "Wrong table")
}

@(test)
test_parse_error_mixed_logic :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a=1 AND b=2 OR c=3;"
	_, ok := parser.parse(sql, context.allocator)
	testing.expect(t, !ok, "Should fail on mixed AND/OR logic")
}

@(test)
test_parse_error_syntax :: proc(t: ^testing.T) {
	sql := "CREATE user (id INT);"
	_, ok := parser.parse(sql, context.allocator)
	testing.expect(t, !ok, "Should fail on bad syntax (missing TABLE keyword)")
}
