package tests

import "core:fmt"
import "core:testing"
import "src:parser"
import "src:types"

@(test)
test_tokenize_basic :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

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
	defer free_all(context.temp_allocator)

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
test_tokenize_operators :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "= <> != < > <= >="
	tokens, ok := parser.tokenize(sql, context.temp_allocator)
	testing.expect(t, ok, "Tokenization failed")

	testing.expect(t, tokens[0].type == .EQUALS, "Expected =")
	testing.expect(t, tokens[1].type == .NOT_EQUALS, "Expected <>")
	testing.expect(t, tokens[2].type == .NOT_EQUALS, "Expected !=")
	testing.expect(t, tokens[3].type == .LESS_THAN, "Expected <")
	testing.expect(t, tokens[4].type == .GREATER_THAN, "Expected >")
	testing.expect(t, tokens[5].type == .LESS_EQUAL, "Expected <=")
	testing.expect(t, tokens[6].type == .GREATER_EQUAL, "Expected >=")
}

@(test)
test_parse_create_table :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .CREATE_TABLE, "Wrong statement type")
	testing.expect(t, stmt.table_name == "products", "Wrong table name")
	testing.expect(t, len(stmt.columns) == 3, "Wrong column count")

	c1 := stmt.columns[0]
	testing.expect(t, c1.name == "id", "Col 1 name mismatch")
	testing.expect(t, c1.type == .INTEGER, "Col 1 type mismatch")
	testing.expect(t, c1.pk == true, "Col 1 should be PK")

	c2 := stmt.columns[1]
	testing.expect(t, c2.name == "name", "Col 2 name mismatch")
	testing.expect(t, c2.type == .TEXT, "Col 2 type mismatch")
	testing.expect(t, c2.not_null == true, "Col 2 should be NOT NULL")
	testing.expect(t, c2.pk == false, "Col 2 should not be PK")

	c3 := stmt.columns[2]
	testing.expect(t, c3.type == .REAL, "Col 3 type mismatch")
}

@(test)
test_parse_insert :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "INSERT INTO users VALUES (1, 'Alice', NULL, 99.9);"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .INSERT, "Wrong type")
	testing.expect(t, stmt.table_name == "users", "Wrong table")
	testing.expect(t, len(stmt.insert_values) == 4, "Value count mismatch")

	v0 := stmt.insert_values[0].(i64)
	testing.expect(t, v0 == 1, "Val 0 mismatch")

	v1 := stmt.insert_values[1].(string)
	testing.expect(t, v1 == "Alice", "Val 1 mismatch")
	testing.expect(t, types.is_null(stmt.insert_values[2]), "Val 2 should be NULL")

	v3 := stmt.insert_values[3].(f64)
	testing.expect(t, v3 == 99.9, "Val 3 mismatch")
}

@(test)
test_parse_select_star :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "SELECT * FROM data;"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .SELECT, "Wrong type")
	testing.expect(t, stmt.from_table == "data", "Wrong table")
	testing.expect(t, len(stmt.select_columns) == 0, "Star should result in empty column list")
	testing.expect(t, stmt.where_clause == nil, "Should have no where clause")
}

@(test)
test_parse_select_specific :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "SELECT id, name, age FROM users;"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, len(stmt.select_columns) == 3, "Column count mismatch")
	testing.expect(t, stmt.select_columns[0] == "id", "Col 0 mismatch")
	testing.expect(t, stmt.select_columns[1] == "name", "Col 1 mismatch")
}

@(test)
test_parse_select_where :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "SELECT * FROM users WHERE age >= 18 AND status = 'active';"
	stmt, ok := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	clause, has_clause := stmt.where_clause.?
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
	defer free_all(context.temp_allocator)

	sql := "UPDATE employees SET salary = 50000, rank = 2 WHERE id = 10;"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .UPDATE, "Wrong type")
	testing.expect(t, stmt.from_table == "employees", "Wrong table")

	testing.expect(t, len(stmt.update_columns) == 2, "Update col count mismatch")
	testing.expect(t, stmt.update_columns[0] == "salary", "Col 0 mismatch")
	testing.expect(t, stmt.update_values[0].(i64) == 50000, "Val 0 mismatch")

	testing.expect(t, stmt.update_columns[1] == "rank", "Col 1 mismatch")

	_, has_where := stmt.where_clause.?
	testing.expect(t, has_where, "Missing WHERE clause")
}

@(test)
test_parse_delete :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "DELETE FROM logs WHERE date < '2023-01-01';"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .DELETE, "Wrong type")
	testing.expect(t, stmt.from_table == "logs", "Wrong table")

	clause, _ := stmt.where_clause.?
	testing.expect(t, len(clause.conditions) == 1, "Cond count mismatch")
	testing.expect(t, clause.conditions[0].operator == .LESS_THAN, "Op mismatch")
}

@(test)
test_parse_drop :: proc(t: ^testing.T) {
	defer free_all(context.temp_allocator)

	sql := "DROP TABLE old_data;"
	stmt, ok := parser.parse(sql, context.temp_allocator)

	testing.expect(t, ok, "Parse failed")
	testing.expect(t, stmt.type == .DROP_TABLE, "Wrong type")
	testing.expect(t, stmt.table_name == "old_data", "Wrong table")
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
	testing.expect(t, !ok, "Should fail on bad syntax")
}
