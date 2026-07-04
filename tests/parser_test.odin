package tests

import "core:fmt"
import "core:strings"
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")

	tbl_name, _ := sel.from.(string)
	testing.expect(t, tbl_name == "data", "Wrong table")
	testing.expect(t, len(sel.columns) == 0, "Star should result in empty column list")
	testing.expect(t, sel.where_clause == nil, "Should have no where clause")
}

@(test)
test_parse_select_specific :: proc(t: ^testing.T) {
	sql := "SELECT id, name, age FROM users;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	c1_val, c1_val_ok := c1.rhs.(types.Value)
	testing.expect(t, c1_val_ok && c1_val.(i64) == 18, "C1 val mismatch")

	c2 := clause.conditions[1]
	testing.expect(t, c2.column == "status", "C2 column mismatch")
	testing.expect(t, c2.operator == .EQUALS, "C2 op mismatch")
	c2_val, c2_val_ok := c2.rhs.(types.Value)
	testing.expect(t, c2_val_ok && c2_val.(string) == "active", "C2 val mismatch")
}

@(test)
test_parse_update :: proc(t: ^testing.T) {
	sql := "UPDATE employees SET salary = 50000, rank = 2 WHERE id = 10;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
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
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed")

	drop, is_drop := stmt.type.(parser.Drop_Stmt)
	testing.expect(t, is_drop, "Expected Drop_Stmt")
	testing.expect(t, drop.table_name == "old_data", "Wrong table")
}

@(test)
test_parse_error_mixed_logic :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a=1 AND b=2 OR c=3;"
	_, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, !ok, "Should fail on mixed AND/OR logic")
}

@(test)
test_parse_error_syntax :: proc(t: ^testing.T) {
	sql := "CREATE user (id INT);"
	_, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, !ok, "Should fail on bad syntax (missing TABLE keyword)")
}

@(test)
test_parse_error_messages :: proc(t: ^testing.T) {
	tests := []struct {
		sql:      string,
		contains: string,
	} {
		{"CREATE TABLE test;", "column definition"},
		{"INSERT INTO test;", "VALUES"},
		{"INSERT test VALUES (1);", "INTO"},
		{"DELETE test;", "FROM"},
	}
	for test in tests {
		_, ok, err_msg := parser.parse(test.sql, context.temp_allocator)
		testing.expect(t, !ok, fmt.tprintf("'%s' should fail to parse", test.sql))
		if test.contains != "" {
			testing.expect(
				t,
				strings.contains(err_msg, test.contains),
				fmt.tprintf("error '%s' should contain '%s'", err_msg, test.contains),
			)
		}

		@(test)
		test_line_comment :: proc(t: ^testing.T) {
			sql := "SELECT * FROM t -- inline comment"
			stmt, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "-- comment should parse")
			sel, is_sel := stmt.type.(parser.Select_Stmt)
			testing.expect(t, is_sel, "expected Select_Stmt")
			testing.expect(t, len(sel.columns) == 1 && sel.columns[0] == "*", "expected star")
		}

		@(test)
		test_line_comment_before_semicolon :: proc(t: ^testing.T) {
			sql := "SELECT * FROM t -- comment before semicolon\n;"
			_, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "-- comment before semicolon")
		}

		@(test)
		test_hex_integer_literal :: proc(t: ^testing.T) {
			sql := "SELECT * FROM t WHERE id = 0xFF"
			_, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "hex literal")
		}

		@(test)
		test_scientific_float_literal :: proc(t: ^testing.T) {
			sql := "SELECT * FROM t WHERE score = 1.5e3"
			_, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "scientific float")
		}

		@(test)
		test_unicode_string_literal :: proc(t: ^testing.T) {
			sql := "SELECT 'héllo 世界' FROM t"
			_, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "unicode string")
		}
	}
}

@(test)
test_parse_or_clause :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a = 1 OR b = 2;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "OR clause parse failed")
	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")
	clause, has := sel.where_clause.?
	testing.expect(t, has, "WHERE clause missing")
	testing.expect(t, !clause.is_and, "Should be OR logic")
	testing.expect_value(t, len(clause.conditions), 2)
}

@(test)
test_parse_not_equals :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE x != 5;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed for !=")
	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")
	clause, _ := sel.where_clause.?
	testing.expect_value(t, clause.conditions[0].operator, parser.Token_Type.NOT_EQUALS)
}

@(test)
test_parse_empty_sql :: proc(t: ^testing.T) {
	_, ok, _ := parser.parse("", context.temp_allocator)
	testing.expect(t, !ok, "Empty SQL should fail")
	_, ok2, _ := parser.parse("   ;", context.temp_allocator)
	testing.expect(t, !ok2, "Whitespace-only SQL should fail")
}

@(test)
test_parse_update_no_where :: proc(t: ^testing.T) {
	sql := "UPDATE t SET x = 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "UPDATE without WHERE should parse")
	_, is_update := stmt.type.(parser.Update_Stmt)
	testing.expect(t, is_update, "Expected Update_Stmt")
}

@(test)
test_parse_delete_no_where :: proc(t: ^testing.T) {
	sql := "DELETE FROM t;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "DELETE without WHERE should parse")
	_, is_delete := stmt.type.(parser.Delete_Stmt)
	testing.expect(t, is_delete, "Expected Delete_Stmt")
}

@(test)
test_parse_blob_literal :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, X'DEADBEEF');"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "INSERT with BLOB literal should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	testing.expect_value(t, len(ins.values), 2)
	blob_val, is_blob := ins.values[1].([]u8)
	testing.expect(t, is_blob, "Second value should be a BLOB")
	testing.expect_value(t, blob_val[0], u8(0xDE))
	testing.expect_value(t, blob_val[1], u8(0xAD))
	testing.expect_value(t, blob_val[2], u8(0xBE))
	testing.expect_value(t, blob_val[3], u8(0xEF))
}

@(test)
test_parse_blob_literal_lowercase :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, x'deadbeef');"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Lowercase x'...' should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	blob_val, is_blob := ins.values[1].([]u8)
	testing.expect(t, is_blob, "Second value should be a BLOB")
	testing.expect_value(t, len(blob_val), 4)
}

@(test)
test_parse_blob_literal_empty :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, X'');"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Empty BLOB literal should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	blob_val, is_blob := ins.values[1].([]u8)
	testing.expect(t, is_blob, "Second value should be a BLOB")
	testing.expect_value(t, len(blob_val), 0)
}

@(test)
test_parse_blob_literal_invalid_hex :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, X'ZZZZ');"
	_, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, !ok, "Invalid hex in BLOB literal should fail")
}

@(test)
test_parse_insert_column_list :: proc(t: ^testing.T) {
	sql := "INSERT INTO t (id, name) VALUES (1, 'Alice');"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "INSERT with column list should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	testing.expect_value(t, ins.table_name, "t")
	testing.expect_value(t, len(ins.columns), 2)
	testing.expect_value(t, ins.columns[0], "id")
	testing.expect_value(t, ins.columns[1], "name")
}

@(test)
test_parse_insert_column_list_order :: proc(t: ^testing.T) {
	sql := "INSERT INTO t (score, name, id) VALUES (99.5, 'Bob', 42);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "INSERT with reordered columns should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	testing.expect_value(t, len(ins.columns), 3)
	testing.expect_value(t, ins.columns[0], "score")
	testing.expect_value(t, ins.columns[1], "name")
	testing.expect_value(t, ins.columns[2], "id")
}

@(test)
test_parse_insert_no_column_list :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, 'Alice', 99.5);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "INSERT without column list should still parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	testing.expect_value(t, len(ins.columns), 0)
}

@(test)
test_parse_default_value :: proc(t: ^testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'anon', score REAL DEFAULT 0.0);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "CREATE TABLE with DEFAULT should parse")
	create, is_create := stmt.type.(parser.Create_Stmt)
	testing.expect(t, is_create, "Expected Create_Stmt")
	testing.expect_value(t, len(create.columns), 3)

	def_name, has_name := create.columns[1].default_value.?
	testing.expect(t, has_name, "name should have default")
	if name_str, name_ok := def_name.(string); name_ok {
		testing.expect_value(t, name_str, "anon")
	}

	def_score, has_score := create.columns[2].default_value.?
	testing.expect(t, has_score, "score should have default")
	if score_val, score_ok := def_score.(f64); score_ok {
		testing.expect_value(t, score_val, 0.0)
	}
}

@(test)
test_parse_default_value_null :: proc(t: ^testing.T) {
	sql := "CREATE TABLE t (x INTEGER DEFAULT NULL);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "DEFAULT NULL should parse")
	create, is_create := stmt.type.(parser.Create_Stmt)
	testing.expect(t, is_create, "Expected Create_Stmt")
	def, has := create.columns[0].default_value.?
	testing.expect(t, has, "should have default")
	testing.expect(t, types.is_null(def), "default should be NULL")
}

@(test)
test_parse_like_where :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE name LIKE '%foo%';"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT with LIKE should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	wc, has_where := sel.where_clause.?
	testing.expect(t, has_where, "should have WHERE")
	testing.expect_value(t, len(wc.conditions), 1)
	testing.expect_value(t, wc.conditions[0].operator, parser.Token_Type.LIKE)
	pattern_val, val_ok := wc.conditions[0].rhs.(types.Value)
	testing.expect(t, val_ok, "LIKE value should be Value")
	p_str, is_str := pattern_val.(string)
	testing.expect(t, is_str, "LIKE value should be string")
	testing.expect_value(t, p_str, "%foo%")
}

@(test)
test_parse_limit_offset :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t LIMIT 5 OFFSET 10;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT with LIMIT/OFFSET should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	lim, has_lim := sel.limit.?
	testing.expect(t, has_lim, "should have LIMIT")
	testing.expect_value(t, lim, u64(5))
	off, has_off := sel.offset.?
	testing.expect(t, has_off, "should have OFFSET")
	testing.expect_value(t, off, u64(10))
}

@(test)
test_parse_limit_only :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t LIMIT 3;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT with LIMIT only should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	lim, has_lim := sel.limit.?
	testing.expect(t, has_lim, "should have LIMIT")
	testing.expect_value(t, lim, u64(3))
	_, has_off := sel.offset.?
	testing.expect(t, !has_off, "should not have OFFSET")
}

@(test)
test_parse_order_by :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t ORDER BY name;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT with ORDER BY should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	ob, has_ob := sel.order_by.?
	testing.expect(t, has_ob, "should have ORDER BY")
	testing.expect_value(t, len(ob), 1)
	testing.expect_value(t, ob[0].column, "name")
	testing.expect(t, !ob[0].desc, "should default to ASC")
}

@(test)
test_parse_order_by_desc :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t ORDER BY score DESC;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "ORDER BY DESC should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	ob, has_ob := sel.order_by.?
	testing.expect(t, has_ob, "should have ORDER BY")
	testing.expect_value(t, len(ob), 1)
	testing.expect(t, ob[0].desc, "should be DESC")
}

@(test)
test_parse_order_by_multi :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t ORDER BY score DESC, name ASC;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Multi-column ORDER BY should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	ob, has_ob := sel.order_by.?
	testing.expect(t, has_ob, "should have ORDER BY")
	testing.expect_value(t, len(ob), 2)
	testing.expect_value(t, ob[0].column, "score")
	testing.expect(t, ob[0].desc, "score should be DESC")
	testing.expect_value(t, ob[1].column, "name")
	testing.expect(t, !ob[1].desc, "name should be ASC")
}

@(test)
test_parse_aggregate_count_star :: proc(t: ^testing.T) {
	sql := "SELECT COUNT(*) FROM t;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT COUNT(*) should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.aggregates), 1)
	testing.expect_value(t, sel.aggregates[0].func, parser.Aggregate_Func.COUNT)
	testing.expect_value(t, sel.aggregates[0].column, "")
	testing.expect_value(t, len(sel.columns), 1)
}

@(test)
test_parse_aggregate_sum :: proc(t: ^testing.T) {
	sql := "SELECT SUM(score) FROM t;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT SUM(col) should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.aggregates), 1)
	testing.expect_value(t, sel.aggregates[0].func, parser.Aggregate_Func.SUM)
	testing.expect_value(t, sel.aggregates[0].column, "score")
}

@(test)
test_parse_select_group_by :: proc(t: ^testing.T) {
	sql := "SELECT name, COUNT(*) FROM t GROUP BY name;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "SELECT with GROUP BY should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.group_by), 1)
	testing.expect_value(t, sel.group_by[0], "name")
	testing.expect_value(t, len(sel.aggregates), 1)
}

@(test)
test_parse_cross_join :: proc(t: ^testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "CROSS JOIN should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 1)
	testing.expect(t, sel.joins[0].join_type == .CROSS, "Expected CROSS join")
	tbl_name, _ := sel.joins[0].source.(string)
	testing.expect_value(t, tbl_name, "b")
	_, has_on_bool := sel.joins[0].on_clause.?
	testing.expect(t, !has_on_bool, "CROSS JOIN should have no ON clause")
}

@(test)
test_parse_inner_join :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t1 INNER JOIN t2 ON t1.id = 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "INNER JOIN should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 1)
	testing.expect(t, sel.joins[0].join_type == .INNER, "Expected INNER join")
	tbl_name, _ := sel.joins[0].source.(string)
	testing.expect_value(t, tbl_name, "t2")
	on_cl, has_on := sel.joins[0].on_clause.?
	testing.expect(t, has_on, "INNER JOIN should have ON clause")
	testing.expect_value(t, len(on_cl.conditions), 1)
	testing.expect_value(t, on_cl.conditions[0].column, "t1.id")
	testing.expect(t, on_cl.conditions[0].operator == .EQUALS, "Expected = operator")
}

@(test)
test_parse_multiple_joins :: proc(t: ^testing.T) {
	sql := "SELECT * FROM a INNER JOIN b ON a.x=1 JOIN c ON b.y=2;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Multiple JOINs should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 2)
	tbl0, _ := sel.joins[0].source.(string)
	testing.expect_value(t, tbl0, "b")
	tbl1, _ := sel.joins[1].source.(string)
	testing.expect_value(t, tbl1, "c")
}

@(test)
test_parse_equi_join :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Equi-join should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 1)
	on_cl, has_on := sel.joins[0].on_clause.?
	testing.expect(t, has_on, "Equi-join should have ON clause")
	testing.expect_value(t, len(on_cl.conditions), 1)
	cond := on_cl.conditions[0]
	testing.expect_value(t, cond.column, "t1.x")
	testing.expect(t, cond.operator == .EQUALS, "Expected = operator")
	rc, has_rc := cond.rhs.(string)
	testing.expect(t, has_rc, "Equi-join should have string rhs")
	testing.expect_value(t, rc, "t2.y")
}

@(test)
test_parse_left_join :: proc(t: ^testing.T) {
	sql := "SELECT * FROM a LEFT JOIN b ON a.x = b.y;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "LEFT JOIN should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 1)
	testing.expect(t, sel.joins[0].join_type == .LEFT, "Expected LEFT join")
	tbl_name, _ := sel.joins[0].source.(string)
	testing.expect_value(t, tbl_name, "b")
	_, has_on := sel.joins[0].on_clause.?
	testing.expect(t, has_on, "LEFT JOIN should have ON clause")
}

@(test)
test_parse_left_outer_join :: proc(t: ^testing.T) {
	sql := "SELECT * FROM a LEFT OUTER JOIN b ON a.x = b.y;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "LEFT OUTER JOIN should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.joins), 1)
	testing.expect(t, sel.joins[0].join_type == .LEFT, "Expected LEFT join")
}

@(test)
test_parse_table_alias_as :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t AS a;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Table alias with AS should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, sel.from_alias, "a")
}

@(test)
test_parse_table_alias_implicit :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t a;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Implicit table alias should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, sel.from_alias, "a")
}

@(test)
test_parse_join_alias :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t1 AS a JOIN t2 AS b ON a.x = b.y;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "JOIN with aliases should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, sel.from_alias, "a")
	testing.expect_value(t, sel.joins[0].alias, "b")
}

@(test)
test_parse_subquery :: proc(t: ^testing.T) {
	sql := "SELECT * FROM (SELECT a, b FROM t) AS sub;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Subquery should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	inner_ptr, has_sub := sel.from.(^parser.Select_Stmt)
	testing.expect(t, has_sub, "Expected subquery")
	testing.expect_value(t, sel.from_alias, "sub")
	inner_tbl, _ := inner_ptr.from.(string)
	testing.expect_value(t, inner_tbl, "t")
	testing.expect_value(t, len(inner_ptr.columns), 2)
	testing.expect_value(t, inner_ptr.columns[0], "a")
	testing.expect_value(t, inner_ptr.columns[1], "b")
}

@(test)
test_parse_subquery_no_as :: proc(t: ^testing.T) {
	sql := "SELECT * FROM (SELECT x FROM t) sub;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Subquery without AS should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	_, has_sub := sel.from.(^parser.Select_Stmt)
	testing.expect(t, has_sub, "Expected subquery")
	testing.expect_value(t, sel.from_alias, "sub")
}

@(test)
test_parse_int_alias :: proc(t: ^testing.T) {
	sql := "CREATE TABLE t (x INT);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "CREATE TABLE with INT alias should parse")
	create, is_create := stmt.type.(parser.Create_Stmt)
	testing.expect(t, is_create, "Expected Create_Stmt")
	testing.expect(t, len(create.columns) == 1, "Expected 1 column")
	testing.expect_value(t, create.columns[0].type, types.Column_Type.INTEGER)
}

@(test)
test_parse_as_of_snapshot :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t AS OF SNAPSHOT 5;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "AS OF SNAPSHOT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	snap_id, has_snap := sel.as_of_snapshot.?
	testing.expect(t, has_snap, "Expected as_of_snapshot to be set")
	testing.expect_value(t, snap_id, u64(5))
}

@(test)
test_parse_as_of_snapshot_no_as :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t OF SNAPSHOT 3;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parses as SELECT without AS OF (unconsumed tokens)")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	_, has_snap := sel.as_of_snapshot.?
	testing.expect(t, !has_snap, "as_of_snapshot should NOT be set without AS")
}

@(test)
test_parse_as_of_snapshot_with_where :: proc(t: ^testing.T) {
	sql := "SELECT id, name FROM t AS OF SNAPSHOT 2 WHERE id > 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "AS OF SNAPSHOT with WHERE should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	snap_id, has_snap := sel.as_of_snapshot.?
	testing.expect(t, has_snap, "Expected as_of_snapshot to be set")
	testing.expect_value(t, snap_id, u64(2))
	_, has_where := sel.where_clause.?
	testing.expect(t, has_where, "Expected WHERE clause")
}

@(test)
test_parse_as_of_snapshot_with_limit :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t AS OF SNAPSHOT 1 LIMIT 10;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "AS OF SNAPSHOT with LIMIT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	snap_id, has_snap := sel.as_of_snapshot.?
	testing.expect(t, has_snap, "Expected as_of_snapshot")
	testing.expect_value(t, snap_id, u64(1))
	lim, has_lim := sel.limit.?
	testing.expect(t, has_lim, "Expected LIMIT")
	testing.expect_value(t, lim, u64(10))
}

@(test)
test_parse_as_of_snapshot_snapshot_as_table_name :: proc(t: ^testing.T) {
	sql := "SELECT snapshot FROM t;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "snapshot as column name should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect(t, len(sel.columns) == 1, "Expected 1 column")
	testing.expect_value(t, sel.columns[0], "snapshot")
}
