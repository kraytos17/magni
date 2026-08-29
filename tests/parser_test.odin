package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:parser"
import "src:types"

// where_leaf_conditions flattens a top-level AND chain of comparisons into leaf
// conditions (for asserting on parsed WHERE/HAVING/ON trees in tests).
where_leaf_conditions :: proc(clause: parser.Where_Clause) -> []parser.Condition {
	if clause.root == nil { return nil }
	if clause.root.kind == .COND {
		conds := make([dynamic]parser.Condition, context.temp_allocator)
		append(&conds, clause.root.cond)
		return conds[:]
	}
	if clause.root.kind == .AND {
		conds := make([dynamic]parser.Condition, context.temp_allocator)
		for child in clause.root.children {
			if child.kind == .COND {
				append(&conds, child.cond)
			}
		}
		return conds[:]
	}
	return nil
}

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
	testing.expect(t, len(insert_stmt.values) == 1, "Row count mismatch")
	testing.expect(t, len(insert_stmt.values[0]) == 4, "Value count mismatch")

	v0 := insert_stmt.values[0][0].(i64)
	testing.expect(t, v0 == 1, "Val 0 mismatch")

	v1 := insert_stmt.values[0][1].(string)
	testing.expect(t, v1 == "Alice", "Val 1 mismatch")

	_, is_null := insert_stmt.values[0][2].(types.Null)
	testing.expect(t, is_null, "Val 2 should be NULL")

	v3 := insert_stmt.values[0][3].(f64)
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
	testing.expect(t, clause.root.kind == .AND, "Should be AND logic")
	conds := where_leaf_conditions(clause)
	testing.expect(t, len(conds) == 2, "Should have 2 conditions")

	c1 := conds[0]
	testing.expect(t, c1.column == "age", "C1 column mismatch")
	testing.expect(t, c1.operator == .GREATER_EQUAL, "C1 op mismatch")
	c1_val, c1_val_ok := c1.rhs.(types.Value)
	testing.expect(t, c1_val_ok && c1_val.(i64) == 18, "C1 val mismatch")

	c2 := conds[1]
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
	testing.expect(t, clause.root.kind == .COND, "Cond count mismatch")
	testing.expect(t, clause.root.cond.operator == .LESS_THAN, "Op mismatch")
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
test_parse_mixed_logic_precedence :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a=1 AND b=2 OR c=3;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Mixed AND/OR should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, has := sel.where_clause.?
	testing.expect(t, has, "WHERE clause missing")
	// AND binds tighter than OR: (a=1 AND b=2) OR c=3
	testing.expect(t, clause.root.kind == .OR, "Root should be OR (AND precedence)")
	testing.expect_value(t, len(clause.root.children), 2)
	left := clause.root.children[0]
	testing.expect(t, left.kind == .AND, "Left should be AND")
	testing.expect_value(t, len(left.children), 2)
	right := clause.root.children[1]
	testing.expect(t, right.kind == .COND, "Right should be a condition")
	testing.expect_value(t, right.cond.column, "c")
}

@(test)
test_parse_parenthesized_where :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE (a=1 OR a=2) AND b=3;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parenthesized WHERE should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, has := sel.where_clause.?
	testing.expect(t, has, "WHERE clause missing")
	testing.expect(t, clause.root.kind == .AND, "Root should be AND")
	testing.expect_value(t, len(clause.root.children), 2)
	left := clause.root.children[0]
	testing.expect(t, left.kind == .OR, "Parenthesized group should be OR")
	testing.expect_value(t, len(left.children), 2)
}

@(test)
test_parse_nested_parenthesized_where :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE (a=1 AND (b=2 OR b=3)) OR c=4;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Nested parenthesized WHERE should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, has := sel.where_clause.?
	testing.expect(t, has, "WHERE clause missing")
	testing.expect(t, clause.root.kind == .OR, "Root should be OR")
	left := clause.root.children[0]
	testing.expect(t, left.kind == .AND, "Left should be AND")
	testing.expect(t, left.children[0].kind == .COND, "a=1 should be a leaf")
	grp := left.children[1]
	testing.expect(t, grp.kind == .OR, "Inner parens should be OR")
	testing.expect_value(t, len(grp.children), 2)
	right := clause.root.children[1]
	testing.expect(t, right.kind == .COND, "Right should be a condition")
	testing.expect_value(t, right.cond.column, "c")
}

@(test)
test_parse_error_dangling_and :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a=1 AND;"
	_, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, !ok, "Dangling AND should fail to parse")
}

@(test)
test_parse_error_unclosed_paren :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE (a=1 OR b=2;"
	_, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, !ok, "Unclosed parenthesis should fail to parse")
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
			testing.expect(t, len(sel.columns) == 0, "SELECT * yields empty columns slice")
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
			sql := "SELECT 'héllo 世界'"
			stmt, ok, _ := parser.parse(sql, context.temp_allocator)
			testing.expect(t, ok, "string literals in SELECT list should parse")
			if ok {
				sel, is_select := stmt.type.(parser.Select_Stmt)
				testing.expect(t, is_select, "Expected Select_Stmt")
				testing.expect_value(t, len(sel.literal_values), 1)
			}
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
	testing.expect(t, clause.root.kind == .OR, "Should be OR logic")
	testing.expect_value(t, len(clause.root.children), 2)
}

@(test)
test_parse_not_equals :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE x != 5;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Parse failed for !=")
	sel, is_select := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_select, "Expected Select_Stmt")
	clause, _ := sel.where_clause.?
	testing.expect_value(t, clause.root.cond.operator, parser.Token_Type.NOT_EQUALS)
}

@(test)
test_parse_column_alias_aggregate :: proc(t: ^testing.T) {
	sql := "SELECT g, COUNT(*) AS n, SUM(v) AS sm FROM s GROUP BY g;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Aliased aggregate SELECT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	_, is_nf := sel.from.(parser.No_From)
	testing.expect(t, !is_nf, "FROM should be parsed (not a FROM-less literal select)")
	testing.expect_value(t, len(sel.aliases), 3)
	testing.expect_value(t, sel.aliases[0], "")
	testing.expect_value(t, sel.aliases[1], "n")
	testing.expect_value(t, sel.aliases[2], "sm")
	testing.expect(t, len(sel.group_by) == 1, "GROUP BY should be parsed")
	testing.expect(t, len(sel.aggregates) == 2, "aggregates should be parsed")
}

@(test)
test_parse_column_alias_plain :: proc(t: ^testing.T) {
	sql := "SELECT a AS x, b FROM t WHERE a = 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Plain column alias should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.aliases), 2)
	testing.expect_value(t, sel.aliases[0], "x")
	testing.expect_value(t, sel.aliases[1], "")
	_, is_nf := sel.from.(parser.No_From)
	testing.expect(t, !is_nf, "FROM should be parsed")
	testing.expect(t, sel.where_clause != nil, "WHERE should be present after aliased columns")
}

@(test)
test_parse_literal_alias :: proc(t: ^testing.T) {
	sql := "SELECT 1 AS x;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Literal alias should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.aliases), 1)
	testing.expect_value(t, sel.aliases[0], "x")
}

@(test)
test_parse_not_prefix :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE NOT a = 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "NOT prefix should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, has := sel.where_clause.?
	testing.expect(t, has, "WHERE clause missing")
	testing.expect(t, clause.root.kind == .NOT, "Root should be NOT node")
	testing.expect(t, len(clause.root.children) == 1, "NOT has one child")
	testing.expect(t, clause.root.children[0].kind == .COND, "NOT child is a condition")
}

@(test)
test_parse_not_in :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE a NOT IN (1, 3);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "NOT IN should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, _ := sel.where_clause.?
	testing.expect(t, clause.root.kind == .COND, "Root should be a condition")
	testing.expect(t, clause.root.cond.negated, "Condition should be negated")
	testing.expect(t, clause.root.cond.operator == .IN, "Operator should be IN")
}

@(test)
test_parse_not_like :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE name NOT LIKE 'x%';"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "NOT LIKE should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, _ := sel.where_clause.?
	testing.expect(t, clause.root.kind == .COND, "Root should be a condition")
	testing.expect(t, clause.root.cond.negated, "Condition should be negated")
	testing.expect(t, clause.root.cond.operator == .LIKE, "Operator should be LIKE")
}

@(test)
test_parse_not_paren_group :: proc(t: ^testing.T) {
	sql := "SELECT * FROM t WHERE NOT (a = 1 OR b = 2);"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "NOT (paren group) should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	clause, _ := sel.where_clause.?
	testing.expect(t, clause.root.kind == .NOT, "Root should be NOT node")
	testing.expect(t, clause.root.children[0].kind == .OR, "NOT wraps an OR group")
}

@(test)
test_parse_bare_identifier_alias :: proc(t: ^testing.T) {
	sql := "SELECT a b, c FROM t;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Bare identifier alias should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.aliases), 2)
	testing.expect_value(t, sel.aliases[0], "b")
	testing.expect_value(t, sel.aliases[1], "")
	_, is_nf := sel.from.(parser.No_From)
	testing.expect(t, !is_nf, "FROM should be parsed")
}

@(test)
test_parse_multi_row_insert :: proc(t: ^testing.T) {
	sql := "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "Multi-row INSERT should parse")
	ins, is_insert := stmt.type.(parser.Insert_Stmt)
	testing.expect(t, is_insert, "Expected Insert_Stmt")
	testing.expect_value(t, len(ins.values), 3)
	testing.expect(t, len(ins.values[0]) == 2, "Row 0 has 2 values")
	testing.expect(t, len(ins.values[1]) == 2, "Row 1 has 2 values")
	testing.expect(t, len(ins.values[2]) == 2, "Row 2 has 2 values")
	testing.expect_value(t, ins.values[2][0].(i64), i64(3))
}

@(test)
test_parse_having_aggregate :: proc(t: ^testing.T) {
	sql := "SELECT g FROM s GROUP BY g HAVING COUNT(*) >= 2 OR SUM(v) > 50;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "HAVING with aggregate refs should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect(t, len(sel.aggregates) == 2, "HAVING aggregates registered")
	if len(sel.aggregates) == 2 {
		testing.expect_value(t, sel.aggregates[0].func, parser.Aggregate_Func.COUNT)
		testing.expect_value(t, sel.aggregates[0].column, "")
		testing.expect_value(t, sel.aggregates[1].func, parser.Aggregate_Func.SUM)
		testing.expect_value(t, sel.aggregates[1].column, "v")
	}
}

@(test)
test_parse_having_aggregate_dedup :: proc(t: ^testing.T) {
	sql := "SELECT g, COUNT(*) FROM s GROUP BY g HAVING count > 1;"
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	testing.expect(t, ok, "HAVING with dup aggregate ref should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect(t, len(sel.aggregates) == 1, "HAVING aggregate deduped against select list")
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
	testing.expect_value(t, len(ins.values), 1)
	testing.expect_value(t, len(ins.values[0]), 2)
	blob_val, is_blob := ins.values[0][1].([]u8)
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
	blob_val, is_blob := ins.values[0][1].([]u8)
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
	blob_val, is_blob := ins.values[0][1].([]u8)
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
	testing.expect(t, wc.root != nil && wc.root.kind == .COND, "WHERE should be a single condition")
	testing.expect_value(t, wc.root.cond.operator, parser.Token_Type.LIKE)
	pattern_val, val_ok := wc.root.cond.rhs.(types.Value)
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
	testing.expect(t, on_cl.root != nil && on_cl.root.kind == .COND, "ON should be a single condition")
	testing.expect_value(t, on_cl.root.cond.column, "t1.id")
	testing.expect(t, on_cl.root.cond.operator == .EQUALS, "Expected = operator")
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
	testing.expect(t, on_cl.root != nil && on_cl.root.kind == .COND, "ON should be a single condition")
	cond := on_cl.root.cond
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

@(test)
test_parse_union :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT x FROM a UNION SELECT y FROM b;", context.temp_allocator)
	testing.expect(t, ok, "UNION should parse")
	comp, is_comp := stmt.type.(parser.Compound_Stmt)
	testing.expect(t, is_comp, "Expected Compound_Stmt")
	testing.expect_value(t, len(comp.operands), 1)
	testing.expect(t, comp.operands[0].op == .UNION, "Expected UNION op")
}

@(test)
test_parse_union_all :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT x FROM a UNION ALL SELECT y FROM b;", context.temp_allocator)
	testing.expect(t, ok, "UNION ALL should parse")
	comp, is_comp := stmt.type.(parser.Compound_Stmt)
	testing.expect(t, is_comp, "Expected Compound_Stmt")
	testing.expect(t, comp.operands[0].op == .UNION_ALL, "Expected UNION_ALL op")
}

@(test)
test_parse_intersect_except :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT x FROM a INTERSECT SELECT y FROM b EXCEPT SELECT z FROM c;", context.temp_allocator)
	testing.expect(t, ok, "INTERSECT/EXCEPT chain should parse")
	comp, is_comp := stmt.type.(parser.Compound_Stmt)
	testing.expect(t, is_comp, "Expected Compound_Stmt")
	testing.expect_value(t, len(comp.operands), 2)
	testing.expect(t, comp.operands[0].op == .INTERSECT, "Expected INTERSECT op")
	testing.expect(t, comp.operands[1].op == .EXCEPT, "Expected EXCEPT op")
}

@(test)
test_parse_compound_order_limit :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT x FROM a UNION SELECT y FROM b ORDER BY x LIMIT 5;", context.temp_allocator)
	testing.expect(t, ok, "compound ORDER BY LIMIT should parse")
	comp, is_comp := stmt.type.(parser.Compound_Stmt)
	testing.expect(t, is_comp, "Expected Compound_Stmt")
	order, has_order := comp.order_by.?
	testing.expect(t, has_order, "Expected compound ORDER BY")
	_ = order
	lim, has_lim := comp.limit.?
	testing.expect(t, has_lim, "Expected compound LIMIT")
	testing.expect_value(t, lim, u64(5))
}

@(test)
test_parse_fromless_literal_select :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT 1, 'a', X'CAFE', NULL;", context.temp_allocator)
	testing.expect(t, ok, "FROM-less literal SELECT should parse")
	sel, is_sel := stmt.type.(parser.Select_Stmt)
	testing.expect(t, is_sel, "Expected Select_Stmt")
	testing.expect_value(t, len(sel.literal_values), 4)
	testing.expect(t, len(sel.columns) == 4, "Expected 4 columns")
}

@(test)
test_parse_literal_union :: proc(t: ^testing.T) {
	stmt, ok, _ := parser.parse("SELECT 1 UNION SELECT 2;", context.temp_allocator)
	testing.expect(t, ok, "literal UNION should parse")
	comp, is_comp := stmt.type.(parser.Compound_Stmt)
	testing.expect(t, is_comp, "Expected Compound_Stmt")
	testing.expect_value(t, len(comp.operands), 1)
	testing.expect(t, comp.operands[0].op == .UNION, "Expected UNION op")
}

@(test)
test_parse_deep_nesting_guard :: proc(t: ^testing.T) {
	// Regression (fuzzing): deeply nested subqueries must return a parse error
	// rather than exhaust the native stack. MAX_PARSE_NESTING is 512; 600 nested
	// levels exceed it.
	build := proc(t: ^testing.T, levels: int) -> string {
		// Wrap inward: "SELECT * FROM (" * levels + body + ") AS s0 .. ) AS s{levels-1}".
		sb: strings.Builder
		strings.builder_init(&sb, context.temp_allocator)
		for _ in 0 ..< levels {
			strings.write_string(&sb, "SELECT * FROM (")
		}

		strings.write_string(&sb, "SELECT * FROM t")
		for i in 0 ..< levels {
			strings.write_string(&sb, fmt.tprintf(") AS s%d", i))
		}
		return strings.to_string(sb)
	}

	_, ok, _ := parser.parse(build(t, 600), context.temp_allocator)
	testing.expect(t, !ok, "over-limit nesting returns an error, not a crash")

	_, ok2, _ := parser.parse(build(t, 8), context.temp_allocator)
	testing.expect(t, ok2, "moderate nesting parses")
}

@(test)
test_parse_deep_paren_where_guard :: proc(t: ^testing.T) {
	// Regression (fuzzing): deeply nested parentheses in a WHERE expression
	// used to recurse parse_primary <-> parse_or_expr <-> parse_and_expr
	// until the native stack overflowed. MAX_PARSE_NESTING is 512; 600 nested
	// parens exceed it and must produce a parse error, not a crash.
	build := proc(t: ^testing.T, levels: int) -> string {
		sb: strings.Builder
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "SELECT * FROM t WHERE ")
		for _ in 0 ..< levels {
			strings.write_string(&sb, "(")
		}
		
		strings.write_string(&sb, "a=1")
		for _ in 0 ..< levels {
			strings.write_string(&sb, ")")
		}
		return strings.to_string(sb)
	}

	_, ok, _ := parser.parse(build(t, 600), context.temp_allocator)
	testing.expect(t, !ok, "over-limit paren nesting returns an error, not a crash")

	_, ok2, _ := parser.parse(build(t, 8), context.temp_allocator)
	testing.expect(t, ok2, "moderate paren nesting parses")
}

@(test)
test_tokenizer_keyword_length_guard :: proc(t: ^testing.T) {
	// Regression (fuzzing): an identifier longer than the longest keyword
	// ("references", 10 chars) used to hit keyword_bucket_offsets[9] = 58, which
	// exposed "references" to 11-char identifiers and indexed kw.word[10] OOB.
	_, _, _ = parser.parse("SELECT id FROM t AS REFERENCEST 5;", context.temp_allocator)
	testing.expect(t, true, "over-long identifier must not crash the tokenizer")

	// "references" must still tokenize as a keyword (FOREIGN KEY REFERENCES).
	_, ok, _ := parser.parse("CREATE TABLE a (x INT, FOREIGN KEY (x) REFERENCES b(id));", context.temp_allocator)
	testing.expect(t, ok, "FOREIGN KEY ... REFERENCES still parses")
}

@(test)
test_create_table_fk_error_cleanup :: proc(t: ^testing.T) {
	// Regression (fuzzing): when CREATE TABLE parses a FOREIGN KEY but the
	// statement then fails, the error cleanup freed the FK strings with
	// context.allocator instead of the passed allocator -> bad free under ASan.
	_, ok, _ := parser.parse(
		"CREATE TABLE products (price INT CHECK (price > 0), FOREIGN KEY (cat) REFERENCES c(idI);",
		context.temp_allocator,
	)
	testing.expect(t, !ok, "malformed FOREIGN KEY must return a parse error, not crash")
}

@(test)
test_create_table_unbalanced_check_parens :: proc(t: ^testing.T) {
	// Regression (fuzzing): an unbalanced `(` inside a CHECK(...) constraint used
	// to make the CHECK scan loop spin forever on EOF (peek/advance return EOF
	// without advancing), hanging the parser. It must return a parse error.
	build := proc(levels: int) -> string {
		sb: strings.Builder
		strings.builder_init(&sb, context.temp_allocator)
		strings.write_string(&sb, "CREATE TABLE t (a INT CHECK (a > 0")
		for _ in 0 ..< levels {
			strings.write_string(&sb, "(")
		}
		return strings.to_string(sb)
	}

	_, ok, _ := parser.parse(build(1), context.temp_allocator)
	testing.expect(t, !ok, "unbalanced CHECK parens return an error, not a hang")

	_, ok2, _ := parser.parse(build(200), context.temp_allocator)
	testing.expect(t, !ok2, "deep unbalanced CHECK parens return an error, not a hang")

	_, ok3, _ := parser.parse(
		"CREATE TABLE t (a INT CHECK (a > 0 AND (b < 5)));",
		context.temp_allocator,
	)
	testing.expect(t, ok3, "balanced nested CHECK parens still parse")
}

@(test)
test_create_table_check_eof_no_paren :: proc(t: ^testing.T) {
	// Regression (fuzzing): `CHECK` with no closing paren at all (or no opening
	// content) must terminate, not spin at EOF.
	_, ok, _ := parser.parse("CREATE TABLE t (a INT CHECK (a > 0", context.temp_allocator)
	testing.expect(t, !ok, "CHECK with unterminated paren returns an error, not a hang")

	_, ok2, _ := parser.parse("CREATE TABLE t (a INT CHECK", context.temp_allocator)
	testing.expect(t, !ok2, "bare CHECK keyword returns an error, not a hang")
}
