package parser

import "core:strconv"
import "core:strings"
import "core:unicode"
import "src:types"

// Token types
Token_Type :: enum {
	EOF,
	IDENTIFIER,
	NUMBER,
	STRING,
	COMMA,
	SEMICOLON,
	LPAREN,
	RPAREN,
	ASTERISK,
	// Keywords
	CREATE,
	TABLE,
	INSERT,
	INTO,
	VALUES,
	SELECT,
	FROM,
	WHERE,
	UPDATE,
	SET,
	DELETE,
	DROP,
	INTEGER,
	TEXT,
	REAL,
	BLOB,
	PRIMARY,
	KEY,
	NOT,
	NULL,
	AND,
	OR,
	// Operators
	EQUALS,
	NOT_EQUALS,
	LESS_THAN,
	GREATER_THAN,
	LESS_EQUAL,
	GREATER_EQUAL,
}

// Token represents a single unit of SQL code.
//
// Fields :-
// type - The category of the token
// lexeme - The actual string representation of the token
// line - Line number in source code
Token :: struct {
	type:   Token_Type,
	lexeme: string,
	line:   int,
}

// Statement Types correspond to the top-level SQL commands supported.
Statement_Type :: enum {
	CREATE_TABLE,
	INSERT,
	SELECT,
	UPDATE,
	DELETE,
	DROP_TABLE,
}

// Represents a single comparison in a WHERE clause.
Condition :: struct {
	column:   string,
	operator: Token_Type,
	value:    types.Value,
}

/*
 Where Clause

 Encapsulates filtering logic.

 LIMITATION: This parser currently only supports "Uniform Logic".
 You can have all ANDs or all ORs, but not a mix (e.g., `A AND B OR C` is invalid).

 Fields:
 - conditions: List of individual comparisons.
 - is_and:     If true, all conditions are joined by AND. If false, joined by OR.
 */
Where_Clause :: struct {
	conditions: []Condition,
	is_and:     bool,
}

/*
 The Abstract Syntax Tree (AST) node representing a complete SQL command.
 Not all fields are used for all statement types (i.e. "fat node" design).

 Note:
 String fields (table_name, columns names) are allocated using the provided allocator
 and must be freed using `statement_free`.
 */
Statement :: struct {
	type:           Statement_Type,
	table_name:     string,
	columns:        []types.Column,
	insert_values:  []types.Value,
	select_columns: []string,
	where_clause:   Maybe(Where_Clause),
	update_columns: []string,
	update_values:  []types.Value,
	from_table:     string,
}

// Parser state
Parser :: struct {
	tokens:  []Token,
	current: int,
}

// Helper: Maps string literals to their corresponding keyword Token_Type.
get_keyword_type :: proc(ident: string) -> Token_Type {
	switch ident {
	case "CREATE":
		return .CREATE
	case "TABLE":
		return .TABLE
	case "INSERT":
		return .INSERT
	case "INTO":
		return .INTO
	case "VALUES":
		return .VALUES
	case "SELECT":
		return .SELECT
	case "FROM":
		return .FROM
	case "WHERE":
		return .WHERE
	case "UPDATE":
		return .UPDATE
	case "SET":
		return .SET
	case "DELETE":
		return .DELETE
	case "DROP":
		return .DROP
	case "INTEGER":
		return .INTEGER
	case "TEXT":
		return .TEXT
	case "REAL":
		return .REAL
	case "BLOB":
		return .BLOB
	case "PRIMARY":
		return .PRIMARY
	case "KEY":
		return .KEY
	case "NOT":
		return .NOT
	case "NULL":
		return .NULL
	case "AND":
		return .AND
	case "OR":
		return .OR
	}
	return .IDENTIFIER
}

/*
Converts raw SQL source string into a list of Tokens.

Returns:
- []Token: Dynamic array of tokens.
- bool: Success status (false if illegal character or unterminated string).
 */
tokenize :: proc(sql: string, allocator := context.allocator) -> ([]Token, bool) {
	tokens := make([dynamic]Token, allocator)
	i := 0
	line := 1
	for i < len(sql) {
		c := rune(sql[i])
		if unicode.is_space(c) {
			if c == '\n' do line += 1
			i += 1
			continue
		}
		if c == '-' && i + 1 < len(sql) && sql[i + 1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i += 1
			}
			continue
		}
		if c == '\'' {
			start := i + 1
			i += 1
			for i < len(sql) {
				if sql[i] == '\'' {
					// Check for escaped quote ''
					if i + 1 < len(sql) && sql[i + 1] == '\'' {
						i += 2 // Skip both quotes
						continue
					} else {
						break // End of string
					}
				}
				i += 1
			}
			if i >= len(sql) {
				delete(tokens)
				return nil, false // Unterminated string
			}
			str_val := sql[start:i]
			append(&tokens, Token{.STRING, str_val, line})
			i += 1
			continue
		}
		if unicode.is_digit(c) || (c == '-' && i + 1 < len(sql) && unicode.is_digit(rune(sql[i + 1]))) {
			start := i
			if c == '-' do i += 1

			has_dot := false
			for i < len(sql) {
				ch := sql[i]
				if unicode.is_digit(rune(ch)) {
					i += 1
				} else if ch == '.' && !has_dot {
					has_dot = true
					i += 1
				} else {
					break
				}
			}

			num_str := sql[start:i]
			append(&tokens, Token{.NUMBER, num_str, line})
			continue
		}
		if unicode.is_alpha(c) || c == '_' {
			start := i
			for i < len(sql) &&
			    (unicode.is_alpha(rune(sql[i])) || unicode.is_digit(rune(sql[i])) || sql[i] == '_') {
				i += 1
			}

			ident := sql[start:i]
			upper := strings.to_upper(ident, context.temp_allocator)
			token_type := get_keyword_type(upper)
			append(&tokens, Token{token_type, ident, line})
			continue
		}

		switch c {
		case ',':
			append(&tokens, Token{.COMMA, ",", line})
			i += 1
		case ';':
			append(&tokens, Token{.SEMICOLON, ";", line})
			i += 1
		case '(':
			append(&tokens, Token{.LPAREN, "(", line})
			i += 1
		case ')':
			append(&tokens, Token{.RPAREN, ")", line})
			i += 1
		case '*':
			append(&tokens, Token{.ASTERISK, "*", line})
			i += 1
		case '=':
			append(&tokens, Token{.EQUALS, "=", line})
			i += 1
		case '<':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.LESS_EQUAL, "<=", line})
				i += 2
			} else if i + 1 < len(sql) && sql[i + 1] == '>' {
				append(&tokens, Token{.NOT_EQUALS, "<>", line})
				i += 2
			} else {
				append(&tokens, Token{.LESS_THAN, "<", line})
				i += 1
			}
		case '>':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.GREATER_EQUAL, ">=", line})
				i += 2
			} else {
				append(&tokens, Token{.GREATER_THAN, ">", line})
				i += 1
			}
		case '!':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.NOT_EQUALS, "!=", line})
				i += 2
			} else {
				delete(tokens)
				return nil, false // Unexpected character
			}
		case:
			// Unknown character
			delete(tokens)
			return nil, false
		}
	}
	append(&tokens, Token{.EOF, "", line})
	return tokens[:], true
}

// Return current token without consuming it.
peek :: proc(p: ^Parser) -> Token {
	if p.current >= len(p.tokens) {
		return Token{.EOF, "", 0}
	}
	return p.tokens[p.current]
}

// Return current token and advance cursor.
advance :: proc(p: ^Parser) -> Token {
	if p.current >= len(p.tokens) {
		return Token{.EOF, "", 0}
	}
	token := p.tokens[p.current]
	p.current += 1
	return token
}

// Check if current token matches any of the given types.
match :: proc(p: ^Parser, types: ..Token_Type) -> bool {
	for t in types {
		if peek(p).type == t {
			advance(p)
			return true
		}
	}
	return false
}

// Require the next token to be of a specific type.
expect :: proc(p: ^Parser, type: Token_Type) -> (Token, bool) {
	token := peek(p)
	if token.type != type {
		return token, false
	}
	advance(p)
	return token, true
}

// Parse CREATE TABLE statement
parse_create_table :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .CREATE_TABLE,
	}
	if !match(p, .TABLE) {
		return stmt, false
	}

	name_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.table_name = strings.clone(name_token.lexeme, allocator)
	if !match(p, .LPAREN) {
		return stmt, false
	}

	columns := make([dynamic]types.Column, allocator)
	for {
		col_name_token, ok2 := expect(p, .IDENTIFIER)
		if !ok2 {
			delete(columns)
			return stmt, false
		}

		col := types.Column {
			name = strings.clone(col_name_token.lexeme, allocator),
		}

		type_token := peek(p)
		#partial switch type_token.type {
		case .INTEGER:
			col.type = .INTEGER
			advance(p)
		case .TEXT:
			col.type = .TEXT
			advance(p)
		case .REAL:
			col.type = .REAL
			advance(p)
		case .BLOB:
			col.type = .BLOB
			advance(p)
		case:
			delete(columns)
			return stmt, false
		}

		for {
			if match(p, .PRIMARY) {
				if !match(p, .KEY) {
					delete(columns)
					return stmt, false
				}
				col.pk = true
			} else if match(p, .NOT) {
				if !match(p, .NULL) {
					delete(columns)
					return stmt, false
				}
				col.not_null = true
			} else {
				break
			}
		}

		append(&columns, col)
		if match(p, .RPAREN) {
			break
		} else if !match(p, .COMMA) {
			delete(columns)
			return stmt, false
		}
	}

	stmt.columns = columns[:]
	return stmt, true
}

// Parse INSERT statement
parse_insert :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .INSERT,
	}
	if !match(p, .INTO) {
		return stmt, false
	}

	name_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.table_name = strings.clone(name_token.lexeme, allocator)
	if !match(p, .VALUES) {
		return stmt, false
	}
	if !match(p, .LPAREN) {
		return stmt, false
	}

	values := make([dynamic]types.Value, allocator)
	for {
		token := peek(p)
		if token.type == .NUMBER {
			advance(p)
			if strings.contains(token.lexeme, ".") {
				val, ok2 := strconv.parse_f64(token.lexeme)
				if !ok2 {
					delete(values)
					return stmt, false
				}
				append(&values, types.value_real(val))
			} else {
				val, ok2 := strconv.parse_i64(token.lexeme)
				if !ok2 {
					delete(values)
					return stmt, false
				}
				append(&values, types.value_int(val))
			}
		} else if token.type == .STRING {
			advance(p)
			append(&values, types.value_text(strings.clone(token.lexeme, allocator)))
		} else if match(p, .NULL) {
			append(&values, types.value_null())
		} else {
			delete(values)
			return stmt, false
		}

		if match(p, .RPAREN) {
			break
		} else if !match(p, .COMMA) {
			delete(values)
			return stmt, false
		}
	}
	stmt.insert_values = values[:]
	return stmt, true
}

// Parse SELECT statement
parse_select :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .SELECT,
	}

	columns := make([dynamic]string, allocator)
	if match(p, .ASTERISK) {
	} else {
		for {
			col_token, ok := expect(p, .IDENTIFIER)
			if !ok {
				delete(columns)
				return stmt, false
			}
			append(&columns, strings.clone(col_token.lexeme, allocator))
			if !match(p, .COMMA) {
				break
			}
		}
	}

	stmt.select_columns = columns[:]
	if !match(p, .FROM) {
		return stmt, false
	}

	table_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.from_table = strings.clone(table_token.lexeme, allocator)
	if match(p, .WHERE) {
		where_clause_val := parse_where_clause(p, allocator)
		if where_clause_val == nil {
			return stmt, false
		}
		stmt.where_clause = where_clause_val
	}
	return stmt, true
}

// Parse UPDATE statement
parse_update :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .UPDATE,
	}

	table_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.from_table = strings.clone(table_token.lexeme, allocator)
	if !match(p, .SET) {
		return stmt, false
	}

	columns := make([dynamic]string, allocator)
	values := make([dynamic]types.Value, allocator)
	for {
		col_token, ok2 := expect(p, .IDENTIFIER)
		if !ok2 {
			delete(columns)
			delete(values)
			return stmt, false
		}

		append(&columns, strings.clone(col_token.lexeme, allocator))
		if !match(p, .EQUALS) {
			delete(columns)
			delete(values)
			return stmt, false
		}

		token := peek(p)
		if token.type == .NUMBER {
			advance(p)
			if strings.contains(token.lexeme, ".") {
				val, ok3 := strconv.parse_f64(token.lexeme)
				if !ok3 {
					delete(columns)
					delete(values)
					return stmt, false
				}
				append(&values, types.value_real(val))
			} else {
				val, ok3 := strconv.parse_i64(token.lexeme)
				if !ok3 {
					delete(columns)
					delete(values)
					return stmt, false
				}
				append(&values, types.value_int(val))
			}
		} else if token.type == .STRING {
			advance(p)
			append(&values, types.value_text(strings.clone(token.lexeme, allocator)))
		} else if match(p, .NULL) {
			append(&values, types.value_null())
		} else {
			delete(columns)
			delete(values)
			return stmt, false
		}

		if !match(p, .COMMA) {
			break
		}
	}

	stmt.update_columns = columns[:]
	stmt.update_values = values[:]
	if match(p, .WHERE) {
		clause := parse_where_clause(p, allocator)
		if clause == nil {
			return stmt, false
		}
		stmt.where_clause = clause
	}
	return stmt, true
}

// Parse DELETE statement
parse_delete :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .DELETE,
	}
	if !match(p, .FROM) {
		return stmt, false
	}

	table_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.from_table = strings.clone(table_token.lexeme, allocator)
	if match(p, .WHERE) {
		clause := parse_where_clause(p, allocator)
		if clause == nil {
			return stmt, false
		}
		stmt.where_clause = clause
	}
	return stmt, true
}

// Parse DROP TABLE statement
parse_drop_table :: proc(p: ^Parser, allocator := context.allocator) -> (Statement, bool) {
	stmt := Statement {
		type = .DROP_TABLE,
	}
	if !match(p, .TABLE) {
		return stmt, false
	}

	table_token, ok := expect(p, .IDENTIFIER)
	if !ok {
		return stmt, false
	}

	stmt.table_name = strings.clone(table_token.lexeme, allocator)
	return stmt, true
}

cleanup_where_conditions :: proc(conditions: [dynamic]Condition) {
	for cond in conditions {
		delete(cond.column)
		#partial switch v in cond.value {
		case string:
			delete(v)
		case []u8:
			delete(v)
		}
	}
	delete(conditions)
}

// Parse WHERE clause
// LIMITATION: Only supports uniform AND or uniform OR connectives
// Examples:
//   Valid:   WHERE a=1 AND b=2 AND c=3
//   Valid:   WHERE a=1 OR b=2 OR c=3
//   Invalid: WHERE a=1 AND b=2 OR c=3 (mixing AND/OR not supported)
parse_where_clause :: proc(p: ^Parser, allocator := context.allocator) -> Maybe(Where_Clause) {
	clause := Where_Clause {
		is_and = true,
	}

	conditions := make([dynamic]Condition, allocator)
	first_logical_op_seen := false
	for {
		col_token, ok := expect(p, .IDENTIFIER)
		if !ok {
			cleanup_where_conditions(conditions)
			return nil
		}

		cond := Condition {
			column = strings.clone(col_token.lexeme, allocator),
		}

		op_token := peek(p)
		#partial switch op_token.type {
		case .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL:
			cond.operator = op_token.type
			advance(p)
		case:
			delete(cond.column)
			cleanup_where_conditions(conditions)
			return nil
		}

		val_token := peek(p)
		if val_token.type == .NUMBER {
			advance(p)
			if strings.contains(val_token.lexeme, ".") {
				val, ok2 := strconv.parse_f64(val_token.lexeme)
				if !ok2 {
					delete(cond.column)
					cleanup_where_conditions(conditions)
					return nil
				}
				cond.value = types.value_real(val)
			} else {
				val, ok2 := strconv.parse_i64(val_token.lexeme)
				if !ok2 {
					delete(cond.column)
					cleanup_where_conditions(conditions)
					return nil
				}
				cond.value = types.value_int(val)
			}
		} else if val_token.type == .STRING {
			advance(p)
			cond.value = types.value_text(strings.clone(val_token.lexeme, allocator))
		} else if match(p, .NULL) {
			cond.value = types.value_null()
		} else {
			delete(cond.column)
			cleanup_where_conditions(conditions)
			return nil
		}

		append(&conditions, cond)
		if match(p, .AND) {
			if !first_logical_op_seen {
				clause.is_and = true
				first_logical_op_seen = true
			} else if !clause.is_and {
				// Error: mixing AND and OR not supported
				cleanup_where_conditions(conditions)
				return nil
			}
			continue
		} else if match(p, .OR) {
			if !first_logical_op_seen {
				clause.is_and = false
				first_logical_op_seen = true
			} else if clause.is_and {
				// Error: mixing AND and OR not supported
				cleanup_where_conditions(conditions)
				return nil
			}
			continue
		} else {
			break
		}
	}
	clause.conditions = conditions[:]
	return clause
}

/*
 Main function to take an SQL string and return a parsed AST.

 Parameters:
 - sql: The raw SQL string.
 - allocator: The allocator for the AST nodes (strings, arrays).

 Returns:
 - Statement: The parsed AST.
 - bool: Success flag.

 Note: Uses context.temp_allocator for the intermediate token list.
 */
parse :: proc(sql: string, allocator := context.allocator) -> (Statement, bool) {
	tokens, ok := tokenize(sql, context.temp_allocator)
	if !ok {
		return Statement{}, false
	}

	parser := Parser {
		tokens  = tokens,
		current = 0,
	}

	first := peek(&parser)
	stmt: Statement
	success: bool
	#partial switch first.type {
	case .CREATE:
		advance(&parser)
		stmt, success = parse_create_table(&parser, allocator)
	case .INSERT:
		advance(&parser)
		stmt, success = parse_insert(&parser, allocator)
	case .SELECT:
		advance(&parser)
		stmt, success = parse_select(&parser, allocator)
	case .UPDATE:
		advance(&parser)
		stmt, success = parse_update(&parser, allocator)
	case .DELETE:
		advance(&parser)
		stmt, success = parse_delete(&parser, allocator)
	case .DROP:
		advance(&parser)
		stmt, success = parse_drop_table(&parser, allocator)
	case:
		return Statement{}, false
	}

	if !success {
		statement_free(stmt)
		return Statement{}, false
	}
	return stmt, true
}

// Recursively frees all memory associated with a Statement AST.
statement_free :: proc(stmt: Statement) {
	delete(stmt.table_name)
	delete(stmt.from_table)

	for col in stmt.columns {
		delete(col.name)
	}

	delete(stmt.columns)
	for val in stmt.insert_values {
		if text, ok := val.(string); ok {
			delete(text)
		} else if blob, ok := val.([]u8); ok {
			delete(blob)
		}
	}

	delete(stmt.insert_values)
	for col in stmt.select_columns {
		delete(col)
	}

	delete(stmt.select_columns)
	for col in stmt.update_columns {
		delete(col)
	}

	delete(stmt.update_columns)
	for val in stmt.update_values {
		if text, ok := val.(string); ok {
			delete(text)
		} else if blob, ok := val.([]u8); ok {
			delete(blob)
		}
	}

	delete(stmt.update_values)
	if clause, has_where := stmt.where_clause.?; has_where {
		for cond in clause.conditions {
			delete(cond.column)
			if text, ok := cond.value.(string); ok {
				delete(text)
			} else if blob, ok := cond.value.([]u8); ok {
				delete(blob)
			}
		}
		delete(clause.conditions)
	}
}
