package parser

import "core:strconv"
import "core:strings"
import "core:unicode"
import "src:types"

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

Token :: struct {
	type:   Token_Type,
	lexeme: string,
	line:   int,
}

Condition :: struct {
	column:   string,
	operator: Token_Type,
	value:    types.Value,
}

Where_Clause :: struct {
	conditions: []Condition,
	is_and:     bool,
}

Create_Stmt :: struct {
	table_name: string,
	columns:    []types.Column,
}

Insert_Stmt :: struct {
	table_name: string,
	values:     []types.Value,
}

Select_Stmt :: struct {
	table_name:   string,
	columns:      []string,
	where_clause: Maybe(Where_Clause),
}

Update_Stmt :: struct {
	table_name:     string,
	update_columns: []string,
	update_values:  []types.Value,
	where_clause:   Maybe(Where_Clause),
}

Delete_Stmt :: struct {
	table_name:   string,
	where_clause: Maybe(Where_Clause),
}

Drop_Stmt :: struct {
	table_name: string,
}

Statement :: union {
	Create_Stmt,
	Insert_Stmt,
	Select_Stmt,
	Update_Stmt,
	Delete_Stmt,
	Drop_Stmt,
}

Parser :: struct {
	tokens:  []Token,
	current: int,
}

// Maps string literals to their corresponding keyword Token_Type.
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
					if i + 1 < len(sql) && sql[i + 1] == '\'' {
						i += 2 // Escaped quote
						continue
					}
					break
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
			append(&tokens, Token{.NUMBER, sql[start:i], line})
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
				return nil, false
			}
		case:
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
parse_create_table :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	if !match(p, .TABLE) do return nil, false

	name_token := expect(p, .IDENTIFIER) or_return
	table_name := strings.clone(name_token.lexeme, allocator)
	if !match(p, .LPAREN) {
		delete(table_name, allocator)
		return nil, false
	}

	columns := make([dynamic]types.Column, allocator)
	defer if !ok {
		delete(table_name, allocator)
		delete(columns)
	}

	for {
		col_name_token := expect(p, .IDENTIFIER) or_return
		col := types.Column {
			name = strings.clone(col_name_token.lexeme, allocator),
		}

		type_token := peek(p)
		#partial switch type_token.type {
		case .INTEGER:
			col.type = .INTEGER; advance(p)
		case .TEXT:
			col.type = .TEXT; advance(p)
		case .REAL:
			col.type = .REAL; advance(p)
		case .BLOB:
			col.type = .BLOB; advance(p)
		case:
			return nil, false
		}

		for {
			if match(p, .PRIMARY) {
				if !match(p, .KEY) do return nil, false
				col.pk = true
			} else if match(p, .NOT) {
				if !match(p, .NULL) do return nil, false
				col.not_null = true
			} else {
				break
			}
		}

		append(&columns, col)
		if match(p, .RPAREN) {
			break
		} else if !match(p, .COMMA) {
			return nil, false
		}
	}
	return Create_Stmt{table_name = table_name, columns = columns[:]}, true
}

// Parse INSERT statement
parse_insert :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	if !match(p, .INTO) do return nil, false

	name_token := expect(p, .IDENTIFIER) or_return
	table_name := strings.clone(name_token.lexeme, allocator)
	if !match(p, .VALUES) || !match(p, .LPAREN) {
		delete(table_name, allocator)
		return nil, false
	}

	values := make([dynamic]types.Value, allocator)
	defer if !ok {
		delete(table_name, allocator)
		delete(values)
	}

	for {
		token := peek(p)
		#partial switch token.type {
		case .NUMBER:
			advance(p)
			if strings.contains(token.lexeme, ".") {
				val := strconv.parse_f64(token.lexeme) or_return
				append(&values, types.value_real(val))
			} else {
				val := strconv.parse_i64(token.lexeme) or_return
				append(&values, types.value_int(val))
			}
		case .STRING:
			advance(p)
			append(&values, types.value_text(strings.clone(token.lexeme, allocator)))
		case .NULL:
			advance(p)
			append(&values, types.value_null())
		case:
			return nil, false
		}

		if match(p, .RPAREN) {
			break
		} else if !match(p, .COMMA) {
			return nil, false
		}
	}
	return Insert_Stmt{table_name = table_name, values = values[:]}, true
}

// Parse SELECT statement
parse_select :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	columns := make([dynamic]string, allocator)
	defer if !ok do delete(columns)

	if match(p, .ASTERISK) {
	} else {
		for {
			col_token := expect(p, .IDENTIFIER) or_return
			append(&columns, strings.clone(col_token.lexeme, allocator))
			if !match(p, .COMMA) do break
		}
	}

	if !match(p, .FROM) do return nil, false

	table_token := expect(p, .IDENTIFIER) or_return
	table_name := strings.clone(table_token.lexeme, allocator)
	defer if !ok do delete(table_name, allocator)

	where_clause: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_clause = parse_where_clause(p, allocator) or_return
	}
	return Select_Stmt{table_name = table_name, columns = columns[:], where_clause = where_clause}, true
}

// Parse UPDATE statement
parse_update :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	table_token := expect(p, .IDENTIFIER) or_return
	table_name := strings.clone(table_token.lexeme, allocator)
	if !match(p, .SET) {
		delete(table_name, allocator)
		return nil, false
	}

	columns := make([dynamic]string, allocator)
	values := make([dynamic]types.Value, allocator)
	defer if !ok {
		delete(table_name, allocator)
		delete(columns)
		delete(values)
	}

	for {
		col_token := expect(p, .IDENTIFIER) or_return
		append(&columns, strings.clone(col_token.lexeme, allocator))
		if !match(p, .EQUALS) do return nil, false

		token := peek(p)
		#partial switch token.type {
		case .NUMBER:
			advance(p)
			if strings.contains(token.lexeme, ".") {
				val := strconv.parse_f64(token.lexeme) or_return
				append(&values, types.value_real(val))
			} else {
				val := strconv.parse_i64(token.lexeme) or_return
				append(&values, types.value_int(val))
			}
		case .STRING:
			advance(p)
			append(&values, types.value_text(strings.clone(token.lexeme, allocator)))
		case .NULL:
			advance(p)
			append(&values, types.value_null())
		case:
			return nil, false
		}
		if !match(p, .COMMA) do break
	}

	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_cl = parse_where_clause(p, allocator) or_return
	}

	return Update_Stmt {
			table_name = table_name,
			update_columns = columns[:],
			update_values = values[:],
			where_clause = where_cl,
		},
		true
}

// Parse DELETE statement
parse_delete :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	if !match(p, .FROM) do return nil, false

	table_token := expect(p, .IDENTIFIER) or_return
	table_name := strings.clone(table_token.lexeme, allocator)
	defer if !ok do delete(table_name, allocator)
	
	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_cl = parse_where_clause(p, allocator) or_return
	}
	return Delete_Stmt{table_name = table_name, where_clause = where_cl}, true
}

// Parse DROP TABLE statement
parse_drop_table :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement, ok: bool) {
	if !match(p, .TABLE) do return nil, false
	table_token := expect(p, .IDENTIFIER) or_return
	return Drop_Stmt{table_name = strings.clone(table_token.lexeme, allocator)}, true
}

@(private = "file")
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
//
// LIMITATION: Only supports uniform AND or uniform OR connectives
//
// Examples:
//   Valid:   WHERE a=1 AND b=2 AND c=3
//   Valid:   WHERE a=1 OR b=2 OR c=3
//   Invalid: WHERE a=1 AND b=2 OR c=3 (mixing AND/OR not supported)
parse_where_clause :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	clause: Maybe(Where_Clause),
	ok: bool,
) {
	w := Where_Clause {
		is_and = true,
	}
	conditions := make([dynamic]Condition, allocator)
	defer if !ok do cleanup_where_conditions(conditions)

	first_logical_op_seen := false
	for {
		col_token := expect(p, .IDENTIFIER) or_return
		cond := Condition {
			column = strings.clone(col_token.lexeme, allocator),
		}

		op_token := peek(p)
		#partial switch op_token.type {
		case .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL:
			cond.operator = op_token.type
			advance(p)
		case:
			delete(cond.column, allocator)
			return nil, false
		}

		val_token := peek(p)
		#partial switch val_token.type {
		case .NUMBER:
			advance(p)
			if strings.contains(val_token.lexeme, ".") {
				val := strconv.parse_f64(val_token.lexeme) or_return
				cond.value = types.value_real(val)
			} else {
				val := strconv.parse_i64(val_token.lexeme) or_return
				cond.value = types.value_int(val)
			}
		case .STRING:
			advance(p)
			cond.value = types.value_text(strings.clone(val_token.lexeme, allocator))
		case .NULL:
			advance(p)
			cond.value = types.value_null()
		case:
			delete(cond.column, allocator)
			return nil, false
		}

		append(&conditions, cond)
		if match(p, .AND) {
			if !first_logical_op_seen {
				w.is_and = true
				first_logical_op_seen = true
			} else if !w.is_and {
				return nil, false
			}
		} else if match(p, .OR) {
			if !first_logical_op_seen {
				w.is_and = false
				first_logical_op_seen = true
			} else if w.is_and {
				return nil, false
			}
		} else {
			break
		}
	}
	w.conditions = conditions[:]
	return w, true
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
		return nil, false
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
		return nil, false
	}

	if !success {
		statement_free(stmt, allocator)
		return nil, false
	}
	return stmt, true
}

where_clause_free :: proc(w: Where_Clause, allocator := context.allocator) {
	for cond in w.conditions {
		delete(cond.column, allocator)
		#partial switch v in cond.value {
		case string:
			delete(v, allocator)
		case []u8:
			delete(v, allocator)
		}
	}
	delete(w.conditions, allocator)
}

// Recursively frees all memory associated with a Statement AST.
statement_free :: proc(stmt: Statement, allocator := context.allocator) {
	if stmt == nil { return }

	switch s in stmt {
	case Create_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col.name, allocator)
		}
		delete(s.columns, allocator)
	case Insert_Stmt:
		delete(s.table_name, allocator)
		for val in s.values {
			#partial switch v in val {
			case string:
				delete(v, allocator)
			case []u8:
				delete(v, allocator)
			}
		}
		delete(s.values, allocator)
	case Select_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col, allocator)
		}
		delete(s.columns, allocator)
		if w, ok := s.where_clause.?; ok {
			where_clause_free(w, allocator)
		}
	case Update_Stmt:
		delete(s.table_name, allocator)
		for col in s.update_columns {
			delete(col, allocator)
		}
		delete(s.update_columns, allocator)
		for val in s.update_values {
			#partial switch v in val {
			case string:
				delete(v, allocator)
			case []u8:
				delete(v, allocator)
			}
		}
		delete(s.update_values, allocator)
		if w, ok := s.where_clause.?; ok {
			where_clause_free(w, allocator)
		}
	case Delete_Stmt:
		delete(s.table_name, allocator)
		if w, ok := s.where_clause.?; ok {
			where_clause_free(w, allocator)
		}
	case Drop_Stmt:
		delete(s.table_name, allocator)
	}
}
