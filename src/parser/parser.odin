package parser

import "core:encoding/hex"
import "core:strconv"
import "core:strings"
import "core:unicode"
import "src:types"

Token_Type :: enum u8 {
	EOF,
	IDENTIFIER,
	NUMBER,
	STRING,
	BLOB_LITERAL,
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
	DEFAULT,
	LIKE,
	LIMIT,
	OFFSET,
	ORDER,
	BY,
	ASC,
	DESC,
	GROUP,
	HAVING,
	// Operators
	EQUALS,
	NOT_EQUALS,
	LESS_THAN,
	GREATER_THAN,
	LESS_EQUAL,
	GREATER_EQUAL,
	DOT,
	// JOIN keywords
	JOIN,
	INNER,
	CROSS,
	LEFT,
	RIGHT,
	ON,
	AS,
	OUTER,
	// Transaction keywords
	BEGIN,
	COMMIT,
	ROLLBACK,
	// Time-travel keywords
	OF,
	SNAPSHOT,
	TIMESTAMP,
}

Token :: struct {
	type:   Token_Type,
	lexeme: string,
	line:   u32,
}

Condition :: struct {
	column:   string,
	operator: Token_Type,
	rhs:      union {
		types.Value,
		string,
	}, // string = column ref for equi-join
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
	columns:    []string, // Optional column list; nil means all columns
	values:     []types.Value,
}

Order_By_Column :: struct {
	column: string,
	desc:   bool,
}

Aggregate_Func :: enum {
	COUNT,
	SUM,
	AVG,
	MIN,
	MAX,
}

Aggregate_Expr :: struct {
	func:   Aggregate_Func,
	column: string, // empty string for *
}

Join_Type :: enum {
	INNER,
	CROSS,
	LEFT,
}

Join_Clause :: struct {
	join_type: Join_Type,
	source:    From_Source,
	alias:     string,
	on_clause: Maybe(Where_Clause),
}

From_Source :: union {
	string, // table name
	^Select_Stmt, // subquery
}

Join_Source_Result :: struct {
	source:  From_Source,
	alias:   string,
	success: bool,
}

Select_Stmt :: struct {
	from:            From_Source,
	from_alias:      string,
	joins:           []Join_Clause,
	columns:         []string,
	aggregates:      []Aggregate_Expr,
	where_clause:    Maybe(Where_Clause),
	order_by:        Maybe([]Order_By_Column),
	limit:           Maybe(u64),
	offset:          Maybe(u64),
	group_by:        []string,
	having:          Maybe(Where_Clause),
	as_of_snapshot:  Maybe(u64),
	as_of_timestamp: Maybe(u64),
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

Txn_Op :: enum {
	BEGIN,
	COMMIT,
	ROLLBACK,
}

Txn_Stmt :: struct {
	op: Txn_Op,
}

Statement_Variant :: union {
	Create_Stmt,
	Insert_Stmt,
	Select_Stmt,
	Update_Stmt,
	Delete_Stmt,
	Drop_Stmt,
	Txn_Stmt,
}

Statement :: struct {
	type: Statement_Variant,
	sql:  string,
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
	case "INT", "INTEGER":
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
	case "DEFAULT":
		return .DEFAULT
	case "LIKE":
		return .LIKE
	case "LIMIT":
		return .LIMIT
	case "OFFSET":
		return .OFFSET
	case "ORDER":
		return .ORDER
	case "BY":
		return .BY
	case "ASC":
		return .ASC
	case "DESC":
		return .DESC
	case "GROUP":
		return .GROUP
	case "HAVING":
		return .HAVING
	case "JOIN":
		return .JOIN
	case "INNER":
		return .INNER
	case "CROSS":
		return .CROSS
	case "LEFT":
		return .LEFT
	case "RIGHT":
		return .RIGHT
	case "ON":
		return .ON
	case "AS":
		return .AS
	case "OUTER":
		return .OUTER
	case "BEGIN":
		return .BEGIN
	case "COMMIT":
		return .COMMIT
	case "ROLLBACK":
		return .ROLLBACK
	case "OF":
		return .OF
	case "SNAPSHOT":
		return .SNAPSHOT
	case "TIMESTAMP":
		return .TIMESTAMP
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
	line := u32(1)
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
		if (c == 'X' || c == 'x') && i + 1 < len(sql) && sql[i + 1] == '\'' {
			start := i + 2
			i += 2
			for i < len(sql) && sql[i] != '\'' {
				i += 1
			}
			if i >= len(sql) {
				delete(tokens)
				return nil, false
			}

			append(&tokens, Token{.BLOB_LITERAL, sql[start:i], line})
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
		case '.':
			append(&tokens, Token{.DOT, ".", line})
			i += 1
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

is_keyword_token :: proc(t: Token_Type) -> bool {
	#partial switch t {
	case .EOF,
	     .IDENTIFIER,
	     .NUMBER,
	     .STRING,
	     .BLOB_LITERAL,
	     .COMMA,
	     .SEMICOLON,
	     .LPAREN,
	     .RPAREN,
	     .ASTERISK,
	     .EQUALS,
	     .NOT_EQUALS,
	     .LESS_THAN,
	     .GREATER_THAN,
	     .LESS_EQUAL,
	     .GREATER_EQUAL,
	     .DOT:
		return false
	}
	return true
}

parse_identifier :: proc(p: ^Parser, allocator := context.allocator) -> (str: string, ok: bool) {
	tok := peek(p)
	if tok.type != .IDENTIFIER && !is_keyword_token(tok.type) {
		return {}, false
	}
	advance(p)
	return strings.clone(tok.lexeme, allocator), true
}

// Parse an optionally qualified identifier: "name" or "table.name"
// Always returns the full identifier (including qualifier via dot).
parse_qualified_identifier :: proc(p: ^Parser, allocator := context.allocator) -> (str: string, ok: bool) {
	first := parse_identifier(p, allocator) or_return
	if match(p, .DOT) {
		second := parse_identifier(p, allocator) or_return
		result := strings.concatenate({first, ".", second}, allocator)
		delete(first, allocator)
		delete(second, allocator)
		return result, true
	}
	return first, true
}

// Parse a join source: either a table name or a subquery (SELECT ...)
// Returns the table name (empty for subquery), alias, optional subquery pointer, and success.
parse_join_source :: proc(p: ^Parser, allocator := context.allocator) -> Join_Source_Result {
	if is_subquery_start(p) {
		advance(p); advance(p)
		inner_variant, sel_ok := parse_select(p, allocator)
		if !sel_ok { return {} }

		inner_sel, _ := inner_variant.(Select_Stmt)
		subq := new(Select_Stmt, allocator)
		subq^ = inner_sel
		if !match(p, .RPAREN) {
			free(subq, allocator)
			return {}
		}

		match(p, .AS)
		al, al_ok := parse_identifier(p, allocator)
		if !al_ok {
			free(subq, allocator)
			return {}
		}
		return {source = subq, alias = al, success = true}
	}

	tbl, tbl_ok := parse_identifier(p, allocator)
	if !tbl_ok { return {} }
	if match(p, .AS) {
		// Peek ahead: if the next keyword is OF, this AS belongs to
		// "AS OF SNAPSHOT", not to an alias. Back out by decrementing.
		if peek(p).type == .OF {
			p.current -= 1
		} else {
			al2, al2_ok := parse_identifier(p, allocator)
			if !al2_ok { return {} }
			return {source = tbl, alias = al2, success = true}
		}
	} else if is_alias(p) {
		al3, al3_ok := parse_identifier(p, allocator)
		if !al3_ok { return {} }
		return {source = tbl, alias = al3, success = true}
	}
	return {source = tbl, alias = tbl, success = true}
}

is_subquery_start :: proc(p: ^Parser) -> bool {
	return(
		peek(p).type == .LPAREN &&
		p.current + 1 < len(p.tokens) &&
		p.tokens[p.current + 1].type == .SELECT \
	)
}

is_alias :: proc(p: ^Parser) -> bool {
	return peek(p).type == .IDENTIFIER && peek(p).lexeme != "("
}

parse_single_join :: proc(
	p: ^Parser,
	allocator := context.allocator,
	join_type: Join_Type,
	on_required: bool,
) -> (
	jc: Join_Clause,
	ok: bool,
) {
	js := parse_join_source(p, allocator)
	if !js.success { return {}, false }

	on_cl: Maybe(Where_Clause)
	if on_required {
		if !match(p, .ON) { return {}, false }
		on_cl, ok = parse_where_clause(p, allocator)
		if !ok { return {}, false }
	} else if match(p, .ON) {
		on_cl, ok = parse_where_clause(p, allocator)
		if !ok { return {}, false }
	}
	return Join_Clause{join_type = join_type, source = js.source, alias = js.alias, on_clause = on_cl}, true
}

// Parse CREATE TABLE statement
parse_create_table :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !match(p, .TABLE) do return nil, false

	table_name := parse_identifier(p, allocator) or_return
	if !match(p, .LPAREN) {
		delete(table_name, allocator)
		return nil, false
	}

	columns := make([dynamic]types.Column, allocator)
	defer if !ok {
		for col in columns { delete(col.name, allocator) }
		delete(table_name, allocator)
		delete(columns)
	}

	for {
		col := types.Column {
			name = parse_identifier(p, allocator) or_return,
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
			} else if match(p, .DEFAULT) {
				val, val_ok := parse_value(p, allocator)
				if !val_ok { return nil, false }
				col.default_value = val
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
parse_insert :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	if !match(p, .INTO) do return nil, false

	table_name := parse_identifier(p, allocator) or_return
	columns := make([dynamic]string, allocator)
	defer if !ok {
		for c in columns do delete(c, allocator)
		delete(columns)
	}

	// Parse optional column list: INSERT INTO t (col1, col2) VALUES ...
	if peek(p).type == .LPAREN && p.current + 1 < len(p.tokens) {
		next_type := p.tokens[p.current + 1].type
		if next_type == .IDENTIFIER || next_type == .RPAREN {
			advance(p)
			for {
				col := parse_identifier(p, allocator) or_return
				append(&columns, col)
				if match(p, .RPAREN) {
					break
				} else if !match(p, .COMMA) {
					return nil, false
				}
			}
		}
	}
	if !match(p, .VALUES) || !match(p, .LPAREN) {
		delete(table_name, allocator)
		return nil, false
	}

	values := make([dynamic]types.Value, allocator)
	defer if !ok {
		for v in values {
			types.value_delete(v, allocator)
		}
		delete(table_name, allocator)
		delete(values)
	}

	for {
		val, val_ok := parse_value(p, allocator)
		if !val_ok { return nil, false }

		append(&values, val)
		if match(p, .RPAREN) {
			break
		} else if !match(p, .COMMA) {
			return nil, false
		}
	}
	return Insert_Stmt{table_name = table_name, columns = columns[:], values = values[:]}, true
}

// Parse the SELECT column list (including aggregate functions).
// Returns true on success, false on parse error.
@(private = "file")
parse_select_columns :: proc(
	p: ^Parser,
	columns: ^[dynamic]string,
	aggregates: ^[dynamic]Aggregate_Expr,
	allocator := context.allocator,
) -> bool {
	if match(p, .ASTERISK) {  } else {
		for {
			tok := peek(p)
			if tok.type == .IDENTIFIER &&
			   p.current + 1 < len(p.tokens) &&
			   p.tokens[p.current + 1].type == .LPAREN {
				func_name_upper := strings.to_upper(tok.lexeme, context.temp_allocator)
				agg_func: Aggregate_Func
				agg_ok := false
				switch func_name_upper {
				case "COUNT":
					agg_func = .COUNT; agg_ok = true
				case "SUM":
					agg_func = .SUM; agg_ok = true
				case "AVG":
					agg_func = .AVG; agg_ok = true
				case "MIN":
					agg_func = .MIN; agg_ok = true
				case "MAX":
					agg_func = .MAX; agg_ok = true
				}

				if !agg_ok {
					col, cok := parse_identifier(p, allocator)
					if !cok { return false }
					append(columns, col)
				} else {
					advance(p)
					advance(p)
					is_star := match(p, .ASTERISK)
					arg_col: string
					if !is_star {
						var, acok := parse_qualified_identifier(p, allocator)
						if !acok { return false }
						arg_col = var
					}
					if !match(p, .RPAREN) { return false }

					arg_display := "*" if is_star else arg_col
					display := strings.concatenate({tok.lexeme, "(", arg_display, ")"}, allocator)
					append(columns, display)
					agg_col := "" if is_star else arg_col
					append(aggregates, Aggregate_Expr{func = agg_func, column = agg_col})
				}
			} else {
				col, cok := parse_qualified_identifier(p, allocator)
				if !cok { return false }
				append(columns, col)
			}
			if !match(p, .COMMA) do break
		}
	}
	return true
}

// Parse JOIN clauses after the FROM clause.
@(private = "file")
parse_join_clauses :: proc(p: ^Parser, allocator := context.allocator) -> [dynamic]Join_Clause {
	joins := make([dynamic]Join_Clause, allocator)
	for {
		if match(p, .COMMA) {
			jc, jc_ok := parse_single_join(p, allocator, .CROSS, false)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .JOIN) {
			jc, jc_ok := parse_single_join(p, allocator, .INNER, false)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .INNER) {
			if !match(p, .JOIN) do break
			jc, jc_ok := parse_single_join(p, allocator, .INNER, true)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .CROSS) {
			if !match(p, .JOIN) do break
			jc, jc_ok := parse_single_join(p, allocator, .CROSS, false)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .LEFT) {
			match(p, .OUTER)
			if !match(p, .JOIN) do break
			jc, jc_ok := parse_single_join(p, allocator, .LEFT, true)
			if !jc_ok { break }
			append(&joins, jc)
		} else {
			break
		}
	}
	return joins
}

// Parse SELECT statement
parse_select :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	columns := make([dynamic]string, allocator)
	defer if !ok do delete(columns)
	aggregates := make([dynamic]Aggregate_Expr, allocator)
	defer if !ok do delete(aggregates)

	if !parse_select_columns(p, &columns, &aggregates, allocator) {
		return nil, false
	}
	if !match(p, .FROM) do return nil, false

	from_val: From_Source
	from_alias := ""
	js := parse_join_source(p, allocator)
	if !js.success { return nil, false }

	from_val = js.source
	from_alias = js.alias
	joins := parse_join_clauses(p, allocator)
	defer if !ok do delete(joins)

	as_of_snapshot: Maybe(u64)
	as_of_timestamp: Maybe(u64)
	if match(p, .AS) && match(p, .OF) {
		if match(p, .SNAPSHOT) {
			id_token := expect(p, .NUMBER) or_return
			as_of_snapshot = strconv.parse_u64(id_token.lexeme) or_return
		} else if match(p, .TIMESTAMP) {
			id_token := expect(p, .NUMBER) or_return
			as_of_timestamp = strconv.parse_u64(id_token.lexeme) or_return
		}
	}

	where_clause: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_clause = parse_where_clause(p, allocator) or_return
	}

	group_by := make([dynamic]string, allocator)
	defer if !ok do delete(group_by)
	if match(p, .GROUP) {
		if !match(p, .BY) do return nil, false
		for {
			append(&group_by, parse_qualified_identifier(p, allocator) or_return)
			if !match(p, .COMMA) do break
		}
	}

	having_cl: Maybe(Where_Clause)
	if match(p, .HAVING) {
		having_cl = parse_where_clause(p, allocator) or_return
	}

	order_by: Maybe([]Order_By_Column)
	if match(p, .ORDER) {
		if !match(p, .BY) do return nil, false
		order_cols := make([dynamic]Order_By_Column, allocator)
		defer if !ok do delete(order_cols)
		for {
			col := parse_qualified_identifier(p, allocator) or_return
			desc := false
			if match(p, .ASC) {  } else if match(p, .DESC) { desc = true }
			append(&order_cols, Order_By_Column{column = col, desc = desc})
			if !match(p, .COMMA) do break
		}
		order_by = order_cols[:]
	}

	limit: Maybe(u64)
	offset: Maybe(u64)
	if match(p, .LIMIT) {
		limit_token := expect(p, .NUMBER) or_return
		limit_val := strconv.parse_u64(limit_token.lexeme) or_return
		limit = limit_val
		if match(p, .OFFSET) {
			offset_token := expect(p, .NUMBER) or_return
			offset_val := strconv.parse_u64(offset_token.lexeme) or_return
			offset = offset_val
		}
	}

	return Select_Stmt {
			from = from_val,
			from_alias = from_alias,
			joins = joins[:],
			columns = columns[:],
			aggregates = aggregates[:],
			where_clause = where_clause,
			order_by = order_by,
			limit = limit,
			offset = offset,
			group_by = group_by[:],
			having = having_cl,
			as_of_snapshot = as_of_snapshot,
			as_of_timestamp = as_of_timestamp,
		},
		true
}

// Parse UPDATE statement
parse_update :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	table_name := parse_identifier(p, allocator) or_return
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
		append(&columns, parse_identifier(p, allocator) or_return)
		if !match(p, .EQUALS) do return nil, false

		val, val_ok := parse_value(p, allocator)
		if !val_ok { return nil, false }
		append(&values, val)
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
parse_delete :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	if !match(p, .FROM) do return nil, false

	table_name := parse_identifier(p, allocator) or_return
	defer if !ok do delete(table_name, allocator)

	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_cl = parse_where_clause(p, allocator) or_return
	}
	return Delete_Stmt{table_name = table_name, where_clause = where_cl}, true
}

// Parse DROP TABLE statement
parse_drop_table :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	if !match(p, .TABLE) do return nil, false
	table_name := parse_identifier(p, allocator) or_return
	return Drop_Stmt{table_name = table_name}, true
}

@(private = "file")
cleanup_where_conditions :: proc(conditions: [dynamic]Condition, allocator := context.allocator) {
	for cond in conditions {
		delete(cond.column, allocator)
		if rc, ok := cond.rhs.(string); ok {
			delete(rc, allocator)
		}
		if val, ok := cond.rhs.(types.Value); ok {
			types.value_delete(val)
		}
	}
	delete(conditions)
}

// Parse a value: NUMBER, STRING, BLOB, NULL, or an identifier (which might be a qualified column ref like "t.col")
parse_value :: proc(p: ^Parser, allocator := context.allocator) -> (val: types.Value, ok: bool) {
	token := peek(p)
	#partial switch token.type {
	case .NUMBER:
		advance(p)
		if strings.contains(token.lexeme, ".") {
			val := strconv.parse_f64(token.lexeme) or_return
			return types.value_real(val), true
		} else {
			val := strconv.parse_i64(token.lexeme) or_return
			return types.value_int(val), true
		}
	case .STRING:
		advance(p)
		return types.value_text(strings.clone(token.lexeme, allocator)), true
	case .BLOB_LITERAL:
		advance(p)
		bytes, decode_ok := hex.decode(transmute([]u8)token.lexeme, allocator)
		if !decode_ok { return {}, false }
		return types.value_blob(bytes), true
	case .NULL:
		advance(p)
		return types.value_null(), true
	case .IDENTIFIER:
		advance(p)
		if match(p, .DOT) {
			second := parse_identifier(p, allocator) or_return
			qualified := strings.concatenate({token.lexeme, ".", second}, allocator)
			delete(second, allocator)
			return types.value_text(qualified), true
		}
		return types.value_text(strings.clone(token.lexeme, allocator)), true
	}
	return {}, false
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
	defer if !ok do cleanup_where_conditions(conditions, allocator)

	first_logical_op_seen := false
	for {
		cond := Condition {
			column = parse_qualified_identifier(p, allocator) or_return,
		}

		op_token := peek(p)
		#partial switch op_token.type {
		case .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL, .LIKE:
			cond.operator = op_token.type
			advance(p)
		case:
			delete(cond.column, allocator)
			return nil, false
		}

		// If RHS is an identifier (optionally qualified), treat as column ref for equi-join
		if peek(p).type == .IDENTIFIER {
			right_col := parse_qualified_identifier(p, allocator) or_return
			cond.rhs = right_col
		} else {
			val, val_ok := parse_value(p, allocator)
			if !val_ok {
				delete(cond.column, allocator)
				return nil, false
			}
			cond.rhs = val
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
		return {}, false
	}

	parser := Parser {
		tokens  = tokens,
		current = 0,
	}

	first := peek(&parser)
	variant: Statement_Variant
	success: bool
	#partial switch first.type {
	case .CREATE:
		advance(&parser)
		variant, success = parse_create_table(&parser, allocator)
	case .INSERT:
		advance(&parser)
		variant, success = parse_insert(&parser, allocator)
	case .SELECT:
		advance(&parser)
		variant, success = parse_select(&parser, allocator)
	case .UPDATE:
		advance(&parser)
		variant, success = parse_update(&parser, allocator)
	case .DELETE:
		advance(&parser)
		variant, success = parse_delete(&parser, allocator)
	case .DROP:
		advance(&parser)
		variant, success = parse_drop_table(&parser, allocator)
	case .BEGIN:
		advance(&parser)
		variant = Txn_Stmt {
			op = .BEGIN,
		}
		success = true
	case .COMMIT:
		advance(&parser)
		variant = Txn_Stmt {
			op = .COMMIT,
		}
		success = true
	case .ROLLBACK:
		advance(&parser)
		variant = Txn_Stmt {
			op = .ROLLBACK,
		}
		success = true
	case:
		return {}, false
	}

	if !success {
		return {}, false
	}
	return Statement{type = variant, sql = strings.clone(sql, allocator)}, true
}

where_clause_free :: proc(w: Where_Clause, allocator := context.allocator) {
	for cond in w.conditions {
		delete(cond.column, allocator)
		if rc, ok := cond.rhs.(string); ok {
			delete(rc, allocator)
		}
		if val, ok := cond.rhs.(types.Value); ok {
			types.value_delete(val, allocator)
		}
	}
	delete(w.conditions, allocator)
}

// Recursively frees all memory associated with a Statement AST.
statement_free :: proc(stmt: Statement, allocator := context.allocator) {
	delete(stmt.sql, allocator)
	switch s in stmt.type {
	case Create_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col.name, allocator)
			if def, ok := col.default_value.?; ok {
				types.value_delete(def, allocator)
			}
		}
		delete(s.columns, allocator)
	case Insert_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col, allocator)
		}

		delete(s.columns, allocator)
		for val in s.values {
			types.value_delete(val, allocator)
		}
		delete(s.values, allocator)
	case Select_Stmt:
		#partial switch src in s.from {
		case string:
			delete(src, allocator)
		case ^Select_Stmt:
			statement_free(Statement{type = src^, sql = ""}, allocator)
			free(src, allocator)
		}
		if s.from_alias != "" {
			delete(s.from_alias, allocator)
		}
		for j in s.joins {
			#partial switch j_src in j.source {
			case string:
				delete(j_src, allocator)
			case ^Select_Stmt:
				statement_free(Statement{type = j_src^, sql = ""}, allocator)
				free(j_src, allocator)
			}
			if j.alias != "" {
				delete(j.alias, allocator)
			}
			if on_cl, ok := j.on_clause.?; ok {
				where_clause_free(on_cl, allocator)
			}
		}

		delete(s.joins, allocator)
		for col in s.columns {
			delete(col, allocator)
		}

		delete(s.columns, allocator)
		for agg in s.aggregates {
			delete(agg.column, allocator)
		}

		delete(s.aggregates, allocator)
		if w, ok := s.where_clause.?; ok {
			where_clause_free(w, allocator)
		}
		if order, ok := s.order_by.?; ok {
			for o in order {
				delete(o.column, allocator)
			}
			delete(order, allocator)
		}
		for col in s.group_by {
			delete(col, allocator)
		}

		delete(s.group_by, allocator)
		if h, ok := s.having.?; ok {
			where_clause_free(h, allocator)
		}
	case Update_Stmt:
		delete(s.table_name, allocator)
		for col in s.update_columns {
			delete(col, allocator)
		}

		delete(s.update_columns, allocator)
		for val in s.update_values {
			types.value_delete(val, allocator)
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
	case Txn_Stmt:
	}
}
