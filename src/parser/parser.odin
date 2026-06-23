package parser

import "core:strings"

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
	CREATE,
	TABLE,
	INSERT,
	INTO,
	VALUES,
	SELECT,
	DISTINCT,
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
	NULLS,
	FIRST,
	LAST,
	GROUP,
	HAVING,
	EQUALS,
	NOT_EQUALS,
	LESS_THAN,
	GREATER_THAN,
	LESS_EQUAL,
	GREATER_EQUAL,
	DOT,
	JOIN,
	INNER,
	CROSS,
	LEFT,
	RIGHT,
	ON,
	AS,
	OUTER,
	BEGIN,
	COMMIT,
	ROLLBACK,
	OF,
	SNAPSHOT,
	TIMESTAMP,
	EXPLAIN,
	CHECK,
	IN,
	FOREIGN,
	REFERENCES,
}

parse :: proc(sql: string, allocator := context.allocator) -> (Statement, bool) {
	tokens, ok := tokenize(sql, context.temp_allocator)
	if !ok { return {}, false }
	parser := Parser {
		tokens  = tokens,
		current = 0,
	}

	first := peek(&parser)
	variant: Statement_Variant
	success: bool
	#partial switch first.type {
	case .CREATE:
		advance(&parser); variant, success = parse_create_table(&parser, allocator)
	case .INSERT:
		advance(&parser); variant, success = parse_insert(&parser, allocator)
	case .SELECT:
		advance(&parser); variant, success = parse_select(&parser, allocator)
	case .UPDATE:
		advance(&parser); variant, success = parse_update(&parser, allocator)
	case .DELETE:
		advance(&parser); variant, success = parse_delete(&parser, allocator)
	case .DROP:
		advance(&parser); variant, success = parse_drop_table(&parser, allocator)
	case .BEGIN:
		advance(&parser); variant = Txn_Stmt {
			op = .BEGIN,
		}; success = true
	case .COMMIT:
		advance(&parser); variant = Txn_Stmt {
			op = .COMMIT,
		}; success = true
	case .ROLLBACK:
		advance(&parser); variant = Txn_Stmt {
			op = .ROLLBACK,
		}; success = true
	case .EXPLAIN:
		advance(&parser)
		inner := strings.trim_space(sql[len("EXPLAIN"):])
		variant = Explain_Stmt {
			sql = strings.clone(inner, allocator),
		}; success = true
	case:
		return {}, false
	}

	if !success { return {}, false }
	return Statement{type = variant, sql = strings.clone(sql, allocator)}, true
}
