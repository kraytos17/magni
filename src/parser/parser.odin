// Package parser tokenizes SQL and produces an AST for all supported DDL/DML/transaction commands.
package parser

import "core:strings"

err :: proc(p: ^Parser, msg: string) -> (Statement_Variant, bool) {
	if p.err_msg == "" { p.err_msg = msg }
	return nil, false
}

expect_match :: proc(p: ^Parser, tt: Token_Type, msg: string) -> bool {
	if match(p, tt) { return true }
	if p.err_msg == "" { p.err_msg = msg }
	return false
}

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

parse :: proc(sql: string, allocator := context.allocator) -> (Statement, bool, string) {
	tokens, ok := tokenize(sql, context.temp_allocator)
	if !ok { return {}, false, "Tokenizer error" }

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
		if parser.err_msg == "" { parser.err_msg = "Unexpected token" }
		return {}, false, parser.err_msg
	}

	if !success {
		if parser.err_msg == "" { parser.err_msg = "Syntax error" }
		return {}, false, parser.err_msg
	}
	return Statement{type = variant, sql = strings.clone(sql, allocator)}, true, ""
}
