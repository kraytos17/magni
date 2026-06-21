package parser

import "core:unicode"

@(private)
match_keyword :: proc(ident: string) -> Token_Type {
	if len(ident) == 2 {
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' { return .IN }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'f' { return .OF }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'n' { return .ON }
		if (ident[0] | 0x20) == 'a' && (ident[1] | 0x20) == 's' { return .AS }
		if (ident[0] | 0x20) == 'b' && (ident[1] | 0x20) == 'y' { return .BY }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'r' { return .OR }
	}
	if len(ident) == 3 {
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 't' { return .INTEGER }
		if (ident[0] | 0x20) == 'n' && (ident[1] | 0x20) == 'o' && (ident[2] | 0x20) == 't' { return .NOT }
		if (ident[0] | 0x20) == 's' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 't' { return .SET }
		if (ident[0] | 0x20) == 'k' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'y' { return .KEY }
		if (ident[0] | 0x20) == 'a' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 'd' { return .AND }
		if (ident[0] | 0x20) == 'a' && (ident[1] | 0x20) == 's' && (ident[2] | 0x20) == 'c' { return .ASC }
	}
	if len(ident) == 4 {
		if (ident[0] | 0x20) == 'f' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'o' && (ident[3] | 0x20) == 'm' { return .FROM }
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 't' && (ident[3] | 0x20) == 'o' { return .INTO }
		if (ident[0] | 0x20) == 'j' && (ident[1] | 0x20) == 'o' && (ident[2] | 0x20) == 'i' && (ident[3] | 0x20) == 'n' { return .JOIN }
		if (ident[0] | 0x20) == 'l' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 'k' && (ident[3] | 0x20) == 'e' { return .LIKE }
		if (ident[0] | 0x20) == 'n' && (ident[1] | 0x20) == 'u' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'l' { return .NULL }
		if (ident[0] | 0x20) == 't' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'x' && (ident[3] | 0x20) == 't' { return .TEXT }
		if (ident[0] | 0x20) == 'b' && (ident[1] | 0x20) == 'l' && (ident[2] | 0x20) == 'o' && (ident[3] | 0x20) == 'b' { return .BLOB }
		if (ident[0] | 0x20) == 'r' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'a' && (ident[3] | 0x20) == 'l' { return .REAL }
		if (ident[0] | 0x20) == 'd' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'o' && (ident[3] | 0x20) == 'p' { return .DROP }
		if (ident[0] | 0x20) == 'l' && (ident[1] | 0x20) == 'a' && (ident[2] | 0x20) == 's' && (ident[3] | 0x20) == 't' { return .LAST }
		if (ident[0] | 0x20) == 'l' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'f' && (ident[3] | 0x20) == 't' { return .LEFT }
		if (ident[0] | 0x20) == 'd' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 's' && (ident[3] | 0x20) == 'c' { return .DESC }
	}
	if len(ident) == 5 {
		if (ident[0] | 0x20) == 't' && (ident[1] | 0x20) == 'a' && (ident[2] | 0x20) == 'b' && (ident[3] | 0x20) == 'l' && (ident[4] | 0x20) == 'e' { return .TABLE }
		if (ident[0] | 0x20) == 'w' && (ident[1] | 0x20) == 'h' && (ident[2] | 0x20) == 'e' && (ident[3] | 0x20) == 'r' && (ident[4] | 0x20) == 'e' { return .WHERE }
		if (ident[0] | 0x20) == 'l' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 'm' && (ident[3] | 0x20) == 'i' && (ident[4] | 0x20) == 't' { return .LIMIT }
		if (ident[0] | 0x20) == 'g' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'o' && (ident[3] | 0x20) == 'u' && (ident[4] | 0x20) == 'p' { return .GROUP }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'd' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'r' { return .ORDER }
		if (ident[0] | 0x20) == 'c' && (ident[1] | 0x20) == 'h' && (ident[2] | 0x20) == 'e' && (ident[3] | 0x20) == 'c' && (ident[4] | 0x20) == 'k' { return .CHECK }
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 'n' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'r' { return .INNER }
		if (ident[0] | 0x20) == 'c' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'o' && (ident[3] | 0x20) == 's' && (ident[4] | 0x20) == 's' { return .CROSS }
		if (ident[0] | 0x20) == 'f' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 'r' && (ident[3] | 0x20) == 's' && (ident[4] | 0x20) == 't' { return .FIRST }
		if (ident[0] | 0x20) == 'r' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 'g' && (ident[3] | 0x20) == 'h' && (ident[4] | 0x20) == 't' { return .RIGHT }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'u' && (ident[2] | 0x20) == 't' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'r' { return .OUTER }
		if (ident[0] | 0x20) == 'b' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'g' && (ident[3] | 0x20) == 'i' && (ident[4] | 0x20) == 'n' { return .BEGIN }
		if (ident[0] | 0x20) == 'n' && (ident[1] | 0x20) == 'u' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'l' && (ident[4] | 0x20) == 's' { return .NULLS }
	}
	if len(ident) == 6 {
		if (ident[0] | 0x20) == 's' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'c' && (ident[5] | 0x20) == 't' { return .SELECT }
		if (ident[0] | 0x20) == 'd' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 't' && (ident[5] | 0x20) == 'e' { return .DELETE }
		if (ident[0] | 0x20) == 'u' && (ident[1] | 0x20) == 'p' && (ident[2] | 0x20) == 'd' && (ident[3] | 0x20) == 'a' && (ident[4] | 0x20) == 't' && (ident[5] | 0x20) == 'e' { return .UPDATE }
		if (ident[0] | 0x20) == 'c' && (ident[1] | 0x20) == 'o' && (ident[2] | 0x20) == 'm' && (ident[3] | 0x20) == 'm' && (ident[4] | 0x20) == 'i' && (ident[5] | 0x20) == 't' { return .COMMIT }
		if (ident[0] | 0x20) == 'c' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'e' && (ident[3] | 0x20) == 'a' && (ident[4] | 0x20) == 't' && (ident[5] | 0x20) == 'e' { return .CREATE }
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 's' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'r' && (ident[5] | 0x20) == 't' { return .INSERT }
		if (ident[0] | 0x20) == 'o' && (ident[1] | 0x20) == 'f' && (ident[2] | 0x20) == 'f' && (ident[3] | 0x20) == 's' && (ident[4] | 0x20) == 'e' && (ident[5] | 0x20) == 't' { return .OFFSET }
		if (ident[0] | 0x20) == 'h' && (ident[1] | 0x20) == 'a' && (ident[2] | 0x20) == 'v' && (ident[3] | 0x20) == 'i' && (ident[4] | 0x20) == 'n' && (ident[5] | 0x20) == 'g' { return .HAVING }
		if (ident[0] | 0x20) == 'v' && (ident[1] | 0x20) == 'a' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'u' && (ident[4] | 0x20) == 'e' && (ident[5] | 0x20) == 's' { return .VALUES }
	}
	if len(ident) == 7 {
		if (ident[0] | 0x20) == 'd' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'f' && (ident[3] | 0x20) == 'a' && (ident[4] | 0x20) == 'u' && (ident[5] | 0x20) == 'l' && (ident[6] | 0x20) == 't' { return .DEFAULT }
		if (ident[0] | 0x20) == 'p' && (ident[1] | 0x20) == 'r' && (ident[2] | 0x20) == 'i' && (ident[3] | 0x20) == 'm' && (ident[4] | 0x20) == 'a' && (ident[5] | 0x20) == 'r' && (ident[6] | 0x20) == 'y' { return .PRIMARY }
		if (ident[0] | 0x20) == 'i' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 't' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'g' && (ident[5] | 0x20) == 'e' && (ident[6] | 0x20) == 'r' { return .INTEGER }
		if (ident[0] | 0x20) == 'e' && (ident[1] | 0x20) == 'x' && (ident[2] | 0x20) == 'p' && (ident[3] | 0x20) == 'l' && (ident[4] | 0x20) == 'a' && (ident[5] | 0x20) == 'i' && (ident[6] | 0x20) == 'n' { return .EXPLAIN }
		if (ident[0] | 0x20) == 'f' && (ident[1] | 0x20) == 'o' && (ident[2] | 0x20) == 'r' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'i' && (ident[5] | 0x20) == 'g' && (ident[6] | 0x20) == 'n' { return .FOREIGN }
	}
	if len(ident) == 8 {
		if (ident[0] | 0x20) == 'd' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 's' && (ident[3] | 0x20) == 't' && (ident[4] | 0x20) == 'i' && (ident[5] | 0x20) == 'n' && (ident[6] | 0x20) == 'c' && (ident[7] | 0x20) == 't' { return .DISTINCT }
		if (ident[0] | 0x20) == 'r' && (ident[1] | 0x20) == 'o' && (ident[2] | 0x20) == 'l' && (ident[3] | 0x20) == 'l' && (ident[4] | 0x20) == 'b' && (ident[5] | 0x20) == 'a' && (ident[6] | 0x20) == 'c' && (ident[7] | 0x20) == 'k' { return .ROLLBACK }
		if (ident[0] | 0x20) == 's' && (ident[1] | 0x20) == 'n' && (ident[2] | 0x20) == 'a' && (ident[3] | 0x20) == 'p' && (ident[4] | 0x20) == 's' && (ident[5] | 0x20) == 'h' && (ident[6] | 0x20) == 'o' && (ident[7] | 0x20) == 't' { return .SNAPSHOT }
	}
	if len(ident) == 9 {
		if (ident[0] | 0x20) == 't' && (ident[1] | 0x20) == 'i' && (ident[2] | 0x20) == 'm' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 's' && (ident[5] | 0x20) == 't' && (ident[6] | 0x20) == 'a' && (ident[7] | 0x20) == 'm' && (ident[8] | 0x20) == 'p' { return .TIMESTAMP }
	}
	if len(ident) == 11 {
		if (ident[0] | 0x20) == 'r' && (ident[1] | 0x20) == 'e' && (ident[2] | 0x20) == 'f' && (ident[3] | 0x20) == 'e' && (ident[4] | 0x20) == 'r' && (ident[5] | 0x20) == 'e' && (ident[6] | 0x20) == 'n' && (ident[7] | 0x20) == 'c' && (ident[8] | 0x20) == 'e' && (ident[9] | 0x20) == 's' { return .REFERENCES }
	}
	return .IDENTIFIER
}

// Maps string literals to their corresponding keyword Token_Type.
get_keyword_type :: proc(ident: string) -> Token_Type {
	switch ident {
	case "CREATE": return .CREATE
	case "TABLE": return .TABLE
	case "INSERT": return .INSERT
	case "INTO": return .INTO
	case "VALUES": return .VALUES
	case "SELECT": return .SELECT
	case "DISTINCT": return .DISTINCT
	case "FROM": return .FROM
	case "WHERE": return .WHERE
	case "UPDATE": return .UPDATE
	case "SET": return .SET
	case "DELETE": return .DELETE
	case "DROP": return .DROP
	case "INT", "INTEGER": return .INTEGER
	case "TEXT": return .TEXT
	case "REAL": return .REAL
	case "BLOB": return .BLOB
	case "PRIMARY": return .PRIMARY
	case "KEY": return .KEY
	case "NOT": return .NOT
	case "NULL": return .NULL
	case "AND": return .AND
	case "OR": return .OR
	case "DEFAULT": return .DEFAULT
	case "LIKE": return .LIKE
	case "LIMIT": return .LIMIT
	case "OFFSET": return .OFFSET
	case "ORDER": return .ORDER
	case "BY": return .BY
	case "ASC": return .ASC
	case "DESC": return .DESC
	case "NULLS": return .NULLS
	case "FIRST": return .FIRST
	case "LAST": return .LAST
	case "GROUP": return .GROUP
	case "HAVING": return .HAVING
	case "JOIN": return .JOIN
	case "INNER": return .INNER
	case "CROSS": return .CROSS
	case "LEFT": return .LEFT
	case "RIGHT": return .RIGHT
	case "ON": return .ON
	case "AS": return .AS
	case "OUTER": return .OUTER
	case "BEGIN": return .BEGIN
	case "COMMIT": return .COMMIT
	case "ROLLBACK": return .ROLLBACK
	case "OF": return .OF
	case "SNAPSHOT": return .SNAPSHOT
	case "TIMESTAMP": return .TIMESTAMP
	case "EXPLAIN": return .EXPLAIN
	case "CHECK": return .CHECK
	case "IN": return .IN
	case "FOREIGN": return .FOREIGN
	case "REFERENCES": return .REFERENCES
	}
	return .IDENTIFIER
}

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
			for i < len(sql) && sql[i] != '\n' { i += 1 }
			continue
		}
		if c == '\'' {
			start := i + 1; i += 1
			for i < len(sql) {
				if sql[i] == '\'' {
					if i + 1 < len(sql) && sql[i + 1] == '\'' { i += 2; continue }
					break
				}
				i += 1
			}
			if i >= len(sql) { delete(tokens); return nil, false }
			append(&tokens, Token{.STRING, sql[start:i], line})
			i += 1; continue
		}
		if (c == 'X' || c == 'x') && i + 1 < len(sql) && sql[i + 1] == '\'' {
			start := i + 2; i += 2
			for i < len(sql) && sql[i] != '\'' { i += 1 }
			if i >= len(sql) { delete(tokens); return nil, false }
			append(&tokens, Token{.BLOB_LITERAL, sql[start:i], line})
			i += 1; continue
		}
		if unicode.is_digit(c) || (c == '-' && i + 1 < len(sql) && unicode.is_digit(rune(sql[i + 1]))) {
			start := i
			if c == '-' do i += 1
			has_dot := false
			for i < len(sql) {
				ch := sql[i]
				if unicode.is_digit(rune(ch)) { i += 1 }
				else if ch == '.' && !has_dot { has_dot = true; i += 1 }
				else { break }
			}
			append(&tokens, Token{.NUMBER, sql[start:i], line}); continue
		}
		if unicode.is_alpha(c) || c == '_' {
			start := i
			for i < len(sql) && (unicode.is_alpha(rune(sql[i])) || unicode.is_digit(rune(sql[i])) || sql[i] == '_') { i += 1 }
			token_type := match_keyword(sql[start:i])
			append(&tokens, Token{token_type, sql[start:i], line}); continue
		}
		switch c {
		case ',': append(&tokens, Token{.COMMA, ",", line}); i += 1
		case ';': append(&tokens, Token{.SEMICOLON, ";", line}); i += 1
		case '(': append(&tokens, Token{.LPAREN, "(", line}); i += 1
		case ')': append(&tokens, Token{.RPAREN, ")", line}); i += 1
		case '*': append(&tokens, Token{.ASTERISK, "*", line}); i += 1
		case '=': append(&tokens, Token{.EQUALS, "=", line}); i += 1
		case '<':
			if i + 1 < len(sql) && sql[i + 1] == '=' { append(&tokens, Token{.LESS_EQUAL, "<=", line}); i += 2 }
			else if i + 1 < len(sql) && sql[i + 1] == '>' { append(&tokens, Token{.NOT_EQUALS, "<>", line}); i += 2 }
			else { append(&tokens, Token{.LESS_THAN, "<", line}); i += 1 }
		case '>':
			if i + 1 < len(sql) && sql[i + 1] == '=' { append(&tokens, Token{.GREATER_EQUAL, ">=", line}); i += 2 }
			else { append(&tokens, Token{.GREATER_THAN, ">", line}); i += 1 }
		case '.': append(&tokens, Token{.DOT, ".", line}); i += 1
		case '!':
			if i + 1 < len(sql) && sql[i + 1] == '=' { append(&tokens, Token{.NOT_EQUALS, "!=", line}); i += 2 }
			else { delete(tokens); return nil, false }
		case: delete(tokens); return nil, false
		}
	}
	append(&tokens, Token{.EOF, "", line})
	return tokens[:], true
}

peek :: proc(p: ^Parser) -> Token {
	if p.current >= len(p.tokens) { return Token{.EOF, "", 0} }
	return p.tokens[p.current]
}

advance :: proc(p: ^Parser) -> Token {
	if p.current >= len(p.tokens) { return Token{.EOF, "", 0} }
	token := p.tokens[p.current]
	p.current += 1
	return token
}

match :: proc(p: ^Parser, types: ..Token_Type) -> bool {
	for t in types {
		if peek(p).type == t { advance(p); return true }
	}
	return false
}

expect :: proc(p: ^Parser, type: Token_Type) -> (Token, bool) {
	token := peek(p)
	if token.type != type { return token, false }
	advance(p)
	return token, true
}

is_keyword_token :: proc(t: Token_Type) -> bool {
	#partial switch t {
	case .EOF, .IDENTIFIER, .NUMBER, .STRING, .BLOB_LITERAL, .COMMA, .SEMICOLON, .LPAREN, .RPAREN, .ASTERISK, .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL, .DOT:
		return false
	}
	return true
}
