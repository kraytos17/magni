package parser

import "core:unicode"

Keyword_Entry :: struct {
	word: string,
	tok:  Token_Type,
}

// Keyword table for the SQL lexer. Entries are grouped by word length so the
// lookup can skip mismatched lengths without any per-identifier allocation.
keyword_table := []Keyword_Entry{
	// len 2
	{"in", .IN}, {"of", .OF}, {"on", .ON}, {"as", .AS}, {"by", .BY}, {"or", .OR},
	// len 3
	{"int", .INTEGER}, {"not", .NOT}, {"set", .SET}, {"key", .KEY}, {"and", .AND},
	{"asc", .ASC},
	// len 4
	{"from", .FROM}, {"into", .INTO}, {"join", .JOIN}, {"like", .LIKE}, {"null", .NULL},
	{"text", .TEXT}, {"blob", .BLOB}, {"real", .REAL}, {"drop", .DROP}, {"last", .LAST},
	{"left", .LEFT}, {"desc", .DESC},
	// len 5
	{"table", .TABLE}, {"where", .WHERE}, {"limit", .LIMIT}, {"group", .GROUP},
	{"order", .ORDER}, {"check", .CHECK}, {"inner", .INNER}, {"cross", .CROSS},
	{"first", .FIRST}, {"right", .RIGHT}, {"outer", .OUTER}, {"begin", .BEGIN},
	{"nulls", .NULLS},
	// len 6
	{"select", .SELECT}, {"delete", .DELETE}, {"update", .UPDATE}, {"create", .CREATE},
	{"insert", .INSERT}, {"offset", .OFFSET}, {"having", .HAVING}, {"values", .VALUES},
	// len 7
	{"default", .DEFAULT}, {"primary", .PRIMARY}, {"integer", .INTEGER},
	{"explain", .EXPLAIN}, {"foreign", .FOREIGN},
	// len 8
	{"distinct", .DISTINCT}, {"rollback", .ROLLBACK}, {"snapshot", .SNAPSHOT},
	// len 9
	{"timestamp", .TIMESTAMP},
	// len 11
	{"references", .REFERENCES},
}

// keyword_bucket_offsets[i] = start index into keyword_table for words of length i+2.
// The final value equals len(keyword_table); the bucket for length N spans
// keyword_table[offsets[N-2]:offsets[N-1]].
keyword_bucket_offsets := [10]int{0, 6, 12, 24, 37, 45, 50, 53, 54, 55}

match_keyword :: proc(ident: string) -> Token_Type {
	if len(ident) < 2 || len(ident) > 11 {
		return .IDENTIFIER
	}

	// Fold identifier to lowercase in a stack buffer (max keyword len is 11).
	folded: [12]u8
	for i in 0 ..< len(ident) {
		folded[i] = ident[i] | 0x20
	}

	// Linear scan within the matching-length bucket only.
	bi := len(ident) - 2
	start := keyword_bucket_offsets[bi]
	end := len(keyword_table) if bi == len(keyword_bucket_offsets) - 1 else keyword_bucket_offsets[bi + 1]
	for kw in keyword_table[start:end] {
		match := true
		for i in 0 ..< len(ident) {
			if folded[i] != kw.word[i] {
				match = false
				break
			}
		}
		if match { return kw.tok }
	}
	return .IDENTIFIER
}

is_hex_digit :: proc(c: rune) -> bool {
	return unicode.is_digit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

tokenize :: proc(sql: string, allocator := context.allocator) -> ([]Token, bool) {
	tokens := make([dynamic]Token, 0, len(sql) / 4, allocator)
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
		if c == '/' && i + 1 < len(sql) && sql[i + 1] == '*' {
			i += 2
			for i + 1 < len(sql) && !(sql[i] == '*' && sql[i + 1] == '/') { i += 1 }
			if i >= len(sql) { delete(tokens); return nil, false }
			i += 2; continue
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
		if unicode.is_digit(c) ||
		   (c == '-' && i + 1 < len(sql) && unicode.is_digit(rune(sql[i + 1]))) {
			start := i
			if c == '-' do i += 1
			// Hex literal: 0xFF, 0xDEAD
			if i + 1 < len(sql) && sql[i] == '0' && (sql[i + 1] | 0x20) == 'x' {
				i += 2
				for i < len(sql) && is_hex_digit(rune(sql[i])) { i += 1 }
				append(&tokens, Token{.NUMBER, sql[start:i], line}); continue
			}

			has_dot := false
			for i < len(sql) {
				ch := sql[i]
				if unicode.is_digit(rune(ch)) {
					i += 1
				} else if ch == '.' && !has_dot {
					has_dot = true
					i += 1
				} else if (ch == 'e' || ch == 'E') && i + 1 < len(sql) {
					i += 1
					if sql[i] == '+' || sql[i] == '-' { i += 1 }
					for i < len(sql) && unicode.is_digit(rune(sql[i])) { i += 1 }
					break
				} else {
					break
				}
			}
			append(&tokens, Token{.NUMBER, sql[start:i], line}); continue
		}
		if unicode.is_alpha(c) || c == '_' {
			start := i
			for i < len(sql) &&
			    (unicode.is_alpha(rune(sql[i])) ||
					    unicode.is_digit(rune(sql[i])) ||
					    sql[i] == '_') { i += 1 }

			token_type := match_keyword(sql[start:i])
			append(&tokens, Token{token_type, sql[start:i], line}); continue
		}

		switch c {
		case ',':
			append(&tokens, Token{.COMMA, ",", line}); i += 1
		case ';':
			append(&tokens, Token{.SEMICOLON, ";", line}); i += 1
		case '(':
			append(&tokens, Token{.LPAREN, "(", line}); i += 1
		case ')':
			append(&tokens, Token{.RPAREN, ")", line}); i += 1
		case '*':
			append(&tokens, Token{.ASTERISK, "*", line}); i += 1
		case '=':
			append(&tokens, Token{.EQUALS, "=", line}); i += 1
		case '<':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.LESS_EQUAL, "<=", line}); i += 2
			} else if i + 1 < len(sql) && sql[i + 1] == '>' {
				append(&tokens, Token{.NOT_EQUALS, "<>", line}); i += 2
			} else {
				append(&tokens, Token{.LESS_THAN, "<", line}); i += 1
			}
		case '>':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.GREATER_EQUAL, ">=", line}); i += 2
			} else {
				append(&tokens, Token{.GREATER_THAN, ">", line}); i += 1
			}
		case '.':
			append(&tokens, Token{.DOT, ".", line}); i += 1
		case '!':
			if i + 1 < len(sql) && sql[i + 1] == '=' {
				append(&tokens, Token{.NOT_EQUALS, "!=", line}); i += 2
			} else {
				delete(tokens); return nil, false
			}
		case:
			delete(tokens); return nil, false
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
