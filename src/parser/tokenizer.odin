package parser

Keyword_Entry :: struct {
	word: string,
	tok : Token_Type,
}

// Keyword table for the SQL lexer. Entries are grouped by word length so the
// lookup can skip mismatched lengths without any per-identifier allocation.
keyword_table := []Keyword_Entry {
	// len 2
	{"in", .IN},
	{"of", .OF},
	{"on", .ON},
	{"as", .AS},
	{"by", .BY},
	{"or", .OR},
	{"is", .IS},
	// len 3
	{"int", .INTEGER},
	{"not", .NOT},
	{"set", .SET},
	{"key", .KEY},
	{"and", .AND},
	{"asc", .ASC},
	{"all", .ALL},
	// len 4
	{"from", .FROM},
	{"into", .INTO},
	{"join", .JOIN},
	{"like", .LIKE},
	{"null", .NULL},
	{"text", .TEXT},
	{"blob", .BLOB},
	{"real", .REAL},
	{"drop", .DROP},
	{"last", .LAST},
	{"left", .LEFT},
	{"desc", .DESC},
	// len 5
	{"table", .TABLE},
	{"where", .WHERE},
	{"limit", .LIMIT},
	{"group", .GROUP},
	{"order", .ORDER},
	{"check", .CHECK},
	{"inner", .INNER},
	{"cross", .CROSS},
	{"first", .FIRST},
	{"right", .RIGHT},
	{"outer", .OUTER},
	{"begin", .BEGIN},
	{"nulls", .NULLS},
	{"union", .UNION},
	{"using", .USING},
	// len 6
	{"select", .SELECT},
	{"delete", .DELETE},
	{"update", .UPDATE},
	{"create", .CREATE},
	{"insert", .INSERT},
	{"offset", .OFFSET},
	{"having", .HAVING},
	{"values", .VALUES},
	{"except", .EXCEPT},
	{"commit", .COMMIT},
	// len 7
	{"default", .DEFAULT},
	{"primary", .PRIMARY},
	{"integer", .INTEGER},
	{"explain", .EXPLAIN},
	{"foreign", .FOREIGN},
	{"between", .BETWEEN},
	// len 8
	{"distinct", .DISTINCT},
	{"rollback", .ROLLBACK},
	{"snapshot", .SNAPSHOT},
	// len 9
	{"timestamp", .TIMESTAMP},
	{"intersect", .INTERSECT},
	// len 10
	{"references", .REFERENCES},
}

// keyword_bucket_offsets[i] = start index into keyword_table for words of length i+2.
// The final value equals len(keyword_table); the bucket for length N spans
// keyword_table[offsets[N-2]:offsets[N-1]].
keyword_bucket_offsets := [10]int{0, 7, 14, 26, 41, 51, 57, 60, 62, 63}

@(private = "file")
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
	end :=
		len(keyword_table) if bi == len(keyword_bucket_offsets) - 1 else keyword_bucket_offsets[bi + 1]
	for kw in keyword_table[start:end] {
		if len(kw.word) != len(ident) { continue }

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

@(private = "file")
is_hex_digit :: proc(c: byte) -> bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// ASCII byte predicates for the lexer hot loop. SQL lexing is an ASCII
// problem — these avoid the Unicode table lookups in core:unicode per byte.
@(private = "file")
is_space_byte :: proc(c: byte) -> bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

@(private = "file")
is_digit_byte :: proc(c: byte) -> bool {
	return c >= '0' && c <= '9'
}

@(private = "file")
is_alpha_byte :: proc(c: byte) -> bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// Lexer holds the scanner cursor over the SQL text. The per-class lex_*
// scanners below advance it; tokenize dispatches on the leading byte.
Lexer :: struct {
	sql : string,
	pos : int,
	line: u32,
}

// lex_line_comment consumes a `--` comment to (not past) the newline.
@(private = "file")
lex_line_comment :: proc(l: ^Lexer) {
	for l.pos < len(l.sql) && l.sql[l.pos] != '\n' { l.pos += 1 }
}

// lex_block_comment consumes a `/* ... */` comment. False on unterminated.
@(private = "file")
lex_block_comment :: proc(l: ^Lexer) -> bool {
	l.pos += 2
	for l.pos + 1 < len(l.sql) && !(l.sql[l.pos] == '*' && l.sql[l.pos + 1] == '/') {
		if l.sql[l.pos] == '\n' { l.line += 1 }
		l.pos += 1
	}
	if l.pos + 1 >= len(l.sql) {
		return false
	}

	l.pos += 2
	return true
}

// lex_string consumes a quoted string with '' escapes. False on unterminated.
@(private = "file")
lex_string :: proc(l: ^Lexer, tokens: ^[dynamic]Token) -> bool {
	start := l.pos + 1; l.pos += 1; token_line := l.line
	for l.pos < len(l.sql) {
		if l.sql[l.pos] == '\'' {
			if l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '\'' {
				l.pos += 2
				continue
			}
			break
		}
		if l.sql[l.pos] == '\n' { l.line += 1 }
		l.pos += 1
	}
	if l.pos >= len(l.sql) { return false }

	append(tokens, Token{.STRING, l.sql[start:l.pos], token_line})
	l.pos += 1
	return true
}

// lex_blob consumes an X'...' hex literal with even digit count.
// False on unterminated, odd-length, or non-hex content.
@(private = "file")
lex_blob :: proc(l: ^Lexer, tokens: ^[dynamic]Token) -> bool {
	start := l.pos + 2; l.pos += 2; token_line := l.line
	for l.pos < len(l.sql) && l.sql[l.pos] != '\'' {
		if l.sql[l.pos] == '\n' { l.line += 1 }
		l.pos += 1
	}
	if l.pos >= len(l.sql) { return false }

	hex_len := l.pos - start
	if hex_len % 2 != 0 { return false }
	for j in start ..< l.pos {
		if !is_hex_digit(l.sql[j]) { return false }
	}

	append(tokens, Token{.BLOB_LITERAL, l.sql[start:l.pos], token_line})
	l.pos += 1
	return true
}

// lex_number consumes decimal, 0x hex, float, and exponent forms, including
// a leading minus. False on a bare 0x with no hex digits.
@(private = "file")
lex_number :: proc(l: ^Lexer, tokens: ^[dynamic]Token) -> bool {
	start := l.pos
	if l.sql[l.pos] == '-' { l.pos += 1 }
	if l.pos + 1 < len(l.sql) && l.sql[l.pos] == '0' && (l.sql[l.pos + 1] | 0x20) == 'x' {
		l.pos += 2
		if l.pos >= len(l.sql) || !is_hex_digit(l.sql[l.pos]) {
			return false
		}
		for l.pos < len(l.sql) && is_hex_digit(l.sql[l.pos]) { l.pos += 1 }

		append(tokens, Token{.NUMBER, l.sql[start:l.pos], l.line})
		return true
	}

	has_dot := false
	for l.pos < len(l.sql) {
		ch := l.sql[l.pos]
		if is_digit_byte(ch) {
			l.pos += 1
		} else if ch == '.' && !has_dot {
			has_dot = true
			l.pos += 1
		} else if (ch == 'e' || ch == 'E') && l.pos + 1 < len(l.sql) {
			ep := l.pos + 1
			if l.sql[ep] == '+' || l.sql[ep] == '-' {
				if ep + 1 < len(l.sql) && is_digit_byte(l.sql[ep + 1]) {
					l.pos = ep + 2
				} else {
					break
				}
			} else if is_digit_byte(l.sql[ep]) {
				l.pos = ep + 1
			} else {
				break
			}

			for l.pos < len(l.sql) && is_digit_byte(l.sql[l.pos]) { l.pos += 1 }
			break
		} else {
			break
		}
	}

	append(tokens, Token{.NUMBER, l.sql[start:l.pos], l.line})
	return true
}

// lex_ident consumes an identifier/keyword word and classifies it.
@(private = "file")
lex_ident :: proc(l: ^Lexer, tokens: ^[dynamic]Token) {
	start := l.pos
	for l.pos < len(l.sql) &&
	    (is_alpha_byte(l.sql[l.pos]) ||
			    is_digit_byte(l.sql[l.pos]) ||
			    l.sql[l.pos] == '_') { l.pos += 1 }

	token_type := match_keyword(l.sql[start:l.pos])
	append(tokens, Token{token_type, l.sql[start:l.pos], l.line})
}

// lex_symbol consumes one operator/punctuation token. False on `!` alone
// and any other unrecognized byte.
@(private = "file")
lex_symbol :: proc(l: ^Lexer, tokens: ^[dynamic]Token) -> bool {
	c := l.sql[l.pos]
	switch c {
	case ',':
		append(tokens, Token{.COMMA, ",", l.line}); l.pos += 1
	case ';':
		append(tokens, Token{.SEMICOLON, ";", l.line}); l.pos += 1
	case '(':
		append(tokens, Token{.LPAREN, "(", l.line}); l.pos += 1
	case ')':
		append(tokens, Token{.RPAREN, ")", l.line}); l.pos += 1
	case '*':
		append(tokens, Token{.ASTERISK, "*", l.line}); l.pos += 1
	case '=':
		append(tokens, Token{.EQUALS, "=", l.line}); l.pos += 1
	case '<':
		if l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '=' {
			append(tokens, Token{.LESS_EQUAL, "<=", l.line}); l.pos += 2
		} else if l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '>' {
			append(tokens, Token{.NOT_EQUALS, "<>", l.line}); l.pos += 2
		} else {
			append(tokens, Token{.LESS_THAN, "<", l.line}); l.pos += 1
		}
	case '>':
		if l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '=' {
			append(tokens, Token{.GREATER_EQUAL, ">=", l.line}); l.pos += 2
		} else {
			append(tokens, Token{.GREATER_THAN, ">", l.line}); l.pos += 1
		}
	case '.':
		append(tokens, Token{.DOT, ".", l.line}); l.pos += 1
	case '!':
		if l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '=' {
			append(tokens, Token{.NOT_EQUALS, "!=", l.line}); l.pos += 2
		} else {
			return false
		}
	case:
		return false
	}
	return true
}

tokenize :: proc(sql: string, allocator := context.allocator) -> ([]Token, bool) {
	tokens := make([dynamic]Token, 0, len(sql) / 4, allocator)
	l := Lexer {
		sql  = sql,
		line = 1,
	}
	for l.pos < len(l.sql) {
		c := l.sql[l.pos]
		if is_space_byte(c) {
			if c == '\n' { l.line += 1 }
			l.pos += 1
			continue
		}
		if c == '-' && l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '-' {
			lex_line_comment(&l)
			continue
		}
		if c == '/' && l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '*' {
			if !lex_block_comment(&l) { delete(tokens); return nil, false }
			continue
		}
		if c == '\'' {
			if !lex_string(&l, &tokens) { delete(tokens); return nil, false }
			continue
		}
		if (c == 'X' || c == 'x') && l.pos + 1 < len(l.sql) && l.sql[l.pos + 1] == '\'' {
			if !lex_blob(&l, &tokens) { delete(tokens); return nil, false }
			continue
		}
		if is_digit_byte(c) ||
		   (c == '-' && l.pos + 1 < len(l.sql) && is_digit_byte(l.sql[l.pos + 1])) {
			if !lex_number(&l, &tokens) { delete(tokens); return nil, false }
			continue
		}
		if is_alpha_byte(c) || c == '_' {
			lex_ident(&l, &tokens)
			continue
		}
		if !lex_symbol(&l, &tokens) { delete(tokens); return nil, false }
	}

	append(&tokens, Token{.EOF, "", l.line})
	return tokens[:], true
}

@(private)
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

@(private)
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
