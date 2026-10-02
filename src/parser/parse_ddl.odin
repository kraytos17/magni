package parser

import "core:strings"
import "src:types"

@(private)
parse_create_table :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .TABLE, "Expected TABLE after CREATE") do return nil, false

	table_name := parse_identifier(p, allocator) or_return
	if !expect_match(p, .LPAREN, "CREATE TABLE requires at least one column definition") {
		delete(table_name, allocator)
		return nil, false
	}

	fks := make([dynamic]Foreign_Key, allocator)
	columns := make([dynamic]types.Column, allocator)
	defer if !ok {
		for col in columns { delete(col.name, allocator) }

		delete(table_name, allocator)
		delete(columns)
		for fk in fks {
			delete(fk.col, allocator)
			delete(fk.ref_table, allocator)
			delete(fk.ref_col, allocator)
		}
		delete(fks)
	}
	for {
		if match(p, .FOREIGN) {
			fk, fk_ok := parse_foreign_key_clause(p, allocator)
			if !fk_ok { return nil, false }
			append(&fks, fk)
		} else {
			col, col_ok := parse_column_def(p, allocator)
			if !col_ok { return nil, false }
			append(&columns, col)
		}
		if match(
			p,
			.RPAREN,
		) { break } else if !expect_match(p, .COMMA, "Expected , or ) after column definition") { return nil, false }
	}
	return Create_Stmt{table_name = table_name, columns = columns[:], foreign_keys = fks[:]}, true
}

// parse_foreign_key_clause parses `FOREIGN KEY (col) REFERENCES table(col)`.
// The caller has already consumed FOREIGN.
@(private)
parse_foreign_key_clause :: proc(p: ^Parser, allocator := context.allocator) -> (fk: Foreign_Key, ok: bool) {
	if !expect_match(p, .KEY, "Expected KEY after FOREIGN") { return }
	if !expect_match(p, .LPAREN, "Expected ( after FOREIGN KEY") { return }

	fk_col := parse_identifier(p, allocator) or_return
	if !expect_match(p, .RPAREN, "Expected ) after foreign key column") { return }
	if !expect_match(p, .REFERENCES, "Expected REFERENCES after FOREIGN KEY") { return }

	fk_table := parse_identifier(p, allocator) or_return
	if !expect_match(p, .LPAREN, "Expected ( after REFERENCES table") { return }

	fk_ref_col := parse_identifier(p, allocator) or_return
	if !expect_match(p, .RPAREN, "Expected ) after referenced column") { return }
	return Foreign_Key{col = fk_col, ref_table = fk_table, ref_col = fk_ref_col}, true
}

// parse_column_modifier parses one trailing column modifier (PRIMARY KEY,
// NOT NULL, DEFAULT, CHECK) after the type. Returns handled=false when the
// next token starts no modifier (caller breaks); ok=false on error.
@(private="file")
parse_column_modifier :: proc(
	p: ^Parser,
	col: ^types.Column,
	allocator := context.allocator,
) -> (
	handled: bool,
	ok: bool,
) {
	if match(p, .PRIMARY) {
		if !expect_match(p, .KEY, "Expected KEY after PRIMARY") { return true, false }
		col.pk = true
		return true, true
	} else if match(p, .NOT) {
		if !expect_match(p, .NULL, "Expected NULL after NOT") { return true, false }
		col.not_null = true
		return true, true
	} else if match(p, .DEFAULT) {
		val, val_ok := parse_value(p, allocator)
		if !val_ok {
			err(p, "Invalid DEFAULT value")
			return true, false
	 	}

		col.default_value = val
		return true, true
	} else if match(p, .CHECK) {
		expr, expr_ok := collect_check_source(p, allocator)
		if !expr_ok { return true, false }

		col.check_expr = expr
		return true, true
	}
	return false, true
}

// parse_column_def parses one `name TYPE [modifiers]` column definition.
@(private)
parse_column_def :: proc(p: ^Parser, allocator := context.allocator) -> (col: types.Column, ok: bool) {
	col.name = parse_identifier(p, allocator) or_return
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
		err(p, "Expected column type (INTEGER, TEXT, REAL, or BLOB)")
		return {}, false
	}

	for {
		handled, mok := parse_column_modifier(p, &col, allocator)
		if !mok { return {}, false }
		if !handled { break }
	}
	return col, true
}

// collect_check_source captures the raw text of a parenthesised CHECK expression,
// tracking nesting depth. The caller has consumed CHECK.
@(private)
collect_check_source :: proc(p: ^Parser, allocator := context.allocator) -> (expr: string, ok: bool) {
	if !expect_match(p, .LPAREN, "Expected ( after CHECK") { return }

	b := strings.builder_make(allocator)
	depth := 1
	for depth > 0 {
		tok := peek(p); advance(p)
		if tok.type == .EOF {
			strings.builder_destroy(&b)
			err(p, "Expected ) in CHECK constraint")
			return "", false
		}
		if tok.type == .LPAREN { depth += 1 }
		if tok.type == .RPAREN { depth -= 1; if depth == 0 { break } }
		if strings.builder_len(b) > 0 { strings.write_byte(&b, ' ') }
		strings.write_string(&b, tok.lexeme)
	}
	return strings.to_string(b), true
}

@(private)
parse_drop_table :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .TABLE, "Expected TABLE after DROP") do return nil, false
	table_name := parse_identifier(p, allocator) or_return
	return Drop_Stmt{table_name = table_name}, true
}
