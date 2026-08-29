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
			if !expect_match(p, .KEY, "Expected KEY after FOREIGN") { return nil, false }
			if !expect_match(p, .LPAREN, "Expected ( after FOREIGN KEY") {
				return nil, false
			}

			fk_col := parse_identifier(p, allocator) or_return
			if !expect_match(
				p,
				.RPAREN,
				"Expected ) after foreign key column",
			) { return nil, false }
			if !expect_match(
				p,
				.REFERENCES,
				"Expected REFERENCES after FOREIGN KEY",
			) { return nil, false }

			fk_table := parse_identifier(p, allocator) or_return
			if !expect_match(p, .LPAREN, "Expected ( after REFERENCES table") {
				return nil, false
			}

			fk_ref_col := parse_identifier(p, allocator) or_return
			if !expect_match(
				p,
				.RPAREN,
				"Expected ) after referenced column",
			) { return nil, false }
			append(&fks, Foreign_Key{col = fk_col, ref_table = fk_table, ref_col = fk_ref_col})
		} else {
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
				return err(p, "Expected column type (INTEGER, TEXT, REAL, or BLOB)")
			}

			for {
				if match(p, .PRIMARY) {
					if !expect_match(p, .KEY, "Expected KEY after PRIMARY") {
						return nil, false
					}
					col.pk = true
				} else if match(p, .NOT) {
					if !expect_match(p, .NULL, "Expected NULL after NOT") {
						return nil, false
					}
					col.not_null = true
				} else if match(p, .DEFAULT) {
					val, val_ok := parse_value(p, allocator)
					if !val_ok { return err(p, "Invalid DEFAULT value") }
					col.default_value = val
				} else if match(p, .CHECK) {
					if !expect_match(p, .LPAREN, "Expected ( after CHECK") {
						return nil, false
					}

					b := strings.builder_make(allocator)
					depth := 1
					for depth > 0 {
						tok := peek(p); advance(p)
						if tok.type == .LPAREN { depth += 1 }
						if tok.type == .RPAREN { depth -= 1; if depth == 0 { break } }
						if strings.builder_len(b) > 0 { strings.write_byte(&b, ' ') }
						strings.write_string(&b, tok.lexeme)
					}
					col.check_expr = strings.to_string(b)
				} else { break }
			}
			append(&columns, col)
		}
		if match(
			p,
			.RPAREN,
		) { break } else if !expect_match(p, .COMMA, "Expected , or ) after column definition") { return nil, false }
	}
	return Create_Stmt{table_name = table_name, columns = columns[:], foreign_keys = fks[:]}, true
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
