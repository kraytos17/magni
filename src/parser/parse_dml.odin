package parser

import "src:types"

// parse_insert_column_list parses the optional (col, ...) list after the
// table name: present only when ( is followed by an identifier or an empty
// () — anything else (VALUES' paren) leaves the cursor untouched and the
// list empty (positional insert). A present-but-malformed list is an error,
// not a fallback to positional.
@(private = "file")
parse_insert_column_list :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	columns: [dynamic]string,
	ok: bool,
) {
	columns = make([dynamic]string, allocator)
	defer if !ok {
		for c in columns {
			delete(c, allocator)
		}
		delete(columns)
	}
	if peek(p).type == .LPAREN && p.current + 1 < len(p.tokens) {
		next_type := p.tokens[p.current + 1].type
		if next_type == .IDENTIFIER || next_type == .RPAREN {
			advance(p)
			for {
				col := parse_identifier(p, allocator) or_return; append(&columns, col)
				if match(p, .RPAREN) {
					break
				} else if !expect_match(p, .COMMA, "Expected , or ) after column") {
					return {}, false
				}
			}
		}
	}
	return columns, true
}

// parse_insert_value_row parses one parenthesized VALUES row (the opening
// paren is already consumed). Partial values are freed on failure.
@(private = "file")
parse_insert_value_row :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	values: []types.Value,
	ok: bool,
) {
	acc := make([dynamic]types.Value, allocator)
	defer if !ok {
		for v in acc {
			types.value_delete(v, allocator)
		}
		delete(acc)
	}
	for {
		val, val_ok := parse_value(p, allocator); if !val_ok {
			return nil, false
		}

		append(&acc, val)
		if match(p, .RPAREN) {
			break
		} else if !expect_match(p, .COMMA, "Expected , or ) after value") {
			return nil, false
		}
	}
	return acc[:], true
}

// parse_insert parses INSERT INTO t [(cols)] VALUES (row), (row)... after
// INSERT: one or more comma-separated value rows. Column count vs value
// count is an execution check (arity), not a parse error. Partial rows are
// freed on failure; the statement owns the survivors.
@(private)
parse_insert :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .INTO, "Expected INTO after INSERT") {
		return nil, false
	}

	table_name := parse_identifier(p, allocator) or_return
	columns, cok := parse_insert_column_list(p, allocator)
	if !cok {
		return nil, false
	}
	defer if !ok {
		for c in columns {
			delete(c, allocator)
		}
		delete(columns)
	}
	if !expect_match(p, .VALUES, "Expected VALUES after INSERT") ||
	   !expect_match(p, .LPAREN, "Expected ( after VALUES") {
		delete(table_name, allocator)
		return nil, false
	}

	rows := make([dynamic][]types.Value, allocator)
	defer if !ok {
		for r in rows {
			types.values_delete(r, allocator)
		}

		delete(table_name, allocator)
		delete(rows)
	}
	for {
		values, vok := parse_insert_value_row(p, allocator)
		if !vok {
			return err(p, "Invalid value in INSERT")
		}

		append(&rows, values)
		if !match(p, .COMMA) {
			break
		}
		if !expect_match(p, .LPAREN, "Expected ( after , for next VALUES row") {
			return nil, false
		}
	}
	return Insert_Stmt{table_name = table_name, columns = columns[:], values = rows[:]}, true
}

// parse_update parses UPDATE t SET col=val, ... [WHERE ...] after UPDATE.
// SET needs at least one assignment (the loop always runs once); the WHERE
// clause is optional (absent Maybe = update every row).
@(private)
parse_update :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	table_name := parse_identifier(p, allocator) or_return
	if !expect_match(p, .SET, "Expected SET after UPDATE table") {
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
		if !expect_match(p, .EQUALS, "Expected = after column in SET") {
			return nil, false
		}

		val, val_ok := parse_value(p, allocator)
		if !val_ok {
			return err(p, "Invalid value in SET")
		}

		append(&values, val)
		if !match(p, .COMMA) {
			break
		}
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

// parse_delete parses DELETE FROM t [WHERE ...] after DELETE. No RETURNING,
// no USING, no multi-table form — just the filter (absent Maybe = delete
// every row).
@(private)
parse_delete :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .FROM, "Expected FROM after DELETE") {
		return nil, false
	}

	table_name := parse_identifier(p, allocator) or_return
	defer if !ok {
		delete(table_name, allocator)
	}

	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) {
		where_cl = parse_where_clause(p, allocator) or_return
	}
	return Delete_Stmt{table_name = table_name, where_clause = where_cl}, true
}
