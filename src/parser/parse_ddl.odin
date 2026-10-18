package parser

import "core:strings"
import "src:types"

// parse_create_table parses CREATE TABLE name (defs...) after CREATE: column
// definitions interleaved with table-level FOREIGN KEY and UNIQUE clauses,
// comma separated. Single-column UNIQUE groups fold into Column.unique;
// multi-column groups resolve to indices. On failure the deferred cleanup
// frees the partial names/rows (arena abandonment covers the rest).
@(private)
parse_create_table :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .TABLE, "Expected TABLE after CREATE") {
		return nil, false
	}

	table_name := parse_identifier(p, allocator) or_return
	if !expect_match(p, .LPAREN, "CREATE TABLE requires at least one column definition") {
		delete(table_name, allocator)
		return nil, false
	}

	fks := make([dynamic]Foreign_Key, allocator)
	columns := make([dynamic]types.Column, allocator)
	ugroups := make([dynamic][]string, allocator)
	defer if !ok {
		for col in columns {
			delete(col.name, allocator)
		}

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
			if !fk_ok {
				return nil, false
			}
			append(&fks, fk)
		} else if match(p, .UNIQUE) {
			ug, ug_ok := parse_unique_clause(p, allocator)
			if !ug_ok {
				return nil, false
			}
			append(&ugroups, ug)
		} else {
			col, col_ok := parse_column_def(p, &fks, allocator)
			if !col_ok {
				return nil, false
			}
			append(&columns, col)
		}
		if match(p, .RPAREN) {
			break
		} else if !expect_match(p, .COMMA, "Expected , or ) after column definition") {
			return nil, false
		}
	}

	groups, g_ok := resolve_unique_groups(p, &columns, ugroups, allocator)
	if !g_ok {
		return nil, false
	}
	return Create_Stmt {
			table_name = table_name,
			columns = columns[:],
			foreign_keys = fks[:],
			unique_groups = groups,
		},
		true
}

// parse_create_index parses `CREATE INDEX name ON table (column)` —
// single-column only (one identifier; the executor additionally requires a
// TEXT column). The caller consumed CREATE INDEX. Same arena-ownership +
// fail-cleanup discipline as parse_create_table: partial nodes die with
// the arena on failure.
@(private)
parse_create_index :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	index_name := parse_identifier(p, allocator) or_return
	defer if !ok {
		delete(index_name, allocator)
	}
	if !expect_match(p, .ON, "Expected ON after index name") {
		return nil, false
	}

	table_name := parse_identifier(p, allocator) or_return
	defer if !ok {
		delete(table_name, allocator)
	}
	if !expect_match(p, .LPAREN, "Expected ( after table name") {
		return nil, false
	}

	column := parse_identifier(p, allocator) or_return
	defer if !ok {
		delete(column, allocator)
	}
	if !expect_match(p, .RPAREN, "Expected ) after column name") {
		return nil, false
	}
	return Create_Index_Stmt{index_name = index_name, table_name = table_name, column = column},
		true
}

// parse_foreign_key_clause parses `FOREIGN KEY (col) REFERENCES table(col)`.
// The caller has already consumed FOREIGN.
@(private)
parse_foreign_key_clause :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	fk: Foreign_Key,
	ok: bool,
) {
	if !expect_match(p, .KEY, "Expected KEY after FOREIGN") {
		return
	}
	if !expect_match(p, .LPAREN, "Expected ( after FOREIGN KEY") {
		return
	}

	fk_col := parse_identifier(p, allocator) or_return
	if !expect_match(p, .RPAREN, "Expected ) after foreign key column") {
		return
	}
	if !expect_match(p, .REFERENCES, "Expected REFERENCES after FOREIGN KEY") {
		return
	}

	fk_table := parse_identifier(p, allocator) or_return
	if !expect_match(p, .LPAREN, "Expected ( after REFERENCES table") {
		return
	}

	fk_ref_col := parse_identifier(p, allocator) or_return
	if !expect_match(p, .RPAREN, "Expected ) after referenced column") {
		return
	}
	return Foreign_Key{col = fk_col, ref_table = fk_table, ref_col = fk_ref_col}, true
}

// parse_unique_clause parses `(a, b, ...)` after UNIQUE: one or more owned
// column-name strings. The caller owns the names (and the array) on success;
// on failure partial names are freed here.
@(private = "file")
parse_unique_clause :: proc(p: ^Parser, allocator := context.allocator) -> ([]string, bool) {
	if !expect_match(p, .LPAREN, "Expected ( after UNIQUE") {
		return nil, false
	}

	members := make([dynamic]string, allocator)
	for {
		name, n_ok := parse_identifier(p, allocator)
		if !n_ok {
			for m in members {
				delete(m, allocator)
			}

			delete(members)
			return nil, false
		}

		append(&members, name)
		if match(p, .RPAREN) {
			break
		}
		if !expect_match(p, .COMMA, "Expected , or ) in UNIQUE column list") {
			for m in members {
				delete(m, allocator)
			}

			delete(members)
			return nil, false
		}
	}
	return members[:], true
}

// resolve_unique_groups turns parsed UNIQUE name lists into resolved column
// index lists against the complete columns. Single-column groups fold into
// Column.unique; the returned lists hold multi-column groups only. Consumes
// ugroups fully on both paths (names and array freed here); partial index
// arrays are freed on failure with err set. Unknown members fail the CREATE.
@(private = "file")
resolve_unique_groups :: proc(
	p: ^Parser,
	columns: ^[dynamic]types.Column,
	ugroups: [dynamic][]string,
	allocator := context.allocator,
) -> (
	[][]int,
	bool,
) {
	free_names := proc(ugs: [dynamic][]string, allocator := context.allocator) {
		for g in ugs {
			for name in g {
				delete(name, allocator)
			}
			delete(g, allocator)
		}
		delete(ugs)
	}

	resolved := make([dynamic][]int, 0, len(ugroups), allocator)
	for g in ugroups {
		idxs := make([dynamic]int, 0, len(g), allocator)
		ok := true
		for name in g {
			found := -1
			for k in 0 ..< len(columns) {
				if columns[k].name == name {
					found = k
					break
				}
			}
			if found < 0 {
				err(p, "Unknown column in UNIQUE constraint")
				ok = false
				break
			}

			seen := false
			for e in idxs {
				if e == found {
					seen = true
					break
				}
			}
			if !seen {
				append(&idxs, found)
			}
		}
		if !ok {
			delete(idxs)
			for r in resolved {
				delete(r)
			}

			delete(resolved)
			free_names(ugroups, allocator)
			return nil, false
		}
		if len(idxs) == 1 {
			columns[idxs[0]].unique = true
			delete(idxs)
		} else {
			append(&resolved, idxs[:])
		}
	}

	free_names(ugroups, allocator)
	return resolved[:], true
}

// parse_column_modifier parses one trailing column modifier (PRIMARY KEY,
// UNIQUE, NOT NULL, DEFAULT, CHECK, REFERENCES) after the type. Returns
// handled=false when the next token starts no modifier (caller breaks);
// ok=false on error. REFERENCES appends a table-level Foreign_Key for the
// column being defined.
@(private = "file")
parse_column_modifier :: proc(
	p: ^Parser,
	col: ^types.Column,
	fks: ^[dynamic]Foreign_Key,
	allocator := context.allocator,
) -> (
	handled: bool,
	ok: bool,
) {
	if match(p, .PRIMARY) {
		if !expect_match(p, .KEY, "Expected KEY after PRIMARY") {
			return true, false
		}

		col.pk = true
		return true, true
	} else if match(p, .UNIQUE) {
		col.unique = true
		return true, true
	} else if match(p, .NOT) {
		if !expect_match(p, .NULL, "Expected NULL after NOT") {
			return true, false
		}

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
		if !expr_ok {
			return true, false
		}

		col.check_expr = expr
		return true, true
	} else if match(p, .REFERENCES) {
		ref_table, rt_ok := parse_identifier(p, allocator)
		if !rt_ok {
			return true, false
		}
		if !expect_match(p, .LPAREN, "Expected ( after REFERENCES table") {
			delete(ref_table, allocator)
			return true, false
		}

		ref_col, rc_ok := parse_identifier(p, allocator)
		if !rc_ok {
			delete(ref_table, allocator)
			return true, false
		}
		if !expect_match(p, .RPAREN, "Expected ) after referenced column") {
			delete(ref_table, allocator)
			delete(ref_col, allocator)
			return true, false
		}

		append(
			fks,
			Foreign_Key {
				col = strings.clone(col.name, allocator),
				ref_table = ref_table,
				ref_col = ref_col,
			},
		)
		return true, true
	}
	return false, true
}

// parse_column_def parses one `name TYPE [modifiers]` column definition.
// REFERENCES modifiers append to `fks` (table-level FK list).
@(private)
parse_column_def :: proc(
	p: ^Parser,
	fks: ^[dynamic]Foreign_Key,
	allocator := context.allocator,
) -> (
	col: types.Column,
	ok: bool,
) {
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
		handled, mok := parse_column_modifier(p, &col, fks, allocator)
		if !mok {
			return {}, false
		}
		if !handled {
			break
		}
	}
	return col, true
}

// collect_check_source captures the raw text of a parenthesised CHECK expression,
// tracking nesting depth. The caller has consumed CHECK.
@(private)
collect_check_source :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	expr: string,
	ok: bool,
) {
	if !expect_match(p, .LPAREN, "Expected ( after CHECK") {
		return
	}

	b := strings.builder_make(allocator)
	depth := 1
	for depth > 0 {
		tok := peek(p); advance(p)
		if tok.type == .EOF {
			strings.builder_destroy(&b)
			err(p, "Expected ) in CHECK constraint")
			return "", false
		}
		if tok.type == .LPAREN {
			depth += 1
		}
		if tok.type == .RPAREN {
			depth -= 1
			if depth == 0 {
				break
			}
		}
		if strings.builder_len(b) > 0 {
			strings.write_byte(&b, ' ')
		}
		strings.write_string(&b, tok.lexeme)
	}
	return strings.to_string(b), true
}

// parse_drop_table parses DROP TABLE name after DROP. Existence is an
// execution concern (absent tables error there, not here).
@(private)
parse_drop_table :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	if !expect_match(p, .TABLE, "Expected TABLE after DROP") {
		return nil, false
	}

	table_name := parse_identifier(p, allocator) or_return
	return Drop_Stmt{table_name = table_name}, true
}

// parse_drop_index parses `DROP INDEX name [ON table]` — same
// arena-ownership as parse_drop_table. Existence resolves at execution
// against stored index names.
@(private)
parse_drop_index :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	index_name := parse_identifier(p, allocator) or_return
	defer if !ok {
		delete(index_name, allocator)
	}
	if match(p, .ON) {
		table_name := parse_identifier(p, allocator) or_return
		defer if !ok {
			delete(table_name, allocator)
		}
		return Drop_Index_Stmt{index_name = index_name, table_name = table_name}, true
	}
	return Drop_Index_Stmt{index_name = index_name}, true
}
