package parser

import "core:strconv"
import "core:strings"
import "src:types"

parse_identifier :: proc(p: ^Parser, allocator := context.allocator) -> (str: string, ok: bool) {
	tok := peek(p)
	if tok.type != .IDENTIFIER && !is_keyword_token(tok.type) { return {}, false }
	advance(p)
	return strings.clone(tok.lexeme, allocator), true
}

parse_qualified_identifier :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	str: string,
	ok: bool,
) {
	first := parse_identifier(p, allocator) or_return
	if match(p, .DOT) {
		second := parse_identifier(p, allocator) or_return
		result := strings.concatenate({first, ".", second}, allocator)
		delete(first, allocator); delete(second, allocator)
		return result, true
	}
	return first, true
}

parse_join_source :: proc(p: ^Parser, allocator := context.allocator) -> Join_Source_Result {
	if is_subquery_start(p) {
		advance(p); advance(p)
		inner_variant, sel_ok := parse_select(p, allocator)
		if !sel_ok { return {} }

		inner_sel, _ := inner_variant.(Select_Stmt)
		subq := new(Select_Stmt, allocator)
		subq^ = inner_sel
		if !match(p, .RPAREN) {
			free(subq, allocator); return {}
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
		if peek(p).type == .OF { p.current -= 1 } else {
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

	return Join_Clause {
			join_type = join_type,
			source = js.source,
			alias = js.alias,
			on_clause = on_cl,
		},
		true
}

parse_select_columns :: proc(
	p: ^Parser,
	columns: ^[dynamic]string,
	literal_values: ^[dynamic]types.Value,
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
					advance(p); advance(p)
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
				// Literal tokens (NUMBER/STRING/BLOB_LITERAL/NULL) are allowed in a
				// FROM-less SELECT; their values are captured for materialization.
				#partial switch tok.type {
				case .NUMBER, .STRING, .BLOB_LITERAL, .NULL:
					val, vok := parse_value(p, allocator)
					if !vok { return false }

					append(columns, strings.clone(tok.lexeme, allocator))
					append(literal_values, val)
				case:
					col, cok := parse_qualified_identifier(p, allocator)
					if !cok { return false }
					append(columns, col)
				}
			}
			if !match(p, .COMMA) { break }
		}
	}
	return true
}

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
			if !match(p, .JOIN) { break }
			jc, jc_ok := parse_single_join(p, allocator, .INNER, true)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .CROSS) {
			if !match(p, .JOIN) { break }
			jc, jc_ok := parse_single_join(p, allocator, .CROSS, false)
			if !jc_ok { break }
			append(&joins, jc)
		} else if match(p, .LEFT) {
			match(p, .OUTER)
			if !match(p, .JOIN) { break }
			jc, jc_ok := parse_single_join(p, allocator, .LEFT, true)
			if !jc_ok { break }
			append(&joins, jc)
		} else {
			break
		}
	}
	return joins
}

parse_select :: proc(
	p: ^Parser,
	allocator := context.allocator,
	consume_order_limit: bool = true,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	columns := make([dynamic]string, allocator)
	defer if !ok do delete(columns)

	literal_values := make([dynamic]types.Value, allocator)
	defer if !ok do delete(literal_values)

	aggregates := make([dynamic]Aggregate_Expr, allocator)
	defer if !ok do delete(aggregates)

	is_distinct := match(p, .DISTINCT)
	if !parse_select_columns(p, &columns, &literal_values, &aggregates, allocator) {
		return nil, false
	}

	// FROM is optional. A SELECT without FROM evaluates its columns as literals
	// (e.g. `SELECT 1, 'a'`) producing a single row.
	from_val: From_Source = No_From{}
	from_alias := ""
	joins: [dynamic]Join_Clause
	has_from := match(p, .FROM)
	if has_from {
		js := parse_join_source(p, allocator)
		if !js.success { return nil, false }

		from_val = js.source; from_alias = js.alias
		joins = parse_join_clauses(p, allocator)
	}
	defer if !ok do delete(joins)

	as_of_snapshot: Maybe(u64); as_of_timestamp: Maybe(u64)
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
	if match(p, .WHERE) { where_clause = parse_where_clause(p, allocator) or_return }

	group_by := make([dynamic]string, allocator)
	defer if !ok do delete(group_by)
	if match(p, .GROUP) {
		if !match(p, .BY) { return nil, false }
		for {
			append(&group_by, parse_qualified_identifier(p, allocator) or_return)
			if !match(p, .COMMA) { break }
		}
	}

	having_cl: Maybe(Where_Clause)
	if match(p, .HAVING) { having_cl = parse_where_clause(p, allocator) or_return }

	order_by: Maybe([]Order_By_Column)
	limit: Maybe(u64)
	offset: Maybe(u64)
	if consume_order_limit {
		order_by, limit, offset = parse_order_limit(p, allocator) or_return
	}
	return Select_Stmt {
			from = from_val,
			from_alias = from_alias,
			joins = joins[:],
			columns = columns[:],
			literal_values = literal_values[:],
			aggregates = aggregates[:],
			is_distinct = is_distinct,
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

// parse_order_limit parses a trailing `ORDER BY ... LIMIT n OFFSET m` clause.
// Used by both single SELECTs and compound (set-operation) statements.
parse_order_limit :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	order_by: Maybe([]Order_By_Column),
	limit: Maybe(u64),
	offset: Maybe(u64),
	ok: bool,
) {
	if match(p, .ORDER) {
		if !match(p, .BY) { return {}, {}, {}, false }
		order_cols := make([dynamic]Order_By_Column, allocator)
		defer if !ok do delete(order_cols)
		for {
			col := parse_qualified_identifier(p, allocator) or_return
			desc := false; nulls_first := false
			if match(p, .ASC) {  } else if match(p, .DESC) { desc = true }
			if match(p, .NULLS) {
				if match(p, .FIRST) { nulls_first = true } else { match(p, .LAST) }
			}

			append(
				&order_cols,
				Order_By_Column{column = col, desc = desc, nulls_first = nulls_first},
			)
			if !match(p, .COMMA) { break }
		}
		order_by = order_cols[:]
	}
	if match(p, .LIMIT) {
		limit_token := expect(p, .NUMBER) or_return
		lv, lu_ok := strconv.parse_u64(limit_token.lexeme)
		if !lu_ok {
			err(p, "LIMIT must be a non-negative integer")
			return {}, {}, {}, false
		}

		limit = lv
		if match(p, .OFFSET) {
			offset_token := expect(p, .NUMBER) or_return
			ov, ou_ok := strconv.parse_u64(offset_token.lexeme)
			if !ou_ok {
				err(p, "OFFSET must be a non-negative integer")
				return {}, {}, {}, false
			}
			offset = ov
		}
	}
	return order_by, limit, offset, true
}

// parse_set_op reads a single set-operation keyword, consuming `ALL` when present.
parse_set_op :: proc(p: ^Parser) -> (op: Set_Op, ok: bool) {
	if match(p, .UNION) {
		return .UNION_ALL if match(p, .ALL) else .UNION, true
	}
	if match(p, .INTERSECT) {
		return .INTERSECT_ALL if match(p, .ALL) else .INTERSECT, true
	}
	if match(p, .EXCEPT) {
		return .EXCEPT_ALL if match(p, .ALL) else .EXCEPT, true
	}
	return {}, false
}

// parse_compound_select parses `SELECT ... [UNION|INTERSECT|EXCEPT [ALL] SELECT ...]...`
// followed by an optional compound-level ORDER BY / LIMIT. Returns a Compound_Stmt
// when a set-operation follows the first SELECT, otherwise the plain Select_Stmt.
parse_compound_select :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	first_variant, first_ok := parse_select(p, allocator)
	if !first_ok { return nil, false }

	first_sel, _ := first_variant.(Select_Stmt)
	op, has_op := parse_set_op(p)
	if !has_op { return first_variant, true }

	first_ptr := new(Select_Stmt, allocator)
	first_ptr^ = first_sel
	operands := make([dynamic]Set_Operand, allocator)
	for {
		// Each operand begins with its own `SELECT` keyword (the dispatch consumed
		// only the first one).
		if !match(p, .SELECT) {
			free(first_ptr, allocator)
			return nil, false
		}

		sel_variant, sel_ok := parse_select(p, allocator, false)
		if !sel_ok {
			free(first_ptr, allocator)
			return nil, false
		}

		sel, _ := sel_variant.(Select_Stmt)
		sel_ptr := new(Select_Stmt, allocator)
		sel_ptr^ = sel

		append(&operands, Set_Operand{select = sel_ptr, op = op})
		next_op, has_next := parse_set_op(p)
		if !has_next { break }
		op = next_op
	}

	order_by, limit, offset, o_ok := parse_order_limit(p, allocator)
	if !o_ok {
		free(first_ptr, allocator)
		return nil, false
	}
	return Compound_Stmt {
			first    = first_ptr,
			operands = operands[:],
			order_by = order_by,
			limit    = limit,
			offset   = offset,
		},
		true
}
