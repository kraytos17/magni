package parser

import "core:mem"
import "core:strconv"
import "core:strings"
import "src:types"

@(private)
parse_identifier :: proc(p: ^Parser, allocator := context.allocator) -> (str: string, ok: bool) {
	tok := peek(p)
	if tok.type != .IDENTIFIER && !is_keyword_token(tok.type) { return {}, false }
	advance(p)
	return strings.clone(tok.lexeme, allocator), true
}

@(private)
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

@(private="file")
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

@(private="file")
is_subquery_start :: proc(p: ^Parser) -> bool {
	return(
		peek(p).type == .LPAREN &&
		p.current + 1 < len(p.tokens) &&
		p.tokens[p.current + 1].type == .SELECT \
	)
}

@(private="file")
is_alias :: proc(p: ^Parser) -> bool {
	return peek(p).type == .IDENTIFIER && peek(p).lexeme != "("
}

@(private="file")
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

@(private="file")
parse_select_columns :: proc(
	p: ^Parser,
	columns: ^[dynamic]string,
	aliases: ^[dynamic]string,
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
				agg_func, agg_ok := resolve_aggregate_name(tok.lexeme)
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
			consume_column_alias(p, aliases, allocator)
			if !match(p, .COMMA) { break }
		}
	}
	return true
}

// resolve_aggregate_name maps a function name (case-insensitive) to its
// aggregate func, or false if it is not a supported aggregate.
@(private="file")
resolve_aggregate_name :: proc(name: string) -> (Aggregate_Func, bool) {
	switch strings.to_upper(name, context.temp_allocator) {
	case "COUNT":
		return .COUNT, true
	case "SUM":
		return .SUM, true
	case "AVG":
		return .AVG, true
	case "MIN":
		return .MIN, true
	case "MAX":
		return .MAX, true
	}
	return .COUNT, false
}

// collect_having_aggregates walks a HAVING boolean tree and registers any
// aggregate-named leaf conditions (COUNT(*), SUM(v), ...) into `out` so the
// executor computes them. `out` holds select-list aggregates first; HAVING
// references are appended (deduped) and their column arg is cloned because
// both statement_free and condition_free own their strings.
@(private="file")
collect_having_aggregates :: proc(
	node: ^Where_Node,
	out: ^[dynamic]Aggregate_Expr,
	allocator: mem.Allocator,
) {
	if node == nil { return }
	switch node.kind {
	case .COND:
		agg_func, is_agg := resolve_aggregate_name(node.cond.column)
		if !is_agg { return }
		for agg in out {
			if agg.func == agg_func && agg.column == node.cond.agg_column {
				return
			}
		}

		append(out, Aggregate_Expr {
			func = agg_func,
			column = strings.clone(node.cond.agg_column, allocator),
		})
	case .AND, .OR, .NOT:
		for child in node.children {
			collect_having_aggregates(child, out, allocator)
		}
	}
}

// consume_column_alias appends the next column's alias entry ("" if none) and
// consumes an optional `AS <identifier>` or bare `<identifier>` alias. Clause
// words tokenize as keywords (not IDENTIFIER), so FROM/WHERE/GROUP/ORDER/COMMA
// can never be mistaken for a bare alias. `AS` in a SELECT column list is always
// an alias marker (AS OF appears only after the FROM clause).
@(private="file")
consume_column_alias :: proc(
	p: ^Parser,
	aliases: ^[dynamic]string,
	allocator: mem.Allocator,
) {
	append(aliases, "")
	if match(p, .AS) {
		if al, ok := parse_identifier(p, allocator); ok {
			aliases[len(aliases) - 1] = al
		}
		return
	}
	if is_alias(p) {
		if al, ok := parse_identifier(p, allocator); ok {
			aliases[len(aliases) - 1] = al
		}
	}
}

@(private="file")
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

@(private)
// MAX_PARSE_NESTING bounds recursive SELECT/subquery/set-op parsing. Each
// nesting level adds a parse_select frame; beyond ~1000 levels the native stack
// overflows, so cap well below that and reject the query with an error.
MAX_PARSE_NESTING :: 512

parse_select :: proc(
	p: ^Parser,
	allocator := context.allocator,
	consume_order_limit: bool = true,
) -> (
	stmt: Statement_Variant,
	ok: bool,
) {
	p.nest_depth += 1
	defer p.nest_depth -= 1
	if p.nest_depth > MAX_PARSE_NESTING {
		if p.err_msg == "" { p.err_msg = "Query nesting too deep" }
		return {}, false
	}

	columns := make([dynamic]string, allocator)
	defer if !ok do delete(columns)

	aliases := make([dynamic]string, allocator)
	defer if !ok do delete(aliases)

	literal_values := make([dynamic]types.Value, allocator)
	defer if !ok do delete(literal_values)

	aggregates := make([dynamic]Aggregate_Expr, allocator)
	defer if !ok do delete(aggregates)

	is_distinct := match(p, .DISTINCT)
	if !parse_select_columns(p, &columns, &aliases, &literal_values, &aggregates, allocator) {
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
	// Register aggregates referenced only by HAVING (e.g. `HAVING COUNT(*) >= 2`
	// with no aggregate in the SELECT list) so the executor computes them.
	if hc, has_h := having_cl.?; has_h {
		collect_having_aggregates(hc.root, &aggregates, allocator)
	}

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
			aliases = aliases[:],
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
@(private="file")
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
@(private="file")
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
@(private)
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
