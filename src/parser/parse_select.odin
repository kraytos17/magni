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

		has_as := match(p, .AS)
		// A subquery source may omit the alias. Without AS, only a plain
		// identifier counts — a following keyword (WHERE, GROUP, ...) starts
		// the next clause and must not be eaten as an alias. With AS, keep
		// the historical permissive parse (keywords are valid aliases).
		if has_as || is_alias(p) {
			al, al_ok := parse_identifier(p, allocator)
			if !al_ok {
				free(subq, allocator)
				return {}
			}
			return {source = subq, alias = al, success = true}
		}
		return {source = subq, alias = "", success = true}
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
	return {source = tbl, alias = "", success = true}
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
	left_alias: string = "",
) -> (
	jc: Join_Clause,
	ok: bool,
) {
	js := parse_join_source(p, allocator)
	if !js.success { return {}, false }

	right_alias := js.alias
	if right_alias == "" {
		if tbl, is_tbl := js.source.(string); is_tbl { right_alias = tbl }
	}

	parse_using := proc(p: ^Parser, left: string, right: string, alloc: mem.Allocator) -> (cl: Where_Clause, ok: bool) {
		if !match(p, .LPAREN) { return {}, false }

		cols := make([dynamic]string, alloc)
		for {
			col, col_ok := parse_identifier(p, alloc)
			if !col_ok { return {}, false }

			append(&cols, col)
			if match(p, .RPAREN) { break }
			if !match(p, .COMMA) { return {}, false }
		}
		if len(cols) == 0 { return {}, false }

		root: ^Where_Node = nil
		for i in 0 ..< len(cols) {
			c := cols[i]
			left_col := strings.concatenate({left, ".", c}, alloc)
			right_col := strings.concatenate({right, ".", c}, alloc)
			cond := Condition { column = left_col, operator = .EQUALS, rhs = right_col }
			node := new(Where_Node, alloc)
			node^ = Where_Node{kind = .COND, cond = cond}
			if i == 0 {
				root = node
			} else {
				children := make([dynamic]^Where_Node, alloc)
				append(&children, root)
				append(&children, node)

				wrapper := new(Where_Node, alloc)
				wrapper^ = Where_Node{kind = .AND, children = children}
				root = wrapper
			}
		}
		for c in cols { delete(c, alloc) }

		delete(cols)
		return Where_Clause{root = root}, true
	}

	on_cl: Maybe(Where_Clause)
	if on_required {
		if match(p, .ON) {
			on_cl, ok = parse_where_clause(p, allocator)
			if !ok { return {}, false }
		} else if match(p, .USING) {
			on_cl, ok = parse_using(p, left_alias, right_alias, allocator)
			if !ok { return {}, false }
		} else {
			return {}, false
		}
	} else if match(p, .ON) {
		on_cl, ok = parse_where_clause(p, allocator)
		if !ok { return {}, false }
	} else if match(p, .USING) {
		on_cl, ok = parse_using(p, left_alias, right_alias, allocator)
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
	b: ^Select_Builder,
	allocator := context.allocator,
) -> bool {
	if match(p, .ASTERISK) { return true }
	for {
		tok := peek(p)
		if tok.type == .IDENTIFIER &&
		   p.current + 1 < len(p.tokens) &&
		   p.tokens[p.current + 1].type == .LPAREN {
			if !parse_column_or_aggregate(p, b, tok, allocator) { return false }
		} else {
			if !parse_column_or_literal(p, b, tok, allocator) { return false }
		}

		consume_column_alias(p, &b.aliases, allocator)
		if !match(p, .COMMA) { break }
	}
	return true
}

// parse_column_or_aggregate handles a `name(` token: an aggregate when the name
// resolves (COUNT/SUM/...), otherwise a bare column identifier.
// builder_emit_column appends one projected column to the builder's parallel
// arrays (columns/col_kinds/col_literal_idx stay in lockstep through this
// single choke point). For LITERAL slots the caller appends to literal_values
// first and passes its index.
@(private="file")
builder_emit_column :: proc(
	b: ^Select_Builder,
	display: string,
	kind: Select_Column_Kind,
	lit_idx: int,
) {
	append(&b.columns, display)
	append(&b.col_kinds, kind)
	append(&b.col_literal_idx, lit_idx)
}

@(private="file")
parse_column_or_aggregate :: proc(
	p: ^Parser,
	b: ^Select_Builder,
	tok: Token,
	allocator: mem.Allocator,
) -> bool {
	agg_func, agg_ok := resolve_aggregate_name(tok.lexeme)
	if !agg_ok {
		col, cok := parse_identifier(p, allocator)
		if !cok { return false }

		builder_emit_column(b, col, .COLUMN, -1)
		return true
	}

	advance(p); advance(p) // name (
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
	builder_emit_column(b, display, .AGGREGATE, -1)

	agg_col := "" if is_star else arg_col
	append(&b.aggregates, Aggregate_Expr{func = agg_func, column = agg_col})
	return true
}

// parse_column_or_literal handles a non-`name(` column slot: a literal token
// (captured for materialization) or a (possibly qualified) column.
@(private="file")
parse_column_or_literal :: proc(
	p: ^Parser,
	b: ^Select_Builder,
	tok: Token,
	allocator: mem.Allocator,
) -> bool {
	// Literal tokens (NUMBER/STRING/BLOB_LITERAL/NULL) are captured for
	// materialization. FROM-less SELECTs read them via literal_values; mixed
	// literal+aggregate SELECTs map each LITERAL slot through col_literal_idx
	// (single owner: literal_values, so no double-free).
	#partial switch tok.type {
	case .NUMBER, .STRING, .BLOB_LITERAL, .NULL:
		val, vok := parse_value(p, allocator)
		if !vok { return false }

		append(&b.literal_values, val)
		builder_emit_column(b, strings.clone(tok.lexeme, allocator), .LITERAL, len(b.literal_values) - 1)
	case:
		col, cok := parse_qualified_identifier(p, allocator)
		if !cok { return false }
		builder_emit_column(b, col, .COLUMN, -1)
	}
	return true
}

// eq_fold compares ASCII case-insensitively without allocation.
@(private="file")
eq_fold :: proc(s: string, target: string) -> bool {
	if len(s) != len(target) { return false }
	for i in 0 ..< len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' { c += 32 }
		if c != target[i] { return false }
	}
	return true
}

// resolve_aggregate_name maps a function name (case-insensitive) to its
// aggregate func, or false if it is not a supported aggregate.
// Zero-alloc: ASCII case-fold per byte, no temp_allocator round-trip.
@(private="file")
resolve_aggregate_name :: proc(name: string) -> (Aggregate_Func, bool) {
	switch len(name) {
	case 3:
		if eq_fold(name, "min") { return .MIN, true }
		if eq_fold(name, "max") { return .MAX, true }
		if eq_fold(name, "avg") { return .AVG, true }
		if eq_fold(name, "sum") { return .SUM, true }
	case 5:
		if eq_fold(name, "count") { return .COUNT, true }
	}
	return .COUNT, false
}

// collect_having_aggregates walks a HAVING boolean tree and registers any
// aggregate-named leaf conditions (COUNT(*), SUM(v), ...) into `out` so the
// executor computes them. `out` holds select-list aggregates first; HAVING
// references are appended (deduped) and their column arg is cloned because
// the HAVING tree and the aggregate list are freed independently (the clone
// keeps each owner holding its own string).
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
parse_join_clauses :: proc(p: ^Parser, left_alias: string, allocator := context.allocator) -> [dynamic]Join_Clause {
	joins := make([dynamic]Join_Clause, allocator)
	state := Join_Parse_State{left = left_alias}
	for {
		jt, explicit, matched := match_join_keyword(p)
		if !matched { break }

		jc, jc_ok := parse_single_join(p, allocator, jt, explicit, state.left)
		if !jc_ok { break }

		state.left = jc.alias if jc.alias != "" else state.left
		append(&joins, jc)
	}
	return joins
}

// Join_Parse_State tracks the left-most alias as join clauses chain, so the
// next clause's ON resolution sees the preceding table alias.
Join_Parse_State :: struct {
	left: string,
}

// match_join_keyword consumes a join introducer and reports its type and
// whether an ON clause is permitted (inner/cross are implicit-on). Returns
// matched=false when the next token does not start a join clause. A dangling
// INNER/CROSS/LEFT/RIGHT without JOIN is treated as no-join (matched=false).
@(private="file")
match_join_keyword :: proc(p: ^Parser) -> (jt: Join_Type, explicit: bool, matched: bool) {
	switch {
	case match(p, .COMMA):
		return .CROSS, false, true
	case match(p, .JOIN):
		return .INNER, false, true
	case match(p, .INNER):
		if !match(p, .JOIN) { return .INNER, false, false }
		return .INNER, true, true
	case match(p, .CROSS):
		if !match(p, .JOIN) { return .CROSS, false, false }
		return .CROSS, false, true
	case match(p, .LEFT):
		match(p, .OUTER)
		if !match(p, .JOIN) { return .LEFT, false, false }
		return .LEFT, true, true
	case match(p, .RIGHT):
		match(p, .OUTER)
		if !match(p, .JOIN) { return .RIGHT, false, false }
		return .RIGHT, true, true
	}
	return .INNER, false, false
}

@(private)
// MAX_PARSE_NESTING bounds recursive SELECT/subquery/set-op parsing. Each
// nesting level adds a parse_select frame; beyond ~1000 levels the native stack
// overflows, so cap well below that and reject the query with an error.
MAX_PARSE_NESTING :: 512

// Select_Builder accumulates the SELECT list's parallel arrays so parse sites
// stay in lockstep; finalize slices them into the statement, abandon frees
// them after a parse failure.
Select_Builder :: struct {
	columns:         [dynamic]string,
	aliases:         [dynamic]string,
	literal_values:  [dynamic]types.Value,
	col_kinds:       [dynamic]Select_Column_Kind,
	col_literal_idx: [dynamic]int,
	aggregates:      [dynamic]Aggregate_Expr,
}

@(private="file")
builder_new :: proc(allocator := context.allocator) -> Select_Builder {
	return {
		columns         = make([dynamic]string, allocator),
		aliases         = make([dynamic]string, allocator),
		literal_values  = make([dynamic]types.Value, allocator),
		col_kinds       = make([dynamic]Select_Column_Kind, allocator),
		col_literal_idx = make([dynamic]int, allocator),
		aggregates      = make([dynamic]Aggregate_Expr, allocator),
	}
}

// builder_abandon frees a builder after a parse failure. The allocator must
// be the one the builder was made with: element frees are only valid there
// (a temp-arena string freed with the heap allocator aborts).
@(private="file")
builder_abandon :: proc(b: ^Select_Builder, allocator := context.allocator) {
	delete(b.columns)
	delete(b.aliases)

	types.values_delete(b.literal_values[:], allocator)
	delete(b.col_kinds)
	delete(b.col_literal_idx)
	for agg in b.aggregates { delete(agg.column, allocator) }
	delete(b.aggregates)
}

// From_Clause is the parsed FROM source plus its joins. A missing FROM is
// source No_From{} with no joins (FROM-less literal SELECT).
From_Clause :: struct {
	source: From_Source,
	alias:  string,
	joins:  [dynamic]Join_Clause,
}

@(private="file")
parse_from_clause :: proc(p: ^Parser, allocator := context.allocator) -> (fc: From_Clause, ok: bool) {
	fc.source = No_From{}
	if !match(p, .FROM) { return fc, true }

	js := parse_join_source(p, allocator)
	if !js.success { return {}, false }

	fc.source, fc.alias = js.source, js.alias
	left_name := fc.alias
	if left_name == "" {
		if tbl, is_tbl := js.source.(string); is_tbl { left_name = tbl }
	}

	fc.joins = parse_join_clauses(p, left_name, allocator)
	return fc, true
}

// As_Of holds an optional AS OF SNAPSHOT/TIMESTAMP time-travel clause.
As_Of :: struct {
	snapshot:  Maybe(u64),
	timestamp: Maybe(u64),
}

@(private="file")
parse_as_of :: proc(p: ^Parser) -> (as_of: As_Of, ok: bool) {
	if match(p, .AS) && match(p, .OF) {
		if match(p, .SNAPSHOT) {
			id_token := expect(p, .NUMBER) or_return
			as_of.snapshot = strconv.parse_u64(id_token.lexeme) or_return
		} else if match(p, .TIMESTAMP) {
			id_token := expect(p, .NUMBER) or_return
			as_of.timestamp = strconv.parse_u64(id_token.lexeme) or_return
		}
	}
	return as_of, true
}

@(private="file")
parse_group_by :: proc(p: ^Parser, allocator := context.allocator) -> (group_by: [dynamic]string, ok: bool) {
	group_by = make([dynamic]string, allocator)
	if match(p, .GROUP) {
		if !match(p, .BY) {
			delete(group_by)
			return {}, false
		}
		for {
			col, cok := parse_qualified_identifier(p, allocator)
			if !cok {
				delete(group_by)
				return {}, false
			}

			append(&group_by, col)
			if !match(p, .COMMA) { break }
		}
	}
	return group_by, true
}

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

	b := builder_new(allocator)
	defer if !ok do builder_abandon(&b, allocator)

	is_distinct := match(p, .DISTINCT)
	if !parse_select_columns(p, &b, allocator) {
		return nil, false
	}

	// FROM is optional. A SELECT without FROM evaluates its columns as literals
	// (e.g. `SELECT 1, 'a'`) producing a single row.
	fc, fc_ok := parse_from_clause(p, allocator)
	if !fc_ok { return nil, false }
	defer if !ok do delete(fc.joins)

	as_of := parse_as_of(p) or_return
	where_clause: Maybe(Where_Clause)
	if match(p, .WHERE) { where_clause = parse_where_clause(p, allocator) or_return }

	group_by, gb_ok := parse_group_by(p, allocator)
	if !gb_ok { return nil, false }
	defer if !ok do delete(group_by)

	having_cl: Maybe(Where_Clause)
	if match(p, .HAVING) { having_cl = parse_where_clause(p, allocator) or_return }
	// Register aggregates referenced only by HAVING (e.g. `HAVING COUNT(*) >= 2`
	// with no aggregate in the SELECT list) so the executor computes them.
	if hc, has_h := having_cl.?; has_h {
		collect_having_aggregates(hc.root, &b.aggregates, allocator)
	}

	order_by: Maybe([]Order_By_Column)
	limit: Maybe(u64)
	offset: Maybe(u64)
	if consume_order_limit {
		order_by, limit, offset = parse_order_limit(p, allocator) or_return
	}
	return Select_Stmt {
			from = fc.source,
			from_alias = fc.alias,
			joins = fc.joins[:],
			columns = b.columns[:],
			aliases = b.aliases[:],
			literal_values = b.literal_values[:],
			col_kinds = b.col_kinds[:],
			col_literal_idx = b.col_literal_idx[:],
			aggregates = b.aggregates[:],
			is_distinct = is_distinct,
			where_clause = where_clause,
			order_by = order_by,
			limit = limit,
			offset = offset,
			group_by = group_by[:],
			having = having_cl,
			as_of_snapshot = as_of.snapshot,
			as_of_timestamp = as_of.timestamp,
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
// parse_compound_operand parses one `SELECT ...` operand after a set
// operator (already consumed) and binds it to that operator. Operands never
// consume a trailing ORDER BY/LIMIT (the compound tail owns those).
@(private="file")
parse_compound_operand :: proc(
	p: ^Parser,
	op: Set_Op,
	allocator := context.allocator,
) -> (
	operand: Set_Operand,
	ok: bool,
) {
	if !match(p, .SELECT) { return {}, false }

	sel_variant, sel_ok := parse_select(p, allocator, false)
	if !sel_ok { return {}, false }

	sel, _ := sel_variant.(Select_Stmt)
	sel_ptr := new(Select_Stmt, allocator)
	sel_ptr^ = sel
	return Set_Operand{select = sel_ptr, op = op}, true
}

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
		operand, op_ok := parse_compound_operand(p, op, allocator)
		if !op_ok {
			free(first_ptr, allocator)
			return nil, false
		}

		append(&operands, operand)
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
