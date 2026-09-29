package parser

import "core:encoding/hex"
import "core:mem"
import "core:strconv"
import "core:strings"
import "src:types"

@(private="file")
unescape_sql_string :: proc(s: string, allocator: mem.Allocator) -> string {
	if !strings.contains(s, "''") { return strings.clone(s, allocator) }

	b := strings.builder_make(allocator)
	i := 0
	for i < len(s) {
		if i + 1 < len(s) && s[i] == '\'' && s[i + 1] == '\'' {
			strings.write_byte(&b, '\'')
			i += 2
		} else {
			strings.write_byte(&b, s[i])
			i += 1
		}
	}
	return strings.to_string(b)
}

@(private)
parse_value :: proc(p: ^Parser, allocator := context.allocator) -> (val: types.Value, ok: bool) {
	token := peek(p)
	#partial switch token.type {
	case .NUMBER:
		advance(p)
		if strings.contains(token.lexeme, ".") {
			v := strconv.parse_f64(token.lexeme) or_return
			return types.value_real(v), true
		} else {
			v := strconv.parse_i64(token.lexeme) or_return
			return types.value_int(v), true
		}
	case .STRING:
		advance(p)
		return types.value_text(unescape_sql_string(token.lexeme, allocator)), true
	case .BLOB_LITERAL:
		advance(p)
		bytes, decode_ok := hex.decode(transmute([]u8)token.lexeme, allocator)
		if !decode_ok { return {}, false }
		return types.value_blob(bytes), true
	case .NULL:
		advance(p); return types.value_null(), true
	case .IDENTIFIER:
		advance(p)
		if match(p, .DOT) {
			second := parse_identifier(p, allocator) or_return
			qualified := strings.concatenate({token.lexeme, ".", second}, allocator)
			delete(second, allocator); return types.value_text(qualified), true
		}
		return types.value_text(strings.clone(token.lexeme, allocator)), true
	}
	return {}, false
}

@(private)
parse_where_clause :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	clause: Maybe(Where_Clause),
	ok: bool,
) {
	root, root_ok := parse_or_expr(p, allocator)
	if !root_ok { return nil, false }
	return Where_Clause{root = root}, true
}

// Grammar (SQL precedence: AND binds tighter than OR):
//   or_expr   := and_expr (OR and_expr)*
//   and_expr  := primary (AND primary)*
//   primary   := '(' or_expr ')' | condition
@(private="file")
parse_or_expr :: proc(p: ^Parser, allocator: mem.Allocator) -> (^Where_Node, bool) {
	left, ok := parse_and_expr(p, allocator)
	if !ok { return nil, false }
	if !match(p, .OR) { return left, true }

	children := make([dynamic]^Where_Node, allocator)
	append(&children, left)
	for {
		right, right_ok := parse_and_expr(p, allocator)
		if !right_ok {
			where_nodes_free(children, allocator)
			return nil, false
		}

		append(&children, right)
		if !match(p, .OR) { break }
	}

	node := new(Where_Node, allocator)
	node^ = Where_Node{kind = .OR, children = children}
	return node, true
}

@(private="file")
parse_and_expr :: proc(p: ^Parser, allocator: mem.Allocator) -> (^Where_Node, bool) {
	left, ok := parse_primary(p, allocator)
	if !ok { return nil, false }
	if !match(p, .AND) { return left, true }

	children := make([dynamic]^Where_Node, allocator)
	append(&children, left)
	for {
		right, right_ok := parse_primary(p, allocator)
		if !right_ok {
			where_nodes_free(children, allocator)
			return nil, false
		}

		append(&children, right)
		if !match(p, .AND) { break }
	}

	node := new(Where_Node, allocator)
	node^ = Where_Node{kind = .AND, children = children}
	return node, true
}

@(private="file")
Between_Parse :: enum {
	Not_Between, // lookahead says this isn't a BETWEEN expression; keep parsing
	Parsed, // BETWEEN parsed (node owns its strings)
	Error, // BETWEEN detected but malformed; p.err_msg is set
}

// make_between_cond builds one leaf comparison of a BETWEEN desugar,
// cloning the column name for the node's ownership.
@(private="file")
make_between_cond :: proc(
	op: Token_Type,
	val: types.Value,
	col_name: string,
	allocator: mem.Allocator,
) -> (^Where_Node, bool) {
	c := Condition { column = strings.clone(col_name, allocator), operator = op, rhs = val }
	n := new(Where_Node, allocator)
	n^ = Where_Node{kind = .COND, cond = c}
	return n, true
}

// try_parse_between parses `col BETWEEN lo AND hi` (and NOT BETWEEN),
// desugaring to `col >= lo AND col <= hi` (or `<`/`>` under negation).
@(private="file")
try_parse_between :: proc(
	p: ^Parser,
	allocator: mem.Allocator,
) -> (
	node: ^Where_Node,
	status: Between_Parse,
) {
	peek0 := peek(p)
	peek1_type := Token_Type.EOF
	peek2_type := Token_Type.EOF
	if p.current + 1 < len(p.tokens) { peek1_type = p.tokens[p.current + 1].type }
	if p.current + 2 < len(p.tokens) { peek2_type = p.tokens[p.current + 2].type }

	is_between := peek0.type != .NOT && peek1_type == .BETWEEN
	is_not_between := (peek0.type == .NOT && peek2_type == .BETWEEN) ||
		(peek0.type != .NOT && peek1_type == .NOT && peek2_type == .BETWEEN)
	if !is_between && !is_not_between { return nil, .Not_Between }

	// Detect negation: consume leading NOT(s) and optional NOT before BETWEEN
	negated := false
	for peek(p).type == .NOT { match(p, .NOT); negated = !negated }

	col_name, col_ok := parse_qualified_identifier(p, allocator)
	if !col_ok { return nil, .Error }
	if peek(p).type == .NOT { match(p, .NOT); negated = !negated }
	advance(p) // consume BETWEEN

	lower_val, lower_ok := parse_value(p, allocator)
	if !lower_ok { delete(col_name, allocator); return nil, .Error }
	if !expect_match(p, .AND, "Expected AND in BETWEEN expression") {
		delete(col_name, allocator); types.value_delete(lower_val, allocator)
		return nil, .Error
	}

	upper_val, upper_ok := parse_value(p, allocator)
	if !upper_ok {
		delete(col_name, allocator); types.value_delete(lower_val, allocator)
		return nil, .Error
	}

	left_op, right_op, kind := Token_Type.GREATER_EQUAL, Token_Type.LESS_EQUAL, Where_Kind.AND
	if negated {
		left_op, right_op, kind = Token_Type.LESS_THAN, Token_Type.GREATER_THAN, Where_Kind.OR
	}

	left, lok := make_between_cond(left_op, lower_val, col_name, allocator)
	if !lok {
		delete(col_name, allocator)
		types.value_delete(upper_val, allocator)
		return nil, .Error
	}

	right, rok := make_between_cond(right_op, upper_val, col_name, allocator)
	if !rok {
		delete(col_name, allocator)
		where_node_free(left, allocator)
		return nil, .Error
	}

	delete(col_name, allocator)
	children := make([dynamic]^Where_Node, allocator)
	append(&children, left)
	append(&children, right)
	node = new(Where_Node, allocator)
	node^ = Where_Node{kind = kind, children = children}
	return node, .Parsed
}

@(private="file")
parse_primary :: proc(p: ^Parser, allocator: mem.Allocator) -> (^Where_Node, bool) {
	p.nest_depth += 1
	defer p.nest_depth -= 1
	if p.nest_depth > MAX_PARSE_NESTING {
		if p.err_msg == "" { p.err_msg = "Expression nesting too deep" }
		return nil, false
	}
	if between_node, between_status := try_parse_between(p, allocator); between_status != .Not_Between {
		return between_node, between_status == .Parsed
	}
	if match(p, .NOT) {
		child, child_ok := parse_primary(p, allocator)
		if !child_ok { return nil, false }

		children := make([dynamic]^Where_Node, allocator)
		append(&children, child)
		node := new(Where_Node, allocator)
		node^ = Where_Node{kind = .NOT, children = children}
		return node, true
	}
	if match(p, .LPAREN) {
		inner, inner_ok := parse_or_expr(p, allocator)
		if !inner_ok { return nil, false }
		if !expect_match(p, .RPAREN, "Expected ')' in WHERE expression") {
			where_node_free(inner, allocator)
			return nil, false
		}
		return inner, true
	}

	cond, cond_ok := parse_condition(p, allocator)
	if !cond_ok { return nil, false }

	node := new(Where_Node, allocator)
	node^ = Where_Node{kind = .COND, cond = cond}
	return node, true
}

// condition_cleanup frees the owned string fields of a partially-parsed
// Condition on error paths (column + aggregate argument).
@(private="file")
condition_cleanup :: proc(cond: ^Condition, allocator: mem.Allocator) {
	delete(cond.column, allocator)
	delete(cond.agg_column, allocator)
}

@(private="file")
parse_condition :: proc(p: ^Parser, allocator: mem.Allocator) -> (cond: Condition, ok: bool) {
	cond.column = parse_qualified_identifier(p, allocator) or_return
	// Aggregate reference in a condition: COUNT(*), COUNT(v), SUM(v), ...
	if peek(p).type == .LPAREN {
		advance(p)
		if !match(p, .ASTERISK) {
			arg, arg_ok := parse_qualified_identifier(p, allocator)
			if !arg_ok {
				condition_cleanup(&cond, allocator); return {}, false
			}
			cond.agg_column = arg
		}
		if !expect_match(p, .RPAREN, "Expected ) after aggregate reference") {
			condition_cleanup(&cond, allocator); return {}, false
		}
	}
	// col NOT IN (...) / col NOT LIKE 'x'
	if match(p, .NOT) {
		cond.negated = true
	}

	op_token := peek(p)
	#partial switch op_token.type {
	case .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL, .LIKE:
		cond.operator = op_token.type; advance(p)
	case .IN:
		cond.operator = .IN; advance(p)
		if !match(p, .LPAREN) {
			condition_cleanup(&cond, allocator); return {}, false
		}
		if peek(p).type == .SELECT {
			advance(p)
			subq_variant, subq_ok := parse_select(p, allocator)
			if !subq_ok {
				condition_cleanup(&cond, allocator); return {}, false
			}

			subq_ptr := new(Select_Stmt, allocator)
			subq_ptr^ = subq_variant.(Select_Stmt)
			cond.in_subquery = subq_ptr
			if !match(p, .RPAREN) {
				statement_free(Statement{type = subq_ptr^, sql = ""}, allocator)
				free(subq_ptr, allocator)
				condition_cleanup(&cond, allocator); return {}, false
			}
		} else {
			in_vals := make([dynamic]types.Value, allocator)
			for {
				val, val_ok := parse_value(p, allocator)
				if !val_ok {
					for v in in_vals { types.value_delete(v, allocator) }
					delete(in_vals); condition_cleanup(&cond, allocator)
					return {}, false
				}

				append(&in_vals, val)
				if match(p, .RPAREN) { break }
				if !match(p, .COMMA) {
					for v in in_vals { types.value_delete(v, allocator) }
					delete(in_vals); condition_cleanup(&cond, allocator)
					return {}, false
				}
			}
			cond.in_values = in_vals[:]
		}
	case .IS:
		// `col IS [NOT] NULL`. A NOT directly before IS (`col NOT IS NULL`)
		// is not valid SQL — NOT belongs after IS.
		if cond.negated {
			if p.err_msg == "" { p.err_msg = "Expected NULL after IS" }
			condition_cleanup(&cond, allocator); return {}, false
		}

		advance(p)
		if match(p, .NOT) { cond.negated = true }

		cond.operator = .IS
		if !expect_match(p, .NULL, "Expected NULL after IS [NOT]") {
			condition_cleanup(&cond, allocator); return {}, false
		}
	case:
		if p.err_msg == "" { p.err_msg = "Expected comparison operator in WHERE condition" }
		condition_cleanup(&cond, allocator); return {}, false
	}

	if cond.operator != .IN && cond.operator != .IS {
		if peek(p).type == .IDENTIFIER {
			rhs_str, rhs_ok := parse_qualified_identifier(p, allocator)
			if !rhs_ok {
				condition_cleanup(&cond, allocator); return {}, false
			}
			cond.rhs = rhs_str
		} else {
			val, val_ok := parse_value(p, allocator)
			if !val_ok {
				condition_cleanup(&cond, allocator)
				return {}, false
			}
			cond.rhs = val
		}
	}
	return cond, true
}
