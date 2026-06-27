package parser

import "core:encoding/hex"
import "core:mem"
import "core:strconv"
import "core:strings"
import "src:types"

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

cleanup_where_conditions :: proc(conditions: [dynamic]Condition, allocator := context.allocator) {
	for cond in conditions {
		delete(cond.column, allocator)
		if rc, ok := cond.rhs.(string); ok { delete(rc, allocator) }
		if val, ok := cond.rhs.(types.Value); ok { types.value_delete(val) }
	}
	delete(conditions)
}

parse_where_clause :: proc(
	p: ^Parser,
	allocator := context.allocator,
) -> (
	clause: Maybe(Where_Clause),
	ok: bool,
) {
	w := Where_Clause {
		is_and = true,
	}

	conditions := make([dynamic]Condition, allocator)
	defer if !ok do cleanup_where_conditions(conditions, allocator)
	first_logical_op_seen := false
	for {
		cond := Condition {
			column = parse_qualified_identifier(p, allocator) or_return,
		}

		op_token := peek(p)
		#partial switch op_token.type {
		case .EQUALS, .NOT_EQUALS, .LESS_THAN, .GREATER_THAN, .LESS_EQUAL, .GREATER_EQUAL, .LIKE:
			cond.operator = op_token.type; advance(p)
		case .IN:
			cond.operator = .IN; advance(p)
			if !match(p, .LPAREN) {
				delete(cond.column, allocator); return nil, false
			}
			if peek(p).type == .SELECT {
				subq_variant, subq_ok := parse_select(p, allocator)
				if !subq_ok {
					delete(cond.column, allocator); return nil, false
				}

				subq_ptr := new(Select_Stmt, allocator)
				subq_ptr^ = subq_variant.(Select_Stmt)
				cond.in_subquery = subq_ptr
				if !match(p, .RPAREN) {
					delete(cond.column, allocator); return nil, false
				}
			} else {
				in_vals := make([dynamic]types.Value, allocator)
				for {
					val, val_ok := parse_value(p, allocator)
					if !val_ok {
						for v in in_vals { types.value_delete(v, allocator) }
						delete(in_vals); delete(cond.column, allocator)
						return nil, false
					}

					append(&in_vals, val)
					if match(p, .RPAREN) { break }
					if !match(p, .COMMA) {
						for v in in_vals { types.value_delete(v, allocator) }
						delete(in_vals); delete(cond.column, allocator)
						return nil, false
					}
				}
				cond.in_values = in_vals[:]
			}
		case:
			delete(cond.column, allocator); return nil, false
		}

		if cond.operator != .IN {
			if peek(p).type == .IDENTIFIER {
				cond.rhs = parse_qualified_identifier(p, allocator) or_return
			} else {
				val, val_ok := parse_value(p, allocator)
				if !val_ok {
					delete(cond.column, allocator)
					return nil, false
				}
				cond.rhs = val
			}
		}

		append(&conditions, cond)
		if match(p, .AND) {
			if !first_logical_op_seen {
				w.is_and = true
				first_logical_op_seen = true
			} else if !w.is_and {
				return nil, false
			}
		} else if match(p, .OR) {
			if !first_logical_op_seen {
				w.is_and = false
				first_logical_op_seen = true
			} else if w.is_and {
				return nil, false
			}
		} else {
			break
		}
	}
	w.conditions = conditions[:]
	return w, true
}
