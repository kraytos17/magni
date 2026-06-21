package parser

import "src:types"

parse_insert :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	if !match(p, .INTO) do return nil, false
	table_name := parse_identifier(p, allocator) or_return
	columns := make([dynamic]string, allocator)
	defer if !ok { for c in columns do delete(c, allocator); delete(columns) }
	if peek(p).type == .LPAREN && p.current + 1 < len(p.tokens) {
		next_type := p.tokens[p.current + 1].type
		if next_type == .IDENTIFIER || next_type == .RPAREN {
			advance(p)
			for {
				col := parse_identifier(p, allocator) or_return; append(&columns, col)
				if match(p, .RPAREN) { break }
				else if !match(p, .COMMA) { return nil, false }
			}
		}
	}
	if !match(p, .VALUES) || !match(p, .LPAREN) { delete(table_name, allocator); return nil, false }
	values := make([dynamic]types.Value, allocator)
	defer if !ok { for v in values { types.value_delete(v, allocator) }; delete(table_name, allocator); delete(values) }
	for {
		val, val_ok := parse_value(p, allocator); if !val_ok { return nil, false }
		append(&values, val)
		if match(p, .RPAREN) { break }
		else if !match(p, .COMMA) { return nil, false }
	}
	return Insert_Stmt{table_name = table_name, columns = columns[:], values = values[:]}, true
}

parse_update :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	table_name := parse_identifier(p, allocator) or_return
	if !match(p, .SET) { delete(table_name, allocator); return nil, false }
	columns := make([dynamic]string, allocator)
	values := make([dynamic]types.Value, allocator)
	defer if !ok { delete(table_name, allocator); delete(columns); delete(values) }
	for {
		append(&columns, parse_identifier(p, allocator) or_return)
		if !match(p, .EQUALS) { return nil, false }
		val, val_ok := parse_value(p, allocator); if !val_ok { return nil, false }
		append(&values, val)
		if !match(p, .COMMA) { break }
	}
	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) { where_cl = parse_where_clause(p, allocator) or_return }
	return Update_Stmt{table_name = table_name, update_columns = columns[:], update_values = values[:], where_clause = where_cl}, true
}

parse_delete :: proc(p: ^Parser, allocator := context.allocator) -> (stmt: Statement_Variant, ok: bool) {
	if !match(p, .FROM) do return nil, false
	table_name := parse_identifier(p, allocator) or_return
	defer if !ok do delete(table_name, allocator)
	where_cl: Maybe(Where_Clause)
	if match(p, .WHERE) { where_cl = parse_where_clause(p, allocator) or_return }
	return Delete_Stmt{table_name = table_name, where_clause = where_cl}, true
}
