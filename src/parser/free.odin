package parser

import "src:types"

where_clause_free :: proc(w: Where_Clause, allocator := context.allocator) {
	for cond in w.conditions {
		delete(cond.column, allocator)
		if rc, ok := cond.rhs.(string); ok { delete(rc, allocator) }
		if val, ok := cond.rhs.(types.Value); ok { types.value_delete(val, allocator) }

		types.values_delete(cond.in_values, allocator)
		if subq := cond.in_subquery; subq != nil {
			statement_free(Statement{type = subq^, sql = ""}, allocator)
			free(subq, allocator)
		}
	}
	delete(w.conditions, allocator)
}

statement_free :: proc(stmt: Statement, allocator := context.allocator) {
	delete(stmt.sql, allocator)
	switch s in stmt.type {
	case Create_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col.name, allocator)
			if def, ok := col.default_value.?; ok { types.value_delete(def, allocator) }
		}

		delete(s.columns, allocator)
		for fk in s.foreign_keys {
			delete(fk.col, allocator)
			delete(fk.ref_table, allocator)
			delete(fk.ref_col, allocator)
		}

		delete(s.foreign_keys, allocator)
	case Insert_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns { delete(col, allocator) }
		delete(s.columns, allocator)
		types.values_delete(s.values, allocator)
	case Select_Stmt:
		#partial switch src in s.from {
		case string:
			delete(src, allocator)
		case ^Select_Stmt:
			statement_free(Statement{type = src^, sql = ""}, allocator); free(src, allocator)
		}

		if s.from_alias != "" { delete(s.from_alias, allocator) }
		for j in s.joins {
			#partial switch j_src in j.source {
			case string:
				delete(j_src, allocator)
			case ^Select_Stmt:
				statement_free(Statement{type = j_src^, sql = ""}, allocator)
				free(j_src, allocator)
			}

			if j.alias != "" { delete(j.alias, allocator) }
			if on_cl, ok := j.on_clause.?; ok { where_clause_free(on_cl, allocator) }
		}

		delete(s.joins, allocator)
		for col in s.columns { delete(col, allocator) }

		delete(s.columns, allocator)
		types.values_delete(s.literal_values, allocator)
		for agg in s.aggregates { delete(agg.column, allocator) }

		delete(s.aggregates, allocator)
		if w, ok := s.where_clause.?; ok { where_clause_free(w, allocator) }
		if order, ok := s.order_by.?; ok {
			for o in order {
				delete(o.column, allocator)
			}
			delete(order, allocator)
		}
		for col in s.group_by { delete(col, allocator) }

		delete(s.group_by, allocator)
		if h, ok := s.having.?; ok { where_clause_free(h, allocator) }
	case Compound_Stmt:
		statement_free(Statement{type = s.first^, sql = ""}, allocator)
		free(s.first, allocator)
		for operand in s.operands {
			statement_free(Statement{type = operand.select^, sql = ""}, allocator)
			free(operand.select, allocator)
		}

		delete(s.operands, allocator)
		if order, ok := s.order_by.?; ok {
			for o in order {
				delete(o.column, allocator)
			}
			delete(order, allocator)
		}
	case Update_Stmt:
		delete(s.table_name, allocator)
		for col in s.update_columns { delete(col, allocator) }

		delete(s.update_columns, allocator)
		types.values_delete(s.update_values, allocator)
		if w, ok := s.where_clause.?; ok { where_clause_free(w, allocator) }
	case Delete_Stmt:
		delete(s.table_name, allocator)
		if w, ok := s.where_clause.?; ok { where_clause_free(w, allocator) }
	case Drop_Stmt:
		delete(s.table_name, allocator)
	case Txn_Stmt:
	case Explain_Stmt:
		delete(s.sql, allocator)
	}
}
