package parser

import "src:types"

@(private)
condition_free :: proc(cond: Condition, allocator := context.allocator) {
	delete(cond.column, allocator)
	delete(cond.agg_column, allocator)
	if rc, ok := cond.rhs.(string); ok { delete(rc, allocator) }
	if val, ok := cond.rhs.(types.Value); ok { types.value_delete(val, allocator) }

	types.values_delete(cond.in_values, allocator)
	if subq := cond.in_subquery; subq != nil {
		select_free(subq, allocator)
	}
}

@(private)
where_node_free :: proc(node: ^Where_Node, allocator := context.allocator) {
	if node == nil { return }
	#partial switch node.kind {
	case .COND:
		condition_free(node.cond, allocator)
	case .AND, .OR, .NOT:
		where_nodes_free(node.children, allocator)
	}
	free(node, allocator)
}

@(private)
where_nodes_free :: proc(nodes: [dynamic]^Where_Node, allocator := context.allocator) {
	for n in nodes { where_node_free(n, allocator) }
	delete(nodes)
}

@(private)
where_clause_free :: proc(w: Where_Clause, allocator := context.allocator) {
	where_node_free(w.root, allocator)
}

// delete_each frees each element then the slice itself. Instantiated for
// []string throughout the statement free paths.
@(private)
delete_each :: proc(xs: []$T, allocator := context.allocator) {
	for x in xs { delete(x, allocator) }
	delete(xs, allocator)
}

// select_free releases an owned subquery SELECT and its pointer.
@(private)
select_free :: proc(sel: ^Select_Stmt, allocator := context.allocator) {
	statement_free(Statement{type = sel^, sql = ""}, allocator)
	free(sel, allocator)
}

// from_source_free releases a FROM/JOIN source: a table name or subquery.
@(private)
from_source_free :: proc(src: From_Source, allocator := context.allocator) {
	#partial switch s in src {
	case string:
		delete(s, allocator)
	case ^Select_Stmt:
		select_free(s, allocator)
	}
}

// maybe_where_free releases an optional WHERE/HAVING/ON clause.
@(private)
maybe_where_free :: proc(w: Maybe(Where_Clause), allocator := context.allocator) {
	if clause, ok := w.?; ok { where_clause_free(clause, allocator) }
}

// order_by_free releases an optional ORDER BY column list.
@(private)
order_by_free :: proc(order: Maybe([]Order_By_Column), allocator := context.allocator) {
	if cols, ok := order.?; ok {
		for o in cols { delete(o.column, allocator) }
		delete(cols, allocator)
	}
}

@(private)
statement_free :: proc(stmt: Statement, allocator := context.allocator) {
	delete(stmt.sql, allocator)
	switch s in stmt.type {
	case Create_Stmt:
		delete(s.table_name, allocator)
		for col in s.columns {
			delete(col.name, allocator)
			if def, ok := col.default_value.?; ok { types.value_delete(def, allocator) }
			if chk, ok := col.check_expr.?; ok { delete(chk, allocator) }
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
		delete_each(s.columns, allocator)
		for row in s.values { types.values_delete(row, allocator) }
		delete(s.values, allocator)
	case Select_Stmt:
		from_source_free(s.from, allocator)
		if s.from_alias != "" { delete(s.from_alias, allocator) }
		for j in s.joins {
			from_source_free(j.source, allocator)
			if j.alias != "" { delete(j.alias, allocator) }
			maybe_where_free(j.on_clause, allocator)
		}

		delete(s.joins, allocator)
		delete_each(s.columns, allocator)
		delete_each(s.aliases, allocator)
		types.values_delete(s.literal_values, allocator)

		delete(s.col_kinds, allocator)
		delete(s.col_literal_idx, allocator)
		for agg in s.aggregates { delete(agg.column, allocator) }

		delete(s.aggregates, allocator)
		maybe_where_free(s.where_clause, allocator)
		order_by_free(s.order_by, allocator)
		delete_each(s.group_by, allocator)
		maybe_where_free(s.having, allocator)
	case Compound_Stmt:
		select_free(s.first, allocator)
		for operand in s.operands {
			select_free(operand.select, allocator)
		}

		delete(s.operands, allocator)
		order_by_free(s.order_by, allocator)
	case Update_Stmt:
		delete(s.table_name, allocator)
		delete_each(s.update_columns, allocator)
		types.values_delete(s.update_values, allocator)
		maybe_where_free(s.where_clause, allocator)
	case Delete_Stmt:
		delete(s.table_name, allocator)
		maybe_where_free(s.where_clause, allocator)
	case Drop_Stmt:
		delete(s.table_name, allocator)
	case Txn_Stmt:
	case Explain_Stmt:
		delete(s.sql, allocator)
	}
}
