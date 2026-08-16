package executor

import "core:mem"
import "core:strings"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// split_where_for_join partitions a WHERE clause's top-level AND conjuncts into
// per-table filters (index-aligned with table_ranges) so each join-side scan can
// filter early and use skip-index pruning. A conjunct is pushed to a table only
// when every column it references (LHS and any column-column RHS) is unqualified
// and resolves to exactly one table. Qualified, ambiguous, or cross-table
// conjuncts are left unpushed (nil filter), so the post-join filter must remain
// in place. The returned clauses reference the original AST subtrees (no new
// ownership); synthetic AND nodes are allocated on the supplied allocator.
@(private)
split_where_for_join :: proc(
	clause: parser.Where_Clause,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	allocator := context.temp_allocator,
) -> [dynamic]Maybe(parser.Where_Clause) {
	filters := make([dynamic]Maybe(parser.Where_Clause), len(table_ranges), allocator)
	if clause.root == nil || len(table_ranges) == 0 {
		return filters
	}

	conjuncts := make([dynamic]^parser.Where_Node, 0, 4, allocator)
	#partial switch clause.root.kind {
	case .AND:
		for child in clause.root.children { append(&conjuncts, child) }
	case .COND:
		append(&conjuncts, clause.root)
	case:
		return filters // OR / NOT / nested boolean: no pushdown
	}

	// Decide each conjunct's table in one pass, then assemble per-table clauses.
	ti_of := make([]int, len(conjuncts), allocator)
	for c, i in conjuncts {
		ti, ok := conjunct_table_index(c, combined_cols, table_ranges)
		ti_of[i] = ti if ok else -1
	}

	for ti in 0 ..< len(table_ranges) {
		assigned := make([dynamic]^parser.Where_Node, 0, 4, allocator)
		for c, i in conjuncts {
			if ti_of[i] == ti { append(&assigned, c) }
		}
		if len(assigned) == 0 { continue }
		if len(assigned) == 1 {
			filters[ti] = parser.Where_Clause {root = assigned[0]}
		} else {
			and_node := new(parser.Where_Node, allocator)
			and_node.kind = .AND
			and_node.children = make([dynamic]^parser.Where_Node, 0, len(assigned), allocator)
			append(&and_node.children, ..assigned[:])
			filters[ti] = parser.Where_Clause {root = and_node}
		}
	}
	return filters
}

// conjunct_table_index returns the single table whose columns a WHERE conjunct
// references, or ok=false when it is qualified, ambiguous, unresolvable, or
// spans multiple tables.
@(private)
conjunct_table_index :: proc(
	node: ^parser.Where_Node,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	int,
	bool,
) {
	ti: int
	ti_set := false
	ok := true
	conjunct_collect(node, combined_cols, table_ranges, &ti, &ti_set, &ok)
	if !ok || !ti_set { return -1, false }
	return ti, true
}

@(private)
conjunct_collect :: proc(
	n: ^parser.Where_Node,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	ti: ^int,
	ti_set: ^bool,
	ok: ^bool,
) {
	if n == nil || !ok^ { return }
	switch n.kind {
	case .COND:
		if !col_in_table(n.cond.column, combined_cols, table_ranges, ti, ti_set, ok) { return }
		if rhs_str, is_col := n.cond.rhs.(string); is_col {
			if !col_in_table(rhs_str, combined_cols, table_ranges, ti, ti_set, ok) { return }
		}
	case .AND, .OR, .NOT:
		for child in n.children {
			conjunct_collect(child, combined_cols, table_ranges, ti, ti_set, ok)
		}
	}
}

@(private)
col_in_table :: proc(
	name: string,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
	ti: ^int,
	ti_set: ^bool,
	ok: ^bool,
) -> bool {
	t2, col_ok := column_table_index(name, combined_cols, table_ranges)
	if !col_ok { ok^ = false; return false }
	if !ti_set^ {
		ti^ = t2
		ti_set^ = true
		return true
	}
	if ti^ != t2 { ok^ = false; return false }
	return true
}

// column_table_index returns the table whose range contains the named column,
// or ok=false when the name is qualified, missing, or ambiguous.
@(private)
column_table_index :: proc(
	name: string,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	int,
	bool,
) {
	if strings.contains(name, ".") { return -1, false }
	matches := 0
	found := -1
	for tr, ti in table_ranges {
		for ci in tr.start_col ..< tr.start_col + tr.col_count {
			if ci < len(combined_cols) && combined_cols[ci].name == name {
				matches += 1
				found = ti
				break
			}
		}
	}
	if matches != 1 { return -1, false }
	return found, true
}

@(private)
filter_rows :: proc(
	rows: []Row_Entry,
	where_clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> []Row_Entry {
	filtered := make([dynamic]Row_Entry, 0, len(rows), context.temp_allocator)
	ctx, ctx_ok := init_where_ctx(where_clause, cols, table_ranges, nil, context.temp_allocator).?
	if !ctx_ok { return filtered[:] }
	if ctx.root == nil { return rows }
	for entry in rows {
		if evaluate_where_ctx(ctx, entry.values) {
			append(&filtered, entry)
		}
	}
	return filtered[:]
}

@(private)
init_where_ctx :: proc(
	clause: ^parser.Where_Clause,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	schema_tree: ^btree.Tree,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> Maybe(Where_Eval_Ctx) {
	if clause == nil || clause.root == nil {
		return Where_Eval_Ctx{}
	}

	root, ok := build_resolved_node(clause.root, cols, table_ranges, schema_tree, allocator, cache)
	if !ok { return nil }
	return Where_Eval_Ctx{root = root, schema_tree = schema_tree}
}

@(private="file")
build_resolved_node :: proc(
	node: ^parser.Where_Node,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	schema_tree: ^btree.Tree,
	allocator: mem.Allocator,
	cache: ^schema.Table_Cache = nil,
) -> (^Resolved_Node, bool) {
	rn := new(Resolved_Node, allocator)
	switch node.kind {
	case .COND:
		rc, ok := resolve_condition(node.cond, cols, table_ranges, schema_tree, allocator, cache)
		if !ok {
			free(rn, allocator)
			return nil, false
		}
		rn.kind = .COND
		rn.cond = rc
	case .AND, .OR, .NOT:
		rn.kind = .NOT if node.kind == .NOT else (.AND if node.kind == .AND else .OR)
		children := make([]^Resolved_Node, len(node.children), allocator)
		for child, i in node.children {
			child_rn, ok := build_resolved_node(child, cols, table_ranges, schema_tree, allocator, cache)
			if !ok {
				for j in 0 ..< i { free_resolved_node(children[j], allocator) }
				delete(children, allocator)
				free(rn, allocator)
				return nil, false
			}
			children[i] = child_rn
		}
		rn.children = children
	}
	return rn, true
}

@(private="file")
free_resolved_node :: proc(n: ^Resolved_Node, allocator: mem.Allocator) {
	if n == nil { return }
	switch n.kind {
	case .COND:
		delete(n.cond.in_subquery_results, allocator)
	case .AND, .OR, .NOT:
		for child in n.children { free_resolved_node(child, allocator) }
		delete(n.children, allocator)
	}
	free(n, allocator)
}

@(private="file")
resolve_condition :: proc(
	cond: parser.Condition,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
	schema_tree: ^btree.Tree,
	allocator: mem.Allocator,
	cache: ^schema.Table_Cache = nil,
) -> (Resolved_Condition, bool) {
	idx, found := resolve_qualified_column(cols, table_ranges, cond.column)
	if !found { return {}, false }

	rc := Resolved_Condition {
		col_idx       = idx,
		operator      = cond.operator,
		negated       = cond.negated,
		has_in        = cond.operator == .IN,
		has_right_col = false,
		right_idx     = 0,
		in_values     = nil,
		in_subquery   = nil,
	}

	if rhs_str, is_col := cond.rhs.(string); is_col {
		right_idx, rc_found := resolve_qualified_column(cols, table_ranges, rhs_str)
		if !rc_found { return {}, false }
		rc.has_right_col = true
		rc.right_idx = right_idx
	} else if val, is_val := cond.rhs.(types.Value); is_val {
		rc.rhs = val
	}

	if cond.in_values != nil {
		rc.in_values = cond.in_values
	}
	if cond.in_subquery != nil {
		rc.in_subquery = cond.in_subquery
		if schema_tree != nil {
			subq_rows: []Row_Entry
			subq_rows, _ = exec_subquery(schema_tree, cond.in_subquery^, cache)
			rc.in_subquery_results = make([]types.Value, len(subq_rows), allocator)
			for ri in 0 ..< len(subq_rows) {
				if len(subq_rows[ri].values) > 0 {
					rc.in_subquery_results[ri] = subq_rows[ri].values[0]
				}
			}
		}
	}
	return rc, true
}

@(private)
evaluate_where_ctx :: proc(ctx: Where_Eval_Ctx, row: []types.Value) -> bool {
	if ctx.root == nil { return true }
	return evaluate_node(ctx, ctx.root, row)
}

@(private="file")
evaluate_node :: proc(ctx: Where_Eval_Ctx, node: ^Resolved_Node, row: []types.Value) -> bool {
	switch node.kind {
	case .COND:
		return evaluate_resolved_condition(ctx, node.cond, row)
	case .AND:
		for child in node.children {
			if !evaluate_node(ctx, child, row) { return false }
		}
		return true
	case .OR:
		for child in node.children {
			if evaluate_node(ctx, child, row) { return true }
		}
		return false
	case .NOT:
		for child in node.children {
			return !evaluate_node(ctx, child, row)
		}
		return false
	}
	return false
}

@(private="file")
evaluate_resolved_condition :: proc(ctx: Where_Eval_Ctx, rc: Resolved_Condition, row: []types.Value) -> bool {
	left_val := row[rc.col_idx]
	cond_result: bool
	if rc.has_right_col {
		cond_result = compare_condition(left_val, rc.operator, row[rc.right_idx])
	} else {
		cond_result = compare_condition(left_val, rc.operator, rc.rhs)
	}
	if rc.has_in && rc.in_values != nil {
		cond_result = false
		for v in rc.in_values {
			if compare_values(left_val, v) == 0 {
				cond_result = true; break
			}
		}
	} else if rc.has_in && rc.in_subquery_results != nil {
		cond_result = false
		for v in rc.in_subquery_results {
			if compare_values(left_val, v) == 0 {
				cond_result = true; break
			}
		}
	} else if rc.has_in && rc.in_subquery != nil {
		cond_result = false
		subq_rows, _ := exec_subquery(ctx.schema_tree, rc.in_subquery^)
		for sr in subq_rows {
			if len(sr.values) > 0 && compare_values(left_val, sr.values[0]) == 0 {
				cond_result = true; break
			}
		}
	}
	if rc.negated { cond_result = !cond_result }
	return cond_result
}

@(private)
evaluate_where :: proc(
	clause: ^parser.Where_Clause,
	row: []types.Value,
	cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> bool {
	ctx, ok := init_where_ctx(clause, cols, table_ranges, nil, context.temp_allocator).?
	if !ok { return false }
	return evaluate_where_ctx(ctx, row)
}

@(private)
compare_condition :: proc(val: types.Value, op: parser.Token_Type, target: types.Value) -> bool {
	if op == .LIKE {
		text, text_ok := val.(string)
		pattern, pat_ok := target.(string)
		if !text_ok || !pat_ok { return false }
		return like_match(pattern, text)
	}

	cmp := compare_values(val, target)
	#partial switch op {
	case .EQUALS:
		return cmp == 0
	case .NOT_EQUALS:
		return cmp != 0
	case .LESS_THAN:
		return cmp < 0
	case .GREATER_THAN:
		return cmp > 0
	case .LESS_EQUAL:
		return cmp <= 0
	case .GREATER_EQUAL:
		return cmp >= 0
	}
	return false
}

@(private="file")
like_match :: proc(pattern: string, text: string) -> bool {
	// Fast path: pattern ending with %, no underscore = plain prefix match
	if len(pattern) > 1 && pattern[len(pattern) - 1] == '%' {
		has_underscore := false
		for i in 0 ..< len(pattern) - 1 {
			if pattern[i] == '_' { has_underscore = true; break }
		}
		if !has_underscore {
			prefix := pattern[:len(pattern) - 1]
			return len(text) >= len(prefix) && text[:len(prefix)] == prefix
		}
	}

	pi := 0
	ti := 0
	star_pi := -1
	star_ti := -1
	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == text[ti] || pattern[pi] == '_') {
			pi += 1
			ti += 1
		} else if pi < len(pattern) && pattern[pi] == '%' {
			star_pi = pi
			star_ti = ti
			pi += 1
		} else if star_pi != -1 {
			pi = star_pi + 1
			star_ti += 1
			ti = star_ti
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi += 1
	}
	return pi == len(pattern)
}
