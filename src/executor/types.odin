package executor

import "src:btree"
import "src:parser"
import "src:types"

Table_Info :: struct {
	table:   types.Table, // physical table metadata (for FROM table sources)
	tree:    btree.Tree, // data b-tree for this table
	virtual: Maybe(Virtual_Table), // set when FROM source is a subquery instead of a physical table
}

Table_Col_Range :: struct {
	table_name: string,
	start_col:  int, // first column index in the combined columns array
	col_count:  int,
}

Table_Context :: struct {
	info:  Table_Info,
	range: Table_Col_Range,
}

// Join_Build captures the assembled state of a FROM+JOINs query: resolved
// table contexts, combined column metadata, and (after execution) rows.
Join_Build :: struct {
	ctxs:       []Table_Context,
	ranges:     []Table_Col_Range,
	cols:       []types.Column,
	rows:       []Row_Entry,
	total_cols: int,
	ok:         bool,
}

Row_Entry :: struct {
	rowid:  types.Row_ID,
	values: []types.Value,
}

Virtual_Table :: struct {
	columns: []types.Column,
	rows:    []Row_Entry,
}

Sort_Ctx :: struct {
	order_clause: []parser.Order_By_Column,
	sort_indices: []int,
}

Group :: struct {
	key_values: []types.Value,
	rows:       [dynamic]Row_Entry,
}

Update_Op :: struct #all_or_none {
	rowid:      types.Row_ID,
	new_values: []types.Value,
}

Mutated_Table_Info :: struct #all_or_none {
	name: string,
	root: u32,
}

// Result captures the outcome of executing a statement: a result set for
// SELECT/Compound (is_select + rows/cols), or affected-table info for DML.
// It is data-only — rendering is the caller's responsibility.
Result :: struct {
	rows:      []Row_Entry,
	cols:      []types.Column,
	is_select: bool,
	mutated:   Mutated_Table_Info,
	new_root:  u32,
}

Resolved_Condition :: struct {
	col_idx:             int, // column index in the row's values array
	operator:            parser.Token_Type,
	negated:             bool, // col NOT IN (...) / col NOT LIKE 'x'
	rhs:                 types.Value, // compared value (ignored if has_right_col or has_in)
	has_right_col:       bool, // true → rhs is another column at right_idx
	right_idx:           int,
	has_in:              bool, // true → use in_values or in_subquery instead of rhs
	in_values:           []types.Value, // literal IN list
	in_set:              map[u64]bool, // fingerprint prefilter over in_values (nil = scan)
	in_subquery:         ^parser.Select_Stmt, // subquery IN (SELECT ...)
	in_subquery_results: []types.Value, // materialized subquery (filled once, not per row)
}

Where_Eval_Ctx :: struct {
	root:        ^Resolved_Node, // nil = no filter (always true)
	schema_tree: ^btree.Tree,
}

// Mutation_Filter holds a DML statement's optional row filter plus its
// once-resolved evaluation context. Embedded (via `using`) in Update_Plan
// and Delete_Plan so both verbs share one filter implementation instead of
// parallel eval_plan_filter / eval_delete_filter twins.
Mutation_Filter :: struct {
	filter:     Maybe(parser.Where_Clause),
	filter_ctx: Maybe(Where_Eval_Ctx),
}

// Scan_Plan captures the resolved state for one table scan: optional filter
// context (nil = no filter), skip-index bounds, and row limit. Built once by
// build_scan_plan, consumed by the cursor loop in scan_table.
Scan_Plan :: struct {
	filter:     Maybe(Where_Eval_Ctx),
	skip_conds: []Resolved_Condition,
	skip_start: u32,
	skip_end:   u32,
	max_rows:   Maybe(u64),
}

Resolved_Node_Kind :: enum u8 {
	COND,
	AND,
	OR,
	NOT,
}

Resolved_Node :: struct {
	kind:     Resolved_Node_Kind,
	cond:     Resolved_Condition, // valid when kind == .COND
	children: []^Resolved_Node, // valid when kind == .AND or .OR (n-ary)
}
