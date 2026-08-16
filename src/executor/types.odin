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
	in_subquery:         ^parser.Select_Stmt, // subquery IN (SELECT ...)
	in_subquery_results: []types.Value, // materialized subquery (filled once, not per row)
}

Where_Eval_Ctx :: struct {
	root:        ^Resolved_Node, // nil = no filter (always true)
	schema_tree: ^btree.Tree,
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
