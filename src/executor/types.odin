package executor

import "src:btree"
import "src:parser"
import "src:types"

Table_Info :: struct {
	table:   types.Table,
	tree:    btree.Tree,
	virtual: Maybe(Virtual_Table),
}

Table_Col_Range :: struct {
	table_name: string,
	start_col:  int,
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
mutated_table_info: Mutated_Table_Info

Resolved_Condition :: struct {
	col_idx:       int,
	operator:      parser.Token_Type,
	rhs:           types.Value,
	has_right_col: bool,
	right_idx:     int,
	has_in:        bool,
	in_values:     []types.Value,
	in_subquery:   ^parser.Select_Stmt,
}

Where_Eval_Ctx :: struct {
	conditions:  []Resolved_Condition,
	is_and:      bool,
	schema_tree: ^btree.Tree,
}
