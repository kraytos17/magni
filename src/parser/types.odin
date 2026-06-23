package parser

import "src:types"

Token :: struct {
	type:   Token_Type,
	lexeme: string,
	line:   u32,
}

Condition :: struct {
	column:      string,
	operator:    Token_Type,
	rhs:         union {
		types.Value,
		string,
	},
	in_values:   []types.Value,
	in_subquery: ^Select_Stmt,
}

Where_Clause :: struct {
	conditions: []Condition,
	is_and:     bool,
}

Create_Stmt :: struct {
	table_name:   string,
	columns:      []types.Column,
	foreign_keys: []Foreign_Key,
}

Foreign_Key :: struct {
	col:       string,
	ref_table: string,
	ref_col:   string,
}

Insert_Stmt :: struct {
	table_name: string,
	columns:    []string,
	values:     []types.Value,
}

Order_By_Column :: struct {
	column:      string,
	desc:        bool,
	nulls_first: bool,
}

Aggregate_Func :: enum {
	COUNT,
	SUM,
	AVG,
	MIN,
	MAX,
}

Aggregate_Expr :: struct {
	func:   Aggregate_Func,
	column: string,
}

Join_Type :: enum {
	INNER,
	CROSS,
	LEFT,
}

Join_Clause :: struct {
	join_type: Join_Type,
	source:    From_Source,
	alias:     string,
	on_clause: Maybe(Where_Clause),
}

From_Source :: union {
	string,
	^Select_Stmt,
}

Join_Source_Result :: struct {
	source:  From_Source,
	alias:   string,
	success: bool,
}

Select_Stmt :: struct {
	from:            From_Source,
	from_alias:      string,
	joins:           []Join_Clause,
	columns:         []string,
	aggregates:      []Aggregate_Expr,
	is_distinct:     bool,
	where_clause:    Maybe(Where_Clause),
	order_by:        Maybe([]Order_By_Column),
	limit:           Maybe(u64),
	offset:          Maybe(u64),
	group_by:        []string,
	having:          Maybe(Where_Clause),
	as_of_snapshot:  Maybe(u64),
	as_of_timestamp: Maybe(u64),
}

Update_Stmt :: struct {
	table_name:     string,
	update_columns: []string,
	update_values:  []types.Value,
	where_clause:   Maybe(Where_Clause),
}

Delete_Stmt :: struct {
	table_name:   string,
	where_clause: Maybe(Where_Clause),
}

Drop_Stmt :: struct {
	table_name: string,
}

Txn_Op :: enum {
	BEGIN,
	COMMIT,
	ROLLBACK,
}

Txn_Stmt :: struct {
	op: Txn_Op,
}

Statement_Variant :: union {
	Create_Stmt,
	Insert_Stmt,
	Select_Stmt,
	Update_Stmt,
	Delete_Stmt,
	Drop_Stmt,
	Txn_Stmt,
	Explain_Stmt,
}

Statement :: struct {
	type: Statement_Variant,
	sql:  string,
}

Explain_Stmt :: struct {
	sql: string,
}

Parser :: struct {
	tokens:  []Token,
	current: int,
}
