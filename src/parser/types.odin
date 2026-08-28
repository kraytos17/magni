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
	negated:     bool, // col NOT IN (...) / col NOT LIKE 'x'
	agg_column:  string, // aggregate argument for NAME(...) refs ("" for COUNT(*))
	// rhs: types.Value for literal comparisons, string for column-column comparisons (e.g. t1.a = t2.b)
	rhs:         union {
		types.Value,
		string,
	},
	in_values:   []types.Value, // IN (val1, val2, ...)
	in_subquery: ^Select_Stmt, // IN (SELECT ...); owned pointer freed by where_clause_free
}

Where_Kind :: enum u8 {
	COND,
	AND,
	OR,
	NOT,
}

Where_Node :: struct {
	kind:     Where_Kind,
	cond:     Condition, // valid when kind == .COND
	children: [dynamic]^Where_Node, // valid when kind == .AND or .OR (n-ary)
}

Where_Clause :: struct {
	root: ^Where_Node, // nil = no filter (always true)
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
	values:     [][]types.Value, // one row of values per VALUES (...) group
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
	No_From,
}

// No_From marks a FROM-less SELECT whose columns are literal expressions
// (e.g. `SELECT 1, 'a'`), producing a single row.
No_From :: struct {}

Join_Source_Result :: struct {
	source:  From_Source,
	alias:   string,
	success: bool,
}

Select_Stmt :: struct {
	from:            From_Source, // table name string, subquery ^Select_Stmt, or No_From
	from_alias:      string, // e.g. "FROM t AS a" sets from_alias = "a"
	joins:           []Join_Clause,
	columns:         []string, // projected column names; empty = *
	aliases:         []string, // parallel to columns: AS alias or "" when none
	literal_values:  []types.Value, // FROM-less SELECT: literal column values
	aggregates:      []Aggregate_Expr,
	is_distinct:     bool,
	where_clause:    Maybe(Where_Clause),
	order_by:        Maybe([]Order_By_Column),
	limit:           Maybe(u64),
	offset:          Maybe(u64),
	group_by:        []string,
	having:          Maybe(Where_Clause),
	as_of_snapshot:  Maybe(u64), // AS OF SNAPSHOT <id>
	as_of_timestamp: Maybe(u64), // AS OF TIMESTAMP <micros>
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

Set_Op :: enum {
	UNION,
	UNION_ALL,
	INTERSECT,
	INTERSECT_ALL,
	EXCEPT,
	EXCEPT_ALL,
}

// Set_Operand is a SELECT joined to the compound result by `op`.
Set_Operand :: struct {
	select: ^Select_Stmt,
	op:     Set_Op,
}

// Compound_Stmt is a chain of SELECTs combined with UNION / INTERSECT / EXCEPT.
// `first` is the leftmost SELECT; `operands` hold the rest, each with the
// operator that connects it to the accumulated result. `order_by`/`limit`/
// `offset` apply to the combined result.
Compound_Stmt :: struct {
	first:    ^Select_Stmt,
	operands: []Set_Operand,
	order_by: Maybe([]Order_By_Column),
	limit:    Maybe(u64),
	offset:   Maybe(u64),
}

Statement_Variant :: union {
	Create_Stmt,
	Insert_Stmt,
	Select_Stmt,
	Compound_Stmt,
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
	tokens:     []Token,
	current:    int,
	err_msg:    string,
	nest_depth: int, // guards recursive SELECT/subquery parsing against stack exhaustion
}
