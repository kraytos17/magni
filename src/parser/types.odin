package parser

import "src:types"

// Token is one lexical unit: its kind, the source slice (keywords keep
// original case — matching is case-insensitive at tokenize time), and the
// 1-based line for error messages.
Token :: struct {
	type  : Token_Type,
	lexeme: string,
	line  : u32,
}

// Condition is one WHERE/HAVING/ON predicate: a column compared to a value
// (operator), optionally negated (NOT IN / NOT LIKE / IS NOT — BETWEEN
// instead desugars to two comparisons, NOT BETWEEN to an OR of complements),
// with IN-list / IN-subquery / BETWEEN bounds on the side. rhs holds a
// literal Value for value comparisons or the "table.column" string for
// column-column (join/correlated) comparisons; agg_column names the
// aggregate argument for HAVING NAME(...) refs ("" for COUNT(*)).
Condition :: struct {
	column     : string,
	operator   : Token_Type,
	negated    : bool, // col NOT IN (...) / col NOT LIKE 'x'
	agg_column : string, // aggregate argument for NAME(...) refs ("" for COUNT(*))
	// rhs: types.Value for literal comparisons, string for column-column comparisons (e.g. t1.a = t2.b)
	rhs        : union {
		types.Value,
		string,
	},
	in_values  : []types.Value, // IN (val1, val2, ...)
	in_subquery: ^Select_Stmt, // IN (SELECT ...); arena-owned with the statement
}

// Where_Kind tags a Where_Node: a leaf predicate (COND), an n-ary
// conjunction/disjunction (AND/OR), or a negation (NOT, single child).
Where_Kind :: enum u8 {
	COND,
	AND,
	OR,
	NOT,
}

// Where_Node is one node of the filter tree: cond when kind is COND,
// children when AND/OR/NOT. Filters evaluate the tree recursively;
// children are arena-owned pointers, never freed piecemeal.
Where_Node :: struct {
	kind    : Where_Kind,
	cond    : Condition, // valid when kind == .COND
	children: [dynamic]^Where_Node, // valid when kind == .AND or .OR (n-ary)
}

// Where_Clause is a statement's filter: root nil means no filter (every row
// matches). Maybes of this (where_clause/having/on_clause) distinguish
// "no clause" from "clause matching everything".
Where_Clause :: struct {
	root: ^Where_Node, // nil = no filter (always true)
}

// Create_Stmt is CREATE TABLE: name, column defs (types/defaults/checks),
// and foreign keys (validated + enforced by the executor, not the parser).
Create_Stmt :: struct {
	table_name  : string,
	columns     : []types.Column,
	foreign_keys: []Foreign_Key,
}

// Create_Index_Stmt is `CREATE INDEX name ON table (column)`: single-column
// indexes over TEXT columns only (enforced in exec_create_index).
Create_Index_Stmt :: struct {
	index_name: string,
	table_name: string,
	column    : string,
}

// Foreign_Key is one REFERENCES clause: local column, referenced table,
// referenced column. Name resolution and enforcement are executor-side.
Foreign_Key :: struct {
	col      : string,
	ref_table: string,
	ref_col  : string,
}

// Insert_Stmt is INSERT INTO: target table, optional column list (empty =
// positional, in table order), and one value row per VALUES (...) group.
Insert_Stmt :: struct {
	table_name: string,
	columns   : []string,
	values    : [][]types.Value, // one row of values per VALUES (...) group
}

// Order_By_Column is one ORDER BY key: column, direction, and NULL
// placement (default is nulls-last for ASC, nulls-first for DESC —
// nulls_first overrides when ORDER BY ... NULLS FIRST/LAST is given).
Order_By_Column :: struct {
	column     : string,
	desc       : bool,
	nulls_first: bool,
}

// Aggregate_Func is the supported aggregate set: COUNT/SUM/AVG/MIN/MAX.
// Anything else fails at parse (not the executor).
Aggregate_Func :: enum u8 {
	COUNT,
	SUM,
	AVG,
	MIN,
	MAX,
}

// Aggregate_Expr is one aggregate call: function + argument column
// ("" for COUNT(*)).
Aggregate_Expr :: struct {
	func  : Aggregate_Func,
	column: string,
}

// Select_Column_Kind tags each entry of Select_Stmt.columns so the executor
// can distinguish aggregate slots from literal slots (e.g. SELECT 0,
// COUNT(*)) and bare columns (still a clean error beside aggregates).
Select_Column_Kind :: enum u8 {
	COLUMN,
	LITERAL,
	AGGREGATE,
}

// Join_Type is the supported join set: INNER (with ON), CROSS (no ON),
// LEFT (unmatched left rows survive, right padded NULL), RIGHT (mirror).
Join_Type :: enum u8 {
	INNER,
	CROSS,
	LEFT,
	RIGHT,
}

// Join_Clause is one JOIN arm: kind, right-hand source (table or subquery
// with alias), and the ON filter (absent for CROSS JOIN).
Join_Clause :: struct {
	join_type: Join_Type,
	source   : From_Source,
	alias    : string,
	on_clause: Maybe(Where_Clause),
}

// From_Source is a query's row source: a table name, a parenthesized
// subquery, or No_From for literal-only SELECTs.
From_Source :: union {
	string,
	^Select_Stmt,
	No_From,
}

// No_From marks a FROM-less SELECT whose columns are literal expressions
// (e.g. `SELECT 1, 'a'`), producing a single row.
No_From :: struct {}

Join_Source_Result :: struct {
	source : From_Source,
	alias  : string,
	success: bool,
}

// Select_Stmt is a parsed SELECT: row source(s), projected columns with
// parallel alias/kind/literal-index arrays, aggregates, DISTINCT, and the
// WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET clauses. AS OF pins the read
// to one snapshot (id or timestamp micros). columns empty means SELECT *;
// literal_values holds the constant column values for literal projections.
Select_Stmt :: struct {
	from           : From_Source, // table name string, subquery ^Select_Stmt, or No_From
	from_alias     : string, // e.g. "FROM t AS a" sets from_alias = "a"
	joins          : []Join_Clause,
	columns        : []string, // projected column names; empty = *
	aliases        : []string, // parallel to columns: AS alias or "" when none
	literal_values : []types.Value, // literal column values (FROM-less + mixed)
	col_kinds      : []Select_Column_Kind, // parallel to columns
	col_literal_idx: []int, // parallel to columns: index into literal_values for LITERAL, -1 otherwise
	aggregates     : []Aggregate_Expr,
	is_distinct    : bool,
	where_clause   : Maybe(Where_Clause),
	order_by       : Maybe([]Order_By_Column),
	limit          : Maybe(u64),
	offset         : Maybe(u64),
	group_by       : []string,
	having         : Maybe(Where_Clause),
	as_of_snapshot : Maybe(u64), // AS OF SNAPSHOT <id>
	as_of_timestamp: Maybe(u64), // AS OF TIMESTAMP <micros>
}

// Update_Stmt is UPDATE table SET col=val,... [WHERE ...]: parallel
// column/value arrays (same length), plus the optional filter.
Update_Stmt :: struct {
	table_name    : string,
	update_columns: []string,
	update_values : []types.Value,
	where_clause  : Maybe(Where_Clause),
}

// Delete_Stmt is DELETE FROM table [WHERE ...].
Delete_Stmt :: struct {
	table_name  : string,
	where_clause: Maybe(Where_Clause),
}

// Drop_Stmt is DROP TABLE name. No IF EXISTS — absent tables are an error.
Drop_Stmt :: struct {
	table_name: string,
}

// Drop_Index_Stmt is `DROP INDEX name [ON table]` — resolves the owning
// table by stored index name (unique match, or the ON qualifier when the
// name repeats across tables).
Drop_Index_Stmt :: struct {
	index_name: string,
	table_name: Maybe(string),
}

// Txn_Op is BEGIN/COMMIT/ROLLBACK. Parse-level only carries the op;
// isolation behavior lives in db/txn.odin.
Txn_Op :: enum u8 {
	BEGIN,
	COMMIT,
	ROLLBACK,
}

// Txn_Stmt wraps one transaction keyword.
Txn_Stmt :: struct {
	op: Txn_Op,
}

// Set_Op is UNION/INTERSECT/EXCEPT plus their ALL forms (ALL keeps
// duplicates; bare form deduplicates).
Set_Op :: enum u8 {
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
	op    : Set_Op,
}

// Compound_Stmt is a chain of SELECTs combined with UNION / INTERSECT / EXCEPT.
// `first` is the leftmost SELECT; `operands` hold the rest, each with the
// operator that connects it to the accumulated result. `order_by`/`limit`/
// `offset` apply to the combined result.
Compound_Stmt :: struct {
	first   : ^Select_Stmt,
	operands: []Set_Operand,
	order_by: Maybe([]Order_By_Column),
	limit   : Maybe(u64),
	offset  : Maybe(u64),
}

Statement_Variant :: union {
	Create_Stmt,
	Create_Index_Stmt,
	Insert_Stmt,
	Select_Stmt,
	Compound_Stmt,
	Update_Stmt,
	Delete_Stmt,
	Drop_Stmt,
	Drop_Index_Stmt,
	Txn_Stmt,
	Explain_Stmt,
}

// Statement is one parsed statement: the variant plus the original SQL
// text (cloned — the executor re-displays it in EXPLAIN/plan output).
Statement :: struct {
	type: Statement_Variant,
	sql : string,
}

// Explain_Stmt is EXPLAIN <sql>: the inner text, planned and rendered as a
// one-row text result by the executor (never executed).
Explain_Stmt :: struct {
	sql: string,
}

// Parser is the parse cursor: token slice, position, first error message,
// and a nesting counter capped at MAX_PARSE_NESTING (recursive
// SELECT/subquery/set-op descent rejects past 512 levels instead of
// overflowing the native stack).
Parser :: struct {
	tokens    : []Token,
	current   : int,
	err_msg   : string,
	nest_depth: int,
}
