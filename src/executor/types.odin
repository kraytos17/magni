package executor

import "core:mem"
import "core:strings"
import "src:btree"
import "src:parser"
import "src:types"

Table_Info :: struct {
	table  : types.Table, // physical table metadata (for FROM table sources)
	tree   : btree.Tree, // data b-tree for this table
	virtual: Maybe(Virtual_Table), // set when FROM source is a subquery instead of a physical table
}

Table_Col_Range :: struct {
	table_name: string,
	start_col : int, // first column index in the combined columns array
	col_count : int,
}

Table_Context :: struct {
	info : Table_Info,
	range: Table_Col_Range,
}

// Join_Build captures the assembled state of a FROM+JOINs query: resolved
// table contexts, combined column metadata, and (after execution) rows.
// resolver indexes cols/ranges once so ON-clause resolution is O(1).
Join_Build :: struct {
	ctxs      : []Table_Context,
	ranges    : []Table_Col_Range,
	cols      : []types.Column,
	resolver  : Column_Resolver,
	rows      : []Row_Entry,
	total_cols: int,
	ok        : bool,
}

Row_Entry :: struct {
	rowid : types.Row_ID,
	values: []types.Value,
}

Virtual_Table :: struct {
	columns: []types.Column,
	rows   : []Row_Entry,
}

Sort_Ctx :: struct {
	order_clause: []parser.Order_By_Column,
	sort_indices: []int,
}

Group :: struct {
	key_values: []types.Value,
	rows      : [dynamic]Row_Entry,
}

Mutated_Table_Info :: struct #all_or_none {
	name: string,
	root: u32,
}

// Pending_Roots stages unpublished data roots inside an explicit txn: DML
// updates the map (plus the table-cache overlay) instead of COW-writing the
// schema leaf per statement, and COMMIT flushes one schema COW per dirty
// table. Readers need no changes — the overlay keeps find_table_cached
// serving pending roots transparently. Autocommit never stages (nil pending
// ⇒ immediate publish, exactly as before).
// Index_Stage is one staged secondary-index root. Names are owned by
// Pending_Roots.alloc (cloned at stage, freed at clear/drop).
Index_Stage :: struct {
	name: string, // index name
	root: u32, // pending index root
}

Pending_Roots :: struct {
	roots      : map[string]u32, // table name → pending data root
	// Secondary text index roots ride beside data roots through
	// stage/flush/clear/drop/overlay, one stage per index.
	index_roots: map[string][dynamic]Index_Stage, // table name → staged index roots
	alloc      : mem.Allocator, // owns key clones + maps; set on first stage
}

// pending_stage records a new data root. The name is cloned: callers pass
// statement-borrowed strings but the map outlives the statement. Re-staging
// the same table overwrites the value without a second clone.
pending_stage :: proc(
	p: ^Pending_Roots,
	table_name: string,
	root: u32,
	allocator := context.allocator,
) {
	if p.roots == nil {
		p.roots = make(map[string]u32, 8, allocator)
		p.alloc = allocator
	}
	if table_name in p.roots {
		p.roots[table_name] = root
		return
	}
	p.roots[strings.clone(table_name, p.alloc)] = root
}

// pending_stage_index records a new secondary-index root: same ownership
// contract as pending_stage, keyed by table, matched by index name.
pending_stage_index :: proc(
	p: ^Pending_Roots,
	table_name: string,
	index_name: string,
	root: u32,
	allocator := context.allocator,
) {
	if p.index_roots == nil {
		p.index_roots = make(map[string][dynamic]Index_Stage, 8, allocator)
		p.alloc = allocator
	}
	if table_name in p.index_roots {
		stages := &p.index_roots[table_name]
		for &st in stages {
			if st.name == index_name {
				st.root = root
				return
			}
		}

		append(stages, Index_Stage{name = strings.clone(index_name, p.alloc), root = root})
		return
	}

	list := make([dynamic]Index_Stage, 0, 1, p.alloc)
	append(&list, Index_Stage{name = strings.clone(index_name, p.alloc), root = root})
	p.index_roots[strings.clone(table_name, p.alloc)] = list
}

// pending_drop forgets staged roots (DROP TABLE in txn). No-op when absent.
pending_drop :: proc(p: ^Pending_Roots, table_name: string) {
	if p.roots == nil && p.index_roots == nil {
		return
	}
	for k in p.roots {
		if k == table_name {
			delete(k, p.alloc)
			delete_key(&p.roots, k)
			break
		}
	}
	for k, &stages in p.index_roots {
		if k == table_name {
			for st in stages {
				delete(st.name, p.alloc)
			}

			delete(stages)
			delete(k, p.alloc)
			delete_key(&p.index_roots, k)
			return
		}
	}
}

// pending_drop_index forgets one staged INDEX root (DROP INDEX in txn).
// The table survives, so staged DATA roots are kept — pending_drop would
// wrongly discard them along with the definition. No-op when absent.
pending_drop_index :: proc(p: ^Pending_Roots, table_name: string, index_name: string) {
	if p.index_roots == nil {
		return
	}
	if table_name not_in p.index_roots {
		return
	}

	stages := &p.index_roots[table_name]
	for i in 0 ..< len(stages) {
		if stages[i].name == index_name {
			delete(stages[i].name, p.alloc)
			ordered_remove(stages, i)
			return
		}
	}
}

// pending_clear frees all staged entries. Called at COMMIT (after flush) and
// ROLLBACK (discard).
pending_clear :: proc(p: ^Pending_Roots) {
	if p.roots != nil {
		for k in p.roots {
			delete(k, p.alloc)
		}

		delete(p.roots)
		p.roots = nil
	}
	if p.index_roots != nil {
		for k, &stages in p.index_roots {
			for st in stages {
				delete(st.name, p.alloc)
			}

			delete(stages)
			delete(k, p.alloc)
		}

		delete(p.index_roots)
		p.index_roots = nil
	}
}

// Result captures the outcome of executing a statement: a result set for
// SELECT/Compound (is_select + rows/cols), or affected-table info for DML.
// It is data-only — rendering is the caller's responsibility.
Result :: struct {
	rows     : []Row_Entry,
	cols     : []types.Column,
	is_select: bool,
	mutated  : Mutated_Table_Info,
	new_root : u32,
}

Resolved_Condition :: struct {
	col_idx      : int, // column index in the row's values array
	operator     : parser.Token_Type,
	negated      : bool, // col NOT IN (...) / col NOT LIKE 'x'
	rhs          : types.Value, // compared value (ignored if has_right_col or has_in)
	has_right_col: bool, // true → rhs is another column at right_idx
	right_idx    : int,
	has_in       : bool, // true → consult in_mem instead of rhs
	in_mem       : In_Membership, // resolved IN membership (see below)
	in_subquery  : ^parser.Select_Stmt, // unresolved IN subquery for per-row fallback
}

// In_Kind names which membership source an IN condition resolved to.
In_Kind :: enum u8 {
	None,
	Values, // literal IN list (+ fingerprint prefilter)
	Subquery, // materialized IN (SELECT ...) results
}

// In_Membership bundles one IN condition's resolved state: its source kind,
// the candidate values, and the sorted fingerprint prefilter over Values
// (empty = linear scan, e.g. hand-built nodes). Values borrows the parser's
// list; Subquery results are owned (made at resolve time).
In_Membership :: struct {
	kind  : In_Kind,
	values: []types.Value,
	fps   : []u64,
}

Where_Eval_Ctx :: struct {
	root       : ^Resolved_Node, // nil = no filter (always true)
	schema_tree: ^btree.Tree,
}

// Mutation_Filter holds a DML statement's optional row filter plus its
// once-resolved evaluation context. Embedded (via `using`) in Update_Plan
// and Delete_Plan so both verbs share one filter implementation instead of
// parallel eval_plan_filter / eval_delete_filter twins.
Mutation_Filter :: struct {
	filter    : Maybe(parser.Where_Clause),
	filter_ctx: Maybe(Where_Eval_Ctx),
}

// Scan_Plan captures the resolved state for one table scan: optional filter
// context (nil = no filter), skip-index bounds, and row limit. Built once by
// build_scan_plan, consumed by the cursor loop in scan_table.
Scan_Plan :: struct {
	filter    : Maybe(Where_Eval_Ctx),
	skip_start: u32,
	skip_end  : u32,
	max_rows  : Maybe(u64),
}

Resolved_Node_Kind :: enum u8 {
	COND,
	AND,
	OR,
	NOT,
}

Resolved_Node :: struct {
	kind    : Resolved_Node_Kind,
	cond    : Resolved_Condition, // valid when kind == .COND
	children: []^Resolved_Node, // valid when kind == .AND or .OR (n-ary)
}
