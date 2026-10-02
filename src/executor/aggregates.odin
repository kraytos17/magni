package executor

import "core:bytes"
import "core:log"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

@(private)
find_existing_group :: proc(
	buckets: ^Fp_Buckets,
	groups: []Group,
	hash: u64,
	row_entry: Row_Entry,
	group_by_indices: []int,
) -> (
	int,
	bool,
) {
	h, ok := fp_buckets_probe(buckets, hash)
	if !ok { return -1, false }
	for n := h; n != -1; n = buckets.next[n] {
		gi := buckets.rows[n]
		if values_equal_by_indices(
			row_entry.values,
			groups[gi].key_values,
			group_by_indices,
		) {
			return gi, true
		}
	}
	return -1, false
}

// build_groups resolves GROUP BY column indices and partitions rows into
// groups (a single implicit group when there is no GROUP BY). Shared by the
// printing and data aggregate evaluators.
@(private)
build_groups :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	groups: [dynamic]Group,
	group_by_indices: []int,
	ok: bool,
) {
	group_by_indices = make([]int, len(stmt.group_by), context.temp_allocator)
	resolver := build_column_resolver(combined_cols, table_ranges)
	for col, i in stmt.group_by {
		idx, col_ok := resolve(resolver, col)
		if !col_ok {
			log.errorf("Error: Unknown column in GROUP BY: %s", col)
			return nil, nil, false
		}
		group_by_indices[i] = idx
	}
	// Unknown aggregate arguments (e.g. SUM(nosuchcol)) are a clean error,
	// like unknown WHERE/GROUP BY columns — never silent NULLs. COUNT(*)
	// takes no column and always resolves.
	for agg in stmt.aggregates {
		if agg.column == "" { continue }
		if _, col_ok := resolve(resolver, agg.column); !col_ok {
			log.errorf("Error: Unknown column in aggregate: %s", agg.column)
			return nil, nil, false
		}
	}

	groups = make([dynamic]Group, context.temp_allocator)
	group_map := fp_buckets_make(0, context.temp_allocator)
	defer fp_buckets_destroy(&group_map)
	for row_entry, _ in rows {
		if len(group_by_indices) == 0 {
			if len(groups) == 0 {
				append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
			}
			append(&groups[0].rows, row_entry)
		} else {
			hash := group_key_hash(row_entry.values, group_by_indices)
			gi, exists := find_existing_group(
				&group_map,
				groups[:],
				hash,
				row_entry,
				group_by_indices,
			)
			if exists {
				append(&groups[gi].rows, row_entry)
			} else {
				key_vals := make([]types.Value, len(group_by_indices), context.temp_allocator)
				for col_idx, pos in group_by_indices {
					key_vals[pos] = row_entry.values[col_idx]
				}

				new_grp_rows := make([dynamic]Row_Entry, context.temp_allocator)
				append(&new_grp_rows, row_entry)
				fp_buckets_add(&group_map, hash, len(groups))
				append(&groups, Group{key_values = key_vals, rows = new_grp_rows})
			}
		}
	}
	if len(groups) == 0 && len(group_by_indices) == 0 {
		append(&groups, Group{rows = make([dynamic]Row_Entry, context.temp_allocator)})
	}
	return groups, group_by_indices, true
}

// exec_select_aggregate_data evaluates a SELECT with aggregates/GROUP BY and returns
// the result rows as data (group key values followed by aggregate values), without
// printing. `cols` are synthesized from stmt.columns.
@(private)
exec_select_aggregate_data :: proc(
	stmt: parser.Select_Stmt,
	rows: []Row_Entry,
	combined_cols: []types.Column,
	table_ranges: []Table_Col_Range,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	groups, group_by_indices, g_ok := build_groups(stmt, rows, combined_cols, table_ranges)
	if !g_ok { return nil, nil, false }

	result := make([dynamic]Row_Entry, context.temp_allocator)
	for gi in 0 ..< len(groups) {
		group_rows := make([][]types.Value, len(groups[gi].rows), context.temp_allocator)
		for row_entry, ri in groups[gi].rows { group_rows[ri] = row_entry.values }

		agg_vals := compute_aggregates(
			group_rows,
			stmt.aggregates,
			combined_cols,
			context.temp_allocator,
		)
		if having_cl, has_having := stmt.having.?; has_having {
			if !evaluate_where_having(
				having_cl,
				groups[gi].key_values,
				agg_vals,
				stmt.group_by,
				stmt.aggregates,
			) { continue }
		}

		out, proj_ok := project_group_row(stmt, groups[gi].key_values, agg_vals, len(group_by_indices))
		if !proj_ok { return nil, nil, false }
		append(&result, Row_Entry{rowid = types.Row_ID(gi), values = out})
	}

	cols := make([]types.Column, len(stmt.columns), context.temp_allocator)
	for name, i in stmt.columns {
		display := name
		if i < len(stmt.aliases) && stmt.aliases[i] != "" { display = stmt.aliases[i] }

		col_type := types.Column_Type.INTEGER
		if len(stmt.col_kinds) == len(stmt.columns) &&
		   stmt.col_kinds[i] == .LITERAL &&
		   stmt.col_literal_idx[i] >= 0 &&
		   stmt.col_literal_idx[i] < len(stmt.literal_values) {
			col_type = literal_column_type(stmt.literal_values[stmt.col_literal_idx[i]])
		}
		cols[i] = types.Column{name = display, type = col_type}
	}
	return result[:], cols, true
}

// Group_Proj_Cursor walks the two independent value streams a grouped
// projection consumes: aggregate results and group keys.
Group_Proj_Cursor :: struct {
	agg:   int,
	group: int,
}

// project_group_row materializes one group's output row. Kind-dispatched:
// LITERAL slots read their literal (borrowed header, same as group keys and
// MIN/MAX — the statement outlives execution), AGGREGATE slots consume agg_vals
// in order (select-list aggregates first; HAVING-only aggregates appended
// after), COLUMN slots take the next group key. Hand-built statements without
// kinds fall back to the legacy positional path (first N slots are group keys).
@(private="file")
project_group_row :: proc(
	stmt: parser.Select_Stmt,
	key_values: []types.Value,
	agg_vals: []types.Value,
	group_key_count: int,
) -> (
	out: []types.Value,
	ok: bool,
) {
	out = make([]types.Value, len(stmt.columns), context.temp_allocator)
	cur := Group_Proj_Cursor{}
	use_kinds := len(stmt.col_kinds) == len(stmt.columns) &&
		len(stmt.col_literal_idx) == len(stmt.columns)
	for i in 0 ..< len(stmt.columns) {
		if use_kinds {
			#partial switch stmt.col_kinds[i] {
			case .LITERAL:
				li := stmt.col_literal_idx[i]
				if li < 0 || li >= len(stmt.literal_values) {
					return nil, mix_error(stmt.columns[i])
				}

				out[i] = stmt.literal_values[li]
				continue
			case .AGGREGATE:
				if cur.agg >= len(agg_vals) { return nil, mix_error(stmt.columns[i]) }

				out[i] = agg_vals[cur.agg]
				cur.agg += 1
				continue
			case .COLUMN:
				if cur.group < len(key_values) {
					out[i] = key_values[cur.group]
					cur.group += 1
					continue
				}
				return nil, mix_error(stmt.columns[i])
			}
		}
		if cur.group < group_key_count {
			out[i] = key_values[cur.group]
			cur.group += 1
		} else {
			if cur.agg < 0 || cur.agg >= len(agg_vals) {
				return nil, mix_error(stmt.columns[i])
			}

			out[i] = agg_vals[cur.agg]
			cur.agg += 1
		}
	}
	return out, true
}

// mix_error logs the non-aggregate-column-beside-aggregates error and reports
// the shared failure code (always false).
@(private="file")
mix_error :: proc(col: string) -> bool {
	log.errorf("Error: Cannot mix non-aggregate column '%s' with aggregates", col)
	return false
}

// literal_column_type maps a literal value to its display column type.
@(private="file")
literal_column_type :: proc(v: types.Value) -> types.Column_Type {
	#partial switch _ in v {
	case i64:
		return .INTEGER
	case f64:
		return .REAL
	case string:
		return .TEXT
	case []u8:
		return .BLOB
	}
	return .INTEGER
}

// group_key_hash computes a hash over GROUP BY key columns. Implemented via
// hash_values (single FNV-1a with per-type tags); collisions fall back to
// value_compare at the call site.
group_key_hash :: proc(values: []types.Value, indices: []int) -> u64 {
	return hash_values(values, indices)
}

@(fast_math = {.No_NaNs, .No_Infs, .No_Signed_Zeros})
@(private)
compare_values :: proc(a: types.Value, b: types.Value) -> int {
	if types.is_null(a) && types.is_null(b) do return 0
	if types.is_null(a) do return -1
	if types.is_null(b) do return 1

	ra, rb := value_rank(a), value_rank(b)
	if ra != rb do return -1 if ra < rb else 1
	#partial switch va in a {
	case i64:
		#partial switch vb in b {
		case i64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case f64:
			if f64(va) < vb do return -1
			if f64(va) > vb do return 1
			return 0
		}
	case f64:
		#partial switch vb in b {
		case f64:
			if va < vb do return -1
			if va > vb do return 1
			return 0
		case i64:
			if va < f64(vb) do return -1
			if va > f64(vb) do return 1
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.compare(va, vb)
		}
	case []u8:
		if vb, ok := b.([]u8); ok {
			return bytes.compare(va, vb)
		}
	}
	return 0
}

// value_rank orders storage classes for compare_values. Null never reaches
// here (handled above); the fallback keeps the order total for any future
// variant without ever equating distinct classes.
@(private="file")
value_rank :: proc(v: types.Value) -> int {
	if _, ok := v.(i64); ok { return 0 }
	if _, ok := v.(f64); ok { return 0 }
	if _, ok := v.(string); ok { return 1 }
	if _, ok := v.([]u8); ok { return 2 }
	return 3
}

@(private)
build_display_indices :: proc(
	columns: []string,
	resolver: Column_Resolver,
	total_cols: int,
) -> (
	[]int,
	bool,
) {
	indices := make([dynamic]int, context.temp_allocator)
	if len(columns) == 0 {
		for i in 0 ..< total_cols {
			append(&indices, i)
		}
	} else {
		for req_col in columns {
			idx, ok := resolve(resolver, req_col)
			if !ok {
				log.errorf("Error: Unknown column: %s", req_col)
				return nil, false
			}
			append(&indices, idx)
		}
	}
	return indices[:], true
}

// Agg_Input is one resolved aggregate argument: the group's rows plus the
// column index (-1 for COUNT(*) or unresolvable columns).
Agg_Input :: struct {
	rows:    [][]types.Value,
	col_idx: int,
}

// resolve_agg_input maps an aggregate's column name to its index.
@(private="file")
resolve_agg_input :: proc(
	rows: [][]types.Value,
	agg: parser.Aggregate_Expr,
	columns: []types.Column,
) -> Agg_Input {
	if agg.column == "" { return {rows, -1} }

	idx, found := schema.find_column_index(columns, agg.column)
	if !found { idx = -1 }
	return {rows, idx}
}

// sum_count folds the numeric non-NULL values of one column into a total and
// count. Shared by SUM and AVG.
@(fast_math = {
	.Allow_Reassoc,
	.No_NaNs,
	.No_Infs,
	.No_Signed_Zeros,
	.Allow_Reciprocal,
	.Allow_Contract,
	.Approx_Func,
})
@(private="file")
sum_count :: proc(ai: Agg_Input) -> (sum: f64, count: int) {
	for row_vals in ai.rows {
		if ai.col_idx >= 0 && !types.is_null(row_vals[ai.col_idx]) {
			#partial switch v in row_vals[ai.col_idx] {
			case i64:
				sum += f64(v); count += 1
			case f64:
				sum += v; count += 1
			}
		}
	}
	return sum, count
}

Extremum_Dir :: enum u8 {
	Min,
	Max,
}

// extremum returns the MIN/MAX non-NULL value of one column, or NULL when
// the group is empty or the column is unresolvable.
@(private="file")
extremum :: proc(ai: Agg_Input, dir: Extremum_Dir) -> types.Value {
	if len(ai.rows) == 0 || ai.col_idx < 0 {
		return types.value_null()
	}

	best := ai.rows[0][ai.col_idx]
	for row_vals in ai.rows {
		if ai.col_idx >= 0 && !types.is_null(row_vals[ai.col_idx]) {
			cmp := compare_values(row_vals[ai.col_idx], best)
			if (dir == .Min && cmp < 0) || (dir == .Max && cmp > 0) {
				best = row_vals[ai.col_idx]
			}
		}
	}
	return best
}

@(fast_math = {
	.Allow_Reassoc,
	.No_NaNs,
	.No_Infs,
	.No_Signed_Zeros,
	.Allow_Reciprocal,
	.Allow_Contract,
	.Approx_Func,
})
@(private)
compute_aggregates :: proc(
	rows: [][]types.Value,
	aggregates: []parser.Aggregate_Expr,
	columns: []types.Column,
	allocator := context.temp_allocator,
) -> []types.Value {
	results := make([]types.Value, len(aggregates), allocator)
	for agg, i in aggregates {
		ai := resolve_agg_input(rows, agg, columns)
		switch agg.func {
		case .COUNT:
			if agg.column == "" {
				results[i] = types.value_int(i64(len(ai.rows)))
			} else {
				count := 0
				for row_vals in ai.rows {
					if ai.col_idx >= 0 && !types.is_null(row_vals[ai.col_idx]) {
						count += 1
					}
				}
				results[i] = types.value_int(i64(count))
			}
		case .SUM:
			sum, _ := sum_count(ai)
			results[i] = types.value_real(sum)
		case .AVG:
			sum, count := sum_count(ai)
			results[i] = types.value_real(sum / f64(count)) if count > 0 else types.value_null()
		case .MIN:
			results[i] = extremum(ai, .Min)
		case .MAX:
			results[i] = extremum(ai, .Max)
		}
	}
	return results
}

@(private)
evaluate_where_having :: proc(
	clause: parser.Where_Clause,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	if clause.root == nil { return true }
	return evaluate_having_node(clause.root, group_keys, agg_values, group_cols, aggregates)
}

// aggregate_func_name maps an aggregate to its HAVING reference name
// (lowercase; HAVING count and HAVING COUNT both match via equal_fold).
@(private="file")
aggregate_func_name :: proc(func: parser.Aggregate_Func) -> string {
	switch func {
	case .COUNT:
		return "count"
	case .SUM:
		return "sum"
	case .AVG:
		return "avg"
	case .MIN:
		return "min"
	case .MAX:
		return "max"
	}
	return ""
}

// find_having_aggregate returns the computed value of the aggregate a HAVING
// condition names (case-insensitive), or false.
@(private="file")
find_having_aggregate :: proc(
	column: string,
	aggregates: []parser.Aggregate_Expr,
	agg_values: []types.Value,
) -> (types.Value, bool) {
	for agg, i in aggregates {
		if strings.equal_fold(column, aggregate_func_name(agg.func)) {
			return agg_values[i], true
		}
	}
	return {}, false
}

// resolve_having_value finds the group-key or aggregate value a HAVING
// condition names, without requiring an rhs. Shared lookup for the IS NULL
// branch; the comparison path keeps its inline form.
@(private="file")
resolve_having_value :: proc(
	cond: parser.Condition,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> (types.Value, bool) {
	for col, i in group_cols {
		if col == cond.column {
			return group_keys[i], true
		}
	}
	return find_having_aggregate(cond.column, aggregates, agg_values)
}

@(private="file")
evaluate_having_node :: proc(
	node: ^parser.Where_Node,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	switch node.kind {
	case .COND:
		return evaluate_having_condition(node.cond, group_keys, agg_values, group_cols, aggregates)
	case .AND:
		for child in node.children {
			if !evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates) {
				return false
			}
		}
		return true
	case .OR:
		for child in node.children {
			if evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates) {
				return true
			}
		}
		return false
	case .NOT:
		for child in node.children {
			return !evaluate_having_node(child, group_keys, agg_values, group_cols, aggregates)
		}
		return false
	}
	return false
}

@(private="file")
evaluate_having_condition :: proc(
	cond: parser.Condition,
	group_keys: []types.Value,
	agg_values: []types.Value,
	group_cols: []string,
	aggregates: []parser.Aggregate_Expr,
) -> bool {
	// IS [NOT] NULL tests nullness of the resolved value instead of
	// comparing against an rhs (which IS conditions never carry).
	if cond.operator == .IS {
		cond_result := false
		if v, ok := resolve_having_value(cond, group_keys, agg_values, group_cols, aggregates); ok {
			cond_result = types.is_null(v)
		}
		if cond.negated { cond_result = !cond_result }
		return cond_result
	}

	cond_result := false
	rhs_val, rhs_is_val := cond.rhs.(types.Value)
	for col, i in group_cols {
		if col == cond.column {
			if rhs_is_val {
				cond_result = compare_condition(group_keys[i], cond.operator, rhs_val)
			}
			break
		}
	}
	if !cond_result {
		if v, ok := find_having_aggregate(cond.column, aggregates, agg_values); ok && rhs_is_val {
			cond_result = compare_condition(v, cond.operator, rhs_val)
		}
	}
	if cond.negated { cond_result = !cond_result }
	return cond_result
}
