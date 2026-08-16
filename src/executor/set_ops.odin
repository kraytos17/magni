package executor

import "core:fmt"
import "src:btree"
import "src:parser"
import "src:schema"
import "src:types"

// exec_select_data evaluates a SELECT and returns its result rows and columns
// WITHOUT printing, covering FROM-less literals, single-table selects, subqueries,
// joins, and aggregates. This is the operand evaluator for set operations.
@(private="file")
exec_select_data :: proc(
	t: ^btree.Tree,
	stmt: parser.Select_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	if _, is_nf := stmt.from.(parser.No_From); is_nf {
		return exec_select_literals(t, stmt)
	}
	if _, is_subq := stmt.from.(^parser.Select_Stmt); is_subq {
		return exec_subquery_data(t, stmt, cache)
	}
	if len(stmt.aggregates) > 0 || len(stmt.group_by) > 0 || stmt.having != nil {
		// Single-table aggregate/GROUP BY: route through exec_select_single_data
		// which delegates aggregates to exec_select_aggregate_data.
		return exec_select_single_data(t, stmt, cache)
	}
	if len(stmt.joins) > 0 {
		return exec_select_join_data(t, stmt, cache)
	}
	return exec_select_single_data(t, stmt, cache)
}

// row_equal compares two rows for value equality (column count must match).
@(private="file")
row_equal :: proc(a, b: Row_Entry) -> bool {
	if len(a.values) != len(b.values) { return false }
	for i in 0 ..< len(a.values) {
		if !types.value_compare(a.values[i], b.values[i]) { return false }
	}
	return true
}

// union_all_op appends b to a without dedup.
@(private="file")
union_all_op :: proc(a: ^[dynamic]Row_Entry, b: []Row_Entry) {
	append(a, ..b)
}

// union_op appends b to a, deduplicating the combined result (distinct rows kept once).
@(private="file")
union_op :: proc(a: ^[dynamic]Row_Entry, b: []Row_Entry) {
	seen := make(map[u64]bool, len(a^) + len(b), context.temp_allocator)
	for r in a^ {
		seen[row_fingerprint(r.values)] = true
	}
	for r in b {
		fp := row_fingerprint(r.values)
		if fp in seen { continue }

		seen[fp] = true
		append(a, r)
	}

	// Dedup a's own starting rows (accumulator may begin with duplicates).
	out := dedup_rows(a^[:])
	clear(a)
	append(a, ..out)
}

// intersect keeps rows present in both a and b, deduplicated. Builds a
// fingerprint index of b once, then probes it per row of a.
intersect :: proc(a: []Row_Entry, b: []Row_Entry) -> []Row_Entry {
	index := make(map[u64][dynamic]int, len(b), context.temp_allocator)
	defer {
		for _, bucket in index { delete(bucket) }
		delete(index)
	}
	for i in 0 ..< len(b) {
		fp := row_fingerprint(b[i].values)
		bucket := index[fp]
		append(&bucket, i)
		index[fp] = bucket
	}

	out := make([dynamic]Row_Entry, context.temp_allocator)
	for ra in a {
		fp := row_fingerprint(ra.values)
		if bucket, has := index[fp]; has {
			for bi in bucket {
				if row_equal(ra, b[bi]) {
					append(&out, ra)
					break
				}
			}
		}
	}
	return dedup_rows(out[:])
}

// intersect_all keeps the multiset intersection: each distinct row repeated
// min(count_a, count_b) times.
@(private="file")
intersect_all :: proc(a: []Row_Entry, b: []Row_Entry) -> []Row_Entry {
	count_b := make(map[u64]int, len(b), context.temp_allocator)
	seen_b := make(map[u64][dynamic]int, len(b), context.temp_allocator)
	defer {
		for _, bucket in seen_b { delete(bucket) }
		delete(seen_b)
		delete(count_b)
	}
	for i in 0 ..< len(b) {
		fp := row_fingerprint(b[i].values)
		count_b[fp] += 1
		bucket := seen_b[fp]
		append(&bucket, i)
		seen_b[fp] = bucket
	}

	out := make([dynamic]Row_Entry, context.temp_allocator)
	emitted := make(map[u64]int, len(a), context.temp_allocator)
	defer delete(emitted)
	for ra in a {
		fp := row_fingerprint(ra.values)
		if emitted[fp] >= count_b[fp] { continue }

		// verify value equality with a matching row in b
		matched := false
		if bucket, has := seen_b[fp]; has {
			for bi in bucket {
				if row_equal(ra, b[bi]) { matched = true; break }
			}
		}
		if matched {
			emitted[fp] += 1
			append(&out, ra)
		}
	}
	return out[:]
}

// except keeps rows in a not present in b, deduplicated. O(n+m): builds a
// fingerprint index of b once, then probes it per row of a.
except :: proc(a: []Row_Entry, b: []Row_Entry) -> []Row_Entry {
	index := make(map[u64][dynamic]int, len(b), context.temp_allocator)
	defer {
		for _, bucket in index { delete(bucket) }
		delete(index)
	}
	for i in 0 ..< len(b) {
		fp := row_fingerprint(b[i].values)
		bucket := index[fp]
		append(&bucket, i)
		index[fp] = bucket
	}

	out := make([dynamic]Row_Entry, context.temp_allocator)
	for ra in a {
		fp := row_fingerprint(ra.values)
		found := false
		if bucket, has := index[fp]; has {
			for bi in bucket {
				if row_equal(ra, b[bi]) { found = true; break }
			}
		}
		if !found { append(&out, ra) }
	}
	return dedup_rows(out[:])
}

// except_all keeps the multiset difference: each distinct row repeated
// max(count_a - count_b, 0) times.
@(private="file")
except_all :: proc(a: []Row_Entry, b: []Row_Entry) -> []Row_Entry {
	count_b := make(map[u64]int, len(b), context.temp_allocator)
	for rb in b {
		count_b[row_fingerprint(rb.values)] += 1
	}

	emitted := make(map[u64]int, len(a), context.temp_allocator)
	out := make([dynamic]Row_Entry, context.temp_allocator)
	for ra in a {
		fp := row_fingerprint(ra.values)
		if emitted[fp] < count_b[fp] {
			emitted[fp] += 1
			continue
		}
		append(&out, ra)
	}
	return out[:]
}

// apply_set_op reduces the accumulator with one operand per its operator.
@(private="file")
apply_set_op :: proc(
	acc: ^[dynamic]Row_Entry,
	op: parser.Set_Op,
	other: []Row_Entry,
) {
	switch op {
	case .UNION:
		union_op(acc, other)
	case .UNION_ALL:
		union_all_op(acc, other)
	case .INTERSECT, .INTERSECT_ALL:
		inter := intersect(acc[:], other) if op == .INTERSECT else intersect_all(acc[:], other)
		clear(acc)
		append(acc, ..inter)
	case .EXCEPT, .EXCEPT_ALL:
		diff := except(acc[:], other) if op == .EXCEPT else except_all(acc[:], other)
		clear(acc)
		append(acc, ..diff)
	}
}

// exec_compound_data evaluates a chain of SELECTs combined by set operations and
// returns the result rows/columns without printing. Precedence: INTERSECT binds
// tighter than UNION/EXCEPT; all left-associative. The first operand's columns
// define the output header.
exec_compound_data :: proc(
	t: ^btree.Tree,
	compound: parser.Compound_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	[]types.Column,
	bool,
) {
	acc_rows, acc_cols, ok := exec_select_data(t, compound.first^, cache)
	if !ok { return nil, nil, false }
	// Precedence: INTERSECT binds tighter than UNION/EXCEPT; all are
	// left-associative. Reduce in two phases:
	//   Phase 1: evaluate each maximal run of consecutive INTERSECT ops into a
	//            single segment (segment result = first SELECT, then left-assoc
	//            INTERSECT within the run).
	//   Phase 2: fold the segment results left-to-right with UNION/EXCEPT.
	// Example: A UNION B INTERSECT C EXCEPT D
	//   segments = [A], [B INTERSECT C], [D]
	//   reduce   = ((A UNION (B INTERSECT C)) EXCEPT D)
	segments := make([dynamic][dynamic]Row_Entry, context.temp_allocator)
	segment_ops := make([dynamic]parser.Set_Op, context.temp_allocator) // op joining segment[i] to segment[i-1]
	// Initialize segment 0 from the first SELECT.
	seg0 := make([dynamic]Row_Entry, 0, len(acc_rows), context.temp_allocator)
	append(&seg0, ..acc_rows)
	append(&segments, seg0)

	i := 0
	for i < len(compound.operands) {
		op := compound.operands[i].op
		other_rows, other_cols, other_ok := exec_select_data(t, compound.operands[i].select^, cache)

		if !other_ok { return nil, nil, false }
		// Set operations require equal column counts across operands.
		if len(other_cols) != len(acc_cols) {
			return nil, nil, false
		}

		is_intersect := op == .INTERSECT || op == .INTERSECT_ALL
		// Determine whether this operand joins the current segment (INTERSECT)
		// or starts a new segment (UNION/EXCEPT).
		if len(segment_ops) == 0 && !is_intersect {
			// First operand op is UNION/EXCEPT: start a new segment.
			append(&segment_ops, op)
			new_seg := make([dynamic]Row_Entry, 0, len(other_rows), context.temp_allocator)
			append(&new_seg, ..other_rows)
			append(&segments, new_seg)
			i += 1
			continue
		}
		if is_intersect {
			// Join the current (last) segment via INTERSECT.
			last := &segments[len(segments) - 1]
			apply_set_op(last, op, other_rows)
		} else {
			// UNION/EXCEPT: start a new segment and record the joining op.
			append(&segment_ops, op)
			new_seg := make([dynamic]Row_Entry, 0, len(other_rows), context.temp_allocator)
			append(&new_seg, ..other_rows)
			append(&segments, new_seg)
		}
		i += 1
	}

	// Phase 2: fold segments left-to-right with UNION/EXCEPT ops.
	acc := segments[0]
	for si := 1; si < len(segments); si += 1 {
		apply_set_op(&acc, segment_ops[si - 1], segments[si][:])
	}

	rows := acc[:]
	// Compound-level ORDER BY / LIMIT / OFFSET.
	if order_clause, has_o := compound.order_by.?; has_o && len(order_clause) > 0 {
		range0 := []Table_Col_Range {{table_name = "", start_col = 0, col_count = len(acc_cols)}}
		if !sort_rows(rows, order_clause, acc_cols, range0) { return nil, nil, false }
	}
	if limit, has_limit := compound.limit.?; has_limit {
		off := u64(0)
		if o, has_off := compound.offset.?; has_off { off = o }

		start := int(min(off, u64(len(rows))))
		end := int(min(off + limit, u64(len(rows))))
		rows = rows[start:end]
	}
	return rows, acc_cols, true
}

// exec_compound evaluates a compound SELECT and prints the result.
@(private="file")
exec_compound :: proc(t: ^btree.Tree, compound: parser.Compound_Stmt) -> bool {
	rows, acc_cols, ok := exec_compound_data(t, compound)
	if !ok { return false }

	col_names := make([]string, len(acc_cols), context.temp_allocator)
	for c, i in acc_cols { col_names[i] = c.name }

	table_rows := make([][]string, len(rows), context.temp_allocator)
	for ri in 0 ..< len(rows) {
		row_strs := make([]string, len(rows[ri].values), context.temp_allocator)
		for v, vi in rows[ri].values { row_strs[vi] = value_string(v) }
		table_rows[ri] = row_strs
	}

	render_table(col_names, table_rows)
	fmt.printf("(%d rows)\n", len(rows))
	return true
}
