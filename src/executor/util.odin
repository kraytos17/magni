package executor

import "core:log"
import "core:math"
import "core:mem"
import "core:slice"
import "core:strconv"
import "core:strings"
import "src:parser"
import "src:schema"
import "src:types"

// Column_Resolver resolves column names to absolute indices within a query's
// combined column array. Build it once per statement (from the combined
// columns + per-table ranges) and reuse it for every condition, ORDER BY,
// aggregate, and projection that would otherwise rescan the column list.
//
// Unqualified names go through a name→index hash (built once); qualified names
// (`alias.col`) are matched against the owning table's range, preserving the
// original "a qualifier that names no known table/alias does not resolve"
// behavior.
Column_Resolver :: struct {
	cols  : []types.Column,
	ranges: []Table_Col_Range,
	index : map[string]int, // unqualified name → first matching index
}

// build_column_resolver indexes the combined columns for fast resolution.
// Later duplicate names do not overwrite earlier ones (first-wins, matching
// the linear-scan original).
@(private)
build_column_resolver :: proc(
	cols: []types.Column,
	ranges: []Table_Col_Range,
	allocator := context.temp_allocator,
) -> Column_Resolver {
	r := Column_Resolver {
		cols   = cols,
		ranges = ranges,
	}
	if len(cols) > 0 {
		r.index = make(map[string]int, len(cols), allocator)
		for col, i in cols {
			if _, exists := r.index[col.name]; !exists {
				r.index[col.name] = i
			}
		}
	}
	return r
}

// resolve maps a (possibly qualified) column name to its absolute index.
@(private)
resolve :: proc(r: Column_Resolver, name: string) -> (int, bool) {
	if len(r.ranges) > 0 {
		if dot_pos := strings.last_index_byte(name, '.'); dot_pos >= 0 {
			table_part := name[:dot_pos]
			col_part := name[dot_pos + 1:]
			for tr in r.ranges {
				if tr.table_name == table_part {
					end := tr.start_col + tr.col_count
					for i in tr.start_col ..< end {
						if r.cols[i].name == col_part {
							return i, true
						}
					}
				}
			}
			return -1, false
		}
	}
	if r.index != nil {
		if i, ok := r.index[name]; ok {
			return i, true
		}
		return -1, false
	}
	return schema.find_column_index(r.cols, name)
}

// where_single_condition returns the lone leaf condition when the clause tree is
// exactly one comparison (used by the PK fast-path and hash-join optimization).
@(private)
where_single_condition :: proc(clause: parser.Where_Clause) -> (parser.Condition, bool) {
	root := clause.root
	if root == nil || root.kind != .COND {
		return {}, false
	}
	return root.cond, true
}

// try_pk_lookup resolves WHERE <pk> = <int> into a direct rowid: single
// COND clause, equality operator, column naming the pk (bare or qualified
// by this table/alias), literal i64 rhs. Anything else bails (ok=false)
// and the caller scans. Sound without re-checking the filter because stored
// pk values are always ints: inserts coerce a non-int pk value to an
// auto-assigned rowid (see the rowid-assignment path in dml.odin).
@(private)
try_pk_lookup :: proc(
	table: types.Table,
	clause: parser.Where_Clause,
	table_name: string = "",
	table_alias: string = "",
) -> (
	rowid: types.Row_ID,
	ok: bool,
) {
	cond, has_cond := where_single_condition(clause)
	if !has_cond {
		return
	}
	if cond.operator != .EQUALS {
		return
	}

	pk_idx, has_pk := schema.get_pk_column(table.columns)
	if !has_pk {
		return
	}

	pk_name := table.columns[pk_idx].name
	if cond.column != pk_name {
		qual, col, has_qual := split_qualifier(cond.column)
		if !has_qual || col != pk_name {
			return
		}

		matches := qual == table_name || (table_alias != "" && qual == table_alias)
		if !matches {
			return
		}
	}

	val_untyped, is_val := cond.rhs.(types.Value)
	if !is_val {
		return
	}

	val, is_int := val_untyped.(i64)
	if !is_int {
		return
	}
	return types.Row_ID(val), true
}

// split_qualifier splits "t.col" into ("t", "col"); has=false when unqualified.
@(private)
split_qualifier :: proc(name: string) -> (qual: string, col: string, has: bool) {
	if i := strings.last_index_byte(name, '.'); i >= 0 {
		return name[:i], name[i + 1:], true
	}
	return "", name, false
}

// values_equal_full compares two value slices element-wise (length + each
// element via value_compare): the full-row arm of the values_equal group,
// behind DISTINCT and set-op dedup.
@(private)
values_equal_full :: proc(a, b: []types.Value) -> bool {
	if len(a) != len(b) {
		return false
	}
	for v, i in a {
		if !types.value_compare(v, b[i]) {
			return false
		}
	}
	return true
}

// values_equal_by_indices compares the key values (extracted at `indices`) of two rows.
values_equal_by_indices :: proc(
	values: []types.Value,
	key: []types.Value,
	indices: []int,
) -> bool {
	if len(key) != len(indices) {
		return false
	}
	for col_idx, pos in indices {
		if !types.value_compare(key[pos], values[col_idx]) {
			return false
		}
	}
	return true
}

// values_equal compares two rows: full element-wise equality for two row
// slices, or projected comparison of key values at column indices.
// Overload resolution picks the arm by arity (2 vs 3 arguments). The group
// is public because tests exercise the projected arm directly; the full-row
// arm stays package-private with the other dedup internals.
values_equal :: proc {
	values_equal_full,
	values_equal_by_indices,
}

// Fp_Buckets is a linear-probe fingerprint bucket index: the cache-friendly
// successor to map[u64][dynamic]int for hot dedup/group/join paths. Slot
// probes walk inline (one cache line per step); positions chain through
// parallel arrays instead of per-bucket heap maps. Grows by doubling at 3/4
// load — size it from the input length when known (dedup), small otherwise
// (groups). Walk a probe hit with b.next[h] (-1 ends), reading positions
// from b.rows[h].
Fp_Buckets :: struct {
	slots    : []u64, // slot fingerprint (valid when head[i] >= 0)
	head     : []int, // head position-node, -1 = empty slot
	rows     : [dynamic]int, // row positions in insertion order
	fps      : [dynamic]u64, // entry fingerprints parallel to rows (rehash)
	next     : [dynamic]int, // chain links parallel to rows, -1 = end
	mask     : int,
	allocator: mem.Allocator,
}

// fp_buckets_make sizes an empty bucket index for ~n entries (min 16
// slots, power of two for the mask). n is a hint — the table grows by
// doubling past 3/4 load regardless.
@(private)
fp_buckets_make :: proc(n: int, allocator: mem.Allocator) -> Fp_Buckets {
	cap := max(16, math.next_power_of_two(2 * (n + 1)))
	b := Fp_Buckets {
		mask      = cap - 1,
		allocator = allocator,
	}

	b.slots = make([]u64, cap, allocator)
	b.head = make([]int, cap, allocator)
	slice.fill(b.head, -1)

	b.rows = make([dynamic]int, 0, n, allocator)
	b.fps = make([dynamic]u64, 0, n, allocator)
	b.next = make([dynamic]int, 0, n, allocator)
	return b
}

// fp_slot spreads a fingerprint over the slot mask (splitmix64 finalizer;
// FNV's own low bits are too weak to index with directly).
@(private = "file")
fp_slot :: proc(fp: u64, mask: int) -> int {
	h := fp + 0x9E3779B97F4A7C15
	h = (h ~ (h >> 30)) * 0xBF58476D1CE4E5B9
	h = (h ~ (h >> 27)) * 0x94D049BB133111EB
	return int((h ~ (h >> 31)) & u64(mask))
}

// fp_buckets_insert_slot links (fp, pos) at a known-empty slot; rows/next/
// fps stay parallel by construction.
@(private = "file")
fp_buckets_insert_slot :: proc(b: ^Fp_Buckets, s: int, fp: u64, pos: int) {
	b.slots[s] = fp
	b.head[s] = len(b.rows)

	append(&b.rows, pos)
	append(&b.fps, fp)
	append(&b.next, -1)
}

// fp_buckets_grow doubles the slot table and reinserts every entry.
@(private = "file")
fp_buckets_grow :: proc(b: ^Fp_Buckets) {
	old_slots, old_head := b.slots, b.head
	old_rows, old_fps := b.rows[:], b.fps[:]
	cap := 2 * len(old_slots)
	b.slots = make([]u64, cap, b.allocator)
	b.head = make([]int, cap, b.allocator)
	slice.fill(b.head, -1)

	b.mask = cap - 1
	clear(&b.rows)
	clear(&b.fps)
	clear(&b.next)
	for i in 0 ..< len(old_rows) {
		fp_buckets_add(b, old_fps[i], old_rows[i])
	}

	delete(old_slots, b.allocator)
	delete(old_head, b.allocator)
}

// fp_buckets_add inserts (fp, pos): same-fingerprint entries chain through
// next (walk from head, -1 ends); collisions probe forward. Grows first
// past 3/4 load so a slot always exists. Positions may repeat (multiset) —
// callers dedup or count as needed.
@(private)
fp_buckets_add :: proc(b: ^Fp_Buckets, fp: u64, pos: int) {
	if len(b.rows) >= (3 * len(b.slots)) / 4 {
		fp_buckets_grow(b)
	}

	s := fp_slot(fp, b.mask)
	for {
		if b.head[s] == -1 {
			fp_buckets_insert_slot(b, s, fp, pos)
			return
		}
		if b.slots[s] == fp {
			append(&b.rows, pos)
			append(&b.fps, fp)
			append(&b.next, b.head[s])

			b.head[s] = len(b.rows) - 1
			return
		}
		s = (s + 1) & b.mask
	}
}

// fp_buckets_probe returns the head chain node for fp (walk with b.next,
// -1 ends; positions live in b.rows). Misses never allocate.
@(private)
fp_buckets_probe :: proc(b: ^Fp_Buckets, fp: u64) -> (int, bool) {
	s := fp_slot(fp, b.mask)
	for {
		if b.head[s] == -1 {
			return 0, false
		}
		if b.slots[s] == fp {
			return b.head[s], true
		}
		s = (s + 1) & b.mask
	}
}

// fp_buckets_destroy frees the slot/head arrays (allocator) and the
// parallel chains. Fingerprints are values, not pointers — nothing else to
// release.
@(private)
fp_buckets_destroy :: proc(b: ^Fp_Buckets) {
	delete(b.slots, b.allocator)
	delete(b.head, b.allocator)
	delete(b.rows)
	delete(b.fps)
	delete(b.next)
}

// deep_copy_values clones a row into temp memory (TEXT/BLOB cloned,
// others copied by value). Unclonable values become NULL rather than
// failing the row — the clone path is best-effort by design.
@(private)
deep_copy_values :: proc(values: []types.Value) -> []types.Value {
	new_values := make([]types.Value, len(values), context.temp_allocator)
	for v, i in values {
		new_values[i] = types.value_clone(v, context.temp_allocator) or_else types.Null{}
	}
	return new_values
}

// Parsed_Check is a CHECK expression decomposed into its parts:
// `col <op> int_literal`. Parsing a raw check_expr string is split out so the
// evaluator is a clean comparison instead of an inline string split.
Parsed_Check :: struct {
	col_name: string, // resolved against the table's columns
	op      : Check_Op,
	val     : i64,
}

Check_Op :: enum u8 {
	GT,
	LT,
	GTE,
	LTE,
	EQ,
	NE,
}

// parse_check_predicate decomposes a raw `col <op> int` CHECK string. Returns
// ok=false (already logged) on malformed input or an unsupported operator.
@(private)
parse_check_predicate :: proc(chk: string) -> (Parsed_Check, bool) {
	parts := strings.split(chk, " ", context.temp_allocator)
	if len(parts) < 3 {
		log.errorf("Error: CHECK constraint too complex: %s", chk)
		return {}, false
	}

	op, op_ok := check_op_from_token(parts[1])
	if !op_ok {
		log.errorf("CHECK uses unsupported operator: %s", parts[1])
		return {}, false
	}

	val, parse_num := strconv.parse_i64(parts[2])
	if !parse_num {
		log.errorf("Error: CHECK constraint non-integer comparison: %s", chk)
		return {}, false
	}
	return Parsed_Check{col_name = parts[0], op = op, val = val}, true
}

@(private = "file")
// check_op_from_token maps a CHECK operator spelling to its Check_Op.
// Both not-equals spellings (!= and <>) are accepted; anything else fails
// (CHECK supports exactly these six comparisons).
check_op_from_token :: proc(tok: string) -> (Check_Op, bool) {
	switch tok {
	case ">":
		return .GT, true
	case "<":
		return .LT, true
	case ">=":
		return .GTE, true
	case "<=":
		return .LTE, true
	case "=":
		return .EQ, true
	case "!=", "<>":
		return .NE, true
	}
	return .EQ, false
}

@(private = "file")
// check_op_eval applies a CHECK comparison to two ints. Total (every op
// returns, no fallthrough) — the trailing false is unreachable.
check_op_eval :: proc(op: Check_Op, left, right: i64) -> bool {
	switch op {
	case .GT:
		return left > right
	case .LT:
		return left < right
	case .GTE:
		return left >= right
	case .LTE:
		return left <= right
	case .EQ:
		return left == right
	case .NE:
		return left != right
	}
	return false
}

// Resolved_Check is one CHECK constraint with parsing and column resolution
// done: evaluated per row with zero allocation and no string work. `src`
// keeps the original expression for error messages (borrowed from the table).
Resolved_Check :: struct {
	col_idx: int,
	op     : Check_Op,
	val    : i64,
	src    : string,
}

// resolve_table_checks parses and resolves every CHECK constraint on the
// table once per statement. Returns ok=false (already logged) when a check
// is malformed or names an unknown column.
@(private)
resolve_table_checks :: proc(table: types.Table) -> ([]Resolved_Check, bool) {
	n := 0
	for col in table.columns {
		if _, has_chk := col.check_expr.?; has_chk {
			n += 1
		}
	}
	if n == 0 {
		return nil, true
	}

	checks := make([]Resolved_Check, n, context.temp_allocator)
	resolver := build_column_resolver(table.columns, nil)
	i := 0
	for col in table.columns {
		chk, has_chk := col.check_expr.?
		if !has_chk {
			continue
		}

		parsed, p_ok := parse_check_predicate(chk)
		if !p_ok {
			return nil, false
		}

		col_idx, col_ok := resolve(resolver, parsed.col_name)
		if !col_ok {
			log.errorf("Error: CHECK references unknown column: %s", parsed.col_name)
			return nil, false
		}

		checks[i] = Resolved_Check {
			col_idx = col_idx,
			op      = parsed.op,
			val     = parsed.val,
			src     = chk,
		}
		i += 1
	}
	return checks, true
}

// check_constraints_resolved evaluates statement-resolved CHECK constraints
// against one row. Only the integer assertion and comparison run per row.
@(private)
check_constraints_resolved :: proc(values: []types.Value, checks: []Resolved_Check) -> bool {
	for rc in checks {
		left_i64, is_int := values[rc.col_idx].(i64)
		if !is_int {
			log.errorf("Error: CHECK column value is not an integer: %s", rc.src)
			return false
		}
		if !check_op_eval(rc.op, left_i64, rc.val) {
			log.errorf("CHECK constraint violation: %s", rc.src)
			return false
		}
	}
	return true
}
