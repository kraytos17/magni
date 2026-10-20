package executor

import "core:fmt"
import "core:log"
import "core:slice"
import "core:strings"
import "src:btree"
import "src:cell"
import "src:parser"
import "src:schema"
import "src:types"

// Index_Use names which predicate shape an Index_Plan answers.
Index_Use :: enum u8 {
	None,
	Eq, // col = literal (text or int per index type)
	Prefix, // col LIKE 'stem%' (canonical: single trailing %, no %/_ in stem; TEXT only)
	In, // col IN (...) over literal members of the index's type
	Range, // col >/</>=/<= literal (one open-ended bound, or both after AND-merge)
	Composite, // multi-col Eq run: exact full tuple or leading-column prefix (fetch-only)
	Or, // flat OR of usable same-column CONDs (union, exact)
	And, // flat AND with 2+ usable conjuncts (intersection, needs recheck)
}

// Index_Plan is one resolved index access: the shape plus its operands.
// Strings borrow the parser's arena (statement-scoped); candidates are
// consumed synchronously, same as the dense path's temp arrays.
// Eq/In carry the key in the arm matching col_type (eq_text/in_texts for
// TEXT, eq_int/in_ints for INTEGER, eq_real/in_reals for REAL); the other
// arms stay zero. Range carries one bound per side in the matching arm
// (absent side reads open-ended); range_empty marks a provably empty
// interval (contradictory AND-merge — the btree short-circuits, zero I/O).
// Composite carries pre-framed raws (arena-borrowed like strings):
// comp_key for an exact full tuple, comp_prefix for a leading-column run,
// comp_empty for a contradictory duplicate Eq, comp_full for a whole-index
// key-order scan (no WHERE). Fetch-only (covering
// refuses Composite — tuple decomposition needs the row).
// Or/And carry their member plans in subs (each member routed its own
// index and carries its own type; column is the first sub's, display-only).
Index_Plan :: struct {
	use          : Index_Use,
	root         : u32, // index root this plan reads (per-sub for Or/And)
	column       : string, // indexed column (arena-borrowed; Or/And: first sub's; Composite: joined)
	col_type     : types.Column_Type, // index kind (per-sub for Or/And; Composite: first component's)
	eq_text      : string, // Eq on TEXT
	eq_int       : i64, // Eq on INTEGER
	eq_real      : f64, // Eq on REAL
	prefix       : string, // Prefix stem
	in_texts     : []string, // In TEXT members
	in_ints      : []i64, // In INTEGER members
	in_reals     : []f64, // In REAL members
	range_has_lo : bool, // Range lower bound present
	range_has_hi : bool, // Range upper bound present
	range_lo_inc : bool, // Range lower bound inclusive (>=)
	range_hi_inc : bool, // Range upper bound inclusive (<=)
	range_lo_text: string, // Range lower bound on TEXT
	range_hi_text: string, // Range upper bound on TEXT
	range_lo_int : i64, // Range lower bound on INTEGER
	range_hi_int : i64, // Range upper bound on INTEGER
	range_lo_real: f64, // Range lower bound on REAL
	range_hi_real: f64, // Range upper bound on REAL
	range_empty  : bool, // Range provably matches nothing (AND contradiction)
	comp_key     : []u8, // Composite exact full-tuple raw
	comp_prefix  : []u8, // Composite leading-column framed run
	comp_empty   : bool, // Composite provably matches nothing (duplicate Eq)
	comp_columns : []string, // Composite bound column prefix (arena-borrowed)
	comp_lo      : []u8, // Composite leading-component lower bound (framed)
	comp_hi      : []u8, // Composite leading-component upper bound (framed)
	comp_has_lo  : bool, // Composite lower bound present
	comp_has_hi  : bool, // Composite upper bound present
	comp_lo_inc  : bool, // Composite lower bound inclusive (>=)
	comp_hi_inc  : bool, // Composite upper bound inclusive (<=)
	comp_full    : bool, // Composite full key-order scan (ORDER BY without WHERE)
	subs         : []Index_Plan, // Or/And members (each with its own root)
}

// MAX_INDEX_IN_MEMBERS caps literal IN routing: each TEXT member is one
// point lookup, so an unbounded list turns the index into a slower scan.
// Above the cap the full scan wins back (identical results, one code
// path). Tunable; 128 sits far past the measured wins (3-member IN ≥20×)
// and far below any plausible crossover.
MAX_INDEX_IN_MEMBERS :: 128

// RANGE_OPS names the ordered comparisons the router answers as Range
// plans: one greppable inventory instead of a four-way == chain. The
// bound side (lo/hi) and inclusivity still read the operator directly —
// single-shape distinctions
RANGE_OPS :: bit_set[parser.Token_Type]{.GREATER_THAN, .GREATER_EQUAL, .LESS_THAN, .LESS_EQUAL}

// canonical_like_prefix extracts the stem of `LIKE 'abc%'`: non-empty,
// single trailing %, no % or _ elsewhere. Exactly the shapes where
// like_match reduces to a byte-prefix test, so index candidates and the
// recheck filter agree by construction. Interior wildcards fall back to the
// full scan (like_match's general path, behavior untouched).
@(private)
canonical_like_prefix :: proc(pattern: string) -> (stem: string, ok: bool) {
	if len(pattern) < 2 || !strings.has_suffix(pattern, "%") {
		return "", false
	}

	stem = pattern[:len(pattern) - 1]
	if len(stem) == 0 {
		return "", false
	}
	if strings.contains_any(stem, "%_") {
		return "", false
	}
	return stem, true
}

// index_cond_column_ok matches one raw condition's column against the
// indexed column: bare name or table/alias-qualified (same rule as
// try_pk_lookup).
@(private)
index_cond_column_ok :: proc(
	index_column: string,
	column: string,
	tbl_name: string,
	tbl_alias: string,
) -> bool {
	if column == index_column {
		return true
	}

	qual, col, has_qual := split_qualifier(column)
	if !has_qual || col != index_column {
		return false
	}
	return qual == tbl_name || (tbl_alias != "" && qual == tbl_alias)
}

// cond_text_rhs extracts a condition's TEXT literal rhs: the shared
// precondition behind the EQUALS and LIKE index arms (non-TEXT and
// non-literal rhs never route to a text index).
@(private = "file")
cond_text_rhs :: proc(cond: parser.Condition) -> (string, bool) {
	val, is_val := cond.rhs.(types.Value)
	if !is_val {
		return "", false
	}

	s, is_text := val.(string)
	if !is_text {
		return "", false
	}
	return s, true
}

// cond_int_rhs extracts a condition's INTEGER literal rhs: the shared
// precondition behind the EQUALS and IN index arms on INTEGER indexes
// (non-i64 and non-literal rhs never route to an int index).
@(private = "file")
cond_int_rhs :: proc(cond: parser.Condition) -> (i64, bool) {
	val, is_val := cond.rhs.(types.Value)
	if !is_val {
		return 0, false
	}

	v, is_int := val.(i64)
	if !is_int {
		return 0, false
	}
	return v, true
}

// cond_real_rhs extracts a condition's REAL literal rhs: the shared
// precondition behind the EQUALS, IN, and Range index arms on REAL
// indexes. Non-f64 and non-literal rhs never route; NaN never routes
// either (unordered under every comparison the recheck can express, so no
// bound could delimit it — the scan path answers NaN filters).
@(private = "file")
cond_real_rhs :: proc(cond: parser.Condition) -> (f64, bool) {
	val, is_val := cond.rhs.(types.Value)
	if !is_val {
		return 0, false
	}

	v, is_real := val.(f64)
	if !is_real || !cell.real_index_encodable(v) {
		return 0, false
	}
	return v, true
}

// index_use_of_cond classifies one raw COND against the indexed column of
// the given type. Negated, column-comparison (rhs is a column ref, not a
// Value), NULL and cross-class literals all fall through (same result via
// the full scan, one code path). IN over a literal list routes on members
// of the index's type: other members provably never match (compare_values
// ranks storage classes apart) and NULL members never match
// (membership_test guards the row side), so the member lookups are
// complete. Subquery IN stays on the scan path (its membership materializes
// per resolve, not per lookup). LIKE routes TEXT indexes only — prefixes
// are meaningless on integers. Ordered comparisons route as open-ended
// Range plans (fetch-only — keys unknown, like Prefix); != stays on the
// scan path (a complement, not a contiguous run).
@(private)
index_use_of_cond :: proc(
	index_root: u32,
	index_column: string,
	col_type: types.Column_Type,
	cond: parser.Condition,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if index_root == 0 || len(index_column) == 0 {
		return {}, false
	}
	if cond.negated {
		return {}, false
	}
	if !index_cond_column_ok(index_column, cond.column, tbl_name, tbl_alias) {
		return {}, false
	}
	if cond.operator == .EQUALS {
		switch col_type {
		case .INTEGER:
			if v, is_int := cond_int_rhs(cond); is_int {
				return Index_Plan {
						use = .Eq,
						root = index_root,
						column = index_column,
						col_type = col_type,
						eq_int = v,
					},
					true
			}
			return {}, false
		case .REAL:
			if v, is_real := cond_real_rhs(cond); is_real {
				return Index_Plan {
						use = .Eq,
						root = index_root,
						column = index_column,
						col_type = col_type,
						eq_real = v,
					},
					true
			}
			return {}, false
		case .TEXT:
			if s, is_text := cond_text_rhs(cond); is_text {
				return Index_Plan {
						use = .Eq,
						root = index_root,
						column = index_column,
						col_type = col_type,
						eq_text = s,
					},
					true
			}
			return {}, false
		case .BLOB:
			return {}, false
		}
	}
	if cond.operator == .LIKE {
		switch col_type {
		case .TEXT:
			pat, is_text := cond_text_rhs(cond)
			if !is_text {
				return {}, false
			}
			if stem, stem_ok := canonical_like_prefix(pat); stem_ok {
				return Index_Plan {
						use = .Prefix,
						root = index_root,
						column = index_column,
						col_type = col_type,
						prefix = stem,
					},
					true
			}
			return {}, false
		case .INTEGER, .REAL, .BLOB:
			// Prefixes are meaningless off TEXT; the scan path answers.
			return {}, false
		}
	}
	if cond.operator == .IN {
		if cond.in_subquery != nil || cond.in_values == nil {
			return {}, false
		}
		switch col_type {
		case .INTEGER:
			ints := make([dynamic]i64, 0, len(cond.in_values), context.temp_allocator)
			for v in cond.in_values {
				if n, is_int := v.(i64); is_int {
					append(&ints, n)
				}
			}
			if len(ints) == 0 {
				return {}, false
			}
			if len(ints) > MAX_INDEX_IN_MEMBERS {
				return {}, false
			}
			return Index_Plan {
					use = .In,
					root = index_root,
					column = index_column,
					col_type = col_type,
					in_ints = ints[:],
				},
				true
		case .REAL:
			reals := make([dynamic]f64, 0, len(cond.in_values), context.temp_allocator)
			for v in cond.in_values {
				if n, is_real := v.(f64); is_real && cell.real_index_encodable(n) {
					append(&reals, n)
				}
			}
			if len(reals) == 0 {
				return {}, false
			}
			if len(reals) > MAX_INDEX_IN_MEMBERS {
				return {}, false
			}
			return Index_Plan {
					use = .In,
					root = index_root,
					column = index_column,
					col_type = col_type,
					in_reals = reals[:],
				},
				true
		case .TEXT:
			texts := make([dynamic]string, 0, len(cond.in_values), context.temp_allocator)
			for v in cond.in_values {
				if s, is_text := v.(string); is_text {
					append(&texts, s)
				}
			}
			if len(texts) == 0 {
				return {}, false
			}
			if len(texts) > MAX_INDEX_IN_MEMBERS {
				return {}, false
			}
			return Index_Plan {
					use = .In,
					root = index_root,
					column = index_column,
					col_type = col_type,
					in_texts = texts[:],
				},
				true
		case .BLOB:
			return {}, false
		}
	}
	if cond.operator in RANGE_OPS {
		is_lo := cond.operator == .GREATER_THAN || cond.operator == .GREATER_EQUAL
		inc := cond.operator == .GREATER_EQUAL || cond.operator == .LESS_EQUAL
		switch col_type {
		case .INTEGER:
			if v, is_int := cond_int_rhs(cond); is_int {
				rp := Index_Plan {
					use      = .Range,
					root     = index_root,
					column   = index_column,
					col_type = col_type,
				}
				if is_lo {
					rp.range_has_lo = true
					rp.range_lo_inc = inc
					rp.range_lo_int = v
				} else {
					rp.range_has_hi = true
					rp.range_hi_inc = inc
					rp.range_hi_int = v
				}
				return rp, true
			}
			return {}, false
		case .REAL:
			if v, is_real := cond_real_rhs(cond); is_real {
				rp := Index_Plan {
					use      = .Range,
					root     = index_root,
					column   = index_column,
					col_type = col_type,
				}
				if is_lo {
					rp.range_has_lo = true
					rp.range_lo_inc = inc
					rp.range_lo_real = v
				} else {
					rp.range_has_hi = true
					rp.range_hi_inc = inc
					rp.range_hi_real = v
				}
				return rp, true
			}
			return {}, false
		case .TEXT:
			if s, is_text := cond_text_rhs(cond); is_text {
				rp := Index_Plan {
					use      = .Range,
					root     = index_root,
					column   = index_column,
					col_type = col_type,
				}
				if is_lo {
					rp.range_has_lo = true
					rp.range_lo_inc = inc
					rp.range_lo_text = s
				} else {
					rp.range_has_hi = true
					rp.range_hi_inc = inc
					rp.range_hi_text = s
				}
				return rp, true
			}
			return {}, false
		case .BLOB:
			return {}, false
		}
	}
	return {}, false
}

// composite_cond_value extracts one Eq literal for a composite component:
// the per-type rhs check behind composite folding (same rejections as the
// scalar arms — cross-class, NULL, and NaN never route).
@(private = "file")
composite_cond_value :: proc(
	col_type: types.Column_Type,
	cond: parser.Condition,
) -> (
	types.Value,
	bool,
) {
	switch col_type {
	case .TEXT:
		if s, is_text := cond_text_rhs(cond); is_text {
			return types.value(s), true
		}
	case .INTEGER:
		if v, is_int := cond_int_rhs(cond); is_int {
			return types.value(v), true
		}
	case .REAL:
		if v, is_real := cond_real_rhs(cond); is_real {
			return types.value(v), true
		}
	case .BLOB:
	}
	return {}, false
}

// composite_values_equal reports whether two bound literals of one
// component type denote the same key (-0.0 shares +0.0's word, so float
// equality — not bitwise identity — is the comparator).
@(private = "file")
composite_values_equal :: proc(col_type: types.Column_Type, a, b: types.Value) -> bool {
	switch col_type {
	case .TEXT:
		as, aok := a.(string)
		bs, bok := b.(string)
		return aok && bok && as == bs
	case .INTEGER:
		ai, aok := a.(i64)
		bi, bok := b.(i64)
		return aok && bok && ai == bi
	case .REAL:
		af, aok := a.(f64)
		bf, bok := b.(f64)
		return aok && bok && af == bf
	case .BLOB:
		return false
	}
	return false
}

// composite_bound_cmp orders two bound literals of one component type
// for composite range tightening: numeric for INTEGER, sort-word order
// for REAL, BINARY for TEXT (same order the framed bytes compare in).
@(private = "file")
composite_bound_cmp :: proc(col_type: types.Column_Type, a, b: types.Value) -> int {
	switch col_type {
	case .TEXT:
		as, aok := a.(string)
		bs, bok := b.(string)
		if !aok || !bok {
			return 0
		}
		return cell.bytes_compare(transmute([]u8)as, transmute([]u8)bs)
	case .INTEGER:
		ai, aok := a.(i64)
		bi, bok := b.(i64)
		if !aok || !bok {
			return 0
		}
		if ai == bi {
			return 0
		}
		return -1 if ai < bi else 1
	case .REAL:
		af, aok := a.(f64)
		bf, bok := b.(f64)
		if !aok || !bok {
			return 0
		}

		wa := cell.real_sort_word(af)
		wb := cell.real_sort_word(bf)
		if wa == wb {
			return 0
		}
		return -1 if wa < wb else 1
	case .BLOB:
		return 0
	}
	return 0
}

// frame_composite_bound frames one leading-component bound into temp
// bytes (nil when the side is absent). The file-private framer behind
// composite range plans — bounds are pre-validated literals, so failure
// is defense in depth, not a path.
@(private = "file")
frame_composite_bound :: proc(def: types.Index_Def, v: types.Value, set: bool) -> ([]u8, bool) {
	if !set {
		return nil, true
	}

	vals := [1]types.Value{v}
	need := cell.composite_encoded_len(vals[:], def.col_types[:1])
	if need == 0 {
		return nil, false
	}

	buf := make([]u8, need, context.temp_allocator)
	wn, wok := cell.composite_encode_raw(vals[:], def.col_types[:1], buf)
	if !wok {
		return nil, false
	}
	return buf[:wn], true
}

// try_composite_in_expansion expands one leading literal IN with bound
// trailing Eqs into a union of full-tuple lookups (or prefix scans when
// trailing columns stay unbound): `a IN (1,2) AND b='x'` ≡ `(a=1 AND
// b='x') OR (a=2 AND b='x')`, desugared per member through
// try_composite_plan (the fetch recheck covers the remaining conjuncts,
// so each member plan is exact-or-superset, never partial). Only the
// first usable leading IN expands (later ones ride the recheck);
// subquery IN, empty/unusable member lists, and over-cap lists decline
// to the scan path, mirroring the scalar IN arms.
@(private)
try_composite_in_expansion :: proc(
	def: types.Index_Def,
	conds: []parser.Condition,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	n := len(def.columns)
	if n < 2 || n != len(def.col_types) {
		return {}, false
	}

	picked := -1
	members := make([dynamic]types.Value, 0, 8, context.temp_allocator)
	for cond, ci in conds {
		if cond.negated || cond.operator != .IN {
			continue
		}
		if cond.in_subquery != nil || cond.in_values == nil {
			continue
		}
		if !index_cond_column_ok(def.columns[0], cond.column, tbl_name, tbl_alias) {
			continue
		}
		picked = ci
		for v in cond.in_values {
			switch def.col_types[0] {
			case .TEXT:
				if s, is_text := v.(string); is_text {
					append(&members, types.value(s))
				}
			case .INTEGER:
				if iv, is_int := v.(i64); is_int {
					append(&members, types.value(iv))
				}
			case .REAL:
				if fv, is_real := v.(f64); is_real && cell.real_index_encodable(fv) {
					append(&members, types.value(fv))
				}
			case .BLOB:
			}
		}
		break
	}
	if picked < 0 || len(members) == 0 || len(members) > MAX_INDEX_IN_MEMBERS {
		return {}, false
	}

	rest := make([dynamic]parser.Condition, 0, len(conds), context.temp_allocator)
	for cond, ci in conds {
		if ci != picked {
			append(&rest, cond)
		}
	}
	subs := make([dynamic]Index_Plan, 0, len(members), context.temp_allocator)
	for m in members {
		mcond := parser.Condition {
			column   = def.columns[0],
			operator = .EQUALS,
			rhs      = m,
		}
		mconds := make([dynamic]parser.Condition, 0, len(rest) + 1, context.temp_allocator)
		append(&mconds, mcond)
		append(&mconds, ..rest[:])
		sub, sok := try_composite_plan(def, mconds[:], tbl_name, tbl_alias)
		if !sok {
			return {}, false
		}
		append(&subs, sub)
	}
	if len(subs) == 1 {
		return subs[0], true
	}
	return Index_Plan {
			use = .Or,
			column = subs[0].column,
			col_type = subs[0].col_type,
			subs = subs[:],
		},
		true
}

// try_composite_plan folds usable leading Eq CONDs on one composite def
// into a Composite plan: an exact full-tuple lookup when every column
// binds, otherwise a leading-column prefix scan. Later bound columns
// behind an unbound middle ride the fetch recheck (same superset +
// recheck contract as the scalar And path). Duplicate usable Eqs on one
// column collapse when equal and empty the plan when contradictory.
// Without a leading Eq, usable leading Range CONDs tighten to a
// leading-component scan (Eq dominates when both present). Anything else
// on the leading column (LIKE, IN, cross-class, NULL, negation, column
// refs) declines — the scan path stays correct.
@(private)
try_composite_plan :: proc(
	def: types.Index_Def,
	conds: []parser.Condition,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	n := len(def.columns)
	if n < 2 || n != len(def.col_types) {
		return {}, false
	}

	bound := make([]types.Value, n, context.temp_allocator)
	has := make([]bool, n, context.temp_allocator)
	lo_set, hi_set := false, false
	lo_val, hi_val: types.Value
	lo_inc, hi_inc := false, false
	for cond in conds {
		if cond.negated {
			continue
		}
		if cond.operator == .EQUALS {
			for col, i in def.columns {
				if !index_cond_column_ok(col, cond.column, tbl_name, tbl_alias) {
					continue
				}

				v, usable := composite_cond_value(def.col_types[i], cond)
				if !usable {
					continue
				}
				if !has[i] {
					bound[i], has[i] = v, true
				} else if !composite_values_equal(def.col_types[i], bound[i], v) {
					return Index_Plan {
							use = .Composite,
							root = def.root,
							column = def.column,
							col_type = def.col_types[0],
							comp_empty = true,
						},
						true
				}
			}
			continue
		}
		if cond.operator in RANGE_OPS &&
		   index_cond_column_ok(def.columns[0], cond.column, tbl_name, tbl_alias) {
			v, usable := composite_cond_value(def.col_types[0], cond)
			if !usable {
				continue
			}

			is_lo := cond.operator == .GREATER_THAN || cond.operator == .GREATER_EQUAL
			inc := cond.operator == .GREATER_EQUAL || cond.operator == .LESS_EQUAL
			if is_lo {
				if !lo_set ||
				   composite_bound_cmp(def.col_types[0], lo_val, v) < 0 ||
				   (composite_bound_cmp(def.col_types[0], lo_val, v) == 0 && lo_inc && !inc) {
					lo_val, lo_inc, lo_set = v, inc, true
				}
			} else {
				if !hi_set ||
				   composite_bound_cmp(def.col_types[0], hi_val, v) > 0 ||
				   (composite_bound_cmp(def.col_types[0], hi_val, v) == 0 && hi_inc && !inc) {
					hi_val, hi_inc, hi_set = v, inc, true
				}
			}
		}
	}
	if !has[0] && !lo_set && !hi_set {
		return {}, false
	}

	base := Index_Plan {
		use      = .Composite,
		root     = def.root,
		column   = def.column,
		col_type = def.col_types[0],
	}
	if has[0] {
		run := 1
		for run < n && has[run] {
			run += 1
		}
		// Size exactly (bounds are pre-validated literals, so sizing cannot
		// fail — the check below is defense in depth, not a path).
		need := cell.composite_encoded_len(bound[:run], def.col_types[:run])
		if need == 0 {
			return {}, false
		}

		frame_buf := make([]u8, need, context.temp_allocator)
		raw_n, raw_ok := cell.composite_encode_raw(bound[:run], def.col_types[:run], frame_buf)
		if !raw_ok {
			return {}, false
		}

		framed := frame_buf[:raw_n]
		if run == n {
			base.comp_key = framed
			base.comp_columns = def.columns
			return base, true
		}

		base.comp_prefix = framed
		base.comp_columns = def.columns[:run]
		return base, true
	}
	// Leading range only: tighten to one bounded scan (contradictions
	// empty with zero I/O, like the scalar merge).
	if lo_set && hi_set {
		c := composite_bound_cmp(def.col_types[0], lo_val, hi_val)
		if c > 0 || (c == 0 && !(lo_inc && hi_inc)) {
			base.comp_empty = true
			return base, true
		}
	}

	lo_raw, lo_ok := frame_composite_bound(def, lo_val, lo_set)
	hi_raw, hi_ok := frame_composite_bound(def, hi_val, hi_set)
	if !lo_ok || !hi_ok {
		return {}, false
	}

	base.comp_lo, base.comp_has_lo, base.comp_lo_inc = lo_raw, lo_set, lo_inc
	base.comp_hi, base.comp_has_hi, base.comp_hi_inc = hi_raw, hi_set, hi_inc
	return base, true
}

// COND — or a flat OR of usable CONDs (union of exact sets is exact) —
// on the indexed column: index output is then exact (no recheck possible —
// covering rows carry no column values). AND chains never qualify here:
// a conjunct on another column can't be verified without the row.
@(private)
resolve_index_covering :: proc(
	table: types.Table,
	wc: parser.Where_Clause,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	root := wc.root
	if root == nil {
		return {}, false
	}
	if root.kind == .COND {
		cond, has_cond := where_single_condition(wc)
		if !has_cond {
			return {}, false
		}
		for def in table.indexes {
			if cand, usable := index_use_of_cond(
				def.root,
				def.column,
				def.col_type,
				cond,
				tbl_name,
				tbl_alias,
			); usable {
				return cand, true
			}
		}

		single := [1]parser.Condition{cond}
		for def in table.indexes {
			if len(def.columns) < 2 {
				continue
			}
			if cand, usable := try_composite_plan(def, single[:], tbl_name, tbl_alias); usable {
				return cand, true
			}
			if cand, usable := try_composite_in_expansion(def, single[:], tbl_name, tbl_alias);
			   usable {
				return cand, true
			}
		}
		return {}, false
	}
	if root.kind == .OR {
		if or_plan, or_ok := resolve_index_or(table, root.children, tbl_name, tbl_alias); or_ok {
			return or_plan, true
		}
	}
	return {}, false
}

// resolve_index_or routes a flat OR whose every disjunct is a usable COND
// on the indexed column (union of exact sets is exact — covering-safe),
// or a flat AND whose conjuncts fold into one composite plan (union of
// exact composite scans is likewise exact). One unusable disjunct (other
// column, negated, nested) falls back: its rows are unscannable by the
// index, so the union would be incomplete.
@(private)
resolve_index_or :: proc(
	table: types.Table,
	children: [dynamic]^parser.Where_Node,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if len(children) < 2 {
		return {}, false
	}

	subs := make([dynamic]Index_Plan, 0, len(children), context.temp_allocator)
	for child in children {
		if child.kind != .COND && child.kind != .AND {
			return {}, false
		}
		if child.kind == .AND {
			and_conds := make(
				[dynamic]parser.Condition,
				0,
				len(child.children),
				context.temp_allocator,
			)
			for grand in child.children {
				if grand.kind != .COND {
					return {}, false
				}
				append(&and_conds, grand.cond)
			}

			folded := false
			for def in table.indexes {
				if len(def.columns) < 2 {
					continue
				}
				if sub, usable := try_composite_plan(def, and_conds[:], tbl_name, tbl_alias);
				   usable {
					append(&subs, sub)
					folded = true
					break
				}
			}
			if !folded {
				return {}, false
			}
			continue
		}

		resolved := false
		for def in table.indexes {
			if sub, usable := index_use_of_cond(
				def.root,
				def.column,
				def.col_type,
				child.cond,
				tbl_name,
				tbl_alias,
			); usable {
				append(&subs, sub)
				resolved = true
				break
			}
		}
		if !resolved {
			return {}, false
		}
	}
	return Index_Plan {
			use = .Or,
			column = subs[0].column,
			col_type = subs[0].col_type,
			subs = subs[:],
		},
		true
}

// resolve_index_fetch routes a flat AND-chain (or a lone COND) with at
// least one usable conjunct on the indexed column. Two or more usable
// conjuncts intersect (narrower candidates, fewer fetches); one behaves
// exactly as before. OR, NOT, and nested groups disable routing (same
// rule as skip_chain_conditions) — see resolve_index_or for the flat-OR
// exception, which both covering and fetch accept. The full filter
// rechecks every candidate, so extra conjuncts only narrow — never
// widen — the answer.
@(private)
resolve_index_fetch :: proc(
	table: types.Table,
	wc: parser.Where_Clause,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	root := wc.root
	if root == nil {
		return {}, false
	}
	if root.kind == .COND {
		for def in table.indexes {
			if cand, usable := index_use_of_cond(
				def.root,
				def.column,
				def.col_type,
				root.cond,
				tbl_name,
				tbl_alias,
			); usable {
				return cand, true
			}
		}

		single := [1]parser.Condition{root.cond}
		for def in table.indexes {
			if len(def.columns) < 2 {
				continue
			}
			if cand, usable := try_composite_plan(def, single[:], tbl_name, tbl_alias); usable {
				return cand, true
			}
			if cand, usable := try_composite_in_expansion(def, single[:], tbl_name, tbl_alias);
			   usable {
				return cand, true
			}
		}
		return {}, false
	}
	if root.kind == .OR {
		return resolve_index_or(table, root.children, tbl_name, tbl_alias)
	}
	if root.kind != .AND {
		return {}, false
	}

	conds := make([dynamic]parser.Condition, 0, len(root.children), context.temp_allocator)
	for child in root.children {
		if child.kind != .COND {
			return {}, false
		}
		append(&conds, child.cond)
	}

	subs := make([dynamic]Index_Plan, 0, len(root.children), context.temp_allocator)
	for cond in conds {
		for def in table.indexes {
			if cand, usable := index_use_of_cond(
				def.root,
				def.column,
				def.col_type,
				cond,
				tbl_name,
				tbl_alias,
			); usable {
				append(&subs, cand)
				break
			}
		}
	}
	for def in table.indexes {
		if len(def.columns) < 2 {
			continue
		}
		if cand, usable := try_composite_plan(def, conds[:], tbl_name, tbl_alias); usable {
			append(&subs, cand)
		}
		if cand, usable := try_composite_in_expansion(def, conds[:], tbl_name, tbl_alias); usable {
			append(&subs, cand)
		}
	}
	if len(subs) == 0 {
		return {}, false
	}
	if len(subs) == 1 {
		return subs[0], true
	}
	return collapse_and_subs(subs[:])
}

// range_val_cmp orders two bound literals of one index type: numeric for
// INTEGER, sort-word order for REAL, BINARY for TEXT. The file-private
// comparator behind AND-merge (callers in one group share col_type, so one
// switch covers all arms). REAL compares transformed words, not cmp_f64 —
// bounds need index order, and -0.0/+0.0 share one word by construction.
@(private = "file")
range_val_cmp :: proc(
	col_type: types.Column_Type,
	a_text: string,
	a_int: i64,
	a_real: f64,
	b_text: string,
	b_int: i64,
	b_real: f64,
) -> int {
	switch col_type {
	case .INTEGER:
		if a_int == b_int {
			return 0
		}
		return -1 if a_int < b_int else 1
	case .REAL:
		wa := cell.real_sort_word(a_real)
		wb := cell.real_sort_word(b_real)
		if wa == wb {
			return 0
		}
		return -1 if wa < wb else 1
	case .TEXT, .BLOB:
		// BLOB bounds never construct (BLOB plans never route); sharing
		// the TEXT arm keeps the shape exhaustive.
		return cell.bytes_compare(transmute([]u8)a_text, transmute([]u8)b_text)
	}
	return 0 // Defensive: the switch above is exhaustive.
}

// range_value_ok reports whether one literal satisfies one Range plan's
// bounds: the Eq-dominates and In-filter checks behind AND-collapse.
@(private = "file")
range_value_ok :: proc(
	col_type: types.Column_Type,
	v_text: string,
	v_int: i64,
	v_real: f64,
	bound: Index_Plan,
) -> bool {
	if bound.range_has_lo {
		c := range_val_cmp(
			col_type,
			v_text,
			v_int,
			v_real,
			bound.range_lo_text,
			bound.range_lo_int,
			bound.range_lo_real,
		)
		if c < 0 || (c == 0 && !bound.range_lo_inc) {
			return false
		}
	}
	if bound.range_has_hi {
		c := range_val_cmp(
			col_type,
			v_text,
			v_int,
			v_real,
			bound.range_hi_text,
			bound.range_hi_int,
			bound.range_hi_real,
		)
		if c > 0 || (c == 0 && !bound.range_hi_inc) {
			return false
		}
	}
	return true
}

// collapse_group folds one same-index AND group (2+ Eq/In/Range members)
// into fewer, tighter plans: Eq dominates (checked against every Range
// bound and In membership — violations empty the whole AND), In sets
// intersect then filter by Range bounds (dropping the Ranges they imply),
// and bare Ranges intersect to the tightest interval. Any other member
// (Prefix) disables collapse — the group passes through to the intersect
// path. Kind triage is an exhaustive switch (BLOB declines like Prefix —
// unreached today since BLOB plans are never usable). Surviving strings
// borrow the input plans' arena memory.
@(private = "file")
collapse_group :: proc(group: []Index_Plan) -> (plans: []Index_Plan, is_empty: bool) {
	col_type := group[0].col_type
	for sub in group {
		if sub.use != .Eq && sub.use != .In && sub.use != .Range {
			return group, false
		}
	}

	eq_set := false
	eq_text := ""
	eq_int := i64(0)
	eq_real := f64(0)
	for sub in group {
		if sub.use != .Eq {
			continue
		}
		switch col_type {
		case .INTEGER:
			if !eq_set {
				eq_set, eq_int = true, sub.eq_int
			} else if eq_int != sub.eq_int {
				return nil, true
			}
		case .REAL:
			if !eq_set {
				eq_set, eq_real = true, sub.eq_real
			} else if eq_real != sub.eq_real {
				return nil, true
			}
		case .TEXT:
			if !eq_set {
				eq_set, eq_text = true, sub.eq_text
			} else if eq_text != sub.eq_text {
				return nil, true
			}
		case .BLOB:
			return group, false
		}
	}
	if eq_set {
		for sub in group {
			#partial switch sub.use {
			case .Range:
				if sub.range_empty {
					return nil, true
				}
				if !range_value_ok(col_type, eq_text, eq_int, eq_real, sub) {
					return nil, true
				}
			case .In:
				found := false
				switch col_type {
				case .INTEGER:
					for m in sub.in_ints {
						if m == eq_int {
							found = true
							break
						}
					}
				case .REAL:
					for m in sub.in_reals {
						if m == eq_real {
							found = true
							break
						}
					}
				case .TEXT:
					for m in sub.in_texts {
						if m == eq_text {
							found = true
							break
						}
					}
				case .BLOB:
					return group, false
				}
				if !found {
					return nil, true
				}
			}
		}

		kept := make([dynamic]Index_Plan, 0, 1, context.temp_allocator)
		for sub in group {
			if sub.use == .Eq {
				append(&kept, sub)
				break
			}
		}
		return kept[:], false
	}

	had_in := false
	keep_ints := make([dynamic]i64, 0, 8, context.temp_allocator)
	keep_reals := make([dynamic]f64, 0, 8, context.temp_allocator)
	keep_texts := make([dynamic]string, 0, 8, context.temp_allocator)
	for sub in group {
		if sub.use != .In {
			continue
		}
		switch col_type {
		case .INTEGER:
			if !had_in {
				append(&keep_ints, ..sub.in_ints)
			} else {
				next := make([dynamic]i64, 0, len(keep_ints), context.temp_allocator)
				for m in keep_ints {
					for c in sub.in_ints {
						if m == c {
							append(&next, m)
							break
						}
					}
				}
				keep_ints = next
			}
		case .REAL:
			if !had_in {
				append(&keep_reals, ..sub.in_reals)
			} else {
				next := make([dynamic]f64, 0, len(keep_reals), context.temp_allocator)
				for m in keep_reals {
					for c in sub.in_reals {
						if m == c {
							append(&next, m)
							break
						}
					}
				}
				keep_reals = next
			}
		case .TEXT:
			if !had_in {
				append(&keep_texts, ..sub.in_texts)
			} else {
				next := make([dynamic]string, 0, len(keep_texts), context.temp_allocator)
				for m in keep_texts {
					for c in sub.in_texts {
						if m == c {
							append(&next, m)
							break
						}
					}
				}
				keep_texts = next
			}
		case .BLOB:
			return group, false
		}
		had_in = true
	}
	for sub in group {
		if sub.use != .Range {
			continue
		}
		if sub.range_empty {
			return nil, true
		}
		switch col_type {
		case .INTEGER:
			next := make([dynamic]i64, 0, len(keep_ints), context.temp_allocator)
			for m in keep_ints {
				if range_value_ok(col_type, "", m, 0, sub) {
					append(&next, m)
				}
			}
			keep_ints = next
		case .REAL:
			next := make([dynamic]f64, 0, len(keep_reals), context.temp_allocator)
			for m in keep_reals {
				if range_value_ok(col_type, "", 0, m, sub) {
					append(&next, m)
				}
			}
			keep_reals = next
		case .TEXT:
			next := make([dynamic]string, 0, len(keep_texts), context.temp_allocator)
			for m in keep_texts {
				if range_value_ok(col_type, m, 0, 0, sub) {
					append(&next, m)
				}
			}
			keep_texts = next
		case .BLOB:
			return group, false
		}
	}
	if had_in {
		empty := false
		switch col_type {
		case .INTEGER:
			empty = len(keep_ints) == 0
		case .REAL:
			empty = len(keep_reals) == 0
		case .TEXT:
			empty = len(keep_texts) == 0
		case .BLOB:
			return group, false
		}
		if empty {
			return nil, true
		}

		kept := Index_Plan {
			use      = .In,
			root     = group[0].root,
			column   = group[0].column,
			col_type = col_type,
		}
		switch col_type {
		case .INTEGER:
			kept.in_ints = keep_ints[:]
		case .REAL:
			kept.in_reals = keep_reals[:]
		case .TEXT:
			kept.in_texts = keep_texts[:]
		case .BLOB:
			return group, false
		}

		out := make([dynamic]Index_Plan, 0, 1, context.temp_allocator)
		append(&out, kept)
		return out[:], false
	}

	merged := Index_Plan {
		use      = .Range,
		root     = group[0].root,
		column   = group[0].column,
		col_type = col_type,
	}
	for sub in group {
		if sub.use != .Range {
			continue
		}
		if sub.range_has_lo {
			tighter := !merged.range_has_lo
			if !tighter {
				c := range_val_cmp(
					col_type,
					sub.range_lo_text,
					sub.range_lo_int,
					sub.range_lo_real,
					merged.range_lo_text,
					merged.range_lo_int,
					merged.range_lo_real,
				)
				tighter = c > 0 || (c == 0 && merged.range_lo_inc && !sub.range_lo_inc)
			}
			if tighter {
				merged.range_has_lo = true
				merged.range_lo_inc = sub.range_lo_inc
				merged.range_lo_text = sub.range_lo_text
				merged.range_lo_int = sub.range_lo_int
				merged.range_lo_real = sub.range_lo_real
			}
		}
		if sub.range_has_hi {
			tighter := !merged.range_has_hi
			if !tighter {
				c := range_val_cmp(
					col_type,
					sub.range_hi_text,
					sub.range_hi_int,
					sub.range_hi_real,
					merged.range_hi_text,
					merged.range_hi_int,
					merged.range_hi_real,
				)
				tighter = c < 0 || (c == 0 && merged.range_hi_inc && !sub.range_hi_inc)
			}
			if tighter {
				merged.range_has_hi = true
				merged.range_hi_inc = sub.range_hi_inc
				merged.range_hi_text = sub.range_hi_text
				merged.range_hi_int = sub.range_hi_int
				merged.range_hi_real = sub.range_hi_real
			}
		}
	}
	if merged.range_has_lo && merged.range_has_hi {
		c := range_val_cmp(
			col_type,
			merged.range_lo_text,
			merged.range_lo_int,
			merged.range_lo_real,
			merged.range_hi_text,
			merged.range_hi_int,
			merged.range_hi_real,
		)
		if c > 0 || (c == 0 && !(merged.range_lo_inc && merged.range_hi_inc)) {
			return nil, true
		}
	}

	out := make([dynamic]Index_Plan, 0, 1, context.temp_allocator)
	append(&out, merged)
	return out[:], false
}

// collapse_and_subs folds an AND's same-index Eq/In/Range members into
// fewer, tighter plans (BETWEEN's `>= AND <=` becomes one bounded scan;
// contradictions become an empty Range the btree short-circuits with zero
// I/O). Groups on different indexes pass through untouched for the
// intersect path. Single survivor returns directly so EXPLAIN names the
// real shape ((range, fetch), not (and, fetch)).
@(private = "file")
collapse_and_subs :: proc(subs: []Index_Plan) -> (Index_Plan, bool) {
	n := len(subs)
	assign := make([]int, n, context.temp_allocator)
	roots := make([dynamic]u32, 0, n, context.temp_allocator)
	cols := make([dynamic]string, 0, n, context.temp_allocator)
	ng := 0
	for i in 0 ..< n {
		found := -1
		for g in 0 ..< ng {
			if roots[g] == subs[i].root && cols[g] == subs[i].column {
				found = g
				break
			}
		}
		if found < 0 {
			append(&roots, subs[i].root)
			append(&cols, subs[i].column)
			found = ng
			ng += 1
		}
		assign[i] = found
	}

	merged := make([dynamic]Index_Plan, 0, n, context.temp_allocator)
	for g in 0 ..< ng {
		grp := make([dynamic]Index_Plan, 0, 4, context.temp_allocator)
		for i in 0 ..< n {
			if assign[i] == g {
				append(&grp, subs[i])
			}
		}
		if len(grp) == 1 {
			append(&merged, grp[0])
			continue
		}

		plans, is_empty := collapse_group(grp[:])
		if is_empty {
			return Index_Plan {
					use = .Range,
					root = subs[0].root,
					column = subs[0].column,
					col_type = subs[0].col_type,
					range_empty = true,
				},
				true
		}
		append(&merged, ..plans[:])
	}
	if len(merged) == 1 {
		return merged[0], true
	}
	return Index_Plan {
			use = .And,
			column = subs[0].column,
			col_type = subs[0].col_type,
			subs = merged[:],
		},
		true
}

// index_candidate_rowids runs a plan's btree lookups and returns sorted,
// deduplicated rowids. Rowid order is the full-scan emission order, so LIMIT
// without ORDER BY behaves identically on both paths. Or unions member sets
// (all members usable by construction); And intersects them. A btree error
// fails loudly, mirroring scan errors.
@(private)
index_candidate_rowids :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	if plan.use == .Or || plan.use == .And {
		return index_candidate_multi(t, table, plan, allocator)
	}

	out, ok := index_candidate_single(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	sort_dedup_rowids(&out)
	return out, true
}

// sort_dedup_rowids sorts rowids in place and drops duplicates.
@(private = "file")
sort_dedup_rowids :: proc(out: ^[dynamic]types.Row_ID) {
	slice.sort(out[:])
	resize(out, len(slice.unique(out[:])))
	when ODIN_DEBUG {
		for i in 1 ..< len(out^) {
			assert(out^[i - 1] < out^[i])
		}
	}
}

// index_candidate_multi unions (Or) or intersects (And) member candidate
// sets. Each member arrives sorted+deduped, so union reuses the single
// sort tail and intersection is a linear two-pointer walk with one owned
// accumulator (frees each consumed set as it merges).
@(private)
index_candidate_multi :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	if len(plan.subs) == 0 {
		return nil, false
	}
	if plan.use == .Or {
		out := make([dynamic]types.Row_ID, 0, 8, allocator)
		for sub in plan.subs {
			s, ok := index_candidate_single(t, table, sub, allocator)
			if !ok {
				delete(out)
				return nil, false
			}

			append(&out, ..s[:])
			delete(s)
		}

		sort_dedup_rowids(&out)
		return out, true
	}

	// And.
	acc, ok := index_candidate_single(t, table, plan.subs[0], allocator)
	if !ok {
		return nil, false
	}

	sort_dedup_rowids(&acc)
	for i in 1 ..< len(plan.subs) {
		s, s_ok := index_candidate_single(t, table, plan.subs[i], allocator)
		if !s_ok {
			delete(acc)
			return nil, false
		}

		sort_dedup_rowids(&s)
		merged := make([dynamic]types.Row_ID, 0, min(len(acc), len(s)), allocator)
		a, b := 0, 0
		for a < len(acc) && b < len(s) {
			if acc[a] == s[b] {
				append(&merged, acc[a])
				a += 1
				b += 1
			} else if acc[a] < s[b] {
				a += 1
			} else {
				b += 1
			}
		}

		delete(acc)
		delete(s)
		acc = merged
		if len(acc) == 0 {
			break
		}
	}
	return acc, true
}

// candidate_lookup_text_eq appends one key's rowids for Eq/In candidate plans.
// Lifted from index_candidate_single's nested closure (it captures nothing);
// the Prefix arm keeps its inline lookup (different btree entry point).
@(private = "file")
candidate_lookup_text_eq :: proc(
	idx_tree: ^btree.Tree,
	root: u32,
	text: string,
	out: ^[dynamic]types.Row_ID,
	table_name: string,
) -> bool {
	found, find_err := btree.find_rowids(idx_tree, root, transmute([]u8)text)
	if find_err != .None {
		log_index_search_fail(table_name)
		return false
	}

	append(out, ..found)
	return true
}

// candidate_lookup_int_eq appends one integer key's rowids. Twin of
// candidate_lookup_eq for INTEGER indexes.
@(private = "file")
candidate_lookup_int_eq :: proc(
	idx_tree: ^btree.Tree,
	root: u32,
	val: i64,
	out: ^[dynamic]types.Row_ID,
	table_name: string,
) -> bool {
	found, find_err := btree.find_rowids(idx_tree, root, val)
	if find_err != .None {
		log_index_search_fail(table_name)
		return false
	}

	append(out, ..found)
	return true
}

// candidate_lookup_eq appends one key's rowids for Eq/In candidate plans,
// selecting the text/int/real twin by the key's static type.
@(private = "file")
candidate_lookup_eq :: proc {
	candidate_lookup_text_eq,
	candidate_lookup_int_eq,
	candidate_lookup_real_eq,
}

// candidate_lookup_range appends one range's rowids for Range plans,
// selecting the bound arms by the plan's kind (one switch: the three
// btree overloads take differently-typed bounds, so the arm selection
// lives here and the call site stays a single call).
@(private = "file")
candidate_lookup_range :: proc(
	idx_tree: ^btree.Tree,
	root: u32,
	plan: Index_Plan,
	out: ^[dynamic]types.Row_ID,
	table_name: string,
) -> bool {
	found: []types.Row_ID
	find_err: btree.Error
	switch plan.col_type {
	case .INTEGER:
		found, find_err = btree.range_rowids(
			idx_tree,
			root,
			plan.range_lo_int,
			plan.range_hi_int,
			plan.range_has_lo,
			plan.range_has_hi,
			plan.range_lo_inc,
			plan.range_hi_inc,
		)
	case .REAL:
		found, find_err = btree.range_rowids(
			idx_tree,
			root,
			plan.range_lo_real,
			plan.range_hi_real,
			plan.range_has_lo,
			plan.range_has_hi,
			plan.range_lo_inc,
			plan.range_hi_inc,
		)
	case .TEXT, .BLOB:
		found, find_err = btree.range_rowids(
			idx_tree,
			root,
			plan.range_lo_text,
			plan.range_hi_text,
			plan.range_has_lo,
			plan.range_has_hi,
			plan.range_lo_inc,
			plan.range_hi_inc,
		)
	}
	if find_err != .None {
		log_index_search_fail(table_name)
		return false
	}

	append(out, ..found)
	return true
}

// candidate_lookup_real_eq appends one real key's rowids. Twin of
// candidate_lookup_int_eq for REAL indexes.
@(private = "file")
candidate_lookup_real_eq :: proc(
	idx_tree: ^btree.Tree,
	root: u32,
	val: f64,
	out: ^[dynamic]types.Row_ID,
	table_name: string,
) -> bool {
	found, find_err := btree.find_rowids(idx_tree, root, val)
	if find_err != .None {
		log_index_search_fail(table_name)
		return false
	}

	append(out, ..found)
	return true
}

// index_candidate_single runs one Eq/Prefix/In/Range plan's btree lookups.
// Unsorted, may contain duplicates (IN overlap); the caller sorts. Eq/In
// dispatch on the plan's kind (Prefix is TEXT-only by construction).
@(private = "file")
index_candidate_single :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	idx_tree := btree.init(t.pager, plan.root)
	out := make([dynamic]types.Row_ID, 0, 8, allocator)
	#partial switch plan.use {
	case .Eq:
		switch plan.col_type {
		case .INTEGER:
			if !candidate_lookup_eq(&idx_tree, plan.root, plan.eq_int, &out, table.name) {
				delete(out)
				return nil, false
			}
		case .REAL:
			if !candidate_lookup_eq(&idx_tree, plan.root, plan.eq_real, &out, table.name) {
				delete(out)
				return nil, false
			}
		case .TEXT, .BLOB:
			if !candidate_lookup_eq(&idx_tree, plan.root, plan.eq_text, &out, table.name) {
				delete(out)
				return nil, false
			}
		}
	case .Prefix:
		found, find_err := btree.text_find_prefix(&idx_tree, plan.root, transmute([]u8)plan.prefix)

		if find_err != .None {
			log_index_search_fail(table.name)
			delete(out)
			return nil, false
		}
		append(&out, ..found)
	case .In:
		switch plan.col_type {
		case .INTEGER:
			for v in plan.in_ints {
				if !candidate_lookup_eq(&idx_tree, plan.root, v, &out, table.name) {
					delete(out)
					return nil, false
				}
			}
		case .REAL:
			for v in plan.in_reals {
				if !candidate_lookup_eq(&idx_tree, plan.root, v, &out, table.name) {
					delete(out)
					return nil, false
				}
			}
		case .TEXT, .BLOB:
			for s in plan.in_texts {
				if !candidate_lookup_eq(&idx_tree, plan.root, s, &out, table.name) {
					delete(out)
					return nil, false
				}
			}
		}
	case .Range:
		if plan.range_empty {
			return out, true
		}
		if !candidate_lookup_range(&idx_tree, plan.root, plan, &out, table.name) {
			delete(out)
			return nil, false
		}
	case .Composite:
		// Fetch-only: exact full-tuple lookup, leading-column prefix
		// scan, or leading-component range over pre-framed raws
		// (arena-borrowed, consumed here).
		if plan.comp_empty {
			return out, true
		}
		if plan.comp_key != nil {
			found, find_err := btree.composite_find_rowids(&idx_tree, plan.root, plan.comp_key)
			if find_err != .None {
				log_index_search_fail(table.name)
				delete(out)
				return nil, false
			}
			append(&out, ..found)
		} else if plan.comp_prefix != nil {
			found, find_err := btree.composite_prefix_rowids(
				&idx_tree,
				plan.root,
				plan.comp_prefix,
			)
			if find_err != .None {
				log_index_search_fail(table.name)
				delete(out)
				return nil, false
			}
			append(&out, ..found)
		} else if plan.comp_has_lo || plan.comp_has_hi {
			found, find_err := btree.composite_scan_leading(
				&idx_tree,
				plan.root,
				plan.comp_lo,
				plan.comp_hi,
				plan.comp_has_lo,
				plan.comp_has_hi,
				plan.comp_lo_inc,
				plan.comp_hi_inc,
			)
			if find_err != .None {
				log_index_search_fail(table.name)
				delete(out)
				return nil, false
			}
			append(&out, ..found)
		} else if plan.comp_full {
			found, find_err := btree.composite_scan_leading(
				&idx_tree,
				plan.root,
				nil,
				nil,
				false,
				false,
				true,
				true,
			)
			if find_err != .None {
				log_index_search_fail(table.name)
				delete(out)
				return nil, false
			}
			append(&out, ..found)
		} else {
			delete(out)
			return nil, false
		}
	case:
		delete(out)
		return nil, false
	}
	return out, true
}

// plan_index_columns renders the USING column list: the single column,
// or distinct sub columns joined for Or/And (temp-owned).
plan_index_columns :: proc(plan: Index_Plan, allocator := context.allocator) -> string {
	if plan.use != .Or && plan.use != .And {
		return plan.column
	}

	seen := make([dynamic]string, 0, len(plan.subs), context.temp_allocator)
	for sub in plan.subs {
		if !slice.contains(seen[:], sub.column) {
			append(&seen, sub.column)
		}
	}
	return strings.join(seen[:], ", ", allocator)
}

// index_use_name renders a plan shape for EXPLAIN output.
@(private)
index_use_name :: proc(use: Index_Use) -> string {
	#partial switch use {
	case .Eq:
		return "eq"
	case .Prefix:
		return "prefix"
	case .In:
		return "in"
	case .Range:
		return "range"
	case .Composite:
		return "composite"
	case .Or:
		return "or"
	case .And:
		return "and"
	}
	return "unknown"
}

// EXPLAIN plan vocabulary: one spelling per access path, next to
// index_use_name so the output strings cannot skew.
EXPLAIN_PK_SEEK     :: "PK SEEK ON %s"
EXPLAIN_ROWID_SEEK  :: "ROWID SEEK ON %s"
EXPLAIN_INDEX_COVER :: "INDEX SCAN ON %s USING %s (%s, covering)"
EXPLAIN_INDEX_FETCH :: "INDEX SCAN ON %s USING %s (%s, fetch)"
EXPLAIN_INDEX_ORDER :: "INDEX SCAN ON %s USING %s (order, fetch)"
EXPLAIN_FULL_SCAN   :: "FULL SCAN ON %s"

// explain_plan_text renders the access decision for EXPLAIN <select>: PK
// SEEK, INDEX SCAN (shape + covering/fetch), or FULL SCAN — in the same
// order the fetch path tries them (PK, covering, fetch, scan), so the
// output cannot disagree with execution. Anything that isn't a single-table
// SELECT (joins, other statements, unparseable text, unknown tables)
// echoes the inner SQL unchanged: no plan to render, nothing to decide.
@(private)
explain_plan_text :: proc(
	schema_tree: ^btree.Tree,
	stmt: parser.Explain_Stmt,
	cache: ^schema.Table_Cache = nil,
) -> string {
	echo := strings.trim_space(stmt.sql)
	inner, parse_ok, _ := parser.parse(stmt.sql, context.temp_allocator)
	if !parse_ok {
		return echo
	}

	sel, is_sel := inner.type.(parser.Select_Stmt)
	if !is_sel {
		return echo
	}
	if plan := plan_select(sel); !plan.single_table {
		return echo
	}

	tbl_name, name_ok := sel.from.(string)
	if !name_ok {
		return echo
	}

	table, found := schema.find_table_cached(schema_tree, tbl_name, cache)
	if !found {
		return echo
	}
	if wc, has_wc := sel.where_clause.?; has_wc {
		if _, seek_ok := try_pk_lookup(table^, wc, tbl_name, sel.from_alias); seek_ok {
			return fmt.tprintf(EXPLAIN_PK_SEEK, tbl_name)
		}

		visible := sel.from_alias if sel.from_alias != "" else tbl_name
		if _, seek_ok := try_rowid_lookup(table^, wc, visible); seek_ok {
			return fmt.tprintf(EXPLAIN_ROWID_SEEK, tbl_name)
		}
		if _, chain_ok := try_rowid_chain_seek(table^, wc, visible); chain_ok {
			return fmt.tprintf(EXPLAIN_ROWID_SEEK, tbl_name)
		}
		if is_covering_rowid_select(sel, table.columns) {
			if plan, idx_ok := resolve_index_covering(table^, wc, tbl_name, sel.from_alias);
			   idx_ok {
				return fmt.tprintf(
					EXPLAIN_INDEX_COVER,
					tbl_name,
					plan_index_columns(plan, context.temp_allocator),
					index_use_name(plan.use),
				)
			}
		}
		if plan, idx_ok := resolve_index_covering(table^, wc, tbl_name, sel.from_alias);
		   idx_ok && covering_known_values(plan) && is_covering_col_select(sel, plan) {
			return fmt.tprintf(
				EXPLAIN_INDEX_COVER,
				tbl_name,
				plan_index_columns(plan, context.temp_allocator),
				index_use_name(plan.use),
			)
		}
		if plan, idx_ok := resolve_index_fetch(table^, wc, tbl_name, sel.from_alias); idx_ok {
			return fmt.tprintf(
				EXPLAIN_INDEX_FETCH,
				tbl_name,
				plan_index_columns(plan, context.temp_allocator),
				index_use_name(plan.use),
			)
		}
	}
	if _, has_wc := sel.where_clause.?; !has_wc {
		if oplan, ok := resolve_order_scan(table^, sel, tbl_name, sel.from_alias); ok {
			return fmt.tprintf(
				EXPLAIN_INDEX_ORDER,
				tbl_name,
				plan_index_columns(oplan, context.temp_allocator),
			)
		}
	}
	return fmt.tprintf(EXPLAIN_FULL_SCAN, tbl_name)
}

// is_covering_rowid_select reports whether the statement is literally
// `SELECT rowid` (one projected column, no aggregates/grouping) on a table
// with no user column named "rowid" — which keeps today's meaning there
// (the projection would resolve to the user column, so index routing must
// not hijack it).
@(private)
is_covering_rowid_select :: proc(stmt: parser.Select_Stmt, cols: []types.Column) -> bool {
	if len(stmt.columns) != 1 || stmt.columns[0] != "rowid" {
		return false
	}
	if len(stmt.aggregates) != 0 || len(stmt.group_by) != 0 || stmt.having != nil {
		return false
	}

	_, has_user_rowid := schema.find_column_index(cols, "rowid")
	if has_user_rowid {
		return false
	}
	return true
}

// fetch_covering_index answers a covering `SELECT rowid ... WHERE <routed>`
// straight from the text index. Rows carry synthetic single-INTEGER-column
// values named "rowid"; the caller's finish_select tail (projection,
// ORDER BY, DISTINCT, LIMIT) runs unchanged.
@(private)
fetch_covering_index :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	from_name: string,
	allocator := context.allocator,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	bool,
) {
	cands, ok := index_candidate_rowids(t, table, plan, allocator)
	if !ok {
		return nil, nil, nil, false
	}

	defer delete(cands)
	rows := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		vals := make([]types.Value, 1, allocator)
		vals[0] = types.value(i64(rid))
		append(&rows, Row_Entry{rowid = rid, values = vals})
	}

	cols := make([]types.Column, 1, allocator)
	cols[0] = types.Column {
		name = "rowid",
		type = .INTEGER,
	}

	ranges := make([]Table_Col_Range, 1, allocator)
	ranges[0] = Table_Col_Range {
		table_name = from_name,
		start_col  = 0,
		col_count  = 1,
	}
	return rows[:], cols, ranges, true
}

// is_covering_col_select reports whether the statement projects exactly
// the routed plan's column (bare name, no aggregates/grouping). The
// projection must name plan.column — not merely any indexed column —
// because the caller emits plan.column's values; matching another index's
// column would resolve the projection against the wrong columns
// downstream. OR plans additionally need every member on the projected
// column: a cross-column union would emit other columns' keys as values.
// Like the rowid rule, one projected column only — wider projections take
// the fetch path with its full-filter recheck.
@(private)
is_covering_col_select :: proc(stmt: parser.Select_Stmt, plan: Index_Plan) -> bool {
	if len(stmt.columns) != 1 {
		return false
	}
	if len(stmt.aggregates) != 0 || len(stmt.group_by) != 0 || stmt.having != nil {
		return false
	}
	if plan.use == .Composite {
		// Single projection of a bound composite column (exact plans bind
		// all columns, prefix plans their leading run).
		for c in plan.comp_columns {
			if stmt.columns[0] == c {
				return true
			}
		}
		return false
	}
	if stmt.columns[0] != plan.column {
		return false
	}
	if plan.use == .Or {
		for sub in plan.subs {
			if sub.column != stmt.columns[0] {
				return false
			}
		}
	}
	return true
}

// covering_known_values reports whether a covering plan's output values
// are known without touching data pages: Eq/In carry their keys;
// composite exact and prefix plans carry their bound columns (decoded
// from the framed key at emit); Or of Eq/In unions known keys. Prefix,
// Range (and And, which never reaches covering) need the row. Shared with
// both fetch paths and EXPLAIN so the three can never disagree.
@(private)
covering_known_values :: proc(plan: Index_Plan) -> bool {
	#partial switch plan.use {
	case .Eq, .In:
		return true
	case .Composite:
		return plan.comp_key != nil || plan.comp_prefix != nil
	case .Or:
		for sub in plan.subs {
			if sub.use != .Eq && sub.use != .In {
				return false
			}
		}
		return len(plan.subs) > 0
	}
	return false
}

// Covering_Pair is one covering row: its rowid plus the known TEXT value.
@(private = "file")
Covering_Pair :: struct {
	rid: types.Row_ID,
	val: types.Value, // borrowed text header or by-value scalar; cloned at materialize
}

// covering_emit_text_eq emits the (rowid,text) pairs for one index key lookup.
@(private = "file")
covering_emit_text_eq :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	root: u32,
	text: string,
	out: ^[dynamic]Covering_Pair,
	allocator := context.allocator,
) -> bool {
	idx_tree := btree.init(t.pager, root)
	found, find_err := btree.find_rowids(&idx_tree, root, transmute([]u8)text)
	if find_err != .None {
		log_index_search_fail(table.name)
		return false
	}
	for rid in found {
		append(out, Covering_Pair{rid = rid, val = types.value(text)})
	}
	return true
}

// covering_emit_int_eq emits the (rowid,int) pairs for one integer key
// lookup. Twin of covering_emit_eq for INTEGER indexes.
@(private = "file")
covering_emit_int_eq :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	root: u32,
	val: i64,
	out: ^[dynamic]Covering_Pair,
	allocator := context.allocator,
) -> bool {
	idx_tree := btree.init(t.pager, root)
	found, find_err := btree.find_rowids(&idx_tree, root, val)
	if find_err != .None {
		log_index_search_fail(table.name)
		return false
	}
	for rid in found {
		append(out, Covering_Pair{rid = rid, val = types.value(val)})
	}
	return true
}

// covering_emit_real_eq emits the (rowid,real) pairs for one real key
// lookup. Twin of covering_emit_int_eq for REAL indexes. Emits the
// canonical index key (+0.0 for either zero — the codec normalizes -0.0,
// so the stored sign is unrecoverable without the row): value-equal to
// the stored rows under engine equality, deterministic across spellings.
@(private = "file")
covering_emit_real_eq :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	root: u32,
	val: f64,
	out: ^[dynamic]Covering_Pair,
	allocator := context.allocator,
) -> bool {
	idx_tree := btree.init(t.pager, root)
	found, find_err := btree.find_rowids(&idx_tree, root, val)
	if find_err != .None {
		log_index_search_fail(table.name)
		return false
	}

	key := val
	if key == 0 {
		key = 0.0
	}
	for rid in found {
		append(out, Covering_Pair{rid = rid, val = types.value(key)})
	}
	return true
}

// covering_emit_eq emits the (rowid,value) pairs for one index key lookup,
// selecting the text/int/real twin by the key's static type.
covering_emit_eq :: proc {
	covering_emit_text_eq,
	covering_emit_int_eq,
	covering_emit_real_eq,
}

// covering_emit_plan emits the pairs for one Eq/In plan node: single lookup
// for Eq, one per member for In. False on lookup failure or any other shape
// (Prefix/And route elsewhere) — the shared arm behind covering_pairs'
// top level and its Or members. Eq/In dispatch on the node's kind.
// Composite nodes decode the projected column (col) from the framed key;
// other shapes ignore col.
@(private = "file")
covering_emit_plan :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	sub: Index_Plan,
	col: string,
	out: ^[dynamic]Covering_Pair,
	allocator := context.allocator,
) -> bool {
	#partial switch sub.use {
	case .Eq:
		switch sub.col_type {
		case .INTEGER:
			return covering_emit_eq(t, table, sub.root, sub.eq_int, out, allocator)
		case .REAL:
			return covering_emit_eq(t, table, sub.root, sub.eq_real, out, allocator)
		case .TEXT, .BLOB:
			return covering_emit_eq(t, table, sub.root, sub.eq_text, out, allocator)
		}
		return false
	case .Composite:
		return covering_emit_composite(t, table, sub, col, out, allocator)
	case .In:
		switch sub.col_type {
		case .INTEGER:
			for v in sub.in_ints {
				if !covering_emit_eq(t, table, sub.root, v, out, allocator) {
					return false
				}
			}
			return true
		case .REAL:
			for v in sub.in_reals {
				if !covering_emit_eq(t, table, sub.root, v, out, allocator) {
					return false
				}
			}
			return true
		case .TEXT, .BLOB:
			for s in sub.in_texts {
				if !covering_emit_eq(t, table, sub.root, s, out, allocator) {
					return false
				}
			}
			return true
		}
		return false
	case:
		return false
	}
}

// covering_emit_composite emits the (rowid,value) pairs for one
// composite exact/prefix plan node: looks the framed key up, decodes the
// projected column (col, guaranteed bound by is_covering_col_select),
// and emits the same value per row. False on lookup or decode failure.
@(private = "file")
covering_emit_composite :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	sub: Index_Plan,
	col: string,
	out: ^[dynamic]Covering_Pair,
	allocator := context.allocator,
) -> bool {
	idx := -1
	for c, i in sub.comp_columns {
		if c == col {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}

	src := sub.comp_key if sub.comp_key != nil else sub.comp_prefix
	val, dok := cell.composite_decode_col(src, idx)
	if !dok {
		log.errorf("Error: Failed to decode index key for '%s'", table.name)
		return false
	}

	idx_tree := btree.init(t.pager, sub.root)
	found: []types.Row_ID
	find_err: btree.Error
	if sub.comp_key != nil {
		found, find_err = btree.composite_find_rowids(&idx_tree, sub.root, sub.comp_key)
	} else {
		found, find_err = btree.composite_prefix_rowids(&idx_tree, sub.root, sub.comp_prefix)
	}

	if find_err != .None {
		log_index_search_fail(table.name)
		return false
	}
	for rid in found {
		append(out, Covering_Pair{rid = rid, val = val})
	}
	return true
}

// covering_order_satisfies reports whether a known-values covering plan can
// emit rows in the statement's ORDER BY order: one ORDER BY key naming the
// projected column over Eq/In/Or/Composite emission (bound values arrive
// equal, In/Or members merge in key order; the ordered-input check nets
// NULL placement). The covering gate already pins the projection.
@(private)
covering_order_satisfies :: proc(
	plan: Index_Plan,
	stmt: parser.Select_Stmt,
	col_name: string,
	tbl_name: string,
	tbl_alias: string,
) -> bool {
	if stmt.is_distinct ||
	   len(stmt.aggregates) != 0 ||
	   len(stmt.group_by) != 0 ||
	   stmt.having != nil {
		return false
	}

	order_clause, has_o := stmt.order_by.?
	if !has_o || len(order_clause) != 1 {
		return false
	}
	#partial switch plan.use {
	case .Eq, .In, .Or, .Composite:
	case:
		return false
	}
	if len(col_name) == 0 {
		return false
	}
	return index_cond_column_ok(col_name, order_clause[0].column, tbl_name, tbl_alias)
}

// covering_pairs resolves a known-values covering plan to (rowid, value)
// pairs, one per matching row. Rowid order (scan emission order) by
// default; ordered emits key order instead (DESC reverses) so an ORDER BY
// on the projected column skips the sort. Deduplicated (duplicate IN
// members) either way — same entry carries the same value, so duplicates
// stay adjacent under both orders. col is the projected column
// (composite nodes decode it from the framed key; scalar nodes ignore it).
@(private = "file")
covering_pairs :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	col: string,
	allocator := context.allocator,
	ordered: bool = false,
	desc: bool = false,
) -> (
	[dynamic]Covering_Pair,
	bool,
) {
	out := make([dynamic]Covering_Pair, 0, 8, allocator)
	if plan.use == .Or {
		for sub in plan.subs {
			if !covering_emit_plan(t, table, sub, col, &out, allocator) {
				delete(out)
				return nil, false
			}
		}
	} else if !covering_emit_plan(t, table, plan, col, &out, allocator) {
		delete(out)
		return nil, false
	}

	if ordered {
		slice.sort_by(out[:], proc(a, b: Covering_Pair) -> bool {
			if c := compare_values(a.val, b.val); c != 0 {
				return c < 0
			}
			return a.rid < b.rid
		})
	} else {
		slice.sort_by(out[:], proc(a, b: Covering_Pair) -> bool { return a.rid < b.rid })
	}

	resize(
		&out,
		len(
			slice.unique_proc(out[:], proc(a, b: Covering_Pair) -> bool { return a.rid == b.rid }),
		),
	)
	if ordered && desc {
		slice.reverse(out[:])
	}
	when ODIN_DEBUG {
		if !ordered {
			for i in 1 ..< len(out) {
				assert(out[i - 1].rid < out[i].rid)
			}
		}
	}
	return out, true
}

// fetch_covering_col answers `SELECT <indexed-col> ... WHERE <routed>`
// straight from the index (Eq/In keys, composite bound columns, or Or
// thereof). Same tail contract as fetch_covering_index: single column of
// the projected type, rowid-ordered rows by default (key order when the
// caller passes the covering_order_satisfies verdict), caller slices LIMIT.
@(private)
fetch_covering_col :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	col_name: string,
	from_name: string,
	allocator := context.allocator,
	ordered: bool = false,
	desc: bool = false,
) -> (
	[]Row_Entry,
	[]types.Column,
	[]Table_Col_Range,
	bool,
) {
	pairs, ok := covering_pairs(t, table, plan, col_name, allocator, ordered, desc)
	if !ok {
		return nil, nil, nil, false
	}

	defer delete(pairs)
	rows := make([dynamic]Row_Entry, 0, len(pairs), allocator)
	for p in pairs {
		vals := make([]types.Value, 1, allocator)
		c, c_err := types.value_clone(p.val, allocator)
		if c_err != nil {
			delete(vals, allocator)
			return nil, nil, nil, false
		}

		vals[0] = c
		append(&rows, Row_Entry{rowid = p.rid, values = vals})
	}

	col_type := plan.col_type
	if plan.use == .Composite {
		// The projected column's own type (the plan carries its first
		// component's); the gate guarantees the name resolves.
		if idx, found := schema.find_column_index(table.columns, col_name); found {
			col_type = table.columns[idx].type
		} else {
			return nil, nil, nil, false
		}
	}

	cols := make([]types.Column, 1, allocator)
	cols[0] = types.Column {
		name = strings.clone(col_name, allocator),
		type = col_type,
	}

	ranges := make([]Table_Col_Range, 1, allocator)
	ranges[0] = Table_Col_Range {
		table_name = from_name,
		start_col  = 0,
		col_count  = 1,
	}
	return rows[:], cols, ranges, true
}

// index_order_satisfies reports whether a fetch plan emits rows in the
// statement's ORDER BY order so the later sort can skip: one ORDER BY key
// naming the plan's column over a single-key plan (Eq/Range/Prefix), or
// ORDER BY keys bound-or-tailing a single-run composite plan's column
// order (exact/prefix/leading-range — bound columns arrive equal, the rest
// continues key order from the first unbound column; the ordered-input
// check nets NULL-marker placement and per-key NULLS overrides). Mixed ASC/DESC never
// matches one run (reversal is exact only when every key flips).
// Single-column runs hold no NULLs (skipped, never matched), so placement
// there is vacuous. Unions/intersections concatenate member runs instead
// of merging them and stay on the sort path; DISTINCT and aggregation
// change cardinality and likewise stay sorted.
@(private)
index_order_satisfies :: proc(
	plan: Index_Plan,
	stmt: parser.Select_Stmt,
	tbl_name: string,
	tbl_alias: string,
	table: ^types.Table,
) -> bool {
	if stmt.is_distinct ||
	   len(stmt.aggregates) != 0 ||
	   len(stmt.group_by) != 0 ||
	   stmt.having != nil {
		return false
	}

	order_clause, has_o := stmt.order_by.?
	if !has_o || len(order_clause) == 0 {
		return false
	}
	#partial switch plan.use {
	case .Eq, .Range, .Prefix:
		if len(order_clause) != 1 || len(plan.column) == 0 {
			return false
		}
		return index_cond_column_ok(plan.column, order_clause[0].column, tbl_name, tbl_alias)
	case .Composite:
		if table == nil {
			return false
		}
		// Mixed directions never match one run (reversal is exact only
		// when every key flips); the sort path answers those.
		for o in order_clause[1:] {
			if o.desc != order_clause[0].desc {
				return false
			}
		}
		for def in table.indexes {
			if def.root != plan.root || def.root == 0 {
				continue
			}
			// Bound columns arrive equal, so ORDER BY keys naming them
			// constrain nothing; the rest must continue the index order
			// contiguously from the first unbound column (the bound run
			// is the leading run: all columns when exact, empty when the
			// plan is a leading range or provably empty).
			run := min(len(plan.comp_columns), len(def.columns))
			tailed := 0
			matched := true
			for o in order_clause {
				bound := false
				for c in plan.comp_columns {
					if index_cond_column_ok(c, o.column, tbl_name, tbl_alias) {
						bound = true
						break
					}
				}
				if bound {
					continue
				}
				if run + tailed >= len(def.columns) ||
				   !index_cond_column_ok(
						   def.columns[run + tailed],
						   o.column,
						   tbl_name,
						   tbl_alias,
					   ) {
					matched = false
					break
				}
				tailed += 1
			}
			return matched
		}
		return false
	case:
		return false
	}
}

// index_candidate_ordered runs one single-run plan's lookups in index key
// order (no rowid sort). Single-key and single-run composite plans visit
// each entry once, so rowids arrive unique; an adjacent-dedup guards
// without reordering. Anything else fails closed — member runs
// concatenate rather than merge.
@(private = "file")
index_candidate_ordered :: proc(
	t: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	allocator := context.allocator,
) -> (
	[dynamic]types.Row_ID,
	bool,
) {
	#partial switch plan.use {
	case .Eq, .Range, .Prefix, .Composite:
	case:
		return nil, false
	}

	out, ok := index_candidate_single(t, table, plan, allocator)
	if !ok {
		return nil, false
	}
	if len(out) > 1 {
		w := 1
		for r in 1 ..< len(out) {
			if out[r] != out[r - 1] {
				out[w] = out[r]
				w += 1
			}
		}
		resize(&out, w)
	}
	return out, true
}

// fetch_index_rows_ordered resolves a fetch plan's candidates in index key
// order and rechecks the full WHERE filter preserving that order (DESC
// reverses the key run — exact with no NULLs in the set). Same ownership,
// stale-miss tolerance, and no-LIMIT-pushdown stance as fetch_index_rows.
// The caller's sort skips through the ordered-input check.
@(private)
fetch_index_rows_ordered :: proc(
	t: ^btree.Tree,
	table_tree: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	wc: ^parser.Where_Clause,
	single_range: []Table_Col_Range,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
	desc: bool = false,
) -> (
	[]Row_Entry,
	bool,
) {
	cands, ok := index_candidate_ordered(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	defer delete(cands)
	if desc && len(cands) > 1 {
		slice.reverse(cands[:])
	}

	ctx, ctx_ok := init_where_ctx(wc, table.columns, single_range, t, allocator, cache).?
	if !ctx_ok {
		log.error("Error: Could not resolve WHERE clause")
		return nil, false
	}

	r := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		c, find_err := btree.tree_find(table_tree, rid, allocator)
		if find_err == .Cell_Not_Found {
			continue
		}
		if find_err != .None {
			log.errorf("Error: Failed to fetch indexed row %d", i64(rid))
			return nil, false
		}
		if !evaluate_where_ctx(ctx, c.values) {
			cell.destroy(&c, allocator)
			continue
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		cell.destroy(&c, allocator)
	}
	return r[:], true
}

// fetch_index_rows resolves a fetch plan's candidates through the data tree
// and rechecks the FULL WHERE filter per row (mandatory: the index only
// promises the usable conjunct). Stale misses are tolerated-and-skipped
// (same stance as delete's missing-entry tolerance); btree errors fail
// loudly. Ownership follows the scan loop: values transfer to the entry
// and the deferred destroy is disarmed. No LIMIT pushdown: all candidates
// materialize and the caller's tails slice — candidates arrive in rowid
// order, so the slice matches the scan path exactly.
@(private)
fetch_index_rows :: proc(
	t: ^btree.Tree,
	table_tree: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	wc: ^parser.Where_Clause,
	single_range: []Table_Col_Range,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
) -> (
	[]Row_Entry,
	bool,
) {
	cands, ok := index_candidate_rowids(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	defer delete(cands)
	ctx, ctx_ok := init_where_ctx(wc, table.columns, single_range, t, allocator, cache).?
	if !ctx_ok {
		log.error("Error: Could not resolve WHERE clause")
		return nil, false
	}

	r := make([dynamic]Row_Entry, 0, len(cands), allocator)
	for rid in cands {
		c, find_err := btree.tree_find(table_tree, rid, allocator)
		if find_err == .Cell_Not_Found {
			continue
		}
		if find_err != .None {
			log.errorf("Error: Failed to fetch indexed row %d", i64(rid))
			return nil, false
		}
		if !evaluate_where_ctx(ctx, c.values) {
			cell.destroy(&c, allocator)
			continue
		}

		append(&r, Row_Entry{c.rowid, c.values})
		c.values = nil
		cell.destroy(&c, allocator)
	}
	return r[:], true
}

// resolve_order_scan routes a WHERE-less SELECT whose ORDER BY matches an
// index's column order: one key on a single-column index, or keys
// prefixing a composite's columns from the lead (empty bound run, one
// shared direction). Synthesizes an open Range (single) or a full
// Composite plan; the fetch completes index-skipped rows (NULLs,
// unindexed classes) from the table scan, and the ordered-input check
// nets the merge. DISTINCT, aggregates, WHERE, and mixed directions stay
// on the scan path.
@(private)
resolve_order_scan :: proc(
	table: types.Table,
	stmt: parser.Select_Stmt,
	tbl_name: string,
	tbl_alias: string,
) -> (
	plan: Index_Plan,
	ok: bool,
) {
	if stmt.is_distinct ||
	   len(stmt.aggregates) != 0 ||
	   len(stmt.group_by) != 0 ||
	   stmt.having != nil {
		return {}, false
	}
	if _, has_wc := stmt.where_clause.?; has_wc {
		return {}, false
	}

	order_clause, has_o := stmt.order_by.?
	if !has_o || len(order_clause) == 0 {
		return {}, false
	}
	for o in order_clause[1:] {
		if o.desc != order_clause[0].desc {
			return {}, false
		}
	}
	for def in table.indexes {
		if def.root == 0 {
			continue
		}
		if len(def.columns) < 2 {
			if len(order_clause) != 1 {
				continue
			}
			if index_cond_column_ok(def.column, order_clause[0].column, tbl_name, tbl_alias) {
				return Index_Plan {
						use = .Range,
						root = def.root,
						column = def.column,
						col_type = def.col_type,
					},
					true
			}
			continue
		}
		if len(order_clause) > len(def.columns) {
			continue
		}

		matched := true
		for o, i in order_clause {
			if !index_cond_column_ok(def.columns[i], o.column, tbl_name, tbl_alias) {
				matched = false
				break
			}
		}
		if matched {
			return Index_Plan {
					use = .Composite,
					root = def.root,
					column = def.column,
					col_type = def.col_types[0],
					comp_full = true,
				},
				true
		}
	}
	return {}, false
}

// fetch_order_scan answers a WHERE-less ORDER BY over resolve_order_scan's
// plan: the full index run in key order (DESC reverses) remapped onto one
// table scan by rowid — no per-row point lookups. Index-skipped rows
// (NULLs, unindexed classes) complete from the scan; the merge only
// stands when every leftover is NULL on the first ORDER BY key (the key
// deciding block placement, NULL block first or last per the clause),
// otherwise the scan rows return in rowid order and the sort answers.
// Stale index entries (no table row) skip like Cell_Not_Found; an
// unresolvable ORDER BY key returns the scan rows so the sort fails
// loudly exactly as today.
@(private)
fetch_order_scan :: proc(
	t: ^btree.Tree,
	table_tree: ^btree.Tree,
	table: ^types.Table,
	plan: Index_Plan,
	order_clause: []parser.Order_By_Column,
	cols: []types.Column,
	single_range: []Table_Col_Range,
	allocator := context.allocator,
	cache: ^schema.Table_Cache = nil,
	desc: bool = false,
) -> (
	[]Row_Entry,
	bool,
) {
	cands, ok := index_candidate_ordered(t, table, plan, allocator)
	if !ok {
		return nil, false
	}

	defer delete(cands)
	if desc && len(cands) > 1 {
		slice.reverse(cands[:])
	}

	scanned, scan_err := scan_table(table_tree, table, nil, nil, t, allocator, cache, single_range)
	if scan_err {
		return nil, false
	}

	resolver := build_column_resolver(cols, single_range)
	sort_indices, r_ok := resolve_sort_indices(order_clause, resolver)
	if !r_ok || sort_indices[0] == ROWID_PROJ {
		return scanned, true
	}

	rowmap := make(map[types.Row_ID]int, len(scanned), context.temp_allocator)
	for row, i in scanned {
		rowmap[row.rowid] = i
	}

	consumed := make([]bool, len(scanned), context.temp_allocator)
	ordered := make([dynamic]Row_Entry, 0, len(scanned), context.temp_allocator)
	leftovers := make([dynamic]Row_Entry, 0, len(scanned), context.temp_allocator)
	for rid in cands {
		if si, found := rowmap[rid]; found && !consumed[si] {
			consumed[si] = true
			append(&ordered, scanned[si])
		}
	}
	for row, i in scanned {
		if consumed[i] {
			continue
		}
		if !types.is_null(row.values[sort_indices[0]]) {
			return scanned, true
		}
		append(&leftovers, row)
	}

	nulls_first := order_clause[0].nulls_first
	if !nulls_first {
		nulls_first = desc
	}

	merged := make([dynamic]Row_Entry, 0, len(scanned), allocator)
	if nulls_first {
		append(&merged, ..leftovers[:])
	}

	append(&merged, ..ordered[:])
	if !nulls_first {
		append(&merged, ..leftovers[:])
	}
	return merged[:], true
}
