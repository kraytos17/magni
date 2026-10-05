// Package btree — vacuum: rebuild trees into fresh, densely packed pages.
//
// Collect all live entries (in key order), pack leaves greedily, build
// interior levels bottom-up, return a new root. Original pages are never
// touched, so existing roots/snapshots stay readable until GC reclaims the
// superseded pages. O(n) maintenance for explicit VACUUM — not hot-path use.
// Rowid tree: tree_vacuum; text index: text_tree_vacuum.
package btree

import "src:cell"
import "src:pager"
import "src:types"

// Bulk_Node describes one packed child for bulk interior building: both
// edge keys. Separators store the RIGHT child's minimum — the descent
// convention (interior_lower_bound routes key >= sep rightward), so a left
// maximum would misroute exact-boundary lookups.
Bulk_Node :: struct {
	id     : u32,
	min_key: types.Row_ID,
	max_key: types.Row_ID,
}

// bulk_pack_level packs one interior level over nodes with the same greedy
// FOR capacity rule as vacuum_build_interiors, except separators are each
// non-first child's minimum (never a left maximum). Returns the next level
// up.
@(private = "file")
bulk_pack_level :: proc(t: ^Tree, level: []Bulk_Node) -> ([dynamic]Bulk_Node, Error) {
	next := make([dynamic]Bulk_Node, 0, 64, context.temp_allocator)
	i := 0
	for i < len(level) {
		e := i
		for e + 1 < len(level) {
			use_for, _ := dense_choose_encoding(level[i + 1].min_key, level[e + 1].min_key)
			kw := DENSE_DELTA_WIDTH if use_for else DENSE_FULL_KEY_WIDTH
			m_new := e + 2 - i
			if size_of(Dense_Interior_Header) + (m_new - 1) * kw + m_new * DENSE_CHILD_WIDTH >
			   PAGE_SIZE {
				break
			}
			e += 1
		}

		seps := make([dynamic]types.Row_ID, 0, e - i + 1, context.temp_allocator)
		children := make([dynamic]u32, 0, e - i + 2, context.temp_allocator)
		for k in i + 1 ..= e {
			append(&seps, level[k].min_key)
		}
		for k in i ..= e {
			append(&children, level[k].id)
		}

		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None {
			return next, .Page_Full
		}
		if b_err := dense_build_from_sorted(
			page.data,
			Page_Id(page.page_num),
			seps[:],
			children[:],
		); b_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return next, b_err
		}

		page_id := page.page_num
		pager.mark_dirty(t.pager, page_id)
		pager.unpin_page(t.pager, page_id)
		append(
			&next,
			Bulk_Node{id = page_id, min_key = level[i].min_key, max_key = level[e].max_key},
		)
		i = e + 1
	}
	return next, .None
}

// bulk_build_interiors packs node levels bottom-up until one root remains.
@(private = "file")
bulk_build_interiors :: proc(t: ^Tree, nodes: [dynamic]Bulk_Node) -> (u32, Error) {
	level := nodes
	for len(level) > 1 {
		up, u_err := bulk_pack_level(t, level[:])
		if u_err != .None {
			return 0, u_err
		}
		level = up
	}
	return level[0].id, .None
}

// tree_depth counts levels from root to leaf along the leftmost spine.
tree_depth :: proc(t: ^Tree, root: u32) -> (depth: int, err: Error) {
	curr := root
	for {
		depth += 1
		node := load_node(t, curr) or_return
		if is_leaf(node) {
			unpin_node(t, node)
			return depth, .None
		}

		child, c_err := node.layout.vtable.child_at(node.data, Page_Id(node.id), 0)
		unpin_node(t, node)
		if c_err != .None {
			return 0, c_err
		}
		curr = child
	}
}

// graft_right_chain links a fresh batch chain as the right sibling of an
// existing tree: one new 2-child interior over [old, batch] with the batch
// minimum as separator (the descent convention — never a left maximum).
// Both sides must already sit at equal depth (caller compares tree_depth);
// the pages of the existing tree are never touched, so COW/rollback hold
// unchanged. Counts compose exactly (no recount).
graft_right_chain :: proc(
	t: ^Tree,
	old_root: u32,
	old_count: int,
	batch_root: u32,
	batch_min: types.Row_ID,
	batch_count: int,
) -> (
	root: u32,
	err: Error,
) {
	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None {
		return 0, .Page_Full
	}

	seps := [1]types.Row_ID{batch_min}
	children := [2]u32{old_root, batch_root}
	if b_err := dense_build_from_sorted(page.data, Page_Id(page.page_num), seps[:], children[:]);
	   b_err != .None {
		pager.unpin_page(t.pager, page.page_num)
		return 0, b_err
	}

	page_id := page.page_num
	pager.mark_dirty(t.pager, page_id)
	pager.unpin_page(t.pager, page_id)
	stats_row_count_set(tree_stats(t), page_id, old_count + batch_count)
	return page_id, .None
}

// bulk_start_leaf allocates + inits one empty slotdir leaf for bulk
// packing. The page stays pinned: the caller builds into it, then appends
// the handle, marks dirty, and unpins (same ownership as vacuum_start_leaf,
// split out so the pack loop's two open sites share it).
@(private = "file")
bulk_start_leaf :: proc(t: ^Tree) -> (leaf: Node, err: Error) {
	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None {
		return {}, .Page_Full
	}
	if !init_slot_leaf_page(page.data, page.page_num) {
		pager.unpin_page(t.pager, page.page_num)
		return {}, .Invalid_Page_Header
	}

	layout, _, l_err := layout_for_page(page.data, Page_Id(page.page_num))
	if l_err != .None {
		pager.unpin_page(t.pager, page.page_num)
		return {}, l_err
	}

	n, n_err := node_from_bytes(page.page_num, page.data, layout)
	if n_err != .None {
		pager.unpin_page(t.pager, page.page_num)
		return {}, n_err
	}
	return n, .None
}

// build_sorted_tree bulk-loads pre-sorted (rowid, values) runs into a fresh
// tree: greedy leaf packing (same Page_Full rollover as vacuum_collect_cb)
// then bottom-up dense interiors. Package-visible for the executor's
// multi-row INSERT fast path; vacuum internals stay file-private.
// Callers guarantee sorted, duplicate-free rowids. Fresh pages only — the
// existing tree is untouched, so the COW/rollback contract holds unchanged.
// Single-page results return the leaf itself (no interior level).
build_sorted_tree :: proc(
	t: ^Tree,
	rowids: []types.Row_ID,
	valuess: [][]types.Value,
) -> (
	root: u32,
	err: Error,
) {
	if len(rowids) == 0 {
		return vacuum_empty_root(t)
	}

	handles := make([dynamic]Bulk_Node, 0, 64, context.temp_allocator)
	leaf := Node{}
	leaf_min, leaf_max := types.Row_ID(0), types.Row_ID(0)
	leaf_open := false
	for i in 0 ..< len(rowids) {
		if !leaf_open {
			n, s_err := bulk_start_leaf(t)
			if s_err != .None {
				return 0, s_err
			}

			leaf = n
			leaf_min = rowids[i]
			leaf_open = true
		}
		if e := node_insert_leaf_cell(t, &leaf, rowids[i], valuess[i]); e == .Page_Full {
			append(&handles, Bulk_Node{id = leaf.id, min_key = leaf_min, max_key = leaf_max})
			pager.mark_dirty(t.pager, leaf.id)
			pager.unpin_page(t.pager, leaf.id)
			leaf_open = false
			n, s_err := bulk_start_leaf(t)
			if s_err != .None {
				return 0, s_err
			}

			leaf = n
			leaf_min = rowids[i]
			leaf_open = true
			if r_err := node_insert_leaf_cell(t, &leaf, rowids[i], valuess[i]); r_err != .None {
				return 0, r_err
			}
		} else if e != .None {
			return 0, e
		}
		leaf_max = rowids[i]
	}
	if leaf_open {
		append(&handles, Bulk_Node{id = leaf.id, min_key = leaf_min, max_key = leaf_max})
		pager.mark_dirty(t.pager, leaf.id)
		pager.unpin_page(t.pager, leaf.id)
	}
	if len(handles) == 1 {
		stats_row_count_set(tree_stats(t), handles[0].id, len(rowids))
		return handles[0].id, .None
	}

	new_root, b_err := bulk_build_interiors(t, handles)
	if b_err != .None {
		return 0, b_err
	}

	stats_row_count_set(tree_stats(t), new_root, len(rowids))
	return new_root, .None
}

// vacuum_empty_root allocates a fresh empty slotdir leaf for vacuuming an
// empty tree (collection yielded nothing to rebuild).
@(private = "file")
vacuum_empty_root :: proc(t: ^Tree) -> (root: u32, err: Error) {
	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None {
		return 0, .Page_Full
	}
	if !init_slot_leaf_page(page.data, page.page_num) {
		pager.unpin_page(t.pager, page.page_num)
		return 0, .Invalid_Page_Header
	}

	root = page.page_num
	pager.unpin_page(t.pager, root)
	return root, .None
}

// vacuum_build_interiors packs handle levels bottom-up into dense interiors
// until one root remains. Each interior node holds cells for all children
// except the last, which becomes the rightmost pointer. handles must be
// non-empty (the caller routes the empty tree to vacuum_empty_root).
@(private = "file")
vacuum_build_interiors :: proc(
	t: ^Tree,
	handles: [dynamic]Node_Handle,
) -> (
	root: u32,
	err: Error,
) {
	level := handles
	for len(level) > 1 {
		next := make([dynamic]Node_Handle, 0, 64, context.temp_allocator)
		i := 0
		for i < len(level) {
			// Greedy chunk: children [i..e] with separators mins[i+1..e]
			// (each non-first child's minimum — the descent convention;
			// a left maximum would misroute exact-boundary lookups). The
			// last child becomes the rightmost. Capacity from the stored
			// range via the FOR rule — arithmetic only, no trial builds.
			// Single-child chunks always fit.
			e := i
			for e + 1 < len(level) {
				first := level[i + 1].min_key
				last := level[e + 1].min_key
				use_for, _ := dense_choose_encoding(first, last)
				kw := DENSE_DELTA_WIDTH if use_for else DENSE_FULL_KEY_WIDTH
				m_new := e + 2 - i
				if size_of(Dense_Interior_Header) + (m_new - 1) * kw + m_new * DENSE_CHILD_WIDTH >
				   PAGE_SIZE {
					break
				}
				e += 1
			}

			ckeys := make([dynamic]types.Row_ID, 0, e - i + 1, context.temp_allocator)
			cchildren := make([dynamic]u32, 0, e - i + 2, context.temp_allocator)
			for k in i + 1 ..= e {
				append(&ckeys, level[k].min_key)
			}
			for k in i ..= e {
				append(&cchildren, level[k].id)
			}

			page, a_err := pager.allocate_page(t.pager)
			if a_err != .None {
				return 0, .Page_Full
			}
			if b_err := dense_build_from_sorted(
				page.data,
				Page_Id(page.page_num),
				ckeys[:],
				cchildren[:],
			); b_err != .None {
				pager.unpin_page(t.pager, page.page_num)
				return 0, b_err
			}

			max_key := level[e].max_key
			page_id := page.page_num
			pager.unpin_page(t.pager, page_id)
			append(&next, Node_Handle{id = page_id, min_key = level[i].min_key, max_key = max_key})
			i = e + 1
		}
		level = next
	}
	return level[0].id, .None
}

// tree_vacuum rebuilds the tree into fresh, densely packed pages and returns
// the new root. The original pages are left untouched (COW-safe), so
// time-travel snapshots remain readable; the superseded pages are reclaimed
// by the next garbage-collection pass. It is an O(n) maintenance operation
// intended for an explicit VACUUM, not for hot-path use.
tree_vacuum :: proc(t: ^Tree, allocator := context.allocator) -> (new_root: u32, err: Error) {
	handles := make([dynamic]Node_Handle, 0, 64, context.temp_allocator)
	vc := vacuum_ctx {
		t          = t,
		leaf_empty = true,
		handles    = &handles,
	}

	if e := tree_foreach(t, vacuum_collect_cb, &vc); e != .None {
		return 0, e
	}
	if vc.failed {
		return 0, .Page_Full
	}
	if !vc.leaf_empty {
		if e := vacuum_finish_leaf(&vc); e != .None {
			return 0, e
		}
	}
	if len(handles) == 0 {
		return vacuum_empty_root(t)
	}
	return vacuum_build_interiors(t, handles)
}

// Node_Handle identifies a packed node and its subtree edge keys, used
// while building the new interior levels of a vacuumed tree. Separators
// store right-child minima (the descent convention), so both edges travel.
@(private)
Node_Handle :: struct {
	id     : u32,
	min_key: types.Row_ID,
	max_key: types.Row_ID,
}

// vacuum_ctx carries the bulk-loader state across tree_foreach callbacks.
@(private)
vacuum_ctx :: struct {
	t         : ^Tree,
	leaf      : Node,
	leaf_empty: bool,
	handles   : ^[dynamic]Node_Handle,
	leaf_min  : types.Row_ID,
	leaf_max  : types.Row_ID,
	failed    : bool,
}

// vacuum_collect_cb serializes each row into the current packed leaf, starting
// a fresh leaf when the current one is full.
@(private)
vacuum_collect_cb :: proc(c: ^cell.Cell, ud: rawptr) -> bool {
	vc := cast(^vacuum_ctx)ud
	if vc.failed {
		return false
	}
	if vc.leaf_empty {
		if v_err := vacuum_start_leaf(vc); v_err != .None {
			vc.failed = true
			return false
		}
		vc.leaf_min = c.rowid
	}
	if e := node_insert_leaf_cell(vc.t, &vc.leaf, c.rowid, c.values); e == .Page_Full {
		if f_err := vacuum_finish_leaf(vc); f_err != .None {
			vc.failed = true
			return false
		}
		if s_err := vacuum_start_leaf(vc); s_err != .None {
			vc.failed = true
			return false
		}

		vc.leaf_min = c.rowid
		if r_err := node_insert_leaf_cell(vc.t, &vc.leaf, c.rowid, c.values); r_err != .None {
			vc.failed = true
			return false
		}
	} else if e != .None {
		vc.failed = true
		return false
	}

	vc.leaf_max = c.rowid
	return true
}

@(private)
vacuum_start_leaf :: proc(vc: ^vacuum_ctx) -> Error {
	page, a_err := pager.allocate_page(vc.t.pager)
	if a_err != .None {
		return .Page_Full
	}
	if !init_slot_leaf_page(page.data, page.page_num) {
		pager.unpin_page(vc.t.pager, page.page_num)
		return .Invalid_Page_Header
	}

	leaf_layout, _, l_err := layout_for_page(page.data, Page_Id(page.page_num))
	if l_err != .None {
		pager.unpin_page(vc.t.pager, page.page_num)
		return l_err
	}

	n, n_err := node_from_bytes(page.page_num, page.data, leaf_layout)
	if n_err != .None {
		pager.unpin_page(vc.t.pager, page.page_num)
		return n_err
	}

	vc.leaf = n
	vc.leaf_empty = false
	return .None
}

@(private)
vacuum_finish_leaf :: proc(vc: ^vacuum_ctx) -> Error {
	if vc.leaf_empty {
		return .None
	}

	append(vc.handles, Node_Handle{id = vc.leaf.id, min_key = vc.leaf_min, max_key = vc.leaf_max})
	pager.unpin_page(vc.t.pager, vc.leaf.id)
	vc.leaf_empty = true
	return .None
}

// Text_Node_Handle identifies a packed text node and its subtree edge keys
// (full codec keys, temp-owned) for rebuilding text interior levels.
// Separators are right-child minima on every path (the text descent
// convention — equality routes rightward); max_key travels alongside so
// each handle keeps its node's full edge range.
@(private)
Text_Node_Handle :: struct {
	id     : u32,
	min_key: []u8,
	max_key: []u8,
}

// text_vacuum_collect walks a text tree in order, appending (full text,
// rowid) pairs. Recursive descent mirroring count_recursive's structure —
// not tree_foreach (rowid-primary cell callbacks don't fit text entries).
@(private)
text_vacuum_collect :: proc(
	t: ^Tree,
	page_id: u32,
	texts: ^[dynamic][]u8,
	rids: ^[dynamic]types.Row_ID,
) -> Error {
	node, n_err := load_node(t, page_id)
	if n_err != .None {
		return n_err
	}

	defer unpin_node(t, node)
	if is_leaf(node) {
		if node.header.page_type != .LEAF_TEXT {
			return .Invalid_Page_Header
		}

		prefix, p_err := text_prefix(node.data, Page_Id(node.id))
		if p_err != .None {
			return p_err
		}

		count := int(node.header.cell_count)
		for i in 0 ..< count {
			suf, rid, k_err := text_entry_at(node.data, Page_Id(node.id), i)
			if k_err != .None {
				return k_err
			}

			full := make([]u8, len(prefix) + len(suf), context.temp_allocator)
			copy(full, prefix)
			copy(full[len(prefix):], suf)
			append(texts, full)
			append(rids, rid)
		}
		return .None
	}
	if node.header.page_type != .TEXT_INTERIOR {
		return .Invalid_Page_Header
	}

	n := get_cell_count(node.data, node.id)
	for i in 0 ..< n + 1 {
		child, c_err := text_interior_child_at(node.data, Page_Id(node.id), i)
		if c_err != .None {
			return c_err
		}
		if r_err := text_vacuum_collect(t, child, texts, rids); r_err != .None {
			return r_err
		}
	}
	return .None
}

// text_leaf_chunk_bytes measures entries [lo,hi) as one packed leaf:
// prefix (exact shared of first/last — sorted input shares it across all),
// slots, and cells. Must match text_build_from_sorted's own accounting
// exactly, or chunk planning would under/over-fill pages the builder then
// rejects.
@(private = "file")
text_leaf_chunk_bytes :: proc(texts: [][]u8, lo: int, hi: int) -> (total: int, plen: int) {
	plen = cell.text_index_shared_prefix(texts[lo], texts[hi - 1], PAGE_SIZE)
	total = 10 + plen + (hi - lo) * size_of(Text_Slot)
	for k in lo ..< hi {
		total += TEXT_ENTRY_ROWID_LEN + (len(texts[k]) - plen)
	}
	return
}

// vacuum_empty_text_root allocates a fresh empty text leaf for vacuuming an
// empty text index (collection yielded nothing to rebuild).
@(private = "file")
vacuum_empty_text_root :: proc(t: ^Tree) -> (root: u32, err: Error) {
	page, a_err := pager.allocate_page(t.pager)
	if a_err != .None {
		return 0, .Page_Full
	}
	if !init_text_leaf_page(page.data, page.page_num) {
		pager.unpin_page(t.pager, page.page_num)
		return 0, .Invalid_Page_Header
	}

	root = page.page_num
	pager.unpin_page(t.pager, root)
	return root, .None
}

// vacuum_pack_text_leaves greedily packs collected (text, rowid) runs into
// fresh leaves, extending each chunk while it measures to fit. A lone
// oversize entry fails the build loudly (same as primary).
@(private = "file")
vacuum_pack_text_leaves :: proc(
	t: ^Tree,
	texts: [dynamic][]u8,
	rids: [dynamic]types.Row_ID,
) -> (
	handles: [dynamic]Text_Node_Handle,
	err: Error,
) {
	handles = make([dynamic]Text_Node_Handle, 0, 64, context.temp_allocator)
	n := len(texts)
	i := 0
	for i < n {
		e := i
		for e + 1 < n {
			total, _ := text_leaf_chunk_bytes(texts[:], i, e + 2)
			if total > PAGE_SIZE {
				break
			}
			e += 1
		}

		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None {
			return nil, .Page_Full
		}
		if b_err := text_build_from_sorted(
			page.data,
			Page_Id(page.page_num),
			texts[i:e + 1],
			rids[i:e + 1],
		); b_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return nil, b_err
		}

		max_key, m_err := text_make_key(texts[e], rids[e])
		if m_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return nil, m_err
		}

		min_key, n_err := text_make_key(texts[i], rids[i])
		if n_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return nil, n_err
		}

		page_id := page.page_num
		pager.unpin_page(t.pager, page_id)
		append(&handles, Text_Node_Handle{id = page_id, min_key = min_key, max_key = max_key})
		i = e + 1
	}
	return handles, .None
}

// vacuum_build_text_interiors packs text handle levels bottom-up until one
// root remains. Chunk children [lo..hi] with separators keys[lo..<hi] (the
// last child becomes the rightmost); capacity is exact byte arithmetic — no
// trial builds. A lone child always fits (~16 bytes), so every chunk is
// non-empty. handles must be non-empty (the caller routes the empty index
// to vacuum_empty_text_root).
@(private = "file")
vacuum_build_text_interiors :: proc(
	t: ^Tree,
	handles: [dynamic]Text_Node_Handle,
) -> (
	root: u32,
	err: Error,
) {
	level := handles
	for len(level) > 1 {
		next := make([dynamic]Text_Node_Handle, 0, 64, context.temp_allocator)
		lo := 0
		for lo < len(level) {
			hi := lo
			for hi + 1 < len(level) {
				// Try extending to hi+1: children [lo..hi+1], stored
				// separators are the minima of [lo+1..hi+1] (descent
				// routes equality rightward — never a left maximum).
				m := (hi + 2) - lo
				total := 8 + m * 4 + (m - 1) * 4
				for k in lo + 1 ..= hi + 1 {
					total += len(level[k].min_key)
				}
				if total > PAGE_SIZE {
					break
				}
				hi += 1
			}

			seps := make([dynamic][]u8, 0, hi - lo + 1, context.temp_allocator)
			children := make([dynamic]u32, 0, hi - lo + 2, context.temp_allocator)
			for k in lo + 1 ..= hi {
				append(&seps, level[k].min_key)
			}
			for k in lo ..= hi {
				append(&children, level[k].id)
			}

			page, a_err := pager.allocate_page(t.pager)
			if a_err != .None {
				return 0, .Page_Full
			}
			if b_err := text_interior_build_from_sorted(
				page.data,
				Page_Id(page.page_num),
				seps[:],
				children[:],
			); b_err != .None {
				pager.unpin_page(t.pager, page.page_num)
				return 0, b_err
			}

			page_id := page.page_num
			pager.unpin_page(t.pager, page_id)
			append(
				&next,
				Text_Node_Handle {
					id = page_id,
					min_key = level[lo].min_key,
					max_key = level[hi].max_key,
				},
			)
			lo = hi + 1
		}
		level = next
	}
	return level[0].id, .None
}

// bulk_pack_text_level packs one text interior level with the same greedy
// byte capacity rule as vacuum_build_text_interiors, except separators are
// each non-first child's minimum full key (the text descent convention —
// find_upper routes equality rightward). Sizes are summed over the stored
// separators exactly, so the build cannot overflow from estimation.
@(private = "file")
bulk_pack_text_level :: proc(
	t: ^Tree,
	level: []Text_Node_Handle,
) -> (
	[dynamic]Text_Node_Handle,
	Error,
) {
	next := make([dynamic]Text_Node_Handle, 0, 64, context.temp_allocator)
	lo := 0
	for lo < len(level) {
		hi := lo
		for hi + 1 < len(level) {
			// Try extending to hi+1: children [lo..hi+1], stored
			// separators are the minima of [lo+1..hi+1].
			m := (hi + 2) - lo
			total := 8 + m * 4 + (m - 1) * 4
			for k in lo + 1 ..= hi + 1 {
				total += len(level[k].min_key)
			}
			if total > PAGE_SIZE {
				break
			}
			hi += 1
		}

		seps := make([dynamic][]u8, 0, hi - lo + 1, context.temp_allocator)
		children := make([dynamic]u32, 0, hi - lo + 2, context.temp_allocator)
		for k in lo + 1 ..= hi {
			append(&seps, level[k].min_key)
		}
		for k in lo ..= hi {
			append(&children, level[k].id)
		}

		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None {
			return next, .Page_Full
		}
		if b_err := text_interior_build_from_sorted(
			page.data,
			Page_Id(page.page_num),
			seps[:],
			children[:],
		); b_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return next, b_err
		}

		page_id := page.page_num
		pager.mark_dirty(t.pager, page_id)
		pager.unpin_page(t.pager, page_id)
		append(
			&next,
			Text_Node_Handle {
				id = page_id,
				min_key = level[lo].min_key,
				max_key = level[hi].max_key,
			},
		)
		lo = hi + 1
	}
	return next, .None
}

// bulk_build_text_interiors packs text node levels bottom-up until one root
// remains (right-min separators throughout).
@(private = "file")
bulk_build_text_interiors :: proc(t: ^Tree, nodes: [dynamic]Text_Node_Handle) -> (u32, Error) {
	level := nodes
	for len(level) > 1 {
		up, u_err := bulk_pack_text_level(t, level[:])
		if u_err != .None {
			return 0, u_err
		}
		level = up
	}
	return level[0].id, .None
}

// build_sorted_text_index bulk-loads pre-sorted (text, rowid) pairs into a
// fresh text index: greedy leaf packing (same measure rule as
// vacuum_pack_text_leaves) then bottom-up right-min interiors. Sort order
// must be codec order (text bytes, then rowid). Fresh pages only — the
// existing index is untouched. Zero pairs build nothing and return root 0
// (the caller keeps the empty index as-is, matching the per-row path which
// inserts nothing).
build_sorted_text_index :: proc(
	t: ^Tree,
	texts: [][]u8,
	rids: []types.Row_ID,
) -> (
	root: u32,
	err: Error,
) {
	if len(texts) == 0 {
		return 0, .None
	}

	handles := make([dynamic]Text_Node_Handle, 0, 64, context.temp_allocator)
	n := len(texts)
	i := 0
	for i < n {
		e := i
		for e + 1 < n {
			total, _ := text_leaf_chunk_bytes(texts[:], i, e + 2)
			if total > PAGE_SIZE {
				break
			}
			e += 1
		}

		page, a_err := pager.allocate_page(t.pager)
		if a_err != .None {
			return 0, .Page_Full
		}
		if b_err := text_build_from_sorted(
			page.data,
			Page_Id(page.page_num),
			texts[i:e + 1],
			rids[i:e + 1],
		); b_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return 0, b_err
		}

		max_key, m_err := text_make_key(texts[e], rids[e])
		if m_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return 0, m_err
		}

		min_key, n_err := text_make_key(texts[i], rids[i])
		if n_err != .None {
			pager.unpin_page(t.pager, page.page_num)
			return 0, n_err
		}

		page_id := page.page_num
		pager.mark_dirty(t.pager, page_id)
		pager.unpin_page(t.pager, page_id)
		append(&handles, Text_Node_Handle{id = page_id, min_key = min_key, max_key = max_key})
		i = e + 1
	}
	if len(handles) == 1 {
		return handles[0].id, .None
	}
	return bulk_build_text_interiors(t, handles)
}

// text_index_is_empty reports whether a text index holds no entries: root 0
// (never built) or a zero-cell leaf (fresh root from CREATE INDEX before
// any row arrives). Package-visible for the bulk-text gate.
text_index_is_empty :: proc(t: ^Tree, root: u32) -> bool {
	if root == 0 {
		return true
	}

	node, l_err := load_node(t, root)
	if l_err != .None {
		return false
	}

	defer unpin_node(t, node)
	return is_leaf(node) && node.header.cell_count == 0
}

// text_tree_vacuum rebuilds a text index into fresh, packed pages and
// returns the new root: collect in key order, pack leaves greedily, build
// interior levels bottom-up; originals untouched for COW safety. Separators
// propagate verbatim as right-child minima — no re-encoding, no
// recomparison. O(n) maintenance for explicit VACUUM, not hot-path use.
text_tree_vacuum :: proc(t: ^Tree, allocator := context.allocator) -> (new_root: u32, err: Error) {
	texts := make([dynamic][]u8, 0, 64, context.temp_allocator)
	rids := make([dynamic]types.Row_ID, 0, 64, context.temp_allocator)
	if c_err := text_vacuum_collect(t, t.root, &texts, &rids); c_err != .None {
		return 0, c_err
	}

	n := len(texts)
	if n == 0 {
		return vacuum_empty_text_root(t)
	}

	handles, h_err := vacuum_pack_text_leaves(t, texts, rids)
	if h_err != .None {
		return 0, h_err
	}
	return vacuum_build_text_interiors(t, handles)
}
