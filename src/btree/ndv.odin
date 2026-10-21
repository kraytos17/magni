// Package btree — index distinct-key counts (ANALYZE input).
//
// index_ndv walks one secondary index in key order counting distinct full
// keys (duplicates collapsed, NULLs never indexed so never counted).
// Entries arrive sorted, so one transition-counting pass suffices; keys
// are opaque bytes (prefix + suffix reconstructed per entry), making the
// walk kind-agnostic across text/int/real/composite leaves — the same
// entry readers the vacuum collect uses. Cold path only (.analyze);
// failures are I/O or corruption, never empty results.
package btree

import "core:bytes"

// ndv_acc accumulates the transition count across the recursive walk:
// prev holds the last full key (nil before the first entry).
@(private = "file")
ndv_acc :: struct {
	prev : []u8,
	count: int,
}

// index_ndv returns the distinct full-key count of the index at root, or
// 0 for an empty index. Fails closed on unreadable pages or non-index
// roots (root 0 reads 0 — nothing indexed). Callers store through
// stats_index_ndv_set; staleness is handled by invalidation, not here.
index_ndv :: proc(t: ^Tree, root: u32) -> (int, Error) {
	if root == 0 {
		return 0, .None
	}

	acc := ndv_acc{}
	if w_err := ndv_walk(t, root, &acc); w_err != .None {
		return 0, w_err
	}
	return acc.count, .None
}

// index_ndv_store walks the index at root, caches the distinct count,
// and returns it. The single writer behind .analyze (tests read through
// index_ndv_get); failures leave any previous value untouched.
index_ndv_store :: proc(t: ^Tree, root: u32) -> (int, Error) {
	ndv, w_err := index_ndv(t, root)
	if w_err != .None {
		return 0, w_err
	}
	stats_index_ndv_set(tree_stats(t), root, ndv)
	return ndv, .None
}

// ndv_walk counts one subtree's key transitions into acc (recursive like
// text_vacuum_collect: leaves read entries, interiors fan out).
@(private = "file")
ndv_walk :: proc(t: ^Tree, page_id: u32, acc: ^ndv_acc) -> Error {
	node, n_err := load_node(t, page_id)
	if n_err != .None {
		return n_err
	}

	defer unpin_node(t, node)
	if is_leaf(node) {
		return ndv_walk_leaf(node, acc)
	}
	if !is_index_interior_type(node.header.page_type) {
		return .Invalid_Page_Header
	}

	n := get_cell_count(node.data, node.id)
	for i in 0 ..< n + 1 {
		child, c_err := text_interior_child_at(node.data, Page_Id(node.id), i)
		if c_err != .None {
			return c_err
		}
		if r_err := ndv_walk(t, child, acc); r_err != .None {
			return r_err
		}
	}
	return .None
}

// ndv_walk_leaf counts one leaf's transitions: the first entry always
// opens a run, later entries increment on byte difference from prev.
// Full keys are temp-owned scratch (cold path; pages stay pinned during
// the comparison, so borrows would also be sound — owned is simpler).
@(private = "file")
ndv_walk_leaf :: proc(node: Node, acc: ^ndv_acc) -> Error {
	if !is_index_leaf_type(node.header.page_type) {
		return .Invalid_Page_Header
	}

	prefix, p_err := text_prefix(node.data, Page_Id(node.id))
	if p_err != .None {
		return p_err
	}

	count := int(node.header.cell_count)
	for i in 0 ..< count {
		suf, _, k_err := text_entry_at(node.data, Page_Id(node.id), i)
		if k_err != .None {
			return k_err
		}

		full := make([]u8, len(prefix) + len(suf), context.temp_allocator)
		copy(full, prefix)
		copy(full[len(prefix):], suf)
		if acc.prev == nil || bytes.compare(full, acc.prev) != 0 {
			acc.count += 1
		}
		acc.prev = full
	}
	return .None
}
