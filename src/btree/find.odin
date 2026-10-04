// Package btree — interior descend routing: lower bound and child lookup.
package btree

import "src:types"

// find_interior_cell_for_child returns the cell index whose child pointer is
// child_page, or -1. Scans cells only (0..<count) through child_at, which
// normalizes byte order across encodings; the rightmost child is the
// caller's right_ptr branch, never a cell.
@(private)
find_interior_cell_for_child :: #force_inline proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	layout: Page_Layout,
) -> int {
	pid := Page_Id(page_id)
	cell_count := get_cell_count(data, page_id)
	for i in 0 ..< cell_count {
		child, c_err := layout.vtable.child_at(data, pid, i)
		if c_err != .None {
			continue
		}
		if child == child_page {
			return i
		}
	}
	return -1
}

// interior_lower_bound returns the first cell index whose separator is
// strictly greater than key (equality continues rightward — separators are
// right-child minima). ok=false when a separator is unreadable.
@(private)
interior_lower_bound :: #force_inline proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: Page_Layout,
) -> (
	int,
	bool,
) {
	cell_count := get_cell_count(data, page_id)
	pid := Page_Id(page_id)
	left := 0
	right := cell_count
	for left < right {
		mid := left + (right - left) / 2
		k, k_err := layout.vtable.key_at(data, pid, mid)
		if k_err != .None {
			return left, false
		}
		if key >= k {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}
