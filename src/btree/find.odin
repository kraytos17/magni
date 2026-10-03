package btree

import "src:types"

@(private)
find_interior_cell_for_child :: #force_inline proc(
	data: []u8,
	page_id: u32,
	child_page: u32,
	layout: Page_Layout,
) -> int {
	// Child-pointer scan through child_at: correct on both encodings
	// (V2 cells store u32be children, dense arrays u32le — the table
	// normalizes). Cells only (0..<count); the rightmost child stays the
	// caller's right_ptr branch, exactly as before.
	pid := Page_Id(page_id)
	cell_count := get_cell_count(data, page_id)
	for i in 0 ..< cell_count {
		child, c_err := layout.vtable.child_at(data, pid, i)
		if c_err != .None { continue }
		if child == child_page {
			return i
		}
	}
	return -1
}

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
		if k_err != .None { return left, false }
		if key >= k {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, true
}
