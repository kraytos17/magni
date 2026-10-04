// Package btree — copy-on-write write path.
//
// Every mutation copies pages along the touched path (copy_on_write), so
// readers keep their root and snapshots stay readable. Entry points return
// the new root; callers publish it (schema/pending overlay). A root split
// happens at the copy: the current root page is rewritten when possible.
package btree

import "core:mem"
import "src:pager"
import "src:types"

// copy_on_write copies a page via the pager and, for a special (header-carrying)
// page, relocates the page-1 database header to offset 0 in the new copy. The
// pager owns the page-1 semantics; the layout relocation is btree's concern.
@(private, require_results)
copy_on_write :: proc(t: ^Tree, page_id: u32) -> (u32, Error) {
	new_page, err := pager.copy_page(t.pager, page_id)
	if err != .None {
		return 0, .Page_Read_Failed
	}
	if pager.is_special_page(page_id) {
		if !relocate_copied_page1(new_page) {
			return 0, .Invalid_Page_Header
		}
	}
	return new_page.page_num, .None
}

// relocate_copied_page1 moves a page-1 copy's data area (which starts at the
// 100-byte database header boundary) down to offset 0, so the COW copy is a
// normal B-tree page. Returns false on an invalid page header.
@(private = "file")
relocate_copied_page1 :: proc(page: ^pager.Page) -> bool {
	hdr := get_header(page.data, 1)
	if hdr == nil {
		return false
	}

	SRC_HDR_OFF :: types.DATABASE_HEADER_SIZE
	DST_HDR_OFF :: 0

	// Whole-area move (header + all bytes uniformly down by 100): valid for
	// every live layout (slotdir, dense, text) because all of them are
	// preserved by a uniform shift.
	data_sz := types.PAGE_SIZE - SRC_HDR_OFF
	tmp := make([]u8, data_sz, context.temp_allocator)
	copy(tmp, page.data[SRC_HDR_OFF:])

	mem.zero_slice(page.data[SRC_HDR_OFF:])
	copy(page.data[DST_HDR_OFF:], tmp)
	return true
}

// tree_insert_cow inserts (rowid, values) with COW copies along the touched
// path. Returns the new root (== t.root when the page was rewritten in
// place). Leaf roots fast-path; full leaves split the root.
tree_insert_cow :: proc(
	t: ^Tree,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	new_root: u32,
	err: Error,
) {
	root_node, load_err := load_node(t, t.root)
	if load_err != .None {
		return 0, load_err
	}

	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		new_root, err = copy_on_write(t, t.root)
		if err != .None {
			return 0, err
		}

		cow_node, n_err := load_node(t, new_root)
		if n_err != .None {
			return 0, n_err
		}

		defer unpin_node(t, cow_node)
		e := node_insert_leaf_cell(t, &cow_node, rowid, values)
		if e != .Page_Full {
			pager.unpin_page(t.pager, new_root)
			return new_root, e
		}

		result_root, split_err := split_leaf_root(t, new_root, rowid, values)
		pager.unpin_page(t.pager, new_root)
		return result_root, split_err
	}

	result, r_err := insert_recursive(t, t.root, rowid, values, true)
	if r_err != .None {
		return 0, r_err
	}

	new_root = result.new_page
	if result.did_split {
		new_root_page, a_err := pager.allocate_page(t.pager)
		if a_err != .None {
			return 0, .Page_Full
		}

		// Single-separator dense root: keys=[split_key], children are
		// the split halves (left = new_page, rightmost = right_page).
		rkeys := [1]types.Row_ID{result.split_key}
		rchildren := [2]u32{result.new_page, result.right_page}
		if b_err := dense_build_from_sorted(
			new_root_page.data,
			Page_Id(new_root_page.page_num),
			rkeys[:],
			rchildren[:],
		); b_err != .None {
			pager.unpin_page(t.pager, new_root_page.page_num)
			return 0, b_err
		}

		pager.mark_dirty(t.pager, new_root_page.page_num)
		pager.unpin_page(t.pager, new_root_page.page_num)
		new_root = new_root_page.page_num
	}

	pager.unpin_page(t.pager, new_root)
	return new_root, .None
}

// tree_delete_cow deletes key with COW copies along the path. No merge/
// rebalance: emptied leaves stay, reclaimed by vacuum. Returns the new root
// (== t.root when the root was rewritten in place).
tree_delete_cow :: proc(t: ^Tree, key: types.Row_ID) -> (new_root: u32, err: Error) {
	Update_COW_Result :: struct {
		new_page: u32,
	}

	delete_cow_recursive :: proc(
		t: ^Tree,
		pid: u32,
		key: types.Row_ID,
		cow: bool,
	) -> (
		Update_COW_Result,
		Error,
	) {
		page_id := pid
		if cow {
			new_id, c_err := copy_on_write(t, page_id)
			if c_err != .None {
				return {}, c_err
			}
			page_id = new_id
		}

		node, n_err := load_node(t, page_id)
		if n_err != .None {
			return {}, n_err
		}

		defer unpin_node(t, node)
		if is_leaf(node) {
			return Update_COW_Result{new_page = node.id}, delete_from_leaf(t, &node, key)
		}

		child_id, _ := node_find_child(&node, key)
		child_result, c_err := delete_cow_recursive(t, child_id, key, true)
		if c_err != .None {
			return {}, c_err
		}
		if child_result.new_page != child_id {
			pager.unpin_page(t.pager, child_result.new_page)
		}
		if child_result.new_page != child_id {
			if !node_update_child_ptr(&node, key, child_result.new_page) {
				return {}, .Invalid_Cell_Pointer
			}
		}

		pager.mark_dirty(t.pager, node.id)
		return Update_COW_Result{new_page = node.id}, .None
	}

	result, rec_err := delete_cow_recursive(t, t.root, key, true)
	if rec_err != .None {
		return 0, rec_err
	}

	pager.unpin_page(t.pager, result.new_page)
	return result.new_page, .None
}

// tree_update_cow replaces the value at rowid (delete + reinsert within the
// copied leaf) with COW copies along the path. The rowid must already exist
// (callers resolve/validate before reaching here). Returns the new root.
tree_update_cow :: proc(
	t: ^Tree,
	rowid: types.Row_ID,
	values: []types.Value,
) -> (
	new_root: u32,
	err: Error,
) {
	Update_Result :: struct {
		new_page: u32,
	}

	update_recursive :: proc(
		t: ^Tree,
		pid: u32,
		rowid: types.Row_ID,
		values: []types.Value,
		cow: bool,
	) -> (
		Update_Result,
		Error,
	) {
		page_id := pid
		if cow {
			new_id, c_err := copy_on_write(t, page_id)
			if c_err != .None {
				return {}, c_err
			}
			page_id = new_id
		}

		node, n_err := load_node(t, page_id)
		if n_err != .None {
			return {}, n_err
		}

		defer unpin_node(t, node)
		if is_leaf(node) {
			if d_err := delete_from_leaf(t, &node, rowid); d_err != .None {
				return {}, d_err
			}
			if i_err := node_insert_leaf_cell(t, &node, rowid, values); i_err != .None {
				return {}, i_err
			}
			return Update_Result{new_page = node.id}, .None
		}

		child_id, _ := node_find_child(&node, rowid)
		child_result, c_err := update_recursive(t, child_id, rowid, values, true)
		if c_err != .None {
			return {}, c_err
		}
		if child_result.new_page != child_id {
			pager.unpin_page(t.pager, child_result.new_page)
		}
		if child_result.new_page != child_id {
			if !node_update_child_ptr(&node, rowid, child_result.new_page) {
				return {}, .Invalid_Cell_Pointer
			}
		}

		pager.mark_dirty(t.pager, node.id)
		return Update_Result{new_page = node.id}, .None
	}

	result, rec_err := update_recursive(t, t.root, rowid, values, true)
	if rec_err != .None {
		return 0, rec_err
	}

	pager.unpin_page(t.pager, result.new_page)
	return result.new_page, .None
}
