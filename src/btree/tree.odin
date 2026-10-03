// Package btree implements the copy-on-write B+tree.
package btree

import "base:intrinsics"
import "core:encoding/endian"
import "core:log"
import "core:mem"
import "core:strings"
import "src:cell"
import "src:pager"
import "src:types"
import "src:util/varint"

MAX_TREE_DEPTH :: 12

DEFAULT_CONFIG := Config {
	allocator        = {},
	zero_copy        = false,
	check_duplicates = true,
}

Tree :: struct {
	pager : ^pager.Pager,
	root  : u32,
	config: Config,
}

Config :: struct #all_or_none {
	using _         : types.Storage_Config,
	check_duplicates: bool,
}

Error :: enum u8 {
	None,
	Page_Read_Failed,
	Invalid_Page_Header,
	Invalid_Cell_Pointer,
	Cell_Deserialize_Failed,
	Page_Full,
	Duplicate_Rowid,
	Cell_Not_Found,
	Invalid_Bounds,
	Serialization_Failed,
	Duplicate_Key,
	Unsupported_Format,
}

Node :: struct {
	id    : u32,
	data  : []u8,
	header: ^Page_Header,
	layout: Page_Layout,
}

Insert_COW_Result :: struct #all_or_none {
	new_page  : u32,
	did_split : bool,
	right_page: u32,
	split_key : types.Row_ID,
}

init :: proc(p: ^pager.Pager, root_page: u32, config := DEFAULT_CONFIG) -> Tree {
	c := config
	if c.allocator.procedure == nil { c.allocator = context.allocator }

	t := Tree {
		pager  = p,
		root   = root_page,
		config = c,
	}

	attach_stats(&t)
	return t
}

// is_leaf is public: the leaf/interior question is asked by every page
// reader (and pinned by tests for each new page type).
is_leaf :: #force_inline proc "contextless" (n: Node) -> bool {
	return(
		n.header.page_type == .LEAF_TABLE_COLUMNAR ||
		n.header.page_type == .LEAF_SLOTDIR ||
		n.header.page_type == .LEAF_TEXT \
	)
}

@(private)
node_leaf :: #force_inline proc "contextless" (n: Node) -> ^Leaf_Header {return get_leaf_header(
		n.data,
		n.id,
	)}

@(private)
unpin_node :: #force_inline proc(t: ^Tree, n: Node) { pager.unpin_page(t.pager, n.id) }

@(require_results)
load_node :: proc(t: ^Tree, page_id: u32) -> (Node, Error) {
	page, err := pager.get_page(t.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }

	layout, _, l_err := layout_for_page(page.data, Page_Id(page_id))
	if l_err != .None { return {}, l_err }
	return node_from_bytes(page_id, page.data, layout)
}

@(private)
node_from_bytes :: proc(id: u32, data: []u8, layout: Page_Layout) -> (Node, Error) {
	common_hdr := get_header(data, id)
	if common_hdr == nil { return {}, .Invalid_Page_Header }
	return Node{id = id, data = data, header = common_hdr, layout = layout}, .None
}

@(private = "file")
leaf_lower_bound :: #force_inline proc(
	data: []u8,
	page_id: u32,
	target: types.Row_ID,
	layout: Page_Layout,
) -> (
	int,
	bool,
) {
	cell_count := get_cell_count(data, page_id)
	pid := Page_Id(page_id)
	left, right := 0, cell_count
	for left < right {
		mid := left + (right - left) / 2
		k, k_err := layout.vtable.key_at(data, pid, mid)
		if k_err != .None { return left, false }
		if k < target { left = mid + 1 } else { right = mid }
	}
	return left, true
}

@(private)
node_find_child :: #force_inline proc(n: ^Node, key: types.Row_ID) -> (u32, int) {
	return node_find_child_data(n.data, n.id, key, n.layout)
}

@(private, require_results)
node_insert_leaf_cell :: proc(
	t: ^Tree,
	n: ^Node,
	rowid: types.Row_ID,
	values: []types.Value,
) -> Error {
	if !is_leaf(n^) { return .Invalid_Page_Header }
	// A columnar page that cannot expand to row-major in place fails the
	// insert loudly (.Cell_Deserialize_Failed) instead of proceeding on
	// undecodable bytes. .Page_Full is deliberately not used: it would send
	// a doomed split down the abort path and leak the allocated right page.
	if intrinsics.unlikely(!ensure_row_major(n.data, n.id)) {
		return .Cell_Deserialize_Failed
	}

	rl, _, r_err := layout_for_page(n.data, Page_Id(n.id))
	if r_err != .None { return r_err }

	n.layout = rl
	idx, lb_ok := leaf_lower_bound(n.data, n.id, rowid, n.layout)
	if t.config.check_duplicates {
		if lb_ok && idx < int(n.header.cell_count) {
			ptr, p_err := n.layout.vtable.cell_ptr_at(n.data, Page_Id(n.id), idx)
			if p_err != .None { return .Invalid_Cell_Pointer }

			rid, rid_ok := cell.get_rowid(n.data, int(ptr))
			if intrinsics.unlikely(rid_ok && rid == rowid) { return .Duplicate_Rowid }
		}
	}

	cinfo := cell.compute_info(rowid, values)
	free_off := freeblock_alloc(
		n.data,
		n.header.first_freeblock,
		u16(cinfo.total_size),
		&n.header.first_freeblock,
	)
	if free_off != 0 {
		bytes_written, ok := cell.serialize(n.data[int(free_off):], rowid, values, cinfo)
		if !ok || bytes_written != cinfo.total_size { return .Serialization_Failed }
		if s_err := n.layout.vtable.slot_insert(
			n.data,
			Page_Id(n.id),
			idx,
			rowid,
			Cell_Off(u16(free_off)),
		); s_err != .None {
			return s_err
		}

		n.header.cell_count += 1
		pager.mark_dirty(t.pager, n.id)
		invalidate_page_int_range(t, n.id)
		return .None
	}

	base_offset := get_page_header_offset(n.id)
	header_size := page_header_size(n.header.page_type)
	ptr_area_end := base_offset + header_size + int(n.header.cell_count + 1) * size_of(Slot)

	if ptr_area_end >= int(n.header.cell_content_offset) { return .Page_Full }
	if cinfo.total_size > int(n.header.cell_content_offset) - ptr_area_end {
		return .Page_Full
	}

	new_offset := int(n.header.cell_content_offset) - cinfo.total_size
	bytes_written, ok := cell.serialize(n.data[new_offset:], rowid, values, cinfo)
	if !ok || bytes_written != cinfo.total_size { return .Serialization_Failed }
	if s_err := n.layout.vtable.slot_insert(
		n.data,
		Page_Id(n.id),
		idx,
		rowid,
		Cell_Off(u16(new_offset)),
	); s_err != .None {
		return s_err
	}

	n.header.cell_count += 1
	n.header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(t.pager, n.id)
	invalidate_page_int_range(t, n.id)
	return .None
}

@(private, require_results)
node_update_child_ptr :: proc(n: ^Node, key: types.Row_ID, new_sibling: u32) -> bool {
	pid := Page_Id(n.id)
	_, children_off, count, _, _, g_err := dense_geometry(n.data, pid)
	if g_err != .None { return false }

	idx, ok := interior_lower_bound(n.data, n.id, key, n.layout)
	if !ok || idx > count {
		return false
	}
	if !endian.put_u32(n.data[children_off + idx * DENSE_CHILD_WIDTH:], .Little, new_sibling) {
		return false
	}
	return true
}

@(private, require_results)
insert_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	cow: bool,
) -> (
	result: Insert_COW_Result,
	err: Error,
) {
	new_page_num := page_id
	if cow {
		var, cow_err := copy_on_write(t, page_id)
		if cow_err != .None { return {}, cow_err }
		new_page_num = var
	}

	curr := load_node(t, new_page_num) or_return
	defer unpin_node(t, curr)
	if is_leaf(curr) {
		return insert_into_leaf(t, &curr, rowid, values, new_page_num)
	}
	return insert_into_interior(t, &curr, rowid, values, cow, new_page_num)
}

// insert_into_leaf inserts into a leaf node, splitting and retrying on
// Page_Full. Returns did_split=true with the new right page on split.
@(private = "file", require_results)
insert_into_leaf :: proc(
	t: ^Tree,
	curr: ^Node,
	rowid: types.Row_ID,
	values: []types.Value,
	new_page_num: u32,
) -> (
	Insert_COW_Result,
	Error,
) {
	e := node_insert_leaf_cell(t, curr, rowid, values)
	if e == .Page_Full {
		original_count := int(curr.header.cell_count)
		split, s_err := split_leaf_node(t, curr)
		if s_err != .None { return {}, s_err }

		mid := original_count / 2
		stats_row_count_set(tree_stats(t), curr.id, mid)
		stats_row_count_set(tree_stats(t), split.right_page, original_count - mid)
		target_id := curr.id
		if rowid >= split.split_key { target_id = split.right_page }

		target_node, t_err := load_node(t, target_id)
		if t_err != .None { return {}, t_err }

		defer unpin_node(t, target_node)
		retry_err := node_insert_leaf_cell(t, &target_node, rowid, values)
		if retry_err != .None { return {}, retry_err }

		stats_row_count_set(tree_stats(t), target_id, int(target_node.header.cell_count))
		return Insert_COW_Result {
				new_page = curr.id,
				did_split = true,
				right_page = split.right_page,
				split_key = split.split_key,
			},
			.None
	}
	if e == .None {
		stats_row_count_set(tree_stats(t), curr.id, int(curr.header.cell_count))
	}
	return Insert_COW_Result {
			new_page = new_page_num,
			did_split = false,
			right_page = 0,
			split_key = 0,
		},
		e
}

// insert_into_interior descends to the child, then handles the child's
// result: repoint on no-split, or absorb the split halves.
@(private = "file", require_results)
insert_into_interior :: proc(
	t: ^Tree,
	curr: ^Node,
	rowid: types.Row_ID,
	values: []types.Value,
	cow: bool,
	new_page_num: u32,
) -> (
	Insert_COW_Result,
	Error,
) {
	child_id, child_idx := node_find_child(curr, rowid)
	was_rightmost := child_idx == -1
	child_result, c_err := insert_recursive(t, child_id, rowid, values, cow)
	if c_err != .None { return {}, c_err }
	if cow && child_result.new_page != child_id {
		pager.unpin_page(t.pager, child_result.new_page)
	}
	if !child_result.did_split {
		if cow && child_result.new_page != child_id {
			if !node_update_child_ptr(curr, rowid, child_result.new_page) {
				return {}, .Invalid_Cell_Pointer
			}
		}

		update_row_count(t, curr.id, 1)
		pager.mark_dirty(t.pager, curr.id)
		return Insert_COW_Result {
				new_page = new_page_num,
				did_split = false,
				right_page = 0,
				split_key = 0,
			},
			.None
	}
	return handle_interior_child_split(
		t,
		curr,
		&child_result,
		was_rightmost,
		child_idx,
		new_page_num,
	)
}

// handle_interior_child_split absorbs a split child: the left half keeps the
// child's slot (with a new upper bound) and the right half gets a new entry.
// The rightmost child (reachable only via right_ptr) is special-cased.
@(private = "file", require_results)
handle_interior_child_split :: proc(
	t: ^Tree,
	curr: ^Node,
	child_result: ^Insert_COW_Result,
	was_rightmost: bool,
	child_idx: int,
	new_page_num: u32,
) -> (
	Insert_COW_Result,
	Error,
) {
	pid := Page_Id(curr.id)
	n := get_cell_count(curr.data, curr.id)
	keys := make([dynamic]types.Row_ID, 0, n + 1, context.temp_allocator)
	children := make([dynamic]u32, 0, n + 2, context.temp_allocator)
	for i in 0 ..< n {
		k, k_err := curr.layout.vtable.key_at(curr.data, pid, i)
		if k_err != .None { return {}, k_err }

		append(&keys, k)
		c, c_err := curr.layout.vtable.child_at(curr.data, pid, i)
		if c_err != .None { return {}, c_err }
		append(&children, c)
	}

	rc, rc_err := curr.layout.vtable.child_at(curr.data, pid, n)
	if rc_err != .None { return {}, rc_err }

	append(&children, rc)
	insert_key := child_result.split_key
	if was_rightmost {
		append(&keys, insert_key)
		children[n] = child_result.new_page
		append(&children, child_result.right_page)
	} else {
		idx := child_idx
		if idx < 0 || idx >= n {
			return {}, .Invalid_Page_Header
		}

		old_sep := keys[idx]
		keys[idx] = insert_key
		children[idx] = child_result.new_page
		nkeys := make([dynamic]types.Row_ID, 0, len(keys) + 1, context.temp_allocator)
		for k in keys[:idx + 1] { append(&nkeys, k) }

		append(&nkeys, old_sep)
		for k in keys[idx + 1:] { append(&nkeys, k) }

		nchildren := make([dynamic]u32, 0, len(children) + 1, context.temp_allocator)
		for c in children[:idx + 1] { append(&nchildren, c) }

		append(&nchildren, child_result.right_page)
		for c in children[idx + 1:] { append(&nchildren, c) }

		keys, children = nkeys, nchildren
		insert_key = old_sep
	}

	if b_err := dense_build_from_sorted(curr.data, Page_Id(curr.id), keys[:], children[:]);
	   b_err == .None {
		update_row_count(t, curr.id, 1)
		pager.mark_dirty(t.pager, curr.id)
		return Insert_COW_Result {
				new_page = new_page_num,
				did_split = false,
				right_page = 0,
				split_key = 0,
			},
			.None
	} else if b_err != .Page_Full {
		return {}, b_err
	}

	m := len(keys)
	mid := dense_split_mid(m)
	if lb_err := dense_build_from_sorted(
		curr.data,
		Page_Id(curr.id),
		keys[:mid],
		children[:mid + 1],
	); lb_err != .None {
		return {}, lb_err
	}

	new_page, a_err := pager.allocate_page(t.pager)
	if a_err != nil { return {}, .Page_Full }
	defer pager.unpin_page(t.pager, new_page.page_num)
	if rb_err := dense_build_from_sorted(
		new_page.data,
		Page_Id(new_page.page_num),
		keys[mid + 1:],
		children[mid + 1:],
	); rb_err != .None {
		return {}, rb_err
	}
	if _, c_err := count_recursive(t, curr.id); c_err != .None {
		return {}, c_err
	}
	if _, c_err := count_recursive(t, new_page.page_num); c_err != .None {
		return {}, c_err
	}

	pager.mark_dirty(t.pager, curr.id)
	pager.mark_dirty(t.pager, new_page.page_num)
	return Insert_COW_Result {
			new_page = new_page_num,
			did_split = true,
			right_page = new_page.page_num,
			split_key = keys[mid],
		},
		.None
}

@(private = "file")
rowid_exists :: proc(
	data: []u8,
	page_id: u32,
	target_rowid: types.Row_ID,
	layout: Page_Layout,
) -> bool {
	cell_count := get_cell_count(data, page_id)
	idx, ok := leaf_lower_bound(data, page_id, target_rowid, layout)
	if !ok || idx >= cell_count { return false }

	ptr, p_err := layout.vtable.cell_ptr_at(data, Page_Id(page_id), idx)
	if p_err != .None { return false }

	rowid, ok2 := cell.get_rowid(data, int(ptr))
	return ok2 && rowid == target_rowid
}

// Insert a row into the b-tree. Handles root splits transparently.
// Returns .Duplicate_Rowid if check_duplicates is enabled and the rowid exists.
@(require_results)
tree_insert :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	root_node := load_node(t, t.root) or_return
	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		e := node_insert_leaf_cell(t, &root_node, rowid, values)
		if e != .Page_Full {
			if e == .None {
				stats_row_count_set(tree_stats(t), t.root, int(root_node.header.cell_count))
			}
			return e
		}
		if _, s_err := split_leaf_root(t, t.root); s_err != .None {
			return s_err
		}

		result, r_err := insert_recursive(t, t.root, rowid, values, false)
		if r_err != .None { return r_err }
		if result.did_split {
			if s_err := split_interior_root(
				t,
				{did_split = true, right_page = result.right_page, split_key = result.split_key},
			); s_err != .None { return s_err }
		}
		if _, c_err := count_recursive(t, t.root); c_err != .None { return c_err }
		return .None
	}

	result, i_err := insert_recursive(t, t.root, rowid, values, false)
	if i_err != .None { return i_err }
	if result.did_split {
		if s_err := split_interior_root(
			t,
			{did_split = true, right_page = result.right_page, split_key = result.split_key},
		); s_err != .None {
			return s_err
		}
		if _, c_err := count_recursive(t, t.root); c_err != .None {
			return c_err
		}
	}
	return .None
}

@(private = "file")
descend_to_leaf :: proc(
	t: ^Tree,
	get_child: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32,
	ctx: rawptr,
	root_override: u32 = 0,
) -> (
	leaf: Node,
	err: Error,
) {
	curr := t.root if root_override == 0 else root_override
	for {
		leaf = load_node(t, curr) or_return
		if is_leaf(leaf) { return }
		curr = get_child(leaf.data, leaf.id, ctx); unpin_node(t, leaf)
	}
}

@(private = "file")
node_find_child_data :: #force_inline proc(
	data: []u8,
	page_id: u32,
	key: types.Row_ID,
	layout: Page_Layout,
) -> (
	u32,
	int,
) {
	pid := Page_Id(page_id)
	cell_count := get_cell_count(data, page_id)
	rightmost, r_err := layout.vtable.child_at(data, pid, cell_count)
	if r_err != .None { return 0, -1 }
	if cell_count == 0 { return rightmost, -1 }

	idx, ok := interior_lower_bound(data, page_id, key, layout)
	if !ok || idx >= cell_count { return rightmost, -1 }

	child, c_err := layout.vtable.child_at(data, pid, idx)
	if c_err != .None { return rightmost, -1 }
	return child, idx
}

@(private = "file")
descend_by_rightmost :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	pid := Page_Id(page_id)
	count := get_cell_count(data, page_id)
	layout, _, l_err := layout_for_page(data, pid)
	if l_err != .None { return 0 }

	rc, rc_err := layout.vtable.child_at(data, pid, count)
	if rc_err != .None { return 0 }
	return rc
}

Descend_Key_Ctx :: struct {
	key: types.Row_ID,
}

@(private = "file")
descend_by_key :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	dk := (^Descend_Key_Ctx)(ctx)
	layout, _, l_err := layout_for_page(data, Page_Id(page_id))
	if l_err != .None { return 0 }

	child, _ := node_find_child_data(data, page_id, dk.key, layout)
	return child
}

// Find a row by Row_ID. Returns a Cell (with deep-copied or zero-copy values per Config).
// Returns .Cell_Not_Found if the row doesn't exist.
@(require_results)
tree_find :: proc(t: ^Tree, key: types.Row_ID, allocator: mem.Allocator) -> (cell.Cell, Error) {
	dk := Descend_Key_Ctx {
		key = key,
	}

	leaf, err := descend_to_leaf(t, descend_by_key, &dk)
	if err != .None { return {}, err }
	defer unpin_node(t, leaf)

	if intrinsics.unlikely(is_columnar(leaf.data, leaf.id)) {
		num_cols, found := detect_columnar_col_count(leaf.data, leaf.id)
		if !found { return {}, .Invalid_Cell_Pointer }

		boff := get_page_header_offset(leaf.id)
		row_count := int(leaf.header.cell_count)
		rowid_pos := boff + cell.COLUMNAR_DIR_OFFSET + num_cols * size_of(cell.Col_Header)
		current_rid: u64 = 0
		for i in 0 ..< row_count {
			delta, n, ok := varint.decode(leaf.data, rowid_pos)
			if !ok { break }

			current_rid += delta
			rowid_pos += n
			if types.Row_ID(current_rid) == key {
				cc, cc_ok := cell.read_columnar_cell(
					leaf.data,
					num_cols,
					i,
					cell.Config{allocator = allocator, zero_copy = t.config.zero_copy},
					boff,
				)
				if cc_ok { return cc, .None }
				return {}, .Cell_Deserialize_Failed
			}
		}
		return {}, .Cell_Not_Found
	}

	lid := Page_Id(leaf.id)
	idx, ok := leaf_lower_bound(leaf.data, leaf.id, key, leaf.layout)
	if !ok { return {}, .Invalid_Cell_Pointer }

	cell_count := get_cell_count(leaf.data, leaf.id)
	if idx < cell_count {
		ptr, p_err := leaf.layout.vtable.cell_ptr_at(leaf.data, lid, idx)
		if p_err != .None { return {}, .Invalid_Cell_Pointer }

		rid, ok1 := cell.get_rowid(leaf.data, int(ptr))
		if ok1 && rid == key {
			c, _, des_ok := cell.deserialize(
				leaf.data,
				int(ptr),
				cell.Config{allocator = allocator, zero_copy = t.config.zero_copy},
			)
			if !des_ok { return {}, .Cell_Deserialize_Failed }
			return c, .None
		}
	}
	return {}, .Cell_Not_Found
}

tree_next_rowid :: proc(t: ^Tree) -> (result: types.Row_ID, err: Error) {
	leaf := descend_to_leaf(t, descend_by_rightmost, nil) or_return
	defer unpin_node(t, leaf)
	if leaf.header.cell_count == 0 { result = 1; return }

	last_ptr, p_err := leaf.layout.vtable.cell_ptr_at(
		leaf.data,
		Page_Id(leaf.id),
		int(leaf.header.cell_count) - 1,
	)
	if p_err != .None { err = .Invalid_Cell_Pointer; return }

	last_id, ok := cell.get_rowid(leaf.data, int(last_ptr))
	if !ok { err = .Invalid_Cell_Pointer; return }
	result = last_id + 1; return
}

tree_count_rows :: proc(t: ^Tree) -> (count: int, err: Error) {
	if c, ok := stats_row_count_get(tree_stats(t), t.root); ok { count = c; return }
	count = count_recursive(t, t.root) or_return
	return
}

@(private, require_results)
count_recursive :: proc(t: ^Tree, page_id: u32) -> (result: int, err: Error) {
	if count, ok := stats_row_count_get(tree_stats(t), page_id); ok {
		result = count
		return
	}

	node := load_node(t, page_id) or_return
	defer unpin_node(t, node)
	if is_leaf(node) {
		result = int(node.header.cell_count)
		stats_row_count_set(tree_stats(t), page_id, result)
		return
	}

	total := 0
	nid := Page_Id(page_id)
	cell_count := get_cell_count(node.data, page_id)
	for i in 0 ..< cell_count {
		child_id, c_err := node.layout.vtable.child_at(node.data, nid, i)
		if c_err != .None { err = .Invalid_Cell_Pointer; return }
		total += count_recursive(t, child_id) or_return
	}

	rightmost, r_err := node.layout.vtable.child_at(node.data, nid, cell_count)
	if r_err != .None { err = .Invalid_Cell_Pointer; return }

	total += count_recursive(t, rightmost) or_return
	stats_row_count_set(tree_stats(t), page_id, total)
	result = total
	return
}

@(private)
update_row_count :: proc(t: ^Tree, page_id: u32, delta: int) {
	s := tree_stats(t)
	if count, ok := stats_row_count_get(s, page_id); ok {
		stats_row_count_set(s, page_id, count + delta)
	}
}

@(private = "file", require_results)
delete_recursive :: proc(t: ^Tree, page_id: u32, key: types.Row_ID) -> (bool, Error) {
	node, err := load_node(t, page_id)
	if err != .None { return false, err }
	defer unpin_node(t, node)

	if is_leaf(node) {
		e := delete_from_leaf(t, &node, key)
		if e != .None { return false, e }
		stats_row_count_set(tree_stats(t), page_id, int(node.header.cell_count))
		return true, .None
	}

	child_id, _ := node_find_child(&node, key)
	deleted, d_err := delete_recursive(t, child_id, key)
	if d_err != .None { return false, d_err }
	if deleted {
		update_row_count(t, page_id, -1)
	}
	return deleted, .None
}

@(private, require_results)
delete_from_leaf :: proc(t: ^Tree, leaf_node: ^Node, key: types.Row_ID) -> Error {
	if !is_leaf(leaf_node^) { return .Invalid_Page_Header }
	if !ensure_row_major(leaf_node.data, leaf_node.id) { return .Cell_Deserialize_Failed }

	rl, _, r_err := layout_for_page(leaf_node.data, Page_Id(leaf_node.id))
	if r_err != .None { return r_err }

	leaf_node.layout = rl
	limit := int(leaf_node.header.cell_count)
	delete_idx, cell_off, cell_sz := -1, 0, 0
	idx, ok := leaf_lower_bound(leaf_node.data, leaf_node.id, key, leaf_node.layout)
	if ok && idx < limit {
		ptr, p_err := leaf_node.layout.vtable.cell_ptr_at(
			leaf_node.data,
			Page_Id(leaf_node.id),
			idx,
		)
		if p_err != .None { return .Invalid_Cell_Pointer }

		rid, ok2 := cell.get_rowid(leaf_node.data, int(ptr))
		if ok2 && rid == key {
			delete_idx = idx
			cell_off = int(ptr)
			sz, ok3 := cell.get_size(leaf_node.data, cell_off)
			if ok3 { cell_sz = sz }
		}
	}
	if delete_idx == -1 {
		return .Cell_Not_Found
	}
	if delete_idx < limit - 1 {
		if d_err := leaf_node.layout.vtable.slot_delete(
			leaf_node.data,
			Page_Id(leaf_node.id),
			delete_idx,
		); d_err != .None {
			return d_err
		}
	}

	leaf_node.header.cell_count -= 1
	if cell_off == int(leaf_node.header.cell_content_offset) {
		leaf_node.header.cell_content_offset += u16le(cell_sz)
	} else if cell_sz >= FREEBLOCK_HDR_SIZE {
		freeblock_insert(
			leaf_node.data,
			u16(cell_off),
			u16(cell_sz),
			&leaf_node.header.first_freeblock,
		)
	} else if cell_sz > 0 && cell_sz < 255 {
		leaf_node.header.fragmented_bytes = u8(
			min(u16(leaf_node.header.fragmented_bytes) + u16(cell_sz), 255),
		)
	}

	pager.mark_dirty(t.pager, leaf_node.id)
	invalidate_page_int_range(t, leaf_node.id)
	return .None
}

// Delete a row by Row_ID. Removes the cell and adds the freed space to the freeblock list.
@(require_results)
tree_delete :: proc(t: ^Tree, key: types.Row_ID) -> Error {
	_, err := delete_recursive(t, t.root, key)
	return err
}

@(require_results)
tree_update :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	dk := Descend_Key_Ctx {
		key = rowid,
	}

	leaf_node := descend_to_leaf(t, descend_by_key, &dk) or_return
	defer unpin_node(t, leaf_node)
	if d_err := delete_from_leaf(t, &leaf_node, rowid); d_err != .None { return d_err }
	if i_err := node_insert_leaf_cell(t, &leaf_node, rowid, values); i_err != .None {
		return i_err
	}

	stats_row_count_set(tree_stats(t), leaf_node.id, int(leaf_node.header.cell_count))
	return .None
}

@(require_results)
tree_foreach :: proc(
	t: ^Tree,
	callback: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	user_data: rawptr = nil,
) -> Error {
	return foreach_recursive(t, t.root, callback, user_data)
}

@(private = "file", require_results)
foreach_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	cb: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	ud: rawptr,
) -> Error {
	node := load_node(t, page_id) or_return
	defer unpin_node(t, node)

	nid := Page_Id(page_id)
	if is_leaf(node) {
		cell_count := get_cell_count(node.data, page_id)
		for i in 0 ..< cell_count {
			ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, i)
			if p_err != .None { return .Cell_Deserialize_Failed }

			c, _, ok := cell.deserialize(
				node.data,
				int(ptr),
				cell.Config{allocator = t.config.allocator, zero_copy = t.config.zero_copy},
			)
			if !ok { return .Cell_Deserialize_Failed }

			continue_iter := cb(&c, ud)
			cell.destroy(&c, t.config.allocator)
			if !continue_iter { return .None }
		}
		return .None
	}

	cell_count := get_cell_count(node.data, page_id)
	for i in 0 ..< cell_count {
		child, c_err := node.layout.vtable.child_at(node.data, nid, i)
		if c_err != .None { return .Invalid_Cell_Pointer }
		if e := foreach_recursive(t, child, cb, ud); e != .None { return e }
	}

	rightmost, r_err := node.layout.vtable.child_at(node.data, nid, cell_count)
	if r_err != .None { return .Invalid_Cell_Pointer }
	return foreach_recursive(t, rightmost, cb, ud)
}

@(cold)
tree_debug_print_node :: proc(t: ^Tree, page_id: u32) {
	node, err := load_node(t, page_id)
	if err != .None { log.debugf("Error reading page %d", page_id); return }

	log.debugf(
		"Page %d (type=%v, cells=%d, off=%d, frag=%d)",
		page_id,
		node.header.page_type,
		node.header.cell_count,
		node.header.cell_content_offset,
		node.header.fragmented_bytes,
	)

	nid := Page_Id(page_id)
	cell_count := get_cell_count(node.data, page_id)
	for i in 0 ..< cell_count {
		ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, i)
		if p_err != .None {
			log.debugf("  Cell %d: [Bad Slot]", i)
			continue
		}

		c, _, ok := cell.deserialize(
			node.data,
			int(ptr),
			cell.Config{allocator = t.config.allocator, zero_copy = false},
		)
		if !ok {
			log.debugf("  Cell %d: [Error Deserializing]", i)
			continue
		}
		log.debugf("  Cell %d: ", i); cell.debug_print(c); cell.destroy(&c)
	}
}

@(cold)
tree_verify :: proc(t: ^Tree) -> bool {
	visited := make(map[u32]bool, context.temp_allocator)
	defer delete(visited)
	return verify_recursive(t, t.root, 0, types.Row_ID(max(i64)), 0, &visited)
}

// verify_config_enabled can be set via -define:VERIFY_TREE=true at build time.
// tree_verify allocates a map and walks the full tree — do not call on hot paths.
VERIFY_TREE            :: #config(VERIFY_TREE, false)
tree_verify_if_enabled :: proc(t: ^Tree) -> bool {
	if !VERIFY_TREE { return true }
	return tree_verify(t)
}

@(private = "file", cold)
verify_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	min_k: types.Row_ID,
	max_k: types.Row_ID,
	depth: int,
	visited: ^map[u32]bool,
) -> bool {
	if page_id == 0 { return false }
	if page_id in visited {
		log.debugf("Cycle detected: page %d revisited", page_id)
		return false
	}
	if depth > MAX_TREE_DEPTH {
		log.debugf("Tree too deep (depth=%d), possible cycle", depth)
		return false
	}

	visited[page_id] = true
	node, err := load_node(t, page_id)
	if err != .None {
		log.debugf("Failed to load page %d", page_id)
		return false
	}
	defer unpin_node(t, node)

	indent := strings.repeat("  ", depth, context.temp_allocator)
	log.debugf(
		"%sPage %d [%v] count=%d",
		indent,
		page_id,
		node.header.page_type,
		node.header.cell_count,
	)

	nid := Page_Id(page_id)
	cell_count := get_cell_count(node.data, page_id)
	if is_leaf(node) {
		prev := min_k
		for i in 0 ..< cell_count {
			rowid, r_err := node.layout.vtable.key_at(node.data, nid, i)
			if r_err != .None {
				log.debugf("Unreadable leaf key at slot %d", i)
				return false
			}
			if rowid < prev {
				log.debugf("Leaf key disorder: %d came after %d", rowid, prev)
				return false
			}
			if rowid > max_k {
				log.debugf("Leaf key %d > max %d", rowid, max_k)
				return false
			}
			prev = rowid
		}
		return true
	}

	prev_k := min_k
	for i in 0 ..< cell_count {
		child, c_err := node.layout.vtable.child_at(node.data, nid, i)
		if c_err != .None {
			log.debugf("Corrupt interior slot %d", i)
			return false
		}

		key, k_err := node.layout.vtable.key_at(node.data, nid, i)
		if k_err != .None {
			log.debugf("Unreadable interior key at slot %d", i)
			return false
		}
		if key < prev_k || key > max_k {
			log.debugf("Interior key %d out of bounds [%d, %d]", key, prev_k, max_k)
			return false
		}
		if !verify_recursive(t, child, prev_k, key, depth + 1, visited) { return false }
		prev_k = key
	}

	rightmost, r_err := node.layout.vtable.child_at(node.data, nid, cell_count)
	if r_err != .None {
		log.debugf("Unreadable rightmost child on page %d", page_id)
		return false
	}
	return verify_recursive(t, rightmost, prev_k, max_k, depth + 1, visited)
}

collect_pages :: proc(t: ^Tree, root: u32, pages: ^map[u32]bool) {
	if root == 0 || root in pages { return }

	pages[root] = true
	node, err := load_node(t, root)
	if err != .None { return }
	defer unpin_node(t, node)
	if is_leaf(node) { return }

	nid := Page_Id(node.id)
	cell_count := get_cell_count(node.data, node.id)
	for i in 0 ..= cell_count {
		child, c_err := node.layout.vtable.child_at(node.data, nid, i)
		if c_err != .None { continue }
		collect_pages(t, child, pages)
	}
}
