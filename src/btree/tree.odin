// Package btree implements a B+tree with COW (copy-on-write) for row storage.
package btree

import "core:encoding/endian"
import "core:fmt"
import "core:mem"
import "core:strings"
import "src:cell"
import "src:pager"
import "src:types"

MAX_TREE_DEPTH :: 64

DEFAULT_CONFIG := Config {
	allocator        = {},
	zero_copy        = false,
	check_duplicates = true,
}

Tree :: struct {
	pager:  ^pager.Pager,
	root:   u32,
	config: Config,
}

Config :: struct #all_or_none {
	using _:          types.Storage_Config,
	check_duplicates: bool,
}

Error :: enum {
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
}

Node :: struct {
	id:     u32,
	data:   []u8,
	header: ^Page_Header,
}

Insert_COW_Result :: struct #all_or_none {
	new_page:   u32,
	did_split:  bool,
	right_page: u32,
	split_key:  types.Row_ID,
}

init :: proc(p: ^pager.Pager, root_page: u32, config := DEFAULT_CONFIG) -> Tree {
	c := config
	if c.allocator.procedure == nil { c.allocator = context.allocator }
	return Tree{pager = p, root = root_page, config = c}
}

is_leaf :: proc(n: Node) -> bool {
	return n.header.page_type == .LEAF_TABLE || n.header.page_type == .LEAF_TABLE_COLUMNAR
}

node_leaf :: proc(n: Node) -> ^Leaf_Header { return get_leaf_header(n.data, n.id) }

node_interior :: proc(n: Node) -> ^Interior_Header {
	return get_interior_header(n.data, n.id)
}

unpin_node :: proc(t: ^Tree, n: Node) { pager.unpin_page(t.pager, n.id) }

load_node :: proc(t: ^Tree, page_id: u32) -> (Node, Error) {
	page, err := pager.get_page(t.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }
	return node_from_bytes(page_id, page.data)
}

node_from_bytes :: proc(id: u32, data: []u8) -> (Node, Error) {
	common_hdr := get_header(data, id)
	if common_hdr == nil { return {}, .Invalid_Page_Header }
	return Node{id = id, data = data, header = common_hdr}, .None
}

leaf_lower_bound :: proc(data: []u8, page_id: u32, target: types.Row_ID, version: u32 = 1) -> (int, bool) {
	cell_count := get_cell_count(data, page_id)
	left, right := 0, cell_count
	for left < right {
		mid := left + (right - left) / 2
		if get_cell_key(data, page_id, mid, version) < target { left = mid + 1 } else { right = mid }
	}
	return left, true
}

node_find_child :: proc(n: ^Node, key: types.Row_ID, version: u32 = 1) -> u32 {
	return node_find_child_data(n.data, n.id, key, version)
}

node_insert_leaf_cell :: proc(
	t: ^Tree,
	n: ^Node,
	rowid: types.Row_ID,
	values: []types.Value,
) -> Error {
	if !is_leaf(n^) { return .Invalid_Page_Header }

	ensure_row_major(n.data, n.id)
	v := t.pager.page_format_version
	idx, lb_ok := leaf_lower_bound(n.data, n.id, rowid, v)

	if v >= 2 {
		entries := get_entries(n.data, n.id)
		if t.config.check_duplicates {
			if lb_ok && idx < len(entries) {
				rid, _ := cell.get_rowid(n.data, int(entries[idx].ptr))
				if rid == rowid { return .Duplicate_Rowid }
			}
		}
	} else {
		pointers := get_pointers(n.data, n.id)
		if t.config.check_duplicates {
			if lb_ok && idx < len(pointers) {
				rid, _ := cell.get_rowid(n.data, int(pointers[idx]))
				if rid == rowid { return .Duplicate_Rowid }
			}
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

		if v >= 2 {
			raw_entries := get_raw_entries(n.data, n.id)
			if idx < int(n.header.cell_count) {
				copy(raw_entries[idx + 1:], raw_entries[idx:n.header.cell_count])
			}
			raw_entries[idx] = Cell_Entry{ptr = Cell_Pointer(free_off), key = rowid}
		} else {
			raw_ptrs := get_raw_pointers(n.data, n.id)
			if idx < int(n.header.cell_count) {
				copy(raw_ptrs[idx + 1:], raw_ptrs[idx:n.header.cell_count])
			}
			raw_ptrs[idx] = Cell_Pointer(free_off)
		}
		n.header.cell_count += 1
		pager.mark_dirty(t.pager, n.id)
		pager.invalidate_page_int_range(t.pager, n.id)
		return .None
	}

	base_offset := get_page_header_offset(n.id)
	header_size := page_header_size(n.header.page_type)
	entry_sz := size_of(Cell_Entry) if v >= 2 else size_of(Cell_Pointer)
	ptr_area_end :=
		base_offset + header_size + int(n.header.cell_count + 1) * entry_sz

	if ptr_area_end >= int(n.header.cell_content_offset) { return .Page_Full }
	if cinfo.total_size > int(n.header.cell_content_offset) - ptr_area_end { return .Page_Full }

	new_offset := int(n.header.cell_content_offset) - cinfo.total_size
	bytes_written, ok := cell.serialize(n.data[new_offset:], rowid, values, cinfo)
	if !ok || bytes_written != cinfo.total_size { return .Serialization_Failed }

	if v >= 2 {
		raw_entries := get_raw_entries(n.data, n.id)
		if idx < int(n.header.cell_count) {
			copy(raw_entries[idx + 1:], raw_entries[idx:n.header.cell_count])
		}
		raw_entries[idx] = Cell_Entry{ptr = Cell_Pointer(new_offset), key = rowid}
	} else {
		raw_ptrs := get_raw_pointers(n.data, n.id)
		if idx < int(n.header.cell_count) {
			copy(raw_ptrs[idx + 1:], raw_ptrs[idx:n.header.cell_count])
		}
		raw_ptrs[idx] = Cell_Pointer(new_offset)
	}
	n.header.cell_count += 1
	n.header.cell_content_offset = u16le(new_offset)
	pager.mark_dirty(t.pager, n.id)
	pager.invalidate_page_int_range(t.pager, n.id)
	return .None
}

node_update_child_ptr :: proc(n: ^Node, key: types.Row_ID, new_sibling: u32, version: u32 = 1) -> bool {
	cell_count := get_cell_count(n.data, n.id)
	stride := CELL_ENTRY_STRIDE if version >= 2 else CELL_POINTER_STRIDE
	idx, ok := interior_lower_bound(n.data, n.id, key, version)
	if ok && idx < cell_count {
		cell_offset := int(get_cell_ptr(n.data, n.id, idx, stride))
		endian.put_u32(n.data[cell_offset:], .Big, new_sibling)
		return true
	}
	if idx == cell_count {
		set_right_ptr(n.data, n.id, new_sibling)
		return true
	}
	return false
}

node_find_insert_index :: proc(n: ^Node, target_rowid: types.Row_ID, version: u32 = 1) -> int {
	idx, _ := leaf_lower_bound(n.data, n.id, target_rowid, version)
	return idx
}

insert_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	rowid: types.Row_ID,
	values: []types.Value,
	cow: bool,
) -> (
	Insert_COW_Result,
	Error,
) {
	new_page_num := page_id
	if cow {
		var, cow_err := copy_on_write(t, page_id)
		if cow_err != .None { return {}, cow_err }
		new_page_num = var
	}

	curr, err := load_node(t, new_page_num)
	if err != .None { return {}, err }
	defer unpin_node(t, curr)
	if is_leaf(curr) {
		e := node_insert_leaf_cell(t, &curr, rowid, values)
		if e == .Page_Full {
			original_count := int(curr.header.cell_count)
			split, s_err := split_leaf_node(t, &curr)
			if s_err != .None { return {}, s_err }

			mid := original_count / 2
			t.pager.row_counts[curr.id] = mid
			t.pager.row_counts[split.right_page] = original_count - mid
			target_id := curr.id
			if rowid >= split.split_key { target_id = split.right_page }

			target_node, t_err := load_node(t, target_id)
			if t_err != .None { return {}, t_err }

			defer unpin_node(t, target_node)
			retry_err := node_insert_leaf_cell(t, &target_node, rowid, values)
			if retry_err != .None { return {}, retry_err }

			t.pager.row_counts[target_id] = int(target_node.header.cell_count)
			return Insert_COW_Result {
					new_page = curr.id,
					did_split = true,
					right_page = split.right_page,
					split_key = split.split_key,
				},
				.None
		}
		if e == .None {
			t.pager.row_counts[curr.id] = int(curr.header.cell_count)
		}
		return Insert_COW_Result {
				new_page = new_page_num,
				did_split = false,
				right_page = 0,
				split_key = 0,
			},
			e
	}

	child_id := node_find_child(&curr, rowid)
	child_result, c_err := insert_recursive(t, child_id, rowid, values, cow)
	if c_err != .None { return {}, c_err }
	if cow && child_result.new_page != child_id {
		node_update_child_ptr(&curr, rowid, child_result.new_page, t.pager.page_format_version)
	}
	if !child_result.did_split {
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

	is_rightmost := child_id == get_right_ptr(curr.data, curr.id)
	ptr_for_insert := child_result.right_page
	insert_key := child_result.split_key
	if !is_rightmost {
		idx := find_interior_cell_for_child(curr.data, curr.id, child_id, t.pager.page_format_version)
		if idx == -1 { return {}, .Invalid_Page_Header }

		v := t.pager.page_format_version
		stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
		cell_offset := int(get_cell_ptr(curr.data, curr.id, idx, stride))
		old_sep_u64, _, ok := cell.varint_decode(curr.data, cell_offset + 4)
		if !ok { return {}, .Invalid_Cell_Pointer }

		insert_key = types.Row_ID(old_sep_u64)
		ptr_for_insert = child_result.right_page
		cell.varint_encode(curr.data[cell_offset + 4:], u64(child_result.split_key))
	} else {
		ptr_for_insert = child_id
	}

	ok := insert_interior_cell(curr.data, curr.id, ptr_for_insert, insert_key, t.pager.page_format_version)
	if ok {
		if is_rightmost { set_right_ptr(curr.data, curr.id, child_result.right_page) }

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

	interior_split, split_err := split_interior_node(t, &curr)
	if split_err != .None { return {}, split_err }

	count_recursive(t, curr.id)
	count_recursive(t, interior_split.right_page)
	target_id := curr.id
	if insert_key > interior_split.split_key { target_id = interior_split.right_page }

	target_node, t_err := load_node(t, target_id)
	if t_err != .None { return {}, t_err }

	defer unpin_node(t, target_node)
	if is_rightmost { set_right_ptr(target_node.data, target_id, child_result.right_page) }

	insert_interior_cell(target_node.data, target_id, ptr_for_insert, insert_key, t.pager.page_format_version)
	pager.mark_dirty(t.pager, target_id)
	count_recursive(t, target_id)
	return Insert_COW_Result {
			new_page = new_page_num,
			did_split = true,
			right_page = interior_split.right_page,
			split_key = interior_split.split_key,
		},
		.None
}

rowid_exists :: proc(data: []u8, page_id: u32, target_rowid: types.Row_ID) -> bool {
	pointers := get_pointers(data, page_id)
	idx, ok := leaf_lower_bound(data, page_id, target_rowid)
	if !ok || idx >= len(pointers) { return false }

	rowid, ok2 := cell.get_rowid(data, int(pointers[idx]))
	return ok2 && rowid == target_rowid
}

// Insert a row into the b-tree. Handles root splits transparently.
// Returns .Duplicate_Rowid if check_duplicates is enabled and the rowid exists.
tree_insert :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	root_node, err := load_node(t, t.root)
	if err != .None { return err }
	defer unpin_node(t, root_node)
	if is_leaf(root_node) {
		e := node_insert_leaf_cell(t, &root_node, rowid, values)
		if e != .Page_Full {
			if e == .None {
				t.pager.row_counts[t.root] = int(root_node.header.cell_count)
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
		count_recursive(t, t.root)
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
		count_recursive(t, t.root)
	}
	return .None
}

descend_to_leaf :: proc(
	t: ^Tree,
	get_child: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32,
	ctx: rawptr,
	root_override: u32 = 0,
) -> (
	Node,
	Error,
) {
	curr := t.root if root_override == 0 else root_override
	for {
		n, err := load_node(t, curr)
		if err != .None { return {}, err }
		if is_leaf(n) { return n, .None }
		curr = get_child(n.data, n.id, ctx); unpin_node(t, n)
	}
}

node_find_child_data :: proc(data: []u8, page_id: u32, key: types.Row_ID, version: u32 = 1) -> u32 {
	cell_count := get_cell_count(data, page_id)
	if cell_count == 0 { return get_right_ptr(data, page_id) }
	idx, ok := interior_lower_bound(data, page_id, key, version)
	if !ok || idx >= cell_count { return get_right_ptr(data, page_id) }
	stride := CELL_ENTRY_STRIDE if version >= 2 else CELL_POINTER_STRIDE
	ptr := get_cell_ptr(data, page_id, idx, stride)
	child, _ := endian.get_u32(data[int(ptr):], .Big); return child
}

descend_by_rightmost :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	return get_right_ptr(data, page_id)
}

Descend_Key_Ctx :: struct {
	key:     types.Row_ID,
	version: u32,
}

descend_by_key :: proc(data: []u8, page_id: u32, ctx: rawptr) -> u32 {
	dk := (^Descend_Key_Ctx)(ctx)
	return node_find_child_data(data, page_id, dk.key, dk.version)
}

// Find a row by Row_ID. Returns a Cell (with deep-copied or zero-copy values per Config).
// Returns .Cell_Not_Found if the row doesn't exist.
tree_find :: proc(t: ^Tree, key: types.Row_ID, allocator: mem.Allocator) -> (cell.Cell, Error) {
	dk := Descend_Key_Ctx{key = key, version = t.pager.page_format_version}
	leaf, err := descend_to_leaf(t, descend_by_key, &dk)
	if err != .None { return {}, err }
	defer unpin_node(t, leaf)

	// Columnar page: linear scan rowids
	if is_columnar(leaf.data, leaf.id) {
		num_cols, found := detect_columnar_col_count(leaf.data, leaf.id)
		if !found { return {}, .Invalid_Cell_Pointer }

		boff := get_page_header_offset(leaf.id)
		row_count := int(leaf.header.cell_count)
		for i in 0 ..< row_count {
			rid, _ := cell.read_columnar_rowid(leaf.data, num_cols, i, boff)
			if rid == key {
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

	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	idx, ok := leaf_lower_bound(leaf.data, leaf.id, key, v)
	if !ok { return {}, .Invalid_Cell_Pointer }
	cell_count := get_cell_count(leaf.data, leaf.id)
	if idx < cell_count {
		ptr := get_cell_ptr(leaf.data, leaf.id, idx, stride)
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

tree_next_rowid :: proc(t: ^Tree) -> (types.Row_ID, Error) {
	leaf, err := descend_to_leaf(t, descend_by_rightmost, nil)
	if err != .None { return 0, err }
	defer unpin_node(t, leaf)
	if leaf.header.cell_count == 0 { return 1, .None }

	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	last_ptr := get_cell_ptr(leaf.data, leaf.id, int(leaf.header.cell_count) - 1, stride)
	last_id, ok := cell.get_rowid(leaf.data, int(last_ptr))
	if !ok { return 0, .Invalid_Cell_Pointer }
	return last_id + 1, .None
}

tree_count_rows :: proc(t: ^Tree) -> (int, Error) {
	if count, ok := t.pager.row_counts[t.root]; ok {
		return count, .None
	}
	return count_recursive(t, t.root)
}

count_recursive :: proc(t: ^Tree, page_id: u32) -> (int, Error) {
	if count, ok := t.pager.row_counts[page_id]; ok {
		return count, .None
	}

	node, err := load_node(t, page_id)
	if err != .None { return 0, err }
	defer unpin_node(t, node)
	if is_leaf(node) {
		c := int(node.header.cell_count)
		t.pager.row_counts[page_id] = c
		return c, .None
	}

	total := 0
	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	cell_count := get_cell_count(node.data, page_id)
	for i in 0 ..< cell_count {
		ptr := get_cell_ptr(node.data, page_id, i, stride)
		child_id, ok := endian.get_u32(node.data[int(ptr):], .Big)
		if !ok { return 0, .Invalid_Cell_Pointer }
		count, c_err := count_recursive(t, child_id)
		if c_err != .None { return 0, c_err }
		total += count
	}

	right_count, r_err := count_recursive(t, get_right_ptr(node.data, page_id))
	if r_err != .None { return 0, r_err }

	total += right_count
	t.pager.row_counts[page_id] = total
	return total, .None
}

update_row_count :: proc(t: ^Tree, page_id: u32, delta: int) {
	if _, ok := t.pager.row_counts[page_id]; ok {
		t.pager.row_counts[page_id] += delta
	}
}

delete_recursive :: proc(t: ^Tree, page_id: u32, key: types.Row_ID) -> (bool, Error) {
	node, err := load_node(t, page_id)
	if err != .None { return false, err }
	defer unpin_node(t, node)

	if is_leaf(node) {
		e := delete_from_leaf(t, &node, key)
		if e != .None { return false, e }
		t.pager.row_counts[page_id] = int(node.header.cell_count)
		return true, .None
	}

	child_id := node_find_child(&node, key, t.pager.page_format_version)
	deleted, d_err := delete_recursive(t, child_id, key)
	if d_err != .None { return false, d_err }
	if deleted {
		update_row_count(t, page_id, -1)
	}
	return deleted, .None
}

delete_from_leaf :: proc(t: ^Tree, leaf_node: ^Node, key: types.Row_ID) -> Error {
	if !is_leaf(leaf_node^) { return .Invalid_Page_Header }

	ensure_row_major(leaf_node.data, leaf_node.id)
	v := t.pager.page_format_version
	limit := int(leaf_node.header.cell_count)
	delete_idx, cell_off, cell_sz := -1, 0, 0
	idx, ok := leaf_lower_bound(leaf_node.data, leaf_node.id, key, v)

	if v >= 2 {
		raw_entries := get_raw_entries(leaf_node.data, leaf_node.id)
		if ok && idx < limit {
			rid, ok2 := cell.get_rowid(leaf_node.data, int(raw_entries[idx].ptr))
			if ok2 && rid == key {
				delete_idx = idx
				cell_off = int(raw_entries[idx].ptr)
				sz, ok3 := cell.get_size(leaf_node.data, cell_off)
				if ok3 { cell_sz = sz }
			}
		}
		if delete_idx == -1 {
			return .Cell_Not_Found
		}
		if delete_idx < limit - 1 {
			copy(raw_entries[delete_idx:], raw_entries[delete_idx + 1:limit])
		}
	} else {
		pointers := get_raw_pointers(leaf_node.data, leaf_node.id)
		if ok && idx < limit {
			rid, ok2 := cell.get_rowid(leaf_node.data, int(pointers[idx]))
			if ok2 && rid == key {
				delete_idx = idx
				cell_off = int(pointers[idx])
				sz, ok3 := cell.get_size(leaf_node.data, cell_off)
				if ok3 { cell_sz = sz }
			}
		}
		if delete_idx == -1 {
			return .Cell_Not_Found
		}
		if delete_idx < limit - 1 {
			copy(pointers[delete_idx:], pointers[delete_idx + 1:limit])
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
	pager.invalidate_page_int_range(t.pager, leaf_node.id)
	return .None
}

// Delete a row by Row_ID. Removes the cell and adds the freed space to the freeblock list.
tree_delete :: proc(t: ^Tree, key: types.Row_ID) -> Error {
	_, err := delete_recursive(t, t.root, key)
	return err
}

tree_update :: proc(t: ^Tree, rowid: types.Row_ID, values: []types.Value) -> Error {
	dk := Descend_Key_Ctx{key = rowid, version = t.pager.page_format_version}
	leaf_node, err := descend_to_leaf(t, descend_by_key, &dk)
	if err != .None { return err }
	defer unpin_node(t, leaf_node)
	if d_err := delete_from_leaf(t, &leaf_node, rowid); d_err != .None { return d_err }
	if i_err := node_insert_leaf_cell(t, &leaf_node, rowid, values); i_err != .None {
		return i_err
	}
	t.pager.row_counts[leaf_node.id] = int(leaf_node.header.cell_count)
	return .None
}

tree_foreach :: proc(
	t: ^Tree,
	callback: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	user_data: rawptr = nil,
) -> Error {
	return foreach_recursive(t, t.root, callback, user_data)
}

foreach_recursive :: proc(
	t: ^Tree,
	page_id: u32,
	cb: proc(c: ^cell.Cell, user_data: rawptr) -> bool,
	ud: rawptr,
) -> Error {
	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	node, err := load_node(t, page_id)
	if err != .None { return err }
	defer unpin_node(t, node)
	if is_leaf(node) {
		cell_count := get_cell_count(node.data, page_id)
		for i in 0 ..< cell_count {
			ptr := get_cell_ptr(node.data, page_id, i, stride)
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
		ptr := get_cell_ptr(node.data, page_id, i, stride)
		child, ok := endian.get_u32(node.data[int(ptr):], .Big)
		if !ok { return .Invalid_Cell_Pointer }
		if e := foreach_recursive(t, child, cb, ud); e != .None { return e }
	}
	return foreach_recursive(t, get_right_ptr(node.data, page_id), cb, ud)
}

tree_debug_print_node :: proc(t: ^Tree, page_id: u32) {
	node, err := load_node(t, page_id)
	if err != .None { fmt.printf("Error reading page %d\n", page_id); return }
	fmt.printf(
		"Page %d (type=%v, cells=%d, off=%d, frag=%d)\n",
		page_id,
		node.header.page_type,
		node.header.cell_count,
		node.header.cell_content_offset,
		node.header.fragmented_bytes,
	)

	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	cell_count := get_cell_count(node.data, page_id)
	for i in 0 ..< cell_count {
		ptr := get_cell_ptr(node.data, page_id, i, stride)
		c, _, ok := cell.deserialize(
			node.data,
			int(ptr),
			cell.Config{allocator = t.config.allocator, zero_copy = false},
		)
		if !ok {
			fmt.printf("  Cell %d: [Error Deserializing]\n", i)
			continue
		}
		fmt.printf("  Cell %d: ", i); cell.debug_print(c); cell.destroy(&c)
	}
}

tree_verify :: proc(t: ^Tree) -> bool {
	visited := make(map[u32]bool, context.temp_allocator)
	defer delete(visited)
	return verify_recursive(t, t.root, 0, types.Row_ID(max(i64)), 0, &visited)
}

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
		fmt.printf("Cycle detected: page %d revisited\n", page_id)
		return false
	}
	if depth > MAX_TREE_DEPTH {
		fmt.printf("Tree too deep (depth=%d), possible cycle\n", depth)
		return false
	}

	visited[page_id] = true
	node, err := load_node(t, page_id)
	if err != .None {
		fmt.printf("Failed to load page %d\n", page_id)
		return false
	}
	defer unpin_node(t, node)

	indent := strings.repeat("  ", depth, context.temp_allocator)
	fmt.printf(
		"%sPage %d [%v] count=%d\n",
		indent,
		page_id,
		node.header.page_type,
		node.header.cell_count,
	)
	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	cell_count := get_cell_count(node.data, page_id)
	if is_leaf(node) {
		prev := min_k
		for i in 0 ..< cell_count {
			rowid := get_cell_key(node.data, page_id, i, v)
			if rowid < prev {
				fmt.printf("Leaf key disorder: %d came after %d\n", rowid, prev)
				return false
			}
			if rowid > max_k {
				fmt.printf("Leaf key %d > max %d\n", rowid, max_k)
				return false
			}
			prev = rowid
		}
		return true
	}

	prev_k := min_k
	for i in 0 ..< cell_count {
		ptr := get_cell_ptr(node.data, page_id, i, stride)
		child, r_ok := endian.get_u32(node.data[int(ptr):], .Big)
		if !r_ok {
			fmt.printf("Corrupt interior cell at offset %d\n", int(ptr))
			return false
		}

		key := get_cell_key(node.data, page_id, i, v)
		if key < prev_k || key > max_k {
			fmt.printf("Interior key %d out of bounds [%d, %d]\n", key, prev_k, max_k)
			return false
		}
		if !verify_recursive(t, child, prev_k, key, depth + 1, visited) { return false }
		prev_k = key
	}
	return verify_recursive(
		t,
		get_right_ptr(node.data, page_id),
		prev_k,
		max_k,
		depth + 1,
		visited,
	)
}

collect_pages :: proc(t: ^Tree, root: u32, pages: ^map[u32]bool) {
	if root == 0 || root in pages { return }
	pages[root] = true
	node, err := load_node(t, root)
	if err != .None { return }
	defer unpin_node(t, node)
	if is_leaf(node) { return }

	v := t.pager.page_format_version
	stride := CELL_ENTRY_STRIDE if v >= 2 else CELL_POINTER_STRIDE
	cell_count := get_cell_count(node.data, node.id)
	for i in 0 ..< cell_count {
		ptr := get_cell_ptr(node.data, node.id, i, stride)
		child, _ := endian.get_u32(node.data[int(ptr):], .Big)
		collect_pages(t, child, pages)
	}
	if right_ptr := get_right_ptr(node.data, node.id); right_ptr != 0 {
		collect_pages(t, right_ptr, pages)
	}
}
