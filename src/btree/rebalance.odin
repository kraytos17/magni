package btree

import "core:encoding/endian"
import "src:cell"
import "src:pager"
import "src:types"

MERGE_MAX_OCCUPANCY_PCT :: 70

Leaf_Info :: struct {
	page_id:    u32,
	cell_count: int,
	used_bytes: int,
	first_key:  types.Row_ID,
	last_key:   types.Row_ID,
}

collect_leaf_info :: proc(t: ^Tree, page_id: u32, infos: ^[dynamic]Leaf_Info) -> Error {
	node, err := load_node(t, page_id)
	if err != .None { return err }
	defer unpin_node(t, node)

	if is_leaf(node) {
		pointers := get_pointers(node.data, node.id)
		if len(pointers) == 0 {
			append(infos, Leaf_Info{page_id = page_id, cell_count = 0, used_bytes = 0})
			return .None
		}

		off := get_page_header_offset(node.id)
		hdr_sz := page_header_size(node.header.page_type)
		cell_data := types.PAGE_SIZE - int(node.header.cell_content_offset)
		overhead := off + hdr_sz + int(node.header.cell_count) * node.layout.stride
		used_bytes := cell_data + overhead

		fk, _ := cell.get_rowid(node.data, int(pointers[0]))
		lk, _ := cell.get_rowid(node.data, int(pointers[len(pointers) - 1]))
		append(
			infos,
			Leaf_Info {
				page_id = page_id,
				cell_count = int(node.header.cell_count),
				used_bytes = used_bytes,
				first_key = fk,
				last_key = lk,
			},
		)
		return .None
	}

	pointers := get_pointers(node.data, page_id)
	for ptr in pointers {
		child, _ := endian.get_u32(node.data[int(ptr):], .Big)
		if e := collect_leaf_info(t, child, infos); e != .None { return e }
	}
	return collect_leaf_info(t, get_right_ptr(node.data, page_id), infos)
}

rebalance :: proc(t: ^Tree) -> Error {
	infos := make([dynamic]Leaf_Info, context.temp_allocator)
	defer delete(infos)
	if err := collect_leaf_info(t, t.root, &infos); err != .None { return err }
	for i := 0; i < len(infos); i += 1 {
		if infos[i].cell_count == 0 { continue }
		if i + 1 >= len(infos) { break }

		combined := infos[i].used_bytes + infos[i + 1].used_bytes
		capacity := MERGE_MAX_OCCUPANCY_PCT * types.PAGE_SIZE / 100
		if combined <= capacity {
			if merge_leaf_pages(t, infos[i].page_id, infos[i + 1].page_id) {
				infos[i].cell_count = infos[i].cell_count + infos[i + 1].cell_count
				infos[i].used_bytes = combined
				infos[i].last_key = infos[i + 1].last_key
				ordered_remove(&infos, i + 1)
			}
		}
	}
	return .None
}

merge_leaf_pages :: proc(t: ^Tree, left_id: u32, right_id: u32) -> bool {
	left_node, l_err := load_node(t, left_id)
	if l_err != .None { return false }
	defer unpin_node(t, left_node)

	right_node, r_err := load_node(t, right_id)
	if r_err != .None { return false }
	defer unpin_node(t, right_node)

	if !left_node.layout.move_leaf(&right_node, &left_node, 0, int(right_node.header.cell_count)) {
		return false
	}

	pager.mark_dirty(t.pager, left_id)
	pager.free_page(t.pager, right_id)
	return true
}
