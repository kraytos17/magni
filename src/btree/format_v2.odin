package btree

import "src:types"

@(private)
v2_get_key :: proc(data: []u8, page_id: u32, i: int) -> types.Row_ID {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	entry := (^Cell_Entry)(raw_data(data[start + i * CELL_ENTRY_STRIDE:]))
	return entry.key
}

@(private)
v2_set_entry :: proc(data: []u8, page_id: u32, i: int, ptr: u16, key: types.Row_ID) {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	entry := (^Cell_Entry)(raw_data(data[start + i * CELL_ENTRY_STRIDE:]))
	entry^ = Cell_Entry {
		ptr = Cell_Pointer(ptr),
		key = key,
	}
}

cell_layout_v2 := Cell_Layout {
	version       = 2,
	stride        = CELL_ENTRY_STRIDE,
	get_key       = v2_get_key,
	set_entry     = v2_set_entry,
	move_leaf     = node_move_leaf_cells_v2,
	move_interior = node_move_interior_cells_v2,
}
