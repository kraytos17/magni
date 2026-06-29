package btree

import "src:cell"
import "src:types"

@(private)
v1_get_key :: proc(data: []u8, page_id: u32, i: int) -> types.Row_ID {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	ptr := u16((^u16le)(raw_data(data[start + i * CELL_POINTER_STRIDE:]))^)
	if hdr.page_type == .LEAF_TABLE || hdr.page_type == .LEAF_TABLE_COLUMNAR {
		rid, _ := cell.get_rowid(data, int(ptr))
		return rid
	}

	sep, _, _ := cell.varint_decode(data, int(ptr) + 4)
	return types.Row_ID(sep)
}

@(private)
v1_set_entry :: proc(data: []u8, page_id: u32, i: int, ptr: u16, key: types.Row_ID) {
	off := get_page_header_offset(page_id)
	hdr := get_header(data, page_id)
	hdr_sz := page_header_size(hdr.page_type)
	start := off + hdr_sz
	cell_ptr := (^Cell_Pointer)(raw_data(data[start + i * CELL_POINTER_STRIDE:]))
	cell_ptr^ = Cell_Pointer(ptr)
}

cell_layout_v1 := Cell_Layout {
	version       = 1,
	stride        = CELL_POINTER_STRIDE,
	get_key       = v1_get_key,
	set_entry     = v1_set_entry,
	move_leaf     = node_move_leaf_cells,
	move_interior = node_move_interior_cells,
}
