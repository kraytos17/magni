package btree

import "src:types"

MAX_FORMAT_VERSION :: 64

Cell_Layout :: struct {
	version:       u32,
	stride:        int,
	get_key:       proc(data: []u8, page_id: u32, i: int) -> types.Row_ID,
	set_entry:     proc(data: []u8, page_id: u32, i: int, ptr: u16, key: types.Row_ID),
	move_leaf:     proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool,
	move_interior: proc(src: ^Node, dst: ^Node, start_idx: int, count: int) -> bool,
}

format_registry: [MAX_FORMAT_VERSION]^Cell_Layout

@(init)
register_cell_layouts :: proc "contextless" () {
	format_registry[cell_layout_v1.version] = &cell_layout_v1
	format_registry[cell_layout_v2.version] = &cell_layout_v2
}

get_layout :: proc(version: u32) -> ^Cell_Layout {
	assert(version < MAX_FORMAT_VERSION, "page format version out of range")
	layout := format_registry[version]
	assert(layout != nil, "unregistered page format version")
	return layout
}
