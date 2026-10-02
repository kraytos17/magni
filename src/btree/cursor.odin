package btree

import "base:intrinsics"
import "core:encoding/endian"
import "core:mem"
import "src:cell"
import "src:pager"
import "src:types"
import "src:util/varint"

Cursor_Stack_Item :: struct {
	page_id   : u32,
	cell_index: u16,
}

Cursor :: struct {
	tree             : ^Tree,
	path             : [MAX_TREE_DEPTH]Cursor_Stack_Item,
	depth            : u8,
	is_valid         : bool,
	cached_page_id   : u32,
	cached_page_data : []u8,
	cached_cell_count: u16,
	cached_is_leaf   : bool,
	// Resolved layout for the cached page (one interface hop per page
	// visit, not per cell). Set on every cache fill; the hit path reuses
	// it without re-resolving.
	cached_layout    : Page_Layout,
	// Incremental columnar decode state, nil unless positioned on a
	// columnar page — row-major scans don't pay for it. Statement-scoped
	// (temp arena); freed on page-leave/destroy, else reclaimed with it.
	col              : ^Columnar_Cursor_State,
}

// Columnar_Cursor_State caches the column count plus the incremental
// per-column decode state for columnar pages
Columnar_Cursor_State :: struct {
	num_cols : u8, // >0 when synced to a columnar page; caches the column count
	rowid    : u64, // accumulated rowid at the current cell_index
	rowid_pos: int, // byte position in the rowid region for the current row
	encodings: [types.MAX_COLS]u8, // cell.ENCODING_RAW / cell.ENCODING_DELTA
	offsets  : [types.MAX_COLS]u32, // byte offset of each column's data (relative to page data)
	val_pos  : [types.MAX_COLS]int, // byte position after the current DELTA value
	mins     : [types.MAX_COLS]i64, // per-column min (added to each delta to recover the value)
	running  : [types.MAX_COLS]i64, // current value at cell_index (DELTA columns = min + delta)
}

// cursor_col_state returns the columnar decode state, allocating it on first
// landing on a columnar page.
@(private = "file")
cursor_col_state :: #force_inline proc(c: ^Cursor) -> ^Columnar_Cursor_State {
	if c.col == nil {
		c.col = new(Columnar_Cursor_State, context.temp_allocator)
	}
	return c.col
}

// cursor_col_clear drops the columnar decode state when leaving a columnar
// page (or destroying the cursor). Row-major pages never hold state.
@(private = "file")
cursor_col_clear :: #force_inline proc(c: ^Cursor) {
	if c.col != nil {
		free(c.col, context.temp_allocator)
		c.col = nil
	}
}

@(private = "file")
drill_down_leftmost :: proc(c: ^Cursor, start_page: u32) -> Error {
	curr := start_page
	for {
		if int(c.depth) >= MAX_TREE_DEPTH { return .Invalid_Page_Header }
		c.path[c.depth] = Cursor_Stack_Item {
			page_id    = curr,
			cell_index = 0,
		}

		c.depth += 1
		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)

		if is_leaf(node) { break }
		if node.header.cell_count > 0 {
			ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, Page_Id(curr), 0)
			if p_err != .None { return .Invalid_Cell_Pointer }

			child, ok := endian.get_u32(node.data[int(ptr):], .Big)
			if !ok { return .Invalid_Cell_Pointer }
			curr = child
		} else {
			curr = get_right_ptr(node.data, curr)
		}
	}
	return .None
}

cursor_destroy :: proc(c: ^Cursor) {
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}
	cursor_col_clear(c)
}

// Initialize a cursor for in-order traversal starting at the leftmost leaf.
// Returns an invalid cursor if the tree is empty. Empty leaves (possible after
// COW deletes empty a leading leaf) are skipped so the cursor always lands on
// the first cell-bearing leaf.
cursor_start :: proc(t: ^Tree, allocator := context.allocator) -> (c: Cursor, err: Error) {
	c = Cursor {
		tree     = t,
		is_valid = true,
	}

	drill_down_leftmost(&c, t.root) or_return
	if c.depth > 0 {
		for c.is_valid {
			top := c.path[c.depth - 1]
			node, e := load_node(t, top.page_id)
			if e != .None {
				c.is_valid = false
				break
			}

			non_empty := is_leaf(node) && node.header.cell_count > 0
			pager.unpin_page(t.pager, node.id)
			if non_empty { break }
			if a_err := cursor_advance(&c); a_err != .None {
				c.is_valid = false
				break
			}
		}
	} else {
		c.is_valid = false
	}
	return
}

// cursor_start_at_page positions a cursor at the first cell of the leaf page
// `page_id`, building the full root→leaf path so traversal can continue past
// the leaf. Used to start a scan at a skip-index lower bound.
@(private = "file")
cursor_start_at_page :: proc(
	t: ^Tree,
	page_id: u32,
	allocator := context.allocator,
) -> (
	c: Cursor,
	err: Error,
) {
	c.tree = t
	cursor_seek_to_page(&c, page_id) or_return
	return c, .None
}

// cursor_seek_to_page descends from the root to the leaf `page_id`, pushing
// the ancestor chain onto the path stack (interior cell indices included) so
// cursor_advance can leave the leaf correctly. Returns .Page_Not_Found when
// page_id is not reachable as a leaf (e.g. a stale skip-index page); callers
// should fall back to a full scan in that case.
// require_results: an unhandled seek error leaves the cursor invalid while
// the caller scans from nowhere — always check.
@(require_results)
cursor_seek_to_page :: proc(c: ^Cursor, page_id: u32) -> Error {
	c.depth = 0
	c.is_valid = false
	c.cached_page_id = 0
	c.cached_page_data = nil
	c.cached_cell_count = 0
	c.cached_is_leaf = false
	c.cached_layout = {}
	cursor_col_clear(c)

	curr := c.tree.root
	for {
		if int(c.depth) >= MAX_TREE_DEPTH { return .Invalid_Page_Header }

		node := load_node(c.tree, curr) or_return
		defer pager.unpin_page(c.tree.pager, node.id)
		if is_leaf(node) {
			if curr != page_id { return .Cell_Not_Found }

			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = 0,
			}

			c.depth += 1
			c.is_valid = true
			return .None
		}

		cell_count := get_cell_count(node.data, curr)
		idx := find_interior_cell_for_child(node.data, curr, page_id, node.layout)
		if idx >= 0 {
			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = u16(idx),
			}

			c.depth += 1
			ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, Page_Id(curr), idx)
			if p_err != .None { return .Invalid_Cell_Pointer }

			child, ok := endian.get_u32(node.data[int(ptr):], .Big)
			if !ok { return .Invalid_Cell_Pointer }
			curr = child
		} else if get_right_ptr(node.data, curr) == page_id {
			c.path[c.depth] = Cursor_Stack_Item {
				page_id    = curr,
				cell_index = u16(cell_count),
			}

			c.depth += 1
			curr = page_id
		} else {
			return .Cell_Not_Found
		}
	}
}

// Loads a node, caching the page in the cursor to avoid repeated loads.
// The page stays pinned until the cursor moves to a different page or is destroyed.
// likely(hit): sequential scans hit the pinned page once per cell, so the
// cached path dominates by orders of magnitude.
// require_results: using a zero Node after a failed load reads garbage.
@(private = "file", require_results)
load_cached_page :: proc(c: ^Cursor, page_id: u32) -> (Node, Error) {
	if intrinsics.likely(page_id == c.cached_page_id) {
		// Hit: reuse the cached bytes AND the resolved layout (no
		// re-resolve per cell). cached_layout is set on every fill below
		// and cleared on seek; the hit path always follows a fill.
		return node_from_bytes(page_id, c.cached_page_data, c.cached_layout)
	}
	if c.cached_page_id != 0 {
		pager.unpin_page(c.tree.pager, c.cached_page_id)
	}

	page, err := pager.get_page(c.tree.pager, page_id)
	if err != nil { return {}, .Page_Read_Failed }

	c.cached_page_id = page_id
	c.cached_page_data = page.data
	cursor_col_clear(c)

	layout, _, l_err := layout_for_page(page.data, Page_Id(page_id))
	if l_err != .None { return {}, l_err }

	n, n_err := node_from_bytes(page_id, page.data, layout)
	if n_err != .None { return {}, n_err }

	c.cached_layout = layout
	c.cached_cell_count = u16(n.header.cell_count)
	c.cached_is_leaf = is_leaf(n)
	return n, .None
}

// Move the cursor to the next cell in in-order. Sets is_valid=false at end of tree.
cursor_advance :: proc(c: ^Cursor) -> Error {
	if !c.is_valid || c.depth == 0 {
		return .None
	}

	// Leaf fast path: use cached cell count to avoid load_cached_page
	item := &c.path[c.depth - 1]
	if c.cached_is_leaf {
		item.cell_index += 1
		if int(item.cell_index) < int(c.cached_cell_count) {
			columnar_advance_state(c)
			return .None
		}

		cursor_col_clear(c)
		c.depth -= 1
		if c.depth == 0 {
			c.is_valid = false
			return .None
		}
	}
	return descend_to_next_leaf(c)
}

// columnar_advance_state advances the incremental columnar decode state by one
// row: the rowid stream plus each DELTA value column. RAW columns are indexed
// by cell_index, so they carry no state.
@(private = "file")
columnar_advance_state :: proc(c: ^Cursor) {
	if c.col == nil { return }

	cs := c.col
	if cs.num_cols == 0 || cs.rowid_pos == 0 { return }

	delta, n, ok := varint.decode(c.cached_page_data, cs.rowid_pos)
	if ok {
		cs.rowid += delta
		cs.rowid_pos += n
	}
	for col_i in 0 ..< int(cs.num_cols) {
		if cs.encodings[col_i] == cell.ENCODING_DELTA {
			d, dn, d_ok := varint.decode(c.cached_page_data, cs.val_pos[col_i])
			if d_ok {
				cs.running[col_i] = cs.mins[col_i] + i64(d)
				cs.val_pos[col_i] += dn
			}
		}
	}
}

// descend_to_next_leaf walks up from a finished leaf/interior cell to the next
// sibling in in-order, drilling down its leftmost leaf. Sets is_valid=false at
// end of tree. Caller has already popped the finished leaf's cell_index.
@(private = "file")
descend_to_next_leaf :: proc(c: ^Cursor) -> Error {
	for c.depth > 0 {
		top_idx := c.depth - 1
		item := &c.path[top_idx]
		node := load_cached_page(c, item.page_id) or_return
		item.cell_index += 1
		limit := int(node.header.cell_count)
		if is_leaf(node) {
			if int(item.cell_index) < limit {
				return .None
			}
			c.depth -= 1
		} else {
			if int(item.cell_index) <= limit {
				child_page: u32
				if int(item.cell_index) == limit {
					child_page = get_right_ptr(node.data, item.page_id)
				} else {
					nid := Page_Id(item.page_id)
					ptr, p_err := node.layout.vtable.cell_ptr_at(
						node.data,
						nid,
						int(item.cell_index),
					)
					if p_err != .None { return .Invalid_Cell_Pointer }
					child_page, _ = endian.get_u32(node.data[int(ptr):], .Big)
				}
				return drill_down_leftmost(c, child_page)
			}
			c.depth -= 1
		}
	}

	c.is_valid = false
	return .None
}

// cursor_get_cell_needed decodes the cell at the current cursor position but
// materializes only the columns flagged in `needed` (index = serial
// position) into `out_values` (caller storage: stack or reused batch
// buffer). Unneeded positions are set to Null. TEXT/BLOB for needed
// columns are ALWAYS borrowed from the page: valid only while the cursor
// stays on the page (single-page pin, slot-buffer reuse on eviction) —
// clone survivors before advancing. Zero allocations on both page kinds:
// row-major via cell.deserialize_needed, columnar via direct needed-slot
// reads from the cursor's sync-once decode state.
cursor_get_cell_needed :: proc(
	c: ^Cursor,
	needed: []bool,
	out_values: []types.Value,
) -> (
	rowid: types.Row_ID,
	err: Error,
) {
	if !c.is_valid || c.depth == 0 {
		return 0, .Cell_Not_Found
	}

	item := c.path[c.depth - 1]
	node, l_err := load_cached_page(c, item.page_id)
	if l_err != .None {
		return 0, l_err
	}
	if !is_leaf(node) {
		return 0, .Invalid_Page_Header
	}
	if intrinsics.unlikely(is_columnar(node.data, item.page_id)) {
		return cursor_get_cell_needed_columnar(c, node, item, needed, out_values)
	}

	nid := Page_Id(item.page_id)
	cell_count := get_cell_count(node.data, item.page_id)
	if int(item.cell_index) >= cell_count {
		return 0, .Cell_Not_Found
	}

	cell_ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, int(item.cell_index))
	if p_err != .None { return 0, .Cell_Deserialize_Failed }

	rid, _, ok := cell.deserialize_needed(node.data, int(cell_ptr), needed, out_values)
	if !ok {
		return 0, .Cell_Deserialize_Failed
	}
	return rid, .None
}

// cursor_get_cell_needed_columnar serves cursor_get_cell_needed on columnar
// pages without allocating: it runs the same sync-once state machine as
// read_columnar_cursor_cell, then reads only needed slots straight from
// the running state (DELTA ints) or the raw f64 region — the same value
// computation as columnar_assemble_row, minus the full-width make+copy.
// Unneeded positions are set to Null. Columnar pages hold ints/reals/Null
// only (no text/blob), so nothing here is borrowed or cloned. Same file
// so the private columnar state is in reach.
@(private = "file")
cursor_get_cell_needed_columnar :: proc(
	c: ^Cursor,
	node: Node,
	item: Cursor_Stack_Item,
	needed: []bool,
	out_values: []types.Value,
) -> (
	types.Row_ID,
	Error,
) {
	num_cols, found := detect_columnar_col_count(node.data, item.page_id)
	if !found || int(item.cell_index) < 0 { return 0, .Cell_Not_Found }
	// The fixed decode-state arrays are MAX_COLS wide; a wider page is
	// corrupt — fail loudly instead of indexing past them.
	if num_cols > types.MAX_COLS || num_cols > len(out_values) {
		return 0, .Cell_Deserialize_Failed
	}

	cs := cursor_col_state(c)
	cs.num_cols = u8(num_cols)
	if cs.rowid_pos == 0 {
		if !columnar_sync_to_cell(c, node, item, num_cols) {
			return 0, .Cell_Deserialize_Failed
		}
	}
	for i in 0 ..< len(out_values) {
		if i >= num_cols || i >= len(needed) || !needed[i] {
			out_values[i] = types.value_null()
			continue
		}
		if cs.encodings[i] == cell.ENCODING_DELTA {
			out_values[i] = types.value_int(cs.running[i])
			continue
		}

		pos := int(cs.offsets[i]) + int(item.cell_index) * 8
		if pos + 8 <= len(node.data) {
			if fv, fv_ok := endian.get_f64(node.data[pos:], .Big); fv_ok {
				out_values[i] = types.value_real(fv)
			} else {
				out_values[i] = types.value_null()
			}
		} else {
			out_values[i] = types.value_null()
		}
	}
	return types.Row_ID(cs.rowid), .None
}

// Deserialize and return the cell at the current cursor position.
// Values are allocated per the allocator. Zero-copy mode returns string/blob pointing into the page.
cursor_get_cell :: proc(c: ^Cursor, allocator: mem.Allocator) -> (cell.Cell, Error) {
	if !c.is_valid || c.depth == 0 {
		return {}, .Cell_Not_Found
	}

	item := c.path[c.depth - 1]
	node, err := load_cached_page(c, item.page_id)
	if err != .None {
		return {}, err
	}
	if !is_leaf(node) {
		return {}, .Invalid_Page_Header
	}

	actual_alloc := allocator
	if actual_alloc.procedure == nil {
		actual_alloc = context.allocator
	}
	if intrinsics.unlikely(is_columnar(node.data, item.page_id)) {
		return read_columnar_cursor_cell(c, node, item, actual_alloc)
	}

	nid := Page_Id(item.page_id)
	cell_count := get_cell_count(node.data, item.page_id)
	if int(item.cell_index) >= cell_count {
		return {}, .Cell_Not_Found
	}

	cell_ptr, p_err := node.layout.vtable.cell_ptr_at(node.data, nid, int(item.cell_index))
	if p_err != .None { return {}, .Cell_Deserialize_Failed }

	cell_cfg := cell.Config {
		allocator = actual_alloc,
		zero_copy = c.tree.config.zero_copy,
	}

	res_cell, _, ok := cell.deserialize(node.data, int(cell_ptr), cell_cfg)
	if !ok {
		return {}, .Cell_Deserialize_Failed
	}
	return res_cell, .None
}

// read_columnar_cursor_cell materializes one row from a columnar leaf page,
// advancing the cursor's incremental per-column decode state. On the first
// access to a page it syncs the rowid stream and every DELTA column to the
// current cell_index; subsequent rows advance O(columns) via cursor_advance.
@(private = "file")
read_columnar_cursor_cell :: proc(
	c: ^Cursor,
	node: Node,
	item: Cursor_Stack_Item,
	allocator: mem.Allocator,
) -> (
	cell.Cell,
	Error,
) {
	num_cols, found := detect_columnar_col_count(node.data, item.page_id)
	if !found || int(item.cell_index) < 0 { return {}, .Cell_Not_Found }

	cs := cursor_col_state(c)
	cs.num_cols = u8(num_cols)
	if cs.rowid_pos == 0 {
		if !columnar_sync_to_cell(c, node, item, num_cols) {
			return {}, .Cell_Deserialize_Failed
		}
	}

	values, v_ok := columnar_assemble_row(c, node, item, num_cols, allocator)
	if !v_ok { return {}, .Cell_Deserialize_Failed }
	return cell.Cell {
			rowid = types.Row_ID(cs.rowid),
			values = values,
			owns_data = !c.tree.config.zero_copy,
		},
		.None
}

// columnar_sync_to_cell positions the rowid stream and every DELTA value column
// at the cursor's current cell_index on first access to a columnar page.
@(private = "file")
columnar_sync_to_cell :: proc(
	c: ^Cursor,
	node: Node,
	item: Cursor_Stack_Item,
	num_cols: int,
) -> bool {
	cs := c.col
	boff := get_page_header_offset(item.page_id)
	cs.rowid_pos = boff + cell.COLUMNAR_DIR_OFFSET + num_cols * size_of(cell.Col_Header)
	cs.rowid = 0
	for _ in 0 ..= int(item.cell_index) {
		delta, n, ok := varint.decode(node.data, cs.rowid_pos)
		if !ok { break }

		cs.rowid += delta
		cs.rowid_pos += n
	}
	for col_i in 0 ..< num_cols {
		h, h_ok := cell.read_col_header(node.data, col_i, boff)
		if !h_ok { return false }

		cs.encodings[col_i] = h.encoding
		cs.offsets[col_i] = u32(boff + int(h.byte_offset))
		if h.encoding == cell.ENCODING_DELTA {
			if !columnar_sync_delta_col(c, node, col_i, h, boff) { return false }
		}
	}
	return true
}

// columnar_sync_delta_col advances one DELTA column's min/delta stream to the
// cursor's current cell_index (the stored delta yields value == min + delta,
// not a running sum).
@(private = "file")
columnar_sync_delta_col :: proc(
	c: ^Cursor,
	node: Node,
	col_i: int,
	h: cell.Col_Header,
	boff: int,
) -> bool {
	pos := boff + int(h.byte_offset)
	min, n1, ok1 := varint.decode(node.data, pos)
	if !ok1 { return false }

	pos += n1
	cs := c.col
	cs.mins[col_i] = i64(min)
	running := i64(min)
	for _ in 0 ..= int(c.path[c.depth - 1].cell_index) {
		d, n2, ok2 := varint.decode(node.data, pos)
		if !ok2 { return false }

		running = i64(min) + i64(d)
		pos += n2
	}

	cs.running[col_i] = running
	cs.val_pos[col_i] = pos
	return true
}

// columnar_assemble_row builds the Value row from the cursor's cached
// per-column state (O(1) per row): DELTA columns read the running value, RAW
// columns index directly by cell_index.
@(private = "file")
columnar_assemble_row :: proc(
	c: ^Cursor,
	node: Node,
	item: Cursor_Stack_Item,
	num_cols: int,
	allocator: mem.Allocator,
) -> (
	[]types.Value,
	bool,
) {
	scratch: [dynamic; types.MAX_COLS]types.Value
	cs := c.col
	for col_i in 0 ..< num_cols {
		if cs.encodings[col_i] == cell.ENCODING_DELTA {
			append(&scratch, types.value_int(cs.running[col_i]))
		} else {
			pos := int(cs.offsets[col_i]) + int(item.cell_index) * 8
			if pos + 8 <= len(node.data) {
				if fv, fv_ok := endian.get_f64(node.data[pos:], .Big); fv_ok {
					append(&scratch, types.value_real(fv))
				} else {
					append(&scratch, types.Null{})
				}
			} else {
				append(&scratch, types.Null{})
			}
		}
	}

	result_values := make([]types.Value, len(scratch), allocator)
	copy(result_values, scratch[:])
	return result_values, true
}
