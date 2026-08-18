package btree

// Freeblock format:
//   [offset+0]: next freeblock offset (u16le, 0 = end of list)
//   [offset+2]: block size        (u16le, total bytes including header)
FREEBLOCK_HDR_SIZE :: 4


@(private="file")
freeblock_read_next :: proc(data: []u8, off: u16) -> u16le {
	return (^u16le)(raw_data(data[int(off):]))^
}

@(private="file")
freeblock_read_size :: proc(data: []u8, off: u16) -> u16le {
	return (^u16le)(raw_data(data[int(off) + 2:]))^
}

@(private="file")
freeblock_write_next :: proc(data: []u8, off: u16, next: u16le) {
	(^u16le)(raw_data(data[int(off):]))^ = next
}

@(private="file")
freeblock_write_size :: proc(data: []u8, off: u16, sz: u16le) {
	(^u16le)(raw_data(data[int(off) + 2:]))^ = sz
}

// Inserts a cell-sized freeblock into the chain. cell_off/cell_sz are native u16.
@(private)
freeblock_insert :: proc(data: []u8, cell_off: u16, cell_sz: u16, first: ^u16le) {
	if cell_sz < FREEBLOCK_HDR_SIZE { return }

	co := u16le(cell_off)
	cs := u16le(cell_sz)
	if first^ == 0 {
		freeblock_write_next(data, cell_off, 0)
		freeblock_write_size(data, cell_off, cs)
		first^ = co
		return
	}

	// Before first block
	f := first^
	if co + cs == f {
		sz := freeblock_read_size(data, u16(f)) + cs
		freeblock_write_size(data, cell_off, sz)
		freeblock_write_next(data, cell_off, freeblock_read_next(data, u16(f)))
		first^ = co
		return
	}
	if co < f {
		freeblock_write_next(data, cell_off, f)
		freeblock_write_size(data, cell_off, cs)
		first^ = co
		return
	}

	// Walk chain
	prev := f
	for {
		nxt := freeblock_read_next(data, u16(prev))
		if nxt == 0 {
			end_prev := u16(prev) + u16(freeblock_read_size(data, u16(prev)))
			if u16le(end_prev) == co {
				sz := freeblock_read_size(data, u16(prev)) + cs
				freeblock_write_size(data, u16(prev), sz)
			} else {
				freeblock_write_next(data, u16(prev), co)
				freeblock_write_next(data, cell_off, 0)
				freeblock_write_size(data, cell_off, cs)
			}
			return
		}

		end_prev := u16(prev) + u16(freeblock_read_size(data, u16(prev)))
		if u16le(end_prev) > co { return }
		if u16le(end_prev) == co {
			// Merge with prev
			new_sz := freeblock_read_size(data, u16(prev)) + cs
			if co + cs == nxt {
				// Three-way merge: prev + new + next
				new_sz += freeblock_read_size(data, u16(nxt))
				freeblock_write_size(data, u16(prev), new_sz)
				freeblock_write_next(data, u16(prev), freeblock_read_next(data, u16(nxt)))
			} else {
				freeblock_write_size(data, u16(prev), new_sz)
			}
			return
		}
		if co + cs == nxt {
			sz := cs + freeblock_read_size(data, u16(nxt))
			freeblock_write_size(data, cell_off, sz)
			freeblock_write_next(data, cell_off, freeblock_read_next(data, u16(nxt)))
			freeblock_write_next(data, u16(prev), co)
			return
		}
		if co < nxt {
			freeblock_write_next(data, cell_off, nxt)
			freeblock_write_size(data, cell_off, cs)
			freeblock_write_next(data, u16(prev), co)
			return
		}
		prev = nxt
	}
}

// Allocates space from a freeblock. Returns offset (native u16), or 0.
@(private)
freeblock_alloc :: proc(data: []u8, first_hdr: u16le, need: u16, first: ^u16le) -> u16 {
	f := u16(first_hdr)
	if f == 0 { return 0 }

	walk_and_alloc :: proc(data: []u8, prev: u16, curr: u16, need: u16, first: ^u16le) -> u16 {
		fsz := u16(freeblock_read_size(data, curr))
		if fsz >= need {
			if fsz == need {
				nxt := freeblock_read_next(data, curr)
				if prev == 0 { first^ = nxt } else { freeblock_write_next(data, prev, nxt) }
				return curr
			}

			remain := fsz - need
			if remain >= FREEBLOCK_HDR_SIZE {
				freeblock_write_size(data, curr, u16le(remain))
				return curr + remain
			}

			nxt := freeblock_read_next(data, curr)
			if prev == 0 { first^ = nxt } else { freeblock_write_next(data, prev, nxt) }
			return curr
		}
		return 0
	}

	if r := walk_and_alloc(data, 0, f, need, first); r != 0 { return r }
	for {
		nxt_u16 := u16(freeblock_read_next(data, f))
		if nxt_u16 == 0 { return 0 }
		if r := walk_and_alloc(data, f, nxt_u16, need, first); r != 0 { return r }
		f = nxt_u16
	}
	return 0
}
