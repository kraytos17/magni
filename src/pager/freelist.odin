package pager

import "core:container/bit_array"
import "core:mem"
import "core:os"
import "core:sync"
import "src:types"

// read_page_into reads the latest image of page_num into slot._data_buf.
// WAL frames shadow the main file (same order as get_page). Caller MUST hold
// p.mutex (write-locked). Returns false if the page could not be read.
@(private)
read_page_into :: proc(p: ^Pager, slot: ^Page_Slot, page_num: u32) -> bool {
	ws := &p.wal_state
	if fo, ok := ws.txn_index[page_num]; ok {
		_, read_err := os.read_at(ws.file, slot._data_buf[:], fo + types.WAL_FRAME_HEADER_SIZE)
		if read_err == nil { return true }
	}
	if fo, ok := ws.page_index[page_num]; ok {
		_, read_err := os.read_at(ws.file, slot._data_buf[:], fo + types.WAL_FRAME_HEADER_SIZE)
		if read_err == nil { return true }
	}

	offset := i64(page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, slot._data_buf[:], offset)
	return read_err == nil && bytes_read == int(p.page_size)
}

// release_slot returns a popped slot to the pool after a failed fill.
// Caller MUST hold p.mutex (write-locked).
@(private = "file")
release_slot :: proc(p: ^Pager, slot: ^Page_Slot) {
	slot.page = {}
	slot.page.data = nil
	p.slot_count -= 1
	append(&p.free_slots, slot)
}

// Allocates a page from the free-page linked list. Caller MUST hold p.mutex (write-locked).
// Reads the first 4 bytes of the free page as the next-free pointer.
@(private)
alloc_from_freelist :: proc(p: ^Pager) -> (^Page, Error) {
	free_page_num := p.first_free_page
	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }
	// WAL-first read: free_page persists links via WAL frames, which may not
	// have checkpointed yet. A raw main-file read would see stale content.
	if !read_page_into(p, slot, free_page_num) {
		release_slot(p, slot)
		return nil, .IO_Error
	}

	next_free := (^u32)(raw_data(slot._data_buf[:]))^
	max_page := u32(p.file_len / i64(p.page_size))
	if next_free == free_page_num || next_free > max_page + 1 { next_free = 0 }

	p.first_free_page = next_free
	mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
	slot.page.page_num = free_page_num; slot.page.pin_count = 1

	mark_slot_dirty(p, slot)
	cache_insert(p, free_page_num, slot)
	bit_array.set(&p.page_bitmap, free_page_num, true, p.allocator)
	return &slot.page, .None
}

// rewind_after_abort discards the file tail allocated by an aborted
// transaction and persists the rewind to disk. The caller must have already
// restored live roots from the last snapshot and aborted the WAL txn, so
// every page past the cut is unreachable by construction.
//
// Fail-closed: a pinned page past the cut (something still references it)
// or any I/O error aborts the rewind with the file untouched. The freelist
// head resets because its links may reference the abandoned tail; the next
// GC rebuilds it from scratch (temporary regrowth, never corruption).
rewind_after_abort :: proc(p: ^Pager, new_page_count: u32) -> (rewound: bool, err: Error) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	max_page := u32(p.file_len / i64(p.page_size))
	if new_page_count >= max_page || new_page_count < 1 {
		return false, .None
	}
	for i in 0 ..< len(p.slots) {
		slot := &p.slots[i]
		pn := slot.page.page_num
		if pn == 0 || pn <= new_page_count { continue }
		if slot.page.pin_count > 0 { return false, .None }
		evict_slot(p, slot, false)
	}
	for pn := new_page_count + 1; pn <= max_page; pn += 1 {
		bit_array.unset(&p.page_bitmap, pn, p.allocator)
	}

	p.first_free_page = 0
	new_len := i64(new_page_count) * i64(p.page_size)
	if terr := os.truncate(p.file, new_len); terr != nil { return false, .IO_Error }

	p.file_len = new_len
	if serr := os.sync(p.file); serr != nil { return true, .IO_Error }
	return true, .None
}

// Adds a page to the free-page linked list and evicts it from cache.
free_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if page_num <= 1 { return }

	slot := find_slot(p, page_num)
	if slot == nil {
		// Page not cached: bring it in so the freelist link is actually
		// written to storage. Skipping the write leaves stale content on
		// disk, which alloc_from_freelist would misread as a next pointer.
		slot = find_empty_slot(p)
		if slot == nil { return } 	// Cache exhausted: leave allocated; retry next GC.
		if !read_page_into(p, slot, page_num) {
			release_slot(p, slot)
			return
		}

		slot.page.page_num = page_num
		slot.page.pin_count = 0
		cache_insert(p, page_num, slot)
	}
	if slot.page.pin_count > 0 { return }

	(^u32)(raw_data(slot._data_buf[:]))^ = p.first_free_page
	slot.page.dirty = true
	wal_append_frame(p, page_num, slot._data_buf[:], false, 0)
	// Link frame already written above, so drop without a second writeback.
	// evict_slot fully resets the slot (old code left dirty=true, which
	// would make the next mark_slot_dirty skip dirty_pages tracking).
	evict_slot(p, slot, false)
	p.first_free_page = page_num
	bit_array.unset(&p.page_bitmap, page_num, p.allocator)
}
