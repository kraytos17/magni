package pager

import "core:mem"
import "core:os"
import "core:sync"
import "src:types"
import "src:util/bitmap"

// Allocates a page from the free-page linked list. Caller MUST hold p.mutex (write-locked).
// Reads the first 4 bytes of the free page as the next-free pointer.
@(private)
alloc_from_freelist :: proc(p: ^Pager) -> (^Page, Error) {
	free_page_num := p.first_free_page
	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }

	offset := i64(free_page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, slot._data_buf[:], offset)
	if read_err != nil || bytes_read < int(p.page_size) {
		slot.page.page_num = 0
		slot.page.data = nil
		p.slot_count -= 1
		return nil, .IO_Error
	}

	next_free := (^u32)(raw_data(slot._data_buf[:]))^
	p.first_free_page = next_free
	mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
	slot.page.page_num = free_page_num; slot.page.pin_count = 1
	mark_slot_dirty(p, slot)
	p.cache_index[free_page_num] = slot
	bitmap_grow(p, free_page_num)
	bitmap.set(p.page_bitmap, free_page_num)
	return &slot.page, .None
}

// Adds a page to the free-page linked list and evicts it from cache.
free_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if page_num <= 1 { return }
	if slot := find_slot(p, page_num); slot != nil {
		if slot.page.pin_count > 0 { return }

		(^u32)(raw_data(slot._data_buf[:]))^ = p.first_free_page
		slot.page.dirty = true
		wal_append_frame(p, page_num, slot._data_buf[:], false, 0)
		delete_key(&p.cache_index, page_num)
		if p.on_evict != nil { p.on_evict(p.stats, page_num) }
		slot.page.page_num = 0; slot.page.data = nil; p.slot_count -= 1
	}
	p.first_free_page = page_num
	bitmap.clear(p.page_bitmap, page_num)
}
