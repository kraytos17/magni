// Package pager provides page-level I/O, caching, WAL, freelist, and page bitmap tracking.
package pager

import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "core:sync"
import "src:types"

PAGE_CACHE_SIZE :: 256

Page :: struct {
	data:      []u8,
	page_num:  u32,
	dirty:     bool,
	pin_count: u32,
}

Page_Slot :: struct {
	page:      Page,
	_data_buf: [types.PAGE_SIZE]u8,
}

Pager :: struct {
	file:            ^os.File,
	file_name:       string,
	file_len:        i64,
	page_size:       u32,
	slots:           [PAGE_CACHE_SIZE]Page_Slot,
	max_cache_pages: u32,
	mutex:           sync.RW_Mutex, // shared lock for reads, exclusive for writes
	allocator:       mem.Allocator,
	first_free_page: u32, // head of the free-page linked list
	slot_count:      u32, // cache slots currently in use
	cache_index:     map[u32]^Page_Slot, // page_num → cache slot
	wal_state:       Wal_State, // WAL frame indices (txn_index + page_index)
	page_bitmap:     []u64, // bit N = 1 → page N was ever allocated (GC uses this)
}

Error :: enum {
	None,
	File_Open_Failed,
	IO_Error,
	Out_Of_Memory,
	Cache_Full,
	Page_Not_Found,
	Invalid_Page_Num,
}

find_slot :: proc(p: ^Pager, page_num: u32) -> ^Page_Slot { return p.cache_index[page_num] }

find_empty_slot :: proc(p: ^Pager) -> ^Page_Slot {
	if p.slot_count >=
	   p.max_cache_pages { if err := evict_one_slot(p); err != .None { return nil } }
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num == 0 {
			slot.page.data = slot._data_buf[:]; p.slot_count += 1
			return slot
		}
	}
	if evict_one_slot(p) != .None { return nil }
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num == 0 {
			slot.page.data = slot._data_buf[:]; p.slot_count += 1
			return slot
		}
	}
	return nil
}

evict_one_slot :: proc(p: ^Pager) -> Error {
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num != 0 && slot.page.pin_count == 0 {
			if slot.page.dirty {
				if err := wal_append_frame(p, slot.page.page_num, slot.page.data, false, 0);
				   err != .None { return err }
			}

			delete_key(&p.cache_index, slot.page.page_num)
			slot.page.page_num = 0; slot.page.data = nil; p.slot_count -= 1
			return .None
		}
	}
	return .Cache_Full
}

// Open (or create) a database file at path. Initializes the pager, page cache,
// WAL, and page bitmap. Returns nil + error on failure.
open :: proc(
	path: string,
	max_pages: u32 = PAGE_CACHE_SIZE,
	allocator := context.allocator,
) -> (
	^Pager,
	Error,
) {
	p := new(Pager, allocator)
	if p == nil { return nil, .Out_Of_Memory }

	p.allocator = allocator; p.page_size = types.PAGE_SIZE
	p.max_cache_pages = min(max_pages, PAGE_CACHE_SIZE)
	p.cache_index = make(map[u32]^Page_Slot, PAGE_CACHE_SIZE, allocator)
	p.wal_state.page_index = make(map[u32]i64, allocator)
	p.wal_state.txn_index = make(map[u32]i64, allocator)
	flags := os.O_RDWR | os.O_CREATE
	file, open_err := os.open(path, flags)
	if open_err != nil {
		free(p)
		return nil, .File_Open_Failed
	}

	p.file = file; p.file_name = strings.clone(path, allocator)
	file_size, size_err := os.file_size(file)
	if size_err != nil {
		os.close(file)
		free(p)
		return nil, .IO_Error
	}

	p.file_len = file_size if file_size != 0 else 0
	// Initialize page bitmap: mark all existing pages as allocated
	page_count := u32(p.file_len / i64(p.page_size))
	bitmap_len := (int(page_count) + 63) / 64
	if bitmap_len > 0 {
		p.page_bitmap = make([]u64, bitmap_len, p.allocator)
		for i := 0; i < bitmap_len; i += 1 { p.page_bitmap[i] = ~u64(0) }
	}
	if err := wal_open(p, path); err != .None {
		fmt.eprintln("Pager: WAL open failed:", err)
		os.close(file)
		free(p)
		return nil, err
	}
	return p, .None
}

// Close the pager: flush WAL, checkpoint to main file, close file, free all resources.
close :: proc(p: ^Pager) -> Error {
	if p == nil { return .None }

	wal_begin_txn(p)
	wal_commit_txn(p)
	wal_checkpoint(p)
	wal_close(p)
	if p.file != nil { os.close(p.file) }

	delete(p.file_name); delete(p.cache_index); delete(p.page_bitmap); free(p, p.allocator)
	return .None
}

// Page numbers are 1-indexed; 0 is the sentinel for "no page".
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	if page_num < 1 { return nil, .Invalid_Page_Num }

	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		return &slot.page, .None
	}

	max_page := u32(p.file_len / i64(p.page_size))
	if page_num > max_page { return nil, .Page_Not_Found }

	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }

	ws := &p.wal_state
	fo: i64
	has_fo := false
	{
		v, txn_ok := ws.txn_index[page_num]
		if txn_ok { fo = v; has_fo = true }
	}
	if !has_fo {
		v, idx_ok := ws.page_index[page_num]
		if idx_ok { fo = v; has_fo = true }
	}
	if has_fo {
		_, read_err := os.read_at(ws.file, slot._data_buf[:], fo + types.WAL_FRAME_HEADER_SIZE)
		if read_err == nil {
			slot.page.page_num = page_num; slot.page.pin_count = 1; slot.page.dirty = false
			p.cache_index[page_num] = slot
			return &slot.page, .None
		}
	}

	offset := i64(page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, slot._data_buf[:], offset)
	if read_err != nil || bytes_read < int(p.page_size) {
		slot.page.page_num = 0
		slot.page.data = nil
		p.slot_count -= 1
		return nil, .IO_Error
	}

	slot.page.page_num = page_num; slot.page.pin_count = 1; slot.page.dirty = false
	p.cache_index[page_num] = slot
	return &slot.page, .None
}

// Allocate a new page (from freelist or extending the file). Returns a pinned,
// dirty page zeroed to DATABASE_HEADER_SIZE. Caller must unpin when done.
allocate_page :: proc(p: ^Pager) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if p.first_free_page != 0 { return alloc_from_freelist(p) }

	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }

	new_page_num := u32(p.file_len / i64(p.page_size)) + 1
	mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
	slot.page.page_num = new_page_num; slot.page.pin_count = 1; slot.page.dirty = true
	p.cache_index[new_page_num] = slot; p.file_len += i64(p.page_size)
	bitmap_grow(p, new_page_num)
	bitmap_set(p.page_bitmap, new_page_num)
	return &slot.page, .None
}

get_or_allocate_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if page_num < 1 { return nil, .Page_Not_Found }
	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		return &slot.page, .None
	}

	current_max := u32(p.file_len / i64(p.page_size))
	if page_num == current_max + 1 {
		slot := find_empty_slot(p)
		if slot == nil { return nil, .Cache_Full }

		mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
		slot.page.page_num = page_num; slot.page.pin_count = 1; slot.page.dirty = true
		p.cache_index[page_num] = slot; p.file_len += i64(p.page_size)
		bitmap_grow(p, page_num)
		bitmap_set(p.page_bitmap, page_num)
		return &slot.page, .None
	}
	return nil, .Page_Not_Found
}

// Decrement the pin count for a page. When pin_count reaches 0, the page is eligible for eviction.
unpin_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if slot := find_slot(p, page_num); slot != nil && slot.page.pin_count > 0 {
		slot.page.pin_count -= 1
	}
}

// Logical page count (may differ from actual file size with WAL).
page_count :: proc(p: ^Pager) -> u32 {
	sync.rw_mutex_shared_lock(&p.mutex); defer sync.rw_mutex_shared_unlock(&p.mutex)
	return u32(p.file_len / i64(p.page_size))
}

page_in_cache :: proc(p: ^Pager, page_num: u32) -> bool {
	sync.rw_mutex_shared_lock(&p.mutex); defer sync.rw_mutex_shared_unlock(&p.mutex)
	return find_slot(p, page_num) != nil
}

// Copy the content of src_page_num to a newly allocated page. Source is unpinned,
// destination is pinned + dirty. Used by COW operations.
copy_page :: proc(p: ^Pager, src_page_num: u32) -> (^Page, Error) {
	src, err := get_page(p, src_page_num)
	if err != .None { return nil, err }
	defer unpin_page(p, src_page_num)

	dst, dst_err := allocate_page(p)
	if dst_err != .None { return nil, dst_err }

	copy(dst.data, src.data); dst.dirty = true
	return dst, .None
}

mark_dirty :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if slot := find_slot(p, page_num); slot != nil { slot.page.dirty = true }
}

bitmap_set :: proc(bm: []u64, pn: u32) {
	idx := int(pn) / 64
	if idx < len(bm) { bm[idx] |= u64(1) << uint(pn % 64) }
}

bitmap_grow :: proc(p: ^Pager, max_pn: u32) {
	needed := int(max_pn) / 64 + 1
	if needed > len(p.page_bitmap) {
		old := p.page_bitmap
		p.page_bitmap = make([]u64, needed, p.allocator)
		copy(p.page_bitmap, old)
		for i := len(old); i < needed; i += 1 { p.page_bitmap[i] = ~u64(0) }
		delete(old)
	}
}

bitmap_clear :: proc(bm: []u64, pn: u32) {
	idx := int(pn) / 64
	if idx < len(bm) {
		mask := ~(u64(1) << uint(pn % 64))
		bm[idx] &= mask
	}
}

bitmap_test :: proc(bm: []u64, pn: u32) -> bool {
	idx := int(pn) / 64
	return idx < len(bm) && (bm[idx] & (u64(1) << uint(pn % 64))) != 0
}
