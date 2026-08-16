// Package pager provides page-level I/O, caching, WAL, freelist, and page bitmap tracking.
package pager

import "core:log"
import "core:mem"
import "core:os"
import "core:strings"
import "core:sync"
import "src:types"
import "src:util/bitmap"

PAGE_CACHE_SIZE :: 256

Page :: struct {
	data:      []u8,
	page_num:  u32,
	dirty:     bool,
	pin_count: u32,
}

Page_Slot :: struct {
	page:       Page,
	_data_buf:  [types.PAGE_SIZE]u8,
	referenced: bool,
}

Page_Int_Range :: struct {
	col_index: u8,
	min_int:   i64,
	max_int:   i64,
}

// is_special_page reports whether a page carries a fixed-size leading header
// (page 1 embeds the 100-byte database header). Page-1 semantics belong to the
// pager/storage layer; consumers that COW-copy page 1 must relocate the header.
is_special_page :: proc(page_num: u32) -> bool {
	return page_num == 1
}

Pager :: struct {
	mutex:               sync.RW_Mutex, // guards storage state; see docs/concurrency.md (acquire after db.mu)
	cache_index:         map[u32]^Page_Slot,
	free_slots:          [dynamic]^Page_Slot,
	slot_count:          u32,
	evict_hand:          u32,
	dirty_pages:         [dynamic]u32, // pages dirtied in the current WAL txn; iterated by wal_commit/abort
	file:                ^os.File,
	file_len:            i64,
	page_bitmap:         []u64,
	wal_state:           Wal_State,
	slots:               []Page_Slot,
	file_name:           string,
	page_size:           u32,
	max_cache_pages:     u32,
	first_free_page:     u32,
	page_format_version: u32,
	allocator:           mem.Allocator,
	// Opaque B-tree statistics, owned by the btree package. The pager stores
	// them (so they survive transient btree.Tree instances) but never
	// interprets them: it clears entries via on_evict and frees via free_stats.
	stats:               rawptr,
	on_evict:            proc(data: rawptr, page_num: u32),
	free_stats:          proc(data: rawptr),
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
	if p.slot_count >= p.max_cache_pages {
		if evict_one_slot(p) != .None {
			return nil
		}
	}
	if len(p.free_slots) == 0 {
		return nil
	}

	slot := pop(&p.free_slots)
	slot.page.data = slot._data_buf[:]
	slot.referenced = false
	p.slot_count += 1
	return slot
}

evict_one_slot :: proc(p: ^Pager) -> Error {
	n := len(p.slots)
	for pass := 0; pass < 2; pass += 1 {
		for _ in 0 ..< n {
			idx := int(p.evict_hand) % n
			slot := &p.slots[idx]
			p.evict_hand = u32((int(p.evict_hand) + 1) % n)
			if slot.page.page_num == 0 || slot.page.pin_count > 0 {
				continue
			}
			if slot.referenced {
				slot.referenced = false
				if pass == 0 { continue }
			}
			if slot.page.dirty {
				wal_append_frame(p, slot.page.page_num, slot.page.data, false, 0) or_return
			}

			delete_key(&p.cache_index, slot.page.page_num)
			if p.on_evict != nil { p.on_evict(p.stats, slot.page.page_num) }

			slot.page = {}
			slot.referenced = false
			p.slot_count -= 1
			append(&p.free_slots, slot)
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
	p.max_cache_pages = clamp(max(max_pages, 1), 1, PAGE_CACHE_SIZE)
	p.slots = make([]Page_Slot, p.max_cache_pages, allocator)
	p.cache_index = make(map[u32]^Page_Slot, p.max_cache_pages, allocator)
	p.wal_state.page_index = make(map[u32]i64, allocator)
	p.wal_state.txn_index = make(map[u32]i64, allocator)
	p.free_slots = make([dynamic]^Page_Slot, 0, p.max_cache_pages, allocator)
	p.dirty_pages = make([dynamic]u32, 0, 32, allocator)
	p.page_format_version = 1
	p.slot_count = 0
	for i in 0 ..< p.max_cache_pages {
		append(&p.free_slots, &p.slots[i])
	}

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
		log.errorf("Pager: WAL open failed: %v", err)
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

	delete(p.file_name)
	delete(p.cache_index)
	delete(p.page_bitmap)
	delete(p.free_slots)
	delete(p.dirty_pages)
	if p.free_stats != nil { p.free_stats(p.stats) }

	delete(p.slots)
	free(p, p.allocator)
	return .None
}

// Page numbers are 1-indexed; 0 is the sentinel for "no page".
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	if page_num < 1 { return nil, .Invalid_Page_Num }

	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)
	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		slot.referenced = true
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
		slot.page = {}
		p.slot_count -= 1
		append(&p.free_slots, slot)
		return nil, .IO_Error
	}

	slot.page.page_num = page_num
	slot.page.pin_count = 1
	slot.page.dirty = false
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
	slot.page.page_num = new_page_num; slot.page.pin_count = 1
	mark_slot_dirty(p, slot)

	p.cache_index[new_page_num] = slot; p.file_len += i64(p.page_size)
	bitmap_grow(p, new_page_num)
	bitmap.set(p.page_bitmap, new_page_num)
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
		slot.page.page_num = page_num; slot.page.pin_count = 1
		mark_slot_dirty(p, slot)

		p.cache_index[page_num] = slot; p.file_len += i64(p.page_size)
		bitmap_grow(p, page_num)
		bitmap.set(p.page_bitmap, page_num)
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
copy_page :: proc(p: ^Pager, src_page_num: u32) -> (dst: ^Page, err: Error) {
	src: ^Page
	src, err = get_page(p, src_page_num)
	if err != .None { return }
	defer unpin_page(p, src_page_num)

	dst = allocate_page(p) or_return
	copy(dst.data, src.data); dst.dirty = true
	return
}

// mark_slot_dirty marks a cached page dirty and records it in the current WAL
// txn's dirty list so wal_commit_txn/wal_abort_txn need only scan dirtied pages.
// Caller must hold p.mutex.
mark_slot_dirty :: proc(p: ^Pager, slot: ^Page_Slot) {
	if slot == nil || slot.page.page_num == 0 { return }
	if !slot.page.dirty {
		slot.page.dirty = true
		append(&p.dirty_pages, slot.page.page_num)
	}
}

mark_dirty :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	mark_slot_dirty(p, find_slot(p, page_num))
}

bitmap_grow :: proc(p: ^Pager, max_pn: u32) {
	p.page_bitmap = bitmap.grow(p.page_bitmap, max_pn, p.allocator)
}
