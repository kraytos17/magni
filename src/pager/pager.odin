package pager

import "core:mem"
import "core:os"
import "core:sync"
import "src:types"

PAGE_CACHE_SIZE :: 256

Page :: struct {
	data:      []u8,
	page_num:  u32,
	dirty:     bool,
	pin_count: u8,
}

Page_Slot :: struct {
	page:      Page,
	_data_buf: [types.PAGE_SIZE]u8,
}

Pager :: struct {
	file:            ^os.File,
	file_len:        i64,
	page_size:       u32,
	slots:           [PAGE_CACHE_SIZE]Page_Slot,
	max_cache_pages: u32,
	mutex:           sync.RW_Mutex,
	allocator:       mem.Allocator,
	first_free_page: u32,
	slot_count:      u32,
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

@(private = "file")
slot_hash :: proc(page_num: u32) -> u32 {
	return page_num % PAGE_CACHE_SIZE
}

@(private = "file")
find_slot :: proc(p: ^Pager, page_num: u32) -> ^Page_Slot {
	start := slot_hash(page_num)
	for i := u32(0); i < PAGE_CACHE_SIZE; i += 1 {
		idx := (start + i) % PAGE_CACHE_SIZE
		slot := &p.slots[idx]
		if slot.page.page_num == page_num { return slot }
	}
	return nil
}

@(private = "file")
find_empty_slot :: proc(p: ^Pager) -> ^Page_Slot {
	if p.slot_count >= p.max_cache_pages {
		if err := evict_one_slot(p); err != .None {
			return nil
		}
	}
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num == 0 {
			slot.page.data = slot._data_buf[:]
			p.slot_count += 1
			return slot
		}
	}
	if evict_one_slot(p) != .None { return nil }
	// Retry after eviction
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num == 0 {
			slot.page.data = slot._data_buf[:]
			p.slot_count += 1
			return slot
		}
	}
	return nil
}

@(private = "file")
evict_one_slot :: proc(p: ^Pager) -> Error {
	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.page_num != 0 && slot.page.pin_count == 0 {
			if slot.page.dirty {
				if err := flush_page_unsafe(p, &slot.page); err != .None {
					return err
				}
			}
			slot.page.page_num = 0
			slot.page.data = nil
			p.slot_count -= 1
			return .None
		}
	}
	return .Cache_Full
}

open :: proc(path: string, max_pages: u32 = 256, allocator := context.allocator) -> (^Pager, Error) {
	p := new(Pager, allocator)
	if p == nil { return nil, .Out_Of_Memory }

	p.allocator = allocator
	p.page_size = types.PAGE_SIZE
	p.max_cache_pages = min(max_pages, PAGE_CACHE_SIZE)

	flags := os.O_RDWR | os.O_CREATE
	file, open_err := os.open(path, flags)
	if open_err != nil {
		free(p)
		return nil, .File_Open_Failed
	}

	p.file = file
	file_size, size_err := os.file_size(file)
	if size_err != nil {
		os.close(file)
		free(p)
		return nil, .IO_Error
	}
	if file_size == 0 {
		p.file_len = 0
	} else {
		p.file_len = file_size
	}
	return p, .None
}

close :: proc(p: ^Pager) -> Error {
	if p == nil { return .None }

	flush_err := flush_all(p)
	if p.file != nil {
		os.close(p.file)
	}

	free(p, p.allocator)
	return flush_err
}

// Retrieves an existing page from cache or disk
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	if page_num < 1 { return nil, .Invalid_Page_Num }

	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		return &slot.page, .None
	}

	max_page := u32(p.file_len / i64(p.page_size))
	if page_num > max_page {
		return nil, .Page_Not_Found
	}

	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }

	offset := i64(page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, slot._data_buf[:], offset)
	if read_err != nil || bytes_read < int(p.page_size) {
		slot.page.page_num = 0
		slot.page.data = nil
		p.slot_count -= 1
		return nil, .IO_Error
	}

	slot.page.page_num = page_num
	slot.page.pin_count = 1
	slot.page.dirty = false
	return &slot.page, .None
}

// Creates a new page. Tries the freelist first; if empty, appends to end of file.
allocate_page :: proc(p: ^Pager) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	if p.first_free_page != 0 {
		return alloc_from_freelist(p)
	}

	slot := find_empty_slot(p)
	if slot == nil { return nil, .Cache_Full }

	new_page_num := u32(p.file_len / i64(p.page_size)) + 1
	mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
	slot.page.page_num = new_page_num
	slot.page.pin_count = 1
	slot.page.dirty = true
	p.file_len += i64(p.page_size)
	return &slot.page, .None
}

// Helper for algorithms that might need to get OR create (like root page init)
get_or_allocate_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	if page_num < 1 { return nil, .Page_Not_Found }

	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		return &slot.page, .None
	}

	current_max := u32(p.file_len / i64(p.page_size))
	if page_num == current_max + 1 {
		slot := find_empty_slot(p)
		if slot == nil { return nil, .Cache_Full }

		new_page_num := current_max + 1
		mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
		slot.page.page_num = new_page_num
		slot.page.pin_count = 1
		slot.page.dirty = true
		p.file_len += i64(p.page_size)
		return &slot.page, .None
	}
	return nil, .Page_Not_Found
}

unpin_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	if slot := find_slot(p, page_num); slot != nil {
		if slot.page.pin_count > 0 {
			slot.page.pin_count -= 1
		}
	}
}

flush_all :: proc(p: ^Pager) -> Error {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	for i in 0 ..< PAGE_CACHE_SIZE {
		slot := &p.slots[i]
		if slot.page.dirty {
			if err := flush_page_unsafe(p, &slot.page); err != .None {
				return err
			}
		}
	}
	if err := os.sync(p.file); err != nil {
		return .IO_Error
	}
	return .None
}

// Creates a copy of an existing page at a new page number. Returns the new page.
copy_page :: proc(p: ^Pager, src_page_num: u32) -> (^Page, Error) {
	src, err := get_page(p, src_page_num)
	if err != .None { return nil, err }
	defer unpin_page(p, src_page_num)

	dst, dst_err := allocate_page(p)
	if dst_err != .None { return nil, dst_err }

	copy(dst.data, src.data)
	dst.dirty = true
	return dst, .None
}

mark_dirty :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)
	if slot := find_slot(p, page_num); slot != nil {
		slot.page.dirty = true
	}
}

page_count :: proc(p: ^Pager) -> u32 {
	sync.rw_mutex_shared_lock(&p.mutex)
	defer sync.rw_mutex_shared_unlock(&p.mutex)
	return u32(p.file_len / i64(p.page_size))
}

// Returns true if the given page number is currently in the cache.
page_in_cache :: proc(p: ^Pager, page_num: u32) -> bool {
	sync.rw_mutex_shared_lock(&p.mutex)
	defer sync.rw_mutex_shared_unlock(&p.mutex)
	return find_slot(p, page_num) != nil
}

@(private = "file")
flush_page_unsafe :: proc(p: ^Pager, page: ^Page) -> Error {
	if !page.dirty { return .None }

	offset := i64(page.page_num - 1) * i64(p.page_size)
	_, err := os.write_at(p.file, page.data, offset)
	if err != nil { return .IO_Error }

	page.dirty = false
	return .None
}

// Allocates a page from the free-page linked list. Caller must hold p.mutex.
@(private = "file")
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
	slot.page.page_num = free_page_num
	slot.page.pin_count = 1
	slot.page.dirty = true
	return &slot.page, .None
}

// Adds a page to the free-page linked list and evicts it from cache.
free_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	if page_num <= 1 { return }

	if slot := find_slot(p, page_num); slot != nil {
		if slot.page.pin_count > 0 { return }
		(^u32)(raw_data(slot._data_buf[:]))^ = p.first_free_page
		slot.page.dirty = true
		if err := flush_page_unsafe(p, &slot.page); err != nil {
			return
		}
		slot.page.page_num = 0
		slot.page.data = nil
		p.slot_count -= 1
	}
	p.first_free_page = page_num
}
