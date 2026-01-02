package pager

import "core:mem"
import os "core:os/os2"
import "core:sync"
import "src:types"

/*
  Page Structure
*/
Page :: struct {
	data:      []u8,
	page_num:  u32,
	dirty:     bool,
	pin_count: int,
}

Pager :: struct {
	file:            ^os.File,
	file_len:        i64,
	page_size:       u32,
	page_cache:      map[u32]^Page,
	max_cache_pages: u32,
	mutex:           sync.Mutex,
	allocator:       mem.Allocator,
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

open :: proc(path: string, max_pages: u32 = 256, allocator := context.allocator) -> (^Pager, Error) {
	p := new(Pager, allocator)
	if p == nil { return nil, .Out_Of_Memory }

	p.allocator = allocator
	p.page_size = types.PAGE_SIZE
	p.max_cache_pages = max_pages
	p.page_cache = make(map[u32]^Page, int(max_pages), allocator)

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
	p.file_len = file_size
	return p, .None
}

close :: proc(p: ^Pager) {
	if p == nil { return }

	flush_all(p)
	if p.file != nil {
		os.close(p.file)
	}

	for _, page in p.page_cache {
		delete(page.data, p.allocator)
		free(page, p.allocator)
	}
	delete(p.page_cache)
	free(p, p.allocator)
}

// Retrieves an existing page from cache or disk
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	if page_num < 1 { return nil, .Invalid_Page_Num }

	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if page, ok := p.page_cache[page_num]; ok {
		page.pin_count += 1
		return page, .None
	}

	max_page := u32(p.file_len / i64(p.page_size))
	if page_num > max_page {
		return nil, .Page_Not_Found
	}

	page, err := alloc_free_slot(p)
	if err != .None { return nil, err }

	offset := i64(page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, page.data, offset)
	if read_err != nil || bytes_read < int(p.page_size) {
		delete(page.data, p.allocator)
		free(page, p.allocator)
		return nil, .IO_Error
	}

	page.page_num = page_num
	page.pin_count = 1
	page.dirty = false
	p.page_cache[page_num] = page
	return page, .None
}

// Creates a new page at the end of the file
allocate_page :: proc(p: ^Pager) -> (^Page, Error) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	page, err := alloc_free_slot(p)
	if err != .None { return nil, err }

	new_page_num := u32(p.file_len / i64(p.page_size)) + 1
	mem.set(raw_data(page.data), 0, int(p.page_size))
	page.page_num = new_page_num
	page.pin_count = 1
	page.dirty = true
	p.page_cache[new_page_num] = page
	p.file_len += i64(p.page_size)
	return page, .None
}

// Helper for algorithms that might need to get OR create (like root page init)
get_or_allocate_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	page, err := get_page(p, page_num)
	if err == .None { return page, .None }

	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if existing, ok := p.page_cache[page_num]; ok {
		existing.pin_count += 1
		return existing, .None
	}

	current_max := u32(p.file_len / i64(p.page_size))
	if page_num == current_max + 1 {
		sync.unlock(&p.mutex)
		return allocate_page(p)
	}
	return nil, .Page_Not_Found
}

unpin_page :: proc(p: ^Pager, page_num: u32) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if page, ok := p.page_cache[page_num]; ok {
		if page.pin_count > 0 {
			page.pin_count -= 1
		}
	}
}

flush_all :: proc(p: ^Pager) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	for _, page in p.page_cache {
		if page.dirty {
			flush_page_unsafe(p, page)
		}
	}
	os.sync(p.file)
}

mark_dirty :: proc(p: ^Pager, page_num: u32) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)
	if page, ok := p.page_cache[page_num]; ok {
		page.dirty = true
	}
}

page_count :: proc(p: ^Pager) -> u32 {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)
	return u32(p.file_len / i64(p.page_size))
}

// Finds a free memory slot. Evicts if cache is full.
// Returns a Page struct with allocated data buffer, NOT yet in the map.
@(private = "file")
alloc_free_slot :: proc(p: ^Pager) -> (^Page, Error) {
	if len(p.page_cache) >= int(p.max_cache_pages) {
		if err := evict_one_page(p); err != .None {
			return nil, err
		}
	}

	page := new(Page, p.allocator)
	if page == nil { return nil, .Out_Of_Memory }

	page.data = make([]u8, p.page_size, p.allocator)
	if page.data == nil {
		free(page, p.allocator)
		return nil, .Out_Of_Memory
	}
	return page, .None
}

// Finds one unpinned page and removes it from cache (flushing if dirty).
@(private = "file")
evict_one_page :: proc(p: ^Pager) -> Error {
	for id, page in p.page_cache {
		if page.pin_count == 0 {
			if page.dirty {
				if err := flush_page_unsafe(p, page); err != .None {
					return err
				}
			}
			delete(page.data, p.allocator)
			free(page, p.allocator)
			delete_key(&p.page_cache, id)
			return .None
		}
	}
	return .Cache_Full
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
