package pager

import "core:fmt"
import "core:mem"
import os "core:os/os2"
import "core:sync"
import "src:types"

/*
 Page Structure

 Represents a single fixed-size block of memory (typically 4KB) that mirrors
 a block in the database file.

 Fields:
 - data:     The raw byte buffer containing the page content.
 - dirty:    True if the page has been modified in memory and differs from disk. Dirty pages must be flushed back to disk before eviction.
 - pinned:   True if the page is currently being used by an operation. Pinned pages are immune to eviction.
 - page_num: The index of this page in the database file (0-indexed).
 */
Page :: struct {
	data:     []u8,
	dirty:    bool,
	pinned:   bool,
	page_num: u32,
}

/*
 Pager

 The Pager acts as the intermediary between the persistent storage (Disk) and
 the volatile memory (RAM). It manages a cache of pages to minimize expensive
 file I/O operations.

 Responsibilities:
 1. Reading pages from the file handle into memory.
 2. Writing dirty pages from memory back to the file handle.
 3. Managing a fixed-size cache (evicting old pages to make room for new ones).
 4. Ensuring thread safety for page access.

 Edge Case:
 This Pager assumes a dense file structure. Page allocation is purely sequential
 (appending to the end of the file). Sparse files or "holes" in page numbering
 are not supported.
 */
Pager :: struct {
	file:            ^os.File,
	file_len:        i64,
	page_cache:      map[u32]^Page,
	page_size:       u32,
	max_cache_pages: u32,
	mutex:           sync.Mutex,
}

// Create a new page instance in memory (does not read from disk).
@(private = "file")
create_page :: proc(page_num: u32, page_size: u32) -> ^Page {
	page := new(Page)
	if page == nil {
		return nil
	}

	page.data = make([]u8, page_size)
	if page.data == nil {
		free(page)
		return nil
	}

	mem.zero_slice(page.data)
	page.dirty = false
	page.pinned = false
	page.page_num = page_num
	return page
}

// Free page memory
@(private = "file")
destroy_page :: proc(page: ^Page) {
	if page == nil {
		return
	}
	delete(page.data)
	free(page)
}

/*
 Opens the database file at the given path. If the file does not exist,
 it is created.

 Returns:
 - ^Pager: Pointer to the initialized pager.
 - os.Error: Error code if file permissions fail or allocation fails.
 */
open :: proc(path: string) -> (^Pager, os.Error) {
	p := new(Pager)
	if p == nil {
		return nil, .Out_Of_Memory
	}

	p.page_size = types.PAGE_SIZE
	p.max_cache_pages = 256
	p.page_cache = make(map[u32]^Page, int(p.max_cache_pages))

	flags := os.O_RDWR | os.O_CREATE
	file, open_err := os.open(path, flags)
	if open_err != nil {
		delete(p.page_cache)
		free(p)
		return nil, open_err
	}

	p.file = file
	file_size, size_err := os.file_size(file)
	if size_err != nil {
		os.close(file)
		delete(p.page_cache)
		free(p)
		return nil, size_err
	}

	p.file_len = file_size
	return p, nil
}

/*
 Flushes all dirty pages to disk, syncs the file, closes the file, and frees all memory associated with the cache.
 */
close :: proc(p: ^Pager) {
	if p == nil {
		return
	}

	sync.lock(&p.mutex)
	assert_no_pinned_pages(p)

	file := p.file
	p.file = nil
	pages := p.page_cache
	p.page_cache = nil
	page_size := p.page_size

	sync.unlock(&p.mutex)

	if file != nil {
		for _, page in pages {
			if page.dirty {
				offset := i64(page.page_num) * i64(page_size)
				_, err := os.write_at(file, page.data, offset)
				if err != nil {
					panic(fmt.tprintf("Pager close write failed page=%d err=%v", page.page_num, err))
				}
			}
		}
		os.sync(file)
		os.close(file)
	}

	for _, page in pages {
		destroy_page(page)
	}
	delete(pages)
	free(p)
}

@(private = "file")
assert_no_pinned_pages :: proc(p: ^Pager) {
	for page_num, page in p.page_cache {
		if page.pinned {
			panic(fmt.tprintf("Pager close with pinned page %d", page_num))
		}
	}
}

// Get a page from cache or disk (thread-safe)
// Returns error if page doesn't exist in file yet
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, os.Error) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if page, ok := p.page_cache[page_num]; ok {
		return page, nil
	}

	num_pages := u32(p.file_len / i64(p.page_size))
	if page_num >= num_pages {
		return nil, .Not_Exist
	}
	if page_num > (1 << 30) {
		return nil, .Invalid_Argument
	}

	evict_if_needed_unsafe(p)
	page := create_page(page_num, p.page_size)
	if page == nil {
		return nil, .Out_Of_Memory
	}

	offset := i64(page_num) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, page.data, offset)
	if read_err != nil {
		destroy_page(page)
		return nil, read_err
	}
	if bytes_read < int(p.page_size) {
		destroy_page(page)
		return nil, .Unexpected_EOF
	}

	page.dirty = false
	p.page_cache[page_num] = page
	return page, nil
}

// Allocate a new page at the end of the file and returns the newly allocated page
//
// Note: The new page is marked `dirty` immediately so it will be written to disk
// on the next flush/eviction.
allocate_page :: proc(p: ^Pager) -> (^Page, os.Error) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	page_num := u32(p.file_len / i64(p.page_size))
	if page_num > (1 << 30) {
		return nil, .Invalid_Argument
	}

	evict_if_needed_unsafe(p)
	page := create_page(page_num, p.page_size)
	if page == nil {
		return nil, .Out_Of_Memory
	}

	page.dirty = true
	p.page_cache[page_num] = page
	p.file_len += i64(p.page_size)
	return page, nil
}

// Get existing page or allocate new one
get_or_allocate_page :: proc(p: ^Pager, page_num: u32) -> (^Page, os.Error) {
	page, err := get_page(p, page_num)
	if err == nil {
		return page, nil
	}
	if err == .Not_Exist {
		if page_num == page_count(p) {
			return allocate_page(p)
		}
	}
	return nil, err
}

// Mark a page as dirty (modified)
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

pin_page :: proc(p: ^Pager, page_num: u32) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)
	if page, ok := p.page_cache[page_num]; ok {
		page.pinned = true
	}
}

unpin_page :: proc(p: ^Pager, page_num: u32) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)
	if page, ok := p.page_cache[page_num]; ok {
		page.pinned = false
	}
}

// Flushes a single page to disk.
flush_page :: proc(p: ^Pager, page_num: u32) -> os.Error {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)
	return flush_page_unsafe(p, page_num)
}

// Flushes all dirty pages.
flush_all :: proc(p: ^Pager) {
	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if !flush_all_strict(p) {
		panic("flush_all failed")
	}
}

shutdown :: proc(p: ^Pager) {
	if p == nil {
		return
	}
	if p.file == nil {
		return
	}
	close(p)
}

// Flushes and fsyncs the file.
sync_file :: proc(p: ^Pager) -> os.Error {
	flush_all(p)

	sync.lock(&p.mutex)
	defer sync.unlock(&p.mutex)

	if p.file != nil {
		return os.sync(p.file)
	}
	return nil
}

@(private = "file")
flush_page_unsafe :: proc(p: ^Pager, page_num: u32) -> os.Error {
	page, found := p.page_cache[page_num]
	if !found || !page.dirty {
		return nil
	}

	offset := i64(page_num) * i64(p.page_size)
	bytes_written, write_err := os.write_at(p.file, page.data, offset)
	if write_err != nil {
		return write_err
	}
	if bytes_written != len(page.data) {
		return .Short_Write
	}
	page.dirty = false
	return nil
}

@(private = "file")
flush_all_strict :: proc(p: ^Pager) -> bool {
	for page_num, page in p.page_cache {
		if page.dirty {
			if err := flush_page_unsafe(p, page_num); err != nil {
				fmt.eprintf("FATAL: flush failed page=%d err=%v\n", page_num, err)
				return false
			}
		}
	}
	return true
}

@(private = "file")
evict_page_unsafe :: proc(p: ^Pager, page_num: u32, page: ^Page) {
	if page.pinned {
		return
	}
	if page.dirty {
		panic("Evicting dirty page without flush")
	}
	destroy_page(page)
	delete_key(&p.page_cache, page_num)
}

@(private = "file")
evict_if_needed_unsafe :: proc(p: ^Pager) {
	if len(p.page_cache) < int(p.max_cache_pages) {
		return
	}

	for page_num, page in p.page_cache {
		if !page.pinned && !page.dirty {
			evict_page_unsafe(p, page_num, page)
			return
		}
	}

	if !flush_all_strict(p) {
		panic("Pager eviction failed: flush error")
	}

	for page_num, page in p.page_cache {
		if !page.pinned && !page.dirty {
			evict_page_unsafe(p, page_num, page)
			return
		}
	}
	panic("All pages pinned; eviction impossible")
}
