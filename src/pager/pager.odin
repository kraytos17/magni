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

// Create a new page instance in memory.
//
// Note: This does not read from disk; it allocates the container.
page_new :: proc(page_num: u32, page_size: u32) -> ^Page {
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
page_destroy :: proc(page: ^Page) {
	if page == nil {
		return
	}
	delete(page.data)
	free(page)
}

// Mark page as dirty (modified)
page_mark_dirty :: proc(page: ^Page) {
	if page != nil {
		page.dirty = true
	}
}

// Mark page as clean (synced to disk)
page_mark_clean :: proc(page: ^Page) {
	if page != nil {
		page.dirty = false
	}
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

/*
 Opens the database file at the given path. If the file does not exist,
 it is created.

 Returns:
 - ^Pager: Pointer to the initialized pager.
 - os.Error: Error code if file permissions fail or allocation fails.
 */
pager_open :: proc(path: string) -> (^Pager, os.Error) {
	pager := new(Pager)
	if pager == nil {
		return nil, .Out_Of_Memory
	}

	pager.page_size = types.PAGE_SIZE
	pager.max_cache_pages = 256
	pager.page_cache = make(map[u32]^Page, int(pager.max_cache_pages))
	flags := os.O_RDWR | os.O_CREATE
	file, open_err := os.open(path, flags)
	if open_err != nil {
		delete(pager.page_cache)
		free(pager)
		return nil, open_err
	}

	pager.file = file
	file_size, size_err := os.file_size(file)
	if size_err != nil {
		os.close(file)
		delete(pager.page_cache)
		free(pager)
		return nil, size_err
	}
	pager.file_len = file_size
	return pager, nil
}

/*
 Flushes all dirty pages to disk, syncs the file, closes the file, and frees all memory associated with the cache.
 */
pager_close :: proc(pager: ^Pager) {
	if pager == nil {
		return
	}

	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	pager_flush_all_unsafe(pager)
	if pager.file != nil {
		os.sync(pager.file)
		os.close(pager.file)
		pager.file = nil
	}

	for _, page in pager.page_cache {
		page_destroy(page)
	}

	delete(pager.page_cache)
	free(pager)
}

// Get a page from cache or disk (thread-safe)
// Returns error if page doesn't exist in file yet
pager_get_page :: proc(pager: ^Pager, page_num: u32) -> (^Page, os.Error) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	if page, ok := pager.page_cache[page_num]; ok {
		return page, nil
	}

	num_pages := u32(pager.file_len / i64(pager.page_size))
	if page_num >= num_pages {
		return nil, .Not_Exist
	}
	if page_num > (1 << 30) {
		return nil, .Invalid_Argument
	}

	pager_evict_if_needed_unsafe(pager)
	page := page_new(page_num, pager.page_size)
	if page == nil {
		return nil, .Out_Of_Memory
	}

	offset := i64(page_num) * i64(pager.page_size)
	bytes_read, read_err := os.read_at(pager.file, page.data, offset)
	if read_err != nil {
		page_destroy(page)
		return nil, read_err
	}
	if bytes_read < int(pager.page_size) {
		page_destroy(page)
		return nil, .Unexpected_EOF
	}

	page.dirty = false
	pager.page_cache[page_num] = page
	return page, nil
}

// Allocate a new page at the end of the file and returns the newly allocated page
//
// Note: The new page is marked `dirty` immediately so it will be written to disk
// on the next flush/eviction.
pager_allocate_page :: proc(pager: ^Pager) -> (^Page, os.Error) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	page_num := u32(pager.file_len / i64(pager.page_size))
	if page_num > (1 << 30) {
		return nil, .Invalid_Argument
	}

	pager_evict_if_needed_unsafe(pager)
	page := page_new(page_num, pager.page_size)
	if page == nil {
		return nil, .Out_Of_Memory
	}

	page.dirty = true
	pager.page_cache[page_num] = page
	pager.file_len += i64(pager.page_size)
	return page, nil
}

// Get existing page or allocate new one
//
// NOTE: Only works for sequential allocation - page_num must equal current page count
pager_get_or_allocate_page :: proc(pager: ^Pager, page_num: u32) -> (^Page, os.Error) {
	page, err := pager_get_page(pager, page_num)
	if err == nil {
		return page, nil
	}

	sync.lock(&pager.mutex)
	expected_page_num := u32(pager.file_len / i64(pager.page_size))
	sync.unlock(&pager.mutex)
	if err == .Not_Exist && page_num == expected_page_num {
		return pager_allocate_page(pager)
	}
	return nil, err
}

// Mark a page as dirty (modified)
pager_mark_dirty :: proc(pager: ^Pager, page_num: u32) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	if page, ok := pager.page_cache[page_num]; ok {
		page_mark_dirty(page)
	}
}

// Internal: Flush a specific page to disk.
// Requires caller to hold the mutex.
@(private = "file")
pager_flush_page_unsafe :: proc(pager: ^Pager, page_num: u32) -> os.Error {
	page, found := pager.page_cache[page_num]
	if !found {
		return nil
	}
	if !page.dirty {
		return nil
	}

	offset := i64(page_num) * i64(pager.page_size)
	bytes_written, write_err := os.write_at(pager.file, page.data, offset)
	if write_err != nil {
		return write_err
	}
	if bytes_written != len(page.data) {
		return .Short_Write
	}
	page_mark_clean(page)
	return nil
}

// Flush all dirty pages to disk
pager_flush_all :: proc(pager: ^Pager) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	pager_flush_all_unsafe(pager)
}

// Internal: Iterates all cached pages and flushes dirty ones.
@(private = "file")
pager_flush_all_unsafe :: proc(pager: ^Pager) {
	for page_num, page in pager.page_cache {
		if page.dirty {
			if err := pager_flush_page_unsafe(pager, page_num); err != nil {
				fmt.eprintf("Warning: failed to flush page %d: %v\n", page_num, err)
			}
		}
	}
}

// Flush a specific page to disk
pager_flush_page :: proc(pager: ^Pager, page_num: u32) -> os.Error {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	return pager_flush_page_unsafe(pager, page_num)
}

// Sync all data to disk (flush + fsync)
pager_sync :: proc(pager: ^Pager) -> os.Error {
	pager_flush_all(pager)

	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	if pager.file != nil {
		return os.sync(pager.file)
	}
	return nil
}

// Evict a clean page from cache if needed (UNSAFE - caller must hold mutex)
@(private = "file")
pager_evict_if_needed_unsafe :: proc(pager: ^Pager) {
	if len(pager.page_cache) < int(pager.max_cache_pages) {
		return
	}

	for page_num, page in pager.page_cache {
		if !page.dirty && !page.pinned {
			page_destroy(page)
			delete_key(&pager.page_cache, page_num)
			return
		}
	}

	pager_flush_all_unsafe(pager)
	for page_num, page in pager.page_cache {
		if !page.dirty && !page.pinned {
			page_destroy(page)
			delete_key(&pager.page_cache, page_num)
			return
		}
	}
	fmt.eprintln("Warning: All cached pages are pinned, cannot evict")
}

// Get current page count
pager_page_count :: proc(pager: ^Pager) -> u32 {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	return u32(pager.file_len / i64(pager.page_size))
}

// Pin a page (prevent eviction)
pager_pin_page :: proc(pager: ^Pager, page_num: u32) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	if page, ok := pager.page_cache[page_num]; ok {
		page.pinned = true
	}
}

// Unpin a page (allow eviction)
pager_unpin_page :: proc(pager: ^Pager, page_num: u32) {
	sync.lock(&pager.mutex)
	defer sync.unlock(&pager.mutex)

	if page, ok := pager.page_cache[page_num]; ok {
		page.pinned = false
	}
}
