package tests

import "core:fmt"
import os "core:os/os2"
import "core:testing"
import "src:pager"
import "src:types"

create_test_pager_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	filename := fmt.tprintf("test_pager_%s.db", test_name)
	os.remove(filename)

	p, err := pager.open(filename)
	testing.expect(t, err == nil, "Failed to open pager")
	testing.expect(t, p != nil, "Pager should not be nil")

	return p, filename
}

destroy_test_pager_env :: proc(p: ^pager.Pager, filename: string) {
	pager.close(p)
	os.remove(filename)
}

@(test)
test_pager_open_close :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "open_close")
	defer destroy_test_pager_env(p, file)

	testing.expect(t, p.file_len == 0, "New pager should be empty")
	testing.expect(t, pager.page_count(p) == 0, "Page count should be 0")
}

@(test)
test_pager_allocate_page :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "allocate")
	defer destroy_test_pager_env(p, file)

	page0, err := pager.allocate_page(p)
	testing.expect(t, err == nil, "Failed to allocate page 0")
	testing.expect(t, page0.page_num == 0, "First page should be 0")
	testing.expect(t, p.file_len == i64(types.PAGE_SIZE), "File length should match page size")
	testing.expect(t, pager.page_count(p) == 1, "Page count should be 1")

	page1, err1 := pager.allocate_page(p)
	testing.expect(t, err1 == nil, "Failed to allocate page 1")
	testing.expect(t, page1.page_num == 1, "Second page should be 1")
	testing.expect(t, p.file_len == i64(types.PAGE_SIZE * 2), "File length mismatch")
	testing.expect(t, pager.page_count(p) == 2, "Page count should be 2")
}

@(test)
test_pager_write_and_flush :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "write_flush")
	defer os.remove(file)

	page, _ := pager.allocate_page(p)
	test_data := "Hello, MagniDB!"
	copy(page.data[:], test_data)
	pager.mark_dirty(p, page.page_num)
	pager.close(p)

	p2, err := pager.open(file)
	testing.expect(t, err == nil, "Failed to reopen pager")
	defer pager.close(p2)

	page_read, read_err := pager.get_page(p2, 0)
	testing.expect(t, read_err == nil, "Failed to read page 0")

	read_str := string(page_read.data[:len(test_data)])
	testing.expect(
		t,
		read_str == test_data,
		fmt.tprintf("Data persistence failed. Expected '%s', got '%s'", test_data, read_str),
	)
}

@(test)
test_pager_caching_behavior :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "caching")
	defer destroy_test_pager_env(p, file)

	page_a, _ := pager.allocate_page(p)
	page_a.data[0] = 0xFF
	page_b, err := pager.get_page(p, 0)

	testing.expect(t, err == nil, "Failed to get page")
	testing.expect(t, page_a == page_b, "Cache should return same pointer for same page")
	testing.expect(t, page_b.data[0] == 0xFF, "Data should be consistent in memory")
}

@(test)
test_pager_get_or_allocate :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "get_alloc")
	defer destroy_test_pager_env(p, file)

	page0, err := pager.get_or_allocate_page(p, 0)
	testing.expect(t, err == nil, "Failed to get_or_allocate page 0")
	testing.expect(t, page0.page_num == 0, "Page num mismatch")
	testing.expect(t, pager.page_count(p) == 1, "Page count should be 1")

	page0_again, err2 := pager.get_or_allocate_page(p, 0)
	testing.expect(t, err2 == nil, "Failed to get existing page 0")
	testing.expect(t, page0 == page0_again, "Should return cached page")

	page1, err3 := pager.get_or_allocate_page(p, 1)
	testing.expect(t, err3 == nil, "Failed to get_or_allocate page 1")
	testing.expect(t, page1.page_num == 1, "Page num mismatch")
	testing.expect(t, pager.page_count(p) == 2, "Page count should be 2")

	_, err4 := pager.get_or_allocate_page(p, 5)
	testing.expect(t, err4 == .Not_Exist, "Should fail on non-sequential allocation")
}

@(test)
test_pager_pin_unpin :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "pinning")
	defer destroy_test_pager_env(p, file)

	page, _ := pager.allocate_page(p)
	testing.expect(t, !page.pinned, "Page should initially be unpinned")

	pager.pin_page(p, page.page_num)
	testing.expect(t, page.pinned, "Page should be pinned")

	pager.unpin_page(p, page.page_num)
	testing.expect(t, !page.pinned, "Page should be unpinned")
}

@(test)
test_pager_max_cache_eviction :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "eviction")
	defer destroy_test_pager_env(p, file)

	p.max_cache_pages = 2
	pager.allocate_page(p) // Page 0
	pager.allocate_page(p) // Page 1
	testing.expect(t, len(p.page_cache) == 2, "Cache should be full")

	page2, err := pager.allocate_page(p)
	testing.expect(t, err == nil, "Failed to allocate page 2")
	testing.expect(t, page2.page_num == 2, "Page num mismatch")
	testing.expect(t, len(p.page_cache) <= 2, "Cache size should respect limit")
}
