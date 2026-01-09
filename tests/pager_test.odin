package tests

import "core:fmt"
import os "core:os/os2"
import "core:testing"
import "src:pager"
import "src:types"

create_test_pager_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	filename := fmt.tprintf("test_pager_%s.db", test_name)
	if os.exists(filename) {
		os.remove(filename)
	}

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "Failed to open pager")
	testing.expect(t, p != nil, "Pager should not be nil")
	return p, filename
}

destroy_test_pager_env :: proc(p: ^pager.Pager, filename: string) {
	pager.close(p)
	if os.exists(filename) {
		os.remove(filename)
	}
}

@(test)
test_pager_open_close :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "open_close")
	defer destroy_test_pager_env(p, file)

	testing.expect_value(t, p.file_len, 0)
	testing.expect_value(t, pager.page_count(p), 0)
}

@(test)
test_pager_allocate_page :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "allocate")
	defer destroy_test_pager_env(p, file)

	page1, err := pager.allocate_page(p)
	testing.expect(t, err == .None, "Failed to allocate page 1")
	testing.expect_value(t, page1.page_num, 1)
	testing.expect_value(t, p.file_len, i64(types.PAGE_SIZE))
	testing.expect_value(t, pager.page_count(p), 1)

	page2, err1 := pager.allocate_page(p)
	testing.expect(t, err1 == .None, "Failed to allocate page 2")
	testing.expect_value(t, page2.page_num, 2)
	testing.expect_value(t, p.file_len, i64(types.PAGE_SIZE * 2))
	testing.expect_value(t, pager.page_count(p), 2)
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
	testing.expect(t, err == .None, "Failed to reopen pager")
	defer pager.close(p2)

	page_read, read_err := pager.get_page(p2, 1)
	testing.expect(t, read_err == .None, "Failed to read page 1")
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
	page_b, err := pager.get_page(p, 1)

	testing.expect(t, err == .None, "Failed to get page")
	testing.expect(t, page_a == page_b, "Cache should return same pointer for same page")
	testing.expect_value(t, page_b.data[0], 0xFF)
	testing.expect_value(t, page_a.pin_count, 2)
}

@(test)
test_pager_get_or_allocate :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "get_alloc")
	defer destroy_test_pager_env(p, file)

	page1, err := pager.get_or_allocate_page(p, 1)
	testing.expect(t, err == .None, "Failed to get_or_allocate page 1")
	testing.expect_value(t, page1.page_num, 1)
	testing.expect_value(t, pager.page_count(p), 1)

	page1_again, err2 := pager.get_or_allocate_page(p, 1)
	testing.expect(t, err2 == .None, "Failed to get existing page 1")
	testing.expect(t, page1 == page1_again, "Should return cached page")

	page2, err3 := pager.get_or_allocate_page(p, 2)
	testing.expect(t, err3 == .None, "Failed to get_or_allocate page 2")
	testing.expect_value(t, page2.page_num, 2)
	testing.expect_value(t, pager.page_count(p), 2)

	_, err4 := pager.get_or_allocate_page(p, 5)
	testing.expect(t, err4 == .Page_Not_Found, "Should fail on non-sequential allocation")
}

@(test)
test_pager_pinning_logic :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "pinning")
	defer destroy_test_pager_env(p, file)

	page, _ := pager.allocate_page(p)
	testing.expect_value(t, page.pin_count, 1)

	pager.unpin_page(p, page.page_num)
	testing.expect_value(t, page.pin_count, 0)

	pager.get_page(p, page.page_num)
	testing.expect_value(t, page.pin_count, 1)
}

@(test)
test_pager_max_cache_eviction :: proc(t: ^testing.T) {
	p, file := create_test_pager_env(t, "eviction")
	defer destroy_test_pager_env(p, file)

	p.max_cache_pages = 2
	p1, _ := pager.allocate_page(p) // Count: 1
	pager.allocate_page(p) // Count: 2
	testing.expect_value(t, len(p.page_cache), 2)

	_, err_full := pager.allocate_page(p)
	testing.expect(t, err_full == .Cache_Full, "Should fail if all pages are pinned")

	pager.unpin_page(p, p1.page_num)
	testing.expect_value(t, p1.pin_count, 0)

	p3, err_ok := pager.allocate_page(p)
	testing.expect(t, err_ok == .None, "Failed to allocate page 3 after unpinning")
	testing.expect_value(t, len(p.page_cache), 2)

	_, p1_exists := p.page_cache[p1.page_num]
	testing.expect(t, !p1_exists, "Page 1 should have been evicted")

	_, p3_exists := p.page_cache[p3.page_num]
	testing.expect(t, p3_exists, "Page 3 should be in cache")
}
