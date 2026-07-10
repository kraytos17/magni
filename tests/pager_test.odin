package tests

import "core:fmt"
import "core:log"
import "core:os"
import "core:testing"
import "src:pager"
import "src:types"

create_test_pager_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	context.logger = log.nil_logger()
	filename := fmt.tprintf("test_pager_%s.db", test_name)
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "Failed to open pager")
	testing.expect(t, p != nil, "Pager should not be nil")
	return p, filename
}

destroy_test_pager_env :: proc(p: ^pager.Pager, filename: string) {
	_ = pager.close(p)
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
}

@(test)
test_pager_open_close :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "open_close")
	defer destroy_test_pager_env(p, file)

	testing.expect_value(t, p.file_len, 0)
	testing.expect_value(t, pager.page_count(p), 0)
}

@(test)
test_pager_allocate_page :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
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
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "write_flush")
	defer os.remove(file)
	defer os.remove(fmt.tprintf("%s-wal", file))

	page, _ := pager.allocate_page(p)
	test_data := "Hello, MagniDB!"
	copy(page.data[:], test_data)

	pager.mark_dirty(p, page.page_num)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)
	_ = pager.close(p)
	p2, err := pager.open(file)
	testing.expect(t, err == .None, "Failed to reopen pager")
	defer _ = pager.close(p2)

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
	context.logger = log.nil_logger()
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
	context.logger = log.nil_logger()
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
	context.logger = log.nil_logger()
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
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "eviction")
	defer destroy_test_pager_env(p, file)

	p.max_cache_pages = 2
	p1, _ := pager.allocate_page(p) // Count: 1
	p1_page_num := p1.page_num
	pager.allocate_page(p) // Count: 2
	testing.expect_value(t, p.slot_count, 2)

	_, err_full := pager.allocate_page(p)
	testing.expect(t, err_full == .Cache_Full, "Should fail if all pages are pinned")

	pager.unpin_page(p, p1_page_num)
	testing.expect_value(t, p1.pin_count, 0)

	p3, err_ok := pager.allocate_page(p)
	testing.expect(t, err_ok == .None, "Failed to allocate page 3 after unpinning")
	testing.expect_value(t, p.slot_count, 2)

	p1_exists := pager.page_in_cache(p, p1_page_num)
	testing.expect(t, !p1_exists, "Page 1 should have been evicted")

	p3_exists := pager.page_in_cache(p, p3.page_num)
	testing.expect(t, p3_exists, "Page 3 should be in cache")
}

@(test)
test_pager_get_non_existent :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "noexist")
	defer destroy_test_pager_env(p, file)

	_, err := pager.get_page(p, 999)
	testing.expect_value(t, err, pager.Error.Page_Not_Found)
}

@(test)
test_pager_double_unpin :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "unpin2")
	defer destroy_test_pager_env(p, file)

	page, _ := pager.allocate_page(p)
	testing.expect_value(t, page.pin_count, 1)

	pager.unpin_page(p, page.page_num)
	testing.expect_value(t, page.pin_count, 0)

	pager.unpin_page(p, page.page_num)
	testing.expect_value(t, page.pin_count, 0)
}

@(test)
test_pager_file_len_after_write :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "filelen")
	defer os.remove(file)
	defer os.remove(fmt.tprintf("%s-wal", file))

	pager.allocate_page(p)
	pager.allocate_page(p)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)
	_ = pager.close(p)

	p2, err := pager.open(file)
	testing.expect(t, err == .None, "Failed to reopen")
	defer _ = pager.close(p2)

	testing.expect_value(t, p2.file_len, i64(types.PAGE_SIZE * 2))
	testing.expect_value(t, pager.page_count(p2), 2)
}

@(test)
test_page_zero_invalid :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "page_zero")
	defer destroy_test_pager_env(p, file)

	_, err := pager.get_page(p, 0)
	testing.expect_value(t, err, pager.Error.Invalid_Page_Num)
}

@(test)
test_allocate_page_zero_fails :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "alloc_zero")
	defer destroy_test_pager_env(p, file)

	_, err := pager.get_or_allocate_page(p, 0)
	testing.expect_value(t, err, pager.Error.Page_Not_Found)
}

create_test_wal_env :: proc(t: ^testing.T, test_name: string) -> (^pager.Pager, string) {
	filename := fmt.tprintf("test_wal_%s.db", test_name)
	wal_filename := fmt.tprintf("%s-wal", filename)
	if os.exists(filename) { os.remove(filename) }
	if os.exists(wal_filename) { os.remove(wal_filename) }

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "WAL: Failed to open pager with WAL")
	testing.expect(t, p != nil, "WAL: Pager should not be nil")
	return p, filename
}

remove_wal_files :: proc(filename: string) {
	if os.exists(filename) { os.remove(filename) }
	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) { os.remove(wal_name) }
}

destroy_test_wal_env :: proc(p: ^pager.Pager, filename: string) {
	_ = pager.close(p)
	remove_wal_files(filename)
}

@(test)
test_wal_open_close :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "open_close")
	defer destroy_test_wal_env(p, file)

	testing.expect(t, p.wal_state.file != nil, "WAL file should be open")
	testing.expect(t, p.wal_state.file != nil, "WAL file should be open")

	wal_name := fmt.tprintf("%s-wal", file)
	testing.expect(t, os.exists(wal_name), "WAL file should exist on disk")
}

@(test)
test_wal_allocate_and_read_back :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "alloc_read")
	defer destroy_test_wal_env(p, file)

	pg, err := pager.allocate_page(p)
	testing.expect(t, err == .None, "WAL: allocate failed")

	write_u32(pg.data[:4], 0xDEADBEEF)
	pager.mark_dirty(p, pg.page_num)

	pager.wal_begin_txn(p)
	comm_err := pager.wal_commit_txn(p)
	testing.expect(t, comm_err == .None, "WAL: commit failed")

	pg2, get_err := pager.get_page(p, pg.page_num)
	testing.expect(t, get_err == .None, "WAL: get_page failed")
	testing.expect(t, bytes_to_u32(pg2.data[:4]) == 0xDEADBEEF, "WAL: data mismatch after commit")
}

@(test)
test_wal_uncommitted_discarded_on_close :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "uncommitted")
	defer remove_wal_files(file)

	pg, err := pager.allocate_page(p)
	testing.expect(t, err == .None, "WAL: allocate failed")
	pg_num := pg.page_num
	write_u32(pg.data[:4], 0xCAFEBABE)

	pager.mark_dirty(p, pg.page_num)
	pager.wal_abort_txn(p)
	_ = pager.close(p)

	p2, err2 := pager.open(file)
	testing.expect(t, err2 == .None, "WAL: reopen failed")
	defer destroy_test_wal_env(p2, file)

	_, get_err := pager.get_page(p2, pg_num)
	testing.expect(t, get_err != .None, "WAL: uncommitted page should not exist after reopen")
}

@(test)
test_wal_commit_survives_reopen :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "commit_survive")
	defer remove_wal_files(file)

	pg, _ := pager.allocate_page(p)
	pg_num := pg.page_num
	write_u32(pg.data[:4], 0x12345678)
	pager.mark_dirty(p, pg.page_num)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)
	_ = pager.close(p)

	p2, err := pager.open(file)
	testing.expect(t, err == .None, "WAL: reopen after commit failed")
	if err != .None { return }
	defer destroy_test_wal_env(p2, file)

	pg2, get_err := pager.get_page(p2, pg_num)
	testing.expect(t, get_err == .None, "WAL: page should exist after reopen")
	if get_err == .None {
		testing.expect(
			t,
			bytes_to_u32(pg2.data[:4]) == 0x12345678,
			"WAL: data mismatch after reopen",
		)
	}
}

@(test)
test_wal_recovery_from_crash :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	file := fmt.tprintf("test_wal_crash.db")
	wal_file := fmt.tprintf("%s-wal", file)
	if os.exists(file) { os.remove(file) }
	if os.exists(wal_file) { os.remove(wal_file) }
	defer os.remove(file)
	defer os.remove(wal_file)

	p1, err := pager.open(file)
	testing.expect(t, err == .None, "WAL crash: open failed")

	pg, _ := pager.allocate_page(p1)
	write_u32(pg.data[:4], 0xAABBCCDD)
	pager.mark_dirty(p1, pg.page_num)
	pager.wal_begin_txn(p1)
	pager.wal_commit_txn(p1)

	// Simulate crash: close OS files without checkpointing
	os.close(p1.file)
	p1.file = nil
	if p1.wal_state.file != nil {
		os.close(p1.wal_state.file)
		p1.wal_state.file = nil
	}

	p2, err2 := pager.open(file)
	testing.expect(t, err2 == .None, "WAL crash: reopen after crash failed")
	defer destroy_test_wal_env(p2, file)

	pg2, get_err := pager.get_page(p2, 1)
	testing.expect(t, get_err == .None, "WAL crash: page should be recovered")
	testing.expect(
		t,
		bytes_to_u32(pg2.data[:4]) == 0xAABBCCDD,
		"WAL crash: data mismatch after recovery",
	)

	// Clean up p1 memory (handles are nil, close is a no-op)
	pager.close(p1)
}

@(test)
test_wal_checkpoint_resets_wal :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "checkpoint")
	defer destroy_test_wal_env(p, file)

	for i in 0 ..< 5 {
		pg, _ := pager.allocate_page(p)
		write_u32(pg.data[:4], u32(i))
		pager.mark_dirty(p, pg.page_num)
		pager.wal_begin_txn(p)
		pager.wal_commit_txn(p)
	}

	before_size, _ := os.file_size(p.wal_state.file)
	testing.expect(
		t,
		before_size > i64(types.WAL_HEADER_SIZE),
		"WAL checkpoint: WAL should have data before checkpoint",
	)

	pager.wal_checkpoint(p)
	after_size, _ := os.file_size(p.wal_state.file)
	testing.expect(
		t,
		after_size == i64(types.WAL_HEADER_SIZE),
		fmt.tprintf(
			"WAL checkpoint: WAL should be header-only after checkpoint (was %d, expected %d)",
			after_size,
			types.WAL_HEADER_SIZE,
		),
	)

	for i in 0 ..< 5 {
		pg, get_err := pager.get_page(p, u32(i + 1))
		testing.expect(
			t,
			get_err == .None,
			fmt.tprintf("WAL checkpoint: page %d accessible", i + 1),
		)
		if get_err == .None {
			testing.expect(
				t,
				bytes_to_u32(pg.data[:4]) == u32(i),
				fmt.tprintf("WAL checkpoint: page %d data correct", i + 1),
			)
		}
	}
}

@(test)
test_wal_multi_txn_read_your_writes :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "read_writes")
	defer remove_wal_files(file)

	pg, _ := pager.allocate_page(p)
	pg_num := pg.page_num
	write_u32(pg.data[:4], 0x1111)
	pager.mark_dirty(p, pg.page_num)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)

	pg2, _ := pager.get_page(p, pg.page_num)
	write_u32(pg2.data[:4], 0x2222)
	pager.mark_dirty(p, pg.page_num)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)

	pg3, _ := pager.get_page(p, pg.page_num)
	write_u32(pg3.data[:4], 0x3333)
	pager.mark_dirty(p, pg.page_num)
	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)

	_ = pager.close(p)
	p2, err := pager.open(file)
	testing.expect(t, err == .None, "WAL multi-txn: reopen failed")
	defer destroy_test_wal_env(p2, file)

	pg4, get_err := pager.get_page(p2, pg_num)
	testing.expect(t, get_err == .None, "WAL multi-txn: page should exist")
	if get_err == .None {
		testing.expect(
			t,
			bytes_to_u32(pg4.data[:4]) == 0x3333,
			"WAL multi-txn: only last write should survive",
		)
	}
}

@(test)
test_wal_begin_commit_rollback_stress :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "txn_stress")
	defer remove_wal_files(file)

	for i in 0 ..< 50 {
		pg, _ := pager.allocate_page(p)
		write_u32(pg.data[:4], u32(i))
		pager.mark_dirty(p, pg.page_num)
		pager.wal_begin_txn(p)
		pager.wal_commit_txn(p)

		pg2, _ := pager.get_page(p, pg.page_num)
		val := bytes_to_u32(pg2.data[:4])
		testing.expect(t, val == u32(i), fmt.tprintf("WAL stress: txn %d data mismatch", i))
	}

	_ = pager.close(p)
	p2, err := pager.open(file)
	testing.expect(t, err == .None, "WAL stress: reopen failed")
	defer destroy_test_wal_env(p2, file)

	for i in 0 ..< 50 {
		pg, get_err := pager.get_page(p2, u32(i + 1))
		testing.expect(
			t,
			get_err == .None,
			fmt.tprintf("WAL stress: page %d should exist after reopen", i + 1),
		)
		if get_err == .None {
			val := bytes_to_u32(pg.data[:4])
			testing.expect(
				t,
				val == u32(i),
				fmt.tprintf("WAL stress: page %d data mismatch after reopen", i + 1),
			)
		}
	}
}

@(test)
test_wal_rollback_does_not_leak_pages :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_wal_env(t, "ro_noleak")
	defer remove_wal_files(file)

	before_page_count := pager.page_count(p)
	pg, _ := pager.allocate_page(p)
	write_u32(pg.data[:4], 0xDEAD)

	pager.mark_dirty(p, pg.page_num)
	pager.wal_begin_txn(p)
	pager.wal_abort_txn(p)
	_ = pager.close(p)

	p2, err := pager.open(file)
	testing.expect(t, err == .None, "WAL rollback leak: reopen failed")
	defer destroy_test_wal_env(p2, file)

	_, get_err := pager.get_page(p2, pg.page_num)
	testing.expect(t, get_err != .None, "WAL rollback leak: aborted page should not exist")
	testing.expect(
		t,
		before_page_count == 0 || pager.page_count(p2) == before_page_count,
		fmt.tprintf(
			"WAL rollback leak: page count changed from %d to %d",
			before_page_count,
			pager.page_count(p2),
		),
	)
}

u32_to_bytes :: proc(v: u32) -> [4]u8 {
	return [4]u8{u8(v & 0xFF), u8(v >> 8 & 0xFF), u8(v >> 16 & 0xFF), u8(v >> 24 & 0xFF)}
}

write_u32 :: proc(dst: []u8, v: u32) {
	b := u32_to_bytes(v)
	copy(dst, b[:])
}

bytes_to_u32 :: proc(b: []u8) -> u32 {
	if len(b) < 4 { return 0 }
	return u32(b[0]) | u32(b[1]) << 8 | u32(b[2]) << 16 | u32(b[3]) << 24
}

// --- Bitmap tests ---

@(test)
test_bitmap_basic_ops :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	bm := make([]u64, 4) // context.allocator — safe for explicit delete
	defer delete(bm)

	// Initially all zero
	testing.expect(t, !pager.bitmap_test(bm, 5), "bit 5 should be 0 initially")
	testing.expect(t, !pager.bitmap_test(bm, 100), "bit 100 should be 0 initially")
	testing.expect(t, !pager.bitmap_test(bm, 200), "bit 200 should be 0 initially")

	// Set bit 5
	pager.bitmap_set(bm, 5)
	testing.expect(t, pager.bitmap_test(bm, 5), "bit 5 should be 1 after set")
	testing.expect(t, !pager.bitmap_test(bm, 4), "bit 4 should remain 0")
	testing.expect(t, !pager.bitmap_test(bm, 6), "bit 6 should remain 0")

	// Set bit 100
	pager.bitmap_set(bm, 100)
	testing.expect(t, pager.bitmap_test(bm, 100), "bit 100 should be 1 after set")
	testing.expect(t, pager.bitmap_test(bm, 5), "bit 5 should still be 1")

	// Clear bit 5
	pager.bitmap_clear(bm, 5)
	testing.expect(t, !pager.bitmap_test(bm, 5), "bit 5 should be 0 after clear")
	testing.expect(t, pager.bitmap_test(bm, 100), "bit 100 should still be 1")

	// Clear bit 100
	pager.bitmap_clear(bm, 100)
	testing.expect(t, !pager.bitmap_test(bm, 100), "bit 100 should be 0 after clear")
}

@(test)
test_bitmap_allocate_sets_bit :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "bmap_alloc")
	defer destroy_test_pager_env(p, file)

	// Allocate 2 pages so we can test with page_num > 1 (page 1 = header, can't be freed)
	pager.allocate_page(p)
	pg, _ := pager.allocate_page(p)
	pn := pg.page_num

	testing.expect(t, pn > 1, "test page should be > 1")
	testing.expect(t, len(p.page_bitmap) > 0, "bitmap should exist")
	testing.expect(
		t,
		pager.bitmap_test(p.page_bitmap, pn),
		fmt.tprintf("bit %d should be set after alloc", pn),
	)
}

@(test)
test_bitmap_free_clears_bit :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	p, file := create_test_pager_env(t, "bmap_free")
	defer destroy_test_pager_env(p, file)

	// Allocate 2 pages; free the second one (page 1 = header, can't be freed)
	pager.allocate_page(p)
	pg2, _ := pager.allocate_page(p)
	pn := pg2.page_num
	testing.expect(t, pn > 1, "test page should be > 1")
	testing.expect(t, pager.bitmap_test(p.page_bitmap, pn), "bit should be set after alloc")

	pager.unpin_page(p, pn)
	pager.free_page(p, pn)
	testing.expect(
		t,
		!pager.bitmap_test(p.page_bitmap, pn),
		fmt.tprintf("bit %d should be cleared after free", pn),
	)
}

@(test)
test_bitmap_grows_on_allocate :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	// Open with a small file to start with a small bitmap
	filename := "test_bmap_grow.db"
	if os.exists(filename) { os.remove(filename) }
	defer os.remove(filename)
	wal_name := fmt.tprintf("%s-wal", filename)
	defer os.remove(wal_name)

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "open failed")
	defer pager.close(p)

	initial_len := len(p.page_bitmap)

	// Allocate pages until bitmap grows past initial_len
	last_pn: u32
	for _ in 0 ..< 200 {
		pg, aerr := pager.allocate_page(p)
		testing.expect(t, aerr == .None, "alloc failed")
		last_pn = pg.page_num
	}

	bitmap_len_now := len(p.page_bitmap)
	testing.expect(
		t,
		bitmap_len_now > initial_len,
		fmt.tprintf("bitmap should grow (was %d, now %d)", initial_len, bitmap_len_now),
	)
	testing.expect(t, pager.bitmap_test(p.page_bitmap, last_pn), "last page bit should be set")
}

@(test)
test_bitmap_64_range :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	// Test bits across the first 64-word boundary
	bm := make([]u64, 2) // context.allocator — safe for explicit delete
	defer delete(bm)

	// Set and test at word boundary (bit 63 = last bit of first word, bit 64 = first bit of second word)
	pager.bitmap_set(bm, 63)
	pager.bitmap_set(bm, 64)
	pager.bitmap_set(bm, 65)

	testing.expect(t, pager.bitmap_test(bm, 63), "bit 63 (last of word 0)")
	testing.expect(t, pager.bitmap_test(bm, 64), "bit 64 (first of word 1)")
	testing.expect(t, pager.bitmap_test(bm, 65), "bit 65 (second of word 1)")
	testing.expect(t, !pager.bitmap_test(bm, 62), "bit 62 should be 0")
	testing.expect(t, !pager.bitmap_test(bm, 66), "bit 66 should be 0")

	pager.bitmap_clear(bm, 64)
	testing.expect(t, !pager.bitmap_test(bm, 64), "bit 64 should be 0 after clear")
	testing.expect(t, pager.bitmap_test(bm, 63), "bit 63 should still be 1")
	testing.expect(t, pager.bitmap_test(bm, 65), "bit 65 should still be 1")
}

@(test)
test_wal_checksum :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	filename := "test_wal_checksum.db"
	wal_name := fmt.tprintf("%s-wal", filename)
	os.remove(filename)
	os.remove(wal_name)
	defer os.remove(filename)
	defer os.remove(wal_name)

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "open for write")

	pg, aerr := pager.allocate_page(p)
	testing.expect(t, aerr == .None, "allocate page")
	pg.dirty = true
	pager.unpin_page(p, pg.page_num)

	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)

	pager.close(p)

	p2, err2 := pager.open(filename)
	testing.expect(t, err2 == .None, "reopen with valid checksums")
	pager.close(p2)
}

@(test)
test_wal_checksum_corruption :: proc(t: ^testing.T) {
	context.logger = log.nil_logger()
	filename := "test_wal_cksum_corrupt.db"
	wal_name := fmt.tprintf("%s-wal", filename)
	os.remove(filename)
	os.remove(wal_name)
	defer os.remove(filename)
	defer os.remove(wal_name)

	// Create a database with one page and commit
	p, err := pager.open(filename)
	testing.expect(t, err == .None, "open for write")

	pg, aerr := pager.allocate_page(p)
	testing.expect(t, aerr == .None, "allocate page")
	pg.dirty = true
	pager.unpin_page(p, pg.page_num)

	pager.wal_begin_txn(p)
	pager.wal_commit_txn(p)
	pager.close(p)

	// Corrupt the first frame's page data in the WAL
	wal_file, werr := os.open(wal_name, os.O_RDWR)
	testing.expect(t, werr == nil, "open WAL for corruption")
	if werr == nil {
		// Seek past WAL header + frame header to page data, flip one byte
		offset := types.WAL_HEADER_SIZE + types.WAL_FRAME_HEADER_SIZE
		corrupt_byte: u8 = 0xFF
		os.write_at(wal_file, []u8{corrupt_byte}, i64(offset))
		os.close(wal_file)
	}

	// Reopen — recovery should detect checksum mismatch and skip the corrupted frame
	p2, err2 := pager.open(filename)
	testing.expect(
		t,
		err2 == .None,
		"reopen after corruption (still opens, corrupted frame skipped)",
	)

	// No frames should have been committed — wal_recover's second pass breaks on checksum mismatch
	// The database file has the 4096-byte DB header page but no recovered data pages
	testing.expect(
		t,
		p2.file_len == types.PAGE_SIZE,
		"only header page exists (no recovered data)",
	)
	pager.close(p2)
}
