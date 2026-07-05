package tests

import "core:fmt"
import "core:os"
import "core:testing"
import "src:pager"
import "src:snapshot"
import "src:types"

setup_snapshot_env :: proc(t: ^testing.T, test_name: string) -> ^pager.Pager {
	filename := fmt.tprintf("test_snap_%s.db", test_name)
	if os.exists(filename) {
		os.remove(filename)
	}
	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}

	p, err := pager.open(filename)
	testing.expect(t, err == .None, "Failed to open pager")
	return p
}

teardown_snapshot_env :: proc(p: ^pager.Pager, test_name: string) {
	filename := fmt.tprintf("test_snap_%s.db", test_name)
	pager.close(p)
	if os.exists(filename) {
		os.remove(filename)
	}
	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
}

@(test)
test_snapshot_create_and_load :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "create_load")
	defer teardown_snapshot_env(p, "create_load")

	page, ok := snapshot.create(p, 1, 0, 42)
	testing.expect(t, ok, "Create snapshot 1 failed")
	testing.expect(t, page > 0, "Snapshot page should be > 0")

	h, ok2 := snapshot.load(p, page)
	testing.expect(t, ok2, "Load snapshot failed")
	testing.expect_value(t, h.snapshot_id, u64(1))
	testing.expect_value(t, h.prev_snapshot, u32(0))
	testing.expect_value(t, h.schema_root, u32(42))
	testing.expect_value(t, h.manifest_page, u32(0))
	testing.expect_value(
		t,
		snapshot.Snapshot_Operation(h.operation),
		snapshot.Snapshot_Operation.UNKNOWN,
	)
}

@(test)
test_snapshot_chain_and_find_by_id :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "chain")
	defer teardown_snapshot_env(p, "chain")

	p1, ok1 := snapshot.create(p, 10, 0, 100)
	testing.expect(t, ok1, "Create snapshot 10 failed")

	p2, ok2 := snapshot.create(p, 20, p1, 200)
	testing.expect(t, ok2, "Create snapshot 20 failed")

	p3, ok3 := snapshot.create(p, 30, p2, 300)
	testing.expect(t, ok3, "Create snapshot 30 failed")

	h, found := snapshot.find_by_id(p, p3, 10)
	testing.expect(t, found, "find_by_id 10 should find it")
	testing.expect_value(t, h.snapshot_id, u64(10))
	testing.expect_value(t, h.schema_root, u32(100))

	h2, found2 := snapshot.find_by_id(p, p3, 20)
	testing.expect(t, found2, "find_by_id 20 should find it")
	testing.expect_value(t, h2.snapshot_id, u64(20))
	testing.expect_value(t, h2.schema_root, u32(200))

	h3, found3 := snapshot.find_by_id(p, p3, 30)
	testing.expect(t, found3, "find_by_id 30 should find it")
	testing.expect_value(t, h3.snapshot_id, u64(30))
	testing.expect_value(t, h3.schema_root, u32(300))

	_, not_found := snapshot.find_by_id(p, p3, 99)
	testing.expect(t, !not_found, "find_by_id 99 should not find it")
}

@(test)
test_snapshot_find_from_middle :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "find_mid")
	defer teardown_snapshot_env(p, "find_mid")

	p1, _ := snapshot.create(p, 1, 0, 10)
	p2, _ := snapshot.create(p, 2, p1, 20)
	_, _ = snapshot.create(p, 3, p2, 30)

	h, found := snapshot.find_by_id(p, p2, 1)
	testing.expect(t, found, "find_by_id from middle should find earlier")
	testing.expect_value(t, h.snapshot_id, u64(1))

	_, found2 := snapshot.find_by_id(p, p2, 3)
	// With packed format, p2 = p1 (same page), so snap 3 is on the same page and found.
	testing.expect(t, found2, "find_by_id from same page finds snap 3")
}

@(test)
test_snapshot_count_committed :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "count")
	defer teardown_snapshot_env(p, "count")

	p1, _ := snapshot.create(p, 1, 0, 10)
	p2, _ := snapshot.create(p, 2, p1, 20)
	p3, _ := snapshot.create(p, 3, p2, 30)

	c := snapshot.count_committed(p, p3)
	testing.expect_value(t, c, 3)

	c2 := snapshot.count_committed(p, p2)
	// With packed format, p1=p2=p3 (same page), so count is all 3.
	c3 := snapshot.count_committed(p, p1)
	testing.expect_value(t, c2, 3)
	testing.expect_value(t, c3, 3)
}

@(test)
test_snapshot_prune :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "prune")
	defer teardown_snapshot_env(p, "prune")

	p1, _ := snapshot.create(p, 1, 0, 10)
	p2, _ := snapshot.create(p, 2, p1, 20)
	p3, _ := snapshot.create(p, 3, p2, 30)
	p4, _ := snapshot.create(p, 4, p3, 40)

	snapshot.prune(p, p4, 2)
	c := snapshot.count_committed(p, p4)
	testing.expect_value(t, c, 2)
}

@(test)
test_snapshot_manifest :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "manifest")
	defer teardown_snapshot_env(p, "manifest")

	tables := []types.Table {
		{name = "users", root_page = 100},
		{name = "orders", root_page = 200},
		{name = "products", root_page = 300},
	}

	man_page := snapshot.create_manifest(p, tables)
	testing.expect(t, man_page > 0, "Manifest page should be created")

	root, found := snapshot.find_in_manifest(p, man_page, "users")
	testing.expect(t, found, "users should be in manifest")
	testing.expect_value(t, root, u32(100))

	root2, found2 := snapshot.find_in_manifest(p, man_page, "orders")
	testing.expect(t, found2, "orders should be in manifest")
	testing.expect_value(t, root2, u32(200))

	_, not_found := snapshot.find_in_manifest(p, man_page, "nonexistent")
	testing.expect(t, !not_found, "nonexistent should not be in manifest")

	snap_page, snap_ok := snapshot.create(p, 1, 0, 42, man_page, .INSERT)
	testing.expect(t, snap_ok, "Create snapshot with manifest failed")

	h, load_ok := snapshot.load(p, snap_page)
	testing.expect(t, load_ok, "Load snapshot with manifest failed")
	testing.expect_value(t, h.manifest_page, man_page)
	testing.expect_value(
		t,
		snapshot.Snapshot_Operation(h.operation),
		snapshot.Snapshot_Operation.INSERT,
	)

	found_root, manifest_found := snapshot.find_in_manifest(p, h.manifest_page, "users")
	testing.expect(t, manifest_found, "users should be findable from snapshot manifest")
	testing.expect_value(t, found_root, u32(100))
}

@(test)
test_snapshot_manifest_empty :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "manifest_empty")
	defer teardown_snapshot_env(p, "manifest_empty")

	no_tables: []types.Table = nil
	man_page := snapshot.create_manifest(p, no_tables)
	testing.expect_value(t, man_page, u32(0))

	_, found := snapshot.find_in_manifest(p, 0, "anything")
	testing.expect(t, !found, "find_in_manifest on page 0 should fail")
}

@(test)
test_snapshot_diff :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "diff")
	defer teardown_snapshot_env(p, "diff")

	tables_a := []types.Table {
		{name = "users", root_page = 100},
		{name = "orders", root_page = 200},
	}

	man_a := snapshot.create_manifest(p, tables_a)
	testing.expect(t, man_a > 0, "manifest A created")
	tables_b := []types.Table {
		{name = "users", root_page = 100}, // unchanged
		{name = "orders", root_page = 300}, // modified
		{name = "products", root_page = 400}, // created
	}

	man_b := snapshot.create_manifest(p, tables_b)
	testing.expect(t, man_b > 0, "manifest B created")

	// Diff A → B
	entries, ok := snapshot.diff_manifests(p, man_a, man_b)
	defer snapshot.diff_entries_free(entries)
	testing.expect(t, ok, "diff_manifests A→B should succeed")
	testing.expect_value(t, len(entries), 2)

	has_orders_mod := false
	has_products_cre := false
	for e in entries {
		if e.table_name == "orders" &&
		   e.change == .MODIFIED &&
		   e.old_root == 200 &&
		   e.new_root == 300 {
			has_orders_mod = true
		}
		if e.table_name == "products" &&
		   e.change == .CREATED &&
		   e.old_root == 0 &&
		   e.new_root == 400 {
			has_products_cre = true
		}
	}

	testing.expect(t, has_orders_mod, "orders should be MODIFIED (200→300)")
	testing.expect(t, has_products_cre, "products should be CREATED")
	// Diff B → A (reverse) — should show orders MODIFIED, products DROPPED
	entries_rev, ok_rev := snapshot.diff_manifests(p, man_b, man_a)
	defer snapshot.diff_entries_free(entries_rev)

	testing.expect(t, ok_rev, "diff_manifests B→A should succeed")
	testing.expect_value(t, len(entries_rev), 2)

	has_orders_mod_rev := false
	has_products_drop := false
	for e in entries_rev {
		if e.table_name == "orders" &&
		   e.change == .MODIFIED &&
		   e.old_root == 300 &&
		   e.new_root == 200 {
			has_orders_mod_rev = true
		}
		if e.table_name == "products" &&
		   e.change == .DROPPED &&
		   e.old_root == 400 &&
		   e.new_root == 0 {
			has_products_drop = true
		}
	}

	testing.expect(t, has_orders_mod_rev, "orders should be MODIFIED (300→200) in reverse")
	testing.expect(t, has_products_drop, "products should be DROPPED in reverse")
	// Diff same manifest — should be empty
	same_diff, ok_same := snapshot.diff_manifests(p, man_a, man_a)
	defer snapshot.diff_entries_free(same_diff)

	testing.expect(t, ok_same, "diff same manifest should succeed")
	testing.expect_value(t, len(same_diff), 0)
}

@(test)
test_snapshot_diff_snapshots :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "diff_snap")
	defer teardown_snapshot_env(p, "diff_snap")

	tables_a := []types.Table{{name = "users", root_page = 100}}
	man_a := snapshot.create_manifest(p, tables_a)
	testing.expect(t, man_a > 0, "manifest A created")

	tables_b := []types.Table {
		{name = "users", root_page = 200}, // modified
		{name = "orders", root_page = 300}, // created
	}

	man_b := snapshot.create_manifest(p, tables_b)
	testing.expect(t, man_b > 0, "manifest B created")

	s1, ok1 := snapshot.create(p, 1, 0, 100, man_a, .CREATE)
	testing.expect(t, ok1, "snapshot 1 created")
	s2, ok2 := snapshot.create(p, 2, s1, 200, man_b, .INSERT)
	testing.expect(t, ok2, "snapshot 2 created")

	entries, ok := snapshot.diff_snapshots(p, 1, 2, s2)
	defer snapshot.diff_entries_free(entries)

	testing.expect(t, ok, "diff_snapshots should succeed")
	testing.expect_value(t, len(entries), 2)

	has_users_mod := false
	has_orders_cre := false
	for e in entries {
		if e.table_name == "users" &&
		   e.change == .MODIFIED &&
		   e.old_root == 100 &&
		   e.new_root == 200 {
			has_users_mod = true
		}
		if e.table_name == "orders" &&
		   e.change == .CREATED &&
		   e.old_root == 0 &&
		   e.new_root == 300 {
			has_orders_cre = true
		}
	}

	testing.expect(t, has_users_mod, "users should be MODIFIED (100→200)")
	testing.expect(t, has_orders_cre, "orders should be CREATED")
	// Auto-swap: passing older > newer should still work
	entries_swap, ok_swap := snapshot.diff_snapshots(p, 2, 1, s2)
	defer snapshot.diff_entries_free(entries_swap)

	testing.expect(t, ok_swap, "diff_snapshots with swapped args should succeed")
	testing.expect_value(t, len(entries_swap), 2)
}

@(test)
test_snapshot_tag :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "tag")
	defer teardown_snapshot_env(p, "tag")

	page, ok := snapshot.create(p, 1, 0, 42)
	testing.expect(t, ok, "create snapshot")

	snapshot.set_tag(p, page, "my-label")
	tag := snapshot.get_tag(p, page)
	testing.expect_value(t, tag, "my-label")

	// Reload and verify tag persists
	h, ok2 := snapshot.load(p, page)
	testing.expect(t, ok2, "reload")
	_ = h
	tag2 := snapshot.get_tag(p, page)
	testing.expect_value(t, tag2, "my-label")
}

@(test)
test_snapshot_find_by_timestamp :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "find_ts")
	defer teardown_snapshot_env(p, "find_ts")

	p1, _ := snapshot.create(p, 1, 0, 0, 0, .UNKNOWN, 100)
	p2, _ := snapshot.create(p, 2, p1, 0, 0, .UNKNOWN, 200)

	// Find by timestamp between the two
	found, ts_ok := snapshot.find_by_timestamp(p, p2, 150)
	testing.expect(t, ts_ok, "find_by_timestamp should find snapshot at or before ts")
	testing.expect_value(t, found.snapshot_id, u64(1))

	// Find by a very old timestamp (before any snapshot)
	_, not_found := snapshot.find_by_timestamp(p, p2, 0)
	testing.expect(t, !not_found, "find_by_timestamp with ts=0 should fail")
}

@(test)
test_snapshot_gc :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "gc")
	defer teardown_snapshot_env(p, "gc")

	p1, _ := snapshot.create(p, 1, 0, 100)
	p2, _ := snapshot.create(p, 2, p1, 200)
	p3, _ := snapshot.create(p, 3, p2, 300)

	// Keep only the latest 2 snapshots
	snapshot.prune(p, p3, 2)
	snapshot.gc(p, p3, 2)

	// Verify page count didn't crash or go to zero
	c := snapshot.count_committed(p, p3)
	testing.expect_value(t, c, 2)
}

@(test)
test_snapshot_packed_overflow :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "pck_ovf")
	defer teardown_snapshot_env(p, "pck_ovf")

	// Create more than MAX_HEADERS_PER_PAGE snapshots to trigger overflow
	n := snapshot.MAX_HEADERS_PER_PAGE + 5
	first_page: u32
	last_page: u32
	for i in 1 ..= n {
		page, ok := snapshot.create(p, u64(i), last_page, u32(i * 100))
		testing.expect(t, ok, fmt.tprintf("create snap %d", i))
		if i == 1 { first_page = page }
		last_page = page
	}

	// Verify they form a chain of at least 2 pages
	c := snapshot.count_committed(p, last_page)
	testing.expect_value(t, c, n)

	// Verify all IDs are findable
	for i in 1 ..= n {
		h, found := snapshot.find_by_id(p, last_page, u64(i))
		testing.expect(t, found, fmt.tprintf("find snap %d", i))
		testing.expect_value(t, h.schema_root, u32(i * 100))
	}

	// Packed pages: first_page should equal last_page since they overflow
	testing.expect(t, first_page != last_page, "overflow created at least 2 pages")
}

@(test)
test_snapshot_load_with_id :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "load_id")
	defer teardown_snapshot_env(p, "load_id")

	p1, _ := snapshot.create(p, 1, 0, 100)
	p2, _ := snapshot.create(p, 2, p1, 200)
	_, _ = snapshot.create(p, 3, p2, 300)

	// load with explicit ID should return the correct header
	h1, ok1 := snapshot.load(p, p1, 1)
	testing.expect(t, ok1, "load with id=1")
	testing.expect_value(t, h1.snapshot_id, u64(1))
	testing.expect_value(t, h1.schema_root, u32(100))

	h3, ok3 := snapshot.load(p, p1, 3)
	testing.expect(t, ok3, "load with id=3")
	testing.expect_value(t, h3.snapshot_id, u64(3))
	testing.expect_value(t, h3.schema_root, u32(300))

	// load with nonexistent ID should fail
	_, bad := snapshot.load(p, p1, 999)
	testing.expect(t, !bad, "load with nonexistent id fails")
}

@(test)
test_snapshot_tag_on_packed :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "tag_pck")
	defer teardown_snapshot_env(p, "tag_pck")

	p1, _ := snapshot.create(p, 1, 0, 42)
	p2, _ := snapshot.create(p, 2, p1, 84)

	// Set tag on packed page (p1 == p2 with packed format)
	snapshot.set_tag(p, p1, "packed-tag")
	tag := snapshot.get_tag(p, p1)
	testing.expect_value(t, tag, "packed-tag")

	// Tag should be persistent
	tag2 := snapshot.get_tag(p, p2)
	testing.expect_value(t, tag2, "packed-tag")
}

@(test)
test_snapshot_set_header_state :: proc(t: ^testing.T) {
	p := setup_snapshot_env(t, "set_hdr")
	defer teardown_snapshot_env(p, "set_hdr")

	p1, _ := snapshot.create(p, 1, 0, 10)
	p2, _ := snapshot.create(p, 2, p1, 20)
	p3, _ := snapshot.create(p, 3, p2, 30)

	// set_header_state on the middle snapshot in a packed page
	ok := snapshot.set_header_state(p, p2, 2, .ABANDONED)
	testing.expect(t, ok, "set_header_state for snap 2")

	h, found := snapshot.find_by_id(p, p3, 2)
	testing.expect(t, found, "snap 2 still findable")
	testing.expect_value(t, snapshot.Snapshot_State(h.state), snapshot.Snapshot_State.ABANDONED)

	// Other snapshots should still be COMMITTED
	h1, _ := snapshot.find_by_id(p, p3, 1)
	testing.expect_value(t, snapshot.Snapshot_State(h1.state), snapshot.Snapshot_State.COMMITTED)
	h3, _ := snapshot.find_by_id(p, p3, 3)
	testing.expect_value(t, snapshot.Snapshot_State(h3.state), snapshot.Snapshot_State.COMMITTED)

	// set_header_state on nonexistent snapshot should fail
	bad := snapshot.set_header_state(p, p3, 999, .ABANDONED)
	testing.expect(t, !bad, "set_header_state on nonexistent id fails")
}
