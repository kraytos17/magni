package db

import "core:fmt"
import "core:sync"
import "src:pager"
import "src:snapshot"

print_snapshots :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	if db.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return
	}
	snapshot.debug_print_chain(db.pager, db.latest_snapshot)
}

snapshot_diff :: proc(db: ^Database, older_id: u64, newer_id: u64) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	if db.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return false
	}

	entries, ok := snapshot.diff_snapshots(db.pager, older_id, newer_id, db.latest_snapshot)
	if !ok {
		fmt.eprintf("Error: failed to diff snapshots %d → %d\n", older_id, newer_id)
		return false
	}
	defer {
		for e in entries { delete(e.table_name) }
		delete(entries)
	}
	if len(entries) == 0 {
		fmt.printf("No changes between snapshots %d and %d.\n", older_id, newer_id)
		return true
	}

	fmt.printf("=== Snapshot Diff: %d → %d ===\n", older_id, newer_id)
	for e in entries {
		switch e.change {
		case .CREATED:
			fmt.printf("  %-20s CREATED  (root %d)\n", e.table_name, e.new_root)
		case .DROPPED:
			fmt.printf("  %-20s DROPPED  (was root %d)\n", e.table_name, e.old_root)
		case .MODIFIED:
			fmt.printf("  %-20s MODIFIED (root %d → %d)\n", e.table_name, e.old_root, e.new_root)
		}
	}
	fmt.printf("(%d table(s) changed)\n", len(entries))
	return true
}

snapshot_tag :: proc(db: ^Database, snapshot_id: u64, tag: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		fmt.eprintln("Error: Snapshot", snapshot_id, "not found")
		return false
	}
	snapshot.set_tag(db.pager, page, tag)
	return true
}

snapshot_restore :: proc(db: ^Database, snapshot_id: u64) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	if db.latest_snapshot == 0 {
		fmt.eprintln("Error: No snapshots")
		return false
	}

	snap_page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		fmt.eprintln("Error: Snapshot", snapshot_id, "not found")
		return false
	}

	snap_h, snap_ok := snapshot.load(db.pager, snap_page)
	if !snap_ok {
		fmt.eprintln("Error: Failed to load snapshot", snapshot_id)
		return false
	}

	db.txn_snapshot_id += 1
	new_id := db.txn_snapshot_id
	new_page, create_ok := snapshot.create(
		db.pager,
		new_id,
		db.latest_snapshot,
		snap_h.schema_root,
		snap_h.manifest_page,
		.RESTORE,
	)
	if !create_ok {
		fmt.eprintln("Error: Failed to create restore snapshot")
		return false
	}
	
	db.snapshot_index[new_id] = new_page
	db.latest_snapshot = new_page
	db.schema_root_page = snap_h.schema_root
	snapshot.prune(db.pager, db.latest_snapshot, 100)
	snapshot.gc(db.pager, db.latest_snapshot, 100)
	pager.flush_all(db.pager)
	update_header(db)
	fmt.printf("Restored to snapshot %d (schema root %d)\n", snapshot_id, snap_h.schema_root)
	return true
}
