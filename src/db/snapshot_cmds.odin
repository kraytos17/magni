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

	// Log current tip before moving ref (for rollforward)
	current_id, _ := snapshot.get_ref(db.pager, db.refs_page, snapshot.MAIN_REF)
	if current_id != 0 && current_id != snapshot_id {
		snapshot.log_push(db.pager, db.refs_page, current_id)
	}
	// Ref-pointer-move: update "main" ref to point at target snapshot
	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snapshot_id, .BRANCH, false)

	db.latest_snapshot = snap_page
	db.schema_root_page = snap_h.schema_root
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Restored to snapshot %d (schema root %d)\n", snapshot_id, snap_h.schema_root)
	return true
}

rollforward :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	if db.latest_snapshot == 0 {
		fmt.eprintln("Error: No snapshots")
		return false
	}

	current_id, found := snapshot.get_ref(db.pager, db.refs_page, snapshot.MAIN_REF)
	if !found {
		fmt.eprintln("Error: No main ref")
		return false
	}

	prev_id, popped := snapshot.log_pop(db.pager, db.refs_page)
	if !popped {
		fmt.eprintln("Nothing to roll forward to")
		return false
	}
	if prev_id == current_id {
		fmt.eprintln("Nothing to roll forward to (already at tip)")
		return false
	}

	target_page, has_page := db.snapshot_index[prev_id]
	if !has_page {
		fmt.eprintln("Error: Cannot roll forward — snapshot", prev_id, "has been expired")
		return false
	}

	target_h, load_ok := snapshot.load(db.pager, target_page)
	if !load_ok {
		fmt.eprintln("Error: Failed to load snapshot", prev_id)
		return false
	}

	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, prev_id, .BRANCH, false)
	db.latest_snapshot = target_page
	db.schema_root_page = target_h.schema_root
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Rolled forward to snapshot %d (schema root %d)\n", prev_id, target_h.schema_root)
	return true
}

expire_snapshots :: proc(db: ^Database, keep_count: int) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return expire_snapshots_impl(db, keep_count)
}

expire_snapshots_impl :: proc(db: ^Database, keep_count: int) -> bool {
	if db.latest_snapshot == 0 { return true }

	expired_ids := snapshot.expire_snapshots(db.pager, db.latest_snapshot, keep_count)
	for id in expired_ids {
		delete_key(&db.snapshot_index, id)
	}

	snapshot.expire_and_collect(db.pager, db.latest_snapshot, keep_count)
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Expired snapshots older than last %d, garbage collected\n", keep_count)
	return true
}
