package db

import "core:fmt"
import "core:sync"
import "src:pager"
import "src:snapshot"

snapshot_diff :: proc(db: ^Database, older_id: u64, newer_id: u64) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_shared_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	if db.latest_snapshot == 0 {
		return .Snapshot_Not_Found
	}

	entries, ok := snapshot.diff_snapshots(db.pager, older_id, newer_id, db.latest_snapshot)
	if !ok {
		return .Snapshot_Failed
	}
	defer {
		for e in entries { delete(e.table_name) }
		delete(entries)
	}
	if len(entries) == 0 {
		fmt.printf("No changes between snapshots %d and %d.\n", older_id, newer_id)
		return .None
	}

	fmt.printf("Snapshot diff: %d → %d\n", older_id, newer_id)
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
	return .None
}

snapshot_tag :: proc(db: ^Database, snapshot_id: u64, tag: string) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu); defer sync.rw_mutex_unlock(&db.mu)
	page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		return .Snapshot_Not_Found
	}
	snapshot.set_tag(db.pager, page, tag)
	return .None
}

snapshot_restore :: proc(db: ^Database, snapshot_id: u64) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu); defer sync.rw_mutex_unlock(&db.mu)
	if db.latest_snapshot == 0 {
		return .Snapshot_Not_Found
	}

	snap_page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		return .Snapshot_Not_Found
	}

	snap_h, snap_ok := snapshot.load(db.pager, snap_page, snapshot_id)
	if !snap_ok {
		return .Snapshot_Failed
	}

	current_id, _ := snapshot.get_ref(db.pager, db.refs_page, snapshot.MAIN_REF)
	if current_id != 0 && current_id != snapshot_id {
		snapshot.log_push(db.pager, db.refs_page, current_id)
	}

	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snapshot_id, .BRANCH, false)
	db.latest_snapshot = snap_page
	db.schema_root_page = snap_h.schema_root
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Restored to snapshot %d (schema root %d)\n", snapshot_id, snap_h.schema_root)
	return .None
}

rollforward :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu); defer sync.rw_mutex_unlock(&db.mu)
	if db.latest_snapshot == 0 {
		return .Snapshot_Not_Found
	}

	current_id, found := snapshot.get_ref(db.pager, db.refs_page, snapshot.MAIN_REF)
	if !found {
		return .No_Ref
	}

	prev_id, popped := snapshot.log_pop(db.pager, db.refs_page)
	if !popped {
		return .Nothing_To_Roll
	}
	if prev_id == current_id {
		return .Nothing_To_Roll
	}

	target_page, has_page := db.snapshot_index[prev_id]
	if !has_page {
		return .Snapshot_Expired
	}

	target_h, load_ok := snapshot.load(db.pager, target_page, prev_id)
	if !load_ok {
		return .Snapshot_Failed
	}

	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, prev_id, .BRANCH, false)
	db.latest_snapshot = target_page
	db.schema_root_page = target_h.schema_root
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Rolled forward to snapshot %d (schema root %d)\n", prev_id, target_h.schema_root)
	return .None
}

expire_snapshots :: proc(db: ^Database, keep_count: int) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu); defer sync.rw_mutex_unlock(&db.mu)
	return expire_snapshots_impl(db, keep_count)
}

expire_snapshots_impl :: proc(db: ^Database, keep_count: int) -> DB_Error {
	if db.latest_snapshot == 0 { return .None }

	expired_ids := snapshot.expire_snapshots(db.pager, db.latest_snapshot, keep_count)
	for id in expired_ids {
		delete_key(&db.snapshot_index, id)
	}

	snapshot.expire_and_collect(db.pager, db.latest_snapshot, keep_count)
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
	fmt.printf("Expired snapshots older than last %d, garbage collected\n", keep_count)
	return .None
}
