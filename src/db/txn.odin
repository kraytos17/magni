package db

import "core:fmt"
import "core:sync"
import "src:pager"
import "src:snapshot"
import "src:types"

begin_impl :: proc(db: ^Database) -> bool {
	if db.txn_state == .ACTIVE {
		fmt.eprintln("Warning: Transaction already in progress")
		return false
	}

	db.txn_state = .ACTIVE
	db.txn_start_file_len = db.pager.file_len
	pager.wal_begin_txn(db.pager)
	fmt.println("BEGIN transaction")
	return true
}

commit_impl :: proc(db: ^Database) -> bool {
	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to commit")
		return false
	}

	db.txn_snapshot_id += 1
	snap_id := db.txn_snapshot_id
	ensure_table_roots(db)
	tables := make([dynamic]types.Table, context.temp_allocator)
	for name, root in db.table_roots {
		append(&tables, types.Table{name = name, root_page = root})
	}

	manifest_page := snapshot.create_manifest(db.pager, tables[:])
	defer if manifest_page != 0 { pager.unpin_page(db.pager, manifest_page) }
	snap_page, snap_ok := snapshot.create(
		db.pager,
		snap_id,
		db.latest_snapshot,
		db.schema_root_page,
		manifest_page,
		.COMMIT,
	)
	if !snap_ok {
		fmt.eprintln("Error: Failed to create snapshot")
		return false
	}

	db.latest_snapshot = snap_page
	db.snapshot_index[snap_id] = snap_page
	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snap_id, .BRANCH, false)
	if err := pager.wal_commit_txn(db.pager); err != .None {
		fmt.eprintln("Error: WAL commit failed:", err)
		return false
	}

	db.txn_state = .NONE
	fmt.println("COMMIT transaction (snapshot", db.txn_snapshot_id, ")")
	return true
}

rollback_impl :: proc(db: ^Database) -> bool {
	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to roll back")
		return false
	}

	pager.wal_abort_txn(db.pager)
	// Reclaim pages allocated during the aborted transaction
	if db.txn_start_file_len < db.pager.file_len {
		db.pager.file_len = db.txn_start_file_len
	}
	if db.latest_snapshot != 0 {
		snap_h, snap_ok := snapshot.load(db.pager, db.latest_snapshot)
		if snap_ok { db.schema_root_page = snap_h.schema_root }
	}

	db.txn_state = .NONE
	fmt.println("ROLLBACK transaction")
	return true
}

// Begin an explicit transaction (acquires db.mu). Fails if a transaction is already active.
begin :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return begin_impl(db)
}

// Commit the active transaction: create a snapshot, update refs, flush WAL.
commit :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return commit_impl(db)
}

// Roll back the active transaction: abort WAL, reclaim pages, restore schema root from
// the last snapshot.
rollback :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return rollback_impl(db)
}
