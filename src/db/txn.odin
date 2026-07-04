package db

import "core:fmt"
import "core:sync"
import "src:pager"
import "src:snapshot"
import "src:types"

begin_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state == .ACTIVE {
		fmt.eprintln("Warning: Transaction already in progress")
		return .Transaction_Error
	}

	db.txn_state = .ACTIVE
	db.txn_start_file_len = u64(db.pager.file_len)
	pager.wal_begin_txn(db.pager)
	fmt.println("BEGIN transaction")
	return .None
}

commit_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to commit")
		return .Transaction_Error
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
		return .Snapshot_Failed
	}

	db.latest_snapshot = snap_page
	db.snapshot_index[snap_id] = snap_page
	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snap_id, .BRANCH, false)
	if err := pager.wal_commit_txn(db.pager); err != .None {
		return .IO_Error
	}

	db.txn_state = .NONE
	fmt.println("COMMIT transaction (snapshot", db.txn_snapshot_id, ")")
	return .None
}

rollback_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to roll back")
		return .Transaction_Error
	}

	pager.wal_abort_txn(db.pager)
	if db.txn_start_file_len < u64(db.pager.file_len) {
		db.pager.file_len = i64(db.txn_start_file_len)
	}
	if db.latest_snapshot != 0 {
		snap_h, snap_ok := snapshot.load(db.pager, db.latest_snapshot)
		if snap_ok { db.schema_root_page = snap_h.schema_root }
	}

	db.txn_state = .NONE
	fmt.println("ROLLBACK transaction")
	return .None
}

begin :: proc(db: ^Database) -> DB_Error {
	if err := db_check(db); err != .None { return err }
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return begin_impl(db)
}

commit :: proc(db: ^Database) -> DB_Error {
	if err := db_check(db); err != .None { return err }
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return commit_impl(db)
}

rollback :: proc(db: ^Database) -> DB_Error {
	if err := db_check(db); err != .None { return err }
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return rollback_impl(db)
}
