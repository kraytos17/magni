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
	db.txn_state = .NONE
	snapshot.prune(db.pager, db.latest_snapshot, 100)
	snapshot.gc(db.pager, db.latest_snapshot, 100)
	fmt.println("COMMIT transaction (snapshot", db.txn_snapshot_id, ")")
	return true
}

rollback_impl :: proc(db: ^Database) -> bool {
	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to roll back")
		return false
	}
	if db.latest_snapshot != 0 {
		snap_h, snap_ok := snapshot.load(db.pager, db.latest_snapshot)
		if snap_ok { db.schema_root_page = snap_h.schema_root }
	}

	db.txn_state = .NONE
	fmt.println("ROLLBACK transaction")
	return true
}

begin :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return begin_impl(db)
}

commit :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return commit_impl(db)
}

rollback :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	return rollback_impl(db)
}
