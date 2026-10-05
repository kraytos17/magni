package db

import "core:log"
import "core:sync"
import "src:executor"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

// begin_impl opens a transaction: records the file length (later rewind
// target) and starts the WAL txn so frame appends route to txn_index.
// .Transaction_Error if one is already active.
@(private)
begin_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state == .Active {
		log.warn("Transaction already in progress")
		return .Transaction_Error
	}

	db.txn_state = .Active
	db.txn_start_file_len = u64(db.pager.file_len)
	pager.wal_begin_txn(db.pager)
	log.info("BEGIN transaction")
	return .None
}

// commit_impl closes an active transaction in four steps: flush staged
// roots into the catalog (one schema COW per dirty table/index, so DDL
// published mid-txn survives), build a manifest and snapshot, record MAIN,
// then WAL-commit (the durability point). Any failure returns before
// wal_commit_txn, so the WAL txn is still open for a later rollback.
// Ends with an auto-checkpoint decision.
@(private)
commit_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state != .Active {
		log.warn("No active transaction to commit")
		return .Transaction_Error
	}

	db.txn_snapshot_id += 1
	snap_id := db.txn_snapshot_id
	st := Schema_Tree(db)
	// Flush staged data roots: one schema COW per dirty table (not per
	// statement) against the current tree, so DDL published mid-txn is
	// preserved and the snapshot below captures post-flush roots.
	for name, root in db.txn_pending.roots {
		new_r, flush_ok := schema.update_root_page_cow(&st, name, root)
		if !flush_ok {
			return .IO_Error
		}
		st.root = new_r
	}
	for name, stages in db.txn_pending.index_roots {
		for stg in stages {
			new_r, flush_ok := schema.update_index_root_cow(&st, name, stg.name, stg.root)
			if !flush_ok {
				return .IO_Error
			}
			st.root = new_r
		}
	}
	if len(db.txn_pending.roots) > 0 || len(db.txn_pending.index_roots) > 0 {
		db.schema_root_page = st.root
		update_header(db)
	}

	executor.pending_clear(&db.txn_pending)
	schema_tables := schema.list_tables(&st, context.temp_allocator)
	tables := make([dynamic]types.Table, context.temp_allocator)
	for tbl in schema_tables {
		append(&tables, types.Table{name = tbl.name, root_page = tbl.root_page})
	}

	manifest_page := snapshot.create_manifest(db.pager, tables[:])
	defer if manifest_page != 0 {
		pager.unpin_page(db.pager, manifest_page)
	}

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
	record_main_ref(db, snap_id)
	if err := pager.wal_commit_txn(db.pager); err != .None {
		return .IO_Error
	}

	db.txn_state = .None
	log.infof("COMMIT transaction (snapshot %d)", db.txn_snapshot_id)
	maybe_auto_checkpoint(db)
	return .None
}

// rollback_impl aborts an active transaction: WAL frames dropped and cache
// purged (wal_abort_txn), the file rewound to its pre-txn length (pages the
// txn allocated were never snapshotted), the schema root restored from the
// latest snapshot, and staged roots + table cache cleared. A failed rewind
// is only a warning — disk stays larger than needed while the in-memory
// state is already consistent.
@(private)
rollback_impl :: proc(db: ^Database) -> DB_Error {
	if db.txn_state != .Active {
		log.warn("No active transaction to roll back")
		return .Transaction_Error
	}

	pager.wal_abort_txn(db.pager)
	if db.txn_start_file_len < u64(db.pager.file_len) {
		// Persist the rewind: pages allocated by the aborted txn were never
		// snapshotted and are unreachable once roots are restored below.
		// On failure the disk simply stays big (status quo ante) while the
		// in-memory state is already consistent.
		rewound_pages := u32(db.txn_start_file_len / u64(types.PAGE_SIZE))
		if _, rerr := pager.rewind_after_abort(db.pager, rewound_pages); rerr != .None {
			log.warnf("rollback could not truncate file; space will be reused")
		}
	}
	if db.latest_snapshot != 0 {
		snap_h, snap_ok := snapshot.load(db.pager, db.latest_snapshot)
		if snap_ok {
			db.schema_root_page = snap_h.schema_root
		}
	}

	// Staged roots die with the txn; the cache never saw a root bump (roots
	// were overlaid, not published), so clear it explicitly — otherwise
	// stale pending roots leak past the rollback.
	executor.pending_clear(&db.txn_pending)
	schema.table_cache_clear(&db.table_cache)
	db.txn_state = .None
	log.info("ROLLBACK transaction")
	return .None
}

// begin starts a transaction (exclusive lock). Nested BEGIN fails with
// .Transaction_Error; DML inside stages roots until commit/rollback.
begin :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return begin_impl(db)
}

// commit persists an active transaction and captures its snapshot
// (exclusive lock). .Transaction_Error when no txn is active; on inner
// failures the txn stays open (not rolled back) for the caller to decide.
commit :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return commit_impl(db)
}

// rollback aborts an active transaction, restoring the pre-txn roots and
// file length (exclusive lock). .Transaction_Error when none is active.
rollback :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)
	return rollback_impl(db)
}
