package db

import "core:fmt"
import "core:log"
import "core:sync"
import "src:executor"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

// Reclaim_Decision explains an expire call's fate. Skips stay .None
// (warned no-ops, never errors) so existing flows that expire mid-txn keep
// working, minus the corruption: uncommitted COW pages exist in no snapshot
// live set, so a sweep would free them out from under the txn.
Reclaim_Decision :: enum u8 {
	Proceed,
	Empty_No_Snapshots,
	Blocked_Active_Txn,
}

// reclaim_decision gates a sweep: no snapshots yet (.Empty_No_Snapshots),
// an active txn (.Blocked_Active_Txn — uncommitted COW pages belong to no
// snapshot live set, so sweeping would free them mid-txn), else .Proceed.
// Blocked/empty callers warn and no-op rather than error (see
// Reclaim_Decision).
@(private = "file")
reclaim_decision :: proc(db: ^Database) -> Reclaim_Decision {
	if db.latest_snapshot == 0 {
		return .Empty_No_Snapshots
	}
	if db.txn_state == .Active {
		return .Blocked_Active_Txn
	}
	return .Proceed
}

// snapshot_diff prints a table of per-table root changes between two
// snapshots (taken from the latest snapshot's manifest). Read lock held
// for the whole call; names are heap-allocated and freed before return.
snapshot_diff :: proc(db: ^Database, older_id: u64, newer_id: u64) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_shared_lock(&db.mu)
	// NOTE: shared locks must release with shared_unlock. The write unlock
	// clears only the writer bit and leaks the reader count, which hangs the
	// next write-lock (e.g. db.close) in sema_wait forever.
	defer sync.rw_mutex_shared_unlock(&db.mu)
	if db.latest_snapshot == 0 {
		return .Snapshot_Not_Found
	}

	entries, ok := snapshot.diff_snapshots(db.pager, older_id, newer_id, db.latest_snapshot)
	if !ok {
		return .Snapshot_Failed
	}
	defer {
		for e in entries {
			delete(e.table_name)
		}
		delete(entries)
	}
	if len(entries) == 0 {
		fmt.printf("No changes between snapshots %d and %d.\n", older_id, newer_id)
		return .None
	}

	cols := []string{"table", "change", "old_root", "new_root"}
	rows := make([][]string, len(entries), context.temp_allocator)
	for e, i in entries {
		old_root, new_root := "-", "-"
		#partial switch e.change {
		case .CREATED:
			new_root = fmt.aprintf("%d", e.new_root, allocator = context.temp_allocator)
		case .DROPPED:
			old_root = fmt.aprintf("%d", e.old_root, allocator = context.temp_allocator)
		case .MODIFIED:
			old_root = fmt.aprintf("%d", e.old_root, allocator = context.temp_allocator)
			new_root = fmt.aprintf("%d", e.new_root, allocator = context.temp_allocator)
		}

		rows[i] = executor.row_of(
			context.temp_allocator,
			e.table_name,
			fmt.aprintf("%s", e.change, allocator = context.temp_allocator),
			old_root,
			new_root,
		)
	}

	executor.render_table(cols, rows)
	fmt.printf("(%d table(s) changed)\n", len(entries))
	return .None
}

// snapshot_tag attaches a human-readable tag to a snapshot's header page.
// .Snapshot_Not_Found for an id absent from the index. Marks the page
// dirty without framing a WAL txn: the write reaches disk on the next
// eviction writeback or checkpoint.
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

// snapshot_restore points the database at an earlier snapshot: pushes the
// current MAIN ref onto the undo log, repoints MAIN, and adopts the
// snapshot's schema root + latest_snapshot, then persists the header
// through the WAL. Data pages are untouched — restore is a root swap, so
// subsequent writes COW as usual. .Snapshot_Not_Found for unknown ids.
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

	record_main_ref(db, snapshot_id)
	db.latest_snapshot = snap_page
	db.schema_root_page = snap_h.schema_root

	wal_update_header(db)
	fmt.printf("Restored to snapshot %d (schema root %d)\n", snapshot_id, snap_h.schema_root)
	return .None
}

// rollforward undoes a snapshot_restore: pops the undo log (where restore
// pushed the snapshot it displaced) and moves MAIN back to it, adopting
// that snapshot's schema root and persisting the header through the WAL.
// .Nothing_To_Roll when the log is empty or the entry equals the current
// ref; .Snapshot_Expired when the entry's snapshot has since been expired
// (its header page is gone).
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

	record_main_ref(db, prev_id)
	db.latest_snapshot = target_page
	db.schema_root_page = target_h.schema_root

	wal_update_header(db)
	fmt.printf("Rolled forward to snapshot %d (schema root %d)\n", prev_id, target_h.schema_root)
	return .None
}

// capture_snapshot records the current schema state as a new snapshot:
// manifest, id bump, create, index/latest/ref publication. Shared by the
// per-statement path and close (pending batch at shutdown) so the two can
// never drift apart.
@(private)
capture_snapshot :: proc(db: ^Database, op: snapshot.Snapshot_Operation) {
	st := Schema_Tree(db)
	schema_tables := schema.list_tables(&st, context.temp_allocator)
	tables := make([dynamic]types.Table, context.temp_allocator)
	for tbl in schema_tables {
		append(&tables, types.Table{name = tbl.name, root_page = tbl.root_page})
	}

	manifest_page := snapshot.create_manifest(db.pager, tables[:])
	defer if manifest_page != 0 {
		pager.unpin_page(db.pager, manifest_page)
	}

	db.txn_snapshot_id += 1
	snap_id := db.txn_snapshot_id
	snap_page, snap_ok := snapshot.create(
		db.pager,
		snap_id,
		db.latest_snapshot,
		db.schema_root_page,
		manifest_page,
		op,
	)
	if snap_ok {
		db.snapshot_index[snap_id] = snap_page
		db.latest_snapshot = snap_page
		record_main_ref(db, snap_id)
	}
}

// expire_snapshots keeps the newest keep_count snapshots, garbage-collects
// the rest (frees their pages, drops their index entries), and persists the
// header. Exclusive lock; delegates to expire_snapshots_impl.
expire_snapshots :: proc(db: ^Database, keep_count: int) -> DB_Error {
	db_check(db) or_return
	sync.rw_mutex_lock(&db.mu); defer sync.rw_mutex_unlock(&db.mu)
	return expire_snapshots_impl(db, keep_count)
}

// expire_snapshots_impl is the sweep body, shared by the API and dot paths
// so the keep-count clamp and reclaim gate below live in one place.
expire_snapshots_impl :: proc(db: ^Database, keep_count: int) -> DB_Error {
	keep := keep_count
	if keep < 1 {
		// keep < 1 would mark nothing live (the chain walk keeps zero
		// snapshots) and sweep the whole database into the freelist.
		// Clamp to default: fail-closed, warned, never silent. Lives here
		// (not in callers) so API and dot paths share the choke point.
		log.warnf("expire keep %d invalid; using default %d", keep, DEFAULT_KEEP)
		keep = DEFAULT_KEEP
	}

	#partial switch reclaim_decision(db) {
	case .Empty_No_Snapshots:
		return .None
	case .Blocked_Active_Txn:
		log.warnf(
			"expire skipped: transaction active; uncommitted data is not in any snapshot live set",
		)
		return .None
	case .Proceed:
	}

	expired_ids := snapshot.expire_and_collect(db.pager, db.latest_snapshot, keep)
	for id in expired_ids {
		delete_key(&db.snapshot_index, id)
	}

	wal_update_header(db)
	fmt.printf("Expired snapshots older than last %d, garbage collected\n", keep)
	return .None
}
