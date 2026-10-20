package db

import "core:sync"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"
import "src:types"

// execute runs one SQL statement (any type) against db. Takes the shared
// lock for reads, the exclusive lock for writes; TRANSACTION statements
// dispatch before execution. Successful writes run WAL/snapshot bookkeeping
// (maybe_snapshot) and publish their schema root unless the statement
// deferred it (in-txn DML staging, AS OF). SELECT output is rendered to
// stdout here — use query when you want the rows back instead.
execute :: proc(db: ^Database, sql: string) -> DB_Error {
	db_check(db) or_return
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	if !ok {
		return .Parse_Error
	}

	kind := stmt_kind(stmt)
	ctx := Exec_Ctx {
		is_read = kind.is_read,
	}
	if ctx.is_read {
		sync.rw_mutex_shared_lock(&db.mu)
		defer sync.rw_mutex_shared_unlock(&db.mu)
	} else {
		sync.rw_mutex_lock(&db.mu)
		defer sync.rw_mutex_unlock(&db.mu)
	}

	if handled, txn_err := dispatch_txn(db, stmt); handled {
		return txn_err
	}

	st := Schema_Tree(db)
	if sel, is_sel := stmt.type.(parser.Select_Stmt); is_sel {
		override, as_of_err := resolve_as_of(db, &st, sel)
		if as_of_err != .None {
			return as_of_err
		}
		ctx.as_of_override = override
	}

	result: executor.Result
	// Inside an explicit txn DML stages roots instead of publishing; DDL
	// still publishes immediately (its own path).
	pending := &db.txn_pending if db.txn_state == .Active else nil
	exec_ok, new_root, _ := executor.execute(&st, stmt, &result, &db.table_cache, pending)
	if !ctx.as_of_override && !ctx.is_read && !(db.txn_state == .Active && kind.defers_root) {
		db.schema_root_page = new_root
		update_header(db)
	}
	if exec_ok && !ctx.is_read {
		maybe_snapshot(db, kind, ctx)
	}
	if exec_ok {
		if result.is_select {
			executor.render_result(result)
		}
		return .None
	}
	return .IO_Error
}

// Exec_Ctx carries one execute call's cross-stage decisions: whether the
// statement only reads (shared lock) and whether AS OF redirected the
// schema tree (skip root publication). Built once at dispatch, consumed
// by the commit tail.
Exec_Ctx :: struct {
	is_read       : bool,
	as_of_override: bool,
}

// Stmt_Kind classifies a statement once for dispatch: lock mode, in-txn
// root staging, and snapshot operation. One switch instead of three
// parallel re-switches — adding a statement type touches one site.
Stmt_Kind :: struct {
	is_read:     bool, // shared lock (SELECT, compound)
	defers_root: bool, // in-txn root publication staged (DML only)
	snap_op:     snapshot.Snapshot_Operation, // snapshot op for writes
}

// stmt_kind maps a statement to its dispatch class. Reads and transactions
// carry no snapshot op (UNKNOWN); the txn gate lives at the call site
// (defers_root only matters when a txn is active).
@(private = "file")
stmt_kind :: proc(stmt: parser.Statement) -> Stmt_Kind {
	#partial switch _ in stmt.type {
	case parser.Select_Stmt, parser.Compound_Stmt:
		return {is_read = true}
	case parser.Insert_Stmt:
		return {defers_root = true, snap_op = .INSERT}
	case parser.Update_Stmt:
		return {defers_root = true, snap_op = .UPDATE}
	case parser.Delete_Stmt:
		return {defers_root = true, snap_op = .DELETE}
	case parser.Create_Stmt, parser.Create_Index_Stmt:
		return {snap_op = .CREATE}
	case parser.Drop_Stmt, parser.Drop_Index_Stmt:
		return {snap_op = .DROP}
	}
	return {}
}

// dispatch_txn runs a transaction statement directly.
// Returns (handled, err): unhandled statements fall through to execution.
@(private = "file")
dispatch_txn :: proc(db: ^Database, stmt: parser.Statement) -> (bool, DB_Error) {
	txn_stmt, is_txn := stmt.type.(parser.Txn_Stmt)
	if !is_txn {
		return false, .None
	}

	switch txn_stmt.op {
	case .BEGIN:
		return true, begin_impl(db)
	case .COMMIT:
		return true, commit_impl(db)
	case .ROLLBACK:
		return true, rollback_impl(db)
	}
	return true, .None
}

// maybe_snapshot runs WAL + snapshot bookkeeping for a successful write:
// batch counting, WAL framing, periodic manifest snapshots, and commit.
// No-op inside transactions (txn owns its own commit) or under AS OF
// (read-only view) — the same conditions the execute tail uses inline to
// decide root publication.
@(private = "file")
maybe_snapshot :: proc(db: ^Database, kind: Stmt_Kind, ctx: Exec_Ctx) {
	if db.txn_state != .None || ctx.as_of_override {
		return
	}

	db.snapshot_batch_count += 1
	threshold := db.snapshot_batch_threshold
	if threshold <= 0 {
		threshold = 1
	}

	make_snapshot := db.snapshot_batch_count >= threshold
	if make_snapshot {
		db.snapshot_batch_count = 0
	}

	pager.wal_begin_txn(db.pager)
	if make_snapshot {
		capture_snapshot(db, kind.snap_op)
	}

	pager.wal_commit_txn(db.pager)
	maybe_auto_checkpoint(db)
}

Query_Result :: struct {
	columns  : []string,
	col_types: []types.Column_Type,
	rows     : [][]types.Value,
	ok       : bool,
	err      : DB_Error,
}

// query runs a read-only statement (SELECT or compound) and returns the
// rows instead of rendering them. Always takes the shared lock — writes,
// DDL, and TRANSACTION statements are rejected with .Not_Supported (use
// execute for those). AS OF is honored. Output slices alias temp allocator
// memory: consume before the next call.
query :: proc(db: ^Database, sql: string) -> Query_Result {
	r := Query_Result{}
	if err := db_check(db); err != .None {
		r.err = err
		return r
	}

	sync.rw_mutex_shared_lock(&db.mu)
	defer sync.rw_mutex_shared_unlock(&db.mu)

	stmt, parse_ok, _ := parser.parse(sql, context.temp_allocator)
	if !parse_ok {
		r.err = .Parse_Error
		return r
	}

	st := Schema_Tree(db)
	if sel, is_sel := stmt.type.(parser.Select_Stmt); is_sel {
		_, err := resolve_as_of(db, &st, sel)
		if err != .None {
			r.err = err
			return r
		}

		rows, cols, q_ok := executor.exec_query(&st, sel, &db.table_cache)
		if !q_ok {
			r.err = .IO_Error
			return r
		}
		return pack_query_result(rows, cols)
	} else if comp, is_comp := stmt.type.(parser.Compound_Stmt); is_comp {
		rows, cols, q_ok := executor.exec_compound_data(&st, comp, &db.table_cache)
		if !q_ok {
			r.err = .IO_Error
			return r
		}
		return pack_query_result(rows, cols)
	}

	r.err = .Not_Supported
	return r
}

// resolve_as_of points st at the requested AS OF snapshot (if any).
// Returns (overrode, err); err is .None on success, including when the
// statement has no AS OF clause at all.
@(private = "file")
resolve_as_of :: proc(
	db: ^Database,
	st: ^btree.Tree,
	sel: parser.Select_Stmt,
) -> (
	bool,
	DB_Error,
) {
	if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
		snap_page, has_page := db.snapshot_index[snap_id]
		if !has_page {
			return false, .Snapshot_Not_Found
		}

		snap_h, snap_ok := snapshot.load(db.pager, snap_page, snap_id)
		if !snap_ok {
			return false, .Snapshot_Failed
		}

		// The cache may hold an in-txn overlay staged against this exact
		// root (pending content, not committed content). AS OF wants
		// committed history — drop the generation so the table re-resolves
		// from the snapshot's tree. Rare path; a cold schema re-read is fine.
		schema.table_cache_clear(&db.table_cache)
		st.root = snap_h.schema_root
		return true, .None
	} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
		snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
		if !snap_ok {
			return false, .Snapshot_Not_Found
		}

		schema.table_cache_clear(&db.table_cache)
		st.root = snap_h.schema_root
		return true, .None
	}
	return false, .None
}

// pack_query_result flattens executor rows/cols into the API result struct.
@(private = "file")
pack_query_result :: proc(rows: []executor.Row_Entry, cols: []types.Column) -> Query_Result {
	col_names := make([]string, len(cols), context.temp_allocator)
	col_types := make([]types.Column_Type, len(cols), context.temp_allocator)
	for col, i in cols {
		col_names[i] = col.name
		col_types[i] = col.type
	}

	flat_rows := make([][]types.Value, len(rows), context.temp_allocator)
	for entry, i in rows {
		flat_rows[i] = entry.values
	}
	return Query_Result {
		columns = col_names,
		col_types = col_types,
		rows = flat_rows,
		ok = true,
		err = .None,
	}
}
