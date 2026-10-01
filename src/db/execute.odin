package db

import "core:sync"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"
import "src:types"

execute :: proc(db: ^Database, sql: string) -> DB_Error {
	db_check(db) or_return
	stmt, ok, _ := parser.parse(sql, context.temp_allocator)
	if !ok {
		return .Parse_Error
	}

	ctx := Exec_Ctx{is_read = stmt_is_read(stmt)}
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
	exec_ok, new_root, _ := executor.execute(&st, stmt, &result, &db.table_cache)
	if !ctx.as_of_override && !ctx.is_read {
		db.schema_root_page = new_root
		update_header(db)
	}
	if exec_ok && !ctx.is_read {
		maybe_snapshot(db, stmt, ctx)
	}
	if exec_ok {
		if result.is_select { executor.render_result(result) }
		return .None
	}
	return .IO_Error
}

// Exec_Ctx carries one execute call's cross-stage decisions: whether the
// statement only reads (shared lock), whether AS OF redirected the schema
// tree (skip root publication), and which snapshot operation a write maps
// to. Built once at dispatch, consumed by the commit tail.
Exec_Ctx :: struct {
	is_read:        bool,
	as_of_override: bool,
	snap_op:        snapshot.Snapshot_Operation,
}

// stmt_is_read reports whether a statement takes the shared (read) lock.
@(private="file")
stmt_is_read :: proc(stmt: parser.Statement) -> bool {
	_, is_sel := stmt.type.(parser.Select_Stmt)
	_, is_comp := stmt.type.(parser.Compound_Stmt)
	return is_sel || is_comp
}

// dispatch_txn runs a transaction statement directly.
// Returns (handled, err): unhandled statements fall through to execution.
@(private="file")
dispatch_txn :: proc(db: ^Database, stmt: parser.Statement) -> (bool, DB_Error) {
	txn_stmt, is_txn := stmt.type.(parser.Txn_Stmt)
	if !is_txn { return false, .None }

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

// snapshot_op maps a write statement to its snapshot operation.
// Reads and transactions never reach here (guarded by is_read + dispatch);
// UNKNOWN is the unreachable default.
@(private="file")
snapshot_op :: proc(stmt: parser.Statement) -> snapshot.Snapshot_Operation {
	#partial switch _ in stmt.type {
	case parser.Insert_Stmt:
		return .INSERT
	case parser.Update_Stmt:
		return .UPDATE
	case parser.Delete_Stmt:
		return .DELETE
	case parser.Create_Stmt:
		return .CREATE
	case parser.Drop_Stmt:
		return .DROP
	}
	return .UNKNOWN
}

// maybe_snapshot runs WAL + snapshot bookkeeping for a successful write:
// batch counting, WAL framing, periodic manifest snapshots, and commit.
// No-op inside transactions or under AS OF (mirrors the inline guards).
@(private="file")
maybe_snapshot :: proc(db: ^Database, stmt: parser.Statement, ctx: Exec_Ctx) {
	if db.txn_state != .None || ctx.as_of_override { return }

	db.snapshot_batch_count += 1
	threshold := db.snapshot_batch_threshold
	if threshold <= 0 { threshold = 1 }

	make_snapshot := db.snapshot_batch_count >= threshold
	if make_snapshot {
		db.snapshot_batch_count = 0
	}

	pager.wal_begin_txn(db.pager)
	if make_snapshot {
		write_snapshot(db, snapshot_op(stmt))
	}

	pager.wal_commit_txn(db.pager)
	maybe_auto_checkpoint(db)
}

// write_snapshot manifests the current tables and records one snapshot.
// The manifest page is unpinned once recorded (previously held to
// execute-return; it is unused past snapshot.create).
@(private="file")
write_snapshot :: proc(db: ^Database, op: snapshot.Snapshot_Operation) {
	snap_st := Schema_Tree(db)
	schema_tables := schema.list_tables(&snap_st, context.temp_allocator)
	tables := make([dynamic]types.Table, context.temp_allocator)
	for tbl in schema_tables {
		append(&tables, types.Table{name = tbl.name, root_page = tbl.root_page})
	}

	manifest_page := snapshot.create_manifest(db.pager, tables[:])
	defer if manifest_page != 0 { pager.unpin_page(db.pager, manifest_page) }

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

Query_Result :: struct {
	columns:   []string,
	col_types: []types.Column_Type,
	rows:      [][]types.Value,
	ok:        bool,
	err:       DB_Error,
}

query :: proc(db: ^Database, sql: string) -> Query_Result {
	r := Query_Result{}
	if err := db_check(db); err != .None { r.err = err; return r }

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
@(private="file")
resolve_as_of :: proc(db: ^Database, st: ^btree.Tree, sel: parser.Select_Stmt) -> (bool, DB_Error) {
	if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
		snap_page, has_page := db.snapshot_index[snap_id]
		if !has_page {
			return false, .Snapshot_Not_Found
		}

		snap_h, snap_ok := snapshot.load(db.pager, snap_page, snap_id)
		if !snap_ok {
			return false, .Snapshot_Failed
		}

		st.root = snap_h.schema_root
		return true, .None
	} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
		snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
		if !snap_ok {
			return false, .Snapshot_Not_Found
		}

		st.root = snap_h.schema_root
		return true, .None
	}
	return false, .None
}

// pack_query_result flattens executor rows/cols into the API result struct.
@(private="file")
pack_query_result :: proc(rows: []executor.Row_Entry, cols: []types.Column) -> Query_Result {
	col_names := make([]string, len(cols), context.temp_allocator)
	col_types := make([]types.Column_Type, len(cols), context.temp_allocator)
	for col, i in cols {
		col_names[i] = col.name
		col_types[i] = col.type
	}

	flat_rows := make([][]types.Value, len(rows), context.temp_allocator)
	for entry, i in rows { flat_rows[i] = entry.values }
	return Query_Result{
		columns   = col_names,
		col_types = col_types,
		rows      = flat_rows,
		ok        = true,
		err       = .None,
	}
}
