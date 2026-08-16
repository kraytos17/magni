package db

import "core:sync"
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

	is_read := false
	#partial switch s in stmt.type {
	case parser.Select_Stmt:
		is_read = true
	case parser.Compound_Stmt:
		is_read = true
	}

	if is_read {
		sync.rw_mutex_shared_lock(&db.mu)
		defer sync.rw_mutex_shared_unlock(&db.mu)
	} else {
		sync.rw_mutex_lock(&db.mu)
		defer sync.rw_mutex_unlock(&db.mu)
	}

	if txn_stmt, is_txn := stmt.type.(parser.Txn_Stmt); is_txn {
		switch txn_stmt.op {
		case .BEGIN:
			return begin_impl(db)
		case .COMMIT:
			return commit_impl(db)
		case .ROLLBACK:
			return rollback_impl(db)
		}
	}

	st := Schema_Tree(db)
	as_of_override := false
	if sel, is_sel := stmt.type.(parser.Select_Stmt); is_sel {
		if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
			snap_page, has_page := db.snapshot_index[snap_id]
			if !has_page {
				return .Snapshot_Not_Found
			}

			snap_h, snap_ok := snapshot.load(db.pager, snap_page, snap_id)
			if !snap_ok {
				return .Snapshot_Failed
			}

			st.root = snap_h.schema_root
			as_of_override = true
		} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
			snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
			if !snap_ok {
				return .Snapshot_Not_Found
			}

			st.root = snap_h.schema_root
			as_of_override = true
		}
	}

	exec_ok, new_root, _ := executor.execute(&st, stmt)
	if !as_of_override {
		db.schema_root_page = new_root
		update_header(db)
	}
	if exec_ok && db.txn_state == .NONE && !as_of_override {
		db.snapshot_batch_count += 1
		threshold := db.snapshot_batch_threshold
		if threshold <= 0 { threshold = 1 }

		make_snapshot := db.snapshot_batch_count >= threshold
		if make_snapshot {
			db.snapshot_batch_count = 0
		}

		pager.wal_begin_txn(db.pager)
		snap_op: snapshot.Snapshot_Operation
		#partial switch s in stmt.type {
		case parser.Insert_Stmt:
			snap_op = .INSERT
		case parser.Update_Stmt:
			snap_op = .UPDATE
		case parser.Delete_Stmt:
			snap_op = .DELETE
		case parser.Create_Stmt:
			snap_op = .CREATE
		case parser.Drop_Stmt:
			snap_op = .DROP
		}

		if make_snapshot {
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
				snap_op,
			)
			if snap_ok {
				db.snapshot_index[snap_id] = snap_page
				db.latest_snapshot = snap_page
				snapshot.set_ref(
					db.pager,
					db.refs_page,
					snapshot.MAIN_REF,
					snap_id,
					.BRANCH,
					false,
				)
			}
		}
		pager.wal_commit_txn(db.pager)
	}

	if exec_ok { return .None }
	return .IO_Error
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
		if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
			snap_page, has_page := db.snapshot_index[snap_id]
			if !has_page {
				r.err = .Snapshot_Not_Found
				return r
			}

			snap_h, snap_ok := snapshot.load(db.pager, snap_page, snap_id)
			if !snap_ok {
				r.err = .Snapshot_Failed
				return r
			}
			st.root = snap_h.schema_root
		} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
			snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
			if !snap_ok {
				r.err = .Snapshot_Not_Found
				return r
			}
			st.root = snap_h.schema_root
		}

		rows, cols, q_ok := executor.exec_query(&st, sel)
		if !q_ok {
			r.err = .IO_Error
			return r
		}

		col_names := make([]string, len(cols), context.temp_allocator)
		col_types := make([]types.Column_Type, len(cols), context.temp_allocator)
		for col, i in cols {
			col_names[i] = col.name
			col_types[i] = col.type
		}

		flat_rows := make([][]types.Value, len(rows), context.temp_allocator)
		for entry, i in rows { flat_rows[i] = entry.values }

		r.ok = true
		r.columns = col_names
		r.col_types = col_types
		r.rows = flat_rows
		return r
	} else if comp, is_comp := stmt.type.(parser.Compound_Stmt); is_comp {
		rows, cols, q_ok := executor.exec_compound_data(&st, comp)
		if !q_ok {
			r.err = .IO_Error
			return r
		}

		col_names := make([]string, len(cols), context.temp_allocator)
		col_types := make([]types.Column_Type, len(cols), context.temp_allocator)
		for col, i in cols {
			col_names[i] = col.name
			col_types[i] = col.type
		}

		flat_rows := make([][]types.Value, len(rows), context.temp_allocator)
		for entry, i in rows { flat_rows[i] = entry.values }

		r.ok = true
		r.columns = col_names
		r.col_types = col_types
		r.rows = flat_rows
		return r
	}
	r.err = .Not_Supported
	return r
}
