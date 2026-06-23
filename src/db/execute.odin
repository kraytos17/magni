package db

import "core:fmt"
import "core:sync"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:snapshot"
import "src:types"

execute :: proc(db: ^Database, sql: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	stmt, ok := parser.parse(sql, context.temp_allocator)
	if !ok {
		fmt.eprintln("Error: Failed to parse SQL statement")
		return false
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

	st := schema_tree(db)
	as_of_override := false
	if sel, is_sel := stmt.type.(parser.Select_Stmt); is_sel {
		if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
			snap_page, has_page := db.snapshot_index[snap_id]
			if !has_page {
				fmt.eprintln("Error: Snapshot", snap_id, "not found")
				return false
			}

			snap_h, snap_ok := snapshot.load(db.pager, snap_page)
			if !snap_ok {
				fmt.eprintln("Error: Failed to load snapshot", snap_id)
				return false
			}

			st.root = snap_h.schema_root
			as_of_override = true
		} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
			snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
			if !snap_ok {
				fmt.eprintln("Error: No snapshot found at or before timestamp", ts_val)
				return false
			}

			st.root = snap_h.schema_root
			as_of_override = true
		}
	}

	exec_ok, new_root := executor.execute(&st, stmt)
	if !as_of_override { db.schema_root_page = new_root }
	if exec_ok && db.txn_state == .NONE && !as_of_override {
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

		mt := executor.mutated_table_info
		if mt.name != "" {
			if mt.root != 0 {
				db.table_roots[mt.name] = mt.root
			} else {
				delete_key(&db.table_roots, mt.name)
			}

			executor.mutated_table_info = {}
		} else if snap_op == .CREATE || snap_op == .DROP {
			db.table_roots_dirty = true
		}

		ensure_table_roots(db)
		tables := make([dynamic]types.Table, context.temp_allocator)
		for name, root in db.table_roots {
			append(&tables, types.Table{name = name, root_page = root})
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
			snapshot.prune(db.pager, db.latest_snapshot, 100)
			db.gc_pending_count += 1
			if db.gc_pending_count >= GC_INTERVAL {
				snapshot.gc(db.pager, db.latest_snapshot, 100)
				db.gc_pending_count = 0
			}
		}
		pager.flush_all(db.pager)
	}
	free_all(context.temp_allocator)
	return exec_ok
}

Query_Result :: struct {
	columns:   []string,
	col_types: []types.Column_Type,
	rows:      [][]types.Value,
	ok:        bool,
}

query :: proc(db: ^Database, sql: string) -> Query_Result {
	r := Query_Result{}
	if !db_check(db) { return r }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	stmt, parse_ok := parser.parse(sql, context.temp_allocator)
	if !parse_ok {
		fmt.eprintln("Error: Failed to parse SQL statement")
		return r
	}

	st := schema_tree(db)
	if sel, is_sel := stmt.type.(parser.Select_Stmt); is_sel {
		if snap_id, has_snap := sel.as_of_snapshot.?; has_snap {
			snap_page, has_page := db.snapshot_index[snap_id]
			if !has_page {
				fmt.eprintln("Error: Snapshot", snap_id, "not found")
				return r
			}
			snap_h, snap_ok := snapshot.load(db.pager, snap_page)
			if !snap_ok { return r }
			st.root = snap_h.schema_root
		} else if ts_val, has_ts := sel.as_of_timestamp.?; has_ts {
			snap_h, snap_ok := snapshot.find_by_timestamp(db.pager, db.latest_snapshot, ts_val)
			if !snap_ok {
				fmt.eprintln("Error: No snapshot found at or before timestamp", ts_val)
				return r
			}
			st.root = snap_h.schema_root
		}

		rows, cols, q_ok := executor.exec_query(&st, sel)
		if !q_ok { return r }

		col_names := make([]string, len(cols), context.temp_allocator)
		col_types := make([]types.Column_Type, len(cols), context.temp_allocator)
		for col, i in cols {
			col_names[i] = col.name
			col_types[i] = col.type
		}

		flat_rows := make([][]types.Value, len(rows), context.temp_allocator)
		for entry, i in rows { flat_rows[i] = entry.values }
		return Query_Result{columns = col_names, col_types = col_types, rows = flat_rows, ok = true}
	}
	fmt.eprintln("Error: query() only supports SELECT statements")
	return r
}
