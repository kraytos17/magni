package db

import "core:fmt"
import "core:strings"
import "core:sync"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:snapshot"
import "src:types"

Transaction_State :: enum {
	NONE,
	ACTIVE,
}

// Database handle
//
// Thread safety: All public functions acquire mu (sync.Mutex) before accessing
// the pager or schema tree. This serializes all operations — safe for
// single-client use. True concurrent reads across coroutines would need
// finer-grained locking.
Database :: struct {
	path:              string,
	pager:             ^pager.Pager,
	is_new:            bool,
	mu:                sync.Mutex,
	schema_root_page:  u32,
	latest_snapshot:   u32,
	txn_state:         Transaction_State,
	txn_snapshot_id:   u64,
	snapshot_index:    map[u64]u32,
	gc_pending_count:  int,
	table_roots:       map[string]u32,
	table_roots_dirty: bool,
}

GC_INTERVAL :: 10

Header :: struct #packed {
	magic:                [13]u8,
	page_size:            u32le,
	page_count:           u32le,
	schema_version:       u32le,
	schema_root_page:     u32le, // Root page of schema B-tree (0 if uninitialized)
	latest_snapshot_page: u32le, // Latest snapshot page (0 = none)
	snapshot_id_counter:  u64le, // Monotonic snapshot ID counter
	first_free_page:      u32le, // Head of free-page linked list (0 = empty)
	reserved:             [55]u8,
}

#assert(size_of(Header) == types.DATABASE_HEADER_SIZE)

schema_tree :: proc(db: ^Database) -> btree.Tree {
	return btree.init(db.pager, db.schema_root_page)
}

// Opens an existing database or creates a new one at the specified path.
open :: proc(path: string) -> (^Database, bool) {
	db := new(Database)
	if db == nil {
		fmt.eprintln("Error: Failed to allocate database handle")
		return nil, false
	}

	db.path = strings.clone(path)
	db.latest_snapshot = 0
	p, err := pager.open(path)
	if err != nil {
		fmt.eprintln("Error: Failed to open database file:", err)
		delete(db.path)
		free(db)
		return nil, false
	}

	db.pager = p
	db.is_new = (db.pager.file_len == 0)
	db.txn_state = .NONE
	db.txn_snapshot_id = 0
	db.snapshot_index = make(map[u64]u32, 128)
	db.table_roots = make(map[string]u32)
	if db.is_new {
		fmt.println("Initializing new database...")
		if !initialize(db) {
			close(db)
			return nil, false
		}
	} else {
		if !verify_header(db) {
			fmt.eprintln("Error: Invalid or corrupted database file")
			close(db)
			return nil, false
		}

		page1, h_err := pager.get_page(db.pager, 1)
		if h_err != .None {
			fmt.eprintln("Error: Failed to read database header")
			close(db)
			return nil, false
		}

		header := (^Header)(raw_data(page1.data))
		db.schema_root_page = u32(header.schema_root_page)
		db.latest_snapshot = u32(header.latest_snapshot_page)
		db.txn_snapshot_id = u64(header.snapshot_id_counter)
		db.pager.first_free_page = u32(header.first_free_page)
		pager.unpin_page(db.pager, 1)

		// Build snapshot index by walking the chain
		page := db.latest_snapshot
		for page != 0 {
			h, ok := snapshot.load(db.pager, page)
			if !ok { break }
			db.snapshot_index[h.snapshot_id] = page
			page = h.prev_snapshot
		}
	}
	return db, true
}

// Closes the database and frees all associated resources.
close :: proc(db: ^Database) {
	if db == nil {
		return
	}
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	update_header(db)
	if db.pager != nil {
		if err := pager.close(db.pager); err != .None {
			fmt.eprintln("Warning: error closing database:", err)
		}
	}
	delete(db.snapshot_index)
	delete(db.table_roots)
	delete(db.path)
	free(db)
}

initialize :: proc(db: ^Database) -> bool {
	page1, err := pager.allocate_page(db.pager)
	if err != .None {
		fmt.eprintln("Error: Failed to allocate header page:", err)
		return false
	}
	defer pager.unpin_page(db.pager, page1.page_num)

	header := (^Header)(raw_data(page1.data))
	copy(header.magic[:], types.MAGIC_STRING)

	header.page_size = u32le(types.PAGE_SIZE)
	header.page_count = 1
	header.schema_version = 2

	schema_page, s_err := pager.allocate_page(db.pager)
	if s_err != .None {
		fmt.eprintln("Error: Failed to allocate schema root page:", s_err)
		return false
	}
	defer pager.unpin_page(db.pager, schema_page.page_num)

	btree.init_leaf_page(schema_page.data, schema_page.page_num)
	pager.mark_dirty(db.pager, schema_page.page_num)

	db.schema_root_page = schema_page.page_num
	header.schema_root_page = u32le(schema_page.page_num)
	header.latest_snapshot_page = 0
	header.page_count = u32le(schema_page.page_num)

	st := schema_tree(db)
	if !schema.init(&st) {
		fmt.eprintln("Error: Failed to initialize schema tree")
		return false
	}

	pager.flush_all(db.pager)
	return true
}

// Verifies that the database file has a valid header.
verify_header :: proc(db: ^Database) -> bool {
	page, err := pager.get_page(db.pager, 1)
	if err != .None {
		return false
	}
	defer pager.unpin_page(db.pager, 1)

	header := (^Header)(raw_data(page.data))
	if string(header.magic[:]) != types.MAGIC_STRING {
		return false
	}
	if header.page_size != u32le(types.PAGE_SIZE) {
		fmt.eprintln("Error: Page size mismatch")
		return false
	}
	return true
}

update_header :: proc(db: ^Database) {
	page1, err := pager.get_page(db.pager, 1)
	if err != .None { return }
	defer pager.unpin_page(db.pager, 1)

	header := (^Header)(raw_data(page1.data))
	page_count := pager.page_count(db.pager)
	header.page_count = u32le(page_count)
	header.schema_root_page = u32le(db.schema_root_page)
	header.latest_snapshot_page = u32le(db.latest_snapshot)
	header.snapshot_id_counter = u64le(db.txn_snapshot_id)
	header.first_free_page = u32le(db.pager.first_free_page)

	pager.mark_dirty(db.pager, 1)
	pager.flush_all(db.pager)
}

db_check :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}
	return true
}

// Ensures the table_roots cache is populated. Caller must hold db.mu.
ensure_table_roots :: proc(db: ^Database) {
	if !db.table_roots_dirty && len(db.table_roots) > 0 { return }
	st := schema_tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)
	clear(&db.table_roots)
	for t in tables {
		db.table_roots[t.name] = t.root_page
	}
	db.table_roots_dirty = false
}

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
	if !as_of_override {
		db.schema_root_page = new_root
	}
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

			// Update table_roots cache from executor's mutation report
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
	rows:      [][]types.Value, // nil for DML statements
	ok:        bool,
}

// Executes a SELECT query and returns structured results.
// For DML statements, returns Query_Result{ok = bool}.
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
		for entry, i in rows {
			flat_rows[i] = entry.values
		}

		return Query_Result{columns = col_names, col_types = col_types, rows = flat_rows, ok = true}
	}

	// DML not supported via query — use execute instead
	fmt.eprintln("Error: query() only supports SELECT statements")
	return r
}

checkpoint :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.latest_snapshot != 0 {
		snapshot.gc(db.pager, db.latest_snapshot, 100)
	}

	pager.flush_all(db.pager)
	update_header(db)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

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
		if snap_ok {
			db.schema_root_page = snap_h.schema_root
		}
	}

	db.txn_state = .NONE
	fmt.println("ROLLBACK transaction")
	return true
}

// Begins a new transaction.
begin :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	return begin_impl(db)
}

// Commits the current transaction.
commit :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	return commit_impl(db)
}

// Rolls back the current transaction.
rollback :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	return rollback_impl(db)
}

// Prints the snapshot chain for debugging.
print_snapshots :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return
	}
	snapshot.debug_print_chain(db.pager, db.latest_snapshot)
}

snapshot_diff :: proc(db: ^Database, older_id: u64, newer_id: u64) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return false
	}

	entries, ok := snapshot.diff_snapshots(db.pager, older_id, newer_id, db.latest_snapshot)
	if !ok {
		fmt.eprintf("Error: failed to diff snapshots %d → %d\n", older_id, newer_id)
		return false
	}
	defer {
		for e in entries { delete(e.table_name) }
		delete(entries)
	}

	if len(entries) == 0 {
		fmt.printf("No changes between snapshots %d and %d.\n", older_id, newer_id)
		return true
	}

	fmt.printf("=== Snapshot Diff: %d → %d ===\n", older_id, newer_id)
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
	return true
}

// Tags a snapshot with a human-readable label.
snapshot_tag :: proc(db: ^Database, snapshot_id: u64, tag: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		fmt.eprintln("Error: Snapshot", snapshot_id, "not found")
		return false
	}
	snapshot.set_tag(db.pager, page, tag)
	return true
}

// Restores the database to a historical snapshot state by creating a new
// snapshot that references the historical schema root and manifest.
snapshot_restore :: proc(db: ^Database, snapshot_id: u64) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.latest_snapshot == 0 {
		fmt.eprintln("Error: No snapshots")
		return false
	}

	snap_page, has_page := db.snapshot_index[snapshot_id]
	if !has_page {
		fmt.eprintln("Error: Snapshot", snapshot_id, "not found")
		return false
	}
	snap_h, snap_ok := snapshot.load(db.pager, snap_page)
	if !snap_ok {
		fmt.eprintln("Error: Failed to load snapshot", snapshot_id)
		return false
	}

	db.txn_snapshot_id += 1
	new_id := db.txn_snapshot_id
	new_page, create_ok := snapshot.create(
		db.pager,
		new_id,
		db.latest_snapshot,
		snap_h.schema_root,
		snap_h.manifest_page,
		.RESTORE,
	)
	if !create_ok {
		fmt.eprintln("Error: Failed to create restore snapshot")
		return false
	}

	db.snapshot_index[new_id] = new_page
	db.latest_snapshot = new_page
	db.schema_root_page = snap_h.schema_root
	snapshot.prune(db.pager, db.latest_snapshot, 100)
	snapshot.gc(db.pager, db.latest_snapshot, 100)
	pager.flush_all(db.pager)
	update_header(db)
	fmt.printf("Restored to snapshot %d (schema root %d)\n", snapshot_id, snap_h.schema_root)
	return true
}

integrity_check :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	fmt.println("=== Integrity Check ===")
	if !verify_header(db) {
		fmt.println("✗ Database header is corrupted")
		return false
	}

	fmt.println("✓ Database header is valid")
	_, err := pager.get_page(db.pager, db.schema_root_page)
	if err != .None {
		fmt.println("✗ Schema page is missing")
		return false
	}

	defer pager.unpin_page(db.pager, db.schema_root_page)
	fmt.println("✓ Schema page exists")
	st := schema_tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)
	fmt.printf("✓ Found %d table(s)\n", len(tables))
	for table in tables {
		_, page_err := pager.get_page(db.pager, table.root_page)
		if page_err != .None {
			fmt.printf("✗ Table '%s' root page %d is missing\n", table.name, table.root_page)
			return false
		}
		defer pager.unpin_page(db.pager, table.root_page)
		fmt.printf("✓ Table '%s' is valid\n", table.name)
	}

	fmt.println("======================")
	fmt.println("Integrity check passed!")
	return true
}

list_tables :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := schema_tree(db)
	schema.debug_print_all(&st)
}

describe_table :: proc(db: ^Database, table_name: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := schema_tree(db)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", table_name)
		return false
	}
	schema.debug_print_entry(table)
	return true
}

stats :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	fmt.println("=== Database Statistics ===")
	fmt.printf("Path: %s\n", db.path)
	fmt.printf("Page size: %d bytes\n", types.PAGE_SIZE)

	page_count := pager.page_count(db.pager)
	fmt.printf("Total pages: %d\n", page_count)
	fmt.printf(
		"Database size: %d bytes (%.2f KB)\n",
		page_count * u32(types.PAGE_SIZE),
		f64(page_count * u32(types.PAGE_SIZE)) / 1024.0,
	)

	st := schema_tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
	fmt.println("===========================")
}

dump_table :: proc(db: ^Database, table_name: string) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := schema_tree(db)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
	if !found {
		fmt.printf("Error: Table '%s' not found.\n", table_name)
		return
	}

	fmt.printf("=== Dumping Table: %s (Root Page: %d) ===\n", table.name, table.root_page)
	table_tree := btree.init(db.pager, table.root_page)
	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		fmt.println("Error: Could not start cursor", err)
		return
	}
	defer btree.cursor_destroy(&cursor)

	row_count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err != .None {
			fmt.printf("Error reading cell: %v\n", get_err)
			btree.cursor_advance(&cursor)
			continue
		}

		fmt.printf("Row %d [RowID=%d]: ", row_count + 1, c.rowid)
		for val, i in c.values {
			if i > 0 do fmt.print(", ")
			col_name := "?"
			if i < len(table.columns) {
				col_name = table.columns[i].name
			}
			val_str := types.value_to_string(val, context.temp_allocator)
			fmt.printf("%s=%s", col_name, val_str)
		}

		fmt.println()
		btree.cursor_advance(&cursor)
		row_count += 1
	}
	fmt.printf("=== Total: %d rows ===\n", row_count)
}

print_schema :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := schema_tree(db)
	schema.print_ddl(&st)
}

print_schema_debug :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := schema_tree(db)
	schema.debug_print_all(&st)
}
