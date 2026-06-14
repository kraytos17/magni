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
	path:             string,
	pager:            ^pager.Pager, // Page manager for disk I/O (owned pointer)
	is_new:           bool, // Tells if database was just created
	mu:               sync.Mutex, // Guards all access to pager and schema tree
	schema_root_page: u32, // Root page of the schema B-tree (latest COW'd version)
	latest_snapshot:  u32, // Latest snapshot page (0 = none)
	txn_state:        Transaction_State,
	txn_snapshot_id:  u64, // Monotonic snapshot ID counter
}

Header :: struct #packed {
	magic:          [13]u8,
	page_size:      u32le,
	page_count:     u32le,
	schema_version: u32le,
	schema_root_page: u32le, // Root page of schema B-tree (0 if uninitialized)
	latest_snapshot_page: u32le, // Latest snapshot page (0 = none)
	reserved:       [67]u8,
}

#assert(size_of(Header) == types.DATABASE_HEADER_SIZE)

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
		// Read schema_root_page from header
		page1, h_err := pager.get_page(db.pager, 1)
		if h_err != .None {
			fmt.eprintln("Error: Failed to read database header")
			close(db)
			return nil, false
		}
		header := (^Header)(raw_data(page1.data))
		db.schema_root_page = u32(header.schema_root_page)
		db.latest_snapshot = u32(header.latest_snapshot_page)
		pager.unpin_page(db.pager, 1)
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
	header.schema_version = 1

	// Allocate a dedicated page for the schema tree root (page 2 normally)
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

	schema_tree := btree.init(db.pager, db.schema_root_page)
	if !schema.init(&schema_tree) {
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

execute :: proc(db: ^Database, sql: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	stmt, ok := parser.parse(sql, context.temp_allocator)
	if !ok {
		fmt.eprintln("Error: Failed to parse SQL statement")
		return false
	}
	defer parser.statement_free(stmt, context.temp_allocator)

	schema_tree := btree.init(db.pager, db.schema_root_page)
	exec_ok, new_root := executor.execute(&schema_tree, stmt)
	db.schema_root_page = new_root
	return exec_ok
}

checkpoint :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	pager.flush_all(db.pager)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

// Begins a new transaction.
begin :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.txn_state == .ACTIVE {
		fmt.eprintln("Warning: Transaction already in progress")
		return false
	}
	db.txn_state = .ACTIVE
	fmt.println("BEGIN transaction")
	return true
}

// Commits the current transaction. Creates a snapshot page capturing the
// current schema tree root, links it into the snapshot chain, and updates
// the database header.
commit :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to commit")
		return false
	}

	db.txn_snapshot_id += 1
	snap_page, snap_ok := snapshot.create(db.pager, db.txn_snapshot_id, db.latest_snapshot, db.schema_root_page)
	if !snap_ok {
		fmt.eprintln("Error: Failed to create snapshot")
		return false
	}

	db.latest_snapshot = snap_page
	db.txn_state = .NONE

	// Prune old snapshots
	snapshot.prune(db.pager, db.latest_snapshot, 100)

	fmt.println("COMMIT transaction (snapshot", db.txn_snapshot_id, ")")
	return true
}

// Rolls back the current transaction. The schema root reverts to the last
// committed snapshot's root. COW'd pages from the aborted transaction remain
// in the file but are no longer referenced.
rollback :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	if db.txn_state != .ACTIVE {
		fmt.eprintln("Warning: No active transaction to roll back")
		return false
	}

	// Restore schema root from latest snapshot
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
	schema_tree := btree.init(db.pager, db.schema_root_page)

	tables := schema.list_tables(&schema_tree, context.temp_allocator)
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

	schema_tree := btree.init(db.pager, db.schema_root_page)
	schema.debug_print_all(&schema_tree)
}

describe_table :: proc(db: ^Database, table_name: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	schema_tree := btree.init(db.pager, db.schema_root_page)
	table, found := schema.find_table(&schema_tree, table_name, context.temp_allocator)
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

	schema_tree := btree.init(db.pager, db.schema_root_page)
	tables := schema.list_tables(&schema_tree, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
	fmt.println("===========================")
}

dump_table :: proc(db: ^Database, table_name: string) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	schema_tree := btree.init(db.pager, db.schema_root_page)
	table, found := schema.find_table(&schema_tree, table_name, context.temp_allocator)
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

	schema_tree := btree.init(db.pager, db.schema_root_page)
	schema.print_ddl(&schema_tree)
}

print_schema_debug :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	schema_tree := btree.init(db.pager, db.schema_root_page)
	schema.debug_print_all(&schema_tree)
}
