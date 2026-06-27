// Package db is the top-level database handle: open/close, header, schema access, coordination.
package db

import "core:fmt"
import "core:strings"
import "core:sync"
import "src:btree"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

Transaction_State :: enum {
	NONE,
	ACTIVE,
}

DEFAULT_KEEP :: 100

Database :: struct {
	path:                string,
	pager:               ^pager.Pager,
	is_new:              bool,
	mu:                  sync.Mutex, // serializes all db.* operations
	schema_root_page:    u32,
	latest_snapshot:     u32, // cached page number of the most recent snapshot
	refs_page:           u32, // page storing named refs (branches/tags) and rollforward log
	txn_state:           Transaction_State,
	txn_snapshot_id:     u64, // monotonically increasing snapshot ID counter
	txn_start_file_len:  i64, // file_len snapshot at BEGIN, restored on ROLLBACK
	snapshot_index:      map[u64]u32, // snapshot_id → page_num (built on open)
	table_roots:         map[string]u32, // {table_name → root_page} cache for manifests
	table_roots_dirty:   bool, // invalidated after schema mutation
}

Header :: struct #packed {
	magic:                [13]u8,
	page_size:            u32le,
	page_count:           u32le,
	schema_version:       u32le,
	schema_root_page:     u32le,
	latest_snapshot_page: u32le,
	snapshot_id_counter:  u64le,
	first_free_page:      u32le,
	refs_page:            u32le,
	reserved:             [51]u8,
}

#assert(size_of(Header) == types.DATABASE_HEADER_SIZE)

schema_tree :: proc(db: ^Database) -> btree.Tree {
	return btree.init(db.pager, db.schema_root_page)
}

// Open the database at path. Initializes the pager, loads the header, builds the
// snapshot index, and creates the refs page. Returns nil on error.
open :: proc(path: string) -> (^Database, bool) {
	db := new(Database)
	if db == nil {
		fmt.eprintln("Error: Failed to allocate database handle")
		return nil, false
	}

	db.path = strings.clone(path); db.latest_snapshot = 0
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
	db.snapshot_index = make(map[u64]u32, 128); db.table_roots = make(map[string]u32)
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
		db.refs_page = u32(header.refs_page)
		pager.unpin_page(db.pager, 1)

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

// Close the database: update header, close pager, free all resources.
close :: proc(db: ^Database) {
	if db == nil { return }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	update_header(db)
	if db.pager != nil {
		if err := pager.close(db.pager); err != .None {
			fmt.eprintln("Warning: error closing database:", err)
		}
	}
	delete(db.snapshot_index); delete(db.table_roots); delete(db.path); free(db)
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
	header.schema_version = u32le(types.SCHEMA_VERSION)
	schema_page, s_err := pager.allocate_page(db.pager)
	if s_err != .None {
		fmt.eprintln("Error: Failed to allocate schema root page:", s_err)
		return false
	}
	defer pager.unpin_page(db.pager, schema_page.page_num)

	btree.init_leaf_page(schema_page.data, schema_page.page_num)
	pager.mark_dirty(db.pager, schema_page.page_num)
	db.schema_root_page = schema_page.page_num
	header.schema_root_page = u32le(schema_page.page_num); header.latest_snapshot_page = 0
	header.page_count = u32le(schema_page.page_num)
	st := schema_tree(db)
	if !schema.init(&st) {
		fmt.eprintln("Error: Failed to initialize schema tree")
		return false
	}

	refs_page := snapshot.create_refs_page(db.pager)
	if refs_page == 0 {
		fmt.eprintln("Error: Failed to create refs page")
		return false
	}

	pager.wal_begin_txn(db.pager)
	pager.mark_dirty(db.pager, page1.page_num)
	header.refs_page = u32le(refs_page)
	db.refs_page = refs_page
	pager.wal_commit_txn(db.pager)
	return true
}

verify_header :: proc(db: ^Database) -> bool {
	page, err := pager.get_page(db.pager, 1)
	if err != .None { return false }
	defer pager.unpin_page(db.pager, 1)

	header := (^Header)(raw_data(page.data))
	if string(header.magic[:len(types.MAGIC_STRING)]) != types.MAGIC_STRING {
		return false
	}
	sv := u32(header.schema_version)
	if sv > types.SCHEMA_VERSION {
		fmt.eprintln("Error: Database schema version", sv, "is newer than expected", types.SCHEMA_VERSION)
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
	header.page_count = u32le(pager.page_count(db.pager))
	header.schema_root_page = u32le(db.schema_root_page)
	header.latest_snapshot_page = u32le(db.latest_snapshot)
	header.snapshot_id_counter = u64le(db.txn_snapshot_id)
	header.first_free_page = u32le(db.pager.first_free_page)
	header.refs_page = u32le(db.refs_page)
	pager.mark_dirty(db.pager, 1)
}

db_check :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}
	return true
}

ensure_table_roots :: proc(db: ^Database) {
	if !db.table_roots_dirty && len(db.table_roots) > 0 { return }
	st := schema_tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)

	clear(&db.table_roots)
	for t in tables { db.table_roots[t.name] = t.root_page }
	db.table_roots_dirty = false
}
