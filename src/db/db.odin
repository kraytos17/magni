package db

import "core:log"
import "core:strings"
import "core:sync"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

DEFAULT_KEEP :: 20

Open_Config :: struct {
	wal_size_threshold      : int, // If non-zero, WAL pages are checkpointed when WAL reaches this many pages
	snapshot_batch_threshold: int, // 0 = every mutation, N = batch N mutations
}

DB_Error :: enum u8 {
	None,
	Invalid_Handle,
	Alloc_Failed,
	IO_Error,
	Corrupted,
	Schema_Newer,
	Page_Size_Mismatch,
	Parse_Error,
	Table_Not_Found,
	Snapshot_Not_Found,
	Snapshot_Failed,
	Snapshot_Expired,
	Transaction_Error,
	No_Ref,
	Nothing_To_Roll,
	Not_Supported,
	Unsupported_Format,
}

db_error_string :: proc(err: DB_Error) -> string {
	switch err {
	case .None:
		return ""
	case .Invalid_Handle:
		return "Invalid database handle"
	case .Alloc_Failed:
		return "Memory allocation failed"
	case .IO_Error:
		return "I/O error"
	case .Corrupted:
		return "Database file is corrupted"
	case .Schema_Newer:
		return "Database schema version is too new"
	case .Page_Size_Mismatch:
		return "Page size mismatch"
	case .Parse_Error:
		return "Failed to parse SQL statement"
	case .Table_Not_Found:
		return "Table not found"
	case .Snapshot_Not_Found:
		return "Snapshot not found"
	case .Snapshot_Failed:
		return "Failed to load or create snapshot"
	case .Snapshot_Expired:
		return "Snapshot has been expired"
	case .Transaction_Error:
		return "Transaction error"
	case .No_Ref:
		return "No ref found"
	case .Nothing_To_Roll:
		return "Nothing to roll forward to"
	case .Not_Supported:
		return "Operation not supported"
	case .Unsupported_Format:
		return "Unsupported page format version (export data and reimport)"
	case:
		return "Unknown error"
	}
}

// Txn_State tracks whether a transaction is currently in progress.
Txn_State :: enum u8 {
	None,
	Active,
}

Database :: struct {
	pager                   : ^pager.Pager,
	path                    : string,
	is_new                  : bool,
	schema_root_page        : u32,
	latest_snapshot         : u32,
	txn_snapshot_id         : u64,
	txn_state               : Txn_State,
	txn_start_file_len      : u64,
	snapshot_index          : map[u64]u32,
	refs_page               : u32,
	snapshot_batch_count    : int,
	snapshot_batch_threshold: int,
	wal_size_threshold      : int, // 0 = disabled; auto-checkpoint when the WAL reaches this many frames
	table_cache             : schema.Table_Cache, // in-memory catalog cache; invalidated on schema-root change
	txn_pending             : executor.Pending_Roots, // staged data + index roots; flushed at COMMIT (explicit txn only)
	mu                      : sync.RW_Mutex, // guards database state; see docs/concurrency.md (acquire before pager.mutex)
}

Header :: struct #packed {
	magic               : [13]u8,
	page_size           : u32le,
	page_count          : u32le,
	schema_version      : u32le,
	page_format_version : u32le,
	schema_root_page    : u32le,
	latest_snapshot_page: u32le,
	snapshot_id_counter : u64le,
	first_free_page     : u32le,
	refs_page           : u32le,
	reserved            : [47]u8,
}
#assert(size_of(Header) == types.DATABASE_HEADER_SIZE)

Schema_Tree :: proc(db: ^Database) -> btree.Tree {
	return btree.init(db.pager, db.schema_root_page)
}

open :: proc(path: string, cfg: Open_Config = {}) -> (^Database, DB_Error) {
	db := new(Database)
	if db == nil {
		return nil, .Alloc_Failed
	}

	db.path = strings.clone(path); db.latest_snapshot = 0
	p, err := pager.open(path)
	if err != nil {
		delete(db.path)
		free(db)
		return nil, .IO_Error
	}

	db.pager = p
	if cfg.snapshot_batch_threshold >
	   0 { db.snapshot_batch_threshold = cfg.snapshot_batch_threshold }
	if cfg.wal_size_threshold > 0 { db.wal_size_threshold = cfg.wal_size_threshold }

	db.is_new = (db.pager.file_len == 0)
	db.txn_state = .None
	db.txn_snapshot_id = 0
	db.snapshot_index = make(map[u64]u32, 128)
	db.table_cache.allocator = context.allocator
	if db.is_new {
		log.info("Initializing new database...")
		if init_err := initialize(db); init_err != .None {
			close(db)
			return nil, init_err
		}
		db.pager.page_format_version = types.PAGE_FORMAT_VERSION
	} else {
		if v_err := load_existing(db); v_err != .None {
			close(db)
			return nil, v_err
		}
	}
	return db, .None
}

// load_existing validates the header of an on-disk database and restores all
// persisted state: header fields, page-format gate, and the snapshot index.
// On any failure the caller closes db and propagates the error.
@(private = "file")
load_existing :: proc(db: ^Database) -> DB_Error {
	if v_err := verify_header(db); v_err != .None { return v_err }
	if h_err := load_header_fields(db); h_err != .None { return h_err }
	return rebuild_snapshot_index(db)
}

// load_header_fields reads page 1 into db's runtime fields and gates the page
// format version. Self-heals a freelist head pointing past EOF.
@(private = "file")
load_header_fields :: proc(db: ^Database) -> DB_Error {
	page1, h_err := pager.get_page(db.pager, 1)
	if h_err != .None { return .IO_Error }

	header := (^Header)(raw_data(page1.data))
	db.schema_root_page = u32(header.schema_root_page)
	db.latest_snapshot = u32(header.latest_snapshot_page)
	db.txn_snapshot_id = u64(header.snapshot_id_counter)
	db.pager.first_free_page = u32(header.first_free_page)
	if db.pager.first_free_page > pager.page_count(db.pager) {
		// Self-heal: a header persisted before an uncompleted shrink may
		// reference pages past EOF. Dropping the freelist leaks space until
		// the next GC rebuilds it, but can never misread: freelist links are
		// validated on use.
		log.warnf(
			"Database header references free page %d past end of file; dropping freelist",
			db.pager.first_free_page,
		)
		db.pager.first_free_page = 0
	}

	db.refs_page = u32(header.refs_page)
	pfv := u32(header.page_format_version)
	if pfv == 0 { pfv = u32(header.schema_version) }
	if pfv != u32(types.PAGE_FORMAT_VERSION) {
		pager.unpin_page(db.pager, 1)
		return .Unsupported_Format
	}

	db.pager.page_format_version = pfv
	pager.unpin_page(db.pager, 1)
	return .None
}

// rebuild_snapshot_index walks the snapshot chain from the latest snapshot,
// indexing each packed header by id. Returns .Unsupported_Format on an old
// single-header page (hard break, no migration); stops the walk on other
// mid-chain corruption.
@(private = "file")
rebuild_snapshot_index :: proc(db: ^Database) -> DB_Error {
	page := db.latest_snapshot
	for page != 0 {
		pg, pg_err := pager.get_page(db.pager, page)
		if pg_err != .None { break }

		next_page: u32
		if headers := snapshot.headers_on_page(pg.data); headers != nil {
			for i := 0; i < len(headers); i += 1 {
				db.snapshot_index[headers[i].snapshot_id] = page
			}
			next_page = headers[0].prev_snapshot
		} else {
			// Snapshot magic without a packed count is an old-format page:
			// reject the file. Anything else is corruption mid-chain.
			h := (^snapshot.Snapshot_Header)(raw_data(pg.data))
			if string(h.magic[:]) == snapshot.SNAPSHOT_MAGIC {
				pager.unpin_page(db.pager, page)
				return .Unsupported_Format
			}

			pager.unpin_page(db.pager, page)
			break
		}

		pager.unpin_page(db.pager, page)
		page = next_page
	}
	return .None
}

// maybe_auto_checkpoint checkpoints the WAL once it grows past
// db.wal_size_threshold frames (0 = disabled). Runs after a WAL commit so a
// heavy write or a large explicit transaction can reclaim the WAL proactively.
@(private)
maybe_auto_checkpoint :: proc(db: ^Database) {
	if db.wal_size_threshold > 0 && db.pager.wal_state.frame_count >= u32(db.wal_size_threshold) {
		if pager.wal_checkpoint(db.pager) == .None {
			update_header(db)
		}
	}
}

close :: proc(db: ^Database) {
	if db == nil { return }
	sync.rw_mutex_lock(&db.mu)
	// NOTE: explicit unlock before free at the end (not defer): the mutex
	// lives inside db, so unlocking after free(db) is heap-use-after-free.
	if db.snapshot_batch_count > 0 {
		db.snapshot_batch_threshold = 1
		db.snapshot_batch_count = 1
		capture_snapshot(db, .COMMIT)
		wal_update_header(db)
	}

	update_header(db)
	if db.pager != nil {
		if err := pager.close(db.pager); err != .None {
			log.warnf("error closing database: %v", err)
		}
	}

	schema.table_cache_free(&db.table_cache)
	executor.pending_clear(&db.txn_pending)
	delete(db.snapshot_index)
	delete(db.path)
	sync.rw_mutex_unlock(&db.mu)
	free(db)
}

@(private = "file")
initialize :: proc(db: ^Database) -> DB_Error {
	page1, err := pager.allocate_page(db.pager)
	if err != .None {
		return .Alloc_Failed
	}
	defer pager.unpin_page(db.pager, page1.page_num)

	header := (^Header)(raw_data(page1.data))
	copy(header.magic[:], types.MAGIC_STRING)
	header.page_size = u32le(types.PAGE_SIZE)
	header.page_count = 1
	header.schema_version = u32le(types.SCHEMA_VERSION)
	header.page_format_version = u32le(types.PAGE_FORMAT_VERSION)
	schema_page, s_err := pager.allocate_page(db.pager)
	if s_err != .None {
		return .Alloc_Failed
	}

	defer pager.unpin_page(db.pager, schema_page.page_num)
	if !btree.init_slot_leaf_page(schema_page.data, schema_page.page_num) {
		return .Alloc_Failed
	}

	pager.mark_dirty(db.pager, schema_page.page_num)
	db.schema_root_page = schema_page.page_num
	header.schema_root_page = u32le(schema_page.page_num); header.latest_snapshot_page = 0
	header.page_count = u32le(schema_page.page_num)
	st := Schema_Tree(db)
	if !schema.init(&st) {
		return .Alloc_Failed
	}

	refs_page := snapshot.create_refs_page(db.pager)
	if refs_page == 0 {
		return .Alloc_Failed
	}

	pager.wal_begin_txn(db.pager)
	pager.mark_dirty(db.pager, page1.page_num)
	header.refs_page = u32le(refs_page)
	db.refs_page = refs_page
	pager.wal_commit_txn(db.pager)
	return .None
}

verify_header :: proc(db: ^Database) -> DB_Error {
	page, err := pager.get_page(db.pager, 1)
	if err != .None { return .IO_Error }
	defer pager.unpin_page(db.pager, 1)

	header := (^Header)(raw_data(page.data))
	if string(header.magic[:len(types.MAGIC_STRING)]) != types.MAGIC_STRING {
		return .Corrupted
	}

	sv := u32(header.schema_version)
	if sv > types.SCHEMA_VERSION {
		return .Schema_Newer
	}
	if header.page_size != u32le(types.PAGE_SIZE) {
		return .Page_Size_Mismatch
	}
	return .None
}

// record_main_ref points the MAIN branch ref at the given snapshot id.
record_main_ref :: proc(db: ^Database, snap_id: u64) {
	snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snap_id, .BRANCH, false)
}

// wal_update_header persists a header change inside a WAL transaction.
// Covers the bare begin/update/commit sites; transaction commit and
// multi-page initializers keep their explicit framing (different
// commit-error semantics).
wal_update_header :: proc(db: ^Database) {
	pager.wal_begin_txn(db.pager)
	update_header(db)
	pager.wal_commit_txn(db.pager)
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

db_check :: proc(db: ^Database) -> DB_Error {
	if db == nil || db.pager == nil {
		return .Invalid_Handle
	}
	return .None
}
