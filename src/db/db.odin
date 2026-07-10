package db

import "core:encoding/endian"
import "core:log"
import "core:strings"
import "core:sync"
import "src:btree"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

DEFAULT_KEEP :: 20

Open_Config :: struct {
	wal_size_threshold:       int, // If non-zero, WAL pages are checkpointed when WAL reaches this many pages
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
	case:
		return "Unknown error"
	}
}

Database :: struct {
	pager:                    ^pager.Pager,
	path:                     string,
	is_new:                   bool,
	schema_root_page:         u32,
	latest_snapshot:          u32,
	txn_snapshot_id:          u64,
	txn_state:                enum {
		NONE,
		ACTIVE,
	},
	txn_start_file_len:       u64,
	snapshot_index:           map[u64]u32,
	refs_page:                u32,
	snapshot_batch_count:     int,
	snapshot_batch_threshold: int,
	mu:                       sync.RW_Mutex,
}

Header :: struct #packed {
	magic:                [13]u8,
	page_size:            u32le,
	page_count:           u32le,
	schema_version:       u32le,
	page_format_version:  u32le,
	schema_root_page:     u32le,
	latest_snapshot_page: u32le,
	snapshot_id_counter:  u64le,
	first_free_page:      u32le,
	refs_page:            u32le,
	reserved:             [47]u8,
}

#assert(size_of(Header) == types.DATABASE_HEADER_SIZE)

Schema_Tree :: proc(db: ^Database) -> btree.Tree {
	return btree.init(db.pager, db.schema_root_page)
}

open :: proc(path: string) -> (^Database, DB_Error) {
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
	db.is_new = (db.pager.file_len == 0)
	db.txn_state = .NONE
	db.txn_snapshot_id = 0
	db.snapshot_index = make(map[u64]u32, 128)
	if db.is_new {
		log.info("Initializing new database...")
		if init_err := initialize(db); init_err != .None {
			close(db)
			return nil, init_err
		}
		db.pager.page_format_version = types.PAGE_FORMAT_VERSION
	} else {
		if v_err := verify_header(db); v_err != .None {
			close(db)
			return nil, v_err
		}

		page1, h_err := pager.get_page(db.pager, 1)
		if h_err != .None {
			close(db)
			return nil, .IO_Error
		}

		header := (^Header)(raw_data(page1.data))
		db.schema_root_page = u32(header.schema_root_page)
		db.latest_snapshot = u32(header.latest_snapshot_page)
		db.txn_snapshot_id = u64(header.snapshot_id_counter)
		db.pager.first_free_page = u32(header.first_free_page)
		db.refs_page = u32(header.refs_page)
		pfv := u32(header.page_format_version)
		if pfv == 0 { pfv = u32(header.schema_version) }

		db.pager.page_format_version = pfv
		pager.unpin_page(db.pager, 1)
		page := db.latest_snapshot
		for page != 0 {
			pg, pg_err := pager.get_page(db.pager, page)
			if pg_err != .None { break }

			next_page: u32
			count := int(endian.unchecked_get_u32le(pg.data[:4]))
			if count > 0 && count <= snapshot.MAX_HEADERS_PER_PAGE {
				headers := transmute([]snapshot.Snapshot_Header)pg.data[snapshot.HEADER_PREFIX_SIZE:snapshot.HEADER_PREFIX_SIZE +
				count * size_of(snapshot.Snapshot_Header)]
				for i := 0; i < count; i += 1 {
					db.snapshot_index[headers[i].snapshot_id] = page
				}
				next_page = headers[0].prev_snapshot
			} else {
				h := (^snapshot.Snapshot_Header)(raw_data(pg.data))
				if string(h.magic[:]) != snapshot.SNAPSHOT_MAGIC {
					pager.unpin_page(db.pager, page)
					break
				}

				db.snapshot_index[h.snapshot_id] = page
				next_page = h.prev_snapshot
			}
			pager.unpin_page(db.pager, page)
			page = next_page
		}
	}
	return db, .None
}

close :: proc(db: ^Database) {
	if db == nil { return }
	sync.rw_mutex_lock(&db.mu)
	defer sync.rw_mutex_unlock(&db.mu)

	if db.snapshot_batch_count > 0 {
		db.snapshot_batch_threshold = 1
		db.snapshot_batch_count = 1
		st := Schema_Tree(db)
		schema_tables := schema.list_tables(&st, context.temp_allocator)
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
			.COMMIT,
		)
		if snap_ok {
			db.snapshot_index[snap_id] = snap_page
			db.latest_snapshot = snap_page
			snapshot.set_ref(db.pager, db.refs_page, snapshot.MAIN_REF, snap_id, .BRANCH, false)
		}

		pager.wal_begin_txn(db.pager)
		update_header(db)
		pager.wal_commit_txn(db.pager)
	}

	update_header(db)
	if db.pager != nil {
		if err := pager.close(db.pager); err != .None {
			log.warnf("error closing database: %v", err)
		}
	}
	delete(db.snapshot_index); delete(db.path); free(db)
}

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

	btree.init_leaf_page(schema_page.data, schema_page.page_num)
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
