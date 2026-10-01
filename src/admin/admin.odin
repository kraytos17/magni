// Package admin holds the human-facing introspection and debug commands of the
// CLI: table listing, schema/DDL printing, stats, integrity checks, and
// snapshot presentation.
package admin

import "core:fmt"
import "core:sync"
import "core:time"
import "src:btree"
import "src:cell"
import "src:db"
import "src:executor"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

checkpoint :: proc(database: ^db.Database) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	if database.latest_snapshot != 0 {
		db.expire_snapshots_impl(database, db.DEFAULT_KEEP)
	}

	pager.wal_checkpoint(database.pager)
	db.update_header(database)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return .None
}

// vacuum rebuilds every table's B-tree into fresh, densely packed pages
// (COW-safe: old pages stay readable by snapshots and are reclaimed by the next
// GC pass). This reclaims space lost to deletes that do not merge leaves.
vacuum :: proc(database: ^db.Database) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	new_root := st.root
	for table in tables {
		table_tree := btree.init(database.pager, table.root_page)
		vac_root, v_err := btree.tree_vacuum(&table_tree)
		if v_err != .None {
			return .Corrupted
		}

		updated_root, up_ok := schema.update_root_page_cow(&st, table.name, vac_root)
		if !up_ok {
			return .Corrupted
		}

		st.root = updated_root
		new_root = updated_root
	}

	database.schema_root_page = new_root
	pager.wal_begin_txn(database.pager)
	db.update_header(database)
	pager.wal_commit_txn(database.pager)
	fmt.println("Vacuum complete: tables rebuilt into packed pages")
	return .None
}

integrity_check :: proc(database: ^db.Database) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	if err := db.verify_header(database); err != .None {
		return .Corrupted
	}

	_, err := pager.get_page(database.pager, database.schema_root_page)
	if err != .None {
		return .IO_Error
	}
	defer pager.unpin_page(database.pager, database.schema_root_page)

	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	for table in tables {
		_, page_err := pager.get_page(database.pager, table.root_page)
		if page_err != .None {
			return .IO_Error
		}
		defer pager.unpin_page(database.pager, table.root_page)

		table_tree := btree.init(database.pager, table.root_page)
		if !btree.tree_verify_if_enabled(&table_tree) {
			fmt.printf("Integrity error: Table '%s' B-tree corrupted\n", table.name)
			return .Corrupted
		}
	}

	fmt.println("Integrity check passed.")
	return .None
}

list_tables :: proc(database: ^db.Database) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	cols := []string{"name"}
	rows := make([][]string, len(tables), context.temp_allocator)
	for table, i in tables {
		row := make([]string, 1, context.temp_allocator)
		row[0] = table.name
		rows[i] = row
	}

	executor.render_table(cols, rows)
	fmt.printf("(%d rows)\n", len(tables))
	return .None
}

// resolve_table builds the schema tree and looks up a single table by name.
// Shared by describe_table and dump_table. The returned table borrows from
// `allocator` (callers pass context.temp_allocator), matching schema.find_table.
@(private)
resolve_table :: proc(
	database: ^db.Database,
	table_name: string,
	allocator := context.allocator,
) -> (types.Table, bool) {
	st := db.Schema_Tree(database)
	return schema.get_table(&st, table_name, allocator)
}

describe_table :: proc(database: ^db.Database, table_name: string) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	table, found := resolve_table(database, table_name, context.temp_allocator)
	if !found {
		return .Table_Not_Found
	}

	cols := []string{"name", "type", "pk", "null", "default"}
	table_rows := make([][]string, len(table.columns), context.temp_allocator)
	for i in 0 ..< len(table.columns) {
		col := table.columns[i]
		def := "NULL"
		if d, ok := col.default_value.?; ok {
			def = types.value_to_string(d, context.temp_allocator)
		}

		pk_str := "yes" if col.pk else ""
		nn_str := "no" if col.not_null else ""
		row := make([]string, 5, context.temp_allocator)
		row[0] = col.name
		row[1] = fmt.aprintf("%s", col.type, allocator = context.temp_allocator)
		row[2] = pk_str
		row[3] = nn_str
		row[4] = def
		table_rows[i] = row
	}

	executor.render_table(cols, table_rows)
	return .None
}

stats :: proc(database: ^db.Database) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	page_count := pager.page_count(database.pager)
	size_bytes := u64(page_count) * u64(types.PAGE_SIZE)
	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	cols := []string{"property", "value"}
	rows := [][]string{
		{"path", database.path},
		{"page_size", fmt.aprintf("%d", types.PAGE_SIZE, allocator = context.temp_allocator)},
		{"total_pages", fmt.aprintf("%d", page_count, allocator = context.temp_allocator)},
		{
			"database_size",
			fmt.aprintf(
				"%d bytes (%.2f KB)",
				size_bytes,
				f64(size_bytes) / 1024.0,
				allocator = context.temp_allocator,
			),
		},
		{"total_tables", fmt.aprintf("%d", len(tables), allocator = context.temp_allocator)},
	}

	executor.render_table(cols, rows)
	return .None
}

dump_table :: proc(database: ^db.Database, table_name: string) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	table, found := resolve_table(database, table_name, context.temp_allocator)
	if !found {
		return .Table_Not_Found
	}

	table_tree := btree.init(database.pager, table.root_page)
	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		return .IO_Error
	}
	defer btree.cursor_destroy(&cursor)

	cols := make([]string, len(table.columns), context.temp_allocator)
	for i in 0 ..< len(table.columns) { cols[i] = table.columns[i].name }

	table_rows := make([dynamic][]string, context.temp_allocator)
	row_count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		defer cell.destroy(&c, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}

		row_strs := make([]string, len(c.values), context.temp_allocator)
		for vi in 0 ..< len(c.values) {
			row_strs[vi] = types.value_to_string(c.values[vi], context.temp_allocator)
		}

		append(&table_rows, row_strs)
		btree.cursor_advance(&cursor)
		row_count += 1
	}

	executor.render_table(cols, table_rows[:])
	fmt.printf("(%d rows)\n", row_count)
	return .None
}

print_schema :: proc(database: ^db.Database, debug := false) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	st := db.Schema_Tree(database)
	if debug {
		schema.debug_print_all(&st)
	} else {
		schema.print_ddl(&st)
	}
	return .None
}

print_tree_page :: proc(database: ^db.Database, page_num: u32) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	st := db.Schema_Tree(database)
	btree.tree_debug_print_node(&st, page_num)
	return .None
}

print_snapshots :: proc(database: ^db.Database, debug := false) -> db.DB_Error {
	db.db_check(database) or_return
	sync.rw_mutex_shared_lock(&database.mu)
	// NOTE: shared locks must release with shared_unlock. The write unlock
	// clears only the writer bit and leaks the reader count, which hangs the
	// next write-lock (e.g. db.close) in sema_wait forever.
	defer sync.rw_mutex_shared_unlock(&database.mu)
	if database.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return .None
	}

	infos := snapshot.chain_infos(
		database.pager,
		database.latest_snapshot,
		context.temp_allocator,
	)

	cols := []string{"id", "op", "state", "timestamp", "tag"}
	rows := make([][]string, len(infos), context.temp_allocator)
	for info, i in infos {
		row := make([]string, 5, context.temp_allocator)
		row[0] = fmt.aprintf("%d", info.id, allocator = context.temp_allocator)
		row[1] = fmt.aprintf("%s", info.operation, allocator = context.temp_allocator)
		row[2] = fmt.aprintf("%s", info.state, allocator = context.temp_allocator)
		row[3] = format_snapshot_ts(info.timestamp, context.temp_allocator)
		row[4] = info.tag
		rows[i] = row
	}

	executor.render_table(cols, rows)
	fmt.printf("(%d rows)\n", len(infos))
	return .None
}

// format_snapshot_ts renders unix-microsecond timestamps as UTC wall time.
// Raw values stay available via .snapshot_debug.
@(private="file")
format_snapshot_ts :: proc(micros: u64, allocator := context.allocator) -> string {
	t := time.unix(i64(micros / 1_000_000), i64(micros % 1_000_000) * 1000)
	year, month, day := time.date(t)
	hour, min, sec := time.clock_from_time(t)
	return fmt.aprintf(
		"%04d-%02d-%02d %02d:%02d:%02d",
		year,
		int(month),
		day,
		hour,
		min,
		sec,
		allocator = allocator,
	)
}
