// Package admin holds the human-facing introspection and debug commands of the
// CLI: table listing, schema/DDL printing, stats, integrity checks, and
// snapshot presentation. It is an optional, presentation-oriented layer on top
// of the db API — an embedder that only wants
// db.open/execute/query/close does not need to link this package.
package admin

import "core:fmt"
import "core:log"
import "core:sync"
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

list_tables :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	if len(tables) == 0 {
		fmt.println("No tables found.")
		return
	}

	fmt.println("Tables:")
	for table in tables {
		fmt.printf("  %s\n", table.name)
	}
}

describe_table :: proc(database: ^db.Database, table_name: string) -> db.DB_Error {
	db.db_check(database) or_return
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)

	st := db.Schema_Tree(database)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
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

stats :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	page_count := pager.page_count(database.pager)
	fmt.printf("Path: %s\n", database.path)
	fmt.printf("Page size: %d bytes\n", types.PAGE_SIZE)
	fmt.printf("Total pages: %d\n", page_count)
	fmt.printf(
		"Database size: %d bytes (%.2f KB)\n",
		page_count * u32(types.PAGE_SIZE),
		f64(page_count * u32(types.PAGE_SIZE)) / 1024.0,
	)

	st := db.Schema_Tree(database)
	tables := schema.list_tables(&st, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
}

dump_table :: proc(database: ^db.Database, table_name: string) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	st := db.Schema_Tree(database)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
	if !found {
		log.errorf("Error: Table '%s' not found.", table_name)
		return
	}

	table_tree := btree.init(database.pager, table.root_page)
	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		log.errorf("Error: Could not start cursor: %v", err)
		return
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
}

print_schema :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	st := db.Schema_Tree(database)
	schema.print_ddl(&st)
}

print_schema_debug :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	st := db.Schema_Tree(database)
	schema.debug_print_all(&st)
}

print_tree_page :: proc(database: ^db.Database, page_num: u32) {
	if db.db_check(database) != .None { return }
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	st := db.Schema_Tree(database)
	btree.tree_debug_print_node(&st, page_num)
}

print_snapshots :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	sync.rw_mutex_shared_lock(&database.mu)
	defer sync.rw_mutex_unlock(&database.mu)
	if database.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return
	}
	snapshot.print_chain(database.pager, database.latest_snapshot)
}

print_snapshot_debug :: proc(database: ^db.Database) {
	if db.db_check(database) != .None { return }
	
	sync.lock(&database.mu)
	defer sync.unlock(&database.mu)
	if database.latest_snapshot == 0 {
		fmt.println("No snapshots.")
		return
	}
	snapshot.debug_print_chain(database.pager, database.latest_snapshot)
}
