package db

import "core:fmt"
import "core:sync"
import "src:btree"
import "src:cell"
import "src:pager"
import "src:schema"
import "src:types"

checkpoint :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	if db.latest_snapshot != 0 {
		expire_snapshots_impl(db, DEFAULT_KEEP)
	}

	pager.wal_checkpoint(db.pager)
	update_header(db)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return .None
}

integrity_check :: proc(db: ^Database) -> DB_Error {
	db_check(db) or_return
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	if err := verify_header(db); err != .None {
		return .Corrupted
	}

	_, err := pager.get_page(db.pager, db.schema_root_page)
	if err != .None {
		return .IO_Error
	}
	defer pager.unpin_page(db.pager, db.schema_root_page)

	st := Schema_Tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)
	for table in tables {
		_, page_err := pager.get_page(db.pager, table.root_page)
		if page_err != .None {
			return .IO_Error
		}
		defer pager.unpin_page(db.pager, table.root_page)

		table_tree := btree.init(db.pager, table.root_page)
		if !btree.tree_verify(&table_tree) {
			fmt.printf("Integrity error: Table '%s' B-tree corrupted\n", table.name)
			return .Corrupted
		}
	}

	fmt.println("Integrity check passed.")
	return .None
}

list_tables :: proc(db: ^Database) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	st := Schema_Tree(db)
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

describe_table :: proc(db: ^Database, table_name: string) -> DB_Error {
	db_check(db) or_return
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)

	st := Schema_Tree(db)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
	if !found {
		return .Table_Not_Found
	}

	fmt.printf("%-20s %-8s %-3s %-4s %s\n", "name", "type", "pk", "null", "default")
	fmt.println("-------------------- -------- --- ---- -------")
	for col in table.columns {
		def := "NULL"
		if d, ok := col.default_value.?; ok {
			def = types.value_to_string(d, context.temp_allocator)
		}

		pk_str := "yes" if col.pk else ""
		nn_str := "no" if col.not_null else ""
		fmt.printf("%-20s %-8s %-3s %-4s %s\n", col.name, col.type, pk_str, nn_str, def)
	}
	return .None
}

stats :: proc(db: ^Database) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	page_count := pager.page_count(db.pager)
	fmt.printf("Path: %s\n", db.path)
	fmt.printf("Page size: %d bytes\n", types.PAGE_SIZE)
	fmt.printf("Total pages: %d\n", page_count)
	fmt.printf(
		"Database size: %d bytes (%.2f KB)\n",
		page_count * u32(types.PAGE_SIZE),
		f64(page_count * u32(types.PAGE_SIZE)) / 1024.0,
	)

	st := Schema_Tree(db)
	tables := schema.list_tables(&st, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
}

dump_table :: proc(db: ^Database, table_name: string) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	st := Schema_Tree(db)
	table, found := schema.find_table(&st, table_name, context.temp_allocator)
	if !found {
		fmt.printf("Error: Table '%s' not found.\n", table_name)
		return
	}

	table_tree := btree.init(db.pager, table.root_page)
	cursor, err := btree.cursor_start(&table_tree, context.temp_allocator)
	if err != .None {
		fmt.println("Error: Could not start cursor", err)
		return
	}
	defer btree.cursor_destroy(&cursor)

	for i in 0 ..< len(table.columns) {
		if i > 0 { fmt.print(" | ") }
		fmt.print(table.columns[i].name)
	}

	fmt.println()
	for i in 0 ..< len(table.columns) {
		if i > 0 { fmt.print("-+-") } else { fmt.print("-") }
		for _ in 0 ..< len(table.columns[i].name) { fmt.print("-") }
	}

	fmt.println()
	row_count := 0
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		defer cell.destroy(&c, context.temp_allocator)
		if get_err != .None {
			btree.cursor_advance(&cursor)
			continue
		}
		for vi in 0 ..< len(c.values) {
			if vi > 0 { fmt.print(" | ") }
			val_str := types.value_to_string(c.values[vi], context.temp_allocator)
			fmt.print(val_str)
		}

		fmt.println()
		btree.cursor_advance(&cursor)
		row_count += 1
	}
	fmt.printf("(%d rows)\n", row_count)
}

print_schema :: proc(db: ^Database) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	st := Schema_Tree(db)
	schema.print_ddl(&st)
}

print_schema_debug :: proc(db: ^Database) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	st := Schema_Tree(db)
	schema.debug_print_all(&st)
}

print_tree_page :: proc(db: ^Database, page_num: u32) {
	if db_check(db) != .None { return }
	sync.lock(&db.mu)
	defer sync.unlock(&db.mu)
	st := Schema_Tree(db)
	btree.tree_debug_print_node(&st, page_num)
}
