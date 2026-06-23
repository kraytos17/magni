package db

import "core:fmt"
import "core:sync"
import "src:btree"
import "src:pager"
import "src:schema"
import "src:snapshot"
import "src:types"

checkpoint :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	if db.latest_snapshot != 0 { snapshot.gc(db.pager, db.latest_snapshot, 100) }

	pager.flush_all(db.pager)
	update_header(db)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

integrity_check :: proc(db: ^Database) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	fmt.println("=== Integrity Check ===")
	if !verify_header(db) { fmt.println("✗ Database header is corrupted"); return false }

	fmt.println("✓ Database header is valid")
	_, err := pager.get_page(db.pager, db.schema_root_page)
	if err != .None { fmt.println("✗ Schema page is missing"); return false }
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
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	st := schema_tree(db)
	schema.debug_print_all(&st)
}

describe_table :: proc(db: ^Database, table_name: string) -> bool {
	if !db_check(db) { return false }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)

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
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
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
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
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
			if i < len(table.columns) { col_name = table.columns[i].name }
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
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	st := schema_tree(db)
	schema.print_ddl(&st)
}

print_schema_debug :: proc(db: ^Database) {
	if !db_check(db) { return }
	sync.lock(&db.mu); defer sync.unlock(&db.mu)
	st := schema_tree(db)
	schema.debug_print_all(&st)
}
