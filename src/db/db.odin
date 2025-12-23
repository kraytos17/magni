package db

import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"
import "src:utils"

// Database handle
Database :: struct {
	path:   string,
	pager:  ^pager.Pager,
	is_new: bool,
}

// Database header stored at the beginning of page 0
Database_Header :: struct #packed {
	magic:          [13]u8,
	page_size:      u32,
	page_count:     u32,
	schema_version: u32,
	reserved:       [75]u8, // Reserved for future use
}

MAGIC_STRING :: "MAGNI_DB_v1.0"

// Open or create a database
db_open :: proc(path: string) -> (^Database, bool) {
	db := new(Database)
	if db == nil {
		fmt.eprintln("Error: Failed to allocate database handle")
		return nil, false
	}

	db.path = strings.clone(path)
	p, err := pager.pager_open(path)
	if err != nil {
		fmt.eprintln("Error: Failed to open database file:", err)
		delete(db.path)
		free(db)
		return nil, false
	}

	db.pager = p
	db.is_new = (db.pager.file_len == 0)
	if db.is_new {
		fmt.println("Initializing new database...")
		if !db_initialize(db) {
			db_close(db)
			return nil, false
		}
	} else {
		if !db_verify_header(db) {
			fmt.eprintln("Error: Invalid or corrupted database file")
			db_close(db)
			return nil, false
		}
	}
	return db, true
}

// Initialize a new database
db_initialize :: proc(db: ^Database) -> bool {
	page, err := pager.pager_allocate_page(db.pager)
	if err != nil {
		fmt.eprintln("Error: Failed to allocate page 0")
		return false
	}
	if page.page_num != 0 {
		fmt.eprintln("Error: Allocated page was not 0")
		return false
	}

	header := Database_Header {
		page_size      = types.PAGE_SIZE,
		page_count     = 1,
		schema_version = 1,
	}

	for c, i in MAGIC_STRING {
		if i < len(header.magic) {
			header.magic[i] = u8(c)
		}
	}

	db_write_header(page.data, header)
	pager.pager_mark_dirty(db.pager, 0)
	pager.pager_flush_page(db.pager, 0)
	if !schema.schema_init(db.pager) {
		fmt.eprintln("Error: Failed to initialize schema")
		return false
	}
	return true
}

// Verify database header
db_verify_header :: proc(db: ^Database) -> bool {
	page, err := pager.pager_get_page(db.pager, 0)
	if err != nil {
		return false
	}

	header := db_read_header(page.data)
	magic_match := true
	for c, i in MAGIC_STRING {
		if i < len(header.magic) && header.magic[i] != u8(c) {
			magic_match = false
			break
		}
	}

	if !magic_match {
		return false
	}
	if header.page_size != types.PAGE_SIZE {
		fmt.eprintln("Error: Page size mismatch")
		return false
	}
	return true
}

/// Read database header
db_read_header :: proc(page_data: []u8) -> Database_Header {
	header: Database_Header
	if len(page_data) < size_of(Database_Header) {
		return header
	}

	offset := 0
	for i in 0 ..< len(header.magic) {
		header.magic[i] = page_data[offset + i]
	}

	offset += len(header.magic)
	header.page_size, _ = utils.read_u32_le(page_data, offset)
	offset += 4

	header.page_count, _ = utils.read_u32_le(page_data, offset)
	offset += 4

	header.schema_version, _ = utils.read_u32_le(page_data, offset)
	offset += 4

	return header
}

// Write database header
db_write_header :: proc(page_data: []u8, header: Database_Header) {
	if len(page_data) < size_of(Database_Header) {
		return
	}

	offset := 0
	for i in 0 ..< len(header.magic) {
		page_data[offset + i] = header.magic[i]
	}

	offset += len(header.magic)
	utils.write_u32_le(page_data, offset, header.page_size)
	offset += 4

	utils.write_u32_le(page_data, offset, header.page_count)
	offset += 4

	utils.write_u32_le(page_data, offset, header.schema_version)
	offset += 4
}

// Close database
db_close :: proc(db: ^Database) {
	if db == nil {
		return
	}
	if db.pager != nil {
		pager.pager_close(db.pager)
	}
	delete(db.path)
	free(db)
}

// Execute a SQL string
db_execute :: proc(db: ^Database, sql: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	stmt, ok := parser.parse(sql)
	if !ok {
		fmt.eprintln("Error: Failed to parse SQL statement")
		return false
	}

	defer parser.statement_free(stmt)
	return executor.execute_statement(db.pager, stmt)
}

// List all tables in the database
db_list_tables :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}
	schema.schema_debug_print_all(db.pager)
}

// Get table information
db_describe_table :: proc(db: ^Database, table_name: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	table, found := schema.schema_find_table(db.pager, table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", table_name)
		return false
	}
	schema.schema_debug_print_entry(table)
	return true
}

// Get database statistics
db_stats :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}

	fmt.println("=== Database Statistics ===")
	fmt.printf("Path: %s\n", db.path)
	fmt.printf("Page size: %d bytes\n", types.PAGE_SIZE)

	page_count := pager.pager_page_count(db.pager)
	fmt.printf("Total pages: %d\n", page_count)
	fmt.printf(
		"Database size: %d bytes (%.2f KB)\n",
		page_count * types.PAGE_SIZE,
		f64(page_count * types.PAGE_SIZE) / 1024.0,
	)

	tables := schema.schema_list_tables(db.pager, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
	fmt.println("===========================")
}

// Vacuum database (compact and reclaim space)
db_vacuum :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}

	fmt.println("VACUUM not fully implemented in MVP")
	fmt.println("Would perform:")
	fmt.println("  - Defragment pages")
	fmt.println("  - Reclaim deleted space")
	fmt.println("  - Rebuild indexes")
}

// Checkpoint/flush all dirty pages to disk
db_checkpoint :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	pager.pager_flush_all(db.pager)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

// Begin transaction (just a marker, no actual transaction support yet)
db_begin :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("BEGIN (Note: MVP has no transaction support yet)")
	return true
}

// Commit transaction (just flush all pages)
db_commit :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("COMMIT")
	return db_checkpoint(db)
}

// Rollback transaction (not supported)
db_rollback :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("ROLLBACK (Note: MVP has no transaction support yet)")
	return false
}

// Integrity check
db_integrity_check :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("=== Integrity Check ===")
	if !db_verify_header(db) {
		fmt.println("✗ Database header is corrupted")
		return false
	}

	fmt.println("✓ Database header is valid")
	schema_page, err := pager.pager_get_page(db.pager, schema.SCHEMA_PAGE)
	if err != nil {
		fmt.println("✗ Schema page is missing")
		return false
	}

	fmt.println("✓ Schema page exists")
	tables := schema.schema_list_tables(db.pager, context.temp_allocator)

	fmt.printf("✓ Found %d table(s)\n", len(tables))
	for table in tables {
		page_data, page_err := pager.pager_get_page(db.pager, table.root_page)
		if page_err != nil {
			fmt.printf("✗ Table '%s' root page %d is missing\n", table.name, table.root_page)
			return false
		}
		fmt.printf("✓ Table '%s' is valid\n", table.name)
	}

	fmt.println("======================")
	fmt.println("Integrity check passed!")
	return true
}

// Export database to SQL dump
db_export_sql :: proc(db: ^Database, output_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL export not implemented in MVP")
	fmt.printf("Would export to: %s\n", output_path)
	return false
}

// Import SQL dump
db_import_sql :: proc(db: ^Database, input_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL import not implemented in MVP")
	fmt.printf("Would import from: %s\n", input_path)
	return false
}

// Debug: Dump all content of a specific table to stdout
db_debug_dump_table :: proc(db: ^Database, table_name: string) {
	if db == nil || db.pager == nil {
		fmt.println("Error: Invalid database handle")
		return
	}

	table, found := schema.schema_find_table(db.pager, table_name, context.temp_allocator)
	if !found {
		fmt.printf("Error: Table '%s' not found.\n", table_name)
		return
	}

	fmt.printf("=== Dumping Table: %s (Root Page: %d) ===\n", table.name, table.root_page)
	cursor := btree.btree_cursor_start(table.root_page, context.temp_allocator)
	config := btree.BTree_Config {
		allocator        = context.temp_allocator,
		zero_copy        = false,
		check_duplicates = false,
	}

	row_count := 0
	for !cursor.end_of_table {
		cell_ref, err := btree.btree_cursor_get_cell(db.pager, &cursor, config)
		if err != .None {
			fmt.printf("Error reading cell at index %d: %v\n", cursor.cell_index, err)
			btree.btree_cursor_advance(db.pager, &cursor)
			continue
		}

		fmt.printf("Row %d [RowID=%d]: ", row_count + 1, cell_ref.cell.rowid)
		for val, i in cell_ref.cell.values {
			if i > 0 do fmt.print(", ")
			col_name := "?"
			if i < len(table.columns) {
				col_name = table.columns[i].name
			}
			val_str := types.value_to_string(val, context.temp_allocator)
			fmt.printf("%s=%s", col_name, val_str)
		}

		fmt.println()
		btree.btree_cell_ref_destroy(&cell_ref)
		btree.btree_cursor_advance(db.pager, &cursor)
		row_count += 1
	}
	fmt.printf("=== Total: %d rows ===\n", row_count)
}
