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
	path:   string, // File path to the database
	pager:  ^pager.Pager, // Page manager for disk I/O (owned pointer)
	is_new: bool, // Tells if database was just created
}

// Database_Header is the fixed-size metadata stored at the start of page 0.
//
// Layout:
// - magic[13]:       Identifies this as a MAGNI_DB file
// - page_size[4]:    Size of each page in bytes (must match types.PAGE_SIZE)
// - page_count[4]:   Number of pages allocated in the file
// - schema_version[4]: Schema evolution version (currently always 1)
// - reserved[75]:    Reserved for future extensions
Database_Header :: struct #packed {
	magic:          [13]u8,
	page_size:      u32, // Must equal types.PAGE_SIZE
	page_count:     u32, // Total pages allocated in the file
	schema_version: u32, // Schema version
	reserved:       [75]u8, // Padding for future use (must be zero)
}

MAGIC_STRING :: "MAGNI_DB_v1.0"

// Opens an existing database or creates a new one at the specified path.
// Arguments:
// - path: File system path to the database file (created if it doesn't exist)
//
// Returns:
// - ^Database: A valid database handle (must be freed with db_close)
// - bool: True if successful, false on error
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

// Initializes a brand new database file.
// This procedure is called automatically by db_open() when opening a new/empty file.
// It should NOT be called directly by user code.
//
// Arguments:
// - db: Database handle (must have a valid pager)
//
// Returns:
// - bool: True if initialization succeeded, false on error
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

// Verifies that the database file has a valid header.
//
// Arguments:
// - db: Database handle (must have a valid pager)
//
// Returns:
// - bool: True if the header is valid, false otherwise
//
// This is called automatically during db_open() for existing databases.
// If this returns false, the database is considered corrupted or incompatible.
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

// Deserializes a Database_Header from raw page data.
// The header is stored in little-endian format at the start of page 0.
//
// Arguments:
// - page_data: Raw bytes from page 0 (must be at least size_of(Database_Header))
//
// Returns:
// - Database_Header: The parsed header (zero-initialized if page_data is too small)
//
// Layout:
// - Bytes 0-12:   magic string
// - Bytes 13-16:  page_size (u32, little-endian)
// - Bytes 17-20:  page_count (u32, little-endian)
// - Bytes 21-24:  schema_version (u32, little-endian)
// - Bytes 25-99:  reserved (unused)
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

// Serializes a Database_Header into raw page data.
// Writes the header to the start of page 0 in little-endian format.
//
// Arguments:
// - page_data: Buffer to write into (must be at least size_of(Database_Header))
// - header: The header to serialize
//
// If page_data is too small, this is a no-op (returns silently).
// This is called during db_initialize() to write the initial header.
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

// Closes the database and frees all associated resources.
//
// Arguments:
// - db: Database handle (can be nil, in which case this is a no-op)
//
// After calling db_close(), the database handle becomes invalid and must not be used.
// Always pair db_open() with db_close() using defer
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

// Executes a SQL statement on the database.
//
// This is the primary interface for running SQL commands like:
// - CREATE TABLE
// - INSERT INTO
// - SELECT
// - UPDATE
// - DELETE
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
// - sql: SQL statement string (e.g., "INSERT INTO users VALUES (1, 'Alice')")
//
// Returns:
// - bool: True if the statement executed successfully, false on error
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

// Prints a list of all tables in the database to stdout.
//
// This is a debugging/introspection utility that displays:
// - Table names
// - Column definitions
// - Root page numbers
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
db_list_tables :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}
	schema.schema_debug_print_all(db.pager)
}

// Displays detailed information about a specific table.
//
// Shows:
// - Table name
// - Column names and types
// - NOT NULL constraints
// - Root page number
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
// - table_name: Name of the table to describe
//
// Returns:
// - bool: True if the table was found and described, false otherwise
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

// Prints database statistics to stdout.
//
// Statistics include:
// - Database file path
// - Page size
// - Total number of pages
// - Total database size in bytes and KB
// - Number of tables
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
//
// This is useful for monitoring database growth and understanding storage usage.
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

// Compacts the database and reclaims unused space.
//
// CURRENT STATUS: NOT IMPLEMENTED
//
// Planned operations:
// - Defragment pages to reduce fragmentation
// - Reclaim space from deleted rows
// - Rebuild indexes for better performance
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
//
// This is a placeholder for future implementation.
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

// Flushes all dirty (modified) pages to disk immediately.
//
// This ensures that all changes made so far are persisted to the database file.
// Normally, dirty pages are flushed automatically when the page cache is full
// or when the database is closed.
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
//
// Returns:
// - bool: True if the checkpoint succeeded, false on error
db_checkpoint :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	pager.pager_flush_all(db.pager)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

// Begins a database transaction.
//
// CURRENT STATUS: NOT IMPLEMENTED
//
// This is a placeholder for future ACID transaction support.
// Currently, this just prints a message and returns true.
//
// Arguments:
// - db: Database handle
//
// Returns:
// - bool: Always true in MVP
//
// Planned behavior:
// - Create a transaction context
// - Enable rollback capability
// - Isolate changes until commit
db_begin :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("BEGIN (Note: MVP has no transaction support yet)")
	return true
}

// Commits the current transaction.
//
// CURRENT STATUS: Partial implementation (flushes pages, no rollback protection)
//
// In the MVP, this simply flushes all dirty pages to disk via db_checkpoint().
// In a full implementation, this would:
// - Mark the transaction as committed
// - Make all changes visible to other connections
// - Release locks
//
// Arguments:
// - db: Database handle
//
// Returns:
// - bool: True if the commit (flush) succeeded, false on error
db_commit :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("COMMIT")
	return db_checkpoint(db)
}

// Rolls back the current transaction.
//
// CURRENT STATUS: NOT IMPLEMENTED IN MVP
//
// This is a placeholder for future rollback support.
// Currently, this just prints a message and returns false.
//
// Arguments:
// - db: Database handle
//
// Returns:
// - bool: Always false in MVP (not supported)
//
// Planned behavior:
// - Undo all changes made since db_begin()
// - Restore the database to its pre-transaction state
db_rollback :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("ROLLBACK (Note: MVP has no transaction support yet)")
	return false
}

// Performs an integrity check on the database.
//
// Verifications performed:
// 1. Database header validity (magic string and page size)
// 2. Schema page accessibility
// 3. All table root pages are accessible
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
//
// Returns:
// - bool: True if all checks passed, false if corruption detected
//
// Note: This is a basic check. A full check would also verify:
// - B-tree structure validity
// - Foreign key constraints
// - Index consistency
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

// Exports the database schema and data to a SQL dump file.
//
// CURRENT STATUS: NOT IMPLEMENTED
//
// Planned behavior:
// - Generate CREATE TABLE statements for all tables
// - Generate INSERT statements for all rows
// - Write to the specified output file
//
// Arguments:
// - db: Database handle
// - output_path: File path to write the SQL dump
//
// Returns:
// - bool: Always false in MVP (not implemented)
db_export_sql :: proc(db: ^Database, output_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL export not implemented in MVP")
	fmt.printf("Would export to: %s\n", output_path)
	return false
}

// Imports database schema and data from a SQL dump file.
//
// CURRENT STATUS: NOT IMPLEMENTED
//
// Planned behavior:
// - Parse SQL dump file
// - Execute CREATE TABLE statements
// - Execute INSERT statements
//
// Arguments:
// - db: Database handle
// - input_path: File path to read the SQL dump from
//
// Returns:
// - bool: Always false in MVP (not implemented)
db_import_sql :: proc(db: ^Database, input_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL import not implemented in MVP")
	fmt.printf("Would import from: %s\n", input_path)
	return false
}

// Dumps all rows from a table to stdout for debugging purposes.
//
// This prints:
// - Table name and root page number
// - Each row with its RowID and column values
// - Total row count
//
// Arguments:
// - db: Database handle (must be valid and non-nil)
// - table_name: Name of the table to dump
//
// This is a debugging utility and should not be used in production code.
// For large tables, this will produce a lot of output.
db_dump_table :: proc(db: ^Database, table_name: string) {
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
