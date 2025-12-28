package db

import "core:fmt"
import "core:strings"
import "src:btree"
import "src:executor"
import "src:pager"
import "src:parser"
import "src:schema"
import "src:types"
import "src:utils"

MAGIC_STRING :: "MAGNI_DB_v1.0"

// Database handle
Database :: struct {
	path:   string, // File path to the database
	pager:  ^pager.Pager, // Page manager for disk I/O (owned pointer)
	is_new: bool, // Tells if database was just created
}

Header :: struct #packed {
	magic:          [13]u8,
	page_size:      u32le,
	page_count:     u32le,
	schema_version: u32le,
	reserved:       [75]u8,
}

#assert(size_of(Header) == 100)

// Opens an existing database or creates a new one at the specified path.
open :: proc(path: string) -> (^Database, bool) {
	db := new(Database)
	if db == nil {
		fmt.eprintln("Error: Failed to allocate database handle")
		return nil, false
	}

	db.path = strings.clone(path)
	p, err := pager.open(path)
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
	}
	return db, true
}

// Closes the database and frees all associated resources.
close :: proc(db: ^Database) {
	if db == nil {
		return
	}

	update_header(db)
	if db.pager != nil {
		pager.close(db.pager)
	}
	delete(db.path)
	free(db)
}

initialize :: proc(db: ^Database) -> bool {
	page, err := pager.allocate_page(db.pager)
	if err != nil {
		fmt.eprintln("Error: Failed to allocate page 0")
		return false
	}
	if page.page_num != 0 {
		fmt.eprintln("Error: Allocated page was not 0")
		return false
	}

	header := (^Header)(raw_data(page.data))
	copy(header.magic[:], MAGIC_STRING)

	header.page_size = types.PAGE_SIZE
	header.page_count = 1
	header.schema_version = 1

	pager.mark_dirty(db.pager, 0)
	pager.flush_page(db.pager, 0)
	if !schema.init(db.pager) {
		fmt.eprintln("Error: Failed to initialize schema")
		return false
	}
	return true
}

// Verifies that the database file has a valid header.
verify_header :: proc(db: ^Database) -> bool {
	page, err := pager.get_page(db.pager, 0)
	if err != nil {
		return false
	}

	header := (^Header)(raw_data(page.data))
	if string(header.magic[:]) != MAGIC_STRING {
		return false
	}
	if header.page_size != types.PAGE_SIZE {
		fmt.eprintln("Error: Page size mismatch")
		return false
	}
	return true
}

update_header :: proc(db: ^Database) {
	page_0, err := pager.get_page(db.pager, 0)
	if err != nil { return }

	page_count := u32(db.pager.file_len / i64(db.pager.page_size))
	utils.write_u32_be(page_0.data, 28, page_count)
	pager.mark_dirty(db.pager, 0)
	pager.flush_page(db.pager, 0)
}

execute :: proc(db: ^Database, sql: string) -> bool {
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

checkpoint :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	pager.flush_all(db.pager)
	fmt.println("Checkpoint complete: all pages flushed to disk")
	return true
}

integrity_check :: proc(db: ^Database) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("=== Integrity Check ===")
	if !verify_header(db) {
		fmt.println("✗ Database header is corrupted")
		return false
	}

	fmt.println("✓ Database header is valid")
	_, err := pager.get_page(db.pager, schema.SCHEMA_PAGE)
	if err != nil {
		fmt.println("✗ Schema page is missing")
		return false
	}

	fmt.println("✓ Schema page exists")
	tables := schema.list_tables(db.pager, context.temp_allocator)
	fmt.printf("✓ Found %d table(s)\n", len(tables))
	for table in tables {
		_, page_err := pager.get_page(db.pager, table.root_page)
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

list_tables :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}
	schema.debug_print_all(db.pager)
}

describe_table :: proc(db: ^Database, table_name: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	table, found := schema.find_table(db.pager, table_name, context.temp_allocator)
	if !found {
		fmt.eprintln("Error: Table not found:", table_name)
		return false
	}
	schema.debug_print_entry(table)
	return true
}

stats :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}

	fmt.println("=== Database Statistics ===")
	fmt.printf("Path: %s\n", db.path)
	fmt.printf("Page size: %d bytes\n", types.PAGE_SIZE)

	page_count := pager.page_count(db.pager)
	fmt.printf("Total pages: %d\n", page_count)
	fmt.printf(
		"Database size: %d bytes (%.2f KB)\n",
		page_count * types.PAGE_SIZE,
		f64(page_count * types.PAGE_SIZE) / 1024.0,
	)

	tables := schema.list_tables(db.pager, context.temp_allocator)
	fmt.printf("Total tables: %d\n", len(tables))
	fmt.println("===========================")
}

dump_table :: proc(db: ^Database, table_name: string) {
	if db == nil || db.pager == nil {
		fmt.println("Error: Invalid database handle")
		return
	}

	table, found := schema.find_table(db.pager, table_name, context.temp_allocator)
	if !found {
		fmt.printf("Error: Table '%s' not found.\n", table_name)
		return
	}

	fmt.printf("=== Dumping Table: %s (Root Page: %d) ===\n", table.name, table.root_page)
	cursor := btree.cursor_start(table.root_page, context.temp_allocator)
	config := btree.Config {
		allocator        = context.temp_allocator,
		zero_copy        = false,
		check_duplicates = false,
	}

	row_count := 0
	for !cursor.end_of_table {
		cell_ref, err := btree.cursor_get_cell(db.pager, &cursor, config)
		if err != .None {
			fmt.printf("Error reading cell at index %d: %v\n", cursor.cell_index, err)
			btree.cursor_advance(db.pager, &cursor)
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
		btree.cell_ref_destroy(&cell_ref)
		btree.cursor_advance(db.pager, &cursor)
		row_count += 1
	}
	fmt.printf("=== Total: %d rows ===\n", row_count)
}

vacuum :: proc(db: ^Database) {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return
	}

	fmt.println("VACUUM not fully implemented in MVP")
	fmt.println("  - Defragment pages")
	fmt.println("  - Reclaim deleted space")
	fmt.println("  - Rebuild indexes")
}

begin :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("BEGIN (Note: MVP has no transaction support yet)")
	return true
}

commit :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("COMMIT")
	return checkpoint(db)
}

rollback :: proc(db: ^Database) -> bool {
	if db == nil {
		return false
	}
	fmt.println("ROLLBACK (Note: MVP has no transaction support yet)")
	return false
}

export_sql :: proc(db: ^Database, output_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL export not implemented in MVP")
	fmt.printf("Would export to: %s\n", output_path)
	return false
}

import_sql :: proc(db: ^Database, input_path: string) -> bool {
	if db == nil || db.pager == nil {
		fmt.eprintln("Error: Invalid database handle")
		return false
	}

	fmt.println("SQL import not implemented in MVP")
	fmt.printf("Would import from: %s\n", input_path)
	return false
}
