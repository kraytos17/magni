package main

import "core:bufio"
import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "src:db"
import "src:schema"

PROMPT :: "magni> "
CONT_PROMPT :: "   ...> "
WELCOME_MSG :: "MagniDB v1.0 - Interactive Mode\nEnter .help for usage hints."

main :: proc() {
	database_path := "test.db"
	database, ok := db.open(database_path)
	if !ok {
		fmt.eprintln("Fatal: Could not open database.")
		return
	}

	defer db.close(database)
	fmt.println(WELCOME_MSG)

	reader: bufio.Reader
	bufio.reader_init(&reader, os.to_stream(os.stdin))
	defer bufio.reader_destroy(&reader)

	query_buffer := strings.builder_make()
	defer strings.builder_destroy(&query_buffer)
	loop: for {
		defer free_all(context.temp_allocator)
		if strings.builder_len(query_buffer) == 0 {
			fmt.print(PROMPT)
		} else {
			fmt.print(CONT_PROMPT)
		}

		line, err := bufio.reader_read_string(&reader, '\n')
		if err != nil {
			if err == .EOF {
				break
			}
			fmt.eprintln("Error reading input:", err)
			break
		}

		trimmed := strings.trim_space(line)
		if len(trimmed) == 0 {
			continue
		}
		if strings.builder_len(query_buffer) == 0 && strings.has_prefix(trimmed, ".") {
			switch trimmed {
			case ".exit", ".quit":
				fmt.println("Goodbye.")
				break loop
			case ".help":
				print_help()
			case ".tables":
				fmt.println("--- List of Tables ---")
				db.list_tables(database)
			case ".schema":
				schema.print_ddl(database.pager)
			case ".debug_schema":
				fmt.println("--- Full Schema Dump (Debug) ---")
				schema.debug_print_all(database.pager)
			case ".stats":
				db.stats(database)
			case ".checkpoint":
				if db.checkpoint(database) {
					fmt.println("Database flushed to disk.")
				}
			case ".integrity":
				if db.integrity_check(database) {
					fmt.println("OK")
				}
			case:
				if strings.has_prefix(trimmed, ".dump ") {
					parts := strings.split(trimmed, " ", context.temp_allocator)
					if len(parts) == 2 {
						db.dump_table(database, parts[1])
					} else {
						fmt.println("Usage: .dump <table_name>")
					}
				} else if strings.has_prefix(trimmed, ".desc ") {
					parts := strings.split(trimmed, " ", context.temp_allocator)
					if len(parts) == 2 {
						db.describe_table(database, parts[1])
					} else {
						fmt.println("Usage: .desc <table_name>")
					}
				} else {
					fmt.printf("Error: Unknown command '%s'\n", trimmed)
				}
			}
			continue
		}

		strings.write_string(&query_buffer, line)
		if strings.has_suffix(trimmed, ";") {
			full_sql := strings.to_string(query_buffer)
			is_select := strings.has_prefix(strings.to_upper(strings.trim_space(full_sql)), "SELECT")
			success := db.execute(database, full_sql)
			if success {
				if !is_select {
					fmt.println("Query executed successfully.")
				}
			} else {
				fmt.println("Error executing query.")
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

print_help :: proc() {
	fmt.println("Commands:")
	fmt.println("  .exit, .quit        Exit the application")
	fmt.println("  .tables             List all tables")
	fmt.println("  .schema             Show CREATE TABLE statements")
	fmt.println("  .debug_schema       Show low-level schema (root pages, flags)")
	fmt.println("  .dump <table_name>  Print all raw rows in a table")
	fmt.println("  .desc <table_name>  Describe table columns")
	fmt.println("  .stats              Show database file statistics")
	fmt.println("  .integrity          Run consistency checks")
	fmt.println("  .checkpoint         Flush WAL/Pages to disk")
	fmt.println("\nSQL Support:")
	fmt.println("  CREATE TABLE name (col type [PRIMARY KEY] [NOT NULL], ...);")
	fmt.println("  INSERT INTO name VALUES (val1, val2, ...);")
	fmt.println("  SELECT * FROM name WHERE col = val;")
	fmt.println("  UPDATE name SET col = val WHERE col = val;")
	fmt.println("  DELETE FROM name WHERE col = val;")
	fmt.println("  DROP TABLE name;")
	fmt.println("\nNote: End SQL commands with a semicolon (;).")
}
