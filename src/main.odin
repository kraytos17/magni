package main

import "core:fmt"
import os "core:os/os2"
import "core:strings"
import "src:db"
import "src:schema"

PROMPT :: "magni> "
WELCOME_MSG :: "MagniDB v1.0 - Interactive Mode\nEnter .help for usage hints."

main :: proc() {
	database_path := "test.db"
	database, ok := db.db_open(database_path)
	if !ok {
		fmt.eprintln("Fatal: Could not open database.")
		return
	}

	defer db.db_close(database)
	fmt.println(WELCOME_MSG)

	buffer: [1024]u8
	for {
		defer free_all(context.temp_allocator)
		fmt.print(PROMPT)
		n, err := os.read(os.stdin, buffer[:])
		if err != nil {
			if err == .EOF {
				fmt.println("\nExiting...")
				return
			}
			fmt.eprintln("Error reading input:", err)
			return
		}

		line := string(buffer[:n])
		line = strings.trim_space(line)
		if len(line) == 0 {
			continue
		}
		if strings.has_prefix(line, ".") {
			switch line {
			case ".exit", ".quit":
				fmt.println("Goodbye.")
				return
			case ".help":
				print_help()
			case ".tables":
				fmt.println("--- List of Tables ---")
				db.db_list_tables(database)
			case ".schema":
				schema.schema_print_ddl(database.pager)
			case ".debug_schema":
				fmt.println("--- Full Schema Dump (Debug) ---")
				schema.schema_debug_print_all(database.pager)
			case ".stats":
				db.db_stats(database)
			case ".checkpoint":
				if db.db_checkpoint(database) {
					fmt.println("Database flushed to disk.")
				}
			case ".integrity":
				if db.db_integrity_check(database) {
					fmt.println("OK")
				}
			case:
				if strings.has_prefix(line, ".dump ") {
					parts := strings.split(line, " ")
					if len(parts) == 2 {
						table_name := parts[1]
						db.db_dump_table(database, table_name)
					} else {
						fmt.println("Usage: .dump <table_name>")
					}
					delete(parts)
				} else if strings.has_prefix(line, ".desc ") {
					parts := strings.split(line, " ")
					if len(parts) == 2 {
						table_name := parts[1]
						db.db_describe_table(database, table_name)
					} else {
						fmt.println("Usage: .desc <table_name>")
					}
					delete(parts)
				} else {
					fmt.printf("Error: Unknown command '%s'\n", line)
				}
			}
			continue
		}

		is_select := strings.has_prefix(strings.to_upper(line), "SELECT")
		success := db.db_execute(database, line)
		if success {
			if !is_select {
				fmt.println("Query executed successfully.")
			}
		} else {
			fmt.println("Error executing query.")
		}
	}
}

print_help :: proc() {
	fmt.println("Commands:")
	fmt.println("  .exit, .quit       Exit the application")
	fmt.println("  .tables            List all tables")
	fmt.println("  .schema            Show CREATE TABLE statements")
	fmt.println("  .debug_schema      Show low-level schema (root pages, flags)")
	fmt.println("  .dump <table_name> Print all raw rows in a table")
	fmt.println("  .desc <table_name> Describe table columns")
	fmt.println("  .stats             Show database file statistics")
	fmt.println("  .integrity         Run consistency checks")
	fmt.println("SQL:")
	fmt.println("  CREATE TABLE ...")
	fmt.println("  INSERT INTO ...")
	fmt.println("  SELECT * FROM ...")
}
