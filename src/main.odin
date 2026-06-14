package main

import "core:bufio"
import "core:flags"
import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "src:db"

PROMPT :: "magni> "
CONT_PROMPT :: "   ...> "
WELCOME_MSG :: "MagniDB v1.0\nEnter .help for usage hints."

CLI :: struct {
	database: string `args:"pos=0,usage=Database file path (default: test.db)"`,
	file:     string `args:"name=file,usage=Execute SQL from file and exit"`,
	eval:     string `args:"name=eval,usage=Execute a single SQL statement and exit"`,
}

main :: proc() {
	cli := CLI {
		database = "test.db",
	}

	err := flags.parse(&cli, os.args[1:], .Unix)
	if err != nil {
		if _, ok := err.(flags.Help_Request); ok {
			flags.write_usage(os.to_stream(os.stderr), CLI, os.args[0], .Unix)
			return
		}
		fmt.eprintln("Error:", err)
		return
	}

	database, ok := db.open(cli.database)
	if !ok {
		fmt.eprintf("Fatal: Could not open database '%s'.\n", cli.database)
		os.exit(1)
	}
	defer db.close(database)

	if len(cli.file) > 0 {
		execute_script_file(database, cli.file)
	} else if len(cli.eval) > 0 {
		execute_sql(database, cli.eval)
	} else if os.is_tty(os.stdin) {
		fmt.println(WELCOME_MSG)
		repl(database)
	} else {
		execute_script_stream(database)
	}
}

repl :: proc(database: ^db.Database) {
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
				fmt.println()
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
			handle_dot_command(database, trimmed)
			continue
		}

		strings.write_string(&query_buffer, line)
		if strings.has_suffix(trimmed, ";") {
			full_sql := strings.to_string(query_buffer)
			is_select := strings.has_prefix(strings.to_upper(strings.trim_space(full_sql)), "SELECT")
			if db.execute(database, full_sql) {
				if !is_select {
					fmt.println("Query executed successfully.")
				}
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

handle_dot_command :: proc(database: ^db.Database, trimmed: string) {
	switch trimmed {
	case ".exit", ".quit":
		fmt.println("Goodbye.")
		os.exit(0)
	case ".help":
		print_help()
	case ".tables":
		fmt.println("--- List of Tables ---")
		db.list_tables(database)
	case ".schema":
		db.print_schema(database)
	case ".debug_schema":
		fmt.println("--- Full Schema Dump (Debug) ---")
		db.print_schema_debug(database)
	case ".stats":
		db.stats(database)
	case ".begin":
		if db.begin(database) {
			fmt.println("Transaction started.")
		}
	case ".commit":
		if db.commit(database) {
			fmt.println("Transaction committed.")
		}
	case ".rollback":
		if db.rollback(database) {
			fmt.println("Transaction rolled back.")
		}
	case ".snapshots":
		db.print_snapshots(database)
	case ".snapdiff":
		parts := strings.split(trimmed, " ", context.temp_allocator)
		if len(parts) == 3 {
			older, older_ok := strconv.parse_u64(parts[1])
			newer, newer_ok := strconv.parse_u64(parts[2])
			if older_ok && newer_ok {
				db.snapshot_diff(database, older, newer)
			} else {
				fmt.println("Usage: .snapdiff <older_id> <newer_id>")
			}
		} else {
			fmt.println("Usage: .snapdiff <older_id> <newer_id>")
		}
	case ".checkpoint":
		if db.checkpoint(database) {
			fmt.println("Database flushed to disk.")
		}
	case ".integrity":
		if db.integrity_check(database) {
			fmt.println("OK")
		}
	case:
		if strings.has_prefix(trimmed, ".snapshot tag ") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			if len(parts) >= 3 {
				id, id_ok := strconv.parse_u64(parts[2])
				if id_ok && len(parts) >= 4 {
					tag := strings.join(parts[3:], " ", context.temp_allocator)
					if db.snapshot_tag(database, id, tag) {
						fmt.printf("Tagged snapshot %d as '%s'\n", id, tag)
					}
				} else {
					fmt.println("Usage: .snapshot tag <id> <label>")
				}
			} else {
				fmt.println("Usage: .snapshot tag <id> <label>")
			}
		} else if strings.has_prefix(trimmed, ".snapshot restore ") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			if len(parts) == 3 {
				id, id_ok := strconv.parse_u64(parts[2])
				if id_ok {
					db.snapshot_restore(database, id)
				} else {
					fmt.println("Usage: .snapshot restore <id>")
				}
			} else {
				fmt.println("Usage: .snapshot restore <id>")
			}
		} else if strings.has_prefix(trimmed, ".dump ") {
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
			fmt.printf("Error: Unknown command '%s'. Try .help\n", trimmed)
		}
	}
}

execute_script_file :: proc(database: ^db.Database, path: string) {
	data, err := os.read_entire_file_from_path(path, context.temp_allocator)
	if err != nil {
		fmt.eprintf("Error: Could not read file '%s'\n", path)
		return
	}
	execute_sql(database, string(data))
}

execute_script_stream :: proc(database: ^db.Database) {
	data, err := os.read_entire_file_from_file(os.stdin, context.temp_allocator)
	if err != nil {
		fmt.eprintf("Error: Could not read from stdin\n")
		return
	}
	execute_sql(database, string(data))
}

execute_sql :: proc(database: ^db.Database, sql: string) {
	statements := split_statements(sql)
	for stmt in statements {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) == 0 { continue }
		if !db.execute(database, trimmed) {
			display_len := len(trimmed)
			if display_len > 80 { display_len = 80 }
			fmt.eprintf("Error near: %s\n", trimmed[:display_len])
		}
	}
}

split_statements :: proc(sql: string) -> []string {
	result := make([dynamic]string, context.temp_allocator)
	start := 0
	for i in 0 ..< len(sql) {
		if sql[i] == ';' {
			append(&result, sql[start:i + 1])
			start = i + 1
		}
	}
	if start < len(sql) {
		remaining := strings.trim_space(sql[start:])
		if len(remaining) > 0 {
			append(&result, remaining)
		}
	}
	return result[:]
}

print_help :: proc() {
	fmt.println("Commands:")
	fmt.println("  .exit, .quit        Exit the application")
	fmt.println("  .tables             List all tables")
	fmt.println("  .schema             Show CREATE TABLE statements")
	fmt.println("  .debug_schema       Show low-level schema (root pages, flags)")
	fmt.println("  .dump <table_name>  Print all raw rows in a table")
	fmt.println("  .desc <table_name>  Describe table columns")
	fmt.println("  .begin              Begin a transaction")
	fmt.println("  .commit             Commit the current transaction (creates a snapshot)")
	fmt.println("  .rollback           Roll back the current transaction")
	fmt.println("  .snapshots          Show the snapshot chain")
	fmt.println("  .snapdiff <old> <new>  Show diff between two snapshots")
	fmt.println("  .snapshot tag <id> <label>  Tag a snapshot with a label")
	fmt.println("  .snapshot restore <id>  Restore database to a historical snapshot")
	fmt.println("  .stats              Show database file statistics")
	fmt.println("  .integrity          Run consistency checks")
	fmt.println("  .checkpoint         Flush WAL/Pages to disk")
	fmt.println("\nSQL Support:")
	fmt.println("  DDL:")
	fmt.println("    CREATE TABLE name (col type [PRIMARY KEY] [NOT NULL] [DEFAULT val], ...);")
	fmt.println("    DROP TABLE name;")
	fmt.println("  DML:")
	fmt.println("    INSERT INTO name [(col1, col2, ...)] VALUES (val1, val2, ...);")
	fmt.printf("    SELECT col1, col2, ... FROM name [WHERE cond]")
	fmt.printf(" [ORDER BY col [ASC|DESC]] [LIMIT n [OFFSET m]];\n")
	fmt.println("    SELECT func(col) FROM name [WHERE ...] [GROUP BY col [HAVING cond]];")
	fmt.println("    SELECT * FROM t1 [INNER|CROSS|LEFT [OUTER]] JOIN t2 ON condition;")
	fmt.println("    SELECT * FROM (SELECT ...) AS alias [WHERE ...] [ORDER BY ...];")
	fmt.println("    SELECT t1.col, t2.col FROM t1, t2 [WHERE t1.x = t2.y];")
	fmt.println("    SELECT ... FROM name AS OF SNAPSHOT <id> [WHERE ...];")
	fmt.println("    SELECT ... FROM name AS OF TIMESTAMP <micros> [WHERE ...];")
	fmt.println("    UPDATE name SET col = val, ... [WHERE cond];")
	fmt.println("    DELETE FROM name [WHERE cond];")
	fmt.println("  WHERE:")
	fmt.println("    col = val, col != val, col <> val, col < val, col <= val")
	fmt.println("    col LIKE pattern (% = any, _ = single char)")
	fmt.println("    col AND/OR col  (simple AND/OR, no nesting)")
	fmt.println("  Functions: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)")
	fmt.println("  Literals:  integers, reals, strings ('text'), BLOBs (X'DEAD')")
	fmt.println("\nNote: End SQL commands with a semicolon (;).")
}
