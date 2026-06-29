package main

import "core:bufio"
import "core:flags"
import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sys/posix"
import "src:db"
import "src:linedit"

APP_VERSION :: "1.0"
DEFAULT_DB_PATH :: "test.db"

PROMPT :: "magni> "
CONT_PROMPT :: "   ...> "

CLI :: struct {
	database:      string `args:"pos=0,usage=Database file path (default: test.db)"`,
	file:          string `args:"name=file,usage=Execute SQL from file and exit"`,
	eval:          string `args:"name=eval,usage=Execute a single SQL statement and exit"`,
	stop_on_error: bool `args:"name=stop-on-error,usage=Exit on first SQL error in script mode"`,
	version:       bool `args:"name=version,usage=Print version and exit"`,
}

main :: proc() {
	cli := CLI {
		database = DEFAULT_DB_PATH,
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
	if cli.version {
		fmt.printf("MagniDB v%s\n", APP_VERSION)
		return
	}

	database, open_err := db.open(cli.database)
	if open_err != .None {
		fmt.eprintf(
			"Fatal: Could not open database '%s': %s\n",
			cli.database,
			db.db_error_string(open_err),
		)
		os.exit(1)
	}
	defer db.close(database)

	stop_on_error := cli.stop_on_error
	if len(cli.file) > 0 {
		execute_script_file(database, cli.file, stop_on_error)
	} else if len(cli.eval) > 0 {
		execute_sql(database, cli.eval, stop_on_error)
	} else if os.is_tty(os.stdin) {
		fmt.printf("MagniDB v%s\nEnter .help for usage hints.\n", APP_VERSION)
		repl(database)
	} else {
		execute_script_stream(database, stop_on_error)
	}
}

repl :: proc(database: ^db.Database) {
	history_path := filepath_join_home(".magnidb_history")
	ed, ok := linedit.init(posix.STDIN_FILENO, history_path)
	if !ok {
		repl_fallback(database)
		return
	}
	defer linedit.destroy(&ed)

	query_buffer := strings.builder_make()
	defer strings.builder_destroy(&query_buffer)

	for {
		defer free_all(context.temp_allocator)
		prompt := strings.builder_len(query_buffer) == 0 ? PROMPT : CONT_PROMPT
		line, got := linedit.read_line(&ed, prompt)
		if !got {
			fmt.println()
			break
		}

		trimmed := strings.trim_space(line)
		if len(trimmed) == 0 {
			if strings.builder_len(query_buffer) > 0 {
				strings.builder_reset(&query_buffer)
			}
			continue
		}
		if strings.builder_len(query_buffer) == 0 && strings.has_prefix(trimmed, ".") {
			linedit.history_add(&ed.history, trimmed)
			if handle_dot_command(database, trimmed) { break }
			continue
		}

		strings.write_string(&query_buffer, line)
		strings.write_byte(&query_buffer, '\n')
		if strings.has_suffix(trimmed, ";") {
			full_sql := strings.to_string(query_buffer)
			linedit.history_add(&ed.history, strings.trim_space(full_sql))
			if exec_err := db.execute(database, full_sql); exec_err != .None {
				fmt.eprintln("Error:", db.db_error_string(exec_err))
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

repl_fallback :: proc(database: ^db.Database) {
	reader: bufio.Reader
	bufio.reader_init(&reader, os.to_stream(os.stdin))
	defer bufio.reader_destroy(&reader)

	query_buffer := strings.builder_make()
	defer strings.builder_destroy(&query_buffer)
	for {
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
		if len(trimmed) == 0 { continue }
		if strings.builder_len(query_buffer) == 0 && strings.has_prefix(trimmed, ".") {
			if handle_dot_command(database, trimmed) { break }
			continue
		}

		strings.write_string(&query_buffer, line)
		if strings.has_suffix(trimmed, ";") {
			full_sql := strings.to_string(query_buffer)
			if exec_err := db.execute(database, full_sql); exec_err != .None {
				fmt.eprintln("Error:", db.db_error_string(exec_err))
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

handle_dot_command :: proc(database: ^db.Database, trimmed: string) -> bool {
	switch trimmed {
	case ".exit", ".quit":
		fmt.println("Goodbye.")
		return true
	case ".help":
		print_help()
	case ".version":
		fmt.printf("MagniDB v%s\n", APP_VERSION)
	case ".tables":
		db.list_tables(database)
	case ".schema":
		db.print_schema(database)
	case ".debug_schema":
		db.print_schema_debug(database)
	case ".stats":
		db.stats(database)
	case ".begin":
		if db.begin(database) == .None {
			fmt.println("Transaction started.")
		}
	case ".commit":
		if db.commit(database) == .None {
			fmt.println("Transaction committed.")
		}
	case ".rollback":
		if db.rollback(database) == .None {
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
				if err := db.snapshot_diff(database, older, newer); err != .None {
					fmt.eprintln("Error:", db.db_error_string(err))
				}
			} else {
				fmt.println("Usage: .snapdiff <older_id> <newer_id>")
			}
		} else {
			fmt.println("Usage: .snapdiff <older_id> <newer_id>")
		}
	case ".checkpoint":
		if err := db.checkpoint(database); err != .None {
			fmt.eprintln("Error:", db.db_error_string(err))
		} else {
			fmt.println("Database flushed to disk.")
		}
	case ".integrity":
		if err := db.integrity_check(database); err != .None {
			fmt.eprintln("Error:", db.db_error_string(err))
		} else {
			fmt.println("OK")
		}
	case:
		if strings.has_prefix(trimmed, ".snapshot tag ") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			if len(parts) >= 3 {
				id, id_ok := strconv.parse_u64(parts[2])
				if id_ok && len(parts) >= 4 {
					tag := strings.join(parts[3:], " ", context.temp_allocator)
					if err := db.snapshot_tag(database, id, tag); err != .None {
						fmt.eprintln("Error:", db.db_error_string(err))
					} else {
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
					if err := db.snapshot_restore(database, id); err != .None {
						fmt.eprintln("Error:", db.db_error_string(err))
					}
				} else {
					fmt.println("Usage: .snapshot restore <id>")
				}
			} else {
				fmt.println("Usage: .snapshot restore <id>")
			}
		} else if strings.has_prefix(trimmed, ".expire") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			keep := db.DEFAULT_KEEP
			if len(parts) >= 2 {
				if v, ok := strconv.parse_i64(parts[1]); ok { keep = int(v) }
			}
			db.expire_snapshots(database, keep)
		} else if trimmed == ".rollforward" {
			if err := db.rollforward(database); err != .None {
				fmt.eprintln("Error:", db.db_error_string(err))
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
				if err := db.describe_table(database, parts[1]); err != .None {
					fmt.eprintln("Error:", db.db_error_string(err))
				}
			} else {
				fmt.println("Usage: .desc <table_name>")
			}
		} else {
			fmt.printf("Error: Unknown command '%s'. Try .help\n", trimmed)
		}
	}
	return false
}

execute_script_file :: proc(database: ^db.Database, path: string, stop_on_error: bool = false) {
	data, err := os.read_entire_file_from_path(path, context.temp_allocator)
	if err != nil {
		fmt.eprintf("Error: Could not read file '%s'\n", path)
		return
	}
	execute_sql(database, string(data), stop_on_error)
}

execute_script_stream :: proc(database: ^db.Database, stop_on_error: bool = false) {
	data, err := os.read_entire_file_from_file(os.stdin, context.temp_allocator)
	if err != nil {
		fmt.eprintf("Error: Could not read from stdin\n")
		return
	}
	execute_sql(database, string(data), stop_on_error)
}

execute_sql :: proc(database: ^db.Database, sql: string, stop_on_error: bool = false) {
	statements := split_statements(sql)
	defer delete(statements)
	for stmt in statements {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 { continue }
		exec_err := db.execute(database, trimmed)
		if exec_err != .None && stop_on_error {
			fmt.eprintf("Error: %s\n", trimmed[:min(len(trimmed), 80)])
			os.exit(1)
		}
	}
}

split_statements :: proc(sql: string) -> []string {
	result := make([dynamic]string, context.allocator)
	start := 0
	in_string := false
	for i in 0 ..< len(sql) {
		if sql[i] == '\'' {
			in_string = !in_string
		} else if sql[i] == ';' && !in_string {
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

filepath_join_home :: proc(path: string) -> string {
	buf: [1024]u8
	home := os.get_env_buf(buf[:], "HOME")
	if len(home) == 0 {
		return path
	}

	sb := strings.builder_make(context.temp_allocator)
	strings.write_string(&sb, home)
	strings.write_byte(&sb, '/')
	strings.write_string(&sb, path)
	return strings.to_string(sb)
}

print_help :: proc() {
	fmt.println("Commands:")
	fmt.println("  .tables                   List all tables")
	fmt.println("  .schema                   Show CREATE TABLE statements")
	fmt.println("  .debug_schema             Show low-level schema details")
	fmt.println("  .desc <table>             Describe table columns")
	fmt.println("  .dump <table>             Dump all rows")
	fmt.println("  .stats                    Database statistics")
	fmt.println("  .integrity                Verify all B-trees")
	fmt.println("  .checkpoint               Flush pages + garbage collect")
	fmt.println()
	fmt.println("Transactions:")
	fmt.println("  .begin                    Begin a transaction")
	fmt.println("  .commit                   Commit the current transaction")
	fmt.println("  .rollback                 Roll back the current transaction")
	fmt.println()
	fmt.println("Snapshots:")
	fmt.println("  .snapshots                Show snapshot chain")
	fmt.println("  .snapdiff <a> <b>         Diff two snapshots")
	fmt.println("  .snapshot tag <id> <lbl>  Tag a snapshot")
	fmt.println("  .snapshot restore <id>    Restore to a snapshot")
	fmt.println("  .expire [keep]            Expire old snapshots (default 100)")
	fmt.println("  .rollforward              Advance to latest snapshot")
	fmt.println()
	fmt.println("General:")
	fmt.println("  .exit / .quit             Exit")
	fmt.println("  .help                     This message")
	fmt.println()
	fmt.println("Flags:")
	fmt.println("  --version                 Print version and exit")
	fmt.println("  --stop-on-error           Exit on first SQL error in script mode")
	fmt.println()
	fmt.println("See README.md or ARCH.md for SQL reference.")
}
