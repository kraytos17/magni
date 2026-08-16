package main

import "core:bufio"
import "core:fmt"
import "core:log"
import "core:mem"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sys/posix"
import "src:admin"
import "src:db"
import "src:linedit"
import "src:schema"

@(private)
repl :: proc(database: ^db.Database) {
	history_path := filepath_join_home(".magnidb_history")
	ed, ok := linedit.init(posix.STDIN_FILENO, history_path)
	if !ok {
		repl_fallback(database)
		return
	}

	ed.complete_fn = proc(word: string, user_data: rawptr, allocator: mem.Allocator) -> []string {
		database_ptr := (^db.Database)(user_data)
		st := db.Schema_Tree(database_ptr)
		tables := schema.list_tables(&st, allocator)
		if dot_pos := strings.last_index(word, "."); dot_pos >= 0 {
			tbl_name := word[:dot_pos]
			col_prefix := word[dot_pos + 1:]
			for tbl in tables {
				if tbl.name == tbl_name {
					cands := make([dynamic]string, allocator)
					for col in tbl.columns {
						if strings.has_prefix(col.name, col_prefix) {
							append(&cands, fmt.tprintf("%s.%s", tbl_name, col.name))
						}
					}
					return cands[:]
				}
			}
			return nil
		}

		cands := make([dynamic]string, allocator)
		for tbl in tables {
			if strings.has_prefix(tbl.name, word) {
				append(&cands, tbl.name)
			}
		}
		return cands[:]
	}

	ed.complete_ud = database
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
				log.errorf("%s", db.db_error_string(exec_err))
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

@(private="file")
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
			log.errorf("Error reading input: %v", err)
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
				log.errorf("%s", db.db_error_string(exec_err))
			}
			strings.builder_reset(&query_buffer)
		}
	}
}

@(private="file")
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
		admin.list_tables(database)
	case ".schema":
		admin.print_schema(database)
	case ".debug_schema":
		admin.print_schema_debug(database)
	case ".tree_page":
		parts := strings.split(trimmed, " ", context.temp_allocator)
		if len(parts) == 2 {
			page_num, num_ok := strconv.parse_u64(parts[1])
			if num_ok { admin.print_tree_page(database, u32(page_num)) }
		} else {
			fmt.println("Usage: .tree_page <page_num>")
		}
	case ".snapshot_debug":
		admin.print_snapshot_debug(database)
	case ".stats":
		admin.stats(database)
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
		admin.print_snapshots(database)
	case ".snapdiff":
		parts := strings.split(trimmed, " ", context.temp_allocator)
		if len(parts) == 3 {
			older, older_ok := strconv.parse_u64(parts[1])
			newer, newer_ok := strconv.parse_u64(parts[2])
			if older_ok && newer_ok {
				if err := db.snapshot_diff(database, older, newer); err != .None {
					log.errorf("%s", db.db_error_string(err))
				}
			} else {
				fmt.println("Usage: .snapdiff <older_id> <newer_id>")
			}
		} else {
			fmt.println("Usage: .snapdiff <older_id> <newer_id>")
		}
	case ".checkpoint":
		if err := admin.checkpoint(database); err != .None {
			log.errorf("%s", db.db_error_string(err))
		} else {
			fmt.println("Database flushed to disk.")
		}
	case ".vacuum":
		if err := admin.vacuum(database); err != .None {
			log.errorf("%s", db.db_error_string(err))
		} else {
			fmt.println("Database rebuilt into packed pages.")
		}
	case ".integrity":
		if err := admin.integrity_check(database); err != .None {
			log.errorf("%s", db.db_error_string(err))
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
						log.errorf("%s", db.db_error_string(err))
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
						log.errorf("%s", db.db_error_string(err))
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
				log.errorf("%s", db.db_error_string(err))
			}
		} else if strings.has_prefix(trimmed, ".dump ") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			if len(parts) == 2 {
				admin.dump_table(database, parts[1])
			} else {
				fmt.println("Usage: .dump <table_name>")
			}
		} else if strings.has_prefix(trimmed, ".desc ") {
			parts := strings.split(trimmed, " ", context.temp_allocator)
			if len(parts) == 2 {
				if err := admin.describe_table(database, parts[1]); err != .None {
					log.errorf("%s", db.db_error_string(err))
				}
			} else {
				fmt.println("Usage: .desc <table_name>")
			}
		} else {
			log.errorf("Unknown command '%s'. Try .help", trimmed)
		}
	}
	return false
}


PROMPT :: "magni> "
CONT_PROMPT :: "   ...> "