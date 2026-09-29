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

PROMPT :: "magni> "
CONT_PROMPT :: "   ...> "

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
Dot_Handler :: proc(database: ^db.Database, args: string) -> (exit: bool)

// Dot_Command maps a dot-command to its handler. Exact commands match the
// full input line; prefix commands match a leading prefix (name includes the
// trailing space, e.g. ".dump ") and receive the full line for arg parsing.
Dot_Command :: struct {
	name:    string,
	prefix:  bool,
	handler: Dot_Handler,
}

@(private="file")
dot_cmd_exit :: proc(database: ^db.Database, args: string) -> bool {
	fmt.println("Goodbye.")
	return true
}

@(private="file")
dot_cmd_help :: proc(database: ^db.Database, args: string) -> bool {
	print_help()
	return false
}

@(private="file")
dot_cmd_version :: proc(database: ^db.Database, args: string) -> bool {
	fmt.printf("MagniDB v%s\n", APP_VERSION)
	return false
}

@(private="file")
dot_cmd_tables :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.list_tables(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_schema :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.print_schema(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_debug_schema :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.print_schema(database, debug = true); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_tree_page :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
	if len(parts) == 2 {
		page_num, num_ok := strconv.parse_u64(parts[1])
		if num_ok {
			if err := admin.print_tree_page(database, u32(page_num)); err != .None {
				log.errorf("%s", db.db_error_string(err))
			}
		}
	} else {
		fmt.println("Usage: .tree_page <page_num>")
	}
	return false
}

@(private="file")
dot_cmd_snapshot_debug :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.print_snapshots(database, debug = true); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_stats :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.stats(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_begin :: proc(database: ^db.Database, args: string) -> bool {
	if db.begin(database) == .None {
		fmt.println("Transaction started.")
	}
	return false
}

@(private="file")
dot_cmd_commit :: proc(database: ^db.Database, args: string) -> bool {
	if db.commit(database) == .None {
		fmt.println("Transaction committed.")
	}
	return false
}

@(private="file")
dot_cmd_rollback :: proc(database: ^db.Database, args: string) -> bool {
	if db.rollback(database) == .None {
		fmt.println("Transaction rolled back.")
	}
	return false
}

@(private="file")
dot_cmd_snapshots :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.print_snapshots(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_snapdiff :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
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
	return false
}

@(private="file")
dot_cmd_checkpoint :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.checkpoint(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	} else {
		fmt.println("Database flushed to disk.")
	}
	return false
}

@(private="file")
dot_cmd_vacuum :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.vacuum(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	} else {
		fmt.println("Database rebuilt into packed pages.")
	}
	return false
}

@(private="file")
dot_cmd_integrity :: proc(database: ^db.Database, args: string) -> bool {
	if err := admin.integrity_check(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	} else {
		fmt.println("OK")
	}
	return false
}

@(private="file")
dot_cmd_snapshot_tag :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
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
	return false
}

@(private="file")
dot_cmd_snapshot_restore :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
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
	return false
}

@(private="file")
dot_cmd_expire :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
	keep := db.DEFAULT_KEEP
	if len(parts) >= 2 {
		if v, ok := strconv.parse_i64(parts[1]); ok { keep = int(v) }
	}
	db.expire_snapshots(database, keep)
	return false
}

@(private="file")
dot_cmd_rollforward :: proc(database: ^db.Database, args: string) -> bool {
	if err := db.rollforward(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

@(private="file")
dot_cmd_dump :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
	if len(parts) == 2 {
		if err := admin.dump_table(database, parts[1]); err != .None {
			log.errorf("%s", db.db_error_string(err))
		}
	} else {
		fmt.println("Usage: .dump <table_name>")
	}
	return false
}

@(private="file")
dot_cmd_desc :: proc(database: ^db.Database, args: string) -> bool {
	parts := strings.split(args, " ", context.temp_allocator)
	if len(parts) == 2 {
		if err := admin.describe_table(database, parts[1]); err != .None {
			log.errorf("%s", db.db_error_string(err))
		}
	} else {
		fmt.println("Usage: .desc <table_name>")
	}
	return false
}

// Exact-match commands (full input line must equal name).
@(private="file")
DOT_COMMANDS_EXACT := []Dot_Command{
	{".exit", false, dot_cmd_exit},
	{".quit", false, dot_cmd_exit},
	{".help", false, dot_cmd_help},
	{".version", false, dot_cmd_version},
	{".tables", false, dot_cmd_tables},
	{".schema", false, dot_cmd_schema},
	{".debug_schema", false, dot_cmd_debug_schema},
	{".tree_page", false, dot_cmd_tree_page},
	{".snapshot_debug", false, dot_cmd_snapshot_debug},
	{".stats", false, dot_cmd_stats},
	{".begin", false, dot_cmd_begin},
	{".commit", false, dot_cmd_commit},
	{".rollback", false, dot_cmd_rollback},
	{".snapshots", false, dot_cmd_snapshots},
	{".snapdiff", false, dot_cmd_snapdiff},
	{".checkpoint", false, dot_cmd_checkpoint},
	{".vacuum", false, dot_cmd_vacuum},
	{".integrity", false, dot_cmd_integrity},
	{".rollforward", false, dot_cmd_rollforward},
}

// Prefix commands. Names include the trailing space except ".expire", which
// also matches bare ".expire" (default keep count).
@(private="file")
DOT_COMMANDS_PREFIX := []Dot_Command{
	{".snapshot tag ", true, dot_cmd_snapshot_tag},
	{".snapshot restore ", true, dot_cmd_snapshot_restore},
	{".expire", true, dot_cmd_expire},
	{".dump ", true, dot_cmd_dump},
	{".desc ", true, dot_cmd_desc},
}

@(private="file")
handle_dot_command :: proc(database: ^db.Database, trimmed: string) -> bool {
	// ".tree_page" and ".snapdiff" take args but match exactly on the command
	// word; split off args before the exact lookup.
	cmd_word := trimmed
	if sp := strings.index_byte(trimmed, ' '); sp >= 0 {
		cmd_word = trimmed[:sp]
	}
	for cmd in DOT_COMMANDS_EXACT {
		if cmd_word == cmd.name {
			return cmd.handler(database, trimmed)
		}
	}
	for cmd in DOT_COMMANDS_PREFIX {
		if strings.has_prefix(trimmed, cmd.name) {
			return cmd.handler(database, trimmed)
		}
	}

	log.errorf("Unknown command '%s'. Try .help", trimmed)
	return false
}
