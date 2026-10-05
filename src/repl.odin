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
import "src:pager"
import "src:schema"

PROMPT      :: "magni> "
CONT_PROMPT :: "   ...> "

// Interactive REPL: line-edited input (history file ~/.magnidb_history,
// schema-aware completion), ;-terminated statements buffered across lines
// (continuation prompt), dot-commands dispatched immediately. Falls back to
// plain stdin lines when the terminal won't initialize. An
// exit-requesting dot-command breaks the loop; EOF prints a newline and
// returns.
@(private)
repl :: proc(database: ^db.Database) {
	history_path := filepath_join_home(".magnidb_history")
	ed, ok := linedit.init(posix.STDIN_FILENO, history_path)
	if !ok {
		repl_fallback(database)
		return
	}

	ed.complete_fn = complete_sql_ident
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
			if handle_dot_command(database, trimmed) {
				break
			}
			continue
		}

		strings.write_string(&query_buffer, line)
		strings.write_byte(&query_buffer, '\n')
		if strings.has_suffix(trimmed, ";") {
			repl_submit(database, &ed, &query_buffer, line, trimmed)
		}
	}
}

// complete_sql_ident completes table names, or table-qualified column
// names after a dot, from the live schema. Stateless by design: the
// database arrives via user_data (the linedit completion contract).
@(private = "file")
complete_sql_ident :: proc(word: string, user_data: rawptr, allocator: mem.Allocator) -> []string {
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

// repl_submit appends a line to the pending statement; a ;-terminated line
// runs the buffered SQL (history first), reports errors canonically, and
// resets the buffer for the next statement.
@(private = "file")
repl_submit :: proc(
	database: ^db.Database,
	ed: ^linedit.Editor,
	query_buffer: ^strings.Builder,
	line: string,
	trimmed: string,
) {
	strings.write_string(query_buffer, line)
	strings.write_byte(query_buffer, '\n')
	if strings.has_suffix(trimmed, ";") {
		full_sql := strings.to_string(query_buffer^)
		linedit.history_add(&ed.history, strings.trim_space(full_sql))
		if exec_err := db.execute(database, full_sql); exec_err != .None {
			log.errorf("%s", db.db_error_string(exec_err))
		}
		strings.builder_reset(query_buffer)
	}
}

// repl_fallback is the no-TTY input loop: same ;-buffering and
// dot-command dispatch as repl, without line editing, completion, or
// history. EOF ends the session.
@(private = "file")
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
		if len(trimmed) == 0 {
			continue
		}
		if strings.builder_len(query_buffer) == 0 && strings.has_prefix(trimmed, ".") {
			if handle_dot_command(database, trimmed) {
				break
			}
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

// Dot_Handler is one dot-command's implementation: receives the full input
// line (args) and its table entry (for usage). Returns exit=true only for
// session-ending commands (.exit/.quit) — every other command returns
// false after printing its output or error. Convention: bad args print
// cmd.usage; backend errors log through db_error_string or report_result.
@(private = "file")
Dot_Handler :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool)

// Dot_Command maps a dot-command to its handler. Exact commands match the
// full input line; prefix commands match a leading prefix (name includes the
// trailing space, e.g. ".dump ") and receive the full line for arg parsing.
// usage documents the arg shape once; handlers print cmd.usage on bad args
// instead of hand-written literals.
// Dot_Match selects how a table entry matches input: exact command word,
// or leading-prefix (entry name includes the trailing space, e.g. ".dump ").
Dot_Match :: enum u8 {
	Exact,
	Prefix,
}

Dot_Command :: struct {
	name   : string,
	match  : Dot_Match,
	handler: Dot_Handler,
	usage  : string,
}

// .exit/.quit: the only handler returning exit=true.
@(private = "file")
dot_cmd_exit :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	fmt.println("Goodbye.")
	return true
}

// .help: prints the print_help reference.
@(private = "file")
dot_cmd_help :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	print_help()
	return false
}

// .version: prints APP_VERSION.
@(private = "file")
dot_cmd_version :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	fmt.printf("MagniDB v%s\n", APP_VERSION)
	return false
}

// .tables: lists catalog tables via admin.
@(private = "file")
dot_cmd_tables :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	if err := admin.list_tables(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

// report_result logs a backend error or prints the success message
// (skipped when empty). The shared tail for delegating dot-commands.
@(private = "file")
report_result :: proc(err: db.DB_Error, success_msg: string = "") {
	if err != .None {
		log.errorf("%s", db.db_error_string(err))
	} else if success_msg != "" {
		fmt.println(success_msg)
	}
}

// .schema: prints CREATE statements via admin.
@(private = "file")
dot_cmd_schema :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	report_result(admin.print_schema(database))
	return false
}

// .debug_schema: admin schema dump with internals.
@(private = "file")
dot_cmd_debug_schema :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	report_result(admin.print_schema(database, debug = true))
	return false
}

// dot_parts splits dot-command args on spaces (temp-scoped). Every numeric
// handler shared this split by hand; one choke point, one convention.
@(private = "file")
dot_parts :: proc(args: string) -> []string {
	return strings.split(args, " ", context.temp_allocator)
}

// dot_uint_arg parses parts[idx] as u64. Single choke point for all numeric
// dot args (expire keep stays i64: it must SEE negatives to clamp them).
@(private = "file")
dot_uint_arg :: proc(parts: []string, idx: int) -> (u64, bool) {
	if len(parts) <= idx {
		return 0, false
	}

	v, ok := strconv.parse_u64(parts[idx])
	return v, ok
}

// .tree_page <n>: prints one B-tree page's structure via admin; usage on
// missing/bad page number.
@(private = "file")
dot_cmd_tree_page :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	parts := dot_parts(args)
	if len(parts) == 2 {
		page_num, num_ok := dot_uint_arg(parts, 1)
		if num_ok {
			if err := admin.print_tree_page(database, u32(page_num)); err != .None {
				log.errorf("%s", db.db_error_string(err))
			}
		}
	} else {
		fmt.println(cmd.usage)
	}
	return false
}

// .snapshot_debug: verbose snapshot chain dump via admin.
@(private = "file")
dot_cmd_snapshot_debug :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	report_result(admin.print_snapshots(database, debug = true))
	return false
}

// .stats: database statistics via admin.
@(private = "file")
dot_cmd_stats :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	report_result(admin.stats(database))
	return false
}

// .begin: opens an explicit transaction.
@(private = "file")
dot_cmd_begin :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	if db.begin(database) == .None {
		fmt.println("Transaction started.")
	}
	return false
}

// .commit: commits the explicit transaction.
@(private = "file")
dot_cmd_commit :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	if db.commit(database) == .None {
		fmt.println("Transaction committed.")
	}
	return false
}

// .rollback: rolls back the explicit transaction.
@(private = "file")
dot_cmd_rollback :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	if db.rollback(database) == .None {
		fmt.println("Transaction rolled back.")
	}
	return false
}

// .snapshots: lists the snapshot chain via admin.
@(private = "file")
dot_cmd_snapshots :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	report_result(admin.print_snapshots(database))
	return false
}

// .snapdiff <a> <b>: diffs two snapshots' manifests; usage unless exactly
// two numeric ids parse.
@(private = "file")
dot_cmd_snapdiff :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	parts := dot_parts(args)
	if len(parts) == 3 {
		older, older_ok := dot_uint_arg(parts, 1)
		newer, newer_ok := dot_uint_arg(parts, 2)
		if older_ok && newer_ok {
			report_result(db.snapshot_diff(database, older, newer))
			return false
		}
	}

	fmt.println(cmd.usage)
	return false
}

// .checkpoint: WAL checkpoint + GC via admin.
@(private = "file")
dot_cmd_checkpoint :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	report_result(admin.checkpoint(database), "Database flushed to disk.")
	return false
}

// .vacuum: rebuild into packed pages via admin.
@(private = "file")
dot_cmd_vacuum :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	report_result(admin.vacuum(database), "Database rebuilt into packed pages.")
	return false
}

// .integrity: B-tree verification via admin ("OK" on success).
@(private = "file")
dot_cmd_integrity :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	report_result(admin.integrity_check(database), "OK")
	return false
}

// .pager_stats: prints the pager's hot-path counters.
dot_cmd_pager_stats :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	pager.pager_stats_report(database.pager)
	return false
}

// .pager_layout: prints pager struct sizes/alignments (no database state).
dot_cmd_pager_layout :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	pager.pager_layout_report()
	return false
}

// .snapshot tag <id> <label...>: tags a snapshot; the label is the joined
// remainder (spaces allowed). Usage unless id parses and a label follows.
@(private = "file")
dot_cmd_snapshot_tag :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	parts := dot_parts(args)
	if len(parts) >= 3 {
		id, id_ok := dot_uint_arg(parts, 2)
		if id_ok && len(parts) >= 4 {
			tag := strings.join(parts[3:], " ", context.temp_allocator)
			if err := db.snapshot_tag(database, id, tag); err != .None {
				log.errorf("%s", db.db_error_string(err))
			} else {
				fmt.printf("Tagged snapshot %d as '%s'\n", id, tag)
			}
			return false
		}
	}

	fmt.println(cmd.usage)
	return false
}

// .snapshot restore <id>: repoints the database at a snapshot (pushes the
// displaced MAIN onto the undo log for .rollforward). Usage unless exactly
// one numeric id parses.
@(private = "file")
dot_cmd_snapshot_restore :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	parts := dot_parts(args)
	if len(parts) == 3 {
		id, id_ok := dot_uint_arg(parts, 2)
		if id_ok {
			if err := db.snapshot_restore(database, id); err != .None {
				log.errorf("%s", db.db_error_string(err))
			}
			return false
		}
	}

	fmt.println(cmd.usage)
	return false
}

// .expire [keep]: reclaims snapshots older than the newest keep (default
// 100). keep parses as SIGNED (negatives reach the clamp in
// expire_snapshots_impl); unparsable input keeps the default silently.
@(private = "file")
dot_cmd_expire :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	parts := dot_parts(args)
	keep := db.DEFAULT_KEEP
	if len(parts) >= 2 {
		if v, ok := strconv.parse_i64(parts[1]); ok {
			keep = int(v)
		}
	}

	db.expire_snapshots(database, keep)
	return false
}

// .rollforward: undo a restore via the refs undo log.
@(private = "file")
dot_cmd_rollforward :: proc(
	database: ^db.Database,
	args: string,
	cmd: ^Dot_Command,
) -> (
	exit: bool,
) {
	if err := db.rollforward(database); err != .None {
		log.errorf("%s", db.db_error_string(err))
	}
	return false
}

// .dump <table>: prints all rows via admin; usage unless exactly one arg.
@(private = "file")
dot_cmd_dump :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	parts := strings.split(args, " ", context.temp_allocator)
	if len(parts) == 2 {
		if err := admin.dump_table(database, parts[1]); err != .None {
			log.errorf("%s", db.db_error_string(err))
		}
	} else {
		fmt.println(cmd.usage)
	}
	return false
}

// .desc <table>: prints column definitions via admin; usage unless exactly
// one arg.
@(private = "file")
dot_cmd_desc :: proc(database: ^db.Database, args: string, cmd: ^Dot_Command) -> (exit: bool) {
	parts := strings.split(args, " ", context.temp_allocator)
	if len(parts) == 2 {
		if err := admin.describe_table(database, parts[1]); err != .None {
			log.errorf("%s", db.db_error_string(err))
		}
	} else {
		fmt.println(cmd.usage)
	}
	return false
}

// Exact-match commands (full input line must equal name).
@(private = "file")
DOT_COMMANDS := []Dot_Command {
	{".exit", .Exact, dot_cmd_exit, ""},
	{".quit", .Exact, dot_cmd_exit, ""},
	{".help", .Exact, dot_cmd_help, ""},
	{".version", .Exact, dot_cmd_version, ""},
	{".tables", .Exact, dot_cmd_tables, ""},
	{".schema", .Exact, dot_cmd_schema, ""},
	{".debug_schema", .Exact, dot_cmd_debug_schema, ""},
	{".tree_page", .Exact, dot_cmd_tree_page, "Usage: .tree_page <page_num>"},
	{".snapshot_debug", .Exact, dot_cmd_snapshot_debug, ""},
	{".stats", .Exact, dot_cmd_stats, ""},
	{".begin", .Exact, dot_cmd_begin, ""},
	{".commit", .Exact, dot_cmd_commit, ""},
	{".rollback", .Exact, dot_cmd_rollback, ""},
	{".snapshots", .Exact, dot_cmd_snapshots, ""},
	{".snapdiff", .Exact, dot_cmd_snapdiff, "Usage: .snapdiff <older_id> <newer_id>"},
	{".checkpoint", .Exact, dot_cmd_checkpoint, ""},
	{".vacuum", .Exact, dot_cmd_vacuum, ""},
	{".integrity", .Exact, dot_cmd_integrity, ""},
	{".pager_stats", .Exact, dot_cmd_pager_stats, ""},
	{".pager_layout", .Exact, dot_cmd_pager_layout, ""},
	{".rollforward", .Exact, dot_cmd_rollforward, ""},
	{".snapshot tag ", .Prefix, dot_cmd_snapshot_tag, "Usage: .snapshot tag <id> <label>"},
	{".snapshot restore ", .Prefix, dot_cmd_snapshot_restore, "Usage: .snapshot restore <id>"},
	{".expire", .Prefix, dot_cmd_expire, ""},
	{".dump ", .Prefix, dot_cmd_dump, "Usage: .dump <table_name>"},
	{".desc ", .Prefix, dot_cmd_desc, "Usage: .desc <table_name>"},
}

// handle_dot_command dispatches one dot-command line: exact entries match
// the first word (so ".tree_page"/".snapdiff" take args while matching
// exactly), prefix entries match the leading prefix. First hit wins — the
// table lists exact entries before prefix ones, so ".snapshot ..." never
// loses to a shorter prefix. Unknown commands log and return false
// (keep reading); only .exit/.quit handlers return true.
handle_dot_command :: proc(database: ^db.Database, trimmed: string) -> bool {
	// ".tree_page" and ".snapdiff" take args but match exactly on the command
	// word; split it off once for the exact entries.
	cmd_word := trimmed
	if sp := strings.index_byte(trimmed, ' '); sp >= 0 {
		cmd_word = trimmed[:sp]
	}
	for &cmd in DOT_COMMANDS {
		hit := false
		#partial switch cmd.match {
		case .Exact:
			hit = cmd_word == cmd.name
		case .Prefix:
			hit = strings.has_prefix(trimmed, cmd.name)
		}
		if hit {
			return cmd.handler(database, trimmed, &cmd)
		}
	}

	log.errorf("Unknown command '%s'. Try .help", trimmed)
	return false
}
