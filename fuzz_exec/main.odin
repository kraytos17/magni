// Fuzz harness for the full DB stack (executor + storage + WAL).
// Reads exactly one testcase file from argv[1] and executes it as a SQL
// script against a fresh scratch database. Each process handles one input
// (the AFL++ model): fresh DB per exec guarantees isolation, and all
// temp-allocator state is released at process exit.
//
// Build (coverage + AddressSanitizer combined):
//   odin build fuzz_exec -build-mode:llvm-ir -collection:src=src -o:speed ...
//   afl-clang-fast ...ll -fsanitize=address -o target/fuzz-exec/fuzz_exec_target
// Or via magni.py: python3 magni.py build --exec
// Run:
//   ./target/fuzz-exec/fuzz_exec_target fuzz/corpus_exec/script_ddl
//
// Dot-command lines (`.vacuum`, `.checkpoint`, `.expire [N]`) route to the
// same admin handlers the REPL dispatches, so campaigns also cover the
// maintenance surface (vacuum incl. text-index rebuilds, checkpoints,
// snapshot expiry). Unknown dot-commands are ignored.
package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "src:admin"
import "src:db"
import "src:sqltext"

main :: proc() {
	if len(os.args) != 2 {
		fmt.eprintln("usage: fuzz_exec_target <testcase-file>")
		os.exit(1)
	}

	context.logger.lowest_level = .Error
	data, err := os.read_entire_file_from_path(os.args[1], context.allocator)
	if err != nil {
		os.exit(1)
	}

	defer delete(data)
	db_path := fmt.tprintf("/tmp/opencode/magni_exec_%d.db", os.get_pid())
	wal_path := fmt.tprintf("%s-wal", db_path)
	defer os.remove(string(db_path))
	defer os.remove(string(wal_path))

	database, open_err := db.open(string(db_path))
	if open_err != .None {
		return
	}

	defer db.close(database)
	for stmt in sqltext.split_statements(string(data), context.temp_allocator) {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 {
			continue
		}
		if strings.has_prefix(trimmed, ".") {
			exec_admin_cmd(database, trimmed)
			continue
		}
		db.execute(database, trimmed)
	}
}

// exec_admin_cmd runs one dot-command line against the admin surface.
// Results are intentionally ignored: the harness fuzzes for crashes and
// sanitizer findings, not for command outcomes (same contract as the
// db.execute path above).
exec_admin_cmd :: proc(database: ^db.Database, line: string) {
	space := strings.index_byte(line, ' ')
	cmd := line if space < 0 else line[:space]
	args := "" if space < 0 else strings.trim_space(line[space + 1:])
	switch cmd {
	case ".vacuum":
		admin.vacuum(database)
	case ".checkpoint":
		admin.checkpoint(database)
	case ".expire":
		keep := db.DEFAULT_KEEP
		if len(args) > 0 {
			if v, ok := strconv.parse_i64(args); ok && v >= 0 {
				keep = int(v)
			}
		}
		db.expire_snapshots(database, keep)
	case ".snapshots":
		admin.print_snapshots(database)
	case ".snapshot":
		exec_snapshot_cmd(database, args)
	case ".rollforward":
		db.rollforward(database)
	case ".snapdiff":
		parts := strings.split(args, " ", context.temp_allocator)
		if len(parts) == 2 {
			if older, ook := strconv.parse_u64(parts[0]); ook {
				if newer, nok := strconv.parse_u64(parts[1]); nok {
					db.snapshot_diff(database, older, newer)
				}
			}
		}
	case ".tables":
		admin.list_tables(database)
	case ".schema":
		admin.print_schema(database)
	case ".stats":
		admin.stats(database)
	case ".integrity":
		admin.integrity_check(database)
	case ".dump":
		if len(args) > 0 {
			admin.dump_table(database, args)
		}
	case ".desc":
		if len(args) > 0 {
			admin.describe_table(database, args)
		}
	case ".tree_page":
		if n, ok := strconv.parse_u64(args); ok {
			admin.print_tree_page(database, u32(n))
		}
	}
}

// exec_snapshot_cmd runs the `.snapshot <subcommand>` family: tag/restore
// take a numeric id (tag consumes the joined remainder as the label).
// Malformed lines are ignored, like unknown dot-commands.
exec_snapshot_cmd :: proc(database: ^db.Database, args: string) {
	space := strings.index_byte(args, ' ')
	sub := args if space < 0 else args[:space]
	rest := "" if space < 0 else strings.trim_space(args[space + 1:])
	switch sub {
	case "tag":
		rspace := strings.index_byte(rest, ' ')
		if rspace < 0 {
			return
		}
		if id, ok := strconv.parse_u64(strings.trim_space(rest[:rspace])); ok {
			label := strings.trim_space(rest[rspace + 1:])
			if len(label) > 0 {
				db.snapshot_tag(database, id, label)
			}
		}
	case "restore":
		if id, ok := strconv.parse_u64(rest); ok {
			db.snapshot_restore(database, id)
		}
	}
}
