// Fuzz harness for the full DB stack (executor + storage + WAL).
// Reads exactly one testcase file from argv[1] and executes it as a SQL
// script against a fresh scratch database. Each process handles one input
// (the AFL++ model): fresh DB per exec guarantees isolation, and all
// temp-allocator state is released at process exit.
//
// Build (coverage + AddressSanitizer combined):
//   odin build fuzz_exec -build-mode:llvm-ir -collection:src=src -o:speed ...
//   afl-clang-fast ...ll -fsanitize=address -o fuzz/fuzz_exec_target
// Or via magni.py: python3 magni.py build --exec
// Run:
//   ./fuzz/fuzz_exec_target fuzz/corpus_exec/script_ddl
package main

import "core:fmt"
import "core:os"
import "core:strings"
import "src:db"
import "src:sqltext"

main :: proc() {
	if len(os.args) != 2 {
		fmt.eprintln("usage: fuzz_exec_target <testcase-file>")
		os.exit(1)
	}

	context.logger.lowest_level = .Error
	data, err := os.read_entire_file_from_path(os.args[1], context.allocator)
	if err != nil { os.exit(1) }
	defer delete(data)

	db_path := fmt.tprintf("/tmp/opencode/magni_exec_%d.db", os.get_pid())
	wal_path := fmt.tprintf("%s-wal", db_path)
	defer os.remove(string(db_path))
	defer os.remove(string(wal_path))

	database, open_err := db.open(string(db_path))
	if open_err != .None { return }
	defer db.close(database)

	for stmt in sqltext.split_statements(string(data), context.temp_allocator) {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 { continue }
		db.execute(database, trimmed)
	}
}
