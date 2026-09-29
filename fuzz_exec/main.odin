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

// Statement splitting mirrors split_statements in src/script.odin
// (string-literal-aware ';' split). Kept in sync manually; the fuzzer
// only needs approximate statement boundaries.
split_script :: proc(sql: string) -> []string {
	result := make([dynamic]string, context.temp_allocator)
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

main :: proc() {
	if len(os.args) != 2 {
		fmt.eprintln("usage: fuzz_exec_target <testcase-file>")
		os.exit(1)
	}

	// Mute below Error: db.open logs INFO per database, which would
	// drown exec speed and disk under fuzzing.
	context.logger.lowest_level = .Error
	data, err := os.read_entire_file_from_path(os.args[1], context.allocator)
	if err != nil { os.exit(1) }
	defer delete(data)

	// Fresh scratch DB per input (isolation between testcases).
	db_path := fmt.tprintf("/tmp/opencode/magni_exec_%d.db", os.get_pid())
	wal_path := fmt.tprintf("%s-wal", db_path)
	defer os.remove(string(db_path))
	defer os.remove(string(wal_path))

	database, open_err := db.open(string(db_path))
	if open_err != .None { return }
	defer db.close(database)

	// Execute as a script: DDL → DML → queries interplay is where the
	// executor/storage bugs live. Errors are normal; only crashes matter.
	for stmt in split_script(string(data)) {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 { continue }
		db.execute(database, trimmed)
	}
}
