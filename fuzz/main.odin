// Fuzz harness for the SQL parser. Reads exactly one testcase file from argv[1]
// and feeds its bytes to parser.parse. The parser is deterministic, pure (no
// I/O, no database), and error-returning: invalid input yields ok=false rather
// than a crash. Run one process per input (the AFL++ model); any AST built on
// context.temp_allocator is released when the process exits.
//
// Build (AddressSanitizer):
//   odin build fuzz -collection:src=src -o:none -sanitize:address -out:fuzz_target
// Run:
//   ./fuzz_target fuzz/corpus/hello
package main

import "core:fmt"
import "core:os"
import "src:parser"

main :: proc() {
	if len(os.args) != 2 {
		fmt.eprintln("usage: fuzz_target <testcase-file>")
		os.exit(1)
	}

	data, err := os.read_entire_file_from_path(os.args[1], context.allocator)
	if err != nil { os.exit(1) }
	defer delete(data)

	// Fuzz target: parse arbitrary bytes as a SQL statement. Success or failure
	// is irrelevant to AFL++ — reaching new parse paths is what drives coverage.
	parser.parse(string(data), context.temp_allocator)
}
