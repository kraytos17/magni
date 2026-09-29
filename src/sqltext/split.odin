package sqltext

import "core:strings"

// split_statements splits SQL text on ';' outside string literals, keeping
// the terminator on each statement and passing through any trailing
// fragment. Shared by the REPL script runner (src/script.odin) and the
// exec fuzz harness (fuzz_exec/main.odin) so the two can never drift —
// previously each carried a hand-synced copy.
split_statements :: proc(sql: string, allocator := context.allocator) -> []string {
	result := make([dynamic]string, allocator)
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
