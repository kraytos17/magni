// Package sqltext splits SQL text on statement boundaries for the REPL
// script runner and the exec fuzz harness.
package sqltext

import "core:strings"

// split_statements splits sql on ';' outside single-quoted string literals,
// keeping the terminator on each statement and returning any non-empty
// trailing fragment. Statements borrow sql; only the result slice is
// allocated. Shared so statement boundaries cannot drift between callers.
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
