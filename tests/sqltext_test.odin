package tests

import "core:testing"
import "src:sqltext"

@(test)
test_split_statements_basic :: proc(t: ^testing.T) {
	parts := sqltext.split_statements("SELECT 1; SELECT 2;", context.temp_allocator)
	testing.expect_value(t, len(parts), 2)
	if len(parts) == 2 {
		testing.expect_value(t, parts[0], "SELECT 1;")
		testing.expect_value(t, parts[1], " SELECT 2;")
	}
}

@(test)
test_split_statements_quoted_semi :: proc(t: ^testing.T) {
	// Semicolons inside string literals must not split (fuzz quoted_semi).
	parts := sqltext.split_statements(
		"INSERT INTO t VALUES (1, 'hello; world'); SELECT 1;",
		context.temp_allocator,
	)
	testing.expect_value(t, len(parts), 2)
}

@(test)
test_split_statements_edge_shapes :: proc(t: ^testing.T) {
	// Trailing fragment without a terminator passes through.
	parts := sqltext.split_statements("SELECT 1; SELECT 2", context.temp_allocator)
	testing.expect_value(t, len(parts), 2)

	// Empty and whitespace-only inputs yield nothing.
	testing.expect_value(t, len(sqltext.split_statements("", context.temp_allocator)), 0)
	testing.expect_value(t, len(sqltext.split_statements(" ; ", context.temp_allocator)), 1)

	// Unterminated string: the rest is one trailing fragment, not a split.
	unterminated := sqltext.split_statements("SELECT 'abc;", context.temp_allocator)
	testing.expect_value(t, len(unterminated), 1)
}
