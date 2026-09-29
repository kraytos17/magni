package tests

import "base:runtime"
import "core:fmt"
import "core:log"
import "core:os"

// suppress_expected_errors / restore_logger let a test silence log output while
// a DB call that is *expected* to fail (and logs via log.errorf) runs, so that
// error does not count as an assertion failure. Return-value assertions around
// the call remain meaningful.
//
// Odin's `context` is passed by value to procedures, so these helpers return the
// new context; the caller must assign it: `context = ...`.
suppress_expected_errors :: proc() -> (saved: log.Logger, new_ctx: runtime.Context) {
	saved = context.logger
	new_ctx = context
	new_ctx.logger = log.nil_logger()
	return
}

restore_logger :: proc(saved: log.Logger) -> runtime.Context {
	c := context
	c.logger = saved
	return c
}

// clean_db_files removes a test database and its WAL sidecar if present.
// Shared preamble for every setup/teardown env helper below.
clean_db_files :: proc(filename: string) {
	if os.exists(filename) {
		os.remove(filename)
	}

	wal_name := fmt.tprintf("%s-wal", filename)
	if os.exists(wal_name) {
		os.remove(wal_name)
	}
}
