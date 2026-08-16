package tests

import "base:runtime"
import "core:log"

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
