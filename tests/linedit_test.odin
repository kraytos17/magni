package tests

import "core:fmt"
import "core:os"
import "core:strings"
import "core:sys/posix"
import "core:testing"
import "src:linedit"

@(test)
test_lb_insert :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_insert(&lb, 'a')
	testing.expect_value(t, linedit.lb_len(&lb), 1)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 1)

	linedit.lb_insert(&lb, 'b')
	testing.expect_value(t, linedit.lb_len(&lb), 2)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 2)

	linedit.lb_move_left(&lb)
	linedit.lb_insert(&lb, 'X')
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "aXb")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 2)
}

@(test)
test_lb_backspace :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello")
	linedit.lb_end(&lb)
	ok := linedit.lb_backspace(&lb)
	testing.expect(t, ok, "backspace at end")
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hell")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 4)

	linedit.lb_set(&lb, "hello")
	linedit.lb_home(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	ok = linedit.lb_backspace(&lb)
	testing.expect(t, ok, "backspace at middle")
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hllo")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 1)

	linedit.lb_home(&lb)
	ok = linedit.lb_backspace(&lb)
	testing.expect(t, !ok, "backspace at start no-op")
}

@(test)
test_lb_delete_forward :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello")
	linedit.lb_home(&lb)
	ok := linedit.lb_delete_forward(&lb)
	testing.expect(t, ok, "delete at start")
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "ello")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)

	linedit.lb_end(&lb)
	ok = linedit.lb_delete_forward(&lb)
	testing.expect(t, !ok, "delete at end no-op")
}

@(test)
test_lb_move_left_right :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abc")
	linedit.lb_home(&lb)
	linedit.lb_move_left(&lb)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)

	linedit.lb_move_right(&lb)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 1)

	linedit.lb_end(&lb)
	linedit.lb_move_right(&lb)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 3)
}

@(test)
test_lb_home_end :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abc")
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_home(&lb)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)
	linedit.lb_end(&lb)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 3)
}

@(test)
test_lb_kill_to_end :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abcdef")
	linedit.lb_home(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_kill_to_end(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abc")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 3)

	linedit.lb_kill_to_end(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abc")
}

@(test)
test_lb_kill_to_start :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abcdef")
	linedit.lb_home(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_kill_to_start(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "def")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)

	linedit.lb_kill_to_start(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "def")
}

@(test)
test_lb_delete_word_back :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello world foo")
	linedit.lb_end(&lb)
	linedit.lb_delete_word_back(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello world ")

	linedit.lb_delete_word_back(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello ")

	linedit.lb_home(&lb)
	linedit.lb_delete_word_back(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello ")
}

@(test)
test_lb_undo :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello")
	linedit.lb_end(&lb)
	linedit.lb_backspace(&lb)
	linedit.lb_undo(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 5)

	linedit.lb_insert(&lb, 'X')
	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello")

	linedit.lb_home(&lb)
	linedit.lb_kill_to_start(&lb)
	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello")

	linedit.lb_clear(&lb)
	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "")
}

@(test)
test_lb_clear_set :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello")
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 5)

	linedit.lb_clear(&lb)
	testing.expect_value(t, linedit.lb_len(&lb), 0)
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)
}

@(test)
test_lb_to_string :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_insert(&lb, 'h')
	linedit.lb_insert(&lb, 'i')
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hi")
}

@(test)
test_lb_utf8_content :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "héllo 世界")
	testing.expect_value(t, linedit.lb_len(&lb), 8)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "héllo 世界")

	linedit.lb_end(&lb)
	linedit.lb_move_left(&lb)
	linedit.lb_move_left(&lb)
	linedit.lb_backspace(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "héllo世界")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 5)
}

@(test)
test_lb_empty_buffer :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)
	testing.expect_value(t, linedit.lb_len(&lb), 0)
}

@(test)
test_history_add :: proc(t: ^testing.T) {
	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_add(&h, "SELECT 1")
	testing.expect_value(t, linedit.history_len(&h), 1)

	linedit.history_add(&h, "SELECT 1")
	testing.expect_value(t, linedit.history_len(&h), 1)

	linedit.history_add(&h, "SELECT 2")
	testing.expect_value(t, linedit.history_len(&h), 2)

	linedit.history_add(&h, "")
	testing.expect_value(t, linedit.history_len(&h), 2)
}

@(test)
test_history_navigation :: proc(t: ^testing.T) {
	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_add(&h, "SELECT 1")
	linedit.history_add(&h, "SELECT 2")
	linedit.history_add(&h, "SELECT 3")

	linedit.history_reset_nav(&h)
	linedit.history_begin_nav(&h, "in-progress")

	s, ok := linedit.history_prev(&h)
	testing.expect(t, ok, "history prev 1")
	testing.expect_value(t, s, "SELECT 3")

	s, ok = linedit.history_prev(&h)
	testing.expect(t, ok, "history prev 2")
	testing.expect_value(t, s, "SELECT 2")

	s, ok = linedit.history_prev(&h)
	testing.expect(t, ok, "history prev 3")
	testing.expect_value(t, s, "SELECT 1")

	s, ok = linedit.history_prev(&h)
	testing.expect(t, !ok, "history prev past oldest")

	s, ok = linedit.history_next(&h)
	testing.expect(t, ok, "history next 1")
	testing.expect_value(t, s, "SELECT 2")

	s, ok = linedit.history_next(&h)
	testing.expect(t, ok, "history next 2")
	testing.expect_value(t, s, "SELECT 3")

	s, ok = linedit.history_next(&h)
	testing.expect(t, ok, "history next past newest restores saved")
	testing.expect_value(t, s, "in-progress")

	_, ok = linedit.history_next(&h)
	testing.expect(t, !ok, "history next ends")
}

@(test)
test_history_navigation_no_history :: proc(t: ^testing.T) {
	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_reset_nav(&h)
	linedit.history_begin_nav(&h, "current")

	_, ok := linedit.history_prev(&h)
	testing.expect(t, !ok, "prev on empty history")

	_, ok = linedit.history_next(&h)
	testing.expect(t, !ok, "next on empty history")
}

@(test)
test_history_save_load :: proc(t: ^testing.T) {
	pid := posix.getpid()
	tmp_path := fmt.tprintf("/tmp/magni_test_hist_%d", pid)

	h1: linedit.History
	linedit.history_add(&h1, "SELECT 1")
	linedit.history_add(&h1, "SELECT 2")
	linedit.history_set_path(&h1, tmp_path)
	linedit.history_save(&h1)
	linedit.history_destroy(&h1)
	defer os.remove(tmp_path)

	h2: linedit.History
	defer linedit.history_destroy(&h2)
	linedit.history_load(&h2, tmp_path)
	testing.expect_value(t, linedit.history_len(&h2), 2)
	testing.expect_value(t, linedit.history_get(&h2, 0), "SELECT 1")
	testing.expect_value(t, linedit.history_get(&h2, 1), "SELECT 2")
}

@(test)
test_history_load_missing_file :: proc(t: ^testing.T) {
	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_load(&h, "/tmp/nonexistent_history_file_xyz")
	testing.expect_value(t, linedit.history_len(&h), 0)
}

key_from_pipe :: proc(t: ^testing.T, bytes: []u8) -> linedit.Key_Event {
	fds: [2]posix.FD
	result := posix.pipe(&fds)
	testing.expect(t, result == .OK, "pipe creation")

	n := posix.write(fds[1], &bytes[0], len(bytes))
	testing.expect(t, n == len(bytes), "pipe write")
	posix.close(fds[1])

	ev, ok := linedit.read_key(fds[0])
	testing.expect(t, ok, "read_key succeeded on pipe")
	posix.close(fds[0])
	return ev
}

@(test)
test_read_key_arrows :: proc(t: ^testing.T) {
	ev := key_from_pipe(t, []u8{0x1b, '[', 'A'})
	testing.expect_value(t, ev.key, linedit.Key.Up)

	ev = key_from_pipe(t, []u8{0x1b, '[', 'B'})
	testing.expect_value(t, ev.key, linedit.Key.Down)

	ev = key_from_pipe(t, []u8{0x1b, '[', 'C'})
	testing.expect_value(t, ev.key, linedit.Key.Right)

	ev = key_from_pipe(t, []u8{0x1b, '[', 'D'})
	testing.expect_value(t, ev.key, linedit.Key.Left)
}

@(test)
test_read_key_home_end_delete :: proc(t: ^testing.T) {
	ev := key_from_pipe(t, []u8{0x1b, '[', 'H'})
	testing.expect_value(t, ev.key, linedit.Key.Home)

	ev = key_from_pipe(t, []u8{0x1b, '[', 'F'})
	testing.expect_value(t, ev.key, linedit.Key.End)

	ev = key_from_pipe(t, []u8{0x1b, '[', '3', '~'})
	testing.expect_value(t, ev.key, linedit.Key.Delete)
}

@(test)
test_read_key_control_chars :: proc(t: ^testing.T) {
	ev := key_from_pipe(t, []u8{0x0a})
	testing.expect_value(t, ev.key, linedit.Key.Enter)

	ev = key_from_pipe(t, []u8{0x0d})
	testing.expect_value(t, ev.key, linedit.Key.Enter)

	ev = key_from_pipe(t, []u8{0x7f})
	testing.expect_value(t, ev.key, linedit.Key.Backspace)

	ev = key_from_pipe(t, []u8{0x08})
	testing.expect_value(t, ev.key, linedit.Key.Backspace)

	ev = key_from_pipe(t, []u8{0x03})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_C)

	ev = key_from_pipe(t, []u8{0x04})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_D)

	ev = key_from_pipe(t, []u8{0x01})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_A)

	ev = key_from_pipe(t, []u8{0x05})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_E)

	ev = key_from_pipe(t, []u8{0x0b})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_K)

	ev = key_from_pipe(t, []u8{0x15})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_U)

	ev = key_from_pipe(t, []u8{0x17})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_W)

	ev = key_from_pipe(t, []u8{0x12})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_R)

	ev = key_from_pipe(t, []u8{0x1a})
	testing.expect_value(t, ev.key, linedit.Key.Ctrl_Z)

	ev = key_from_pipe(t, []u8{0x09})
	testing.expect_value(t, ev.key, linedit.Key.Tab)
}

@(test)
test_read_key_utf8 :: proc(t: ^testing.T) {
	ev := key_from_pipe(t, []u8{'a'})
	testing.expect_value(t, ev.key, linedit.Key.Char)
	testing.expect_value(t, ev.char, 'a')

	ev = key_from_pipe(t, []u8{0xC3, 0xA9})
	testing.expect_value(t, ev.key, linedit.Key.Char)
	testing.expect_value(t, ev.char, 'é')

	ev = key_from_pipe(t, []u8{0xE6, 0x97, 0xA5})
	testing.expect_value(t, ev.key, linedit.Key.Char)
	testing.expect_value(t, ev.char, '日')

	ev = key_from_pipe(t, []u8{0xF0, 0x9F, 0x98, 0x80})
	testing.expect_value(t, ev.key, linedit.Key.Char)
	testing.expect_value(t, ev.char, '😀')
}

@(test)
test_read_key_escape_bare :: proc(t: ^testing.T) {
	fds: [2]posix.FD
	result := posix.pipe(&fds)
	testing.expect(t, result == .OK, "pipe creation")

	bytes := []u8{0x1b}
	n := posix.write(fds[1], &bytes[0], len(bytes))
	testing.expect(t, n == len(bytes), "pipe write")
	posix.close(fds[1])

	ev, ok := linedit.read_key(fds[0])
	testing.expect(t, ok, "read_key succeeded")
	testing.expect_value(t, ev.key, linedit.Key.Escape)
	posix.close(fds[0])
}

@(test)
test_read_key_escape_partial_sequence :: proc(t: ^testing.T) {
	fds: [2]posix.FD
	result := posix.pipe(&fds)
	testing.expect(t, result == .OK, "pipe creation")

	bytes := []u8{0x1b, '['}
	n := posix.write(fds[1], &bytes[0], len(bytes))
	testing.expect(t, n == len(bytes), "pipe write")
	posix.close(fds[1])

	ev, ok := linedit.read_key(fds[0])
	testing.expect(t, ok, "read_key succeeded")
	testing.expect_value(t, ev.key, linedit.Key.Escape)
	posix.close(fds[0])
}

@(test)
test_lb_undo_single_level :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abc")
	linedit.lb_end(&lb)
	linedit.lb_insert(&lb, 'X')
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abcX")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abc")

	// lb_set clears undo stack, so only one undo level is available
	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abc")
}

@(test)
test_lb_undo_cursor_restore :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello")
	linedit.lb_home(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_move_right(&lb)
	linedit.lb_delete_forward(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "helo")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 2)
}

@(test)
test_lb_backspace_cursor :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "abc")
	linedit.lb_home(&lb)
	linedit.lb_move_right(&lb)
	ok := linedit.lb_backspace(&lb)
	testing.expect(t, ok, "backspace from pos 1")
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "bc")
	testing.expect_value(t, linedit.lb_cursor_pos(&lb), 0)
}

@(test)
test_history_path_clone :: proc(t: ^testing.T) {
	pid := posix.getpid()
	tmp_path := fmt.tprintf("/tmp/magni_test_hist_clone_%d", pid)

	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_load(&h, tmp_path)
	testing.expect_value(t, linedit.history_len(&h), 0)

	_ = os.write_entire_file_from_string(tmp_path, "SELECT X\n")
	linedit.history_load(&h, tmp_path)
	testing.expect_value(t, linedit.history_len(&h), 1)
	testing.expect_value(t, linedit.history_get(&h, 0), "SELECT X")
	defer os.remove(tmp_path)
}

@(test)
test_history_cap_on_load :: proc(t: ^testing.T) {
	pid := posix.getpid()
	tmp_path := fmt.tprintf("/tmp/magni_test_hist_cap_%d", pid)
	defer os.remove(tmp_path)

	sb := strings.builder_make(context.temp_allocator)
	for i in 0 ..< 1100 {
		fmt.sbprintf(&sb, "line_%d\n", i)
	}
	_ = os.write_entire_file_from_string(tmp_path, strings.to_string(sb))

	h: linedit.History
	defer linedit.history_destroy(&h)
	linedit.history_load(&h, tmp_path)

	testing.expect_value(t, linedit.history_len(&h), 1000)
	testing.expect_value(t, linedit.history_get(&h, 0), "line_100")
	testing.expect_value(t, linedit.history_get(&h, 999), "line_1099")
}

@(test)
test_history_search_prev :: proc(t: ^testing.T) {
	h: linedit.History
	defer linedit.history_destroy(&h)

	linedit.history_add(&h, "SELECT * FROM users")
	linedit.history_add(&h, "INSERT INTO logs VALUES (1)")
	linedit.history_add(&h, "SELECT COUNT(*) FROM users")
	linedit.history_add(&h, "DELETE FROM logs WHERE id=1")

	idx, found := linedit.history_search_prev(&h, "SELECT", len(h.entries))
	testing.expect(t, found, "found SELECT at 2")
	testing.expect_value(t, idx, 2)

	idx, found = linedit.history_search_prev(&h, "SELECT", idx)
	testing.expect(t, found, "found SELECT at 0")
	testing.expect_value(t, idx, 0)

	idx, found = linedit.history_search_prev(&h, "SELECT", idx)
	testing.expect(t, !found, "no more SELECT matches")

	idx, found = linedit.history_search_prev(&h, "logs", len(h.entries))
	testing.expect(t, found, "found logs at 3")
	testing.expect_value(t, idx, 3)

	idx, found = linedit.history_search_prev(&h, "logs", idx)
	testing.expect(t, found, "found logs at 1")
	testing.expect_value(t, idx, 1)

	idx, found = linedit.history_search_prev(&h, "nonexistent", len(h.entries))
	testing.expect(t, !found, "no match for nonexistent")
}

@(test)
test_lb_undo_multi_level :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "")
	linedit.lb_insert(&lb, 'a')
	linedit.lb_insert(&lb, 'b')
	linedit.lb_insert(&lb, 'c')
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "abc")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "ab")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "a")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "")
}

@(test)
test_lb_undo_kill_then_insert :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "hello world")
	linedit.lb_end(&lb)
	linedit.lb_kill_to_start(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "")

	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello world")

	linedit.lb_insert(&lb, 'X')
	linedit.lb_undo(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "hello world")
}

@(test)
test_tab_complete_dot_commands :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, ".h")
	linedit.lb_end(&lb)
	linedit.run_tab_complete(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, ".help")

	linedit.lb_set(&lb, ".tab")
	linedit.lb_end(&lb)
	linedit.run_tab_complete(&lb)
	s = linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, ".tables")
}

@(test)
test_tab_complete_no_dot :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, "SEL")
	linedit.lb_end(&lb)
	linedit.run_tab_complete(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, "SEL")
}

@(test)
test_tab_complete_ambiguous :: proc(t: ^testing.T) {
	lb: linedit.Line_Buffer
	defer linedit.lb_destroy(&lb)

	linedit.lb_set(&lb, ".s")
	linedit.lb_end(&lb)
	linedit.run_tab_complete(&lb)
	s := linedit.lb_to_string(&lb, context.temp_allocator)
	testing.expect_value(t, s, ".s")
}

@(test)
test_rune_width_ascii :: proc(t: ^testing.T) {
	testing.expect_value(t, linedit.rune_width('a'), 1)
	testing.expect_value(t, linedit.rune_width('Z'), 1)
	testing.expect_value(t, linedit.rune_width('0'), 1)
	testing.expect_value(t, linedit.rune_width(' '), 1)
	testing.expect_value(t, linedit.rune_width('~'), 1)
}

@(test)
test_rune_width_cjk :: proc(t: ^testing.T) {
	testing.expect_value(t, linedit.rune_width('世'), 2)
	testing.expect_value(t, linedit.rune_width('界'), 2)
	testing.expect_value(t, linedit.rune_width('日'), 2)
	testing.expect_value(t, linedit.rune_width(0x4E00), 2)
	testing.expect_value(t, linedit.rune_width(0x9FFF), 2)
}

@(test)
test_rune_width_hangul :: proc(t: ^testing.T) {
	testing.expect_value(t, linedit.rune_width(0xAC00), 2)
	testing.expect_value(t, linedit.rune_width(0xD7AF), 2)
}

@(test)
test_rune_width_emoji :: proc(t: ^testing.T) {
	testing.expect_value(t, linedit.rune_width('😀'), 2)
	testing.expect_value(t, linedit.rune_width(0x1F300), 2)
	testing.expect_value(t, linedit.rune_width(0x1F9FF), 2)
}

@(test)
test_rune_width_fullwidth :: proc(t: ^testing.T) {
	// Fullwidth Latin capital A (U+FF21)
	testing.expect_value(t, linedit.rune_width(0xFF21), 2)
	// Fullwidth digit 1 (U+FF11)
	testing.expect_value(t, linedit.rune_width(0xFF11), 2)
}

@(test)
test_terminal_query_size_pipe :: proc(t: ^testing.T) {
	// On a pipe FD, ioctl returns nonzero, and the defaults should be preserved
	old_w := linedit.terminal_width
	old_h := linedit.terminal_height

	linedit.terminal_width = 42
	linedit.terminal_height = 10

	fds: [2]posix.FD
	result := posix.pipe(&fds)
	testing.expect(t, result == .OK, "pipe creation")
	linedit.terminal_query_size(fds[0])
	posix.close(fds[0])
	posix.close(fds[1])

	// On a pipe, ioctl should fail and leave defaults unchanged
	testing.expect_value(t, linedit.terminal_width, 42)
	testing.expect_value(t, linedit.terminal_height, 10)

	linedit.terminal_width = old_w
	linedit.terminal_height = old_h
}

@(test)
test_rune_width_various :: proc(t: ^testing.T) {
	// Control char range — should be width 1 (won't be printed but test the function)
	for i := 0; i < 0x1100; i += 1 {
		w := linedit.rune_width(rune(i))
		testing.expect(t, w >= 1 && w <= 2, "all runes have valid width")
	}
}
