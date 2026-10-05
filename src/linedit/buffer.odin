// Package linedit — interactive line editing for the REPL: UTF-8 rune
// buffer with cursor + bounded undo (this file), persistent history,
// key/escape decoding, terminal raw mode + signals, and re-rendering.
package linedit

import "core:unicode/utf8"

// Undo_State snapshots one buffer generation for lb_undo.
Undo_State :: struct {
	runes : []rune,
	cursor: int,
}

// UNDO_LIMIT caps the undo stack (oldest dropped first); each entry holds
// a full rune copy, so deep stacks on long lines cost memory linearly.
UNDO_LIMIT :: 100

// Line_Buffer is the editable line: runes in cursor (rune) units plus the
// undo stack. Mutating ops snapshot first (lb_save_undo); cursor moves
// never do.
Line_Buffer :: struct {
	runes     : [dynamic]rune,
	cursor    : int,
	undo_stack: [dynamic]Undo_State,
}

// lb_save_undo snapshots the current buffer + cursor, dropping the oldest
// past UNDO_LIMIT (and freeing its copy).
@(private = "file")
lb_save_undo :: proc(lb: ^Line_Buffer) {
	if len(lb.undo_stack) >= UNDO_LIMIT {
		s := lb.undo_stack[0]
		delete(s.runes)
		ordered_remove(&lb.undo_stack, 0)
	}

	s: Undo_State
	s.runes = make([]rune, len(lb.runes))
	copy(s.runes[:], lb.runes[:])

	s.cursor = lb.cursor
	append(&lb.undo_stack, s)
}

// lb_insert inserts a rune at the cursor (undoable).
lb_insert :: proc(lb: ^Line_Buffer, r: rune) {
	lb_save_undo(lb)
	inject_at(&lb.runes, lb.cursor, r)
	lb.cursor += 1
}

// lb_backspace deletes the rune before the cursor (undoable); false at
// position 0 (nothing to delete).
lb_backspace :: proc(lb: ^Line_Buffer) -> bool {
	if lb.cursor == 0 {
		return false
	}

	lb_save_undo(lb)
	ordered_remove(&lb.runes, lb.cursor - 1)
	lb.cursor -= 1
	return true
}

// lb_delete_forward deletes the rune under the cursor (undoable); false at
// end of line.
lb_delete_forward :: proc(lb: ^Line_Buffer) -> bool {
	if lb.cursor >= len(lb.runes) {
		return false
	}

	lb_save_undo(lb)
	ordered_remove(&lb.runes, lb.cursor)
	return true
}

// lb_move_left steps the cursor left (clamped at 0; never undoable).
lb_move_left :: proc(lb: ^Line_Buffer) {
	if lb.cursor > 0 {
		lb.cursor -= 1
	}
}

// lb_move_right steps the cursor right (clamped at end; never undoable).
lb_move_right :: proc(lb: ^Line_Buffer) {
	if lb.cursor < len(lb.runes) {
		lb.cursor += 1
	}
}

// lb_home jumps to line start (never undoable).
lb_home :: proc(lb: ^Line_Buffer) {
	lb.cursor = 0
}

// lb_end jumps to line end (never undoable).
lb_end :: proc(lb: ^Line_Buffer) {
	lb.cursor = len(lb.runes)
}

// lb_kill_to_end deletes cursor-to-end (undoable); no-op at end of line.
lb_kill_to_end :: proc(lb: ^Line_Buffer) {
	if lb.cursor >= len(lb.runes) {
		return
	}

	lb_save_undo(lb)
	resize(&lb.runes, lb.cursor)
}

// lb_kill_to_start deletes start-to-cursor (undoable); no-op at column 0.
lb_kill_to_start :: proc(lb: ^Line_Buffer) {
	if lb.cursor == 0 {
		return
	}

	lb_save_undo(lb)
	remove_range(&lb.runes, 0, lb.cursor)
	lb.cursor = 0
}

// lb_delete_word_back deletes the previous blank-delimited word
// (undoable): skips blanks, then non-blanks. No-op at column 0.
lb_delete_word_back :: proc(lb: ^Line_Buffer) {
	if lb.cursor == 0 {
		return
	}

	lb_save_undo(lb)
	start := lb.cursor
	for start > 0 && lb.runes[start - 1] == ' ' {
		start -= 1
	}
	for start > 0 && lb.runes[start - 1] != ' ' {
		start -= 1
	}

	remove_range(&lb.runes, start, lb.cursor)
	lb.cursor = start
}

// lb_transpose swaps the two runes around the cursor (undoable); at end
// of line it swaps the last two instead. No-op with fewer than 2 runes or
// at column 0.
lb_transpose :: proc(lb: ^Line_Buffer) {
	if len(lb.runes) < 2 || lb.cursor == 0 {
		return
	}

	lb_save_undo(lb)
	left := lb.cursor - 1
	right := lb.cursor
	if lb.cursor == len(lb.runes) {
		left = lb.cursor - 2
		right = lb.cursor - 1
	}

	lb.runes[left], lb.runes[right] = lb.runes[right], lb.runes[left]
	lb.cursor = right + 1
}

// lb_undo restores the last snapshot (no-op on empty stack). Undo itself
// is not redoable — restoring drops the snapshot.
lb_undo :: proc(lb: ^Line_Buffer) {
	if len(lb.undo_stack) == 0 {
		return
	}

	s := pop(&lb.undo_stack)
	clear(&lb.runes)
	append(&lb.runes, ..s.runes)

	lb.cursor = s.cursor
	delete(s.runes)
}

// lb_to_string encodes the buffer to a string (caller-owned).
lb_to_string :: proc(lb: ^Line_Buffer, allocator := context.allocator) -> string {
	s, _ := utf8.runes_to_string(lb.runes[:], allocator)
	return s
}

// lb_clear empties the buffer and drops the undo stack (fresh line).
lb_clear :: proc(lb: ^Line_Buffer) {
	clear(&lb.runes)
	lb.cursor = 0
	for s in lb.undo_stack {
		delete(s.runes)
	}
	clear(&lb.undo_stack)
}

// lb_set replaces the buffer wholesale (history recall, completion
// accept): a reset, not an edit, so the undo stack is dropped too.
lb_set :: proc(lb: ^Line_Buffer, s: string) {
	clear(&lb.runes)
	for r in s {
		append(&lb.runes, r)
	}

	lb.cursor = len(lb.runes)
	// lb_set is a reset, not an edit — clear undo stack too
	for u in lb.undo_stack {
		delete(u.runes)
	}
	clear(&lb.undo_stack)
}

// lb_len reports the rune count.
lb_len :: proc(lb: ^Line_Buffer) -> int {
	return len(lb.runes)
}

// lb_cursor_pos reports the cursor offset in runes.
lb_cursor_pos :: proc(lb: ^Line_Buffer) -> int {
	return lb.cursor
}

// lb_destroy frees the buffer and every undo snapshot.
lb_destroy :: proc(lb: ^Line_Buffer) {
	delete(lb.runes)
	for s in lb.undo_stack {
		delete(s.runes)
	}
	delete(lb.undo_stack)
}
