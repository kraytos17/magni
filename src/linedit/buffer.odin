package linedit

import "core:unicode/utf8"

Undo_State :: struct {
	runes:  []rune,
	cursor: int,
}

UNDO_LIMIT :: 100

Line_Buffer :: struct {
	runes:      [dynamic]rune,
	cursor:     int,
	undo_stack: [dynamic]Undo_State,
}

@(private="file")
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

lb_insert :: proc(lb: ^Line_Buffer, r: rune) {
	lb_save_undo(lb)
	inject_at(&lb.runes, lb.cursor, r)
	lb.cursor += 1
}

lb_backspace :: proc(lb: ^Line_Buffer) -> bool {
	if lb.cursor == 0 {
		return false
	}

	lb_save_undo(lb)
	ordered_remove(&lb.runes, lb.cursor - 1)
	lb.cursor -= 1
	return true
}

lb_delete_forward :: proc(lb: ^Line_Buffer) -> bool {
	if lb.cursor >= len(lb.runes) {
		return false
	}

	lb_save_undo(lb)
	ordered_remove(&lb.runes, lb.cursor)
	return true
}

lb_move_left :: proc(lb: ^Line_Buffer) {
	if lb.cursor > 0 {
		lb.cursor -= 1
	}
}

lb_move_right :: proc(lb: ^Line_Buffer) {
	if lb.cursor < len(lb.runes) {
		lb.cursor += 1
	}
}

lb_home :: proc(lb: ^Line_Buffer) {
	lb.cursor = 0
}

lb_end :: proc(lb: ^Line_Buffer) {
	lb.cursor = len(lb.runes)
}

lb_kill_to_end :: proc(lb: ^Line_Buffer) {
	if lb.cursor >= len(lb.runes) {
		return
	}

	lb_save_undo(lb)
	resize(&lb.runes, lb.cursor)
}

lb_kill_to_start :: proc(lb: ^Line_Buffer) {
	if lb.cursor == 0 {
		return
	}

	lb_save_undo(lb)
	remove_range(&lb.runes, 0, lb.cursor)
	lb.cursor = 0
}

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

lb_to_string :: proc(lb: ^Line_Buffer, allocator := context.allocator) -> string {
	s, _ := utf8.runes_to_string(lb.runes[:], allocator)
	return s
}

lb_clear :: proc(lb: ^Line_Buffer) {
	clear(&lb.runes)
	lb.cursor = 0
	for s in lb.undo_stack {
		delete(s.runes)
	}
	clear(&lb.undo_stack)
}

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

lb_len :: proc(lb: ^Line_Buffer) -> int {
	return len(lb.runes)
}

lb_cursor_pos :: proc(lb: ^Line_Buffer) -> int {
	return lb.cursor
}

lb_destroy :: proc(lb: ^Line_Buffer) {
	delete(lb.runes)
	for s in lb.undo_stack {
		delete(s.runes)
	}
	delete(lb.undo_stack)
}
