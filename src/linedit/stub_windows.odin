#+build windows
package linedit

import "core:mem"
import "core:sys/posix"

Tab_Complete_Callback :: #type proc(word: string, allocator: mem.Allocator) -> []string

Key :: enum {
	None,
	Char,
	Enter,
	Backspace,
	Delete,
	Eof,
	Left,
	Right,
	Up,
	Down,
	Home,
	End,
	Ctrl_A,
	Ctrl_C,
	Ctrl_D,
	Ctrl_E,
	Ctrl_K,
	Ctrl_R,
	Ctrl_U,
	Ctrl_W,
	Ctrl_Z,
	Tab,
	Escape,
	Paste_Start,
	Paste_End,
}

Key_Event :: struct {
	key:  Key,
	char: rune,
}

Editor :: struct {
	history:     History,
	complete_fn: Tab_Complete_Callback,
}

init :: proc(fd: posix.FD, history_path: string) -> (ed: Editor, ok: bool) {
	history_load(&ed.history, history_path)
	ed.history.nav_index = -1
	return ed, true
}

destroy :: proc(ed: ^Editor) {
	history_destroy(&ed.history)
}

read_line :: proc(ed: ^Editor, prompt: string) -> (line: string, ok: bool) {
	return "", false
}

rune_width :: proc(r: rune) -> int {
	return 1
}

run_tab_complete :: proc(ed: ^Editor, lb: ^Line_Buffer) {  }

terminal_query_size :: proc(fd: posix.FD) {  }
