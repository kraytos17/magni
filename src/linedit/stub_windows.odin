// Windows stub: same package surface (Editor/init/destroy/read_line plus
// the key/render helpers) without terminal support — read_line always
// fails so callers fall back to plain stdin. Callback and Editor shapes
// match the unix build (verified via a windows_amd64 typecheck; only
// cross-linking is unsupported by the local toolchain).
#+build windows
package linedit

import "core:mem"
import "core:sys/posix"

Tab_Complete_Callback :: #type proc(
	word: string,
	user_data: rawptr,
	allocator: mem.Allocator,
) -> []string

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
	key : Key,
	char: rune,
}

Editor :: struct {
	history    : History,
	complete_fn: Tab_Complete_Callback,
	complete_ud: rawptr,
}

// init loads history only (no terminal to claim); always succeeds.
init :: proc(fd: posix.FD, history_path: string) -> (ed: Editor, ok: bool) {
	history_load(&ed.history, history_path)
	ed.history.nav_index = -1
	return ed, true
}

// destroy persists history. No terminal state to restore.
destroy :: proc(ed: ^Editor) {
	history_destroy(&ed.history)
}

// read_line always fails (no line editing on Windows) — callers use the
// stdin fallback.
read_line :: proc(ed: ^Editor, prompt: string) -> (line: string, ok: bool) {
	return "", false
}

// rune_width is 1 for everything (no wide-char handling in the stub).
rune_width :: proc(r: rune) -> int {
	return 1
}

// run_tab_complete is a no-op (no completion UI in the stub).
run_tab_complete :: proc(ed: ^Editor, lb: ^Line_Buffer) {  }

// terminal_query_size is a no-op (no terminal to query).
terminal_query_size :: proc(fd: posix.FD) {  }
