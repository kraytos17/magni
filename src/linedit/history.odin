package linedit

import "core:os"
import "core:slice"
import "core:strings"

// Command history with Up/Down navigation and reverse search: entries
// (heap-owned clones), a nav cursor (-1 = editing, not browsing), the
// stashed current line restored when navigation ends, and the persistence
// path. Navigation state is single-cursor — one active readline at a time.
History :: struct {
	entries   : [dynamic]string,
	nav_index : int,
	saved_line: string,
	path      : string,
}

// HISTORY_MAX_ENTRIES caps loaded/saved history (file may hold more).
HISTORY_MAX_ENTRIES :: 1000

// history_add appends a line (cloned). Empty lines and repeats of the
// last entry are dropped — no blank/consecutive-dup spam in the file.
history_add :: proc(h: ^History, line: string) {
	if len(line) == 0 {
		return
	}
	if len(h.entries) > 0 && slice.last(h.entries[:]) == line {
		return
	}
	append(&h.entries, strings.clone(line))
}

// history_begin_nav starts browsing: stashes the current line (restored
// when navigation walks back past the newest entry) and parks the cursor
// past the end. No-op when already browsing.
history_begin_nav :: proc(h: ^History, current: string) {
	if h.nav_index == -1 {
		delete(h.saved_line)
		h.saved_line = strings.clone(current)
		h.nav_index = len(h.entries)
	}
}

// history_prev steps to the older entry; false at the oldest (cursor
// stays, nothing returned).
history_prev :: proc(h: ^History) -> (string, bool) {
	if h.nav_index <= 0 {
		return "", false
	}

	h.nav_index -= 1
	return h.entries[h.nav_index], true
}

// history_next steps to the newer entry; walking past the newest ends
// browsing (cursor -1) and returns the stashed line. False when not
// browsing.
history_next :: proc(h: ^History) -> (string, bool) {
	if h.nav_index == -1 || h.nav_index >= len(h.entries) {
		return "", false
	}

	h.nav_index += 1
	if h.nav_index == len(h.entries) {
		h.nav_index = -1
		return h.saved_line, true
	}
	return h.entries[h.nav_index], true
}

// history_load reads entries from path (missing file = empty history, not
// an error). Only the last HISTORY_MAX_ENTRIES non-empty lines are kept;
// blanks never enter.
history_load :: proc(h: ^History, path: string) {
	delete(h.path)
	h.path = strings.clone(path)
	data, err := os.read_entire_file_from_path(path, context.temp_allocator)
	if err != nil {
		return
	}

	lines := strings.split_lines(string(data), context.temp_allocator)
	non_empty := 0
	for line in lines {
		if len(line) > 0 {
			non_empty += 1
		}
	}

	skip := max(0, non_empty - HISTORY_MAX_ENTRIES)
	skipped := 0
	for line in lines {
		if len(line) == 0 {
			continue
		}
		if skipped < skip {
			skipped += 1
			continue
		}
		history_add(h, line)
	}
}

// history_save writes the last HISTORY_MAX_ENTRIES entries to the path
// (no-op without one). Write errors are ignored — history loss must never
// fail the session.
history_save :: proc(h: ^History) {
	if len(h.path) == 0 {
		return
	}

	sb := strings.builder_make()
	defer strings.builder_destroy(&sb)

	n := len(h.entries)
	start := max(0, n - HISTORY_MAX_ENTRIES)
	for i in start ..< n {
		strings.write_string(&sb, h.entries[i])
		strings.write_byte(&sb, '\n')
	}
	_ = os.write_entire_file_from_string(h.path, strings.to_string(sb))
}

// history_destroy persists (save) then frees entries, stash, and path.
history_destroy :: proc(h: ^History) {
	history_save(h)
	for e in h.entries {
		delete(e)
	}

	delete(h.entries)
	delete(h.saved_line)
	delete(h.path)
}

// history_len reports the entry count.
history_len :: proc(h: ^History) -> int {
	return len(h.entries)
}

// history_get returns entry i (borrowed; bounds-checked by the caller).
history_get :: proc(h: ^History, i: int) -> string {
	return h.entries[i]
}

// history_set_path repoints persistence (cloned; the previous path string
// is not freed here, unlike history_load which deletes first).
history_set_path :: proc(h: ^History, path: string) {
	h.path = strings.clone(path)
}

// history_reset_nav leaves browsing mode (cursor -1) without touching the
// stash — the next begin re-stashes.
history_reset_nav :: proc(h: ^History) {
	h.nav_index = -1
}

// history_search_prev finds the newest entry before index from containing
// query (substring). from clamps to the entry count; searches run
// backwards (index from exclusive). -1/false when nothing matches.
history_search_prev :: proc(h: ^History, query: string, from: int) -> (idx: int, found: bool) {
	i := min(from, len(h.entries))
	for i > 0 {
		i -= 1
		if strings.contains(h.entries[i], query) {
			return i, true
		}
	}
	return -1, false
}
