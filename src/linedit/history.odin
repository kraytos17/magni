package linedit

import "core:os"
import "core:strings"

History :: struct {
	entries:    [dynamic]string,
	nav_index:  int,
	saved_line: string,
	path:       string,
}

history_add :: proc(h: ^History, line: string) {
	if len(line) == 0 {
		return
	}
	if len(h.entries) > 0 && h.entries[len(h.entries) - 1] == line {
		return
	}
	append(&h.entries, strings.clone(line))
}

history_begin_nav :: proc(h: ^History, current: string) {
	if h.nav_index == -1 {
		delete(h.saved_line)
		h.saved_line = strings.clone(current)
		h.nav_index = len(h.entries)
	}
}

history_prev :: proc(h: ^History) -> (string, bool) {
	if h.nav_index <= 0 {
		return "", false
	}

	h.nav_index -= 1
	return h.entries[h.nav_index], true
}

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

	skip := max(0, non_empty - 1000)
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

history_save :: proc(h: ^History) {
	if len(h.path) == 0 {
		return
	}

	sb := strings.builder_make()
	defer strings.builder_destroy(&sb)

	n := len(h.entries)
	start := max(0, n - 1000)
	for i in start ..< n {
		strings.write_string(&sb, h.entries[i])
		strings.write_byte(&sb, '\n')
	}
	_ = os.write_entire_file_from_string(h.path, strings.to_string(sb))
}

history_destroy :: proc(h: ^History) {
	history_save(h)
	for e in h.entries {
		delete(e)
	}

	delete(h.entries)
	delete(h.saved_line)
	delete(h.path)
}

history_len :: proc(h: ^History) -> int {
	return len(h.entries)
}

history_get :: proc(h: ^History, i: int) -> string {
	return h.entries[i]
}

history_set_path :: proc(h: ^History, path: string) {
	h.path = strings.clone(path)
}

history_reset_nav :: proc(h: ^History) {
	h.nav_index = -1
}

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
