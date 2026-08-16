#+build linux, darwin
package linedit

import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "core:sys/posix"

SEARCH_PROMPT :: "(reverse-i-search)`%s': "

Tab_Complete_Callback :: #type proc(
	word: string,
	user_data: rawptr,
	allocator: mem.Allocator,
) -> []string

Editor :: struct {
	term:            Term,
	history:         History,
	complete_fn:     Tab_Complete_Callback,
	complete_ud:     rawptr,
	prev_render_rows: int,
	prev_search_rows: int,
}

init :: proc(fd: posix.FD, history_path: string) -> (ed: Editor, ok: bool) {
	ed.term.fd = fd
	t, tok := term_init(fd)
	if !tok {
		return ed, false
	}

	ed.term = t
	if !term_enable_raw(&ed.term) {
		return ed, false
	}

	history_load(&ed.history, history_path)
	ed.history.nav_index = -1
	return ed, true
}

destroy :: proc(ed: ^Editor) {
	history_destroy(&ed.history)
	term_restore(&ed.term)
}

read_line :: proc(ed: ^Editor, prompt: string) -> (line: string, ok: bool) {
	lb: Line_Buffer
	defer lb_destroy(&lb)

	ed.history.nav_index = -1
	redraw(ed, prompt, &lb)
	for {
		ev, read_ok := read_key(ed.term.fd)
		if !read_ok {
			return "", false
		}

		switch ev.key {
		case .Enter:
			fmt.fprint(os.stdout, "\r\n")
			return lb_to_string(&lb, context.allocator), true
		case .Ctrl_D:
			if lb_len(&lb) == 0 {
				return "", false
			}
		case .Ctrl_C:
			fmt.fprint(os.stdout, "\r\n")
			lb_clear(&lb)
			return "", true
		case .Backspace:
			lb_backspace(&lb)
		case .Delete:
			lb_delete_forward(&lb)
		case .Left:
			lb_move_left(&lb)
		case .Right:
			lb_move_right(&lb)
		case .Home, .Ctrl_A:
			lb_home(&lb)
		case .End, .Ctrl_E:
			lb_end(&lb)
		case .Ctrl_K:
			lb_kill_to_end(&lb)
		case .Ctrl_L:
			fmt.fprint(os.stdout, "\x1b[2J\x1b[H")
			ed.prev_render_rows = 1
		case .Ctrl_U:
			lb_kill_to_start(&lb)
		case .Ctrl_W:
			lb_delete_word_back(&lb)
		case .Ctrl_T:
			lb_transpose(&lb)
		case .Ctrl_Z:
			lb_undo(&lb)
		case .Up:
			history_begin_nav(&ed.history, lb_to_string(&lb))
			if s, hok := history_prev(&ed.history); hok {
				lb_set(&lb, s)
			}
		case .Down:
			if s, hok := history_next(&ed.history); hok {
				lb_set(&lb, s)
			}
		case .Tab:
			run_tab_complete(ed, &lb)
		case .Ctrl_R:
			run_reverse_search(ed, prompt, &lb)
		case .Paste_Start:
			read_pasted_text(ed.term.fd, &lb)
		case .Escape, .None, .Eof, .Paste_End:
		case .Char:
			lb_insert(&lb, ev.char)
		}
		redraw(ed, prompt, &lb)
	}
}

run_reverse_search :: proc(ed: ^Editor, prompt: string, lb: ^Line_Buffer) {
	query := strings.builder_make()
	defer strings.builder_destroy(&query)

	original := lb_to_string(lb, context.temp_allocator)
	search_idx := len(ed.history.entries)
	search_failed := false
	search_wrapped := false
	matched_entry := original

	defer {
		if ed.prev_search_rows > 0 {
			fmt.fprintf(os.stdout, "\x1b[%dA", 1)
		}
		ed.prev_search_rows = 0
	}

	for {
		prompt_prefix := "(reverse-i-search)"
		if search_failed {
			prompt_prefix = "(failed reverse-i-search)"
		} else if search_wrapped {
			prompt_prefix = "(wrapped reverse-i-search)"
		}
		
		search_prompt := fmt.tprintf("%s`%s': ", prompt_prefix, strings.to_string(query))
		render_search_overlay(ed, search_prompt, matched_entry)
		ev, ok := read_key(ed.term.fd)
		if !ok { break }

		#partial switch ev.key {
		case .Enter:
			if !search_failed && strings.builder_len(query) > 0 {
				lb_set(lb, matched_entry)
			}
			return
		case .Escape, .Ctrl_C:
			return
		case .Ctrl_R:
			if strings.builder_len(query) > 0 {
				idx, found := history_search_prev(
					&ed.history,
					strings.to_string(query),
					search_idx,
				)
				if found {
					search_failed = false
					search_wrapped = false
					search_idx = idx
					matched_entry = ed.history.entries[idx]
				} else {
					idx2, found2 := history_search_prev(
						&ed.history,
						strings.to_string(query),
						len(ed.history.entries),
					)
					if found2 {
						search_failed = false
						search_wrapped = true
						search_idx = idx2
						matched_entry = ed.history.entries[idx2]
					}
				}
			}
		case .Char:
			strings.write_rune(&query, ev.char)
			search_wrapped = false
			search_idx = len(ed.history.entries)
			idx, found := history_search_prev(&ed.history, strings.to_string(query), search_idx)
			if found {
				search_failed = false
				search_idx = idx
				matched_entry = ed.history.entries[idx]
			} else {
				search_failed = true
			}
		case .Backspace:
			if strings.builder_len(query) > 0 {
				q := strings.to_string(query)
				last := len(q)
				for last > 0 {
					last -= 1
					if (q[last] & 0xC0) != 0x80 {
						break
					}
				}

				strings.builder_reset(&query)
				strings.write_string(&query, q[:last])
				search_wrapped = false
				search_idx = len(ed.history.entries)
				idx, found := history_search_prev(
					&ed.history,
					strings.to_string(query),
					search_idx,
				)
				if found {
					search_failed = false
					search_idx = idx
					matched_entry = ed.history.entries[idx]
				} else {
					search_failed = true
				}
			}
		}
	}
}

dot_commands :: []string {
	".begin",
	".checkpoint",
	".commit",
	".debug_schema",
	".desc ",
	".dump ",
	".exit",
	".expire ",
	".help",
	".integrity",
	".quit",
	".rollback",
	".rollforward",
	".schema",
	".snapdiff ",
	".snapshot restore ",
	".snapshot tag ",
	".snapshots",
	".stats",
	".tables",
	".version",
}

sql_keywords :: []string {
	"SELECT",
	"FROM",
	"WHERE",
	"AND",
	"OR",
	"NOT",
	"IN",
	"LIKE",
	"INSERT",
	"INTO",
	"VALUES",
	"UPDATE",
	"SET",
	"DELETE",
	"CREATE",
	"TABLE",
	"DROP",
	"INTEGER",
	"TEXT",
	"REAL",
	"BLOB",
	"PRIMARY",
	"KEY",
	"NULL",
	"DEFAULT",
	"CHECK",
	"ORDER",
	"BY",
	"ASC",
	"DESC",
	"LIMIT",
	"OFFSET",
	"GROUP",
	"HAVING",
	"DISTINCT",
	"AS",
	"ON",
	"JOIN",
	"INNER",
	"LEFT",
	"RIGHT",
	"CROSS",
	"OUTER",
	"BEGIN",
	"COMMIT",
	"ROLLBACK",
	"EXPLAIN",
	"SNAPSHOT",
	"TIMESTAMP",
	"OF",
}

run_tab_complete :: proc(ed: ^Editor, lb: ^Line_Buffer) {
	line := lb_to_string(lb, context.temp_allocator)
	if len(line) == 0 { return }

	candidates := make([dynamic]string, context.temp_allocator)
	if line[0] == '.' {
		for cmd in dot_commands {
			if strings.has_prefix(cmd, line) {
				append(&candidates, cmd)
			}
		}
	} else {
		word_start := len(line) - 1
		for word_start >= 0 && line[word_start] != ' ' { word_start -= 1 }

		word_start += 1
		word := line[word_start:]
		if ed.complete_fn != nil && len(word) > 0 {
			for cand in ed.complete_fn(word, ed.complete_ud, context.temp_allocator) {
				append(&candidates, cand)
			}
		}
		if len(candidates) == 0 {
			word_upper := strings.to_upper(word, context.temp_allocator)
			for kw in sql_keywords {
				if strings.has_prefix(kw, word_upper) {
					append(&candidates, kw)
				}
			}
		}
	}
	if len(candidates) == 0 {
		return
	}
	if len(candidates) == 1 {
		line_trimmed := strings.trim_suffix(line, " ")
		rest := candidates[0][len(line_trimmed):]
		for r in rest {
			lb_insert(lb, r)
		}
		return
	}

	fmt.fprint(os.stdout, "\r\n")
	for cmd in candidates {
		fmt.fprintf(os.stdout, "%s  ", cmd)
	}
	fmt.fprint(os.stdout, "\r\n")
}

read_pasted_text :: proc(fd: posix.FD, lb: ^Line_Buffer) {
	buf: [dynamic]u8
	defer delete(buf)

	end := []u8{0x1b, '[', '2', '0', '1', '~'}
	mi := 0
	for {
		b, ok := read_byte(fd)
		if !ok { break }

		append(&buf, b)
		if b == end[mi] {
			mi += 1
			if mi == len(end) {
				resize(&buf, len(buf) - len(end))
				break
			}
		} else {
			mi = 0
		}
	}
	for b in buf {
		switch b {
		case '\n':
			lb_insert(lb, '\n')
		case 0x0d:
		case:
			if b >= 0x20 || b == '\t' {
				lb_insert(lb, rune(b))
			}
		}
	}
}
