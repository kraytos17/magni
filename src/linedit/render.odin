package linedit

import "core:fmt"
import "core:os"
import "core:sys/posix"

prev_render_rows: int = 1

rune_width :: proc(r: rune) -> int {
	if r < 0x1100 {
		return 1
	}

	switch {
	case 0x1100 <= r && r <= 0x115F:
		return 2
	case 0x2329 <= r && r <= 0x232A:
		return 2
	case 0x2E80 <= r && r <= 0x303E:
		return 2
	case 0x3040 <= r && r <= 0x33BF:
		return 2
	case 0x3400 <= r && r <= 0x4DBF:
		return 2
	case 0x4E00 <= r && r <= 0xA4CF:
		return 2
	case 0xAC00 <= r && r <= 0xD7AF:
		return 2
	case 0xF900 <= r && r <= 0xFAFF:
		return 2
	case 0xFE10 <= r && r <= 0xFE19:
		return 2
	case 0xFE30 <= r && r <= 0xFE6F:
		return 2
	case 0xFF01 <= r && r <= 0xFF60:
		return 2
	case 0xFFE0 <= r && r <= 0xFFE6:
		return 2
	case 0x1F300 <= r && r <= 0x1F9FF:
		return 2
	}
	return 1
}

redraw :: proc(prompt: string, lb: ^Line_Buffer) {
	if window_resized {
		terminal_query_size(posix.STDIN_FILENO)
		window_resized = false
	}

	cols := terminal_width
	if cols <= 0 {
		cols = 80
	}
	if prev_render_rows > 1 {
		fmt.fprintf(os.stdout, "\x1b[%dA", prev_render_rows - 1)
	}

	fmt.fprint(os.stdout, "\r\x1b[J")
	col := 0
	for r in prompt {
		w := rune_width(r)
		if col + w > cols {
			fmt.fprint(os.stdout, "\r\n")
			col = 0
		}
		fmt.fprintf(os.stdout, "%c", r)
		col += w
	}
	for r in lb.runes {
		w := rune_width(r)
		if col + w > cols {
			fmt.fprint(os.stdout, "\r\n")
			col = 0
		}
		fmt.fprintf(os.stdout, "%c", r)
		col += w
	}

	cursor_vis := 0
	for r in prompt {
		cursor_vis += rune_width(r)
	}
	for i in 0 ..< min(lb.cursor, len(lb.runes)) {
		cursor_vis += rune_width(lb.runes[i])
	}

	cursor_row := cursor_vis / cols
	cursor_col := cursor_vis % cols
	total_vis := cursor_vis
	for i in lb.cursor ..< len(lb.runes) {
		total_vis += rune_width(lb.runes[i])
	}

	total_rows := (total_vis + cols - 1) / cols
	if total_vis == 0 {
		total_rows = 1
	}

	rows_after := total_rows - cursor_row
	if rows_after > 1 {
		fmt.fprintf(os.stdout, "\x1b[%dA", rows_after - 1)
	}
	if cursor_col > 0 {
		fmt.fprintf(os.stdout, "\x1b[%dC", cursor_col)
	}
	prev_render_rows = total_rows
}
