package linedit

import "core:sys/posix"
import "core:unicode/utf8"

Key :: enum {
	Paste_Start,
	Paste_End,
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
}

Key_Event :: struct {
	key:  Key,
	char: rune,
}

read_byte :: proc(fd: posix.FD) -> (b: u8, ok: bool) {
	buf: [1]u8
	n := posix.read(fd, &buf[0], 1)
	return buf[0], n == 1
}

read_key :: proc(fd: posix.FD) -> (ev: Key_Event, ok: bool) {
	b, bok := read_byte(fd)
	if !bok {
		return
	}

	switch b {
	case 0x1b:
		return read_escape_sequence(fd)
	case 0x7f, 0x08:
		return Key_Event{key = .Backspace}, true
	case 0x0d, 0x0a:
		return Key_Event{key = .Enter}, true
	case 0x01:
		return Key_Event{key = .Ctrl_A}, true
	case 0x03:
		return Key_Event{key = .Ctrl_C}, true
	case 0x04:
		return Key_Event{key = .Ctrl_D}, true
	case 0x05:
		return Key_Event{key = .Ctrl_E}, true
	case 0x09:
		return Key_Event{key = .Tab}, true
	case 0x0b:
		return Key_Event{key = .Ctrl_K}, true
	case 0x12:
		return Key_Event{key = .Ctrl_R}, true
	case 0x15:
		return Key_Event{key = .Ctrl_U}, true
	case 0x17:
		return Key_Event{key = .Ctrl_W}, true
	case 0x1a:
		return Key_Event{key = .Ctrl_Z}, true
	case:
		if b < 0x20 {
			return Key_Event{key = .None}, true
		}
		return decode_utf8(fd, b)
	}
}

read_escape_sequence :: proc(fd: posix.FD) -> (ev: Key_Event, ok: bool) {
	pfd := posix.pollfd {
		fd     = fd,
		events = {.IN},
	}

	has_more := posix.poll(&pfd, 1, 80) > 0
	if !has_more {
		return Key_Event{key = .Escape}, true
	}

	b1, ok1 := read_byte(fd)
	if !ok1 {
		return Key_Event{key = .Escape}, true
	}
	if b1 == '[' || b1 == 'O' {
		b2, ok2 := read_byte(fd)
		if !ok2 {
			return Key_Event{key = .Escape}, true
		}

		switch b2 {
		case 'A':
			return Key_Event{key = .Up}, true
		case 'B':
			return Key_Event{key = .Down}, true
		case 'C':
			return Key_Event{key = .Right}, true
		case 'D':
			return Key_Event{key = .Left}, true
		case 'H':
			return Key_Event{key = .Home}, true
		case 'F':
			return Key_Event{key = .End}, true
		case '3':
			b3, _ := read_byte(fd)
			if b3 == '~' {
				return Key_Event{key = .Delete}, true
			}
		case '1':
			b3, _ := read_byte(fd)
			if b3 == '~' {
				return Key_Event{key = .Home}, true
			}
		case '4':
			b3, _ := read_byte(fd)
			if b3 == '~' {
				return Key_Event{key = .End}, true
			}
		case '2':
			b3, _ := read_byte(fd)
			b4, _ := read_byte(fd)
			b5, _ := read_byte(fd)
			if b3 == '0' && b4 == '0' && b5 == '~' {
				return Key_Event{key = .Paste_Start}, true
			}
			if b3 == '0' && b4 == '1' && b5 == '~' {
				return Key_Event{key = .Paste_End}, true
			}
		}
	}
	return Key_Event{key = .Escape}, true
}

utf8_continuation_count :: proc(first: u8) -> int {
	if first < 0xC0 {
		return 0
	} else if first < 0xE0 {
		return 1
	} else if first < 0xF0 {
		return 2
	} else if first < 0xF8 {
		return 3
	}
	return 0
}

decode_utf8 :: proc(fd: posix.FD, first: u8) -> (ev: Key_Event, ok: bool) {
	n := utf8_continuation_count(first)
	bytes := [4]u8{first, 0, 0, 0}
	for i in 0 ..< n {
		b, bok := read_byte(fd)
		if !bok {
			break
		}
		bytes[1 + i] = b
	}
	r, _ := utf8.decode_rune(bytes[:1 + n])
	return Key_Event{key = .Char, char = r}, true
}
