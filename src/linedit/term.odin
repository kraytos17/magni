package linedit

import "core:c"
import "core:fmt"
import "core:os"
import "core:sys/posix"

when ODIN_OS == .Linux {
	foreign import lib "system:c"
	TIOCGWINSZ :: 0x5413
} else when ODIN_OS == .Darwin {
	foreign import lib "system:System"
	TIOCGWINSZ :: 0x40087468
}

@(default_calling_convention = "c")
foreign lib {
	ioctl :: proc(fd: c.int, request: u32, arg: rawptr) -> c.int ---
}

Term :: struct {
	fd:     posix.FD,
	orig:   posix.termios,
	is_raw: bool,
}

global_term_ptr: ^Term
terminal_width: int = 80
terminal_height: int = 24
window_resized: bool = false

Winsize :: struct {
	ws_row:    u16,
	ws_col:    u16,
	ws_xpixel: u16,
	ws_ypixel: u16,
}

terminal_query_size :: proc(fd: posix.FD) {
	ws: Winsize
	result := ioctl(c.int(fd), TIOCGWINSZ, &ws)
	if result == 0 {
		terminal_width = max(1, int(ws.ws_col))
		terminal_height = max(1, int(ws.ws_row))
	}
}

term_init :: proc(fd: posix.FD) -> (t: Term, ok: bool) {
	t.fd = fd
	if posix.tcgetattr(fd, &t.orig) != .OK {
		return t, false
	}
	return t, true
}

term_enable_raw :: proc(t: ^Term) -> bool {
	raw := t.orig
	raw.c_iflag &= ~posix.CInput_Flags{.BRKINT, .ICRNL, .INPCK, .ISTRIP, .IXON}
	raw.c_oflag &= ~posix.COutput_Flags{.OPOST}
	raw.c_cflag |= posix.CControl_Flags{.CS8}
	raw.c_lflag &= ~posix.CLocal_Flags{.ECHO, .ICANON, .IEXTEN, .ISIG}

	raw.c_cc[posix.Control_Char.VMIN] = 1
	raw.c_cc[posix.Control_Char.VTIME] = 0
	if posix.tcsetattr(t.fd, .TCSAFLUSH, &raw) != .OK {
		return false
	}

	t.is_raw = true
	install_restore_handler(t)
	terminal_query_size(t.fd)
	fmt.fprint(os.stdout, "\x1b[?2004h")
	return true
}

term_restore :: proc(t: ^Term) {
	if t.is_raw {
		fmt.fprint(os.stdout, "\x1b[?2004l")
		posix.tcsetattr(t.fd, .TCSAFLUSH, &t.orig)
		t.is_raw = false
	}
}

install_restore_handler :: proc(t: ^Term) {
	global_term_ptr = t
	action: posix.sigaction_t
	action.sa_handler = restore_and_reraise
	posix.sigaction(.SIGINT, &action, nil)
	posix.sigaction(.SIGTERM, &action, nil)

	winch_action: posix.sigaction_t
	winch_action.sa_handler = sigwinch_handler
	posix.sigaction(posix.Signal(posix.SIGWINCH), &winch_action, nil)
}

restore_and_reraise :: proc "c" (sig: posix.Signal) {
	if global_term_ptr != nil && global_term_ptr.is_raw {
		posix.tcsetattr(global_term_ptr.fd, .TCSAFLUSH, &global_term_ptr.orig)
		global_term_ptr.is_raw = false
	}

	posix.signal(sig, auto_cast posix.SIG_DFL)
	posix.raise(sig)
}

sigwinch_handler :: proc "c" (sig: posix.Signal) {
	window_resized = true
}
