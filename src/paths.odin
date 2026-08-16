package main

import "core:os"
import "core:path/filepath"

filepath_join_home :: proc(path: string) -> string {
	buf: [1024]u8
	home := os.get_env_buf(buf[:], "HOME")
	if len(home) == 0 {
		return path
	}

	joined, _ := filepath.join({home, path}, context.temp_allocator)
	return joined
}
