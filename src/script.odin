package main

import "core:log"
import "core:os"
import "core:strings"
import "src:db"
import "src:sqltext"

@(private)
execute_script_file :: proc(database: ^db.Database, path: string, stop_on_error: bool = false) -> bool {
	data, err := os.read_entire_file_from_path(path, context.temp_allocator)
	if err != nil {
		log.errorf("Could not read file '%s'", path)
		return false
	}
	return execute_sql(database, string(data), stop_on_error)
}

@(private)
execute_script_stream :: proc(database: ^db.Database, stop_on_error: bool = false) -> bool {
	data, err := os.read_entire_file_from_file(os.stdin, context.temp_allocator)
	if err != nil {
		log.errorf("Could not read from stdin")
		return false
	}
	return execute_sql(database, string(data), stop_on_error)
}

@(private)
execute_sql :: proc(database: ^db.Database, sql: string, stop_on_error: bool = false) -> bool {
	// Line-partition first: a dot-command is exactly one trimmed line
	// (REPL semantics). Routing post-split statements is wrong: without a
	// ';' after the dot line it glues to the following SQL and the args
	// misparse (e.g. keep falls back to DEFAULT_KEEP). SQL segments keep
	// the real statement splitter (quoted-semicolon aware).
	seg := 0
	i := 0
	ok := true
	for i <= len(sql) {
		j := strings.index_byte(sql[i:], '\n')
		line_end := len(sql) if j < 0 else i + j
		line := strings.trim_space(sql[i:line_end])
		if len(line) > 1 && line[0] == '.' {
			if !execute_sql_chunk(database, sql[seg:i], stop_on_error) { ok = false }
			if handle_dot_command(database, line) { return ok }
			seg = len(sql) if j < 0 else line_end + 1
		}
		if j < 0 { break }
		i = line_end + 1
	}
	if !execute_sql_chunk(database, sql[seg:], stop_on_error) { ok = false }
	return ok
}

@(private="file")
execute_sql_chunk :: proc(database: ^db.Database, chunk: string, stop_on_error: bool) -> bool {
	statements := sqltext.split_statements(chunk)
	defer delete(statements)

	ok := true
	for stmt in statements {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 {
			continue
		}
		if exec_err := db.execute(database, trimmed); exec_err != .None {
			log.errorf("%s", db.db_error_string(exec_err))
			ok = false
			if stop_on_error {
				log.errorf("%s", trimmed[:min(len(trimmed), 80)])
				os.exit(1)
			}
		}
	}
	return ok
}
