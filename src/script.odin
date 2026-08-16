package main

import "core:log"
import "core:os"
import "core:strings"
import "src:db"

@(private)
execute_script_file :: proc(database: ^db.Database, path: string, stop_on_error: bool = false) {
	data, err := os.read_entire_file_from_path(path, context.temp_allocator)
	if err != nil {
		log.errorf("Could not read file '%s'", path)
		return
	}
	execute_sql(database, string(data), stop_on_error)
}

@(private)
execute_script_stream :: proc(database: ^db.Database, stop_on_error: bool = false) {
	data, err := os.read_entire_file_from_file(os.stdin, context.temp_allocator)
	if err != nil {
		log.errorf("Could not read from stdin")
		return
	}
	execute_sql(database, string(data), stop_on_error)
}

@(private)
execute_sql :: proc(database: ^db.Database, sql: string, stop_on_error: bool = false) {
	statements := split_statements(sql)
	defer delete(statements)
	for stmt in statements {
		trimmed := strings.trim_space(stmt)
		if len(trimmed) <= 1 { continue }
		exec_err := db.execute(database, trimmed)
		if exec_err != .None && stop_on_error {
			log.errorf("%s", trimmed[:min(len(trimmed), 80)])
			os.exit(1)
		}
	}
}

@(private="file")
split_statements :: proc(sql: string) -> []string {
	result := make([dynamic]string, context.allocator)
	start := 0
	in_string := false
	for i in 0 ..< len(sql) {
		if sql[i] == '\'' {
			in_string = !in_string
		} else if sql[i] == ';' && !in_string {
			append(&result, sql[start:i + 1])
			start = i + 1
		}
	}
	if start < len(sql) {
		remaining := strings.trim_space(sql[start:])
		if len(remaining) > 0 {
			append(&result, remaining)
		}
	}
	return result[:]
}
