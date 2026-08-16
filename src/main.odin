package main

import "core:flags"
import "core:fmt"
import "core:log"
import "core:os"
import "core:strings"
import "src:db"

APP_VERSION :: "1.0"
DEFAULT_DB_PATH :: "test.db"

CLI :: struct {
	database:      string `args:"pos=0,usage=Database file path (default: test.db)"`,
	file:          string `args:"name=file,usage=Execute SQL from file and exit"`,
	eval:          string `args:"name=eval,usage=Execute a single SQL statement and exit"`,
	stop_on_error: bool   `args:"name=stop-on-error,usage=Exit on first SQL error in script mode"`,
	version:       bool   `args:"name=version,usage=Print version and exit"`,
	log_level:     string `args:"name=log-level,usage=Log level: debug, info, warn, error (default: info)"`,
	verbose:       bool   `args:"name=verbose,usage=Enable debug-level logging"`,
	v:             bool   `args:"name=v,usage=Enable debug-level logging (alias for --verbose)"`,
}

main :: proc() {
	cli := CLI {
		database = DEFAULT_DB_PATH,
	}

	err := flags.parse(&cli, os.args[1:], .Unix)
	if err != nil {
		if _, ok := err.(flags.Help_Request); ok {
			flags.write_usage(os.to_stream(os.stderr), CLI, os.args[0], .Unix)
			return
		}
		fmt.eprintln("Error:", err)
		return
	}
	if cli.version {
		fmt.printf("MagniDB v%s\n", APP_VERSION)
		return
	}

	log_level := resolve_log_level(cli.verbose, cli.v, cli.log_level)
	Logger_Opts :: log.Options{.Level}
	// File logger on stderr: keeps ALL log levels off stdout so query results stay clean.
	context.logger = log.create_file_logger(os.stderr, log_level, Logger_Opts)
	defer log.destroy_file_logger(context.logger)

	database, open_err := db.open(cli.database)
	if open_err != .None {
		log.fatalf("Could not open database '%s': %s", cli.database, db.db_error_string(open_err))
		os.exit(1)
	}
	defer db.close(database)

	stop_on_error := cli.stop_on_error
	if len(cli.file) > 0 {
		execute_script_file(database, cli.file, stop_on_error)
	} else if len(cli.eval) > 0 {
		execute_sql(database, cli.eval, stop_on_error)
	} else if os.is_tty(os.stdin) {
		fmt.printf("MagniDB v%s\nEnter .help for usage hints.\n", APP_VERSION)
		context.logger.lowest_level = .Error
		repl(database)
	} else {
		execute_script_stream(database, stop_on_error)
	}
}

@(private="file")
resolve_log_level :: proc(verbose: bool, v: bool, level_str: string) -> log.Level {
	if verbose || v {
		return .Debug
	}
	if len(level_str) > 0 {
		switch strings.to_lower(level_str) {
		case "debug": return .Debug
		case "info":  return .Info
		case "warn", "warning": return .Warning
		case "error": return .Error
		}
	}

	env_buf: [256]u8
	env := os.get_env(env_buf[:], "MAGNI_LOG_LEVEL")
	if len(env) > 0 {
		switch env {
		case "DEBUG": return .Debug
		case "INFO":  return .Info
		case "WARN", "WARNING": return .Warning
		case "ERROR": return .Error
		}
	}
	return .Info
}

@(private)
print_help :: proc() {
	fmt.println("Commands:")
	fmt.println("  .tables                   List all tables")
	fmt.println("  .schema                   Show CREATE TABLE statements")
	fmt.println("  .debug_schema             Show low-level schema details")
	fmt.println("  .desc <table>             Describe table columns")
	fmt.println("  .dump <table>             Dump all rows")
	fmt.println("  .stats                    Database statistics")
	fmt.println("  .integrity                Verify all B-trees")
	fmt.println("  .checkpoint               Flush pages + garbage collect")
	fmt.println("  .tree_page <n>            Print B-tree page structure")
	fmt.println("  .snapshot_debug           Show verbose snapshot chain dump")
	fmt.println()
	fmt.println("Transactions:")
	fmt.println("  .begin                    Begin a transaction")
	fmt.println("  .commit                   Commit the current transaction")
	fmt.println("  .rollback                 Roll back the current transaction")
	fmt.println()
	fmt.println("Snapshots:")
	fmt.println("  .snapshots                Show snapshot chain")
	fmt.println("  .snapdiff <a> <b>         Diff two snapshots")
	fmt.println("  .snapshot tag <id> <lbl>  Tag a snapshot")
	fmt.println("  .snapshot restore <id>    Restore to a snapshot")
	fmt.println("  .expire [keep]            Expire old snapshots (default 100)")
	fmt.println("  .rollforward              Advance to latest snapshot")
	fmt.println()
	fmt.println("General:")
	fmt.println("  .version                  Print version")
	fmt.println("  .exit / .quit             Exit")
	fmt.println("  .help                     This message")
	fmt.println()
	fmt.println("Flags:")
	fmt.println("  --version                 Print version and exit")
	fmt.println("  --stop-on-error           Exit on first SQL error in script mode")
	fmt.println("  --verbose / -v            Enable debug-level logging")
	fmt.println("  --log-level <level>       Set log level: debug, info, warn, error")
	fmt.println()
	fmt.println("See README.md or ARCH.md for SQL reference.")
}
