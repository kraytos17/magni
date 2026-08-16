# Magni

Embedded SQL database engine written in [Odin](https://odin-lang.org). Features a
copy-on-write B+tree storage engine, SQLite-compatible row format, append-only
snapshot chain with time-travel queries, and transactional writes backed by a
write-ahead log. The concurrency model is single-writer: writes take an exclusive
lock, reads take a shared lock, and each committed write is durable via one
`fsync` of the WAL.

---

## Quick Start

```bash
# Build and run REPL
make run

# Run all tests
make test

# CLI example
./build/magni mydb.db --eval "CREATE TABLE t (x INT); INSERT INTO t VALUES (42); SELECT * FROM t;"
```

---

## SQL Reference

### DDL

```sql
-- Create table with constraints
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    score REAL DEFAULT 0.0,
    data  BLOB
);

-- Drop table
DROP TABLE users;

-- Table with CHECK constraint
CREATE TABLE products (
    price INT CHECK (price > 0)
);
```

### DML

```sql
-- Insert with explicit values
INSERT INTO users VALUES (1, 'Alice', 99.5, X'CAFE');

-- Insert with column list (reorder/defaults/NULLs)
INSERT INTO users (name, score, id) VALUES ('Bob', 88.0, 2);

-- Update with WHERE
UPDATE users SET score = 100.0 WHERE id = 1;

-- Delete with WHERE
DELETE FROM users WHERE id = 1;
```

### SELECT

```sql
-- Basic queries
SELECT * FROM users;
SELECT name, score FROM users;
SELECT DISTINCT name FROM users;

-- Filtering
SELECT * FROM users WHERE score > 50 AND name != 'Bob';
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE id IN (1, 3, 5);
SELECT * FROM users WHERE id NOT IN (1, 3) AND NOT (name LIKE 'A%');
-- Boolean expressions: AND binds tighter than OR; parens group
SELECT * FROM users WHERE (age < 30 OR age > 60) AND score > 50;

-- Sorting & pagination
SELECT * FROM users ORDER BY score DESC;
SELECT * FROM users ORDER BY name ASC LIMIT 5 OFFSET 10;

-- Aggregates & grouping
SELECT COUNT(*), AVG(score), SUM(score) FROM users;
SELECT name, COUNT(*) FROM users GROUP BY name HAVING count > 1;

-- JOINs
SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 CROSS JOIN t2;

-- Subqueries
SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;
SELECT * FROM t WHERE x IN (SELECT y FROM t2);

-- Set operations
SELECT x FROM t1 UNION SELECT x FROM t2;
SELECT x FROM t1 UNION ALL SELECT x FROM t2;
SELECT x FROM t1 INTERSECT SELECT x FROM t2;
SELECT x FROM t1 EXCEPT SELECT x FROM t2;

-- FROM-less SELECT (literal columns)
SELECT 1, 'a', NULL;

-- EXPLAIN
EXPLAIN SELECT * FROM users WHERE id = 1;
```

### Time-Travel Queries

```sql
-- Query data as of a specific snapshot ID
SELECT * FROM users AS OF SNAPSHOT 5;

-- Query data as of a wall-clock timestamp (microseconds)
SELECT * FROM users AS OF TIMESTAMP 1719000000000000;
```

### Transactions

```sql
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 75.0);
COMMIT;
-- or
ROLLBACK;
```

---

## Key Features

| Area | Capabilities |
|---|---|
| **SQL** | CREATE/DROP/INSERT/SELECT/UPDATE/DELETE, WHERE (full boolean expressions with AND/OR precedence, parentheses, NOT/NOT IN/NOT LIKE), column aliases (AS and bare identifier), multi-row INSERT VALUES, JOINs (INNER/LEFT/CROSS), GROUP BY/HAVING, ORDER BY (multi-column, NULLS FIRST/LAST), LIMIT/OFFSET, DISTINCT, subqueries, set operations (UNION [ALL]/INTERSECT [ALL]/EXCEPT [ALL]), FROM-less literal SELECTs, aggregates (COUNT/SUM/AVG/MIN/MAX), CHECK/FOREIGN KEY constraints, EXPLAIN, transactions, hex literals |
| **Storage** | Copy-on-write B+tree — every mutation creates new pages along the path; old pages persist for time-travel. Single-traversal UPDATE (delete + re-insert in one pass). SQLite-compatible row format with varint encoding. Freeblock chain reuses deleted cell space. **Columnar page format** with delta compression (auto-converted to row-major on write). **Page format versioning** (v1 legacy, v2 current) via format registry. **B-tree rebalancing** — auto-merges sparse adjacent leaves. **Row count tracking** with fast `COUNT(*)` via incremental cache. |
| **Time-Travel** | Append-only snapshot chain. Query data `AS OF SNAPSHOT <id>` or `AS OF TIMESTAMP <micros>`. Restore to any historical state. Diff two snapshots. Tag snapshots with labels. Rollforward log. |
| **WAL** | Write-ahead log with sequential append and single `fsync` per commit. Crash recovery replays committed frames; corrupt frames (bad FNV checksum) are skipped. Checkpoint flushes WAL frames back to the main file. |
| **Line Editor** | Raw-mode REPL with arrow-key navigation, history (Up/Down), Ctrl-R incremental reverse search (results shown below prompt, wraps around), Ctrl-T transpose, Ctrl-L clear screen, Ctrl-Z multi-level undo, Tab dot-command and SQL keyword completion with table/column name support, bracketed paste, SIGWINCH-aware wrap-correct redraw with CJK support. Falls back to `bufio.Reader` on non-TTY input. |
| **Concurrency** | `db.mu` uses `RW_Mutex` — SELECT and read-only admin commands take shared lock (multiple can run); INSERT/UPDATE/DELETE/DDL take exclusive lock. Pager internally uses `RW_Mutex` with shared locks for read-only page operations. COW snapshots enable time-travel reads without blocking. |
| **Performance** | Slab page cache (256 pages, 1MB contiguous, zero per-page heap allocs). O(1) slot allocation via free-list. Hash join (integer key, string fallback). Pre-resolved WHERE indices. LIMIT pushdown. Page bitmap for O(1) 64-page GC range skips. |
| **Logging** | `core:log` with configurable levels (debug/info/warn/error). `--log-level`, `--verbose`/`-v`, `MAGNI_LOG_LEVEL` env var. Logs go to stderr; query output stays clean on stdout. REPL runs at error level. |

See [ARCH.md](ARCH.md) for detailed architecture documentation covering the B-tree, page cache, serialization, snapshot system, and all optimization internals.

---

## CLI Reference

### Dot-Commands

| Command | Description |
|---|---|
| `.exit` / `.quit` | Exit |
| `.help` | Print help message |
| `.version` | Print version |
| `.tables` | List all tables |
| `.schema` | Show CREATE TABLE statements |
| `.debug_schema` | Show low-level schema details (root pages, flags) |
| `.tree_page <n>` | Print B-tree page structure |
| `.desc <table>` | Describe table columns |
| `.dump <table>` | Dump all rows |
| `.stats` | Database statistics |
| `.integrity` | Verify all B-trees |
| `.checkpoint` | Flush pages + garbage collect |
| `.expire [keep]` | Expire old snapshots (default 20) and garbage collect |
| `.snapshots` | Show snapshot chain |
| `.snapdiff <a> <b>` | Diff two snapshots |
| `.snapshot tag <id> <label>` | Tag a snapshot |
| `.snapshot restore <id>` | Restore to historical state |
| `.rollforward` | Advance current state to the most recent snapshot |
| `.begin` / `.commit` / `.rollback` | Transaction control |
| `.snapshot_debug` | Verbose snapshot chain dump |

### REPL Keyboard Shortcuts

| Key | Action |
|---|---|
| ← → | Move cursor |
| ↑ ↓ | History navigation |
| Home / Ctrl-A | Beginning of line |
| End / Ctrl-E | End of line |
| Backspace / Delete | Delete backward/forward |
| Ctrl-K | Kill to end of line |
| Ctrl-U | Kill to start of line |
| Ctrl-W | Delete word backward |
| Ctrl-Z | Undo last edit |
| Ctrl-L | Clear screen (redraws prompt) |
| Ctrl-T | Transpose characters |
| Ctrl-C | Cancel current line / abort multi-line statement |
| Ctrl-D (empty line) | Exit |
| Ctrl-R | Incremental reverse history search (results shown below prompt; wraps around with visual indicator) |
| Tab | Dot-command, SQL keyword, and table/column name completion |
| Paste | Bracketed paste — multi-line pastes inserted as a single block |

### CLI Flags

| Flag | Description |
|---|---|
| `--help` | Print usage |
| `--version` | Print version and exit |
| `--stop-on-error` | Exit on first SQL error in script/pipe mode |
| `--log-level <level>` | Set log level: `debug`, `info`, `warn`, `error` (default: `info`) |
| `--verbose` / `-v` | Enable debug-level logging (alias for `--log-level debug`) |

### CLI Modes

```bash
# Interactive REPL (raw-mode line editor on TTY, bufio fallback on pipe)
./build/magni [database]

# Single statement
./build/magni mydb.db --eval "SELECT * FROM t;"

# Execute SQL file
./build/magni mydb.db --file script.sql

# Pipe mode (non-interactive, uses fallback reader)
echo "SELECT * FROM t;" | ./build/magni mydb.db
```

### Output Format

Query results and the table-valued dot-commands `.desc` and `.dump` are rendered as markdown
tables via Odin's `core:text/table` package: pipe-separated columns with an alignment divider
row (`:---`) below the header. Column widths are computed with `unicode_width_proc`, so
multi-byte CJK characters align correctly. A footer line `(N rows)` reports the number of rows
printed. `.tables` lists table names as plain indented lines; `.schema` prints the stored
`CREATE TABLE` statements verbatim.

```
|a|b|
|:-|:-|
|1|x|
(1 rows)
```

### Logging

Magni logs through Odin's built-in [`core:log`](https://pkg.odin-lang.org/core/log/) package.
Log messages go to **stderr**; query results and dot-command output stay on **stdout** — so
`--eval`, `--file`, and pipe mode stay clean and scriptable.

| Level | Enable via |
|---|---|
| `debug` | `--verbose` / `-v`, `--log-level debug`, or `MAGNI_LOG_LEVEL=DEBUG` |
| `info` | `--log-level info` (default) |
| `warn` | `--log-level warn` |
| `error` | `--log-level error` |

Level resolution order: `--verbose` > `--log-level <level>` > `MAGNI_LOG_LEVEL` env var >
default `info`. The interactive REPL always runs at `error` level to keep the prompt clean.

```bash
# Query results on stdout, log messages on stderr
./build/magni mydb.db --eval "SELECT * FROM t;" 2>magni.log

# Debug diagnostics
./build/magni --verbose mydb.db --eval "CREATE TABLE t (x INT);"
```

---

## Project Layout

```
src/
├── main.odin              CLI entry, REPL, dot-commands
├── btree/                 COW B+tree: tree ops, cursor, split/merge, rebalance,
│                          skip index, page format registry (v1/v2), columnar
│                          conversion, COW helpers
├── cell/                  Row/cell serialization: SQLite varint, columnar encoding
├── db/                    Database handle: open/close, execute, admin, snapshots,
│                          transactions, programmatic Query_Result API
├── executor/              Statement dispatch, SELECT/JOIN/aggregates, WHERE/DML,
│                          sort, set operations, result rendering (core:text/table),
│                          utility types
├── linedit/               Raw-mode line editor (main + term/keys/buffer/render/
│                          history/stub_windows)
├── parser/                Lexer, recursive-descent parser, AST, free helpers
├── pager/                 Slab page cache, WAL, freelist, page bitmap, int range
├── schema/                Table metadata: schema B-tree, column blob serialization
├── snapshot/              Snapshot chain, manifests, GC, refs, expire, rollforward
└── types/                 Core types: Value, Column, Table, SerialType, Foreign_Key
tests/
└── * _test.odin           318 test functions across all packages
```

---

## Build System

```bash
make build         # Debug build (default)
make release       # Release build (aggressive opt)
make test          # Run all tests
make vet-all       # Full vet suite (build + test with all flags)
make clean         # Remove build directory
```

Requires Odin (see [odin-lang.org](https://odin-lang.org)).

> **Platform support:** Linux and macOS are supported. The Windows build compiles
> but the line editor is a stub (`read_line` always fails), so the interactive REPL
> is not usable on Windows yet.

---

## Limitations

- No user-managed secondary indexes (only the implicit primary-key B-tree and auto-built skip indexes exist)
- No `FOREIGN KEY` enforcement on INSERT/UPDATE (validated at CREATE TABLE time)
- `CHECK` expression limited to simple integer comparisons (col > 0, col < 100, >=, <=, =, !=)
- Max 10 columns per table
- REPL line editor: SQL keyword and table/column name completion only (no in-expression or JOIN completion)
