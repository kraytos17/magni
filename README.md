# Magni

Embedded SQL database engine written in [Odin](https://odin-lang.org). Features a
copy-on-write B+tree storage engine, SQLite-compatible row format, append-only
snapshot chain with time-travel queries, and ACID-ish transactions.

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
| **SQL** | CREATE/DROP/INSERT/SELECT/UPDATE/DELETE, WHERE (AND/OR/LIKE/IN), JOINs (INNER/LEFT/CROSS), GROUP BY/HAVING, ORDER BY (multi-column, NULLS FIRST/LAST), LIMIT/OFFSET, DISTINCT, subqueries, aggregates (COUNT/SUM/AVG/MIN/MAX), CHECK/FOREIGN KEY constraints, EXPLAIN, transactions |
| **Storage** | Copy-on-write B+tree — every mutation creates new pages along the path; old pages persist for time-travel. Single-traversal UPDATE (delete + re-insert in one pass). SQLite-compatible row format with varint encoding. Freeblock chain reuses deleted cell space. |
| **Time-Travel** | Append-only snapshot chain. Query data `AS OF SNAPSHOT <id>` or `AS OF TIMESTAMP <micros>`. Restore to any historical state. Diff two snapshots. Tag snapshots with labels. Rollforward log. |
| **WAL** | Write-ahead log with sequential append and single `fsync` per commit. Crash recovery replays committed frames; corrupt frames (bad FNV checksum) are skipped. Checkpoint flushes WAL frames back to the main file. |
| **Line Editor** | Raw-mode REPL with arrow-key navigation, history (Up/Down), Ctrl-R incremental reverse search (results shown below prompt, wraps around), Ctrl-T transpose, Ctrl-L clear screen, Ctrl-Z multi-level undo, Tab dot-command completion, bracketed paste, SIGWINCH-aware wrap-correct redraw with CJK support. Falls back to `bufio.Reader` on non-TTY input. |
| **Performance** | Slab page cache (256 pages, 1MB contiguous, zero per-page heap allocs). O(1) slot allocation via free-list. Hash join (integer key, string fallback). Pre-resolved WHERE indices. LIMIT pushdown. Page bitmap for O(1) 64-page GC range skips. |

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
| `.desc <table>` | Describe table columns |
| `.dump <table>` | Dump all rows |
| `.stats` | Database statistics |
| `.integrity` | Verify all B-trees |
| `.checkpoint` | Flush pages + garbage collect |
| `.expire [keep]` | Expire old snapshots (default 100) and garbage collect |
| `.snapshots` | Show snapshot chain |
| `.snapdiff <a> <b>` | Diff two snapshots |
| `.snapshot tag <id> <label>` | Tag a snapshot |
| `.snapshot restore <id>` | Restore to historical state |
| `.rollforward` | Advance current state to the most recent snapshot |
| `.begin` / `.commit` / `.rollback` | Transaction control |

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
| Tab | Dot-command completion |
| Paste | Bracketed paste — multi-line pastes inserted as a single block |

### CLI Flags

| Flag | Description |
|---|---|
| `--help` | Print usage |
| `--version` | Print version and exit |
| `--stop-on-error` | Exit on first SQL error in script/pipe mode |

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

---

## Project Layout

```
src/
├── main.odin              CLI entry, REPL, dot-commands
├── btree/                 COW B+tree storage engine
├── cell/                  Row serialization (SQLite-compatible varint)
├── db/                    Database handle, execute, admin, snapshots, transactions
├── executor/              Statement dispatch, SELECT/JOIN/aggregates, WHERE, DML, sort
├── linedit/               Raw-mode line editor (REPL input)
├── parser/                Lexer, recursive-descent parser, AST
├── pager/                 Slab page cache, WAL, freelist
├── schema/                Table metadata (schema B-tree)
├── snapshot/              Snapshot chain, manifests, GC, refs, expire
└── types/                 Core types: Value, Column, Table, SerialType
tests/
└── * _test.odin           281 tests across all packages
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

---

## Limitations

- No secondary indexes (only the primary-key B-tree)
- No `UNION`, `INTERSECT`, `EXCEPT`
- No `FOREIGN KEY` enforcement on INSERT/UPDATE (validated at CREATE TABLE time)
- Mixed `AND`/`OR` in WHERE not supported (must be uniform)
- `CHECK` expression limited to simple integer comparisons (col > 0, col < 100)
- REPL line editor: no SQL keyword/table-name completion (dot-commands only)
