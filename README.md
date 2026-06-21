# Magni

Embedded SQL database engine written in [Odin](https://odin-lang.org). Features a
copy-on-write B+tree storage engine, SQLite-compatible row format, append-only
snapshot chain with time-travel queries, and ACID-ish transactions.

---

## Quick Start

```bash
# Build and run REPL
make run

# Run all 142 tests
make test

# Run full vet suite
make vet-all

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

-- Sorting & pagination
SELECT * FROM users ORDER BY score DESC;
SELECT * FROM users ORDER BY name ASC LIMIT 5 OFFSET 10;

-- Aggregates & grouping
SELECT COUNT(*), AVG(score), MIN(score), MAX(score), SUM(score) FROM users;
SELECT name, COUNT(*) FROM users GROUP BY name HAVING count > 1;

-- JOINs
SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 CROSS JOIN t2;
SELECT * FROM t1, t2 WHERE t1.x = t2.y;
SELECT t1.x, t2.y FROM t1, t2 WHERE t1.x = t2.y;

-- Subqueries
SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;

-- Aliases
SELECT a.x FROM t AS a;
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

### Dot-Commands (REPL)

| Command | Description |
|---|---|
| `.exit` / `.quit` | Exit |
| `.tables` | List all tables |
| `.schema` | Show CREATE TABLE statements |
| `.desc <table>` | Describe table columns |
| `.dump <table>` | Dump all rows |
| `.stats` | Database statistics |
| `.integrity` | Verify all B-trees |
| `.checkpoint` | Flush pages + garbage collect |
| `.snapshots` | Show snapshot chain |
| `.snapdiff <a> <b>` | Diff two snapshots |
| `.snapshot tag <id> <label>` | Tag a snapshot |
| `.snapshot restore <id>` | Restore to historical state |
| `.begin` / `.commit` / `.rollback` | Transaction control |

### CLI Modes

```bash
# Interactive REPL
./build/magni [database]

# Single statement
./build/magni mydb.db --eval "SELECT * FROM t;"

# Execute SQL file
./build/magni mydb.db --file script.sql

# Pipe mode
echo "SELECT * FROM t;" | ./build/magni mydb.db
```

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      CLI / REPL                     │
├─────────────────────────────────────────────────────┤
│                  SQL Layer                          │
│  parser/   — lexer + recursive-descent parser       │
│  executor/ — statement execution, WHERE, JOINs      │
├─────────────────────────────────────────────────────┤
│                 Storage Engine                      │
│  btree/    — COW B+tree (insert/find/delete/update) │
│  cell/     — SQLite-compatible row serialization    │
│  pager/    — inline slab page cache, freelist       │
│  schema/   — table metadata (schema B-tree)         │
│  snapshot/ — append-only snapshot chain, manifests  │
│  db/       — coordinator, snapshot index            │
└─────────────────────────────────────────────────────┘
```

See [ARCH.md](ARCH.md) for complete architecture documentation.

---

## Features

### Storage Engine
- **Copy-on-write (COW) B+tree**: every mutation creates new pages along the path.
  Old pages remain readable for time-travel queries. No WAL or MVCC needed.
- **Single-traversal mutations**: `tree_update_cow` does delete + re-insert in one
  root-to-leaf traversal. Halves page writes vs delete-then-insert.
- **Slab page cache**: 256 pages in a contiguous 1MB array. Zero per-page heap
  allocations. Open-addressing hash lookup with linear probe.
- **Freeblock chain**: SQLite-compatible freeblock list. Deleted cell space is
  tracked and reused. Coalesces adjacent free blocks.
- **Header-only zero on allocation**: only 100 bytes zeroed, not 4KB.

### Snapshot System
- **Append-only chain**: each mutation creates an immutable snapshot. The chain
  is never modified — `snapshot_restore` appends a new `.RESTORE` snapshot.
- **O(1) snapshot lookup**: in-memory `map[snapshot_id]page_number` built on
  open, updated on every create/prune.
- **Tags**: human-readable labels stored in unused page space.
- **Time-travel**: `AS OF SNAPSHOT <id>` or `AS OF TIMESTAMP <micros>`.
- **Garbage collection**: mark-sweep frees pages unreachable from the latest N
  snapshots. Walks manifests and B-tree pages via `btree.collect_pages`.

### Concurrency
- `Database.mu` (`sync.Mutex`) serializes all statement execution.
- Pager uses `sync.RW_Mutex`: `page_count` and `page_in_cache` use shared
  (read) locks; all other operations use exclusive locks.

### Memory Management
- All per-statement allocations use `context.temp_allocator`, bulk-freed via
  `free_all()` at end of `db.execute()`. No per-node freeing during parse.
- Cursor path is a fixed-size `[32]` stack array. No heap allocation per cursor.
- Cell serialization uses inline `[MAX_COLS]u64` array. Zero heap allocs.

---

## Test Coverage (142 tests)

| Package | Tests | What's tested |
|---|---|---|
| `parser` | 38 | Tokenization, all SQL forms, JOINs, subqueries, errors |
| `executor` | 64 | Full DML/DDL, WHERE, ORDER BY, GROUP BY, JOINs, aggregates |
| `btree` | 20 | Insert/find/delete/update, cursor, splits, persistence |
| `cell` | 11 | Serialization roundtrip, zero-copy, validation |
| `pager` | 12 | Page cache, I/O, pinning, eviction |
| `schema` | 10 | Column blob, add/find/drop/list |
| `snapshot` | 12 | Chain, manifests, diff, tags, timestamp lookup, GC |
| `integration` | 16 | CRUD, time-travel, JOINs, UPDATE/DELETE, restore |

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

## Project Layout

```
src/
├── main.odin          CLI entry, REPL, dot-commands
├── btree/
│   ├── tree.odin      B+tree: insert, find, delete, update, COW variants
│   ├── headers.odin   Page headers, freeblock helpers
│   └── cursor.odin    In-order row cursor (fixed path stack)
├── cell/
│   └── cell.odin      Row serialization, validation
├── db/
│   └── db.odin        Database handle, execute/query, snapshot index
├── executor/
│   └── executor.odin  SQL execution, WHERE, JOINs, aggregates
├── parser/
│   └── parser.odin    Lexer + recursive-descent parser
├── pager/
│   └── pager.odin     Slab page cache, file I/O, freelist
├── schema/
│   └── schema.odin    Table metadata (schema B-tree)
├── snapshot/
│   └── snapshot.odin  Snapshot chain, manifests, tags, GC, diff
├── types/
│   └── types.odin     Core types: Value, Column, Table, SerialType
└── utils/
    └── utils.odin     Varint, endian I/O helpers
tests/
├── parser_test.odin
├── executor_test.odin
├── btree_test.odin
├── cell_test.odin
├── pager_test.odin
├── schema_test.odin
├── snapshot_test.odin
└── integration_test.odin
```

---

## Limitations

- No secondary indexes (only the primary-key B-tree)
- No WAL / crash recovery
- No `UNION`, `INTERSECT`, `EXCEPT`
- No `FOREIGN KEY` enforcement
- No `CHECK` constraints beyond type validation
- No nested subqueries in WHERE (`IN (SELECT ...)`)
- Mixed `AND`/`OR` in WHERE not supported (must be uniform)
- No B-tree rebalancing on delete (pages may fragment)
