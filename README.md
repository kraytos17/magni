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
SELECT * FROM users WHERE score > 50 AND name <> 'Bob';  -- != or <>
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE id IN (1, 3, 5);
SELECT * FROM users WHERE name IN (SELECT name FROM active_users);

-- Sorting & pagination
SELECT * FROM users ORDER BY score DESC;
SELECT * FROM users ORDER BY name ASC LIMIT 5 OFFSET 10;

-- Aggregates & grouping
SELECT COUNT(*), AVG(score), MIN(score), MAX(score), SUM(score) FROM users;
SELECT COUNT(score) FROM users;  -- COUNT(non-NULL values)
SELECT name, COUNT(*) FROM users GROUP BY name HAVING count > 1;

-- JOINs
SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;
SELECT * FROM t1 CROSS JOIN t2;
SELECT * FROM t1, t2 WHERE t1.x = t2.y;
SELECT t1.x, t2.y FROM t1, t2 WHERE t1.x = t2.y;

-- Subqueries
SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;

-- EXPLAIN
EXPLAIN SELECT * FROM users WHERE id = 1;
EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;

-- Aliases
SELECT a.x FROM t AS a;

-- Multi-column ORDER BY
SELECT * FROM users ORDER BY score DESC, name ASC;
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
|---|---|---|
| `.exit` / `.quit` | Exit |
| `.help` | Print help message |
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

### Highlights
- **Copy-on-write (COW) B+tree**: every mutation creates new pages along the path.
  Old pages remain readable for time-travel queries. No WAL or MVCC needed.
- **Single-traversal mutations**: `tree_update_cow` does delete + re-insert in one
  root-to-leaf traversal. Halves page writes vs delete-then-insert.
- **Stream COW mutations**: UPDATE/DELETE apply mutations immediately in the scan
  loop instead of batch-collecting ops first — O(1) peak memory per operation.
- **SQL feature set**: CREATE/DROP/INSERT/SELECT/UPDATE/DELETE, WHERE with AND/OR/LIKE/IN,
  JOINs (INNER/LEFT/CROSS), GROUP BY/HAVING, ORDER BY with NULLS FIRST/LAST, NULLS LAST,
  multi-column ORDER BY, LIMIT/OFFSET, DISTINCT, subqueries, aggregate functions
  (COUNT/SUM/AVG/MIN/MAX, including COUNT(col)), CHECK constraints,
  FOREIGN KEY validation, EXPLAIN, transactions (BEGIN/COMMIT/ROLLBACK).
- **Time-travel**: `AS OF SNAPSHOT <id>` or `AS OF TIMESTAMP <micros>`.
- **Snapshot system**: append-only chain with O(1) lookup, tagging, diff, restore, refs page,
  rollforward log, and `rollforward`/`expire` operations.
- **WAL (Write-Ahead Log)**: sequential append + single fsync per commit; crash recovery replays
  committed frames on open; checkpoint writes WAL frames back to the main file.
- **Slab page cache**: 256 pages in a contiguous 1MB array. Zero per-page heap
  allocations. `map[u32]` for O(1) lookup.
- **Freeblock chain**: SQLite-compatible freeblock list. Deleted cell space is
  tracked and reused. Coalesces adjacent free blocks.
- **Hash join**: integer-key hash join avoids allocation per row; string-key fallback.
- **Pre-resolved WHERE**: column indices resolved once, not per row.
- **LIMIT pushdown**: LIMIT without ORDER BY stops the table scan early.
- **GC throttling**: garbage collection runs on demand via `.expire` or `.checkpoint`.
- **Inline deserialize buffer**: `[dynamic; MAX_COLS]T` avoids heap alloc per row decode.
- **Page bitmap**: GC sweep skips 64-free-page ranges in O(1), accelerating the scan.
- **Schema_Row**: Named-struct abstraction for schema b-tree entries with compact encoding:
  kind byte (`i64(0)` for table) instead of string, conditional skip_root omitted when zero.
- **Column blob**: `0xFE`-marked versioned format with varint-encoded lengths and a packed
  byte encoding type, not_null, pk, has_check, and has_default in a single byte.
- **Pager free-list**: O(1) slot allocation instead of linear scan across 256 cache slots.
- **WAL (Write-Ahead Log)**: sequential append + single fsync per commit; crash recovery replays committed frames.
- **`@(fast_math)` aggregate loops**: SUM/AVG/MIN/MAX compute with IEEE-relaxed ops.
- **`#simple` / `#all_or_none`**: applied to page headers, snapshot headers, and result structs for safety.
- **`types.Storage_Config`**: shared config struct (`allocator` + `zero_copy`) aliased by both `btree.Config` and `cell.Config`, removing duplicate definitions.

---

## Test Coverage

| Package | Tests | What's tested |
|---|---|---|
| `parser` | 38 | Tokenization, all SQL forms, JOINs, subqueries, errors |
| `executor` | 64 | Full DML/DDL, WHERE, ORDER BY, GROUP BY, JOINs, aggregates |
| `btree` | 20 | Insert/find/delete/update, cursor, splits, persistence |
| `cell` | 11 | Serialization roundtrip, zero-copy, validation |
| `pager` | 19 | Page cache, I/O, pinning, eviction, WAL, bitmap |
| `schema` | 11 | Column blob, add/find/drop/list, row round-trip |
| `snapshot` | 12 | Chain, manifests, diff, tags, timestamp lookup, GC |
| `integration` | 22 | CRUD, time-travel, JOINs, UPDATE/DELETE, restore, persistence, columnar reads |
| **Total** | **197** | |

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
├── main.odin              CLI entry, REPL, dot-commands
├── btree/
│   ├── cow.odin           COW insert/find/delete/update
│   ├── cursor.odin        In-order row cursor (fixed path stack)
│   ├── headers.odin       Page headers, freeblock helpers
│   ├── split.odin         Leaf/interior/root split operations
│   └── tree.odin          B+tree: insert, find, delete, update, verify
├── cell/
│   └── cell.odin          Row serialization, varint, validation
├── db/
│   ├── admin.odin         Stats, dump, describe, checkpoint, integrity
│   ├── db.odin            Database handle, open/close
│   ├── execute.odin       Statement execution, query API
│   ├── snapshot_cmds.odin Snapshot restore, diff, tag, list
│   └── txn.odin           Begin/commit/rollback
├── executor/
│   ├── display.odin       Result formatting, aggregates
│   ├── dml.odin           INSERT/UPDATE/DELETE (legacy + COW)
│   ├── executor.odin      Statement dispatch
│   ├── select.odin        SELECT, JOIN, subqueries, scan_table
│   ├── sort.odin          Sorting, DISTINCT
│   ├── types.odin         Shared type definitions
│   ├── util.odin          Helpers: qualified columns, CHECK, compare
│   └── where.odin         WHERE clause evaluation, LIKE
├── parser/
│   ├── free.odin          AST memory cleanup
│   ├── parse_ddl.odin     CREATE/DROP TABLE
│   ├── parse_dml.odin     INSERT/UPDATE/DELETE
│   ├── parse_select.odin  SELECT, JOIN, identifiers
│   ├── parse_where.odin   WHERE clause, IN, value parsing
│   ├── parser.odin        Token types, parse dispatcher
│   ├── tokenizer.odin     Lexer, keyword matching
│   └── types.odin         AST node types
├── pager/
│   ├── freelist.odin      Free-page linked list
│   └── pager.odin         Page cache, file I/O, eviction
├── schema/
│   ├── schema.odin        Table metadata, add/find/drop
│   └── serialize.odin     Column blob serialization
├── snapshot/
│   ├── expire.odin       Explicit snapshot expiration + GC
│   ├── gc.odin            Prune + garbage collect
│   ├── manifest.odin      Manifest creation, diff
│   ├── refs.odin          Named refs (branches/tags), rollforward log
│   └── snapshot.odin      Snapshot chain, tags, walk
└── types/
    └── types.odin         Core types: Value, Column, Table, SerialType
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
- No `UNION`, `INTERSECT`, `EXCEPT`
- No `FOREIGN KEY` enforcement on INSERT/UPDATE (validated at CREATE TABLE time)
- Mixed `AND`/`OR` in WHERE not supported (must be uniform)
- No B-tree rebalancing on delete (pages may fragment)
- `CHECK` expression limited to simple integer comparisons (col > 0, col < 100)
