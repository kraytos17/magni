# Magni Architecture

Magni is a SQL database engine built from scratch in [Odin](https://odin-lang.org/). It implements a
subset of SQL with a custom B+tree-backed storage engine, an SQLite-compatible record format, and a
recursive-descent parser. This document describes the system's design, component relationships, data
flow, and key implementation details.

---

## Table of Contents

- [Project Layout](#project-layout)
- [Data Flow Overview](#data-flow-overview)
- [Package Responsibilities](#package-responsibilities)
- [Parser](#parser)
- [Executor](#executor)
- [Storage Engine](#storage-engine)
- [CLI & REPL](#cli--repl)
- [Schema Management](#schema-management)
- [Thread Safety](#thread-safety)
- [Build System](#build-system)
- [Test Infrastructure](#test-infrastructure)
- [Key Design Decisions & Limitations](#key-design-decisions--limitations)

---

## Project Layout

```
magni/
├── src/
│   ├── main.odin              CLI entry point, REPL, dot-commands, batch/pipe mode
│   ├── btree/
│   │   ├── tree.odin          B+tree engine: insert, find, delete, cursor, split
│   │   ├── headers.odin       Page header structs, page type enums
│   │   └── cursor.odin        In-order row cursor via path stack
│   ├── cell/
│   │   └── cell.odin          SQLite-compatible row serialization / deserialization
│   ├── db/
│   │   └── db.odin            Top-level database handle, open/close/execute
│   ├── executor/
│   │   └── executor.odin      SQL execution, WHERE evaluation, JOINs, aggregates
│   ├── parser/
│   │   └── parser.odin        Lexer + recursive-descent parser, full AST types
│   ├── pager/
│   │   └── pager.odin         Page cache, file I/O, LRU eviction
│   ├── schema/
│   │   └── schema.odin        Table metadata persistence (schema B-tree on page 1)
│   ├── types/
│   │   └── types.odin         Core types: Value, Column, Table, Serial_Type, constants
│   └── utils/
│       └── utils.odin         Varint encode/decode, endian read/write helpers
├── tests/
│   ├── parser_test.odin       38 tests — parsing, literals, JOINs, subqueries, errors
│   ├── executor_test.odin     64 tests — full end-to-end SQL execution, constraints
│   ├── btree_test.odin        17 tests — B-tree insert/find/delete/split/cursor
│   ├── cell_test.odin         11 tests — serialization roundtrip, validation
│   ├── pager_test.odin        12 tests — page cache, I/O, eviction
│   └── schema_test.odin       10 tests — metadata persistence, columns
├── Makefile                   Build, test, vet, CLI integration targets
├── odinfmt.json               Odin formatter config
├── ols.json                   Odin Language Server config
└── README.md
```

---

## Data Flow Overview

```
SQL string
    │
    ▼
┌────────────────┐
│   parser.odin  │  tokenize → recursive-descent parse → AST
│  (lex + parse) │
└───────┬────────┘
        │  Statement{Create_Stmt | Insert_Stmt | Select_Stmt | ...}
        ▼
┌────────────────┐
│    db.odin     │  lock mutex, dispatch to executor
│  db.execute()  │
└───────┬────────┘
        │
        ▼
┌──────────────────┐
│ executor.odin    │  switch on statement type:
│  execute()       │    CREATE → allocate root page, persist schema
│                  │    INSERT → validate, serialize, btree insert
│                  │    SELECT → scan/filter/aggregate/sort/display
│                  │    UPDATE → collect rowids, delete + reinsert
│                  │    DELETE → collect rowids, batch delete
│                  │    DROP   → remove schema entry
└───────┬──────────┘
        │
        ▼
┌──────────────────┐
│ btree/tree.odin  │  recursive descent, binary search, split & propagate
│  insert / find   │
│  delete / foreach│
└───────┬──────────┘
        │
        ▼
┌──────────────────┐
│ cell/cell.odin   │  encode/decode Value[] ↔ []byte (varint record format)
│  serialize /     │
│  deserialize     │
└───────┬──────────┘
        │
        ▼
┌──────────────────┐
│ pager/pager.odin │  get/allocate/flush pages, LRU cache, file I/O
│  page cache      │
└──────────────────┘
```

---

## Package Responsibilities

### `main` — CLI Entry Point

File: `src/main.odin`

- Parses CLI flags using `core:flags`:
  - Positional argument: database path (default `"test.db"`)
  - `--eval "<sql>"`: execute a single SQL statement
  - `--file <path>`: read and execute SQL from a file
- **Mode selection** (priority order):
  1. `--file <path>` — batch mode from file
  2. `--eval "<sql>"` — single statement mode
  3. stdin is not a TTY — pipe mode (read all of stdin as SQL)
  4. stdin is a TTY — interactive REPL
- **REPL** features:
  - Multi-line input (accumulates until `;`)
  - Continuation prompt `...> ` on subsequent lines
  - Dot-commands: `.exit`, `.quit`, `.help`, `.tables`, `.schema`,
    `.debug_schema`, `.dump <table>`, `.desc <table>`, `.stats`,
    `.integrity`, `.checkpoint`
  - Silent EOF on Ctrl+D (no error message)

### `parser` — Lexer & AST

File: `src/parser/parser.odin`

- **Lexer**: character-by-character scanner producing `[]Token`.
  - Token types enum (~70 values): keywords (`CREATE`, `TABLE`, `SELECT`, ...),
    operators (`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`),
    punctuation, literals.
  - String literals: single-quoted with `''` escape.
  - BLOB literals: `X'hex'` / `x'hex'` notation.
  - Numbers: integers and floats (handles leading negative sign).
  - Comments: `--` line comments.
- **Recursive-descent parser**: one function per grammar rule, rooted at `parse()`.
  - `parse_create_table` — columns, types, constraints (`PK`, `NOT NULL`, `DEFAULT`).
  - `parse_insert` — optional column list, `VALUES` rows.
  - `parse_select` — select list (expr/aggregate/star), `FROM` (table/join/subquery),
    `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`.
  - `parse_update` — `SET col = expr`, optional `WHERE`.
  - `parse_delete` — optional `WHERE`.
  - `parse_drop_table`.
- **`Where_Clause`** uses uniform `AND` or `OR` (no mixing). Conditions are
  `column op value` or `column op column` (for equi-joins).
- Memory: allocated with caller-provided allocator; `statement_free` /
  `where_clause_free` for cleanup.

### `executor` — SQL Execution

File: `src/executor/executor.odin`

Entry point: `execute(schema_tree, stmt) -> bool`

**CREATE TABLE** (`exec_create`):
- Validate column definitions (no duplicates, max 10, single PK).
- Check table doesn't already exist.
- Allocate a root page (skipping schema page 1), initialize as leaf.
- Persist metadata via `schema.add_table`.

**INSERT** (`exec_insert`):
- Look up table from schema.
- Handle optional column list: reorder values, fill defaults / NULLs.
- Validate values against column types via `cell.validate`.
- Determine row ID: explicit PK if provided, else auto-increment via
  `tree_next_rowid`.
- Insert into B-tree via `btree.tree_insert`.

**SELECT** (`exec_select`):
Three dispatch paths:
1. **Single table** — `exec_select_single`: `scan_table` → filter rows →
   aggregate (if aggrs present) → sort → display.
2. **Multi-table JOINs** — build combined column layout with `Table_Col_Range`,
   nested-loop join over source tables, ON clause evaluation,
   LEFT JOIN null-padding.
3. **Subqueries** — execute inner SELECT, treat result as virtual table,
   apply outer WHERE / ORDER BY / LIMIT.

Key subroutines:
- `scan_table` — B-tree cursor iteration, collects `Row_Entry{rowid, values}`.
- `evaluate_where` — match conditions against row values; supports qualified
  names (`t.col`).
- `try_pk_lookup` — fast path: detect `WHERE pk = literal` and use O(log n)
  `tree_find` instead of full scan.
- `compute_aggregates` — `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
- `exec_select_aggregate_combined` — GROUP BY grouping + per-group aggregate
  computation + HAVING filter.
- `sort_rows` — stable sort via `slice.sort_by_with_data`.
- `display_results` — pretty-printed table with headers, LIMIT/OFFSET.
- `try_join_match` — combine two rows and evaluate ON clause, used by both
  virtual-table and real-table join branches.

**UPDATE** (`exec_update`):
- Two-phase: collect matching row IDs, then delete + reinsert with new values.
- PK fast-path for single-row updates.
- Validates new values; skips no-op (unchanged) updates.

**DELETE** (`exec_delete`):
- Collect target row IDs, then batch delete.
- PK fast-path support.

**DROP TABLE** (`exec_drop`):
- Validate table exists.
- `schema.drop_table` removes metadata from schema tree.

### `btree` — B+tree Engine

Files: `src/btree/tree.odin`, `src/btree/headers.odin`, `src/btree/cursor.odin`

- Custom B+tree (table b-trees only, no index b-trees).
- **Page types**: `LEAF_TABLE` (13), `INTERIOR_TABLE` (5).
- **Node layout**:
  ```
  [Page_Header: 8B] [Cell_Pointers: N×2B] [free space] [Cell data grows down]
  ```
  Interior pages append a 4B `rightmost_ptr` after the standard header.
- **Operations**:
  - `tree_insert` — recursive descent, split at midpoint when full.
    Root split creates a new interior root + two leaves.
  - `tree_find` — binary search descending to leaf, then `leaf_lower_bound`.
  - `tree_delete` — remove cell by rowid, update `fragmented_bytes`.
  - `tree_foreach` — iterate all rows (uses cursor).
  - `cursor` — leftmost-deep drill-down, then in-order advancement via
    path stack.
- **Split**: leaf splits move upper half to new sibling; interior splits
  promote the separator key upward. No rebalancing on delete.

### `cell` — Record Serialization

File: `src/cell/cell.odin`

- SQLite-compatible record format:
  ```
  [PayloadLength varint] [RowID varint] [HeaderSize varint] [SerialTypes...] [Values...]
  ```
- Serial types encode both type and byte size:
  - `NULL=0`, `INT8=1` through `INT64=6`, `FLOAT64=7`, `ZERO=8`, `ONE=9`
  - `BLOB = 12 + 2*N`, `TEXT = 13 + 2*N` for length N
- Zero-copy mode: string/BLOB values can point directly into page buffer
  (unsafe but allocation-free). Controlled by `Cell.Config.zero_copy`.
- `Cell` struct owns its data when `owns_data = true`; otherwise values
  borrow from the page.
- `calculate_size` computes serialized size; `validate` checks column type
  compatibility.

### `pager` — Page Cache & I/O

File: `src/pager/pager.odin`

- **Page size**: 4096 bytes (configurable via `PAGE_SIZE` constant).
- **Database header**: 100 bytes on page 1:
  - Magic: `"MAGNI_DB_v1.0"` (13 bytes)
  - `page_size`, `page_count`, `schema_version` (u32le each)
  - 75 bytes reserved
- **Page 1** is special: its cell content area starts at offset 100
  (after the database header); all other pages start at offset 0.
- **Cache**: `map[u32]^Page` with pin/unpin reference counting.
  - `get_page` — cache hit: return; miss: read from disk, insert.
  - `allocate_page` — append zero-filled page to file, cache it.
  - `flush_all` — write all dirty pages to disk.
  - Eviction: linear scan for first unpinned page (simple LRU approximation).
  - Max cache: default 256 pages (1 MB).

### `schema` — Table Metadata

File: `src/schema/schema.odin`

- Schema B-tree lives on page 1 (`SCHEMA_PAGE_ID = 1`).
- Each table is stored as a row:
  - Row ID = `fnv64(table_name) & 0x7FFF...`
  - Values: `["table", name, name, root_page, sql_stmt, columns_blob]`
- **Column blob** format:
  ```
  [count:u32le]  { [name_len:u32le, name_bytes, type_byte, flags_byte,
                    default_marker, default_value...] } × count
  ```
  - Flags: bit 0 = NOT_NULL, bit 1 = PK
  - Default values serialized with type discriminator byte
    (0=null, 1=i64, 2=f64, 3=string, 4=blob)
- `get_table` returns a deep copy (independent of page buffers).
- `add_table` / `drop_table` / `list_tables` / `find_table` interface.

### `types` — Core Type System

File: `src/types/types.odin`

```odin
Value  :: union { i64, f64, string, []u8, Null }
Row_ID :: distinct i64

Column_Type :: enum { INTEGER, TEXT, REAL, BLOB }

Column :: struct {
    name: string,
    type: Column_Type,
    not_null, pk: bool,
    default_value: Maybe(Value),
}

Table :: struct {
    name: string,
    columns: []Column,
    root_page: u32,
    sql: string,
}

Serial_Type :: enum u64 { NULL=0, INT8=1, ..., INT64=6, FLOAT64=7,
                          ZERO=8, ONE=9 }
```

Constants: `PAGE_SIZE = 4096`, `DATABASE_HEADER_SIZE = 100`,
`SCHEMA_PAGE_ID = 1`, `MAX_COLUMNS = 10`.

### `utils` — Encoding Helpers

File: `src/utils/utils.odin`

- `put_varint` / `get_varint` — SQLite-compatible varint (up to 9 bytes).
- `i64_to_be` / `be_to_i64` — big-endian byte conversion for interior page
  separator keys.
- `map_type` — maps `Column_Type` to `Serial_Type`.

### `db` — Database Handle

File: `src/db/db.odin`

```odin
Database :: struct {
    pager: ^pager.Pager,
    schema_tree: ^btree.Tree,
    mu: sync.Mutex,
}
```

- `open(path)` — create/open file, init pager, load/init schema tree.
- `close(db)` — flush, destroy, close file.
- `execute(db, sql)` — lock, parse, execute, unlock.
- `execute_stmt(db, stmt)` — direct statement execution (used by executor
  internally for subqueries).

---

## CLI & REPL

The main entry point (`main.odin`) uses `core:flags` for argument parsing:

| Argument / Flag | Description |
|----------------|-------------|
| `[database]`   | Database file path (positional, default `test.db`) |
| `--eval`       | Execute SQL string and exit |
| `--file`       | Execute SQL from file and exit |

**Modes** (checked in order):

1. **File mode** (`--file <path>`): reads entire file, executes
   statement by statement (semicolon-separated), prints results, exits.
2. **Eval mode** (`--eval "<sql>"`): executes a single statement, exits.
3. **Pipe mode** (stdin is not a TTY): reads all stdin, splits on `;`,
   executes, exits. Enables `echo "SELECT 1;" | magni` usage.
4. **REPL mode** (stdin is a TTY): interactive prompt.

**REPL dot-commands**:

| Command | Action |
|---------|--------|
| `.exit`, `.quit` | Exit |
| `.help`          | List commands |
| `.tables`        | List tables |
| `.schema`        | Show all CREATE TABLE statements |
| `.schema <tbl>`  | Show a specific table's DDL |
| `.debug_schema`  | Dump raw schema tree cells |
| `.dump <tbl>`    | Dump all rows as INSERT statements |
| `.desc <tbl>`    | Describe table columns |
| `.stats`         | Show count of each dot-command usage |
| `.integrity`     | Verify all B-trees (schema + data) |
| `.checkpoint`    | Force flush dirty pages to disk |

---

## Thread Safety

- `Database.mu` (`sync.Mutex`) guards all public `db.*` operations.
- The pager has its own `sync.Mutex` for internal page cache access.
- This design is safe for sequential or single-client use but does not
  support concurrent reads.

---

## Build System

### Makefile targets

| Target | Description |
|--------|-------------|
| `build`       | Debug build (`-debug -o:none -warnings-as-errors`) |
| `release`     | Optimized build (`-o:aggressive -no-bounds-check -microarch:native`) |
| `run`         | Build + execute |
| `test`        | Run all unit tests |
| `test-cli`    | Shell-based CLI integration tests |
| `vet`         | Comprehensive vet (fast check, no LLVM) |
| `vet-all`     | Vet via build + test (full LLVM) |
| `vet-shadowing` | Shadowing check |
| `vet-unused`  | Unused declarations check |
| `vet-style`   | Style + semicolon checks |
| `vet-cast`    | Redundant casts check |
| `check`       | Parse + type check (fast) |
| `clean`       | Remove `build/` directory |

All source files use `-collection:src=src`, mapping import prefix `src:` to
the `src/` directory.

---

## Test Infrastructure

- **Framework**: Odin's built-in `core:testing`.
- **160+ tests** across 6 files in package `tests`.
- Test files create temporary `.db` files via setup/teardown helpers,
  initialize pager + schema tree, run operations, verify results, and
  clean up on completion.
- Categories:
  - **Parser**: tokenization, literal types, all SQL statement forms,
    JOIN syntax, subqueries, aliases, error handling.
  - **Executor**: full end-to-end execution for all DML/DDL, WHERE
    filtering (all operators), ORDER BY, LIMIT/OFFSET, GROUP BY + HAVING,
    aggregates, JOINs (CROSS/INNER/LEFT/self/multi/aliased), subqueries,
    LIKE wildcards, type validation, constraints, defaults, stress test
    (70 rows causing B-tree splits).
  - **B-tree**: insert/find/delete, cursor iteration (ordered output),
    persistence across close/reopen, duplicate detection, split stress
    (200 items), auto-increment, tree verification.
  - **Cell**: serialization roundtrip for all types, zero-copy semantics,
    buffer boundaries, schema validation, `calculate_size` consistency.
  - **Pager**: open/close, allocate/persistence, caching behavior,
    pinning/eviction, edge cases.
  - **Schema**: column blob roundtrip, add/find/drop/list, deep copy
    independence, duplicate rejection.
- Running: `make test` or `odin test tests/ -collection:src=src`.

---

## Key Design Decisions & Limitations

### Decisions

1. **SQLite-compatible record format**: The varint-based serial type system
   matches SQLite's storage format, enabling potential interop and leveraging
   a well-tested design.

2. **Simple B+tree without rebalancing**: Deletes update `fragmented_bytes`
   but do not rebalance or merge underfull nodes. This keeps the code simple
   at the cost of potential space waste over time.

3. **Recursive-descent parser**: Hand-written parser with one function per
   grammar rule. Explicit and debuggable, but requires manual error recovery.

4. **Uniform WHERE connectives**: `AND` and `OR` cannot be mixed in a single
   WHERE clause. This simplifies the condition evaluation model.

5. **PK fast-paths**: WHERE clauses matching `pk = literal` bypass full table
   scans for SELECT, UPDATE, and DELETE, using O(log n) B-tree lookup.

6. **Two-phase mutation**: UPDATE and DELETE first collect all matching row
   IDs into a buffer, then apply modifications. This avoids cursor
   invalidation during mutation but uses extra memory.

7. **Threading model**: Single mutex on the database handle. Sufficient for
   embedded / single-client use, not designed for concurrent OLTP.

8. **Zero-copy cells**: When enabled, string/BLOB values reference page
   buffer memory directly, avoiding allocation overhead. Requires care to
   prevent dangling pointers after page eviction.

### Limitations

- **No `NULLS FIRST` / `NULLS LAST`**: ORDER BY places NULLs according to
  default ordering (min or max depending on direction).
- **No `DISTINCT`**: Not yet implemented.
- **No `UNION` / `INTERSECT` / `EXCEPT`**: Set operations are absent.
- **No `CHECK` constraints**: Only PK, NOT NULL, and type validation.
- **No `FOREIGN KEY` enforcement**: Declared in DDL but not enforced.
- **No indexes**: Only the implicit primary-key B-tree exists.
- **No transactions**: Every statement is auto-committed.
- **No WAL / rollback journal**: Single-file, no crash recovery.
- **No B-tree rebalancing**: Deletes fragment pages without merging.
- **No nested subqueries beyond FROM clause**: Subqueries in WHERE
  `IN (SELECT ...)` are not supported.
- **Simple WHERE only**: No parenthesized precedence groups or mixed
  AND/OR connectives.
