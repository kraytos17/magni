# Magni Architecture

Magni is an embedded SQL database engine built in [Odin](https://odin-lang.org/). It implements a
subset of SQL with a copy-on-write (COW) B+tree storage engine, SQLite-compatible row format, and an
append-only snapshot chain supporting time-travel queries and point-in-time restore.

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Principles](#architecture-principles)
- [Layer Architecture](#layer-architecture)
- [Storage Engine](#storage-engine)
- [Query Execution](#query-execution)
- [Snapshot System](#snapshot-system)
- [Transaction & Concurrency Model](#transaction--concurrency-model)
- [Memory Management](#memory-management)
- [Performance Characteristics](#performance-characteristics)
- [API Surfaces](#api-surfaces)
- [Build & Test](#build--test)
- [Trade-offs & Alternatives](#trade-offs--alternatives)
- [Limitations](#limitations)

---

## System Overview

```
┌────────────────────────────────────────────────────────────────────┐
│                          CLI / REPL                                │
│  main.odin — flag parsing, mode dispatch, dot-commands             │
└──────────────────────────┬─────────────────────────────────────────┘
                           │ SQL string
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                       SQL Layer                                    │
│  parser.odin — tokenize → recursive-descent parse → AST            │
│  executor.odin — plan, execute, evaluate WHERE, JOIN, aggregates   │
└──────────────────────────┬─────────────────────────────────────────┘
                           │ Statement
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                     Storage Engine                                 │
│  schema.odin — table metadata (schema B-tree)                      │
│  btree/     — B+tree with COW: insert/find/delete/update           │
│  cell/      — SQLite-compatible row serialization                  │
│  pager/     — slab page cache, freelist, file I/O                  │
│  snapshot/  — append-only snapshot chain, manifests, GC            │
│  db/        — coordinator: open/close/execute/snapshot mgmt        │
└────────────────────────────────────────────────────────────────────┘
```

**Data flow for a write query:**

1. `db.execute()` acquires `Database.mu` (exclusive lock)
2. `parser.parse()` tokenizes and builds AST on `temp_allocator`
3. `executor.execute()` dispatches to DML-specific handler:
   - Each mutation (INSERT/UPDATE/DELETE) uses COW B-tree operations
   - Schema tree root is updated via `tree_update_cow` (single traversal)
4. After execution, a snapshot is created capturing the new schema root
5. `free_all(context.temp_allocator)` reclaims all temporary memory
6. Lock is released

---

## Architecture Principles

1. **Copy-on-write (COW)** — No page is ever modified in-place. Every mutation creates new pages
   along the path from root to leaf. Old pages remain readable for time-travel.

2. **Append-only history** — Snapshot chain never mutates. `snapshot_restore` creates a new
   `.RESTORE` snapshot rather than rewriting history.

3. **Single-traversal mutations** — Delete + re-insert (the core pattern for UPDATE) is done in
   one root-to-leaf traversal, not two.

4. **Bulk memory management** — All per-statement allocations use `context.temp_allocator` and are
   freed in one shot via `free_all()`. No per-node freeing during parse or execute.

5. **Slab allocation for hot data** — Page cache uses a fixed-size inline slab rather than a
   heap-allocated map. Zero per-page heap allocations.

---

## Layer Architecture

### 1. SQL Layer — `parser/` and `executor/`

**Parser** (`parser.odin`):
- Lexer: character-by-character scanner producing `[]Token`. Token types ~70 values.
- Recursive-descent parser: one function per grammar rule (`parse_create_table`,
  `parse_insert`, `parse_select`, `parse_update`, `parse_delete`, `parse_drop_table`).
- `Select_Stmt` supports `AS OF SNAPSHOT <id>` and `AS OF TIMESTAMP <micros>`.
- `Token_Type` is `enum u8` (compact — 1 byte per token tag).
- All AST nodes allocated on caller-provided allocator; no per-node cleanup needed.

**Executor** (`executor.odin`):
- Entry: `execute(schema_tree, stmt) -> (ok, new_schema_root)`.
- DML dispatch (CREATE, INSERT, SELECT, UPDATE, DELETE, DROP).
- Key subroutines:

| Subroutine | Role | Performance note |
|---|---|---|
| `scan_table` | Full table scan via cursor | Moves cell values directly (no deep copy) |
| `try_pk_lookup` | Fast-path: `WHERE pk = literal` | O(log n) tree_find vs full scan |
| `evaluate_where` | Filter rows against WHERE clause | Supports qualified names (t.col) |
| `try_join_match` | Combine rows + ON evaluation | Uses temp_allocator only on match |
| `compute_aggregates` | COUNT/SUM/AVG/MIN/MAX | Aggregator per group |
| `display_results` | Pretty-printed output | LIMIT/OFFSET applied here |
| `exec_query` | Return row data as struct (for db.query) | Same path as exec_select, no display |
| `exec_select_single_data` | Single-table SELECT returning []Row_Entry | Used by db.query() for precise tests |

**GROUP BY** uses `map[string]int` keyed on serialized group-by values for O(rows) lookups
(was O(rows × groups) linear scan).

### 2. Storage Engine — `btree/`, `cell/`, `pager/`, `schema/`, `db/`

#### B+tree (`btree/tree.odin`, `headers.odin`, `cursor.odin`)

**On-disk page layout (4096 bytes):**

```
Page 1:
  [DB Header: 100B] [B-tree offset 100: Page_Header|Cell_Pointers|Cells...]

Page N (N > 1):
  [offset 0: Page_Header|Cell_Pointers|Cells...]
```

**Page header (8 bytes, `#packed`):**

```
┌──────────┬─────────────────┬────────────┬──────────────────────┬──────────────────┐
│ page_type│ first_freeblock │ cell_count │ cell_content_offset  │ fragmented_bytes │
│  (u8)    │    (u16le)      │  (u16le)   │      (u16le)         │      (u8)        │
└──────────┴─────────────────┴────────────┴──────────────────────┴──────────────────┘
```

Interior pages append `rightmost_ptr: u32be` after the standard header (12 bytes total).

**In-memory `Node` struct (24 bytes):**

```odin
Node :: struct {
    id:     u32,
    data:   []u8,
    header: ^Page_Header,     // computed once on load
}
```

`leaf`/`interior` sub-headers are computed on demand via `node_leaf()` / `node_interior()` —
no redundant pointer storage.

**Freeblock chain:**

Deleted cell space is tracked in a SQLite-compatible freeblock list for reuse:

```
Page_Header.first_freeblock → [next: u16le] [size: u16le] [unused...] → [next: u16le] [...] → 0
```

- `freeblock_insert` — adds a freed cell to the chain, sorted by offset, coalescing adjacent blocks.
- `freeblock_alloc` — first-fit search for a block ≥ requested size. Splits larger blocks; exact-fit
  removes from chain.
- Minimum freeblock size: 4 bytes (2 for next, 2 for size). Cells smaller than 4 bytes fall back to
  `fragmented_bytes`.
- `delete_from_leaf` creates freeblocks for middle-page deletions (was permanently wasted space).
- `node_insert_leaf_cell` checks the freeblock chain before allocating from the end of page.

**B-tree operations:**

| Operation | COW variant | Description | Traversals |
|---|---|---|---|
| `tree_insert` | `tree_insert_cow` | Insert cell, split when full. COW variant copies each page on the path before modifying. | 1 |
| `tree_find` | — | Binary search descending to leaf, then `leaf_lower_bound` (binary). | 1 |
| `tree_delete` | `tree_delete_cow` | Remove cell by rowid via binary search. COW variant COWs the full path (not just root). | 1 |
| `tree_update` | `tree_update_cow` | Delete + re-insert on same leaf, single traversal. | 1 |
| `tree_foreach` | — | Full iteration via cursor. | full scan |

**Cursor** — fixed-size path stack `[32]Cursor_Stack_Item`:

```odin
Cursor_Stack_Item :: struct {
    page_id:    u32,
    cell_index: u16,
}
```

#### Record Serialization (`cell/cell.odin`)

SQLite-compatible varint format:

```
[PayloadLength varint] [RowID varint] [HeaderSize varint] [SerialTypes...] [Payload...]
```

Serial types encode type + byte size in a single u64:

| Type | Encoding | Payload size |
|---|---|---|
| NULL | 0 | 0 |
| INT8–INT64 | 1–6 | 1–8 bytes |
| FLOAT64 | 7 | 8 bytes |
| ZERO / ONE | 8 / 9 | 0 |
| TEXT | 13 + 2*N | N bytes |
| BLOB | 12 + 2*N | N bytes |

`Serialization_Info` computes serial types inline from the `values` slice (no heap alloc —
was `make([]u64)` per call). `cell.deserialize` decodes into a fixed `[MAX_COLS]u64` array
(was `[dynamic]u64` — 0 heap allocs per row).

#### Page Cache (`pager/pager.odin`)

**Architecture:**

```
Pager:
  ├── file: os.File
  ├── slots: [256]Page_Slot    ← contiguous 1MB slab
  │   └── Page_Slot:
  │       ├── page: Page { page_num, dirty, pin_count, data: slice→ }
  │       └── _data_buf: [4096]u8    ← inline page buffer
  ├── first_free_page: u32          ← freelist head (on-disk linked list)
  ├── mutex: RW_Mutex
  └── ...
```

- **Zero per-page heap allocations**: All 256 page buffers are inline in the slab.
- **Lookup**: Open-addressing from `page_num % 256` with linear probe + full scan fallback.
- **Eviction**: Linear scan for first unpinned slot (simple clock-hand approximation).
- **Freelist**: Linked list stored in-page (first 4 bytes = next free page). `first_free_page`
  persisted in database header.
- **Concurrency**: `RW_Mutex` — read-only operations (`page_count`, `page_in_cache`)
  use shared locks; all mutations use exclusive locks.
- **Zero-on-alloc**: Only the first 100 bytes (header region) are zeroed. Callers overwrite
  as needed.

#### Table Metadata (`schema/schema.odin`)

Schema is stored as a B-tree on page 1:

| Column | Type | Description |
|---|---|---|
| RowID | i64 | `fnv64(table_name) & 0x7FFF...` |
| type | TEXT | `"table"` |
| name | TEXT | Table name |
| tbl_name | TEXT | Table name (SQLite compat) |
| root_page | INT | B-tree root page number |
| sql | TEXT | Original CREATE TABLE statement |
| columns_blob | BLOB | Serialized column definitions |

All mutations (`add_table_cow`, `drop_table_cow`, `update_root_page_cow`) use COW and
return a new schema root. `get_table` passes the caller's allocator directly through to
`find_table`.

#### Database Coordinator (`db/db.odin`)

```odin
Database :: struct {
    path:             string,
    pager:            ^pager.Pager,
    is_new:           bool,
    mu:               sync.Mutex,
    schema_root_page: u32,
    latest_snapshot:  u32,
    txn_state:        Transaction_State,
    txn_snapshot_id:  u64,
    snapshot_index:   map[u64]u32,     // O(1) snapshot lookup
}
```

Responsibility: coordinate the full lifecycle of a query.

```
execute(db, sql):
  lock(mu)
  stmt = parse(sql, temp_allocator)
  ok, new_root = executor.execute(schema_tree, stmt)
  db.schema_root_page = new_root
  if ok && !readonly:
    create_snapshot()
    run_prune()
    run_gc()
  free_all(temp_allocator)
  unlock(mu)
```

---

## Snapshot System

### Chain Structure

```
latest_snapshot
    │
    ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ snapshot 3  │───▶│ snapshot 2  │───▶│ snapshot 1  │───▶ 0 (genesis)
│ schema_root │    │ schema_root │    │ schema_root │
│ manifest_3  │    │ manifest_2  │    │ manifest_1  │
│ state=C     │    │ state=C     │    │ state=A     │
│ op=INSERT   │    │ op=CREATE   │    │ op=CREATE   │
│ timestamp=T3│    │ timestamp=T2│    │ timestamp=T1│
└─────────────┘    └─────────────┘    └─────────────┘
                                                ABANDONED (pruned)
```

### Snapshot Header (40 bytes on disk, `#packed`)

```
┌──────┬─────────────┬───────────────┬───────────┬─────────────┬───────────────┬───────┬─────────┬───────┐
│magic │ snapshot_id │ prev_snapshot │ timestamp │ schema_root │ manifest_page │ state │  op     │ pad   │
│ 8B   │    8B       │     4B        │   8B      │    4B       │     4B        │  1B   │  1B     │  2B   │
└──────┴─────────────┴───────────────┴───────────┴─────────────┴───────────────┴───────┴─────────┴───────┘
```

Tags (64 bytes) stored at offset 40 in unused page space — no format change.

### Manifest Page

Each manifest maps table names to their B-tree root pages at a snapshot point-in-time:

```
[MAGIC: 8B] [count: u32le] [entry × count]
entry = [name_hash: u64, root_page: u32, name_len: u16, name_bytes: name_len]
```

### Operations

| Operation | Complexity | Description |
|---|---|---|
| `create` | O(pages) | Allocate page, write header |
| `find_by_id` | O(1) | Map lookup in `Database.snapshot_index` |
| `find_by_timestamp` | O(n) chain | Walk chain for newest `timestamp ≤ target` |
| `diff_manifests` | O(n*m) | Compare two manifests (n, m = table count) |
| `prune` | O(chain) | Mark old snapshots as ABANDONED |
| `gc` | O(pages_in_chain) | Mark-and-sweep: free unreachable pages |
| `set_tag` / `get_tag` | O(1) | Read/write 64 bytes at page offset 40 |

### GC Algorithm

```
gc(pager, latest_page, keep_count):
  live = {page_1}
  walk chain backward from latest_page for keep_count:
    live += snapshot_page, manifest_page, schema_root
    for each table root in manifest:
      live += root
      btree.collect_pages(root) → live += all sub-pages
  for every page from 2..max_page:
    if page not in live:
      pager.free_page(page)
```

---

## Transaction & Concurrency Model

### Lock Hierarchy

```
Database.mu (sync.Mutex)           ← serializes all db.* operations
    │
    └── Pager.mutex (sync.RW_Mutex)  ← per-operation page cache access
        ├── Read shared:  page_count, page_in_cache
        └── Write exclusive: get_page, allocate_page, unpin_page,
                             flush_all, mark_dirty, free_page
```

Single-writer principle: `Database.mu` ensures at most one statement executes at a time.
The pager's `RW_Mutex` allows concurrent read-only cache probes but serializes modifications.

### Snapshot Isolation

Every mutation outside a transaction creates an implicit snapshot. Time-travel queries
(`AS OF SNAPSHOT` / `AS OF TIMESTAMP`) read against the historical schema root. COW
guarantees that old B-tree pages remain intact and readable.

```
INSERT INTO t VALUES (1);   → snapshot 1 created (root = 100)
INSERT INTO t VALUES (2);   → snapshot 2 created (root = 105, COW of root)

SELECT * FROM t AS OF SNAPSHOT 1;  → reads schema_root=100 → old data
```

---

## Memory Management

### Allocator Strategy

| Allocator | Used by | Lifecycle |
|---|---|---|
| `context.allocator` | Persistent: database handle, pager slab, snapshot index | Until `db.close()` |
| `context.temp_allocator` | Per-statement: AST, tokens, intermediate rows, cursor results | `free_all()` at end of `db.execute()` |
| `context.temp_allocator` (REPL) | Per-iteration: REPL output formatting | `free_all()` at top of REPL loop |

### Heap Allocation Profile

| Allocation | Count per INSERT | Previous count | Change |
|---|---|---|---|
| Page struct + data buffer | 0 (inline slab) | 2 (heap Page + heap []u8) | -100% |
| Cell (per row scanned) | 0 (if moving values) | 1 (deep_copy_values) | -100% |
| `Serialization_Info.serial_types` | 0 (inline compute) | 1 (`make([]u64)`) | -100% |
| Cell.allocator | 0 (removed) | 1 (16 bytes) | -100% |
| Cursor path | 0 (`[32]` stack) | 1 (`make([dynamic]`) | -100% |
| AST nodes | many (temp_allocator) | many (temp_allocator) | Same (bulk-freed) |

---

## Performance Characteristics

### B-tree

| Metric | Leaf page | Interior page |
|---|---|---|
| Max cells (INT8 key + 4B pointer) | ~400 | ~650 |
| Avg cells (real-world) | ~100 | ~200 |
| Tree depth (1M rows, 100 cells/page) | 3 | 3 |
| Search complexity | O(log₁₀₀ n) | O(log₂₀₀ n) |
| Insert: pages COW'd | depth + 1 | depth + 1 |
| Delete: pages COW'd (COW variant) | depth | depth |

### Page Cache

| Metric | Value |
|---|---|
| Slots | 256 |
| Slot size | 4120 bytes (24 Page + 4096 data) |
| Total memory | ~1 MB |
| Lookup (hit, avg probes) | ~1.5 (hash from `page_num % 256`) |
| Lookup (miss, full scan) | 256 probes |
| Eviction cost | 1 `os.write_at` + 1 `os.read_at` |

### Snapshot

| Operation | Complexity | Note |
|---|---|---|
| `find_by_id` | O(1) | In-memory map |
| `find_by_timestamp` | O(keep_count) | Chain walk, typically ≤100 |
| `create` | O(tables) | Manifest serialization |
| `diff_manifests` | O(tables²) | Nested loop comparison |

---

## API Surfaces

### Public `db` API

```odin
open(path: string)                  -> ^Database, bool
close(db: ^Database)
execute(db: ^Database, sql: string)  -> bool
query(db: ^Database, sql: string)    -> Query_Result  // returns structured row data

Query_Result :: struct {
    columns:  []string,
    col_types: []types.Column_Type,
    rows:     [][]types.Value,   // nil for DML
    ok:       bool,
}

checkpoint(db: ^Database)           -> bool
begin(db: ^Database)                -> bool
commit(db: ^Database)               -> bool
rollback(db: ^Database)             -> bool
snapshot_restore(db, id)            -> bool
snapshot_tag(db, id, label)         -> bool
print_snapshots(db)
snapshot_diff(db, older, newer)     -> bool
integrity_check(db)                 -> bool
```

### Public `btree` API

```odin
init(pager, root, config) -> Tree
tree_insert(t, rowid, values)                             -> Error
tree_insert_cow(t, rowid, values)                         -> (u32, Error)
tree_find(t, rowid, allocator)                            -> (Cell, Error)
tree_delete(t, rowid)                                     -> Error
tree_delete_cow(t, rowid)                                 -> (u32, Error)
tree_update(t, rowid, values)                             -> Error
tree_update_cow(t, rowid, values)                         -> (u32, Error)
tree_foreach(t, callback, user_data)                      -> Error
tree_next_rowid(t)                                        -> (Row_ID, Error)
cursor_start(t, allocator)                                -> (Cursor, Error)
cursor_advance(c)                                         -> Error
cursor_get_cell(c, allocator)                             -> (Cell, Error)
collect_pages(t, root, &page_set)
```

### Public `snapshot` API

```odin
create(pager, id, prev, schema_root, manifest, op)     -> (u32, bool)
load(pager, page)                                        -> (Snapshot_Header, bool)
find_by_id(pager, start_page, id)                        -> (Snapshot_Header, bool)
find_by_timestamp(pager, start_page, ts)                 -> (Snapshot_Header, bool)
create_manifest(pager, tables)                           -> u32
find_in_manifest(pager, manifest_page, table_name)       -> (u32, bool)
diff_manifests(pager, a, b, allocator)                   -> ([]Diff_Entry, bool)
diff_snapshots(pager, older, newer, latest, allocator)   -> ([]Diff_Entry, bool)
prune(pager, start_page, max_keep)
gc(pager, latest_page, keep_count)
set_tag(pager, page, tag)
get_tag(pager, page)                                     -> string
list_snapshots(pager, latest, allocator)                 -> []Snapshot_Header
```

### CLI Dot-commands

| Command | Action | Implementation |
|---|---|---|
| `.exit` / `.quit` | Exit | `os.exit(0)` |
| `.tables` | List tables | `db.list_tables()` |
| `.schema` | Show DDL | `db.print_schema()` |
| `.dump <table>` | Dump rows | `db.dump_table()` |
| `.desc <table>` | Describe columns | `db.describe_table()` |
| `.stats` | DB statistics | `db.stats()` |
| `.integrity` | Verify B-trees | `db.integrity_check()` |
| `.checkpoint` | Flush + GC | `db.checkpoint()` |
| `.snapshots` | Show chain | `db.print_snapshots()` |
| `.snapdiff <a> <b>` | Diff snapshots | `db.snapshot_diff()` |
| `.snapshot tag <id> <label>` | Tag snapshot | `db.snapshot_tag()` |
| `.snapshot restore <id>` | Restore | `db.snapshot_restore()` |
| `.begin` / `.commit` / `.rollback` | Transaction | `db.begin/commit/rollback()` |

---

## Build & Test

### Makefile

| Target | Flags | Use case |
|---|---|---|
| `build` | `-debug -o:none -warnings-as-errors` | Development |
| `release` | `-o:aggressive -no-bounds-check -microarch:native` | Production |
| `test` | `odin test tests/ -collection:src=src` | Unit tests |
| `vet-all` | `odin build + odin test` with `-vet*` flags | Full CI check |
| `check` | Parse + type check only | Fast pre-commit |

### Test Coverage

| Package | Tests | Coverage |
|---|---|---|
| `parser` | 38 | Tokenization, all SQL forms, JOINs, subqueries, error handling |
| `executor` | 64 | Full DML/DDL, WHERE, ORDER BY, LIMIT, GROUP BY, JOINs, aggregates |
| `btree` | 20 | Insert/find/delete/update, cursor, split, persistence, duplicates |
| `cell` | 11 | Serialization roundtrip, zero-copy, edge cases |
| `pager` | 12 | Open/close, caching, pinning, eviction |
| `schema` | 10 | Column blob, add/find/drop/list |
| `snapshot` | 12 | Chain, manifest, diff, tags, timestamp lookup, GC |
| `integration` | 16 | CRUD, time-travel, JOINs, UPDATE, DELETE, restore, persistence |

---

## Trade-offs & Alternatives

### COW vs WAL

| Aspect | COW (chosen) | WAL (alternative) |
|---|---|---|
| Read concurrency | Single-threaded but historical reads | Concurrent readers + writer |
| Write amplification | Depth × 4KB per mutation | ~1 page per mutation |
| Snapshot isolation | Built-in (old pages persist) | Requires separate version store |
| Crash recovery | No recovery needed (pages intact) | Requires WAL replay |
| Implementation complexity | Moderate | High |

### Slab cache vs Map

| Aspect | Slab (chosen) | Map (previous) |
|---|---|---|
| Per-page heap alloc | 0 | 2 (Page struct + data buffer) |
| Cache locality | Contiguous 1MB | Fragmented across heap |
| Eviction | Linear scan, O(cache_size) | HashMap iteration, O(entries) |
| Resizing | Fixed at 256 slots | Configurable via `max_cache_pages` |

### Single traversal vs Delete+Insert

| Aspect | Single traversal (chosen) | Delete + Insert (previous) |
|---|---|---|
| Tree traversals per UPDATE | 2 (`tree_find` + `tree_update_cow`) | 3 (`tree_find` + `delete` + `insert`) |
| Branch mispredictions | ~depth × 2 | ~depth × 3 |
| Code complexity | Moderate (new recursive function) | Low (two existing functions) |

### Freeblock chain vs Fragmentation bucket

| Aspect | Freeblock chain (chosen) | `fragmented_bytes` (previous) |
|---|---|---|
| Space reuse from middle-page deletes | Full reuse via linked list | Capped at 255 bytes, then permanent waste |
| Insert from freeblock | First-fit search O(freeblocks) | Always from end of page |
| Code complexity | ~120 lines (helpers + integration) | ~10 lines |
| Minimum tracked cell size | 4 bytes (freeblock header) | 1 byte |

---

## Limitations

- **No `DISTINCT`**: Not yet implemented.
- **No `UNION` / `INTERSECT` / `EXCEPT`**: Set operations absent.
- **No `FOREIGN KEY` enforcement**: Declared in DDL but not enforced.
- **No indexes**: Only the implicit primary-key B-tree exists.
- **No WAL / crash recovery**: Single-file, `os.sync` only on flush.
- **No B-tree rebalancing**: Deletes fragment pages without merging.
- **No nested WHERE subqueries**: `IN (SELECT ...)` not supported.
- **Mixed AND/OR WHERE**: Not supported.
