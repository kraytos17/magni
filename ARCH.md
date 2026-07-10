# Magni Architecture

Magni is an embedded SQL database engine built in [Odin](https://odin-lang.org/). It implements a
subset of SQL with a copy-on-write (COW) B+tree storage engine, SQLite-compatible row format, and an
append-only snapshot chain supporting time-travel queries and point-in-time restore.

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Principles](#architecture-principles)
- [Layer Architecture](#layer-architecture)
  - [SQL Layer](#1-sql-layer--parser-and-executor)
  - [Line Editor](#2-line-editor--linedit)
  - [Storage Engine](#3-storage-engine)
- [Snapshot System](#snapshot-system)
- [Transaction & Concurrency Model](#transaction--concurrency-model)
- [Memory Management](#memory-management)
- [Performance Characteristics](#performance-characteristics)
- [Trade-offs & Alternatives](#trade-offs--alternatives)
- [Limitations](#limitations)
- [Appendix: API Surfaces](#appendix-api-surfaces)

---

## System Overview

```
┌────────────────────────────────────────────────────────────────────┐
│                          CLI / REPL                                │
│  main.odin — flag parsing, mode dispatch, dot-commands             │
│  linedit/  — raw-mode terminal editor: history, Ctrl-R, Tab,       │
│              undo, Ctrl-T, Ctrl-L, bracketed paste, SIGWINCH       │
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
│  pager/     — slab page cache, freelist, file I/O, WAL             │
│  snapshot/  — append-only snapshot chain, manifests, GC            │
│  db/        — coordinator: open/close/execute/snapshot mgmt        │
└────────────────────────────────────────────────────────────────────┘
```

**Data flow for a write query:**

1. `parser.parse()` tokenizes and builds AST (no lock needed)
2. `sync.rw_mutex_lock(&db.mu)` — exclusive lock for writes
3. `executor.execute()` dispatches to DML-specific handler:
   - Each mutation (INSERT/UPDATE/DELETE) uses COW B-tree operations
   - Schema tree root is updated via `tree_update_cow` (single traversal)
4. After execution, a snapshot is created capturing the new schema root
5. `free_all(context.temp_allocator)` reclaims all temporary memory
6. Lock is released

**Data flow for a read query:**

1. `parser.parse()` tokenizes and builds AST (no lock needed)
2. `sync.rw_mutex_shared_lock(&db.mu)` — shared lock for reads
3. `executor.execute()` dispatches to SELECT handler
4. Read-only B-tree traversal via cursor or `tree_find` — no pages modified
5. Results are displayed or returned via `Query_Result`
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

6. **WAL durability** — All writes go to a sequential `-wal` file. Commits append a commit frame +
   `fsync` (not the full cache). Crash recovery replays committed frames on open. Checkpoint
   writes WAL frames back to the main file. `wal_abort_txn` discards uncommitted writes without
   touching the main file — no page leak on rollback.

7. **Columnar page encoding** — Pages can be stored in column-major format with delta encoding for
   integer compression. Read-only scans benefit from contiguous column data. Any mutation
   (insert/update/delete) or split triggers `ensure_row_major()` conversion back to row format.

8. **Page format versioning** — A format registry (up to 64 versions) decouples page layout from
   code. v1 uses SQLite-compatible 2-byte cell pointers; v2 uses 10-byte cell entries with
   embedded 8-byte key, eliminating key re-decoding. Existing files remain readable regardless
   of version.

9. **Auto-built skip indexes** — When a table scan encounters `WHERE col = <int>` without an
   existing skip index, one is automatically built mapping integer value ranges to page ranges.
   Subsequent queries skip irrelevant pages without scanning.

10. **B-tree rebalancing** — Adjacent leaf pages with combined occupancy below 70% are
    automatically merged during operations, maintaining dense packing and reducing tree depth.

11. **Row count tracking** — Per-page row counts are maintained incrementally on insert/delete
    and cached in the pager. `COUNT(*)` without WHERE/GROUP BY/DISTINCT/ORDER BY/LIMIT is
    served directly from the cache without scanning.

---

## Layer Architecture

### 1. SQL Layer — `parser/` and `executor/`

**Parser** (`parser.odin`):
- Lexer: character-by-character scanner producing `[]Token` (~78 token types as `enum u8`).
- Recursive-descent parser: one function per grammar rule (`parse_create_table`,
  `parse_insert`, `parse_select`, `parse_update`, `parse_delete`, `parse_drop_table`).
- `Select_Stmt` supports `AS OF SNAPSHOT <id>` and `AS OF TIMESTAMP <micros>`.
- All AST nodes allocated on caller-provided allocator; no per-node cleanup needed.
- `LIMIT` without `ORDER BY` uses pushdown: `scan_table` stops early when `max_rows` is reached.

**Executor** (`executor.odin`):
- Entry: `execute(schema_tree, stmt) -> (ok, new_schema_root)`.
- DML dispatch (CREATE, INSERT, SELECT, UPDATE, DELETE, DROP).
- Stream COW: UPDATE/DELETE apply mutations directly in the scan loop instead of
  batch-collecting all ops first — O(1) peak memory per operation regardless of row count.

Key subroutines:

| Subroutine | Role | Performance note |
|---|---|---|
| `scan_table` | Full table scan via cursor | Moves cell values directly (no deep copy) |
| `try_pk_lookup` | Fast-path: `WHERE pk = literal` | O(log n) tree_find vs full scan |
| `evaluate_where_ctx` | Filter rows (pre-resolved column indices) | Index resolution done once, not per row |
| `try_join_match` | Combine rows + ON evaluation | Uses temp_allocator only on match |
| `dedup_rows` | DISTINCT via hash-set (FNV fingerprint) | O(n), non-adjacent duplicates handled |
| `sort_rows` | ORDER BY with integer fast path | Single-column int: `[]i64` + index sort |
| `compute_aggregates` | COUNT/SUM/AVG/MIN/MAX | `@(fast_math)` on f64 reduction for auto-vectorization |
| `check_constraints` | CHECK enforcement on INSERT/UPDATE | Fail-closed: rejects non-integer, unknown col |

**GROUP BY** uses direct FNV-1a hashing of `Value` union data (raw bit pattern for `f64`,
`u64` for `i64`, FNV of bytes for strings/blobs) keyed on `map[u64]int` with a collision
fallback equality check — no stringification, no allocation per row, and no float-precision
loss. Groups are printed using the original `key_values` `[]types.Value` stored in each `Group` struct.

**HAVING** evaluates against both group-key values and computed aggregate values. Supports
aggregate function references (e.g., `HAVING count > 1`) as well as group-by column comparisons.

### 2. Line Editor — `linedit/`

The REPL uses a hand-rolled raw-mode terminal editor built directly on `core:sys/posix` termios
with no third-party dependencies. On non-TTY input (piped stdin, script mode) it falls back to
a `bufio.Reader` loop.

```
linedit/
├── linedit.odin    Public API: init/destroy/read_line, Ctrl-R, Ctrl-T, Tab
├── term.odin       Raw mode (termios), TIOCGWINSZ, SIGWINCH handler
├── keys.odin       Byte decoder, escape sequences, UTF-8 decode
├── buffer.odin     Line buffer ([dynamic]rune), cursor, undo stack (100 levels)
├── render.odin     Wrap-aware redraw, CJK rune widths, search overlay
└── history.odin    In-memory history, disk persistence, substring search
```

**`read_line` flow:**

1. `term_enable_raw` disables canonical mode, echo, signal chars, IXON
2. On each keystroke, `read_key` reads a raw byte and decodes it:
   - Printable ASCII/UTF-8 → `.Char` with decoded rune
   - Control chars (^A, ^C, ^D, ^E, ^K, ^L, ^R, ^T, ^U, ^W, ^Z) → `.Ctrl_*`
   - `ESC [ ...` → escape sequence detection via `poll()` with 80ms timeout
   - `ESC [ 200 ~` / `ESC [ 201 ~` → `.Paste_Start` / `.Paste_End`
3. `Line_Buffer` stores the editable line as `[dynamic]rune` with cursor index.
   Every mutating operation pushes the prior state onto an `undo_stack` (max 100).
4. `redraw` outputs `\r` + `ESC[0K` + prompt + line, then moves cursor back
   to the edit position. It handles terminal wrapping by inserting `\r\n` at
   column boundaries, re-queries `TIOCGWINSZ` on `SIGWINCH`, and accounts for
   CJK/emoji double-width characters via `rune_width`.
5. On `.Enter`, the line is returned. Multi-line statements accumulate lines
   in `query_buffer` in `main.odin`; Ctrl-C at any point resets the buffer.
6. History is persisted to `~/.magnidb_history` (capped at 1000 entries,
   consecutive duplicate suppressed). `history_search_prev` provides substring
   backward search for Ctrl-R. During search, matches are displayed on a dedicated line
   below the search prompt; when no more matches are found above, the search wraps around
   from the newest entry and shows a `(wrapped ...)` prompt prefix.
7. Tab completion matches against a static list of 21 dot-commands, SQL keywords,
   and (via a callback to the database) table names and column names. On
   unambiguous match the remainder is inserted; on ambiguity the candidates
   are printed below and the prompt is redrawn underneath.

**Keybindings:**

| Key | Action |
|---|---|
| ← → | Move cursor (UTF-8/CJK-aware) |
| ↑ ↓ | History navigation with in-progress line save/restore |
| Home, Ctrl-A | Beginning of line |
| End, Ctrl-E | End of line |
| Backspace, Delete | Delete backward/forward |
| Ctrl-K | Kill to end |
| Ctrl-U | Kill to start |
| Ctrl-W | Delete word backward |
| Ctrl-Z | Undo (multi-level, 100-deep stack) |
| Ctrl-L | Clear screen (redraws prompt) |
| Ctrl-T | Transpose characters |
| Ctrl-C | Return empty line (aborts multi-line statement in main.odin) |
| Ctrl-D (empty) | EOF (exit REPL) |
| Ctrl-R | Incremental reverse history search — results below prompt, wraps with `(wrapped ...)` |
| Tab | Dot-command, SQL keyword, and table/column name completion |
| Paste (bracketed) | Multi-line pastes inserted as single block |

**Platform support:**
- Linux: full raw-mode via `core:sys/posix` termios + `ioctl(TIOCGWINSZ)` for terminal dimensions
- macOS: same `core:sys/posix` path (different `TIOCGWINSZ` constant `0x40087468`)
- Non-POSIX (Windows): `linedit.init` returns `false`; `main.odin` falls back to `bufio.Reader`

### 3. Storage Engine

#### 3a. B+tree (`btree/`)

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

**In-memory `Node` struct:**

```odin
Node :: struct {
    id:     u32,
    data:   []u8,
    header: ^Page_Header,     // computed once on load
    layout: ^Cell_Layout,     // resolved once per page from format registry
}
```

`leaf`/`interior` sub-headers are computed on demand via `node_leaf()` / `node_interior()` —
no redundant pointer storage.

**Freeblock chain:**
Deleted cell space is tracked in a SQLite-compatible freeblock list:
```
Page_Header.first_freeblock → [next: u16le] [size: u16le] [...] → 0
```
- `freeblock_insert` — adds a freed cell to the chain, sorted by offset, coalescing adjacent blocks.
- `freeblock_alloc` — first-fit search for a block ≥ requested size. Splits larger blocks; exact-fit removes from chain.
- Minimum freeblock size: 4 bytes (2 for next, 2 for size). Cells smaller than 4 bytes fall back to `fragmented_bytes`.
- `delete_from_leaf` creates freeblocks for middle-page deletions.
- `node_insert_leaf_cell` checks the freeblock chain before allocating from the end of page.

**B-tree operations:**

| Operation | COW variant | Description | Traversals |
|---|---|---|---|---|
| `tree_insert` | `tree_insert_cow` | Insert cell, split when full. COW copies each page on path before modifying. | 1 |
| `tree_find` | — | Binary search descending to leaf, then `leaf_lower_bound`. | 1 |
| `tree_delete` | `tree_delete_cow` | Remove cell by rowid via binary search. COW variant COWs the full path. | 1 |
| `tree_update` | `tree_update_cow` | Delete + re-insert on same leaf, single traversal. | 1 |
| `tree_foreach` | — | Full iteration via cursor. | full scan |
| `rebalance` | — | Merge adjacent sparse leaves (<70% combined occupancy). Called after mutations. | 1 pass per level |

**Cursor** — fixed-size path stack `[MAX_TREE_DEPTH]Cursor_Stack_Item` (12 entries, ~96 bytes).
`MAX_TREE_DEPTH :: 12` is the single source of truth for both the cursor stack size and
the recursive operation depth guard.

#### 3b. Record Serialization (`cell/`)

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

`Serialization_Info` computes serial types inline from the `values` slice (no heap alloc).
`cell.deserialize` pre‑allocates the result slice once `serial_count` is known and writes
decoded values directly via index (no scratch-buffer `append` + trailing `copy`).

#### 3c. Page Cache (`pager/`)

```
Pager:
  ├── file: os.File
  ├── slots: [256]Page_Slot    ← contiguous 1MB slab
  │   └── Page_Slot:
  │       ├── page: Page { page_num, dirty, pin_count, data: slice→ }
  │       └── _data_buf: [4096]u8    ← inline page buffer
  ├── first_free_page: u32          ← freelist head
  ├── mutex: RW_Mutex
  └── ...
```

- **Zero per-page heap allocations**: All 256 page buffers are inline in the slab.
- **Lookup**: `map[u32]^Page_Slot` — O(1) average via Odin's Robin Hood map.
- **Eviction**: Rotating-hand scan for first unpinned slot.
- **Free-list**: `free_slots: [dynamic]^Page_Slot` provides O(1) slot allocation.
- **Freelist**: Linked list stored in-page. `first_free_page` persisted in database header.
- **Concurrency**: `RW_Mutex` — reads use shared locks; writes use exclusive locks.
- **WAL**: All writes go to a `-wal` sidecar file. Each frame carries a 64-bit FNV checksum
  (split across `checksum1`/`checksum2` in `WAL_Frame_Header`). `wal_recover` verifies
  checksums — a mismatched frame stops collection at that point; earlier frames are still
  replayed. `wal_abort_txn` discards uncommitted frames. `wal_checkpoint` writes WAL frames
  back to the main file and truncates the WAL. Checksums computed incrementally (no temp buffer).
- **Page bitmap**: `[]u64` tracks ever-allocated pages. GC sweep skips zero 64-bit words
  (all 64 pages free) in O(1).

#### 3d. Table Metadata (`schema/`)

Schema is stored as a B-tree on page 1.

| Index | Type | Content |
|---|---|---|
| RowID | i64 | `fnv64(table_name) & 0x7FFF...` |
| [0] | i64 | Kind discriminator (`0` = table) |
| [1] | TEXT | Table name |
| [2] | INT | B-tree root page number |
| [3] | TEXT | Original CREATE TABLE statement |
| [4] | BLOB | Serialized column definitions |
| [5] | INT | Skip-index root page (present only when > 0) |

Column blob format:
```
[0xFE:marker][version:1][count:varint]
  per column: [name_len:varint][name_bytes][packed:1][default_value?][check_len:varint?][check_bytes?]
```

Packed byte bits: 0-2 = type, 3 = not_null, 4 = pk, 5 = has_check, 6 = has_default
```

All mutations use COW and return a new schema root. Both `add_table` and `add_table_cow`
call `tree_find` at the candidate hash key before inserting — if a row exists with a
different name, a hash collision is reported and the insert is rejected.

#### 3e. Database Coordinator (`db/`)

```odin
Database :: struct {
    pager:                    ^pager.Pager,
    path:                     string,
    is_new:                   bool,
    schema_root_page:         u32,
    latest_snapshot:          u32,
    txn_snapshot_id:          u64,
    txn_state:                enum { NONE, ACTIVE },
    txn_start_file_len:       u64,
    snapshot_index:           map[u64]u32,
    refs_page:                u32,
    snapshot_batch_count:     int,
    snapshot_batch_threshold: int,
    mu:                       sync.RW_Mutex,
}
```

`snapshot_batch_count` and `snapshot_batch_threshold` control batch snapshot creation —
snapshots are only created when `count >= threshold`, reducing write amplification
for bulk operations.

**`Open_Config`** provides optional configuration at open time: `wal_size_threshold`
(auto-checkpoint when WAL exceeds a page count) and `snapshot_batch_threshold`
(overrides the default batch threshold).

```
execute(db, sql):
  stmt = parse(sql, temp_allocator)    // no lock yet
  if is_select: lock_shared(mu)        // SELECT: shared lock
  else:         lock_exclusive(mu)      // writes: exclusive lock
  ok, new_root = executor.execute(schema_tree, stmt)
  db.schema_root_page = new_root
  if ok && !readonly && !as_of:
    wal_begin_txn()
    create_snapshot()
    set_ref("main" → snap_id)
    wal_commit_txn()          // single fsync of WAL, not full cache
  unlock(mu)
  ```
  
#### 3f. Page Format Versioning — `btree/format.odin`

A format registry supports up to 64 concurrent page layout versions:

| Version | Cell pointer | Key decoding | Compat |
|---------|-------------|--------------|--------|
| v1 (legacy) | 2-byte `Cell_Pointer` (SQLite-compatible `u16le` offset) | Key decoded from cell body | Existing databases |
| v2 (current) | 10-byte `Cell_Entry` with embedded 8-byte key | Key read from entry directly — no body decode | New databases, auto-convert on write |

On write, pages are always written in the database's current format version. Old-format
pages are converted on first mutation. The version is stored in the database header and
set at database creation time.

#### 3g. Columnar Page Format — `cell/columnar.odin`

Pages can be stored in column-major encoding (`LEAF_TABLE_COLUMNAR` page type = 14):

```
Row-major:                 Columnar:
row 0: [a0, b0, c0]        col A: [a0, a1, a2, ...]
row 1: [a1, b1, c1]  →     col B: [b0, b1, b2, ...]
row 2: [a2, b2, c2]        col C: [c0, c1, c2, ...]
```

- Integers use delta encoding (store difference from previous value) for compression.
- Columnar pages are **read-only** — any mutation (insert, update, delete) or page split
  triggers `ensure_row_major()`, converting the page back to row format.
- The cursor (`cursor.odin`) reads columnar pages transparently, assembling rows on demand.
- Column count is detected via `detect_columnar_col_count()` from the page header.
- Benefits: better compression for integer-heavy data, cache-friendly column scans.

#### 3h. Skip Index — `btree/skip_index.odin`

Auto-built integer column index that accelerates `WHERE int_col = <value>` queries:

- Built on demand during `scan_table` when a WHERE clause matches `col = <integer>` and
  no skip index exists for that column yet.
- Maps integer value ranges to page ranges: `Skip_Entry{page_min, page_max, min_int, max_int}`.
- During subsequent queries, `build_skip_index` narrows the scan to pages whose range
  could contain the target value, skipping irrelevant pages.
- Stored as a sorted list in the schema B-tree root row.
- Complementary to `pager.page_int_ranges` which tracks known integer ranges per page
  and is invalidated on page mutations.
  
### Error Handling: `or_return` Pattern

The codebase uses Odin's `or_return` operator pervasively for error propagation.
Two patterns are used:

1. **Single-return (`-> Error`)**: Internal calls use `foo() or_return` — no named
   returns needed. The error propagates directly.

2. **Multi-return (`-> (T, Error)`)**: The first return is captured with a named
   error return so `or_return` can compose:

```odin
tree_next_rowid :: proc(t: ^Tree) -> (result: types.Row_ID, err: Error) {
    leaf := descend_to_leaf(t, descend_by_rightmost, nil) or_return
    if leaf.header.cell_count == 0 { result = 1; return }
    ...
    result = last_id + 1; return
}
```

Manual `if err != .None` is used where error type mismatches, cleanup actions,
or remapping prevents `or_return` composition (e.g., `pager.Error` → `DB_Error`).

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

Tags (64 bytes) stored at offset 40 in unused page space.

**Multi-header packing**: When multiple snapshot headers fit on one page (each header is 40 bytes,
up to ~100 per page), new snapshots are packed onto the existing latest snapshot page rather than
allocating a new page. This reduces page allocation overhead for frequent small transactions. The
chain diagram above is simplified — in practice a single page may contain several headers chained
via `prev_snapshot`.

### Manifest Page

Maps table names to their B-tree root pages at a snapshot point-in-time:
```
[MAGIC: 8B] [count: u32le] [entry × count]
entry = [name_hash: u64, root_page: u32, name_len: u16, name_bytes: name_len]
```

### Refs Page

Named refs are stored on a dedicated refs page. The `"main"` branch is the current snapshot
pointer. A rollforward log ring buffer (64 entries) tracks previous ref positions.

| Operation | Complexity | Description |
|---|---|---|
| `set_ref` | O(refs) | Add or update a named ref |
| `get_ref` | O(refs) | Look up a ref by name |
| `log_push` | O(1) | Record a ref move in the ring buffer |
| `log_pop` | O(1) | Pop the most recent log entry (for rollforward) |
| `expire_snapshots` | O(chain + pages) | Retain last N, mark older ABANDONED, GC sweep |
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
      if table has skip index: live += skip_index_root
                                btree.collect_pages(skip_index_root)
  sweep:
    if page_bitmap exists:
      for each 64-bit word in bitmap:
        if word == 0: continue       # all 64 pages free, skip
        for each set bit: check against live; free if not live
    else:
      for every page from 2..max_page:
        if page not in live: free
  truncate file_len to highest live page
```

After GC, the page bitmap is updated and `file_len` is truncated to the highest live
page, shrinking the scan range for subsequent GC passes.

---

## Transaction & Concurrency Model

### Lock Hierarchy

```
Database.mu (sync.RW_Mutex)         ← SELECT = shared; writes = exclusive
    ├── Read shared:  SELECT, query(), list_tables, describe_table,
    │                  stats, dump_table, print_schema, integrity_check,
    │                  print_snapshots, snapshot_diff
    └── Write exclusive: INSERT, UPDATE, DELETE, CREATE, DROP,
                          BEGIN/COMMIT/ROLLBACK, checkpoint, expire,
                          snapshot_restore, rollforward, close
    │
    └── Pager.mutex (sync.RW_Mutex)  ← per-operation page cache access
        ├── Read shared:  page_count, page_in_cache
        └── Write exclusive: get_page, allocate_page, unpin_page,
                             mark_dirty, free_page, copy_page
```

`Database.mu` is parsed **before** locking: `execute` parses the SQL first, determines if the statement is a read (`SELECT`) or write, then takes the appropriate lock. This allows multiple concurrent read operations while maintaining exclusive access for writes. The pager's `RW_Mutex` allows concurrent read-only cache probes but serializes modifications.

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
| `context.temp_allocator` | Per-statement: AST, tokens, intermediate rows, cursor results | End of caller's arena (REPL's `free_all` per iteration, `execute_sql` exit, or program exit) |

**Mandatory allocator on hot paths**: `tree_find` and `cursor_get_cell` require an explicit
allocator parameter (no default `context.allocator`). Callers pass `context.temp_allocator`
for per-query results. The allocator is used for deserialized string/blob values; cell data
on zero-copy paths points directly into page buffers. `cell.destroy` must use the same
allocator that was passed at creation time — mismatch causes bad-free on string/blob values.

### Heap Allocation Profile

| Allocation | Count per INSERT | Previous count | Change |
|---|---|---|---|
| Page struct + data buffer | 0 (inline slab) | 2 (heap Page + heap []u8) | -100% |
| Cell (per row scanned) | 0 (if moving values) | 1 (deep_copy_values) | -100% |
| Cell deserialize buffer | 0 (pre‑allocated result + direct index) | 1 (`make` + `copy`) | -100% |
| `Serialization_Info.serial_types` | 0 (inline compute) | 1 (`make([]u64)`) | -100% |
| Cell.allocator | 0 (removed) | 1 (16 bytes) | -100% |
| Cursor path | 0 (`[MAX_TREE_DEPTH]` stack) | 1 (`make([dynamic]`) | -100% |
| Pager slot lookup | 0 (free-list pop) | O(n) scan across 256 slots | -100% |
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
| Slot allocation | O(1) — pop from `free_slots` |
| Eviction | O(n) — rotating-hand scan, 256 slots max |
| Eviction cost | 1 `os.write_at` + 1 `os.read_at` |

### Snapshot

| Operation | Complexity | Note |
|---|---|---|
| `find_by_id` | O(1) | In-memory map |
| `find_by_timestamp` | O(keep_count) | Chain walk, typically ≤100 |
| `create` | O(tables) | Manifest serialization |
| `set_ref` / `get_ref` | O(refs) | Refs page scan |
| `log_push` / `log_pop` | O(1) | Ring buffer on refs page |

### Skip Index

| Metric | Value |
|---|---|
| Build cost | O(scanned_pages) — first scan that triggers it |
| Lookup | O(log entries) — binary search on sorted entries |
| Storage | Entries stored in schema B-tree root row |
| Invalidation | On any page mutation affecting the indexed column |

### B-tree Rebalancing

| Metric | Value |
|---|---|
| Merge threshold | ≤70% combined occupancy in adjacent leaves |
| Traversal | 1 pass per level |
| Impact | Reduces tree depth, improves cache density |

### Row Count Tracking (Fast COUNT(*))

| Metric | Value |
|---|---|
| Cache location | `pager.row_counts: map[u32]int` |
| Update cost | O(1) per insert/delete (incremental) |
| COUNT(*) fast path | O(1) if cached, O(pages) on first access |
| Bypass conditions | Queries with WHERE, GROUP BY, DISTINCT, ORDER BY, or LIMIT use full scan |

---

## Trade-offs & Alternatives

### COW + WAL

| Aspect | COW + WAL (chosen) | WAL-only |
|---|---|---|
| Read concurrency | Single-threaded but historical reads via COW | Concurrent readers + writer |
| Write amplification | Depth × 4KB per mutation + WAL append | ~1 page per mutation |
| Snapshot isolation | Built-in (old pages persist via COW) | Requires separate version store |
| Crash recovery | WAL replay on open | Requires WAL replay |
| Rollback | Instant — discard WAL frames | Instant — discard WAL frames |

### Slab cache vs Map

| Aspect | Slab (chosen) | Map (previous) |
|---|---|---|
| Per-page heap alloc | 0 | 2 (Page struct + data buffer) |
| Cache locality | Contiguous 1MB | Fragmented across heap |
| Slot allocation | O(1) free-list pop | O(cache_size) linear scan |
| Eviction | Rotating-hand scan | HashMap iteration |

### Single traversal vs Delete+Insert

| Aspect | Single traversal | Delete + Insert |
|---|---|---|
| Traversals per UPDATE | 2 (`tree_find` + `tree_update_cow`) | 3 (`tree_find` + `delete` + `insert`) |
| Branch mispredictions | ~depth × 2 | ~depth × 3 |
| Code complexity | Moderate | Low |

### Freeblock chain vs Fragmentation bucket

| Aspect | Freeblock chain | `fragmented_bytes` |
|---|---|---|
| Space reuse from middle deletes | Full reuse via linked list | Capped at 255 bytes, then permanent waste |
| Insert from freeblock | First-fit search O(freeblocks) | Always from end of page |
| Code complexity | ~120 lines | ~10 lines |
| Minimum tracked cell size | 4 bytes (freeblock header) | 1 byte |

---

## Limitations

- **No `UNION` / `INTERSECT` / `EXCEPT`**: Set operations absent.
- **No `FOREIGN KEY` enforcement on INSERT/UPDATE**: Validated at CREATE TABLE time only.
- **No user-managed indexes**: Only the implicit primary-key B-tree and auto-built skip indexes exist.
- **`CHECK` limited to integer comparisons**: `col > 0`, `col < 100`, `>=`, `<=`, `=`, `!=` format.
- **Mixed AND/OR WHERE**: Not supported.
- **Max 10 columns per table**: Enforced by `MAX_COLS` constant (inline `[dynamic; N]T` scratch buffer).
- **REPL line editor**: SQL keyword and table/column name completion only (no in-expression or JOIN completion).

---

### CLI Dot-commands

| Command | Action | Implementation |
|---|---|---|
| `.exit` / `.quit` | Exit | `handle_dot_command` returns `true` |
| `.help` | Show help | `print_help()` |
| `.version` | Print version | `APP_VERSION` |
| `.tables` | List tables | `db.list_tables()` |
| `.schema` | Show DDL | `db.print_schema()` |
| `.debug_schema` | Show verbose schema dump | `db.print_schema_debug()` |
| `.tree_page <n>` | Print B-tree page structure | `db.print_tree_page()` |
| `.dump <table>` | Dump rows | `db.dump_table()` |
| `.desc <table>` | Describe columns | `db.describe_table()` |
| `.stats` | DB statistics | `db.stats()` |
| `.integrity` | Verify B-trees | `db.integrity_check()` |
| `.checkpoint` | Flush + GC | `db.checkpoint()` |
| `.snapshots` | Show chain | `db.print_snapshots()` |
| `.snapdiff <a> <b>` | Diff snapshots | `db.snapshot_diff()` |
| `.snapshot tag <id> <lbl>` | Tag snapshot | `db.snapshot_tag()` |
| `.snapshot restore <id>` | Restore | `db.snapshot_restore()` |
| `.rollforward` | Advance to latest snapshot | `db.rollforward()` |
| `.expire [keep]` | Expire old snapshots (default 20) | `db.expire_snapshots()` |
| `.begin` / `.commit` / `.rollback` | Transaction control | `db.begin/commit/rollback()` |
| `.snapshot_debug` | Verbose snapshot chain dump | `db.print_snapshot_debug()` |
