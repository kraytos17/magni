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

6. **WAL durability** — All writes go to a sequential `-wal` file. Commits append a commit frame +
   `fsync` (not the full cache). Crash recovery replays committed frames on open. Checkpoint
   writes WAL frames back to the main file. `wal_abort_txn` discards uncommitted writes without
   touching the main file — no page leak on rollback.

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
7. Tab completion matches against a static list of 21 dot-commands. On
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
| Tab | Dot-command completion |
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
|---|---|---|---|
| `tree_insert` | `tree_insert_cow` | Insert cell, split when full. COW copies each page on path before modifying. | 1 |
| `tree_find` | — | Binary search descending to leaf, then `leaf_lower_bound`. | 1 |
| `tree_delete` | `tree_delete_cow` | Remove cell by rowid via binary search. COW variant COWs the full path. | 1 |
| `tree_update` | `tree_update_cow` | Delete + re-insert on same leaf, single traversal. | 1 |
| `tree_foreach` | — | Full iteration via cursor. | full scan |

**Cursor** — fixed-size path stack `[MAX_TREE_DEPTH]Cursor_Stack_Item` (64 entries, ~512 bytes).
`MAX_TREE_DEPTH :: 64` is the single source of truth for both the cursor stack size and
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

Packed byte bits: 0-2 = type, 3 = not_null, 4 = pk, 5 = has_check, 6 = has_default
```

All mutations use COW and return a new schema root. Both `add_table` and `add_table_cow`
call `tree_find` at the candidate hash key before inserting — if a row exists with a
different name, a hash collision is reported and the insert is rejected.

#### 3e. Database Coordinator (`db/`)

```odin
Database :: struct {
    path:               string,
    pager:              ^pager.Pager,
    is_new:             bool,
    mu:                 sync.Mutex,
    schema_root_page:   u32,
    latest_snapshot:    u32,
    refs_page:          u32,
    txn_state:          Transaction_State,
    txn_snapshot_id:    u64,
    txn_start_file_len: i64,
    snapshot_index:     map[u64]u32,
    table_roots:        map[string]u32,
    table_roots_dirty:  bool,
}
```

The `table_roots` field is an incremental cache of table → root-page mappings, updated
from COW DML return values. This avoids scanning the schema tree on every
`list_tables` / `describe_table` call.

```
execute(db, sql):
  lock(mu)
  stmt = parse(sql, temp_allocator)
  ok, new_root = executor.execute(schema_tree, stmt)
  db.schema_root_page = new_root
  if ok && !readonly:
    wal_begin_txn()
    create_snapshot()
    set_ref("main" → snap_id)
    wal_commit_txn()          // single fsync of WAL, not full cache
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

Tags (64 bytes) stored at offset 40 in unused page space.

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
Database.mu (sync.Mutex)           ← serializes all db.* operations
    │
    └── Pager.mutex (sync.RW_Mutex)  ← per-operation page cache access
        ├── Read shared:  page_count, page_in_cache
        └── Write exclusive: get_page, allocate_page, unpin_page,
                             mark_dirty, free_page, copy_page
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
- **No indexes**: Only the implicit primary-key B-tree exists.
- **`CHECK` limited to integer comparisons**: `col > 0`, `col < 100` format only.
- **Mixed AND/OR WHERE**: Not supported.
- **Max 10 columns per table**: Enforced by `MAX_COLS` constant (inline `[dynamic; N]T` scratch buffer).
- **REPL line editor**: no SQL keyword/table-name completion (dot-commands only).

---

## Appendix: API Surfaces

### Public `db` API

```odin
open(path: string)                  -> ^Database, bool
close(db: ^Database)
execute(db: ^Database, sql: string)  -> bool
query(db: ^Database, sql: string)    -> Query_Result

Query_Result :: struct {
    columns:   []string,
    col_types: []types.Column_Type,
    rows:      [][]types.Value,
    ok:        bool,
}

checkpoint(db: ^Database)           -> bool
begin(db: ^Database)                -> bool
commit(db: ^Database)               -> bool
rollback(db: ^Database)             -> bool
snapshot_restore(db, id)            -> bool
rollforward(db)                     -> bool
snapshot_tag(db, id, label)         -> bool
expire_snapshots(db, keep)          -> bool
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
expire_snapshots(pager, latest, keep_count)              -> [dynamic]u64
expire_and_collect(pager, latest, keep_count)
set_ref(pager, page, name, id, kind, protected)          -> bool
get_ref(pager, page, name)                               -> (snapshot_id, bool)
log_push(pager, page, snapshot_id)                       -> bool
log_pop(pager, page)                                     -> (snapshot_id, bool)
```

### CLI Dot-commands

| Command | Action | Implementation |
|---|---|---|
| `.exit` / `.quit` | Exit | `handle_dot_command` returns `true` |
| `.help` | Show help | `print_help()` |
| `.version` | Print version | `APP_VERSION` |
| `.tables` | List tables | `db.list_tables()` |
| `.schema` | Show DDL | `db.print_schema()` |
| `.debug_schema` | Show verbose schema dump | `db.print_schema_debug()` |
| `.dump <table>` | Dump rows | `db.dump_table()` |
| `.desc <table>` | Describe columns | `db.describe_table()` |
| `.stats` | DB statistics | `db.stats()` |
| `.integrity` | Verify B-trees | `db.integrity_check()` |
| `.checkpoint` | Flush + GC | `db.checkpoint()` |
| `.snapshots` | Show chain | `db.print_snapshots()` |
| `.snapdiff <a> <b>` | Diff snapshots | `db.snapshot_diff()` |
| `.snapshot tag <id> <label>` | Tag snapshot | `db.snapshot_tag()` |
| `.snapshot restore <id>` | Restore | `db.snapshot_restore()` |
| `.rollforward` | Advance to most recent snapshot | `db.rollforward()` |
| `.expire [keep]` | Expire old snapshots (default 100) | `db.expire_snapshots()` |
| `.begin` / `.commit` / `.rollback` | Transaction | `db.begin/commit/rollback()` |
