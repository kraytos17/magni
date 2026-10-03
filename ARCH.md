# Magni Architecture

Magni is an embedded SQL database engine built in [Odin](https://odin-lang.org/). It implements a
subset of SQL with a copy-on-write (COW) B+tree storage engine, SQLite-compatible row format, and an
append-only snapshot chain supporting time-travel queries and point-in-time restore. This document
is the single reference for the design, the package layering and `@(private)` visibility rules, and
the conventions contributors must uphold.

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Principles](#architecture-principles)
- [Package Layering & Contribution Rules](#package-layering--contribution-rules)
- [Layer Architecture](#layer-architecture)
  - [SQL Layer](#1-sql-layer--parser-and-executor)
  - [Line Editor](#2-line-editor--linedit)
  - [Storage Engine](#3-storage-engine)
  - [Logging](#4-logging--corelog)
- [Snapshot System](#snapshot-system)
- [Transaction & Concurrency Model](#transaction--concurrency-model)
- [Memory Management](#memory-management)
- [Performance Characteristics](#performance-characteristics)
- [Trade-offs & Alternatives](#trade-offs--alternatives)
- [Conventions](#conventions)
- [Limitations](#limitations)
- [CLI Dot-commands](#cli-dot-commands)

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
2. Statement class decides the lock: shared `db.mu` for reads, exclusive for writes
3. `executor.execute()` dispatches to the statement handler with the staged-root
   map (`pending`) when a transaction is active:
   - DML mutates via COW B-tree operations, threading data (and index) roots
     beside the schema root; in-txn roots stage into `pending`, otherwise the
     schema root publishes (DDL always publishes immediately)
4. After execution, a snapshot is created capturing the new schema root
   (batched by threshold; none mid-transaction — one lands at `COMMIT`)
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

2. **Append-only history** — Snapshot chain never mutates. `snapshot_restore` points the `main`
   ref at the target snapshot and updates the schema root; history is not rewritten. The refs-page
   rollforward log records the previous ref so `rollforward` can undo a restore.

3. **Single-descent mutations** — The UPDATE core (delete + re-insert on
   one leaf visit) runs in a single root-to-leaf descent. Counting the
   lookup that finds the target, an UPDATE costs 2 traversals (see the
   trade-offs table, which breaks both numbers down).

4. **Bulk memory management** — All per-statement allocations use `context.temp_allocator` and are
   freed in one shot via `free_all()`. No per-node freeing during parse or execute.

5. **Slab allocation for hot data** — Page cache uses a fixed-size inline slab rather than a
   heap-allocated map. Zero per-page heap allocations.

6. **WAL durability** — All writes go to a sequential `-wal` file. Commits append a commit frame +
   `fsync` (not the full cache). Crash recovery replays committed frames on open. Checkpoint
   writes WAL frames back to the main file. `wal_abort_txn` discards uncommitted writes without
   touching the main file — no page leak on rollback.

7. **Page format versioning** — V3-only by policy (`PAGE_FORMAT_VERSION = 3`):
   dense interiors + slotdir leaves for data, prefix-compressed text
   leaves + full-key text interiors for the secondary index (§3f-ii).
   Files stamped with any other version (including V2) are rejected at
   open with `.Unsupported_Format` — no readers, no migration; move data
   via dump/reimport.

8. **Explicitly-built skip indexes** — Integer value→page-range bounds
   (`=`, `<`, `<=`, `>`, `>=`) apply only when a skip index was built
   explicitly (`btree.build_skip_index`); reads never auto-build (a read
   holds `db.mu` shared and could never publish the new schema root).
   See §3g for the operator-aware bound rules.

9. **Space reclamation via `.vacuum`** — Delete paths remove cells but do not merge sparse
    leaves, so delete-heavy workloads leave sparse pages behind. `btree.tree_vacuum`
    (data) and `btree.text_tree_vacuum` (secondary index) rebuild each tree into
    fresh, densely packed pages (COW-safe: old pages stay readable by
    snapshots and are reclaimed by the next GC pass), exposed as the `.vacuum` dot-command /
    `admin.vacuum`. Run it periodically to reclaim space.

10. **Row count tracking** — Per-page row counts are maintained incrementally on insert/delete
    and cached in the pager. `COUNT(*)` with exactly one projected column and no WHERE/GROUP
    BY/DISTINCT/ORDER BY/LIMIT is served directly from the cache without scanning (companion
    columns take the general aggregate path).

11. **Logging as a side channel** — Library code propagates errors via `DB_Error`/`or_return`;
    `core:log` messages are a side channel, not control flow. Logs go to stderr so stdout
    carries only query results. Tests that exercise expected-error paths silence them with
    the `suppress_expected_errors` / `restore_logger` helpers in `tests/test_util.odin` (nil
    logger), because the test runner counts error-level logs as failures.

---

## Package Layering & Contribution Rules

### Package layers

```
Layer 0  types            util/varint        sqltext
Layer 1  cell             pager            parser           linedit
Layer 2  btree
Layer 3  schema           snapshot
Layer 4  executor
Layer 5  db
Layer 6  admin
Layer 7  main
```

| Package | Depends on | Notes |
|---|---|---|
| `types` | — | Shared domain model (`Value`, `Column`, `Table`, ...). Leaf. |
| `util/varint` | — | Generic primitives, no database knowledge. Leaf. |
| `sqltext` | — | Statement splitter. Leaf (core-only imports); shared by CLI + fuzz harness. |
| `cell` | types, util/varint | Row cell codec. |
| `pager` | types | Page cache, WAL, freelist, page bitmap (`core:container/bit_array`). |
| `parser` | types | Self-contained SQL front end — **must stay storage-independent**. |
| `linedit` | — | Standalone line editor. **Must stay dependency-free.** |
| `btree` | cell, pager, types | COW B+tree. |
| `schema` | btree, cell, types | Table catalog. |
| `snapshot` | btree, pager, types | Snapshot chain, manifests, GC. |
| `executor` | btree, cell, pager, parser, schema, types | Query engine. |
| `db` | btree, cell, executor, pager, parser, schema, snapshot, types | Top-level facade. |
| `admin` | db, btree, cell, executor, pager, schema, snapshot, types | CLI introspection/presentation. |
| `main` | db, admin, linedit, schema, sqltext | CLI entry point. |

### Rules

1. **No package may import a package from a strictly higher layer.** `types`
   cannot import `cell`; `pager` cannot import `btree`; `executor` cannot import
   `db`. This is enforced naturally by Odin (an import that creates a cycle is a
   compile error), but check new imports: `grep -rn '^import "src:'` before
   merging if unsure.
2. **Only `admin` and `main` may import "everything".** Nothing under
   `btree/`, `cell/`, `pager/`, `parser/`, `schema/`, `snapshot/`, `executor/`,
   or `db/` should ever import `db` or `admin`. If you find yourself wanting
   that, the code belongs in `db` or `admin`, not where you were about to put it.
3. **`parser` and `linedit` must stay dependency-free of the storage/execution
   stack.** If a future change makes `parser` need to know about `btree`, that is
   a sign the change belongs in `executor` instead.
4. **An embedder needs only `db`.** `db.open/execute/query/close` and
   `Query_Result` are the engine's API. Presentation code lives in `admin` and
   `main` so a host program can link the engine without `core:text/table` or
   terminal tooling.

### Visibility convention

Odin exposes every declaration by default, so the compiler only enforces a reuse
boundary when you annotate it:

- `@(private="file")` — helper used by exactly one file (e.g.
  `freeblock_read_next`/`freeblock_write_next`).
- `@(private)` — helper shared across files in a package but not meant for other
  packages (e.g. the btree layout accessors, pager cache internals, snapshot GC
  helpers).
- No attribute — the package's genuine public API, listed in the package doc
  comment at the top of each package's primary file.

To mark something private, confirm its only callers are inside the package; the
compiler will reject cross-package uses, which is the point. If another package
later needs it, removing the attribute is a deliberate, visible decision.

### Test layout

Contributor gates and test conventions: [docs/testing.md](docs/testing.md).

Tests currently live in the single `tests` package (per-package colocation is a
planned follow-up). Because `tests` reaches into package internals, a symbol used
by `tests` **cannot** be `@(private)`. When moving tests into package
directories, re-run the private-marking pass to tighten further.

---

## Layer Architecture

### 1. SQL Layer — `parser/` and `executor/`

**Parser** (`parser.odin`):
- Lexer: character-by-character scanner producing `[]Token` (80 token types as `enum u8`,
  including `.EOF`).
- Recursive-descent parser: one function per grammar rule (`parse_create_table`,
  `parse_create_index`, `parse_insert`, `parse_select`, `parse_update`,
  `parse_delete`, `parse_drop_table`).
- `Select_Stmt` supports `AS OF SNAPSHOT <id>` and `AS OF TIMESTAMP <micros>`.
- All AST nodes allocated on caller-provided allocator; no per-node cleanup needed.
- `LIMIT` without `ORDER BY` uses pushdown: `scan_table` stops early when `max_rows` is reached.
  Pushdown is skipped when `ORDER BY`, `DISTINCT`, or aggregates/GROUP BY/HAVING are present,
  since all three change which rows survive.

**Executor** (`executor.odin`):
- Entry: `execute(schema_tree, stmt, out: ^Result = nil, cache: ^schema.Table_Cache = nil, pending: ^Pending_Roots = nil) -> (ok, new_schema_root, mutated)`.
  `pending` carries the transaction's staged data/index roots (nil outside
  explicit transactions); `mutated` reports the affected table for the
  commit tail.
- **Schema catalog cache**: `db.Database.table_cache` (a `schema.Table_Cache`) lazily caches
  deserialized `types.Table` catalog entries keyed by table name, invalidated implicitly by the
  schema-root version (`cache.root != t.root` clears it — any DDL changes the root). Executor
  table lookups use `schema.find_table_cached` (borrowed reference; the cache owns the tables, so
  callers never call `table_free`). The cache has its own mutex (acquired under `db.mu`, before
  `pager.mutex`).
- **Core executor is pure**: it never prints or does direct I/O. SELECT/Compound statements are
  evaluated through the data-returning paths (`exec_query` / `exec_compound_data` /
  `exec_select_join_data` / `exec_subquery_data`) and their rows/columns are captured into the
  optional `out: ^Result`. Rendering happens in the CLI layer (`db.execute` →
  `executor.render_result`), so the engine is embeddable behind any frontend.
- `EXPLAIN` is **side-effect-free**: it renders the access decision from the
  same resolvers execution uses, in the same order — `PK SEEK`, `INDEX SCAN
  ... USING col (eq|prefix|in, covering|fetch)`, or `FULL SCAN` — as a
  single-row `QUERY PLAN` result. Non-single-table statements keep the
  legacy echo of the inner SQL.
- DML dispatch (CREATE, CREATE INDEX, INSERT, SELECT, UPDATE, DELETE, DROP).
- Stream COW: UPDATE/DELETE apply mutations directly in the scan loop instead of
  batch-collecting all ops first — O(1) peak memory per operation regardless of row count.
- `INSERT` supports multi-row `VALUES (..),(..),...`; `exec_insert_cow` inserts each row with COW,
  threading the data root between rows and applying a single `update_root_page_cow` at the end.
  When a column list omits the primary-key column (or provides NULL), the auto-increment rowid is
  written back into the stored PK value (SQLite `INTEGER PRIMARY KEY` semantics).
- `SELECT` column lists support `AS <alias>` and bare-identifier aliases; aliases are stored
  parallel to `columns` (base names kept for resolution) and used for output headers.
- GROUP BY/HAVING are honored even when the SELECT list carries no aggregates: the parser registers
  aggregate references found in the HAVING clause (`COUNT(*)`, `SUM(v)`, ...) into `stmt.aggregates`,
  and the executor routes such queries to the aggregate path. `db.query` uses the data-returning
  join evaluator (`exec_select_join_data`) so JOIN results are returned, not just printed.

Key subroutines:

| Subroutine | Role | Performance note |
|---|---|---|
| `scan_table` | Full table scan via cursor | Moves cell values directly (no deep copy) |
| `try_pk_lookup` | Fast-path: `WHERE pk = literal` | O(log n) tree_find vs full scan |
| `resolve_index_covering` / `resolve_index_fetch` | Secondary-index routing (eq/prefix/IN) | Index seek + optional recheck vs full scan |
| `evaluate_where_ctx` | Filter rows via boolean-expression tree (AND/OR/parens, short-circuit) | Columns pre-resolved once; recursive eval per row |
| `try_join_match` | Combine rows + ON evaluation | Uses temp_allocator only on match |
| `dedup_rows` | DISTINCT via hash-set (FNV fingerprint) | O(n), non-adjacent duplicates handled |
| `sort_rows` | ORDER BY with integer fast path | Single-column int: `[]i64` + index sort |
| `compute_aggregates` | COUNT/SUM/AVG/MIN/MAX | `@(fast_math)` on f64 reduction for auto-vectorization |
| `check_constraints` | CHECK enforcement on INSERT/UPDATE | Fail-closed: rejects non-integer, unknown col |
| `render_table` | Query/command result rendering | `core:text/table` markdown output; `unicode_width_proc` for CJK-aligned columns |
| `exec_select_data` | Data-returning SELECT evaluator (operands for set ops) | Returns `(rows, cols)` without printing; covers literals, single-table, subqueries, joins, aggregates |
| `row_fingerprint` | FNV-1a hash of a row's values | Shared by `dedup_rows` (DISTINCT) and set-op membership |
| `intersect`/`except` | Set membership (distinct) | O(n+m) via fingerprint index of the right operand |
| `intersect_all`/`except_all` | Set membership (multiset) | O(n+m) via fingerprint→count maps |

**WHERE** conditions parse into a boolean-expression tree with standard SQL precedence (AND binds
tighter than OR), parentheses, and `NOT` support — both prefix (`NOT <expr>` wraps its child in a
`.NOT` node) and infix (`col NOT IN (...)` / `col NOT LIKE 'x'` / `col IS NOT NULL` negate the
leaf condition):
`a = 1 AND b = 2 OR c = 3` parses as `(a = 1 AND b = 2) OR c = 3`. `col IS NULL` tests
nullness directly (`= NULL` never matches, per SQL semantics); `IS NOT NULL` negates the leaf. `parse_where_clause` builds the
tree in `parse_where.odin`
(`parse_or_expr` / `parse_and_expr` / `parse_primary`); HAVING and JOIN `ON` clauses reuse it.
`init_where_ctx` resolves each leaf once into a `Resolved_Node` tree (column indices, column-column
comparisons, `IN` lists, materialized `IN` subqueries), and `evaluate_where_ctx` recurses with
short-circuiting (AND fails fast, OR succeeds fast, NOT inverts). The skip-index range optimization in
`scan_table` only applies to a flat top-level AND chain of single-column integer comparisons; OR, NOT,
or nested groups disable skipping (full scan).

**Query planning** (`executor/index_scan.odin`, shared by the scalar and
vector fetch paths): PK seek first (`try_pk_lookup`), then secondary-index
routing for usable predicates on the indexed TEXT column — equality,
canonical `LIKE 'stem%'`, literal `IN` (first usable conjunct of a flat
AND chain; OR/NOT/nested/negated fall back). Covering `SELECT rowid`
answers from the index alone; wider projections fetch per candidate and
recheck the full filter. Index paths never push `LIMIT` down (candidates
arrive in rowid order, so the shared tails slice exactly like the scan
path). User guide: [docs/indexing.md](../docs/indexing.md).

**GROUP BY** uses direct FNV-1a hashing of `Value` union data (raw bit pattern for `f64`,
`u64` for `i64`, FNV of bytes for strings/blobs) keyed on `map[u64][dynamic]int` — a chain of
candidate group indices per hash, so hash collisions between distinct keys are resolved by
verifying every candidate with the full equality check before a new group is created (the same
chained-bucket pattern used by set operations). No stringification, no allocation per row, and
no float-precision loss. Groups are printed using the original `key_values` `[]types.Value`
stored in each `Group` struct.

**HAVING** evaluates the same boolean-expression tree (`evaluate_where_having` in `aggregates.odin`,
recursive over `parser.Where_Node`) against both group-key values and computed aggregate values.
Supports
aggregate function references (e.g., `HAVING count > 1`) as well as group-by column comparisons.
Aggregate names are compared case-insensitively, so `HAVING COUNT > 1` and `HAVING count > 1`
are equivalent. The `(N rows)` footer reports the number of rows after HAVING filtering, not the
total number of groups.

**Set operations** (`UNION` / `INTERSECT` / `EXCEPT`, with `ALL` variants) combine the result
sets of two or more `SELECT` statements. Precedence follows the SQL standard: `INTERSECT` binds
tighter than `UNION` / `EXCEPT`; all are left-associative. The first operand's column names form
the output header; all operands must have equal column counts. `exec_select_data` evaluates each
operand to `(rows, cols)` without printing, then the segment-based reducer in `set_ops.odin`
applies INTERSECT runs before folding UNION/EXCEPT left-to-right. The set primitives
(`union_op`, `union_all_op`, `intersect`, `intersect_all`, `except`, `except_all`) build a
fingerprint index of the right operand once and probe it per row, giving O(n+m) membership; the
`ALL` variants track multiplicities via count maps. A trailing `ORDER BY` / `LIMIT` / `OFFSET`
applies to the combined result. `SELECT` also supports FROM-less literal columns (e.g.
`SELECT 1, 'a'`), which produce a single row and can participate in set operations. Value
rendering is centralized in `types.value_to_string` (compact `%g` floats); `executor.value_string`
delegates to it.

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

V3-only since B4b: dense interiors (`INTERIOR_DENSE` = 6) and slotdir
leaves (`LEAF_SLOTDIR` = 15) for primary data, prefix-compressed text
leaves (`LEAF_TEXT` = 16) with full-key text interiors (`TEXT_INTERIOR`
= 17) for the secondary index. Byte layouts live in §3f-ii (not repeated
here). V2 bytes (`INTERIOR_TABLE`/`LEAF_TABLE`) have no dispatcher arm
and fail closed at resolve — never reinterpreted; migrate via
dump/reimport. All production writers (fresh pages, splits, COW roots,
vacuum output) emit V3 only.

**On-disk page layout (4096 bytes):**

```
Page 1:
  [DB Header: 100B] [B-tree offset 100: Page_Header|page body...]

Page N (N > 1):
  [offset 0: Page_Header|page body...]
```

Page bodies are format-dependent (dense key/child arrays, slot arrays +
cells, prefix-compressed text runs); every format shares the 8-byte
header below, so the pager and cursor stay format-agnostic.

**Page header (8 bytes, `#packed`):**

```
┌──────────┬─────────────────┬────────────┬──────────────────────┬──────────────────┐
│ page_type│ first_freeblock │ cell_count │ cell_content_offset  │ fragmented_bytes │
│  (u8)    │    (u16le)      │  (u16le)   │      (u16le)         │      (u8)        │
└──────────┴─────────────────┴────────────┴──────────────────────┴──────────────────┘
```

Dense interiors carry their own trailing fields (including the rightmost
child, `u32le`) after this prefix — there is no universal 12-byte
interior header anymore.

**In-memory `Node` struct:**

```odin
Node :: struct {
    id:     u32,
    data:   []u8,
    header: ^Page_Header,     // computed once on load
    layout: Page_Layout,      // resolved once per page from format registry
}
```

`leaf`/`interior` sub-headers are computed on demand via `node_leaf()` / `node_interior()` —
no redundant pointer storage.

**Freeblock chain:**
Deleted cell space is tracked in a SQLite-compatible freeblock list (shared
by slotdir and text leaves):
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
| `tree_find` | — | Descend to leaf, then page lower bound. | 1 |
| `tree_delete` | `tree_delete_cow` | Remove cell by rowid via binary search. COW variant COWs the full path. | 1 |
| `tree_update_cow` | — | Lookup, then COW delete + re-insert in a single root-to-leaf descent. | 2 (find + mutation) |
| `tree_foreach` | — | Full iteration via cursor. | full scan |
| `tree_vacuum` / `text_tree_vacuum` | — | Rebuild data / text trees into fresh, densely packed pages (COW-safe). Surfaced as `.vacuum` via `admin.vacuum`. | full scan |
| `text_find_rowids` / `text_find_prefix` | — | Multi-leaf equality / prefix scans over the text index (sibling-or-carry advance). | index range |

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
- **Lookup**: open-addressed `cache_table` (`[]Cache_Entry`, 2048 buckets,
  linear probing at load ≤ 0.125, backward-shift delete) — O(1) average, no hashing.
- **Eviction**: second-chance (clock) scan for first unpinned slot.
- **Free-list**: `free_slots: [dynamic]^Page_Slot` provides O(1) slot allocation.
- **Freelist**: Linked list stored in-page. `first_free_page` persisted in database header.
- **Concurrency**: `RW_Mutex` — reads use shared locks; writes use exclusive locks.
- **WAL**: All writes go to a `-wal` sidecar file. Each frame carries a 64-bit FNV checksum
  (split across `checksum1`/`checksum2` in `WAL_Frame_Header`). `wal_recover` verifies
  checksums — a mismatched frame stops collection at that point; earlier frames are still
  replayed. `wal_abort_txn` discards uncommitted frames. `wal_checkpoint` writes WAL frames
  back to the main file and truncates the WAL. Checksums computed incrementally (no temp buffer).
- **Page bitmap**: `core:container/bit_array.Bit_Array` tracks ever-allocated pages and
  grows on demand (amortized O(1)). GC sweep skips zero 64-bit words (all 64 pages free)
  in O(1).
- **WAL commit is O(pages dirtied)**: `mark_dirty` records page numbers in a `dirty_pages`
  list; `wal_commit_txn`/`wal_abort_txn` iterate that list instead of scanning all cache slots.

#### 3d. Table Metadata (`schema/`)

Schema is stored as a B-tree on page 1.

| Index | Type | Content |
|---|---|---|
| RowID | i64 | `fnv64(table_name) & 0x7FFFFFFFFFFFFFFF` (63-bit, sign bit cleared) |
| [0] | i64 | Kind discriminator (`0` = table) |
| [1] | TEXT | Table name |
| [2] | INT | B-tree root page number |
| [3] | TEXT | Original CREATE TABLE statement |
| [4] | BLOB | Serialized column definitions |
| [5] | INT | Skip-index root page (present only when > 0) |
| [6] | INT | Secondary text index root page (with [7] only) |
| [7] | TEXT | Indexed column name (with [6] only) |

Rows without an index stay 5/6-wide and keep parsing; index fields sit at
fixed positions whenever either is set, so positions never shift.

Column blob format:
```
[0xFE:marker][version:1][count:varint]
  per column: [name_len:varint][name_bytes][packed:1][default_value?][check_len:varint?][check_bytes?]
```

Packed byte bits: 0-2 = type, 3 = not_null, 4 = pk, 5 = has_check, 6 = has_default

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
    txn_state:                Txn_State, // { None, Active }
    txn_start_file_len:       u64,
    snapshot_index:           map[u64]u32,
    refs_page:                u32,
    snapshot_batch_count:     int,
    snapshot_batch_threshold: int,
    wal_size_threshold:       int, // 0 = disabled; auto-checkpoint the WAL at this many frames
    table_cache:              schema.Table_Cache, // schema catalog cache, invalidated on schema-root change
    txn_pending:             executor.Pending_Roots, // staged data + index roots; flushed at COMMIT (explicit txn only)
    mu:                       sync.RW_Mutex,
}
```

`snapshot_batch_count` and `snapshot_batch_threshold` control batch snapshot creation —
snapshots are only created when `count >= threshold`, reducing write amplification
for bulk operations.

**`Open_Config`** provides optional configuration at open time (passed to `db.open`):
`wal_size_threshold` (auto-checkpoint the WAL once it accumulates N frames; 0 =
disabled — runs after each WAL commit via `maybe_auto_checkpoint`, which
checkpoints and re-flushes the header) and `snapshot_batch_threshold` (override
the default batch threshold). Both are exposed on the CLI as `--snapshot-batch`
and `--wal-size-threshold`.

```
execute(db, sql):
  stmt = parse(sql, temp_allocator)      // no lock yet
  is_read = SELECT | Compound
  if is_read: lock_shared(mu) else lock_exclusive(mu)
  pending = &txn_pending if txn active else nil   // DML stages roots; DDL publishes immediately
  ok, new_root, _ = executor.execute(schema_tree, stmt, &result, &db.table_cache, pending)
  if !as_of_override && !is_read && !stmt_defers_root:   // writes only
    db.schema_root_page = new_root
    update_header(db)
  if ok && !is_read:
    wal_begin_txn()
    if snapshot_batch_count >= threshold:   // batched snapshot creation (never mid-txn)
      create_snapshot()
      set_ref("main" → snap_id)
    wal_commit_txn()          // single fsync of WAL; iterates only the dirty-page list
    maybe_auto_checkpoint(db) // only if wal_size_threshold is set
  unlock(mu)
```

#### 3f. Page Format Versioning — `btree/layout.odin`, `btree/layout_iface.odin`

Live layouts (V3, post-B4b full migration): slotdir leaves (10-byte
`Slot{rowid, off}` entries, keys read directly with no body decode) and
dense interiors (position-dependent key/child arrays). The V2 row-major layout (10-byte
`Cell_Entry`) and its `compat` table were removed outright — no frozen
branches, no migration path. Page mechanics go through the `Page_Layout`
interface (`layout_iface.odin`), with key semantics in the
statically-dispatched `Key_Kind` (`.Rowid` / `.Text`).

`page_format_version` is a **database-wide** value stored in the database header
(`PAGE_FORMAT_VERSION :: 3`). New databases are created at the current version; the pager defaults to it. Files stamped with any other version are rejected at open with `DB_Error.Unsupported_Format`. Export with `.dump` under an older binary and reimport to migrate. V2-stamped files are rejected the same way: no V2 reader remains, so old files migrate via dump/reimport only.

#### 3f-ii. V3 Dense Page Vocabulary — `btree/layout_v3.odin`

Live discriminants: `INTERIOR_DENSE` (6) and `LEAF_SLOTDIR` (15), each
with a real table since B2/B3 — dense interiors
(search/read/validate/child/insert) and slot leaves (slot
mechanics/read/search/validate). Only cross-kind ops (leaf slots on
interiors, separators/children on leaves) and the not-yet-designed
prefix pages refuse with `Unsupported_Format`. V2 bytes
(`INTERIOR_TABLE`/`LEAF_TABLE`) have no dispatcher arm and fail closed
at resolve — never reinterpreted. All production writers emit V3 only:
fresh pages, splits, COW roots, and vacuum output. Layout, in brief:

- Dense interiors: 24-byte header (shared 8-byte `Page_Header` prefix, then
  `rightmost u32le`, `flags u16le`, `base u64le`, `reserved u16le`), then
  dense sorted keys (`u64le`, or `u32le` deltas from `base` when
  `DENSE_FLAG_FOR` and `max−min ≤ max(u32)`), then `count+1` `u32le`
  children. Fanout ≈340 full / ≈510 FOR. All arrays little-endian;
  RowIDs sign-biased so unsigned order == numeric order.
- Slotdir leaves: stock 8-byte `Leaf_Header` + freeblock semantics, with
  10-byte `Slot{rowid u64le, off u16le}` entries (same 10-byte stride the
  removed V2 `Cell_Entry` used, so capacity math is unchanged). In
  production since B4a (fresh pages, splits, vacuum output); since B4b
  the tree is V3-only — no V2 encoding remains to upgrade or read.
- Text leaves (`LEAF_TEXT` = 16, `btree/layout_text.odin`): prefix-compressed
  secondary text index leaves — stock `Leaf_Header` + `prefix_len u16le` +
  shared prefix, 4-byte `Text_Slot{off, len}` entries, cells are
  `[rowid u64be biased][suffix]`. Full-key text interiors (`TEXT_INTERIOR`
  = 17) hold separators + dense `u32le` children. Online COW inserts,
  deletes, splits, root growth, and multi-leaf equality scans live in
  `btree/text_tree.odin`; the dispatcher resolves both text page types
  with kind `.Text` (reads via `Key_Kind.Text` dispatch; every
  `Row_ID`-keyed vtable slot refuses).
- Index catalog: one text index per table — `Schema_Row`/`Table`
  carry `index_root` + `index_column` (fixed `[6],[7]` wire slots, old rows
  parse); root swaps reuse the `update_schema_root_cow` closure core;
  pending maps stage both key spaces through commit/vacuum/rollback.
  `CREATE INDEX name ON t (c)` (tokenizer + `Create_Index_Stmt`
  + `exec_create_index` with loop backfill) publishes root+column atomically.
  DML fan-out maintains the index on every mutation path — insert,
  pk/scan update (unchanged-column skips, NULL transitions), pk/scan
  delete — with differential oracle tests, txn rollback coherence, and
  autocommit/txn parity. (The dormant Direct DML paths were deleted
  outright afterward; single-kind COW is the only write surface.)
  Table-cache map keys are
  heap-cloned (a temp-borrowed key dangled across per-statement temp frees
  in-txn, silently serving stale roots). V3.0 scope: BINARY collation only,
  NULL-not-indexed, single-column text, no DROP INDEX.
- Index vacuum: `VACUUM` rebuilds text indexes into packed pages
  (`text_tree_vacuum`: ordered collect, greedy byte-chunked leaves and
  levels, empty index → fresh `LEAF_TEXT` root), hooked into the per-table
  admin loop beside data vacuum (in-txn staged roots publish first, same
  as data). Rowids are logical, so this is space reclamation only —
  correctness never depended on it.
- Planner routing (Phase E, `executor/index_scan.odin`): single-table
  SELECT filters over the indexed column route through the text index —
  equality, canonical `LIKE 'stem%'` (single trailing `%`, no `%`/`_` in
  the stem — exactly where `like_match` reduces to a byte-prefix test, so
  candidates and recheck agree by construction), and literal `IN` lists
  (TEXT members; cross-class/NULL members provably never match a TEXT
  row). Covering `SELECT rowid` answers from the index alone (synthetic
  single-INTEGER `rowid` column through the unchanged `finish_select`
  tail; no user column named `rowid` may exist); wider projections fetch
  `tree_find` per candidate and recheck the full filter, with sorted
  candidates so LIMIT-without-ORDER-BY matches the scan path exactly. Flat
  AND-chains route on the first usable conjunct; OR/NOT/nested groups,
  negated, column-comparison, NULL, and subquery-IN shapes fall back to
  the full scan (same result, one code path). Both fetch paths
  (scalar + vector) route identically; staged in-txn roots ride the cache
  overlay, so routing sees exactly what the scan would see. `EXPLAIN`
  renders the decision — `PK SEEK`, `INDEX SCAN ... USING col
  (eq|prefix|in, covering|fetch)`, `FULL SCAN` — from the same resolvers in
  the same order, so output can never disagree with execution;
  non-single-table statements keep the legacy echo. Reads get faster:
  50k-row on-disk A/B (release, interleaved) shows point-eq ≥20×
  (startup-floor-bound), prefix-fetch ~7×, 3-member IN ≥20× vs full scan.
- Headers, pure accessors, validators, builders, and init procs are
  unit-tested in `tests/btree_v3_test.odin` (header layout, roundtrips vs
  independent endian writes, search-vs-oracle, FOR boundary, corruption
  loudness, table behavior through the dispatcher). A standalone search
  microbench lives in `tests/perf_dense/` (manual release run, not part of
  `make perf`): dense search costs ~20–30ns on full pages vs ~800ns per
  point-lookup level — SIMD probing was dropped on that evidence (saves
  ~20ns of 800ns even at 4×; see B2 notes).

#### 3g. Skip Index — `btree/skip_index.odin`

Explicitly-built integer column index that accelerates `WHERE int_col <op> <value>` queries for the
comparison operators `=`, `<`, `<=`, `>`, `>=`:

- Built explicitly via `btree.build_skip_index` (reads never auto-build:
  a read holds `db.mu` shared and could never publish the new schema root,
  so an auto-build would be silently discarded).
- Maps integer value ranges to page ranges: `Skip_Entry{page_min, page_max, min_int, max_int}`.
  The index page records which column it indexes (`col_index` in the header), and a bound is
  only applied to conditions on that column.
- Bounds are **operator-aware**: `>`/`>=` yield a lower bound (the scan **seeks** to the first
  page whose max could match), `<`/`<=` yield an upper bound (the scan stops past it), and `=`
  yields both. Combined AND conditions intersect their windows (`max` lower bound, `min` upper
  bound). `<>`, `IN`, `LIKE`, and column-to-column conditions disable skipping entirely.
- Only applied to a flat top-level **AND chain** of single-column integer comparisons in the
  WHERE boolean tree; OR subtrees or nested parenthesized groups disable the optimization
  (a union of page ranges is not a safe scan bound), falling back to a full scan.
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

### 4. Logging — `core:log`

Logging uses Odin's built-in [`core:log`](https://pkg.odin-lang.org/core/log/) package,
assigned to `context.logger` in `main()`. A **file logger on stderr** is used with minimal
options `{.Level}` — messages carry a level header but no timestamp/location noise, and every
level stays on stderr so stdout carries only query results.

**Level resolution** (first match wins):

| Source | Value |
|---|---|
| `--verbose` / `-v` | `debug` |
| `--log-level <level>` | `debug`, `info`, `warn`, `error` |
| `MAGNI_LOG_LEVEL` env var | `DEBUG`, `INFO`, `WARN`, `ERROR` (uppercase) |
| default | `info` |

**Stream separation:**

| Destination | Content |
|---|---|
| stdout | Query results, dot-command output, help text — unchanged `fmt.println` |
| stderr | All `log.*` messages (debug/info/warn/error) |

**Behavior by mode:**

- REPL (interactive TTY): logger level forced to `.Error` so the prompt stays clean.
- `--eval` / `--file` / pipe mode: logger runs at the configured level.

**Categorization** across modules:

| Level | Examples |
|---|---|
| `error` | DML/schema/select failures ("Table not found", "Data type validation failed"), WAL open/fsync failures |
| `warn` | "Transaction already in progress", WAL checksum mismatch, "No active transaction" |
| `info` | "WAL: checkpoint complete", "WAL: recovery complete", "BEGIN/COMMIT/ROLLBACK transaction", "Inserted row N", "Created table" |
| `debug` | B-tree verify walk, tree page dumps (`tree_debug_print_node`, `verify_recursive`) |

**Tests:** each test function sets `context.logger.lowest_level = .Error` so expected-error
paths stay quiet while `log.error` events still reach the test runner's failure counting. Using
a nil logger would swallow those events and hide real failures (the runner attributes
`log.error` output to the currently-running test).

**Error propagation is unchanged:** logging is a side channel. Functions still return
`DB_Error`/`Error` values composed via `or_return`; logging never alters control flow.

---

## Snapshot System

User guide (time travel, lifecycle, restore, expiry): [docs/snapshots.md](docs/snapshots.md).

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
    live += snapshot_page, manifest_page
    btree.collect_pages(schema_root) → live += root + all sub-pages
    for each table root in manifest:
      btree.collect_pages(root) → live += root + all sub-pages
      if table has skip index:
        btree.collect_pages(skip_index_root)
  sweep:
    if page_bitmap exists:
      for each 64-bit word in bitmap:
        if word == 0: continue       # all 64 pages free, skip
        for each set bit: check against live; free if not live
    else:
      for every page from 2..max_page:
        if page not in live: free
```

Invariants (violations caused data loss / freelist corruption before the fix,
see `test_gc_reclaims_cow_waste`):

- Never pre-mark a root in `live` before `collect_pages`: it uses presence
  as its visited guard, so a pre-marked root returns early and its subtree
  is freed while still reachable.
- `free_page` must persist the freelist link (next pointer + WAL frame) even
  when the page is not cached; `alloc_from_freelist` reads WAL-first and
  treats an out-of-range link as end-of-list.

After GC the freelist is repopulated and later writes reuse it. Sweep never
truncates the file: with append-COW allocation the live set almost always
reaches the file top (measured `tail_dead=0` on bulk-load fixtures), so
truncate-to-high-water reclaims nothing in steady state — middle holes are
the norm and the freelist is their reclamation path.

On-disk shrinking happens exactly once: rollback. Aborted transactions
abandon their tail pages (never snapshotted, unreachable after roots
restore), so `rollback_impl` persists the rewind via
`pager.rewind_after_abort` (drop unpinned cache copies past the cut without
writeback, clear tail bitmap bits, reset freelist head, `os.truncate` +
`os.sync`, update `file_len`). Fail-closed: a pinned page past the cut or
any I/O error leaves the file at its old size.

Two companion guarantees make this crash-safe:

- `wal_checkpoint` and `wal_abort_txn` copy/drop WAL frames only up to the
  last commit marker (same `committed_upto` rule as `wal_recover`). Without
  this, a checkpoint after rollback would copy aborted frames to main and
  regrow the rewound file (observed: reopen footprint mismatch).
- `db.open` self-heals a freelist head past EOF back to 0 (leaked space
  until the next GC rebuilds it; links are validated on use, so it can
  never misread). Covers the truncate→header-write crash window.

---

## Transaction & Concurrency Model

User guide (txn semantics, staged roots, commit/rollback): [docs/transactions.md](docs/transactions.md).

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
    └── Table_Cache.mu (sync.RW_Mutex)  ← schema catalog cache lookups/population
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

SELECT * FROM tt AS OF SNAPSHOT 1;  → reads schema_root=100 → old data
```

### Transaction / GC interaction rules

- **Expire never runs inside a transaction** (`Reclaim_Decision` in
  `db/snapshot_cmds.odin`): uncommitted COW pages exist in no snapshot live
  set, so a sweep would free them out from under the txn (observed: whole
  tables vanishing mid-txn). The call stays a warned no-op (`.None`), never
  an error, so checkpoint still flushes WAL mid-txn.
- **Abort evicts without writeback** (`pager.evict_aborted`, reported via
  `Evict_Report`): aborted-txn cache copies are unreachable by construction
  once roots restore, so they drop straight to the free-slot pool instead of
  lingering as clean. Pinned pages are fail-open skips for the next GC.

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

**Borrowed strings and the `temp_allocator`**: `schema.find_table`/`list_tables` and the
`combined_cols` slice in `exec_query` return `Column.name` strings that borrow from the
allocator passed to the lookup. `context.temp_allocator` is a bump arena: allocations never
free or overwrite earlier blocks within one statement, so borrowed strings remain valid across
subsequent `make` calls. When a borrowed string must outlive further allocations on the same
arena (e.g. building a render matrix), copy it explicitly with `make([]string, N)` and index
assignment rather than relying on composite-literal aliasing.

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

| Metric | Slotdir leaf | Dense interior |
|---|---|---|
| Fanout / capacity | ~300 small cells | ≈340 full keys, ≈510 FOR-compressed |
| Tree depth (1M rows) | 3 | 3 |
| Search complexity | O(log₃₀₀ n) | O(log₃₄₀ n) |
| Insert: pages COW'd | depth + 1 | depth + 1 |
| Delete: pages COW'd (COW variant) | depth | depth |

### Page Cache

| Metric | Value |
|---|---|
| Slots | 256 |
| Slot size | 4136 bytes (32 Page + 4096 data + referenced flag) |
| Total memory | ~1 MB |
| Lookup (hit, avg probes) | ~1 (open-addressed `cache_table`, 2048 buckets, linear probing at load ≤ 0.125) |
| Slot allocation | O(1) — pop from `free_slots` |
| Eviction | O(n) — second-chance (clock) scan, 256 slots max |
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
| Range window | Operator-aware: `>`/`>=` seek lower bound, `<`/`<=` upper-bound stop, `=` both |
| Storage | Entries stored in schema B-tree root row; header records the indexed column |
| Invalidation | On any page mutation affecting the indexed column |

### Space Reclamation (`.vacuum`)

| Metric | Value |
|---|---|
| Operation | `btree.tree_vacuum` — full COW-safe rebuild into packed pages |
| Surface | `.vacuum` dot-command / `admin.vacuum` |
| Cost | O(n) rebuild per table; old pages reclaimed by the next GC pass |
| Scope | Manual maintenance; delete paths do not auto-merge sparse leaves |

### Row Count Tracking (Fast COUNT(*))

| Metric | Value |
|---|---|
| Cache location | Pager-attached `Stats` (`[dynamic]int row_counts`, page-id indexed, -1 = uncached; survives transient `btree.Tree` instances) |
| Update cost | O(1) per mutation (exact counts written from the touched pages, no recounts) |
| COUNT(*) fast path | O(1) if cached, O(pages) on first access |
| Bypass conditions | Queries with WHERE, GROUP BY, DISTINCT, ORDER BY, LIMIT, or companion projected columns use full scan |

---

## Trade-offs & Alternatives

### COW + WAL

| Aspect | COW + WAL (chosen) | WAL-only |
|---|---|---|
| Read concurrency | Concurrent shared-lock readers; historical reads via COW | Concurrent readers + writer |
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

### Single descent vs Delete+Insert

| Aspect | Single descent | Delete + Insert |
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

## Conventions

### Allocator ownership

`context.temp_allocator` is the default scratch arena for per-statement work; it is
reset wholesale (REPL per iteration, scripts at exit). Anything a function returns
to a caller must either live past the next reset or be explicitly documented. Two
conventions keep this visible:

- `// scratch-only: valid until the next temp_allocator reset` — the return value
  is consumed within the current statement scope.
- `// caller-owned: uses the passed allocator, not temp` — the function takes an
  explicit `allocator` parameter and the caller owns the lifetime.

Prefer passing an explicit `allocator` when a returned value outlives the current
scope (many functions already do, e.g. `schema.find_table(..., allocator :=
context.allocator)`).

## Limitations

- **No `FOREIGN KEY` enforcement on INSERT/UPDATE**: Validated at CREATE TABLE time only.
- **Secondary text index (V3.0 scope)**: one single-column TEXT index per
  table (`CREATE INDEX`), BINARY collation, NULL-not-indexed, no
  `DROP INDEX`. Routes equality, canonical `LIKE 'stem%'`, and literal
  `IN` (covering `SELECT rowid`, fetch+recheck otherwise); everything else
  scans. User guide: [docs/indexing.md](docs/indexing.md).
- **`CHECK` limited to integer comparisons**: `col > 0`, `col < 100`, `>=`, `<=`, `=`, `!=` format.
- **Max 10 columns per table**: Enforced by `MAX_COLS` constant (inline `[dynamic; N]T` scratch buffer).
- **REPL line editor**: SQL keyword and table/column name completion only (no in-expression or JOIN completion).

---

## CLI Dot-commands

| Command | Action | Implementation |
|---|---|---|
| `.exit` / `.quit` | Exit | `handle_dot_command` returns `true` |
| `.help` | Show help | `print_help()` |
| `.version` | Print version | `APP_VERSION` |
| `.tables` | List tables | `admin.list_tables()` |
| `.schema` | Show DDL | `admin.print_schema()` → `schema.print_ddl()` |
| `.debug_schema` | Show verbose schema dump | `admin.print_schema(debug)` → `schema.debug_print_all()` |
| `.tree_page <n>` | Print B-tree page structure | `admin.print_tree_page()` (debug dump) |
| `.dump <table>` | Dump rows | `admin.dump_table()` |
| `.desc <table>` | Describe columns | `admin.describe_table()` |
| `.stats` | DB statistics | `admin.stats()` |
| `.integrity` | Verify B-trees | `admin.integrity_check()` |
| `.checkpoint` | Flush + GC | `admin.checkpoint()` |
| `.vacuum` | Rebuild tables + text indexes into packed pages | `admin.vacuum()` |
| `.snapshots` | Show chain | `admin.print_snapshots()` ← `snapshot.chain_infos()` |
| `.snapdiff <a> <b>` | Diff snapshots | `db.snapshot_diff()` |
| `.snapshot tag <id> <lbl>` | Tag snapshot | `db.snapshot_tag()` |
| `.snapshot restore <id>` | Restore | `db.snapshot_restore()` |
| `.rollforward` | Advance to latest snapshot | `db.rollforward()` |
| `.expire [keep]` | Expire old snapshots (default 20) | `db.expire_snapshots()` |
| `.begin` / `.commit` / `.rollback` | Transaction control | `db.begin/commit/rollback()` |
| `.snapshot_debug` | Verbose snapshot chain dump | `admin.print_snapshots(debug)` (stderr) |

Output dialect: every tabular result (SELECT, `.snapshots`, `.snapdiff`,
`.tables`, `.stats`, `.desc`, `.dump`) renders through the single shared
`render.render_table` markdown printer (via `executor.render_result`) with an `(N rows)` footer; empty
results print the header plus `(0 rows)`. Snapshot timestamps render as
`YYYY-MM-DD HH:MM:SS` (raw micros stay in `.snapshot_debug`). Results go to
stdout; diagnostics and errors go to stderr (the file logger) — never mixed.

Dot-commands are dispatched by the shared `handle_dot_command` in every mode
(REPL, `--eval`, `--file`, piped stdin): a dot-command is exactly one trimmed
input line and `.exit`/`.quit` stops script processing. (Previously script
modes sent dot lines to the SQL parser, where they died silently as
`Parse_Error` — maintenance ops like `.expire` were unreachable outside the
REPL.)
