# Magni Architecture & Contribution Rules

This file records the layering and visibility rules that keep the codebase
modular. See [ARCH.md](ARCH.md) for the detailed design; this file is about the
rules a contributor must uphold.

## Package layers

```
Layer 0  types            util/varint, util/bitmap
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
| `util/varint`, `util/bitmap` | — | Generic primitives, no database knowledge. Leaf. |
| `cell` | types, util/varint | Row/columnar cell codec. |
| `pager` | types, util/bitmap | Page cache, WAL, freelist, bitmap. |
| `parser` | types | Self-contained SQL front end — **must stay storage-independent**. |
| `linedit` | — | Standalone line editor. **Must stay dependency-free.** |
| `btree` | cell, pager, types | COW B+tree. |
| `schema` | btree, cell, types | Table catalog. |
| `snapshot` | btree, pager, types | Snapshot chain, manifests, GC. |
| `executor` | btree, cell, pager, parser, schema, types | Query engine. |
| `db` | btree, cell, executor, pager, parser, schema, snapshot, types | Top-level facade. |
| `admin` | db, btree, cell, executor, pager, schema, snapshot, types | CLI introspection/presentation. |
| `main` | db, admin, linedit, schema | CLI entry point. |

## Rules

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

## Visibility convention

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

## Test layout

Tests currently live in the single `tests` package (per-package colocation is a
planned follow-up). Because `tests` reaches into package internals, a symbol used
by `tests` **cannot** be `@(private)`. When moving tests into package
directories, re-run the private-marking pass to tighten further.
