# Concurrency Model

The engine uses two independent locks. This document records what each protects and
the required acquisition order.

## Locks

### `db.Database.mu` (`sync.RW_Mutex`)
Protects **database-level state**:

- `schema_root_page` and the schema B-tree root it points to
- `snapshot_index`, `latest_snapshot`, the refs page, and the snapshot chain
- transaction state (`txn_state`, `txn_snapshot_id`, `txn_start_file_len`)
- `snapshot_batch_count` / `snapshot_batch_threshold`

Taken as a **shared** lock for read-only operations (`SELECT`, compound queries,
admin read commands) and as an **exclusive** lock for writes (DDL/DML/transactions).
Acquired in `db.execute`, `db.query`, and the admin/snapshot command handlers.

### `pager.Pager.mutex` (`sync.RW_Mutex`)
Protects **storage-layer state**:

- the page cache (`cache_table`, `slots`, `free_slots`, `slot_count`, `evict_hand`,
  `dirty_pages`)
- the WAL state and the page bitmap / free-page list
- file length and the file handle

Acquired inside every pager operation (`get_page`, `allocate_page`, `unpin_page`,
`mark_dirty`, eviction, free-page reuse, WAL ops).

### `schema.Table_Cache.mu` (`sync.RW_Mutex`)
Protects the **schema catalog cache**: the `tables` map and the schema-root version used to
invalidate it. Taken inside `schema.find_table_cached` (both the lookup and the populate-on-miss
path, since concurrent readers can populate it under a shared `db.mu`).

## Acquisition order

**Always acquire `db.mu` before `table_cache.mu` before `pager.mutex`; never the reverse.**

Rationale: a DB operation (`db.execute`) takes `db.mu`, then descends through
`btree`/`executor`, which call pager operations that take `pager.mutex` while
`db.mu` is held. Executor schema lookups go through `find_table_cached`, which takes
`table_cache.mu` in between. No code path acquires `pager.mutex` or `table_cache.mu` and then
reaches for a higher lock, so there is no lock-ordering cycle.

## Lock preconditions

Some pager functions require the caller to hold `p.mutex` (write-locked); this is
documented at the declaration (e.g. `alloc_from_freelist`). Callers are all
internal to the pager and uphold the precondition — the compiler cannot enforce
it, so keep the invariant in mind when adding new pager internals.

## Notes

- COW means reads never block writes: `SELECT` takes `db.mu` shared and touches
  only COW snapshots/old pages, which are immutable.
- The pager's `RW_Mutex` is used asymmetrically: page-fetch (`get_page`,
  `allocate_page`, `unpin_page`, `mark_dirty`) takes the exclusive lock even for
  reads, because loading a page mutates the cache; only cheap probes
  (`page_count`, `page_in_cache`) take the shared lock. Two locks still allow
  reads to hold `db.mu` shared and serialize only on cache-mutating pager ops.
  Consolidating to a single lock (owned by `db`) is a possible future
  simplification if the parallelism is not worth the complexity.
