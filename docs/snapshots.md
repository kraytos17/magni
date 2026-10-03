# Snapshots and Time Travel

Every committed write is a copy-on-write snapshot; old pages stay readable,
so any committed state can be queried, diffed, restored, or garbage-collected.

## Time-travel queries

```sql
SELECT * FROM users AS OF SNAPSHOT 5;
SELECT * FROM users AS OF TIMESTAMP 1719000000000000;
```

Reads resolve against the historical schema root; the live database is
untouched. `AS OF` combines with `WHERE`, `LIMIT`, and the rest of `SELECT`.

## Lifecycle

- By default **every mutation commits a snapshot** (threshold ≤ 0 means 1).
  `--snapshot-batch N` batches N mutations per snapshot instead.
- No snapshots are taken mid-transaction; one is committed with `COMMIT`.
- Inspect: `.snapshots` (chain), `.snapdiff <older> <newer>` (diff),
  `.snapshot_debug` (verbose dump).
- Restore: `.snapshot restore <id>` (rolls live state back),
  `.rollforward` (advances to the most recent snapshot).
- Tag: `.snapshot tag <id> <label>`.

## Reclamation

- `.expire [keep]` drops old snapshots (default keep: 20; invalid values
  warn and fall back to the default) and garbage-collects unreachable pages.
- Expiry never runs inside a transaction (warned no-op): uncommitted COW
  pages belong to no snapshot live set, so a sweep would free them from
  under the txn. `VACUUM` (`.vacuum`) is orthogonal — it repacks live
  tables (data and text indexes) without touching snapshot history.

Internals (chain/manifest/GC algorithm): [ARCH.md](../ARCH.md#snapshot-system).
