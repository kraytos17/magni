# Transactions

```sql
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 75.0);
COMMIT;   -- or ROLLBACK;
```

`.begin` / `.commit` / `.rollback` are the REPL equivalents.

## Model

- **Single writer.** The statement is parsed first, then the engine takes
  `db.mu` shared for reads (`SELECT` and read-only admin) or exclusive
  for writes (DDL/DML/transactions). Concurrent reads proceed; a write
  excludes everything.
- **Staged, not published.** In-txn DML stages new data *and* index roots
  instead of publishing them. The txn's own reads see staged content
  (including through secondary-index routing); `COMMIT` publishes once,
  `ROLLBACK` discards the staged maps and clears the catalog overlay.
- **One snapshot per commit.** No per-statement snapshots run mid-txn; the
  commit snapshot covers the whole transaction (see [snapshots](snapshots.md)).
- **DDL publishes immediately**, even inside a transaction — only DML
  stages. (Dropping a table in-txn clears its staged roots.)
- **Expiry never runs in-txn** (warned no-op); checkpoint still flushes WAL.

Isolation and lock-ordering details: [ARCH.md](../ARCH.md#transaction--concurrency-model)
and [concurrency.md](concurrency.md).
