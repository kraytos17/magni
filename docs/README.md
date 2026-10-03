# Docs

Guide-level documentation. Internals reference lives in [ARCH.md](../ARCH.md);
fuzz workflows in [fuzz/README.md](../fuzz/README.md).

| File | Contents |
|---|---|
| [testing.md](testing.md) | Gates, test layout, and the conventions every test upholds |
| [indexing.md](indexing.md) | Secondary text index: DDL, routable shapes, covering reads, EXPLAIN, maintenance |
| [snapshots.md](snapshots.md) | Time travel, snapshot lifecycle, restore, expiry, reclamation |
| [transactions.md](transactions.md) | Txn model: single writer, staged roots, commit/rollback, DDL behavior |
| [concurrency.md](concurrency.md) | Lock model and acquisition order |
| [build.md](build.md) | `target/` layout, profiles, `make`↔`magni.py` map, toolchain |
