# Secondary Text Index

One single-column TEXT index per table, maintained on every write path and
used to route eligible `SELECT` filters. Scope is deliberately narrow (V3.0);
anything outside it takes the full scan with identical results.

## DDL

```sql
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, v INT);
CREATE INDEX i_body ON docs (body);
```

- One index per table: a second `CREATE INDEX` fails cleanly
  (`Table already has an index: d`).
- Single TEXT column only: `CREATE INDEX i ON t (a, b)` is rejected at
  parse time, as is `DROP INDEX` (no such statement in V3.0).
- `NULL` values are never indexed; empty strings are.

## What routes

The router sees single-table `SELECT` filters on the indexed column:

| Predicate | Route | Notes |
|---|---|---|
| `body = 'alpha'` | Index | TEXT literal only; `NULL`/non-TEXT fall through |
| `body LIKE 'al%'` | Index (prefix) | Canonical shape only: single trailing `%`, non-empty stem, no `%` or `_` in the stem |
| `body IN ('a', 'b')` | Index (union) | Literal lists; non-TEXT members match nothing; subquery `IN` falls back |
| `body = 'x' AND v > 1` | Index on first usable conjunct | Full filter rechecks every candidate |
| `docs.body = 'x'` | Index | Table/alias-qualified names resolve the same |

Falling back (all correct via full scan): `OR`, `NOT`, negated conditions,
nested boolean groups, column-to-column comparisons, `LIKE '%pha'`,
`'a%b%'`, `'a_c%'`, bare `'%'`, subquery `IN`.

## Covering `SELECT rowid`

Literally `SELECT rowid` (one projected column, no aggregates/grouping,
and no user column named `rowid` — which keeps its existing meaning) is
answered from the index alone, without touching data pages. Wider
projections fetch each candidate row and recheck the full filter.
Candidates arrive in rowid order, so `LIMIT` without `ORDER BY` matches
the scan path exactly.

## EXPLAIN

`EXPLAIN` renders the router's decision from the same resolvers in the
same order, so it cannot disagree with execution:

```
EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';
-- INDEX SCAN ON docs USING body (eq, covering)

EXPLAIN SELECT id FROM docs WHERE body LIKE 'al%';
-- INDEX SCAN ON docs USING body (prefix, fetch)

EXPLAIN SELECT id FROM docs WHERE id = 1;
-- PK SEEK ON docs

EXPLAIN SELECT id FROM docs WHERE body = 'a' OR body = 'b';
-- FULL SCAN ON docs
```

Non-single-table statements keep the legacy echo of the inner SQL.

## Maintenance and visibility

- DML fan-out keeps the index coherent on insert/update/delete, in
  autocommit and in transactions (staged roots are visible to the txn's
  own reads; `ROLLBACK` discards them).
- `VACUUM` (`.vacuum`) rebuilds text indexes into packed pages alongside
  data. Rowids are logical, so this is space reclamation only.
- Reads get faster (50k-row on-disk A/B, release, interleaved): point
  lookup ≥20×, prefix fetch ~7×, 3-value `IN` ≥20× vs full scan (indexed
  point times sit at process-startup floor).
