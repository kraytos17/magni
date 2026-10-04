# Secondary Text Indexes

Single-column TEXT indexes, maintained on every write path and used to
route eligible `SELECT` filters. Scope is deliberately narrow;
anything outside it takes the full scan with identical results.

## DDL

```sql
CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT);
CREATE INDEX i_title ON docs (title);
CREATE INDEX i_body ON docs (body);
DROP INDEX i_body;
DROP INDEX i_title ON docs;
```

- Several indexes per table, one column each: names are unique per table
  (a repeat fails cleanly) and may repeat across tables. Unqualified
  `DROP INDEX <name>` needs a unique match; otherwise qualify with
  `ON <table>` (ambiguous names error out).
- Single TEXT column only: `CREATE INDEX i ON t (a, b)` is rejected at
  parse time.
- `DROP INDEX` clears the definition atomically. Data is untouched;
  routed shapes fall back to full scan with identical results (covering
  `SELECT rowid` / `SELECT <indexed-col>` are index-only, so they error
  without the index — select real columns instead). Index pages recycle
  through GC/vacuum, never freed eagerly (snapshots may still reference
  them).
- `NULL` values are never indexed; empty strings are.

## What routes

The router sees single-table `SELECT` filters on indexed columns (each
conjunct/disjunct may use a different index; candidates intersect for
AND, union for OR):

| Predicate | Route | Notes |
|---|---|---|
| `body = 'alpha'` | Index | TEXT literal only; `NULL`/non-TEXT fall through |
| `body LIKE 'al%'` | Index (prefix) | Canonical shape only: single trailing `%`, non-empty stem, no `%` or `_` in the stem |
| `body IN ('a', 'b')` | Index (union) | Literal lists up to 128 TEXT members; non-TEXT members match nothing; longer lists and subquery `IN` fall back |
| `body = 'x' AND v > 1` | Index on usable conjuncts | Full filter rechecks every candidate |
| `title = 't' AND body = 'b'` | Indexes intersect | One candidate set per index |
| `body = 'a' OR body = 'b'` | Indexes union | Every disjunct must be usable, else the union would be incomplete |
| `docs.body = 'x'` | Index | Table/alias-qualified names resolve the same |

Falling back (all correct via full scan): `NOT`, negated conditions,
nested boolean groups, column-to-column comparisons, `LIKE '%pha'`,
`'a%b%'`, `'a_c%'`, bare `'%'`, OR with an unusable disjunct, IN past the
cap, subquery `IN`.

## Covering projections

`SELECT rowid` (literally — one projected column, no aggregates/grouping,
and no user column named `rowid`, which keeps its existing meaning) is
answered from the index alone, without touching data pages. So is
`SELECT <indexed-col>` for Eq/In shapes (and Or thereof — the keys are
known); prefix plans need the row and stay on fetch. Wider projections
fetch each candidate row and recheck the full filter. Candidates arrive
in rowid order, so `LIMIT` without `ORDER BY` matches the scan path
exactly.

## EXPLAIN

`EXPLAIN` renders the router's decision from the same resolvers in the
same order, so it cannot disagree with execution:

```
EXPLAIN SELECT rowid FROM docs WHERE body = 'alpha';
-- INDEX SCAN ON docs USING body (eq, covering)

EXPLAIN SELECT body FROM docs WHERE body = 'alpha';
-- INDEX SCAN ON docs USING body (eq, covering)

EXPLAIN SELECT id FROM docs WHERE body LIKE 'al%';
-- INDEX SCAN ON docs USING body (prefix, fetch)

EXPLAIN SELECT id FROM docs WHERE id = 1;
-- PK SEEK ON docs

EXPLAIN SELECT id FROM docs WHERE body = 'a' OR body = 'b';
-- INDEX SCAN ON docs USING body (or, fetch)

EXPLAIN SELECT id FROM docs WHERE title = 't' AND body = 'b';
-- INDEX SCAN ON docs USING title, body (and, fetch)
```

Non-single-table statements keep the legacy echo of the inner SQL.

## Maintenance and visibility

- DML fan-out keeps every index coherent on insert/update/delete, in
  autocommit and in transactions (staged roots are visible to the txn's
  own reads; `ROLLBACK` discards them).
- `VACUUM` (`.vacuum`) rebuilds text indexes into packed pages alongside
  data. Rowids are logical, so this is space reclamation only.
- Reads get faster (50k-row on-disk A/B, release, interleaved): point
  lookup ≥20×, prefix fetch ~7×, 3-value `IN` ≥20× vs full scan (indexed
  point times sit at process-startup floor).
