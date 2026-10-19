# Secondary Indexes

Single-column TEXT, INTEGER, and REAL indexes, maintained on every write
path and used to route eligible `SELECT` filters. Scope is deliberately
narrow; anything outside it takes the full scan with identical results.

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
- One or more columns of indexable type (TEXT, INTEGER, REAL — BLOB
  fails cleanly, naming the column). One index kind per column type
  (TEXT keys sort binary, INTEGER keys numeric, REAL keys numeric with
  -0.0 normalized to +0.0); cross-class literals never match, so they
  never route. Integers stored in a REAL column index under their
  widened key (REAL affinity); covering projections normalize them to
  REAL while fetch preserves classes.
- Composite `CREATE INDEX i ON t (a, b)` indexes the tuple: components
  sort in order, then rowid; a NULL anywhere skips the tuple. Eq on all
  columns answers exact; Eq on the leading column answers a prefix scan;
  leading-column ranges answer bounded scans (tightened pairs, Eq
  dominating); OR of ANDs unions exact composite scans; leading IN with
  bound trailing Eqs expands to a union of full lookups (bare leading
  IN unions prefix scans); later gaps ride
  the fetch recheck. Single leading-Eq composite plans cover bound
  columns (decoded with column types); wider, unbound, and ranged
  projections fetch. `SELECT rowid` still covers.
- `DROP INDEX` clears the definition atomically. Data is untouched;
  routed shapes fall back to full scan with identical results (covering
  `SELECT rowid` / `SELECT <indexed-col>` answer from data pages instead
  of the index — select real columns instead). Index pages recycle
  through GC/vacuum, never freed eagerly (snapshots may still reference
  them).
- `NULL` values are never indexed; empty strings are.

## What routes

The router sees single-table `SELECT` filters on indexed columns (each
conjunct/disjunct may use a different index; candidates intersect for
AND, union for OR):

| Predicate | Route | Notes |
|---|---|---|
| `body = 'alpha'` | Index | TEXT literal on a TEXT index only; `NULL`/cross-class fall through |
| `v = 42` | Index | INTEGER literal on an INTEGER index only |
| `body LIKE 'al%'` | Index (prefix) | Canonical shape only: single trailing `%`, non-empty stem, no `%` or `_` in the stem; TEXT indexes only (`LIKE` on integers stays on the scan path with identical results) |
| `body IN ('a', 'b')` | Index (union) | Literal lists up to 128 members of the index's type; other members match nothing; longer lists and subquery `IN` fall back |
| `v IN (1, 2)` | Index (union) | Same cap and semantics on INTEGER indexes |
| `r = 1.5` | Index | REAL literal on a REAL index only (`-0.0` matches `+0.0`) |
| `r IN (1.5, 2.5)` | Index (union) | Same cap and semantics on REAL indexes |
| `r > 1.5`, `r BETWEEN 1.0 AND 2.0` | Index (range) | Same merge semantics on REAL indexes; `LIKE` on REAL stays on scan |
| `v > 42`, `v >= 42`, `v < 42`, `v <= 42` | Index (range) | Open-ended bounds in the column's type; fetch-only (keys unknown) |
| `v BETWEEN 1 AND 9` | Index (range) | Desugared to `>= AND <=`; same-column ANDs merge to one bounded scan |
| `v > 9 AND v < 1` | Index (range, empty) | Contradictions collapse to an empty plan with zero I/O |
| `v = 42 AND v > 1` | Index | The Eq dominates: checked against the bound, then answered by lookup |
| `body = 'x' AND v > 1` | Index on usable conjuncts | Full filter rechecks every candidate |
| `title = 't' AND body = 'b'` | Indexes intersect | One candidate set per index (each member routed on its own index and type) |
| `body = 'a' OR body = 'b'` | Indexes union | Every disjunct must be usable, else the union would be incomplete |
| `docs.body = 'x'` | Index | Table/alias-qualified names resolve the same |

Falling back (all correct via full scan): `NOT`, negated conditions,
nested boolean groups, column-to-column comparisons, `!=`, cross-class
ordered comparisons that need rank order (`b > 3` matches every text —
TEXT ranks above INTEGER), `LIKE '%pha'`,
`'a%b%'`, `'a_c%'`, bare `'%'`, OR with an unusable disjunct, IN past the
cap, subquery `IN`.

## Covering projections

`SELECT rowid` (literally — one projected column, no aggregates/grouping,
and no user column named `rowid`, which keeps its existing meaning) is
answered from the index alone, without touching data pages. A bare
`rowid` also projects generally in any position, with `ORDER BY rowid`,
on tables with no index at all. So is
`SELECT <indexed-col>` for Eq/In shapes (and Or thereof — the keys are
known), in the column's type; prefix and range plans need the row and stay
on fetch. A covering projection must name the routed column, and every OR
member must share it — otherwise the query fetches (emitting another
column's keys as values would be wrong). Wider projections
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

EXPLAIN SELECT v FROM nums WHERE v = 42;
-- INDEX SCAN ON nums USING v (eq, covering)

EXPLAIN SELECT id FROM docs WHERE title = 't' AND body = 'b';
-- INDEX SCAN ON docs USING title, body (and, fetch)

EXPLAIN SELECT v FROM nums WHERE v > 40;
-- INDEX SCAN ON nums USING v (range, fetch)

EXPLAIN SELECT v FROM nums WHERE v BETWEEN 1 AND 9;
-- INDEX SCAN ON nums USING v (range, fetch)

EXPLAIN SELECT x FROM reals WHERE x = 1.5;
-- INDEX SCAN ON reals USING x (eq, covering)

EXPLAIN SELECT a, b FROM t WHERE a = 10 AND b = 'x';
-- INDEX SCAN ON t USING a,b (composite, fetch)
```

Non-single-table statements keep the legacy echo of the inner SQL.

## Maintenance and visibility

- DML fan-out keeps every index coherent on insert/update/delete, in
  autocommit and in transactions (staged roots are visible to the txn's
  own reads; `ROLLBACK` discards them).
- `VACUUM` (`.vacuum`) rebuilds indexes into packed pages alongside
  data, preserving each index's kind. Rowids are logical, so this is
  space reclamation only.
- Reads get faster (50k-row on-disk A/B, release, interleaved): point
  lookup ≥20×, prefix fetch ~7×, 3-value `IN` ≥20× vs full scan (indexed
  point times sit at process-startup floor).
