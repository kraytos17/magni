# magni

A mini SQLite clone written in [Odin](https://odin-lang.org). Implements a custom B-tree storage engine, SQL parser, and executor.

## Features

### SQL Statements
| Statement | Example |
|---|---|
| CREATE TABLE | `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0);` |
| INSERT | `INSERT INTO users VALUES (1, 'Alice', 99.5);` |
| INSERT (column list) | `INSERT INTO users (name, score, id) VALUES ('Bob', 88.0, 2);` |
| SELECT | `SELECT * FROM users;` / `SELECT name, score FROM users;` |
| SELECT with WHERE | `SELECT * FROM users WHERE score > 50 AND name != 'Bob';` |
| SELECT ORDER BY | `SELECT * FROM users ORDER BY score DESC;` |
| SELECT LIMIT/OFFSET | `SELECT * FROM users LIMIT 5 OFFSET 10;` |
| SELECT aggregates | `SELECT COUNT(*), AVG(score) FROM users;` |
| SELECT GROUP BY | `SELECT name, COUNT(*) FROM users GROUP BY name HAVING count > 1;` |
| SELECT JOIN | `SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y;` |
| SELECT LEFT JOIN | `SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.y;` |
| SELECT subquery | `SELECT * FROM (SELECT * FROM t WHERE x > 1) AS sub;` |
| SELECT qualified cols | `SELECT t1.x, t2.y FROM t1, t2 WHERE t1.x = t2.y;` |
| Table alias | `SELECT a.x FROM t AS a;` |
| UPDATE | `UPDATE users SET score = 100.0 WHERE id = 1;` |
| DELETE | `DELETE FROM users WHERE id = 1;` / `DELETE FROM users;` |
| DROP TABLE | `DROP TABLE users;` |

### WHERE Clause
- Operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- `LIKE` pattern matching (`%` = any sequence, `_` = single char)
- Multiple conditions with `AND` or `OR` (uniform only)
- PK equality fast-path (direct B-tree lookup)

### Column Types & Constraints
- Types: `INTEGER`, `REAL`, `TEXT`, `BLOB` (+ `INT` alias)
- Constraints: `PRIMARY KEY` (single), `NOT NULL`, `DEFAULT` value
- BLOB literals: `X'DEADBEEF'` / `x'cafe'`

### Aggregate Functions
- `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`

### JOIN Types
- `CROSS JOIN`, `INNER JOIN ... ON`, `LEFT [OUTER] JOIN ... ON`
- Implicit cross join with comma: `FROM t1, t2`
- Equi-join: `ON t1.x = t2.y`

### Subqueries
- `FROM (SELECT ...) AS alias`
- Supports WHERE, ORDER BY, LIMIT inside subqueries

### Storage Engine
- Custom B-tree implementation with leaf/interior nodes, splitting, and page management
- SQLite-compatible serial type system (varint, record format)
- 4 KB pages with LRU page cache
- Auto-incrementing row IDs for tables without explicit PK
- Schema stored in a B-tree on page 1

### CLI (REPL)
```bash
# Interactive mode
./build/magni [database]

# Execute SQL from stdin (pipe/redirect)
echo "SELECT * FROM t;" | ./build/magni mydb.db

# Execute SQL from file
./build/magni --file script.sql mydb.db

# Execute single statement
./build/magni --eval "SELECT * FROM t;" mydb.db
```

Default database is `test.db`. The REPL supports multi-line input (terminated by `;`).

Dot-commands: `.exit`, `.quit`, `.help`, `.tables`, `.schema`, `.desc`, `.dump`, `.stats`, `.integrity`, `.checkpoint`, `.debug_schema`

## Quick Start

```bash
# Build and run
make run

# Or manually
odin build src/ -collection:src=src
./build/magni

# Run tests
make test
# or
odin test tests/ -collection:src=src
```

## Project Structure

```
src/
├── main.odin          # CLI entry point, REPL, batch mode
├── btree/             # B-tree engine (tree, headers, cursor)
├── cell/              # Row serialization & validation
├── db/                # Database handle & top-level API
├── executor/          # SQL statement execution
├── parser/            # SQL lexer & parser
├── pager/             # Page cache & file I/O
├── schema/            # Table metadata management
├── types/             # Core type system & values
└── utils/             # Varint, endian, serial types
tests/                 # 152 tests across 6 test files
```

## Test Coverage

| Test File | Tests | Area |
|---|---|---|
| `parser_test.odin` | 38 | SQL parsing, literals, column-list INSERT, BLOBs, JOINs, subqueries |
| `executor_test.odin` | 64 | Full end-to-end execution, constraints, DML, aggregates, JOINs |
| `btree_test.odin` | 17 | B-tree insert/find/delete, splitting, cursor |
| `cell_test.odin` | 11 | Serialization roundtrip, validation |
| `pager_test.odin` | 12 | Page cache, I/O, eviction |
| `schema_test.odin` | 10 | Metadata persistence, column validation |
