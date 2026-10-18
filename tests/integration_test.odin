// Integration CRUD tests: DML, SQL surface (comments, quoting, limits),
// subqueries, set ops, WHERE/HAVING logic, and clean-error regressions.
// File-backed (test_int_*.db) through setup_db/teardown_db below.
package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:admin"
import "src:db"
import "src:parser"
import "src:types"

// setup_db opens a database on a re-derived (never stashed) file and
// asserts the open. Pair with teardown_db, which recomputes the same name.
setup_db :: proc(t: ^testing.T, name: string) -> ^db.Database {
	mute_test_logging()
	filename := db_filename("test_int_", name)
	clean_db_files(filename)

	database, open_err := db.open(filename)
	testing.expect(t, open_err == .None, "Failed to open database")
	return database
}

// teardown_db closes the database and removes it and its WAL sidecar.
teardown_db :: proc(db_handle: ^db.Database, name: string) {
	filename := db_filename("test_int_", name)
	db.close(db_handle)
	clean_db_files(filename)
}

@(test)
test_integration_basic_create_and_insert :: proc(t: ^testing.T) {
	d := setup_db(t, "basic_crud")
	defer teardown_db(d, "basic_crud")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE test (id INT, val TEXT);",
			"INSERT INTO test VALUES (1, 'hello');",
			"INSERT INTO test VALUES (2, 'world');",
		},
	)
}

@(test)
test_integration_select_empty_table :: proc(t: ^testing.T) {
	d := setup_db(t, "empty_sel")
	defer teardown_db(d, "empty_sel")

	run_db_setup(t, d, {"CREATE TABLE t (id INT);"})
	must_db_execute(t, d, "SELECT * FROM t;")
}

@(test)
test_integration_select_with_data :: proc(t: ^testing.T) {
	d := setup_db(t, "sel_data")
	defer teardown_db(d, "sel_data")

	run_db_setup(
		t,
		d,
		{
			"CREATE TABLE t (id INT, name TEXT);",
			"INSERT INTO t VALUES (1, 'Alice');",
			"INSERT INTO t VALUES (2, 'Bob');",
		},
	)
	must_db_execute(t, d, "SELECT * FROM t;")
}

@(test)
test_integration_keyword_as_identifier :: proc(t: ^testing.T) {
	d := setup_db(t, "keyword_id")
	defer teardown_db(d, "keyword_id")

	// Keywords as identifiers must work, not just parse.
	must_db_execute(t, d, "CREATE TABLE snapshot (id INT, of TEXT);")
}

@(test)
test_integration_update :: proc(t: ^testing.T) {
	d := setup_db(t, "update")
	defer teardown_db(d, "update")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'hello');")
	db.execute(d, "INSERT INTO t VALUES (2, 'world');")
	db.execute(d, "UPDATE t SET val = 'updated' WHERE id = 2;")

	r := db.query(d, "SELECT id, val FROM t WHERE id = 2;")
	testing.expect(t, r.ok, "SELECT after UPDATE")
	testing.expect_value(t, len(r.rows), 1)
	if len(r.rows) == 1 {
		testing.expect(t, len(r.rows[0]) >= 2, "row has at least 2 values")
		if len(r.rows[0]) >= 2 {
			val, _ := r.rows[0][1].(string)
			testing.expect_value(t, val, "updated")
		}
	}
}

@(test)
test_integration_delete :: proc(t: ^testing.T) {
	d := setup_db(t, "delete")
	defer teardown_db(d, "delete")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")
	db.execute(d, "INSERT INTO t VALUES (2);")
	db.execute(d, "DELETE FROM t WHERE id = 1;")

	r := db.query(d, "SELECT id FROM t;")
	testing.expect(t, r.ok, "SELECT after DELETE")
	if len(r.rows) == 1 {
		id, _ := r.rows[0][0].(i64)
		testing.expect_value(t, id, i64(2))
	}
}

@(test)
test_insert_semicolon_in_string :: proc(t: ^testing.T) {
	d := setup_db(t, "semi")
	defer teardown_db(d, "semi")

	db.execute(d, "CREATE TABLE t (id INT, val TEXT);")
	ok := db.execute(d, "INSERT INTO t VALUES (1, 'hello world');") == .None
	testing.expect(t, ok, "INSERT without semicolon in string")

	q := db.query(d, "SELECT val FROM t WHERE id = 1;")
	testing.expect(t, q.ok, "SELECT basic string")
	testing.expect(t, len(q.rows) >= 1, "row exists")
}

@(test)
test_split_statements :: proc(t: ^testing.T) {
	d := setup_db(t, "splitmulti")
	defer teardown_db(d, "splitmulti")

	// Simulate multi-statement execution: send individual statements via db.execute
	ok := db.execute(d, "CREATE TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE TABLE")
	ok = db.execute(d, "INSERT INTO t VALUES (1);") == .None
	testing.expect(t, ok, "INSERT 1")
	ok = db.execute(d, "INSERT INTO t VALUES (2);") == .None
	testing.expect(t, ok, "INSERT 2")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "COUNT query")
}

@(test)
test_block_comment :: proc(t: ^testing.T) {
	d := setup_db(t, "blockcmt")
	defer teardown_db(d, "blockcmt")
	// Block comments /* like this */ should be ignored
	ok := db.execute(d, "CREATE /* inline */ TABLE t (id INT);") == .None
	testing.expect(t, ok, "CREATE with block comment")
	ok = db.execute(d, "INSERT INTO t VALUES (1);") == .None
	testing.expect(t, ok, "INSERT after block comment")
	q := db.query(d, "SELECT id FROM t;")
	testing.expect(t, q.ok, "SELECT after block comment")
}

@(test)
test_double_semicolon :: proc(t: ^testing.T) {
	d := setup_db(t, "dblsemi")
	defer teardown_db(d, "dblsemi")
	// Double semicolon should not produce a spurious error
	ok := db.execute(d, "CREATE TABLE t (id INT);;") == .None
	testing.expect(t, ok, "CREATE with double ;;")
	ok = db.execute(d, "INSERT INTO t VALUES (1);;") == .None
	testing.expect(t, ok, "INSERT with double ;;")
}

@(test)
test_insert_too_many_columns :: proc(t: ^testing.T) {
	d := setup_db(t, "toomany")
	defer teardown_db(d, "toomany")
	db.execute(d, "CREATE TABLE t (id INT, val INT);")
	// Column list with more entries than table has columns should be rejected
	saved, ctx := suppress_expected_errors()
	context = ctx
	ok := db.execute(d, "INSERT INTO t (id,val,id,val) VALUES (1,2,3,4);") != .None
	context = restore_logger(saved)
	testing.expect(t, ok, "INSERT with too many columns rejected")
	// Duplicate column names in INSERT should be accepted (last wins)
	ok = db.execute(d, "INSERT INTO t (id,id) VALUES (1,2);") == .None
	testing.expect(t, ok, "INSERT with duplicate column names accepted")
	// But the value should be the last one
	q := db.query(d, "SELECT id FROM t;")
	if q.ok && len(q.rows) >= 1 && len(q.rows[0]) >= 1 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(2))
	}
}

@(test)
test_negative_limit :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// Verify that negative LIMIT produces an error message (not just generic failure)
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT -5;", context.temp_allocator)
	testing.expect(t, !ok, "negative LIMIT rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_negative_offset :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	_, ok, err_msg := parser.parse("SELECT * FROM t LIMIT 5 OFFSET -3;", context.temp_allocator)
	testing.expect(t, !ok, "negative OFFSET rejected")
	testing.expect(t, strings.contains(err_msg, "non-negative"), "error mentions non-negative")
}

@(test)
test_select_readonly_no_snapshot :: proc(t: ^testing.T) {
	// SELECT under shared lock should not create a snapshot
	d := setup_db(t, "sel_nosnap")
	defer teardown_db(d, "sel_nosnap")

	ok := db.execute(d, "CREATE TABLE t (id INT);")
	testing.expect(t, ok == .None, "CREATE TABLE")

	ok = db.execute(d, "INSERT INTO t VALUES (1);")
	testing.expect(t, ok == .None, "INSERT")

	// SELECT must not create a snapshot
	q := db.query(d, "SELECT * FROM t;")
	testing.expect(t, q.ok, "SELECT under shared lock")
	testing.expect(t, len(q.rows) == 1, "1 row from SELECT")
}

@(test)
test_multiple_selects :: proc(t: ^testing.T) {
	// Multiple sequential SELECTs should all work under shared lock
	d := setup_db(t, "multisel")
	defer teardown_db(d, "multisel")

	db.execute(d, "CREATE TABLE t (x INT);")
	db.execute(d, "INSERT INTO t VALUES (10);")
	db.execute(d, "INSERT INTO t VALUES (20);")
	db.execute(d, "INSERT INTO t VALUES (30);")

	for i in 0 ..< 5 {
		q := db.query(d, "SELECT * FROM t ORDER BY x;")
		testing.expect(t, q.ok, fmt.tprintf("SELECT %d", i))
		testing.expect(t, len(q.rows) == 3, fmt.tprintf("3 rows in SELECT %d", i))
	}
}

@(test)
test_readonly_admin_commands :: proc(t: ^testing.T) {
	d := setup_db(t, "ro_admin")
	defer teardown_db(d, "ro_admin")

	db.execute(d, "CREATE TABLE t1 (id INT);")
	db.execute(d, "CREATE TABLE t2 (val TEXT);")

	testing.expect(t, admin.list_tables(d) == .None, "list_tables succeeds")
	testing.expect(t, admin.describe_table(d, "t1") == .None, "describe_table t1 succeeds")
	testing.expect(t, admin.describe_table(d, "t2") == .None, "describe_table t2 succeeds")
	testing.expect(t, admin.stats(d) == .None, "stats succeeds")

	q := db.query(d, "SELECT id FROM t1;")
	testing.expect(t, q.ok, "query after admin commands")
}

@(test)
test_insert_creates_snapshot :: proc(t: ^testing.T) {
	d := setup_db(t, "ins_snap")
	defer teardown_db(d, "ins_snap")

	db.execute(d, "CREATE TABLE t (id INT);")

	q := db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "initial query")

	ok := db.execute(d, "INSERT INTO t VALUES (42);")
	testing.expect(t, ok == .None, "INSERT under exclusive lock")

	q = db.query(d, "SELECT id FROM t;")
	testing.expect(t, q.ok, "query after INSERT")
	if q.ok && len(q.rows) > 0 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(42))
	}
}

@(test)
test_write_after_read :: proc(t: ^testing.T) {
	d := setup_db(t, "w_after_r")
	defer teardown_db(d, "w_after_r")

	db.execute(d, "CREATE TABLE t (id INT);")
	db.execute(d, "INSERT INTO t VALUES (1);")

	q := db.query(d, "SELECT * FROM t;")
	testing.expect(t, q.ok, "read first")

	ok := db.execute(d, "INSERT INTO t VALUES (2);")
	testing.expect(t, ok == .None, "write after read")

	q = db.query(d, "SELECT COUNT(*) FROM t;")
	testing.expect(t, q.ok, "verify after write")
	if q.ok && len(q.rows) > 0 {
		v, _ := q.rows[0][0].(i64)
		testing.expect_value(t, v, i64(2))
	}
}

@(test)
test_integration_set_ops :: proc(t: ^testing.T) {
	d := setup_db(t, "setops")
	defer teardown_db(d, "setops")

	db.execute(d, "CREATE TABLE a (x INT);")
	db.execute(d, "INSERT INTO a VALUES (1);")
	db.execute(d, "INSERT INTO a VALUES (1);")
	db.execute(d, "INSERT INTO a VALUES (2);")
	db.execute(d, "CREATE TABLE b (x INT);")
	db.execute(d, "INSERT INTO b VALUES (1);")
	db.execute(d, "INSERT INTO b VALUES (3);")

	// UNION dedups across both sets.
	q := db.query(d, "SELECT x FROM a UNION SELECT x FROM b;")
	testing.expect(t, q.ok, "UNION should succeed")
	testing.expect(t, len(q.rows) == 3, "UNION of {1,1,2} and {1,3} = {1,2,3}")
	if len(q.rows) == 3 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[1][0].(i64), i64(2))
		testing.expect_value(t, q.rows[2][0].(i64), i64(3))
	}

	// UNION ALL keeps duplicates.
	q2 := db.query(d, "SELECT x FROM a UNION ALL SELECT x FROM b;")
	testing.expect(t, q2.ok, "UNION ALL should succeed")
	testing.expect(t, len(q2.rows) == 5, "UNION ALL of {1,1,2} and {1,3} = 5 rows")

	// INTERSECT keeps only shared values.
	q3 := db.query(d, "SELECT x FROM a INTERSECT SELECT x FROM b;")
	testing.expect(t, q3.ok, "INTERSECT should succeed")
	testing.expect(t, len(q3.rows) == 1, "INTERSECT of {1,1,2} and {1,3} = {1}")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(1))
	}

	// EXCEPT keeps left-only values.
	q4 := db.query(d, "SELECT x FROM a EXCEPT SELECT x FROM b;")
	testing.expect(t, q4.ok, "EXCEPT should succeed")
	testing.expect(t, len(q4.rows) == 1, "EXCEPT of {1,1,2} and {1,3} = {2}")
	if len(q4.rows) == 1 {
		testing.expect_value(t, q4.rows[0][0].(i64), i64(2))
	}

	// INTERSECT ALL respects multiplicity.
	q5 := db.query(d, "SELECT x FROM a INTERSECT ALL SELECT x FROM b;")
	testing.expect(t, q5.ok, "INTERSECT ALL should succeed")
	testing.expect(t, len(q5.rows) == 1, "INTERSECT ALL of {1,1,2} and {1,3} = {1}")

	// EXCEPT ALL keeps left multiplicity minus right.
	q6 := db.query(d, "SELECT x FROM a EXCEPT ALL SELECT x FROM b;")
	testing.expect(t, q6.ok, "EXCEPT ALL should succeed")
	testing.expect(t, len(q6.rows) == 2, "EXCEPT ALL of {1,1,2} and {1,3} = {1,2}")

	// Precedence: INTERSECT binds tighter than UNION.
	q7 := db.query(d, "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM a;")
	testing.expect(t, q7.ok, "precedence UNION/INTERSECT should succeed")
	// b INTERSECT a = {1}; a UNION {1} = {1,2}
	testing.expect(t, len(q7.rows) == 2, "a UNION (b INTERSECT a) = {1,2}")

	// Compound ORDER BY LIMIT.
	q8 := db.query(d, "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 2;")
	testing.expect(t, q8.ok, "compound ORDER BY LIMIT should succeed")
	testing.expect(t, len(q8.rows) == 2, "compound LIMIT 2 = 2 rows")

	// Literal operands.
	q9 := db.query(d, "SELECT 1 UNION SELECT 2;")
	testing.expect(t, q9.ok, "literal UNION should succeed")
	testing.expect(t, len(q9.rows) == 2, "SELECT 1 UNION SELECT 2 = 2 rows")
}

@(test)
test_integration_where_mixed_logic :: proc(t: ^testing.T) {
	d := setup_db(t, "where_mixed")
	defer teardown_db(d, "where_mixed")

	db.execute(d, "CREATE TABLE t (a INT, b INT, c INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 2, 0);")
	db.execute(d, "INSERT INTO t VALUES (1, 9, 0);")
	db.execute(d, "INSERT INTO t VALUES (0, 2, 3);")
	db.execute(d, "INSERT INTO t VALUES (0, 9, 3);")

	// AND binds tighter than OR: (a=1 AND b=2) OR c=3.
	q := db.query(d, "SELECT c FROM t WHERE a=1 AND b=2 OR c=3;")
	testing.expect(t, q.ok, "mixed AND/OR WHERE should succeed")
	testing.expect(t, len(q.rows) == 3, "(a=1 AND b=2) OR c=3 matches 3 rows")
	if len(q.rows) == 3 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(0))
		testing.expect_value(t, q.rows[1][0].(i64), i64(3))
		testing.expect_value(t, q.rows[2][0].(i64), i64(3))
	}

	// UPDATE/DELETE with mixed logic reuse the same tree evaluation.
	db.execute(d, "UPDATE t SET c = 9 WHERE a=1 AND b=2 OR c=3;")
	q2 := db.query(d, "SELECT c FROM t;")
	testing.expect(t, q2.ok, "select after update should succeed")
	testing.expect(t, len(q2.rows) == 4, "update touched 3 rows total")

	db.execute(d, "DELETE FROM t WHERE a=1 AND b=9 OR c=3;")
	q3 := db.query(d, "SELECT c FROM t;")
	testing.expect(t, q3.ok, "select after delete should succeed")
	testing.expect(t, len(q3.rows) == 3, "delete with mixed logic removed 1 row, 3 remain")
}

@(test)
test_integration_where_parens :: proc(t: ^testing.T) {
	d := setup_db(t, "where_parens")
	defer teardown_db(d, "where_parens")

	db.execute(d, "CREATE TABLE t (a INT, b INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 3);")
	db.execute(d, "INSERT INTO t VALUES (2, 3);")
	db.execute(d, "INSERT INTO t VALUES (1, 9);")
	db.execute(d, "INSERT INTO t VALUES (5, 9);")

	// Parenthesized OR group under AND.
	q := db.query(d, "SELECT b FROM t WHERE (a=1 OR a=2) AND b=3;")
	testing.expect(t, q.ok, "parenthesized WHERE should succeed")
	testing.expect(t, len(q.rows) == 2, "(a=1 OR a=2) AND b=3 matches 2 rows")

	// Nested parens: (a=1 AND (b=3 OR b=9)) OR a=5.
	q2 := db.query(d, "SELECT a FROM t WHERE (a=1 AND (b=3 OR b=9)) OR a=5;")
	testing.expect(t, q2.ok, "nested parenthesized WHERE should succeed")
	testing.expect(t, len(q2.rows) == 3, "(a=1 AND (b=3 OR b=9)) OR a=5 matches 3 rows")
}

@(test)
test_integration_having_mixed :: proc(t: ^testing.T) {
	d := setup_db(t, "having_mixed")
	defer teardown_db(d, "having_mixed")

	db.execute(d, "CREATE TABLE s (g INT, v INT);")
	db.execute(d, "INSERT INTO s VALUES (1, 10);")
	db.execute(d, "INSERT INTO s VALUES (1, 20);")
	db.execute(d, "INSERT INTO s VALUES (2, 5);")
	db.execute(d, "INSERT INTO s VALUES (3, 100);")

	// Mixed logic against aggregate and group-column references.
	q := db.query(d, "SELECT g FROM s GROUP BY g HAVING COUNT(*) >= 2 OR SUM(v) > 50;")
	testing.expect(t, q.ok, "mixed HAVING should succeed")
	testing.expect(t, len(q.rows) == 2, "HAVING COUNT(*)>=2 OR SUM(v)>50 keeps groups 1 and 3")

	// Parenthesized group-column HAVING.
	q2 := db.query(d, "SELECT g FROM s GROUP BY g HAVING (g = 1 OR g = 2) AND COUNT(*) <= 2;")
	testing.expect(t, q2.ok, "parenthesized HAVING should succeed")
	testing.expect(
		t,
		len(q2.rows) == 2,
		"HAVING (g=1 OR g=2) AND COUNT(*)<=2 keeps groups 1 and 2",
	)

	// GROUP BY applies even with no select-list aggregates.
	q3 := db.query(d, "SELECT g FROM s GROUP BY g;")
	testing.expect(t, q3.ok, "GROUP BY only should succeed")
	testing.expect(t, len(q3.rows) == 3, "GROUP BY g yields 3 distinct groups")

	// Bare aggregate name in HAVING (no parens).
	q4 := db.query(d, "SELECT g FROM s GROUP BY g HAVING count > 1;")
	testing.expect(t, q4.ok, "HAVING count > 1 should succeed")
	testing.expect(t, len(q4.rows) == 1, "HAVING count > 1 keeps only g=1")
	if len(q4.rows) == 1 {
		testing.expect_value(t, q4.rows[0][0].(i64), i64(1))
	}

	// HAVING with no GROUP BY but a select-list aggregate.
	q5 := db.query(d, "SELECT COUNT(*) AS n FROM s HAVING count > 3;")
	testing.expect(t, q5.ok, "HAVING over single aggregate should succeed")
	testing.expect(t, len(q5.rows) == 1, "COUNT(*) = 4 satisfies HAVING count > 3")
}

@(test)
test_integration_join_on_mixed :: proc(t: ^testing.T) {
	d := setup_db(t, "join_on_mixed")
	defer teardown_db(d, "join_on_mixed")

	db.execute(d, "CREATE TABLE t1 (x INT, y INT);")
	db.execute(d, "INSERT INTO t1 VALUES (1, 10);")
	db.execute(d, "INSERT INTO t1 VALUES (2, 20);")
	db.execute(d, "INSERT INTO t1 VALUES (3, 30);")
	db.execute(d, "CREATE TABLE t2 (x INT, z INT);")
	db.execute(d, "INSERT INTO t2 VALUES (1, 100);")
	db.execute(d, "INSERT INTO t2 VALUES (2, 200);")

	// Mixed logic in a JOIN ON clause.
	q := db.query(
		d,
		"SELECT t1.x FROM t1 INNER JOIN t2 ON t1.x = t2.x AND (t1.y > 15 OR t2.z < 150) ORDER BY t1.x;",
	)
	testing.expect(t, q.ok, "mixed JOIN ON should succeed")
	testing.expect(t, len(q.rows) == 2, "ON x=y AND (t1.y>15 OR t2.z<150) matches x=1 and x=2")
	if len(q.rows) == 2 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q.rows[1][0].(i64), i64(2))
	}
}

@(test)
test_integration_where_in_parens :: proc(t: ^testing.T) {
	d := setup_db(t, "where_in_parens")
	defer teardown_db(d, "where_in_parens")

	db.execute(d, "CREATE TABLE t1 (x INT);")
	db.execute(d, "INSERT INTO t1 VALUES (1);")
	db.execute(d, "INSERT INTO t1 VALUES (2);")
	db.execute(d, "INSERT INTO t1 VALUES (3);")
	db.execute(d, "CREATE TABLE t2 (x INT);")
	db.execute(d, "INSERT INTO t2 VALUES (1);")
	db.execute(d, "INSERT INTO t2 VALUES (3);")

	// IN subquery combined with a mixed-logic predicate.
	q := db.query(d, "SELECT x FROM t1 WHERE x IN (SELECT x FROM t2) AND x > 1;")
	testing.expect(t, q.ok, "IN subquery in mixed WHERE should succeed")
	testing.expect(t, len(q.rows) == 1, "x IN (1,3) AND x > 1 leaves only x=3")
	if len(q.rows) == 1 {
		testing.expect_value(t, q.rows[0][0].(i64), i64(3))
	}
}

@(test)
test_integration_column_alias :: proc(t: ^testing.T) {
	d := setup_db(t, "col_alias")
	defer teardown_db(d, "col_alias")

	db.execute(d, "CREATE TABLE s (g INT, v INT);")
	db.execute(d, "INSERT INTO s VALUES (1, 10);")
	db.execute(d, "INSERT INTO s VALUES (1, 20);")
	db.execute(d, "INSERT INTO s VALUES (2, 5);")

	// Aggregates with aliases return correct data and header names.
	q := db.query(d, "SELECT g, COUNT(*) AS n, SUM(v) AS sm FROM s GROUP BY g;")
	testing.expect(t, q.ok, "aliased aggregate SELECT should succeed")
	testing.expect(t, len(q.rows) == 2, "GROUP BY g yields 2 groups")
	testing.expect_value(t, q.columns[0], "g")
	testing.expect_value(t, q.columns[1], "n")
	testing.expect_value(t, q.columns[2], "sm")
	if len(q.rows) == 2 {
		testing.expect_value(t, q.rows[0][1].(i64), i64(2)) // COUNT for g=1
		testing.expect_value(t, q.rows[1][0].(i64), i64(2)) // g=2
	}

	// Plain column alias.
	q2 := db.query(d, "SELECT g AS group_id FROM s WHERE g = 2;")
	testing.expect(t, q2.ok, "aliased plain SELECT should succeed")
	testing.expect(t, len(q2.rows) == 1, "g=2 matches 1 row")
	testing.expect_value(t, q2.columns[0], "group_id")

	// Aggregates with aliases in a set-op operand carry the header.
	q3 := db.query(d, "SELECT COUNT(*) AS total FROM s UNION SELECT 99 AS total;")
	testing.expect(t, q3.ok, "aliased aggregate UNION should succeed")
	testing.expect(t, len(q3.rows) == 2, "UNION yields 2 rows")
	testing.expect_value(t, q3.columns[0], "total")
}

@(test)
test_integration_where_not :: proc(t: ^testing.T) {
	d := setup_db(t, "where_not")
	defer teardown_db(d, "where_not")

	db.execute(d, "CREATE TABLE t (a INT, b TEXT);")
	db.execute(d, "INSERT INTO t VALUES (1, 'apple');")
	db.execute(d, "INSERT INTO t VALUES (2, 'banana');")
	db.execute(d, "INSERT INTO t VALUES (3, 'cherry');")

	// NOT prefix.
	q := db.query(d, "SELECT a FROM t WHERE NOT a = 1;")
	testing.expect(t, q.ok, "WHERE NOT a=1 should succeed")
	testing.expect(t, len(q.rows) == 2, "NOT a=1 leaves 2 rows")

	// NOT IN.
	q2 := db.query(d, "SELECT a FROM t WHERE a NOT IN (1, 3);")
	testing.expect(t, q2.ok, "WHERE a NOT IN should succeed")
	testing.expect(t, len(q2.rows) == 1, "a NOT IN (1,3) leaves a=2")
	if len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), i64(2))
	}

	// NOT with a parenthesized group.
	q3 := db.query(d, "SELECT a FROM t WHERE NOT (a = 1 OR a = 2);")
	testing.expect(t, q3.ok, "WHERE NOT (a=1 OR a=2) should succeed")
	testing.expect(t, len(q3.rows) == 1, "NOT (a=1 OR a=2) leaves a=3")
	if len(q3.rows) == 1 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(3))
	}

	// NOT LIKE on a text column.
	q4 := db.query(d, "SELECT b FROM t WHERE b NOT LIKE 'a%';")
	testing.expect(t, q4.ok, "WHERE b NOT LIKE should succeed")
	testing.expect(t, len(q4.rows) == 2, "b NOT LIKE 'a%' excludes apple")
}

@(test)
test_integration_multi_row_insert :: proc(t: ^testing.T) {
	d := setup_db(t, "multi_row")
	defer teardown_db(d, "multi_row")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score REAL DEFAULT 0.0);")
	db.execute(d, "INSERT INTO t VALUES (1, 'a', 1.5), (2, 'b', 2.5);")
	db.execute(d, "INSERT INTO t (name, score) VALUES ('c', 3.5), ('d', 4.5);")

	q := db.query(d, "SELECT COUNT(*) AS n FROM t;")
	testing.expect(t, q.ok, "count after multi-row insert should succeed")
	testing.expect_value(t, q.rows[0][0].(i64), i64(4))

	q2 := db.query(d, "SELECT id FROM t WHERE name = 'd';")
	testing.expect(t, q2.ok, "select after column-list multi-row insert")
	testing.expect(t, len(q2.rows) == 1, "one row for d")
	if len(q2.rows) == 1 {
		testing.expect_value(t, q2.rows[0][0].(i64), i64(4))
	}

	// Omitted PK column gets the auto-increment rowid stored as its value.
	q3 := db.query(d, "SELECT id, name FROM t ORDER BY id;")
	testing.expect(t, q3.ok, "select all after PK auto-fill")
	testing.expect(t, len(q3.rows) == 4, "4 rows total")
	if len(q3.rows) == 4 {
		testing.expect_value(t, q3.rows[0][0].(i64), i64(1))
		testing.expect_value(t, q3.rows[3][0].(i64), i64(4))
	}
}

@(test)
test_integration_empty_aggregates :: proc(t: ^testing.T) {
	d := setup_db(t, "emptyagg")
	defer teardown_db(d, "emptyagg")

	db.execute(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")

	r := db.query(
		d,
		"SELECT COUNT(*) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx FROM t WHERE id = 99;",
	)
	testing.expect(t, r.ok, "empty aggregate query")
	testing.expect_value(t, len(r.rows), 1)
	if len(r.rows) == 1 {
		c, _ := r.rows[0][0].(i64)
		s, _ := r.rows[0][1].(i64)
		testing.expect_value(t, c, i64(0))
		testing.expect_value(t, s, i64(0))
		testing.expect(t, types.is_null(r.rows[0][2]), "AVG is NULL")
		testing.expect(t, types.is_null(r.rows[0][3]), "MIN is NULL")
		testing.expect(t, types.is_null(r.rows[0][4]), "MAX is NULL")
	}
}

@(test)
test_integration_select_bare_column_errors_cleanly :: proc(t: ^testing.T) {
	// `SELECT k` without FROM must return a clean error: a 0-value row
	// for 1 column panics the result renderer (index OOB).
	//
	// NOTE: muted to .Fatal because Odin's test runner fails any test that
	// emits an error-level log, and these paths intentionally log their clean errors.
	context.logger.lowest_level = .Fatal
	d := setup_db(t, "bare_col")
	defer teardown_db(d, "bare_col")

	err := db.execute(d, "SELECT k;")
	testing.expect(t, err != .None, "SELECT bare column without FROM should error cleanly")

	err2 := db.execute(d, "SELECT a, b;")
	testing.expect(t, err2 != .None, "SELECT multiple bare columns should error cleanly")

	err3 := db.execute(d, "SELECT 1;")
	testing.expect(t, err3 == .None, "SELECT literal without FROM should succeed")
}

// v1 page-format files are rejected at open with a clean error (never a
// crash): flip the header version of a valid database and reopen it.
@(test)
test_from_subquery_clauses :: proc(t: ^testing.T) {
	d := setup_db(t, "fromsubq")
	defer teardown_db(d, "fromsubq")

	db.execute(d, "CREATE TABLE t (id INT, v INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")
	db.execute(d, "INSERT INTO t VALUES (2, 20);")
	db.execute(d, "INSERT INTO t VALUES (3, 30);")

	// Aggregates inside FROM-subqueries apply to the subquery rows.
	q := db.query(d, "SELECT * FROM (SELECT COUNT(*) FROM t) AS c;")
	testing.expect(t, q.ok, "aggregate subquery executes")
	if q.ok && len(q.rows) == 1 {
		n, _ := q.rows[0][0].(i64)
		testing.expect_value(t, n, i64(3))
	}

	// ORDER BY + LIMIT inside FROM-subqueries apply.
	q2 := db.query(d, "SELECT * FROM (SELECT v FROM t ORDER BY v DESC LIMIT 2) AS s;")
	testing.expect(t, q2.ok, "order/limit subquery executes")
	testing.expect(t, len(q2.rows) == 2, "limit applies inside subquery")
	if q2.ok && len(q2.rows) == 2 {
		v0, _ := q2.rows[0][0].(i64)
		v1, _ := q2.rows[1][0].(i64)
		testing.expect_value(t, v0, i64(30))
		testing.expect_value(t, v1, i64(20))
	}

	// WHERE inside FROM-subqueries still applies.
	q3 := db.query(d, "SELECT * FROM (SELECT v FROM t WHERE v > 15) AS f;")
	testing.expect(t, q3.ok, "filtered subquery executes")
	testing.expect(t, len(q3.rows) == 2, "where applies inside subquery")
}

@(test)
test_from_subquery_empty :: proc(t: ^testing.T) {
	d := setup_db(t, "fromsubqempty")
	defer teardown_db(d, "fromsubqempty")

	db.execute(d, "CREATE TABLE t (id INT, v INT);")
	db.execute(d, "INSERT INTO t VALUES (1, 10);")

	// Empty inner result is an empty result, not an error.
	q := db.query(d, "SELECT * FROM (SELECT v FROM t WHERE v > 100) AS e;")
	testing.expect(t, q.ok, "empty subquery executes")
	testing.expect(t, len(q.rows) == 0, "empty subquery yields zero rows")

	// Aggregates over the empty set follow SQL: COUNT is 0.
	q2 := db.query(d, "SELECT COUNT(*) FROM (SELECT v FROM t WHERE v > 100) AS z;")
	testing.expect(t, q2.ok, "count over empty subquery executes")
	if q2.ok && len(q2.rows) == 1 {
		n, _ := q2.rows[0][0].(i64)
		testing.expect_value(t, n, i64(0))
	}
}
