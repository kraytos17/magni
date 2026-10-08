package tests

import "core:fmt"
import "core:strings"
import "core:testing"
import "src:btree"
import "src:db"
import "src:pager"
import "src:schema"
import "src:types"

setup_schema_env :: proc(t: ^testing.T, test_name: string) -> (btree.Tree, string) {
	context.logger.lowest_level = .Error
	filename := fmt.tprintf("test_schema_%s.db", test_name)
	safe_filename, _ := strings.clone(filename, context.allocator)
	clean_db_files(safe_filename)

	p, err := pager.open(safe_filename)
	testing.expect(t, err == nil, "Failed to open pager")

	schema_page, aerr := pager.allocate_page(p)
	testing.expect(t, aerr == .None, "Failed to allocate schema page")
	// Schema roots are slotdir post-flip (mirrors production creators).
	if !btree.init_slot_leaf_page(schema_page.data, schema_page.page_num) {
		testing.fail_now(t, "Failed to init slotdir schema page")
	}
	pager.mark_dirty(p, schema_page.page_num)
	pager.unpin_page(p, schema_page.page_num)

	tree := btree.init(p, schema_page.page_num)
	ok := schema.init(&tree)
	testing.expect(t, ok, "Failed to init schema B-Tree")
	return tree, safe_filename
}

teardown_schema_env :: proc(tree: btree.Tree, filename: string) {
	_ = pager.close(tree.pager)
	clean_db_files(filename)
	delete(filename, context.allocator)
}

@(test)
test_column_blob_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	cols := []types.Column {
		{name = "id", type = .INTEGER, pk = true, not_null = true},
		{name = "username", type = .TEXT, pk = false, not_null = true},
		{name = "score", type = .REAL, pk = false, not_null = false},
	}

	blob := schema.serialize_columns_to_blob(cols, context.temp_allocator)
	testing.expect(t, len(blob) > 4, "Blob too small")

	restored := schema.deserialize_columns(blob, context.temp_allocator)
	testing.expect_value(t, len(restored), 3)

	testing.expect_value(t, restored[0].name, "id")
	testing.expect_value(t, restored[0].pk, true)

	testing.expect_value(t, restored[1].name, "username")
	testing.expect_value(t, restored[1].type, types.Column_Type.TEXT)

	testing.expect_value(t, restored[2].name, "score")
	testing.expect_value(t, restored[2].not_null, false)
}

@(test)
test_add_and_find_table :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "basic_ops")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	root_page := u32(2)
	sql := "CREATE TABLE users (id INT)"

	added := schema.add_table(&tree, "users", cols, root_page, sql)
	testing.expect(t, added, "schema.add_table failed")

	tbl, found := schema.find_table(&tree, "users", context.temp_allocator)
	testing.expect(t, found, "Table 'users' not found after insertion")

	testing.expect_value(t, tbl.name, "users")
	testing.expect_value(t, tbl.root_page, root_page)
	testing.expect_value(t, tbl.sql, sql)
	testing.expect_value(t, len(tbl.columns), 1)
}

@(test)
test_table_persistence :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "persistence")
	schema_root := tree.root
	cols := []types.Column{{name = "x", type = .INTEGER}}

	ok := schema.add_table(&tree, "persistent", cols, 99, "")
	testing.expect(t, ok, "add_table failed in persistence test")
	_ = pager.close(tree.pager)

	p2, _ := pager.open(file)
	tree2 := btree.init(p2, schema_root)
	defer teardown_schema_env(tree2, file)

	exists := schema.table_exists(&tree2, "persistent")
	testing.expect(t, exists, "Table lost after reload")
}

@(test)
test_list_tables :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "list")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "a", type = .INTEGER}}
	schema.add_table(&tree, "t1", cols, 2, "")
	schema.add_table(&tree, "t2", cols, 3, "")
	schema.add_table(&tree, "t3", cols, 4, "")

	tables := schema.list_tables(&tree, context.temp_allocator)
	testing.expect_value(t, len(tables), 3)
	found_count := 0
	for tbl in tables {
		if tbl.name == "t1" || tbl.name == "t2" || tbl.name == "t3" {
			found_count += 1
		}
	}
	testing.expect_value(t, found_count, 3)
}

@(test)
test_drop_table :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "drop")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	schema.add_table(&tree, "to_delete", cols, 10, "")
	testing.expect(t, schema.table_exists(&tree, "to_delete"), "Pre-condition failed")

	dropped := schema.drop_table(&tree, "to_delete")
	testing.expect(t, dropped, "drop_table returned false")
	testing.expect(t, !schema.table_exists(&tree, "to_delete"), "Table still exists after drop")
}

@(test)
test_column_validation :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	c1 := []types.Column{{name = "ok", type = .INTEGER}}
	ok1, _ := schema.validate_columns(c1)
	testing.expect(t, ok1, "Valid column failed")

	c2 := []types.Column{}
	ok2, msg2 := schema.validate_columns(c2)
	testing.expect(t, !ok2, "Empty columns allowed")
	testing.expect(t, strings.contains(msg2, "at least one"), "Wrong error message")

	c3 := []types.Column{{name = "dup", type = .INTEGER}, {name = "dup", type = .TEXT}}
	ok3, msg3 := schema.validate_columns(c3)
	testing.expect(t, !ok3, "Duplicate columns allowed")
	testing.expect(t, strings.contains(msg3, "Duplicate"), "Wrong error message")
}

@(test)
test_get_table_deep_copy :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "deep_copy")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "data", type = .BLOB}}
	schema.add_table(&tree, "deep", cols, 50, "")

	tbl, found := schema.get_table(&tree, "deep", context.allocator)
	testing.expect(t, found, "Table not found")
	defer schema.table_free(tbl, context.allocator)

	free_all(context.temp_allocator)
	testing.expect_value(t, tbl.name, "deep")
	testing.expect_value(t, tbl.columns[0].name, "data")
}

@(test)
test_find_nonexistent_table :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "find_nonexist")
	defer teardown_schema_env(tree, file)

	_, found := schema.find_table(&tree, "ghost", context.temp_allocator)
	testing.expect(t, !found, "find_table should return false for non-existent table")
}

@(test)
test_drop_nonexistent_table :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "drop_nonexist")
	defer teardown_schema_env(tree, file)

	dropped := schema.drop_table(&tree, "ghost")
	testing.expect(t, !dropped, "drop_table should return false for non-existent table")
}

@(test)
test_duplicate_table_name :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "dup_name")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}
	ok1 := schema.add_table(&tree, "dup", cols, 2, "")
	testing.expect(t, ok1, "First add should succeed")

	saved, ctx := suppress_expected_errors()
	context = ctx
	ok2 := schema.add_table(&tree, "dup", cols, 3, "")
	context = restore_logger(saved)
	testing.expect(t, !ok2, "Duplicate add should fail")
}

@(test)
test_schema_hash_collision :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "hashcol")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "id", type = .INTEGER}}

	// Normal add succeeds
	ok1 := schema.add_table(&tree, "mytable", cols, 42, "")
	testing.expect(t, ok1, "First add succeeds")

	// Same name again fails (duplicate detection)
	saved, ctx := suppress_expected_errors()
	context = ctx
	ok_dup := schema.add_table(&tree, "mytable", cols, 99, "")
	context = restore_logger(saved)
	testing.expect(t, !ok_dup, "Duplicate name rejected")

	// Force a hash collision: manually insert a row at "mytable"'s hash with a different name.
	// This simulates what happens if two different table names hash to the same value.
	target_hash := types.Row_ID(types.hash("mytable"))
	collision_vals := []types.Value {
		types.value(0),
		types.value("intruder"),
		types.value(999),
		types.value(""),
		types.value([]u8{}),
		types.value(0),
	}

	testing.expect(
		t,
		btree.tree_delete(&tree, target_hash) == .None,
		"delete colliding key succeeds",
	)
	testing.expect(
		t,
		btree.tree_insert(&tree, target_hash, collision_vals) == .None,
		"collision insert succeeds",
	)
	// The original "mytable" row was overwritten by the collision row (same hash key).
	// get_table("mytable") should fail because the row at that hash now has name "intruder".
	_, found_mytable := schema.get_table(&tree, "mytable")
	testing.expect(t, !found_mytable, "mytable overwritten by collision row")

	// The collision row can be found by reading the schema tree at the hash key directly.
	// It has name "intruder" but lives at hash("mytable"), so get_table("intruder") won't find it.
	_, found_intruder := schema.get_table(&tree, "intruder")
	testing.expect(t, !found_intruder, "intruder not accessible by name (lives at different hash)")

	// Verify the collision row is physically present at the hash key
	c, find_err := btree.tree_find(&tree, target_hash, context.temp_allocator)
	testing.expect(t, find_err == .None, "row exists at target hash")
	if find_err == .None {
		stored_name, _ := c.values[1].(string)
		testing.expect_value(t, stored_name, "intruder")
	}
}

@(test)
test_schema_row_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	r := schema.Schema_Row {
		kind         = "table",
		name         = "test_tbl",
		root_page    = 42,
		sql          = "CREATE TABLE test_tbl (id INT)",
		columns_blob = []u8{1, 2, 3, 4},
		skip_root    = 7,
	}

	// New format with skip_root: produces 6 values (kind byte + 5 fields)
	values := schema.schema_row_to_values(r)
	testing.expect(t, len(values) == 6, "skip_root>0 produces 6 values")

	r2, ok := schema.schema_row_from_values(values)
	testing.expect(t, ok, "new format skip>0 round-trip")
	testing.expect_value(t, r2.kind, r.kind)
	testing.expect_value(t, r2.name, r.name)
	testing.expect_value(t, r2.root_page, r.root_page)
	testing.expect_value(t, r2.sql, r.sql)
	testing.expect_value(t, r2.skip_root, r.skip_root)

	// New format without skip_root: produces 5 values
	r0 := r
	r0.skip_root = 0
	values5 := schema.schema_row_to_values(r0)
	testing.expect(t, len(values5) == 5, "skip_root=0 produces 5 values")

	r5, ok5 := schema.schema_row_from_values(values5)
	testing.expect(t, ok5, "5-value format accepted")
	testing.expect_value(t, r5.skip_root, u32(0))
	testing.expect_value(t, r5.name, "test_tbl")
	testing.expect_value(t, r5.root_page, u32(42))

	// values[0] is i64(0) for kind=table
	v0, is_int := values[0].(i64)
	testing.expect(t, is_int && v0 == 0, "values[0] is i64(0)")

	// Invalid: too few values
	_, bad := schema.schema_row_from_values(
		[]types.Value{types.value(0), types.value("x")},
	)
	testing.expect(t, !bad, "<5 values rejected")

	// Invalid: wrong type at values[0]
	_, bad2 := schema.schema_row_from_values(
		[]types.Value {
			types.value("table"),
			types.value("x"),
			types.value(1),
			types.value(""),
			types.value([]u8{}),
		},
	)
	testing.expect(t, !bad2, "string at values[0] rejected")
}

@(test)
test_column_blob_version :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// A blob with a marker byte but wrong version should be rejected
	bad_blob := []u8{0xFE, 0xFF, 0x01} // marker=0xFE, version=0xFF, count=1
	result := schema.deserialize_columns(bad_blob, context.temp_allocator)
	testing.expect(t, result == nil, "wrong column blob version rejected")
}

@(test)
test_list_tables_empty :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "list_empty")
	defer teardown_schema_env(tree, file)

	tables := schema.list_tables(&tree, context.temp_allocator)
	testing.expect_value(t, len(tables), 0)
}

@(test)
test_schema_unknown_kind :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// A row with kind=5 (unknown, not 0=table) should be rejected
	vals := []types.Value {
		types.value(5),
		types.value("weird"),
		types.value(1),
		types.value(""),
		types.value([]u8{}),
	}
	_, ok := schema.schema_row_from_values(vals)
	testing.expect(t, !ok, "unknown kind rejected")
}

@(test)
test_schema_special_char_names :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	tree, file := setup_schema_env(t, "spec_names")
	defer teardown_schema_env(tree, file)

	cols := []types.Column{{name = "col one", type = .INTEGER}}
	ok := schema.add_table(&tree, "my table", cols, 2, "")
	testing.expect(t, ok, "table name with space added")

	tbl, found := schema.find_table(&tree, "my table", context.temp_allocator)
	testing.expect(t, found, "table with space found")
	testing.expect_value(t, tbl.name, "my table")
	testing.expect_value(t, tbl.columns[0].name, "col one")
}

@(test)
test_schema_row_kind_as_string :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	// kind must be an int, not a string
	vals := []types.Value {
		types.value("table"),
		types.value("x"),
		types.value(1),
		types.value(""),
		types.value([]u8{}),
	}
	_, ok := schema.schema_row_from_values(vals)
	testing.expect(t, !ok, "string kind rejected")
}

@(test)
test_schema_row_index_roundtrip :: proc(t: ^testing.T) {
	context.logger.lowest_level = .Error
	r := schema.Schema_Row {
		kind         = "table",
		name         = "docs",
		root_page    = 42,
		sql          = "CREATE TABLE docs (body TEXT)",
		columns_blob = []u8{9, 9, 9},
		indexes      = []types.Index_Def {
			{name = "i_body", column = "body", root = 77},
			{name = "i_v", column = "v", root = 78},
		},
	}

	// Two triples add values [6..11]: 12 values total.
	values := schema.schema_row_to_values(r)
	testing.expect(t, len(values) == 12, "two triples produce 12 values")

	r2, ok := schema.schema_row_from_values(values)
	testing.expect(t, ok, "12-value format round-trips")
	testing.expect_value(t, len(r2.indexes), 2)
	testing.expect_value(t, r2.indexes[0].root, u32(77))
	testing.expect_value(t, r2.indexes[0].column, "body")
	testing.expect_value(t, r2.indexes[0].name, "i_body")
	testing.expect_value(t, r2.indexes[1].root, u32(78))
	testing.expect_value(t, r2.indexes[1].column, "v")
	testing.expect_value(t, r2.indexes[1].name, "i_v")
	testing.expect_value(t, r2.root_page, u32(42))
	testing.expect_value(t, r2.skip_root, u32(0))

	// No index: 5 values, empty on decode (old rows keep parsing).
	r0 := r
	r0.indexes = nil
	values5 := schema.schema_row_to_values(r0)
	testing.expect(t, len(values5) == 5, "no index produces 5 values")
	r5, ok5 := schema.schema_row_from_values(values5)
	testing.expect(t, ok5, "5-value format accepted")
	testing.expect_value(t, len(r5.indexes), 0)

	// Root-only triple still persists: 9 wide; callers predicate per triple.
	half := r0
	half.indexes = []types.Index_Def{{name = "", column = "", root = 9}}
	half_values := schema.schema_row_to_values(half)
	testing.expect(t, len(half_values) == 9, "unpaired root encodes full width")
	half_back, half_ok := schema.schema_row_from_values(half_values)
	testing.expect(t, half_ok, "full-width form accepted")
	testing.expect_value(t, len(half_back.indexes), 1)
	testing.expect_value(t, half_back.indexes[0].root, u32(9))

	// 7-value input: paired leniency leaves indexes empty.
	seven := []types.Value {
		types.value(0),
		types.value("t"),
		types.value(2),
		types.value(""),
		types.value([]u8{}),
		types.value(11),
	}
	seven_back, seven_ok := schema.schema_row_from_values(seven)
	testing.expect(t, seven_ok, "7-value format accepted")
	testing.expect_value(t, len(seven_back.indexes), 0)

	// 8-value rows (pre-name format) parse one unnamed triple.
	eight := []types.Value {
		types.value(0),
		types.value("t"),
		types.value(2),
		types.value(""),
		types.value([]u8{}),
		types.value(0),
		types.value(77),
		types.value("body"),
	}
	eight_back, eight_ok := schema.schema_row_from_values(eight)
	testing.expect(t, eight_ok, "8-value format accepted")
	testing.expect_value(t, len(eight_back.indexes), 1)
	testing.expect_value(t, eight_back.indexes[0].root, u32(77))
	testing.expect_value(t, eight_back.indexes[0].column, "body")
	testing.expect_value(t, eight_back.indexes[0].name, "")
}

@(test)
test_update_index_root_cow :: proc(t: ^testing.T) {
	// Index roots swap by name, independently of data roots (and of each
	// other with two triples). Built on setup_db (full DB init): bare
	// setup_schema_env trees reject tree_update_cow (pre-existing harness
	// gap, not D2).
	context.logger.lowest_level = .Error
	d := setup_db(t, "index_root")
	defer teardown_db(d, "index_root")

	testing.expect(t, db.execute(d, "CREATE TABLE docs (id INT, body TEXT, v INT);") == .None, "create")

	st := db.Schema_Tree(d)
	def_root, def_ok := schema.update_index_def_cow(&st, "docs", 50, "body", "i_body")
	testing.expect(t, def_ok, "index def publish succeeds")
	if !def_ok {
		return
	}
	d.schema_root_page = def_root

	st1 := db.Schema_Tree(d)
	new_schema_root, ok := schema.update_index_root_cow(&st1, "docs", "i_body", 99)
	testing.expect(t, ok, "index root update succeeds")
	if !ok {
		return
	}
	d.schema_root_page = new_schema_root

	st2 := db.Schema_Tree(d)
	tbl, found := schema.find_table(&st2, "docs", context.temp_allocator)
	testing.expect(t, found, "table found after index update")
	if !found {
		return
	}
	def, has := schema.table_index(tbl, "i_body")
	testing.expect(t, has, "index present")
	if has {
		testing.expect_value(t, def.root, u32(99))
		testing.expect_value(t, def.column, "body")
	}
	schema.table_free(tbl, context.temp_allocator)

	// Unknown name fails without touching the row.
	saved, quiet := suppress_expected_errors()
	context = quiet
	st1b := db.Schema_Tree(d)
	_, ok_no := schema.update_index_root_cow(&st1b, "docs", "nope", 100)
	context = restore_logger(saved)
	testing.expect(t, !ok_no, "unknown index name rejected")

	// Data root swap preserves the index triple (independent fields).
	st3 := db.Schema_Tree(d)
	new_schema_root2, ok2 := schema.update_root_page_cow(&st3, "docs", 7)
	testing.expect(t, ok2, "data root update succeeds")
	if !ok2 {
		return
	}
	d.schema_root_page = new_schema_root2
	st4 := db.Schema_Tree(d)
	tbl2, found2 := schema.find_table(&st4, "docs", context.temp_allocator)
	testing.expect(t, found2, "table found after data update")
	if !found2 {
		return
	}
	testing.expect_value(t, tbl2.root_page, u32(7))
	def2, has2 := schema.table_index(tbl2, "i_body")
	testing.expect(t, has2, "index survives data swap")
	if has2 {
		testing.expect_value(t, def2.root, u32(99))
	}
	schema.table_free(tbl2, context.temp_allocator)
}
