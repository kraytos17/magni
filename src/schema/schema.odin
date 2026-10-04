// Package schema manages table metadata stored as rows in a system B-tree.
package schema

import "core:fmt"
import "core:log"
import "core:mem"
import "core:strings"
import "core:sync"
import "src:btree"
import "src:cell"
import "src:types"

init :: proc(t: ^btree.Tree) -> bool {
	_, err := btree.load_node(t, t.root)
	return err == .None
}

// Schema_Row is the canonical representation of a schema b-tree entry.
Schema_Row :: struct {
	kind        : string, // always "table"
	name        : string,
	root_page   : u32,
	sql         : string,
	columns_blob: []u8,
	skip_root   : u32,
	// Secondary text indexes: one [root INT][column TEXT][name TEXT]
	// triple per index from slot [6] (N = 0..). Each field parses with
	// skip-style independent leniency; "indexed" is a caller-side
	// predicate (root > 0 AND column non-empty per triple), not a codec
	// invariant. Legacy widths: 8-wide single unnamed ([6],[7]),
	// 9-wide single named (+[8]); both parse as one triple.
	indexes     : []types.Index_Def,
}

schema_row_to_values :: proc(r: Schema_Row, allocator := context.temp_allocator) -> []types.Value {
	n := 5 // kind + name + root + sql + blob
	// Triples live at fixed [6+3i] whenever any index exists (root-only
	// swaps persist; the skip slot is emitted explicitly, possibly 0, so
	// positions never shift). Old 5/6/8/9-wide rows keep parsing (below).
	k := len(r.indexes)
	if r.skip_root > 0 || k > 0 { n += 1 }
	n += 3 * k

	result := make([]types.Value, n, allocator)
	result[0] = types.value_int(0) // 0 = table
	result[1] = types.value_text(r.name)
	result[2] = types.value_int(i64(r.root_page))
	result[3] = types.value_text(r.sql)
	result[4] = types.value_blob(r.columns_blob)
	if n >= 6 {
		result[5] = types.value_int(i64(r.skip_root))
	}
	for i in 0 ..< k {
		result[6 + 3 * i] = types.value_int(i64(r.indexes[i].root))
		result[6 + 3 * i + 1] = types.value_text(r.indexes[i].column)
		result[6 + 3 * i + 2] = types.value_text(r.indexes[i].name)
	}
	return result
}

schema_row_from_values :: proc(values: []types.Value) -> (Schema_Row, bool) {
	if len(values) < 5 { return {}, false }

	name, ok1 := values[1].(string)
	if !ok1 { return {}, false }

	kind_val, kind_ok := values[0].(i64)
	if !kind_ok || kind_val != 0 { return {}, false }

	sr := Schema_Row {
		kind = "table",
		name = name,
	}

	root, ok2 := values[2].(i64)
	sql, ok3 := values[3].(string)
	blob, ok4 := values[4].([]u8)
	if !ok2 || !ok3 || !ok4 { return {}, false }

	sr.root_page = u32(root)
	sr.sql = sql
	sr.columns_blob = blob
	if len(values) >= 6 {
		if skip, ok5 := values[5].(i64); ok5 { sr.skip_root = u32(skip) }
	}
	// Triples from [6]: complete [root INT][column TEXT][name TEXT]
	// groups only (trailing partials ignored, skip-style). Legacy
	// widths: 8-wide single unnamed ([6],[7]), 9-wide single named
	// (+[8]); both parse as one triple.
	if len(values) >= 8 {
		triples := make([dynamic]types.Index_Def, 0, 2, context.temp_allocator)
		i := 6
		for i + 1 < len(values) {
			iroot, rok := values[i].(i64)
			col, cok := values[i + 1].(string)
			if !rok || !cok { break }
			iname := ""
			if i + 2 < len(values) {
				if nm, nok := values[i + 2].(string); nok { iname = nm } else { break }
			}
			append(&triples, types.Index_Def{name = iname, column = col, root = u32(iroot)})
			i += 3
			// Legacy 8-wide has no name slot: exactly one triple.
			if len(values) == 8 { break }
		}
		sr.indexes = triples[:]
	}
	return sr, true
}

add_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
	sql_stmt: string,
) -> bool {
	rowid := types.Row_ID(types.hash_string(table_name))
	if c, err := btree.tree_find(t, rowid, context.temp_allocator); err == .None {
		defer cell.destroy(&c, context.temp_allocator)
		if existing_name, ok := c.values[1].(string); ok && existing_name == table_name {
			log.errorf("[Schema] Table already exists: %s", table_name)
		} else {
			log.errorf(
				"[Schema] Hash collision: '%s' collides with '%s' at key %v",
				table_name,
				existing_name,
				rowid,
			)
		}
		return false
	}

	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	r := Schema_Row {
		kind         = "table",
		name         = table_name,
		root_page    = root_page,
		sql          = sql_stmt,
		columns_blob = col_blob,
	}

	values := schema_row_to_values(r)
	err := btree.tree_insert(t, rowid, values)
	if err != .None {
		log.errorf("[Schema] add_table failed: %v", err)
		return false
	}
	return true
}

add_table_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	columns: []types.Column,
	root_page: u32,
	sql_stmt: string,
) -> (
	u32,
	bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	if c, err := btree.tree_find(t, rowid, context.temp_allocator); err == .None {
		defer cell.destroy(&c, context.temp_allocator)
		if existing_name, ok := c.values[1].(string); ok && existing_name == table_name {
			log.errorf("[Schema] Table already exists: %s", table_name)
		} else {
			log.errorf(
				"[Schema] Hash collision: '%s' collides with '%s' at key %v",
				table_name,
				existing_name,
				rowid,
			)
		}
		return t.root, false
	}

	col_blob := serialize_columns_to_blob(columns, context.temp_allocator)
	r := Schema_Row {
		kind         = "table",
		name         = table_name,
		root_page    = root_page,
		sql          = sql_stmt,
		columns_blob = col_blob,
	}

	values := schema_row_to_values(r)
	new_root, err := btree.tree_insert_cow(t, rowid, values)
	if err != .None {
		log.errorf("[Schema] add_table_cow failed: %v", err)
		return t.root, false
	}
	return new_root, true
}

find_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None { return {}, false }
	defer cell.destroy(&c, context.temp_allocator)

	table, ok := table_from_values(c.values, allocator)
	if !ok { return {}, false }
	if table.name != table_name {
		table_free(table, allocator)
		return {}, false
	}
	return table, true
}

// Table_Cache is a lazily-populated catalog cache. It is valid only for the
// schema root it was built against (cache.root); any DDL that mutates the schema
// changes the root and implicitly invalidates it. Cached tables are owned by the
// cache and must not be freed by callers.
Table_Cache :: struct {
	root     : u32,
	tables   : map[string]^types.Table,
	allocator: mem.Allocator,
	mu       : sync.RW_Mutex, // guards the cache; taken under db.mu, before pager.mutex
}

// clear_table_cache frees every cached table entry; caller must hold cache.mu.
@(private = "file")
clear_table_cache :: proc(cache: ^Table_Cache) {
	for k, tbl in cache.tables {
		delete(k, cache.allocator)
		table_free(tbl^, cache.allocator)
		free(tbl, cache.allocator)
	}
	clear(&cache.tables)
}

table_cache_free :: proc(cache: ^Table_Cache) {
	sync.rw_mutex_lock(&cache.mu)
	defer sync.rw_mutex_unlock(&cache.mu)
	clear_table_cache(cache)
	if cache.tables != nil {
		delete(cache.tables)
	}

	cache.tables = nil
	cache.root = 0
}

// table_cache_clear drops every cached entry (keeping the map for reuse).
// ROLLBACK needs this: staged in-txn roots never bumped the schema root, so
// the root-change auto-invalidation won't fire and stale entries would leak
// past the rollback without an explicit clear.
table_cache_clear :: proc(cache: ^Table_Cache) {
	sync.rw_mutex_lock(&cache.mu)
	defer sync.rw_mutex_unlock(&cache.mu)
	clear_table_cache(cache)
}

// find_table_cached returns a borrowed reference to the table's cached catalog
// entry (or populates it on a miss). The returned table is owned by the cache;
// callers must not call table_free on it.
find_table_cached :: proc(
	t: ^btree.Tree,
	table_name: string,
	cache: ^Table_Cache,
) -> (
	^types.Table,
	bool,
) {
	if cache == nil {
		table, ok := find_table(t, table_name, context.temp_allocator)
		if !ok { return nil, false }

		tbl := new(types.Table, context.temp_allocator)
		tbl^ = table
		return tbl, true
	}

	sync.rw_mutex_lock(&cache.mu)
	defer sync.rw_mutex_unlock(&cache.mu)
	if cache.tables == nil {
		cache.tables = make(map[string]^types.Table, 16, cache.allocator)
	}
	if cache.root != t.root {
		clear_table_cache(cache)
		cache.root = t.root
	}
	if tbl, ok := cache.tables[table_name]; ok {
		return tbl, true
	}

	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None { return nil, false }
	defer cell.destroy(&c, context.temp_allocator)

	table, ok := table_from_values(c.values, cache.allocator)
	if !ok { return nil, false }
	if table.name != table_name {
		table_free(table, cache.allocator)
		return nil, false
	}

	tbl := new(types.Table, cache.allocator)
	tbl^ = table
	cache.tables[strings.clone(table_name, cache.allocator)] = tbl
	return tbl, true
}

get_table :: proc(
	t: ^btree.Tree,
	table_name: string,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) { return find_table(t, table_name, allocator) }

list_tables :: proc(t: ^btree.Tree, allocator := context.allocator) -> []types.Table {
	tables := make([dynamic]types.Table, allocator)
	cursor, err := btree.cursor_start(t, context.temp_allocator)
	if err != .None { return nil }
	defer btree.cursor_destroy(&cursor)
	for cursor.is_valid {
		c, get_err := btree.cursor_get_cell(&cursor, context.temp_allocator)
		if get_err == .None {
			if tbl, ok := table_from_values(c.values, allocator); ok { append(&tables, tbl) }
			cell.destroy(&c, context.temp_allocator)
		}
		btree.cursor_advance(&cursor)
	}
	return tables[:]
}

drop_table :: proc(t: ^btree.Tree, table_name: string) -> bool {
	return btree.tree_delete(t, types.Row_ID(types.hash_string(table_name))) == .None
}

drop_table_cow :: proc(t: ^btree.Tree, table_name: string) -> (u32, bool) {
	new_root, err := btree.tree_delete_cow(t, types.Row_ID(types.hash_string(table_name)))
	if err != .None {
		log.errorf("[Schema] drop_table_cow failed: %v", err)
		return t.root, false
	}
	return new_root, true
}

table_exists :: proc(t: ^btree.Tree, table_name: string) -> bool {
	c, err := btree.tree_find(
		t,
		types.Row_ID(types.hash_string(table_name)),
		context.temp_allocator,
	)
	if err == .None {
		cell.destroy(&c, context.temp_allocator)
		return true
	}
	return false
}

@(private = "file")
table_from_values :: proc(
	values: []types.Value,
	allocator := context.allocator,
) -> (
	types.Table,
	bool,
) {
	sr, ok := schema_row_from_values(values)
	if !ok { return {}, false }

	table: types.Table
	table.name = strings.clone(sr.name, allocator)
	table.root_page = sr.root_page
	table.sql = strings.clone(sr.sql, allocator)
	cols := deserialize_columns(sr.columns_blob, allocator)
	if cols == nil {
		delete(table.name, allocator)
		delete(table.sql, allocator)
		return {}, false
	}

	table.columns = cols
	table.skip_root = sr.skip_root
	if len(sr.indexes) > 0 {
		defs := make([]types.Index_Def, len(sr.indexes), allocator)
		for def, i in sr.indexes {
			defs[i] = types.Index_Def {
				root   = def.root,
				column = strings.clone(def.column, allocator),
				name   = strings.clone(def.name, allocator),
			}
		}
		table.indexes = defs
	}
	return table, true
}

// table_index returns the named index definition of a resolved table.
// Linear scan: tables carry few indexes, and this rides catalog
// resolution (not row loops).
table_index :: proc(table: types.Table, index_name: string) -> (types.Index_Def, bool) {
	for def in table.indexes {
		if def.name == index_name { return def, true }
	}
	return {}, false
}

table_free :: proc(table: types.Table, allocator := context.allocator) {
	delete(table.name, allocator); delete(table.sql, allocator)
	for def in table.indexes {
		delete(def.column, allocator)
		delete(def.name, allocator)
	}
	delete(table.indexes, allocator)
	for col in table.columns {
		delete(col.name, allocator)
		if def, ok := col.default_value.?; ok { types.value_delete(def, allocator) }
		if chk, has := col.check_expr.?; has { delete(chk, allocator) }
	}
	delete(table.columns, allocator)
}

update_root_page_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_root_page: u32,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	return update_schema_root_cow(
		t,
		table_name,
		new_root_page,
		set_data_root,
		"update_root_page_cow",
	)
}

@(private = "file")
set_data_root :: proc(sr: ^Schema_Row, root: u32) { sr.root_page = root }

@(private = "file")
set_skip_root :: proc(sr: ^Schema_Row, root: u32) { sr.skip_root = root }


// update_schema_root_cow is the shared core behind update_root_page_cow and
// update_skip_root_cow: fetch the schema row, apply the field setter, and
// commit it copy-on-write. Callers keep their names, so no call-site churn.
@(private = "file")
update_schema_root_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_root: u32,
	set_root: proc(sr: ^Schema_Row, root: u32),
	op_name: string,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		log.errorf(
			"[schema] %s: tree_find failed for '%s' rowid=%v root=%d",
			op_name,
			table_name,
			rowid,
			t.root,
		)
		return t.root, false
	}
	defer cell.destroy(&c, context.temp_allocator)

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		log.errorf("[schema] %s: schema_row_from_values failed for '%s'", op_name, table_name)
		return t.root, false
	}

	set_root(&sr, new_root)
	values := schema_row_to_values(sr)
	upd_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		log.errorf(
			"[schema] %s: tree_update_cow failed for '%s': %v",
			op_name,
			table_name,
			upd_err,
		)
		return t.root, false
	}
	return upd_root, true
}

update_skip_root_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_skip_root: u32,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	return update_schema_root_cow(
		t,
		table_name,
		new_skip_root,
		set_skip_root,
		"update_skip_root_cow",
	)
}

// update_index_def_cow publishes one secondary-index definition
// (append or replace by name) in ONE schema COW: readers never see a
// half index. DDL (exec_create_index) is the only writer; per-mutation
// root swaps go through update_index_root_cow; exec_drop_index clears
// via clear_index_def_cow below.
update_index_def_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	new_index_root: u32,
	index_column: string,
	index_name: string,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		log.errorf("[schema] update_index_def_cow: tree_find failed for '%s'", table_name)
		return t.root, false
	}
	defer cell.destroy(&c, context.temp_allocator)

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		log.errorf("[schema] update_index_def_cow: decode failed for '%s'", table_name)
		return t.root, false
	}

	defs := make([dynamic]types.Index_Def, 0, len(sr.indexes) + 1, context.temp_allocator)
	replaced := false
	for def in sr.indexes {
		if def.name == index_name {
			append(&defs, types.Index_Def{name = index_name, column = index_column, root = new_index_root})
			replaced = true
		} else {
			append(&defs, def)
		}
	}
	if !replaced {
		append(&defs, types.Index_Def{name = index_name, column = index_column, root = new_index_root})
	}
	sr.indexes = defs[:]
	values := schema_row_to_values(sr)
	upd_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		log.errorf("[schema] update_index_def_cow failed for '%s': %v", table_name, upd_err)
		return t.root, false
	}
	return upd_root, true
}

// clear_index_def_cow drops a table's secondary-index definition (root +
// column + name, atomically) in ONE schema COW. DDL (exec_drop_index) is
// the only writer. Index pages are NOT freed here: snapshots may still
// reference them (same reason DROP TABLE never frees data pages); GC
// reclaims unreachable pages once snapshots expire.
clear_index_def_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	index_name: string,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		log.errorf("[schema] clear_index_def_cow: tree_find failed for '%s'", table_name)
		return t.root, false
	}
	defer cell.destroy(&c, context.temp_allocator)

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		log.errorf("[schema] clear_index_def_cow: decode failed for '%s'", table_name)
		return t.root, false
	}
	defs := make([dynamic]types.Index_Def, 0, len(sr.indexes), context.temp_allocator)
	dropped := false
	for def in sr.indexes {
		if def.name == index_name { dropped = true } else { append(&defs, def) }
	}
	if !dropped {
		log.errorf("[schema] clear_index_def_cow: no index '%s' on '%s'", index_name, table_name)
		return t.root, false
	}
	sr.indexes = defs[:]
	values := schema_row_to_values(sr)
	upd_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		log.errorf("[schema] clear_index_def_cow failed for '%s': %v", table_name, upd_err)
		return t.root, false
	}
	return upd_root, true
}

// find_tables_by_index resolves all tables owning secondary index
// `index_name` (names repeat across tables; unique per table, enforced
// at CREATE). Returns heap-owned names under allocator (caller frees).
find_tables_by_index :: proc(
	t: ^btree.Tree,
	index_name: string,
	allocator := context.allocator,
) -> []string {
	out := make([dynamic]string, 0, 1, allocator)
	tables := list_tables(t, context.temp_allocator)
	for tbl in tables {
		for def in tbl.indexes {
			if def.name == index_name {
				append(&out, strings.clone(tbl.name, allocator))
				break
			}
		}
	}
	return out[:]
}
// update_index_root_cow swaps one secondary-index root by name
// (fetch-set-COW-commit, same shape as data/skip roots). Per-mutation
// fan-out is the only writer; returns false when the triple is absent.
update_index_root_cow :: proc(
	t: ^btree.Tree,
	table_name: string,
	index_name: string,
	new_index_root: u32,
) -> (
	new_schema_root: u32,
	ok: bool,
) {
	rowid := types.Row_ID(types.hash_string(table_name))
	c, err := btree.tree_find(t, rowid, context.temp_allocator)
	if err != .None {
		log.errorf("[schema] update_index_root_cow: tree_find failed for '%s'", table_name)
		return t.root, false
	}
	defer cell.destroy(&c, context.temp_allocator)

	sr, sr_ok := schema_row_from_values(c.values)
	if !sr_ok {
		log.errorf("[schema] update_index_root_cow: decode failed for '%s'", table_name)
		return t.root, false
	}
	defs := make([dynamic]types.Index_Def, 0, len(sr.indexes), context.temp_allocator)
	swapped := false
	for def in sr.indexes {
		if def.name == index_name {
			append(&defs, types.Index_Def{name = def.name, column = def.column, root = new_index_root})
			swapped = true
		} else {
			append(&defs, def)
		}
	}
	if !swapped {
		log.errorf("[schema] update_index_root_cow: no index '%s' on '%s'", index_name, table_name)
		return t.root, false
	}
	sr.indexes = defs[:]
	values := schema_row_to_values(sr)
	upd_root, upd_err := btree.tree_update_cow(t, rowid, values)
	if upd_err != .None {
		log.errorf("[schema] update_index_root_cow failed for '%s': %v", table_name, upd_err)
		return t.root, false
	}
	return upd_root, true
}

validate_columns :: proc(columns: []types.Column) -> (bool, string) {
	if len(columns) == 0 { return false, "Table must have at least one column" }
	if len(columns) > types.MAX_COLS {
		return false, fmt.tprintf("Too many columns (max %d)", types.MAX_COLS)
	}

	pk_count := 0
	for col, i in columns {
		if len(col.name) == 0 { return false, "Column name cannot be empty" }
		if col.pk { pk_count += 1 }
		for j in i + 1 ..< len(columns) {
			if columns[i].name == columns[j].name {
				return false, fmt.tprintf("Duplicate column name: %s", columns[i].name)
			}
		}
	}
	if pk_count > 1 {
		return false, "Multiple primary keys not supported right now"
	}
	return true, ""
}

find_column_index :: proc(columns: []types.Column, name: string) -> (int, bool) {
	for col, i in columns {
		if col.name == name {
			return i, true
		}
	}
	return -1, false
}

get_pk_column :: proc(columns: []types.Column) -> (int, bool) {
	for col, i in columns {
		if col.pk {
			return i, true
		}
	}
	return -1, false
}

@(private = "file")
debug_print_entry :: proc(table: types.Table) {
	fmt.printf("Table: %s (Root: %d)\n", table.name, table.root_page)
	fmt.printf("SQL:   %s\n", table.sql)
	fmt.println("Columns:")
	for col, i in table.columns {
		flags := make([dynamic]string, context.temp_allocator)
		if col.pk { append(&flags, "PK") }
		if col.not_null { append(&flags, "NN") }

		flags_str := strings.join(flags[:], ", ", context.temp_allocator)
		type_str: string
		switch col.type {
		case .INTEGER:
			type_str = "INT"
		case .TEXT:
			type_str = "TXT"
		case .REAL:
			type_str = "REAL"
		case .BLOB:
			type_str = "BLOB"
		}
		if len(flags) > 0 {
			fmt.printf("  %d. %-10s %-5s [%s]\n", i + 1, col.name, type_str, flags_str)
		} else {
			fmt.printf("  %d. %-10s %-5s\n", i + 1, col.name, type_str)
		}
	}
}

debug_print_all :: proc(t: ^btree.Tree) {
	fmt.println("=== Database Schema ===")
	tables := list_tables(t, context.temp_allocator)
	if len(tables) == 0 {
		fmt.println("No tables found.")
		return
	}
	for table, i in tables {
		if i > 0 { fmt.println("-----------------------") }
		debug_print_entry(table)
	}
	fmt.println("=======================")
}

print_ddl :: proc(t: ^btree.Tree) {
	tables := list_tables(t, context.temp_allocator)
	for table in tables {
		fmt.printfln("%s", strings.trim_space(table.sql))
	}
}
