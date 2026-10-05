package snapshot

import "core:log"
import "core:mem"
import "core:strings"
import "src:pager"
import "src:types"

// Snapshots name tables by root page, not by catalog row: one manifest page
// per snapshot records every table (name hash, root page, name bytes). The
// chain then supports time-travel and diffs without re-reading old catalog
// trees — only the header's schema_root + manifest_page travel along.
//
// Wire: [MAGIC "MAGNIMNF"][u32 count] then per table: Manifest_Entry
// (name_hash u64, root_page u32, name_len u16) + name bytes. Entries carry
// no checksum of their own — a torn manifest page fails the magic or
// length checks below and reads as absent, never as data.
MANIFEST_MAGIC :: "MAGNIMNF"

// Manifest_Entry is one table's entry in the manifest wire format above.
// #packed: the header is memcpy'd straight into the page.
Manifest_Entry :: struct #packed {
	name_hash: u64,
	root_page: u32,
	name_len : u16,
}

// Table_Change classifies how a table differs between two manifests.
Table_Change :: enum u8 {
	CREATED,
	DROPPED,
	MODIFIED,
}

// Snapshot_Diff_Entry is one changed table between two snapshots.
// table_name is heap-owned (strings.clone under allocator); free with
// diff_entries_free.
Snapshot_Diff_Entry :: struct {
	table_name: string,
	change    : Table_Change,
	old_root  : u32,
	new_root  : u32,
}

// diff_entries_free releases a diff_manifests/diff_snapshots result: every
// cloned name plus the slice itself, under allocator.
diff_entries_free :: proc(entries: []Snapshot_Diff_Entry, allocator := context.allocator) {
	for e in entries {
		delete(e.table_name, allocator)
	}
	delete(entries, allocator)
}

// create_manifest snapshots the database's table roots into a fresh page
// (wire format above) and returns it unpinned + dirty. Empty table list
// means "no manifest" and returns page 0. ok is signaled by nonzero alone:
// 0 on allocation failure.
create_manifest :: proc(p: ^pager.Pager, tables: []types.Table) -> u32 {
	if len(tables) == 0 {
		return 0
	}

	page, err := pager.allocate_page(p)
	if err != .None {
		log.error("Snapshot: failed to allocate manifest page")
		return 0
	}

	data := page.data
	offset := 0

	copy(data[offset:], MANIFEST_MAGIC); offset += len(MANIFEST_MAGIC)
	count := u32(len(tables))
	mem.copy_non_overlapping(raw_data(data[offset:]), &count, size_of(count))
	offset += size_of(count)
	for tbl in tables {
		entry := Manifest_Entry {
			name_hash = u64(types.hash_string(tbl.name)),
			root_page = tbl.root_page,
			name_len  = u16(len(tbl.name)),
		}

		mem.copy_non_overlapping(raw_data(data[offset:]), &entry, size_of(entry))
		offset += size_of(entry)
		copy(data[offset:], transmute([]u8)tbl.name)
		offset += len(tbl.name)
	}

	pager.mark_dirty(p, page.page_num); pager.unpin_page(p, page.page_num)
	return page.page_num
}

// find_in_manifest returns table_name's data root from a manifest page.
// The name hash pre-filters, but the stored name bytes decide (a hash
// collision on a different name misses). Page 0, unreadable pages, bad
// magic, and absent tables all read as (0, false).
find_in_manifest :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	table_name: string,
) -> (
	root_page: u32,
	ok: bool,
) {
	if manifest_page == 0 {
		return 0, false
	}

	page, err := pager.get_page(p, manifest_page)
	if err != .None {
		return 0, false
	}

	defer pager.unpin_page(p, manifest_page)
	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC {
		return 0, false
	}

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^; offset += size_of(u32)
	target_hash := u64(types.hash_string(table_name))
	for _ in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^
		offset += size_of(Manifest_Entry)
		if entry.name_hash == target_hash &&
		   string(data[offset:offset + int(entry.name_len)]) == table_name {
			return entry.root_page, true
		}
		offset += int(entry.name_len)
	}
	return 0, false
}

// load_manifest returns a manifest page's raw entries (hash, root, name
// length — NOT name strings; use load_manifest_tables for names). Empty or
// malformed pages return nil.
@(private = "file")
load_manifest :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	allocator := context.allocator,
) -> []Manifest_Entry {
	if manifest_page == 0 {
		return nil
	}

	page, err := pager.get_page(p, manifest_page)
	if err != .None {
		return nil
	}

	defer pager.unpin_page(p, manifest_page)
	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC {
		return nil
	}

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^; offset += size_of(u32)
	entries := make([]Manifest_Entry, count, allocator)
	for i in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^; entries[i] = entry
		offset += size_of(Manifest_Entry) + int(entry.name_len)
	}
	return entries
}

// load_manifest_tables returns a manifest page's (name, root) pairs with
// names cloned under allocator. Page 0 reads as empty-but-ok ({},{},true);
// unreadable or magic-bad pages read as ({},{},false).
@(private)
load_manifest_tables :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	allocator := context.allocator,
) -> (
	names: []string,
	roots: []u32,
	ok: bool,
) {
	if manifest_page == 0 {
		return {}, {}, true
	}

	page, err := pager.get_page(p, manifest_page)
	if err != .None {
		return {}, {}, false
	}

	defer pager.unpin_page(p, manifest_page)
	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC {
		return {}, {}, false
	}

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^; offset += size_of(u32)
	names = make([]string, count, allocator); roots = make([]u32, count, allocator)
	for i in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^; offset += size_of(Manifest_Entry)
		names[i] = strings.clone(string(data[offset:offset + int(entry.name_len)]), allocator)
		roots[i] = entry.root_page; offset += int(entry.name_len)
	}
	return names, roots, true
}

// diff_manifests compares two manifest pages entry-by-entry: same-name
// different-root is MODIFIED, new-only is CREATED (old_root 0), old-only is
// DROPPED (new_root 0). Equal roots are not reported. Table names are
// cloned under allocator (free with diff_entries_free); comparison uses
// names (not hashes). ok=false when either page fails to load.
diff_manifests :: proc(
	p: ^pager.Pager,
	manifest_a: u32,
	manifest_b: u32,
	allocator := context.allocator,
) -> (
	[]Snapshot_Diff_Entry,
	bool,
) {
	old_names, old_roots, ok_a := load_manifest_tables(p, manifest_a, context.temp_allocator)
	new_names, new_roots, ok_b := load_manifest_tables(p, manifest_b, context.temp_allocator)
	if !ok_a || !ok_b {
		return nil, false
	}

	old_map := make(map[string]u32, context.temp_allocator)
	defer delete(old_map)
	for i in 0 ..< len(old_names) {
		old_map[old_names[i]] = old_roots[i]
	}

	entries := make([dynamic]Snapshot_Diff_Entry, allocator)
	for i in 0 ..< len(new_names) {
		if old_root, existed := old_map[new_names[i]]; existed {
			delete_key(&old_map, new_names[i])
			if new_roots[i] != old_root {
				append(
					&entries,
					Snapshot_Diff_Entry {
						table_name = strings.clone(new_names[i], allocator),
						change = .MODIFIED,
						old_root = old_root,
						new_root = new_roots[i],
					},
				)
			}
		} else {
			append(
				&entries,
				Snapshot_Diff_Entry {
					table_name = strings.clone(new_names[i], allocator),
					change = .CREATED,
					new_root = new_roots[i],
				},
			)
		}
	}
	for name, root in old_map {
		append(
			&entries,
			Snapshot_Diff_Entry {
				table_name = strings.clone(name, allocator),
				change = .DROPPED,
				old_root = root,
			},
		)
	}
	return entries[:], true
}

// diff_snapshots diffs the manifests of the snapshots with older_id and
// newer_id (both resolved through find_by_id from latest_page). Argument
// order doesn't matter — reversed ids are swapped internally. Same entry
// ownership as diff_manifests. ok=false when either id is absent.
diff_snapshots :: proc(
	p: ^pager.Pager,
	older_id: u64,
	newer_id: u64,
	latest_page: u32,
	allocator := context.allocator,
) -> (
	[]Snapshot_Diff_Entry,
	bool,
) {
	if older_id > newer_id {
		return diff_snapshots(p, newer_id, older_id, latest_page, allocator)
	}

	older_h, older_ok := find_by_id(p, latest_page, older_id)
	if !older_ok {
		return nil, false
	}

	newer_h, newer_ok := find_by_id(p, latest_page, newer_id)
	if !newer_ok {
		return nil, false
	}
	return diff_manifests(p, older_h.manifest_page, newer_h.manifest_page, allocator)
}
