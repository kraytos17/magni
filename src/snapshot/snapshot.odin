package snapshot

import "core:fmt"
import "core:mem"
import "core:strings"
import "core:time"
import "src:btree"
import "src:pager"
import "src:types"

SNAPSHOT_MAGIC :: "MAGNISNP"
MANIFEST_MAGIC :: "MAGNIMNF"

// What operation triggered this snapshot.
Snapshot_Operation :: enum u8 {
	UNKNOWN = 0,
	INSERT  = 1,
	UPDATE  = 2,
	DELETE  = 3,
	CREATE  = 4,
	DROP    = 5,
	COMMIT  = 6, // Explicit transaction commit
	RESTORE = 7, // Restored from a historical snapshot
}

// On-disk header for a snapshot page.
// Allocated at commit time to capture the current database state.
Snapshot_Header :: struct #packed {
	magic:         [8]u8, // "MAGNISNP"
	snapshot_id:   u64,
	prev_snapshot: u32, // page number of previous snapshot (0 = genesis)
	timestamp:     u64, // unix micros
	schema_root:   u32, // root page of the schema B-tree at this snapshot
	manifest_page: u32, // page with table manifest (0 = none)
	state:         u8, // Snapshot_State
	operation:     u8, // Snapshot_Operation
	padding:       [2]u8,
}

#assert(size_of(Snapshot_Header) == 40)

// On-disk entry in the manifest page.
// Maps a table to its B-tree data root at this snapshot.
Manifest_Entry :: struct #packed {
	name_hash: u64,
	root_page: u32,
	name_len:  u16,
}

Snapshot_State :: enum u8 {
	PENDING   = 0,
	COMMITTED = 1,
	ABANDONED = 2,
}

// Creates a new manifest page listing all tables and their data roots.
// Returns the page number, or 0 on failure.
create_manifest :: proc(p: ^pager.Pager, tables: []types.Table) -> u32 {
	if len(tables) == 0 { return 0 }

	page, err := pager.allocate_page(p)
	if err != .None {
		fmt.eprintln("Snapshot: failed to allocate manifest page")
		return 0
	}

	data := page.data
	offset := 0
	copy(data[offset:], MANIFEST_MAGIC)
	offset += len(MANIFEST_MAGIC)

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

	pager.mark_dirty(p, page.page_num)
	pager.unpin_page(p, page.page_num)
	return page.page_num
}

// Finds a table's data root page from the manifest.
// Returns the root page and whether it was found.
find_in_manifest :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	table_name: string,
) -> (
	root_page: u32,
	ok: bool,
) {
	if manifest_page == 0 { return 0, false }

	page, err := pager.get_page(p, manifest_page)
	if err != .None { return 0, false }
	defer pager.unpin_page(p, manifest_page)

	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC { return 0, false }

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^
	offset += size_of(u32)

	target_hash := u64(types.hash_string(table_name))
	for _ in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^
		offset += size_of(Manifest_Entry)
		if entry.name_hash == target_hash {
			name_data := string(data[offset:offset + int(entry.name_len)])
			if name_data == table_name {
				return entry.root_page, true
			}
		}
		offset += int(entry.name_len)
	}
	return 0, false
}

// Loads a manifest and returns all entries.
load_manifest :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	allocator := context.allocator,
) -> []Manifest_Entry {
	if manifest_page == 0 { return nil }

	page, err := pager.get_page(p, manifest_page)
	if err != .None { return nil }
	defer pager.unpin_page(p, manifest_page)

	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC { return nil }

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^
	offset += size_of(u32)

	entries := make([]Manifest_Entry, count, allocator)
	for i in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^
		entries[i] = entry
		offset += size_of(Manifest_Entry) + int(entry.name_len)
	}
	return entries
}

Table_Change :: enum {
	CREATED,
	DROPPED,
	MODIFIED,
}

Snapshot_Diff_Entry :: struct {
	table_name: string,
	change:     Table_Change,
	old_root:   u32,
	new_root:   u32,
}

diff_entries_free :: proc(entries: []Snapshot_Diff_Entry, allocator := context.allocator) {
	for e in entries { delete(e.table_name, allocator) }
	delete(entries, allocator)
}

// Loads all table names and root pages from a manifest page.
@(private = "file")
load_manifest_tables :: proc(
	p: ^pager.Pager,
	manifest_page: u32,
	allocator := context.allocator,
) -> (
	names: []string,
	roots: []u32,
	ok: bool,
) {
	if manifest_page == 0 { return {}, {}, true }

	page, err := pager.get_page(p, manifest_page)
	if err != .None { return {}, {}, false }
	defer pager.unpin_page(p, manifest_page)

	data := page.data
	if string(data[:len(MANIFEST_MAGIC)]) != MANIFEST_MAGIC { return {}, {}, false }

	offset := len(MANIFEST_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^
	offset += size_of(u32)

	names = make([]string, count, allocator)
	roots = make([]u32, count, allocator)

	for i in 0 ..< count {
		entry := (^Manifest_Entry)(raw_data(data[offset:]))^
		offset += size_of(Manifest_Entry)
		names[i] = strings.clone(string(data[offset:offset + int(entry.name_len)]), allocator)
		roots[i] = entry.root_page
		offset += int(entry.name_len)
	}
	return names, roots, true
}

// Compares two manifest pages and returns the diff.
// manifest_a is the older, manifest_b is the newer.
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
	if !ok_a || !ok_b { return nil, false }

	entries := make([dynamic]Snapshot_Diff_Entry, allocator)
	// Tables in newer but not in older: CREATED or MODIFIED
	for i in 0 ..< len(new_names) {
		found := false
		for j in 0 ..< len(old_names) {
			if new_names[i] == old_names[j] {
				found = true
				if new_roots[i] != old_roots[j] {
					append(
						&entries,
						Snapshot_Diff_Entry {
							table_name = strings.clone(new_names[i], allocator),
							change = .MODIFIED,
							old_root = old_roots[j],
							new_root = new_roots[i],
						},
					)
				}
				break
			}
		}
		if !found {
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
	// Tables in older but not in newer: DROPPED
	for j in 0 ..< len(old_names) {
		found := false
		for i in 0 ..< len(new_names) {
			if old_names[j] == new_names[i] {
				found = true
				break
			}
		}
		if !found {
			append(
				&entries,
				Snapshot_Diff_Entry {
					table_name = strings.clone(old_names[j], allocator),
					change = .DROPPED,
					old_root = old_roots[j],
				},
			)
		}
	}
	return entries[:], true
}

// Finds two snapshots by ID and diffs their manifests.
// Automatically swaps if older_id > newer_id.
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
	actual_older := older_id
	actual_newer := newer_id
	if older_id > newer_id {
		actual_older = newer_id
		actual_newer = older_id
	}

	older_h, older_ok := find_by_id(p, latest_page, actual_older)
	if !older_ok { return nil, false }
	newer_h, newer_ok := find_by_id(p, latest_page, actual_newer)
	if !newer_ok { return nil, false }
	return diff_manifests(p, older_h.manifest_page, newer_h.manifest_page, allocator)
}

// Creates a new snapshot page on the pager.
create :: proc(
	p: ^pager.Pager,
	snapshot_id: u64,
	prev_snapshot: u32,
	schema_root: u32,
	manifest_page: u32 = 0,
	operation: Snapshot_Operation = .UNKNOWN,
) -> (
	snapshot_page: u32,
	ok: bool,
) {
	page, err := pager.allocate_page(p)
	if err != .None {
		fmt.eprintln("Snapshot: failed to allocate page")
		return 0, false
	}
	defer pager.unpin_page(p, page.page_num)

	h := (^Snapshot_Header)(raw_data(page.data))
	copy(h.magic[:], SNAPSHOT_MAGIC)
	h.snapshot_id = snapshot_id
	h.prev_snapshot = prev_snapshot
	h.timestamp = u64(time.now()._nsec / 1000) // micros
	h.schema_root = schema_root
	h.manifest_page = manifest_page
	h.state = u8(Snapshot_State.COMMITTED)
	h.operation = u8(operation)
	pager.mark_dirty(p, page.page_num)
	return page.page_num, true
}

// Loads a snapshot header from a snapshot page.
load :: proc(p: ^pager.Pager, snapshot_page: u32) -> (Snapshot_Header, bool) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None {
		return {}, false
	}
	defer pager.unpin_page(p, snapshot_page)

	h := (^Snapshot_Header)(raw_data(page.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC {
		fmt.eprintln("Snapshot: invalid magic on page", snapshot_page)
		return {}, false
	}
	return h^, true
}

TAG_OFFSET :: size_of(Snapshot_Header)
TAG_SIZE :: 64

// Sets a human-readable tag on a snapshot page.
set_tag :: proc(p: ^pager.Pager, snapshot_page: u32, tag: string) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return }
	defer pager.unpin_page(p, snapshot_page)

	h := (^Snapshot_Header)(raw_data(page.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC { return }

	data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
	n := min(len(tag), TAG_SIZE - 1)
	mem.set(raw_data(data), 0, TAG_SIZE)
	copy(data, tag[:n])
	pager.mark_dirty(p, snapshot_page)
}

// Reads a human-readable tag from a snapshot page.
get_tag :: proc(p: ^pager.Pager, snapshot_page: u32) -> string {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return "" }
	defer pager.unpin_page(p, snapshot_page)

	h := (^Snapshot_Header)(raw_data(page.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC { return "" }

	data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
	length := 0
	for length < TAG_SIZE && data[length] != 0 { length += 1 }
	return string(data[:length])
}

// Collects all snapshot headers from the chain into a slice (newest first).
list_snapshots :: proc(
	p: ^pager.Pager,
	latest_page: u32,
	allocator := context.allocator,
) -> []Snapshot_Header {
	result := make([dynamic]Snapshot_Header, allocator)
	walk_chain(p, latest_page, &result, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		result := cast(^[dynamic]Snapshot_Header)data
		append(result, h)
		return true
	})
	return result[:]
}

// Finds the newest committed snapshot with timestamp ≤ target_ts.
find_by_timestamp :: proc(p: ^pager.Pager, start_page: u32, target_ts: u64) -> (Snapshot_Header, bool) {
	Find_Ts_Data :: struct {
		result:    ^Snapshot_Header,
		found:     ^bool,
		target_ts: u64,
	}

	result: Snapshot_Header
	found := false
	d := Find_Ts_Data {
		result    = &result,
		found     = &found,
		target_ts = target_ts,
	}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Find_Ts_Data)data
		if Snapshot_State(h.state) == .COMMITTED && h.timestamp <= d.target_ts {
			d.result^ = h
			d.found^ = true
			return false
		}
		return true
	})
	return result, found
}

@(private = "file")
walk_chain_data :: struct {
	p:         ^pager.Pager,
	max_keep:  int,
	committed: int,
	target_id: u64,
	result:    ^Snapshot_Header,
	found:     ^bool,
	count:     ^int,
}

@(private = "file")
walk_chain :: proc(
	p: ^pager.Pager,
	start_page: u32,
	data: rawptr,
	callback: proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool,
) {
	page := start_page
	for page != 0 {
		h, ok := load(p, page)
		if !ok { break }
		if !callback(h, page, data) { break }
		page = h.prev_snapshot
	}
}

// Counts committed snapshots by walking the chain from latest backward.
count_committed :: proc(p: ^pager.Pager, start_page: u32) -> int {
	count := 0
	walk_chain(p, start_page, &count, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		count := cast(^int)data
		if Snapshot_State(h.state) == .COMMITTED {
			count^ += 1
		}
		return true
	})
	return count
}

// Marks snapshot pages older than the most recent N as ABANDONED.
prune :: proc(p: ^pager.Pager, start_page: u32, max_keep: int) {
	total := count_committed(p, start_page)
	if total <= max_keep { return }

	d := walk_chain_data {
		p        = p,
		max_keep = max_keep,
	}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^walk_chain_data)data
		if Snapshot_State(h.state) == .COMMITTED {
			d.committed += 1
			if d.committed > d.max_keep {
				pg, err := pager.get_page(d.p, page)
				if err == .None {
					header := (^Snapshot_Header)(raw_data(pg.data))
					header.state = u8(Snapshot_State.ABANDONED)
					pager.mark_dirty(d.p, page)
					pager.unpin_page(d.p, page)
				}
			}
		}
		return true
	})
}

// Walks the snapshot chain backward from start_page to find a snapshot by ID.
find_by_id :: proc(p: ^pager.Pager, start_page: u32, target_id: u64) -> (Snapshot_Header, bool) {
	result: Snapshot_Header
	found := false
	d := walk_chain_data {
		result    = &result,
		found     = &found,
		target_id = target_id,
	}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^walk_chain_data)data
		if h.snapshot_id == d.target_id {
			d.result^ = h
			d.found^ = true
			return false
		}
		return true
	})
	return result, found
}

// Prints the snapshot chain to stdout (debugging).
debug_print_chain :: proc(p: ^pager.Pager, start_page: u32) {
	Debug_Data :: struct {
		p:     ^pager.Pager,
		count: int,
	}
	d := Debug_Data {
		p = p,
	}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Debug_Data)data
		op := Snapshot_Operation(h.operation)
		st := Snapshot_State(h.state)
		tag := get_tag(d.p, page)
		fmt.printf(
			"  Snapshot %-4d  page=%-4d  op=%-6s  state=%-9s  ts=%d",
			h.snapshot_id,
			page,
			op,
			st,
			h.timestamp,
		)
		if tag != "" {
			fmt.printf("  tag=%s", tag)
		}
		fmt.println()
		d.count += 1
		return true
	})
	if d.count == 0 {
		fmt.println("  (empty)")
	}
	fmt.println("======================")
}

// Garbage-collects orphan pages not reachable from the latest keep_count snapshots.
// Walks the snapshot chain, collects all live pages (snapshot pages, manifests,
// schema roots, and all B-tree data pages), then frees everything else.
gc :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int) {
	live := make(map[u32]bool, context.temp_allocator)
	defer delete(live)

	live[1] = true
	count := 0
	page := latest_page
	for page != 0 && count < keep_count {
		h, ok := load(p, page)
		if !ok { break }

		live[page] = true
		if h.manifest_page != 0 { live[h.manifest_page] = true }
		if h.schema_root != 0 { live[h.schema_root] = true }
		if h.manifest_page != 0 {
			t := btree.init(p, h.schema_root)
			_, roots, load_ok := load_manifest_tables(p, h.manifest_page, context.temp_allocator)
			if load_ok {
				for i in 0 ..< len(roots) {
					if roots[i] != 0 {
						live[roots[i]] = true
						btree.collect_pages(&t, roots[i], &live)
					}
				}
			}
		}

		count += 1
		page = h.prev_snapshot
	}

	max_page := pager.page_count(p)
	for pn := u32(2); pn <= max_page; pn += 1 {
		if pn not_in live {
			pager.free_page(p, pn)
		}
	}
}
