package snapshot

import "core:fmt"
import "core:time"
import "src:pager"

SNAPSHOT_MAGIC :: "MAGNISNP"

// On-disk header for a snapshot page.
// Allocated at commit time to capture the current schema tree root.
Snapshot_Header :: struct #packed {
	magic:         [8]u8, // "MAGNISNP"
	snapshot_id:   u64,
	prev_snapshot: u32, // page number of previous snapshot (0 = genesis)
	timestamp:     u64, // unix micros
	schema_root:   u32, // root page of the schema B-tree at this snapshot
	state:         u8, // Snapshot_State
	padding:       [7]u8,
}

#assert(size_of(Snapshot_Header) == 40)

Snapshot_State :: enum u8 {
	PENDING   = 0,
	COMMITTED = 1,
	ABANDONED = 2,
}

// Creates a new snapshot page on the pager.
// Writes: magic, id, prev link, timestamp, schema_root, state=COMMITTED.
create :: proc(
	p: ^pager.Pager,
	snapshot_id: u64,
	prev_snapshot: u32,
	schema_root: u32,
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
	h.state = u8(Snapshot_State.COMMITTED)

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

// Counts committed snapshots by walking the chain from latest backward.
count_committed :: proc(p: ^pager.Pager, start_page: u32) -> int {
	count := 0
	page := start_page
	for page != 0 {
		h, ok := load(p, page)
		if !ok { break }
		if Snapshot_State(h.state) == .COMMITTED {
			count += 1
		}
		page = h.prev_snapshot
	}
	return count
}

// Marks snapshot pages older than the most recent N as ABANDONED.
prune :: proc(p: ^pager.Pager, start_page: u32, max_keep: int) {
	total := count_committed(p, start_page)
	if total <= max_keep { return }

	committed_seen := 0
	page := start_page
	for page != 0 {
		h, ok := load(p, page)
		if !ok { break }
		if Snapshot_State(h.state) == .COMMITTED {
			committed_seen += 1
			if committed_seen > max_keep {
				pg, err := pager.get_page(p, page)
				if err == .None {
					header := (^Snapshot_Header)(raw_data(pg.data))
					header.state = u8(Snapshot_State.ABANDONED)
					pager.mark_dirty(p, page)
					pager.unpin_page(p, page)
				}
			}
		}
		page = h.prev_snapshot
	}
}

// Prints the snapshot chain to stdout (debugging).
debug_print_chain :: proc(p: ^pager.Pager, start_page: u32) {
	fmt.println("=== Snapshot Chain ===")
	count := 0
	page := start_page
	for page != 0 {
		h, ok := load(p, page)
		if !ok { break }
		fmt.printf(
			"  Snapshot %d (page %d, prev=%d, root=%d, state=%d, t=%d)\n",
			h.snapshot_id,
			page,
			h.prev_snapshot,
			h.schema_root,
			h.state,
			h.timestamp,
		)
		count += 1
		page = h.prev_snapshot
	}
	if count == 0 {
		fmt.println("  (empty)")
	}
	fmt.println("======================")
}
