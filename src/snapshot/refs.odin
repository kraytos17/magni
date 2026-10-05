package snapshot

import "core:hash"
import "core:mem"
import "core:time"
import "src:pager"
import "src:types"

// Named refs (branches/tags) plus the restore undo log share one page:
//
//	[MAGIC "MAGNIREFS"][u32 entry count][u32 log_count][u32 log_next]
//	[64-slot ring of Ref_Log_Entry at REFS_LOG_OFFSET]
//	[packed Ref_Entry + name bytes at REFS_ENTRIES_OFFSET]
//
// The entries base is past the ring deliberately: packing entries at 21
// (right after the header counters) collides with ring slots. Pages
// written before the fix keep their entries at 21 and read as ref-less
// here; the next commit/restore re-creates the refs at the new base
// (self-healing — no migration pass).
//
// Ref_Kind marks a ref as BRANCH (MAIN_REF — the commit/restore target) or
// TAG. log_push records the displaced MAIN id on every restore so
// rollforward can move back; the ring holds the last 64 restores.
REFS_MAGIC          :: "MAGNIREFS"
REFS_LOG_OFFSET     :: 24
REFS_LOG_SIZE       :: 64 * size_of(Ref_Log_Entry)
REFS_ENTRIES_OFFSET :: REFS_LOG_OFFSET + REFS_LOG_SIZE
MAX_LOG_ENTRIES     :: 64

// Ref_Kind distinguishes branch refs (move on commit/restore) from tag
// refs (fixed labels).
Ref_Kind :: enum u8 {
	BRANCH = 0,
	TAG    = 1,
}

// Ref_Entry is one named ref: hash of the name (first-pass match),
// snapshot it points at, name length + kind/protection bits, retention
// hints (max_age_ms/min_to_keep), then the name bytes. #packed: memcpy'd
// straight into the page.
Ref_Entry :: struct #packed #simple {
	name_hash   : u64,
	snapshot_id : u64,
	name_len    : u16,
	kind        : u8,
	is_protected: u8,
	max_age_ms  : u64,
	min_to_keep : u32,
}

// Ref_Log_Entry is one undo-log slot: the MAIN id a restore displaced, with
// its timestamp. Fixed 16 bytes so the ring indexes by slot.
Ref_Log_Entry :: struct #packed {
	snapshot_id: u64,
	timestamp  : u64,
}

// MAIN_REF is the branch commit/restore/rollforward all move.
MAIN_REF :: "main"

// create_refs_page allocates and initializes an empty refs page (magic +
// zero entry count), unpinned and dirty on return. 0 on allocation failure.
create_refs_page :: proc(p: ^pager.Pager) -> u32 {
	page, err := pager.allocate_page(p)
	if err != .None {
		return 0
	}

	defer pager.unpin_page(p, page.page_num)
	data := page.data
	copy(data[:], REFS_MAGIC)

	(^u32)(raw_data(data[len(REFS_MAGIC):]))^ = 0
	// Zero the undo-log counters too: allocate_page only zeroes the file
	// header area, so a recycled buffer could otherwise misplace the first
	// log_push (or panic the slice on a garbage log_next).
	(^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^ = 0
	(^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^ = 0
	pager.mark_dirty(p, page.page_num)
	return page.page_num
}

// set_ref upserts the named ref: hash match updates snapshot/kind/flags
// in place, otherwise the entry is appended (false when the page is full —
// nothing is written). Matches on name_hash alone (no byte compare), so a
// 64-bit hash collision would steal the name; get_ref re-checks bytes.
set_ref :: proc(
	p: ^pager.Pager,
	refs_page: u32,
	name: string,
	snapshot_id: u64,
	kind: Ref_Kind,
	is_protected: bool,
) -> bool {
	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return false
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return false
	}

	offset := len(REFS_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^
	// Entries live past the undo-log ring (REFS_ENTRIES_OFFSET), never in
	// the header gap: the ring at [24, ...) would collide with entries
	// packed at 21.
	offset = REFS_ENTRIES_OFFSET
	name_hash := hash.fnv64a(transmute([]u8)name)
	for _ in 0 ..< count {
		entry := (^Ref_Entry)(raw_data(data[offset:]))
		if entry.name_hash == name_hash {
			entry.snapshot_id = snapshot_id
			entry.is_protected = u8(is_protected)
			entry.kind = u8(kind)
			pager.mark_dirty(p, refs_page)
			return true
		}
		offset += size_of(Ref_Entry) + int(entry.name_len)
	}

	entry := Ref_Entry {
		name_hash    = name_hash,
		snapshot_id  = snapshot_id,
		name_len     = u16(len(name)),
		kind         = u8(kind),
		is_protected = u8(is_protected),
	}

	entry_size := size_of(Ref_Entry) + len(name)
	if offset + entry_size > len(data) {
		return false
	}

	mem.copy_non_overlapping(raw_data(data[offset:]), &entry, size_of(Ref_Entry))
	offset += size_of(Ref_Entry)
	copy(data[offset:], transmute([]u8)name)
	(^u32)(raw_data(data[len(REFS_MAGIC):]))^ = count + 1

	pager.mark_dirty(p, refs_page)
	return true
}

// get_ref returns the snapshot id a ref points at. Hash pre-filters, but
// the stored name bytes decide (a different name with the same hash
// misses). Page 0, unreadable pages, bad magic, and absent names all read
// as (0, false).
get_ref :: proc(p: ^pager.Pager, refs_page: u32, name: string) -> (snapshot_id: u64, found: bool) {
	if refs_page == 0 {
		return 0, false
	}

	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return 0, false
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return 0, false
	}

	offset := REFS_ENTRIES_OFFSET // entries live past the undo-log ring
	count := (^u32)(raw_data(data[len(REFS_MAGIC):]))^
	target_hash := hash.fnv64a(transmute([]u8)name)
	for _ in 0 ..< count {
		entry := (^Ref_Entry)(raw_data(data[offset:]))^
		offset += size_of(Ref_Entry)
		if entry.name_hash == target_hash &&
		   string(data[offset:offset + int(entry.name_len)]) == name {
			return entry.snapshot_id, true
		}
		offset += int(entry.name_len)
	}
	return 0, false
}

// list_refs returns the page's raw entries (hash, snapshot, flags — not
// name strings) under allocator. nil on page 0, unreadable pages, or bad
// magic.
@(private = "file")
list_refs :: proc(p: ^pager.Pager, refs_page: u32, allocator := context.allocator) -> []Ref_Entry {
	if refs_page == 0 {
		return nil
	}

	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return nil
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return nil
	}

	offset := REFS_ENTRIES_OFFSET // entries live past the undo-log ring
	count := (^u32)(raw_data(data[len(REFS_MAGIC):]))^
	entries := make([]Ref_Entry, count, allocator)
	for i in 0 ..< count {
		entry := (^Ref_Entry)(raw_data(data[offset:]))^
		entries[i] = entry
		offset += size_of(Ref_Entry) + int(entry.name_len)
	}
	return entries
}

// log_push records snapshot_id (the MAIN id a restore displaced) in the
// ring with the current timestamp, advancing log_next modulo
// MAX_LOG_ENTRIES. Past 64 entries the oldest is overwritten (count stays
// capped) — rollforward history is bounded, not an error. False only on
// unreadable pages or bad magic.
log_push :: proc(p: ^pager.Pager, refs_page: u32, snapshot_id: u64) -> bool {
	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return false
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return false
	}

	log_count := (^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^
	log_next := (^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^
	entry := Ref_Log_Entry {
		snapshot_id = snapshot_id,
		timestamp   = u64(time.to_unix_nanoseconds(time.now()) / types.NANOS_PER_MICRO),
	}

	log_offset := REFS_LOG_OFFSET + int(log_next) * size_of(Ref_Log_Entry)
	mem.copy_non_overlapping(raw_data(data[log_offset:]), &entry, size_of(Ref_Log_Entry))
	if log_count < MAX_LOG_ENTRIES {
		(^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^ = log_count + 1
	}

	(^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^ = (log_next + 1) % MAX_LOG_ENTRIES
	pager.mark_dirty(p, refs_page)
	return true
}

// log_pop removes and returns the most recent log entry (LIFO — the last
// restore's displaced id, which is what rollforward moves MAIN back to).
// ok=false on an empty log (count 0), unreadable pages, or bad magic. Both
// count and log_next retreat, so consecutive pops walk back correctly; the
// slot bytes are left in place for the next push to overwrite.
log_pop :: proc(p: ^pager.Pager, refs_page: u32) -> (snapshot_id: u64, ok: bool) {
	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return 0, false
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return 0, false
	}

	log_count := int((^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^)
	log_next := int((^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^)
	if log_count == 0 {
		return 0, false
	}

	// Most recent entry is the slot behind log_next; popping moves log_next
	// back too, or consecutive pops would return the same slot (count alone
	// does not relocate the top).
	ring_idx := (log_next - 1) % MAX_LOG_ENTRIES
	if ring_idx < 0 {
		ring_idx += MAX_LOG_ENTRIES
	}

	off := REFS_LOG_OFFSET + ring_idx * size_of(Ref_Log_Entry)
	entry := (^Ref_Log_Entry)(raw_data(data[off:]))^

	(^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^ = u32(log_count - 1)
	(^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^ = u32(ring_idx)
	pager.mark_dirty(p, refs_page)
	return entry.snapshot_id, true
}

// log_read_range returns up to count log entries starting at logical index
// start_idx (0 = oldest retained), oldest first, under allocator. Negative
// starts clamp to 0; overruns truncate; empty log or page 0 returns nil.
// Logical order accounts for the ring wrap (oldest slot need not be slot 0).
@(private = "file")
log_read_range :: proc(
	p: ^pager.Pager,
	refs_page: u32,
	start_idx: int,
	count: int,
	allocator := context.allocator,
) -> []Ref_Log_Entry {
	if refs_page == 0 || count <= 0 {
		return nil
	}

	s_idx := max(start_idx, 0)
	page, err := pager.get_page(p, refs_page)
	if err != .None {
		return nil
	}

	defer pager.unpin_page(p, refs_page)
	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC {
		return nil
	}

	log_count := int((^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^)
	log_next := int((^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^)
	if s_idx >= log_count {
		return nil
	}

	available := min(count, log_count - s_idx)
	result := make([]Ref_Log_Entry, available, allocator)
	for i in 0 ..< available {
		ring_idx := (log_next - log_count + s_idx + i) % MAX_LOG_ENTRIES
		if ring_idx < 0 {
			ring_idx += MAX_LOG_ENTRIES
		}

		off := REFS_LOG_OFFSET + ring_idx * size_of(Ref_Log_Entry)
		result[i] = (^Ref_Log_Entry)(raw_data(data[off:]))^
	}
	return result
}
