package snapshot

import "core:hash"
import "core:mem"
import "core:time"
import "src:pager"
import "src:types"

REFS_MAGIC :: "MAGNIREFS"
REFS_LOG_OFFSET :: 24
REFS_LOG_SIZE :: 64 * size_of(Ref_Log_Entry)
REFS_ENTRIES_OFFSET :: REFS_LOG_OFFSET + REFS_LOG_SIZE
MAX_LOG_ENTRIES :: 64

Ref_Kind :: enum u8 {
	BRANCH = 0,
	TAG    = 1,
}

Ref_Entry :: struct #packed #simple {
	name_hash:    u64,
	snapshot_id:  u64,
	name_len:     u16,
	kind:         u8,
	is_protected: u8,
	max_age_ms:   u64,
	min_to_keep:  u32,
}

Ref_Log_Entry :: struct #packed {
	snapshot_id: u64,
	timestamp:   u64,
}

MAIN_REF :: "main"

create_refs_page :: proc(p: ^pager.Pager) -> u32 {
	page, err := pager.allocate_page(p)
	if err != .None { return 0 }
	defer pager.unpin_page(p, page.page_num)

	data := page.data
	copy(data[:], REFS_MAGIC)
	(^u32)(raw_data(data[len(REFS_MAGIC):]))^ = 0
	pager.mark_dirty(p, page.page_num)
	return page.page_num
}

set_ref :: proc(
	p: ^pager.Pager,
	refs_page: u32,
	name: string,
	snapshot_id: u64,
	kind: Ref_Kind,
	is_protected: bool,
) -> bool {
	page, err := pager.get_page(p, refs_page)
	if err != .None { return false }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return false }

	offset := len(REFS_MAGIC)
	count := (^u32)(raw_data(data[offset:]))^; offset += 4

	// Skip log metadata
	offset += 8
	name_hash := hash.fnv64(transmute([]u8)name)
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
	if offset + entry_size > len(data) { return false }

	mem.copy_non_overlapping(raw_data(data[offset:]), &entry, size_of(Ref_Entry))
	offset += size_of(Ref_Entry)
	copy(data[offset:], transmute([]u8)name)
	(^u32)(raw_data(data[len(REFS_MAGIC):]))^ = count + 1

	pager.mark_dirty(p, refs_page)
	return true
}

get_ref :: proc(p: ^pager.Pager, refs_page: u32, name: string) -> (snapshot_id: u64, found: bool) {
	if refs_page == 0 { return 0, false }

	page, err := pager.get_page(p, refs_page)
	if err != .None { return 0, false }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return 0, false }

	offset := len(REFS_MAGIC) + 4 + 8
	count := (^u32)(raw_data(data[len(REFS_MAGIC):]))^
	target_hash := hash.fnv64(transmute([]u8)name)
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

list_refs :: proc(p: ^pager.Pager, refs_page: u32, allocator := context.allocator) -> []Ref_Entry {
	if refs_page == 0 { return nil }

	page, err := pager.get_page(p, refs_page)
	if err != .None { return nil }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return nil }

	offset := len(REFS_MAGIC) + 4 + 8
	count := (^u32)(raw_data(data[len(REFS_MAGIC):]))^
	entries := make([]Ref_Entry, count, allocator)
	for i in 0 ..< count {
		entry := (^Ref_Entry)(raw_data(data[offset:]))^
		entries[i] = entry
		offset += size_of(Ref_Entry) + int(entry.name_len)
	}
	return entries
}

log_push :: proc(p: ^pager.Pager, refs_page: u32, snapshot_id: u64) -> bool {
	page, err := pager.get_page(p, refs_page)
	if err != .None { return false }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return false }

	log_count := (^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^
	log_next := (^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^
	entry := Ref_Log_Entry {
		snapshot_id = snapshot_id,
		timestamp   = u64(time.now()._nsec / types.NANOS_PER_MICRO),
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

log_pop :: proc(p: ^pager.Pager, refs_page: u32) -> (snapshot_id: u64, ok: bool) {
	page, err := pager.get_page(p, refs_page)
	if err != .None { return 0, false }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return 0, false }

	log_count := int((^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^)
	log_next := int((^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^)
	if log_count == 0 { return 0, false }

	// Most recent entry is at index log_count - 1 in logical order
	ring_idx := (log_next - 1) % MAX_LOG_ENTRIES
	if ring_idx < 0 { ring_idx += MAX_LOG_ENTRIES }
	off := REFS_LOG_OFFSET + ring_idx * size_of(Ref_Log_Entry)
	entry := (^Ref_Log_Entry)(raw_data(data[off:]))^

	(^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^ = u32(log_count - 1)
	pager.mark_dirty(p, refs_page)
	return entry.snapshot_id, true
}

log_read_range :: proc(
	p: ^pager.Pager,
	refs_page: u32,
	start_idx: int,
	count: int,
	allocator := context.allocator,
) -> []Ref_Log_Entry {
	if refs_page == 0 || count <= 0 { return nil }
	s_idx := max(start_idx, 0)

	page, err := pager.get_page(p, refs_page)
	if err != .None { return nil }
	defer pager.unpin_page(p, refs_page)

	data := page.data
	if string(data[:len(REFS_MAGIC)]) != REFS_MAGIC { return nil }

	log_count := int((^u32)(raw_data(data[len(REFS_MAGIC) + 4:]))^)
	log_next := int((^u32)(raw_data(data[len(REFS_MAGIC) + 8:]))^)
	if s_idx >= log_count { return nil }

	available := min(count, log_count - s_idx)
	result := make([]Ref_Log_Entry, available, allocator)
	for i in 0 ..< available {
		ring_idx := (log_next - log_count + s_idx + i) % MAX_LOG_ENTRIES
		if ring_idx < 0 { ring_idx += MAX_LOG_ENTRIES }
		off := REFS_LOG_OFFSET + ring_idx * size_of(Ref_Log_Entry)
		result[i] = (^Ref_Log_Entry)(raw_data(data[off:]))^
	}
	return result
}
