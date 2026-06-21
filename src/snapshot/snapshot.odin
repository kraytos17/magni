package snapshot

import "core:fmt"
import "core:mem"
import "core:time"
import "src:pager"

SNAPSHOT_MAGIC :: "MAGNISNP"

Snapshot_Operation :: enum u8 {
	UNKNOWN = 0, INSERT = 1, UPDATE = 2, DELETE = 3,
	CREATE = 4, DROP = 5, COMMIT = 6, RESTORE = 7,
}

Snapshot_Header :: struct #packed {
	magic:         [8]u8,
	snapshot_id:   u64,
	prev_snapshot: u32,
	timestamp:     u64,
	schema_root:   u32,
	manifest_page: u32,
	state:         u8,
	operation:     u8,
	padding:       [2]u8,
}

#assert(size_of(Snapshot_Header) == 40)

Snapshot_State :: enum u8 { PENDING = 0, COMMITTED = 1, ABANDONED = 2 }

TAG_OFFSET :: size_of(Snapshot_Header)
TAG_SIZE :: 64

Find_By_Id_Data :: struct { result: ^Snapshot_Header, found: ^bool, target_id: u64 }
Find_Ts_Data :: struct { result: ^Snapshot_Header, found: ^bool, target_ts: u64 }
Debug_Data :: struct { p: ^pager.Pager, count: int }

create :: proc(p: ^pager.Pager, snapshot_id: u64, prev_snapshot: u32, schema_root: u32, manifest_page: u32 = 0, operation: Snapshot_Operation = .UNKNOWN) -> (snapshot_page: u32, ok: bool) {
	page, err := pager.allocate_page(p)
	if err != .None { fmt.eprintln("Snapshot: failed to allocate page"); return 0, false }
	defer pager.unpin_page(p, page.page_num)
	h := (^Snapshot_Header)(raw_data(page.data))
	copy(h.magic[:], SNAPSHOT_MAGIC)
	h.snapshot_id = snapshot_id; h.prev_snapshot = prev_snapshot
	h.timestamp = u64(time.now()._nsec / 1000)
	h.schema_root = schema_root; h.manifest_page = manifest_page
	h.state = u8(Snapshot_State.COMMITTED); h.operation = u8(operation)
	pager.mark_dirty(p, page.page_num)
	return page.page_num, true
}

load :: proc(p: ^pager.Pager, snapshot_page: u32) -> (Snapshot_Header, bool) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return {}, false }
	defer pager.unpin_page(p, snapshot_page)
	h := (^Snapshot_Header)(raw_data(page.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC { fmt.eprintln("Snapshot: invalid magic on page", snapshot_page); return {}, false }
	return h^, true
}

set_tag :: proc(p: ^pager.Pager, snapshot_page: u32, tag: string) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return }
	defer pager.unpin_page(p, snapshot_page)
	if string((^Snapshot_Header)(raw_data(page.data)).magic[:]) != SNAPSHOT_MAGIC { return }
	data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
	n := min(len(tag), TAG_SIZE - 1)
	mem.set(raw_data(data), 0, TAG_SIZE)
	copy(data, tag[:n])
	pager.mark_dirty(p, snapshot_page)
}

get_tag :: proc(p: ^pager.Pager, snapshot_page: u32) -> string {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return "" }
	defer pager.unpin_page(p, snapshot_page)
	if string((^Snapshot_Header)(raw_data(page.data)).magic[:]) != SNAPSHOT_MAGIC { return "" }
	data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
	length := 0
	for length < TAG_SIZE && data[length] != 0 { length += 1 }
	return string(data[:length])
}

@(private)
walk_chain :: proc(p: ^pager.Pager, start_page: u32, data: rawptr, callback: proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool) {
	page := start_page
	for page != 0 {
		h, ok := load(p, page)
		if !ok { break }
		if !callback(h, page, data) { break }
		page = h.prev_snapshot
	}
}

list_snapshots :: proc(p: ^pager.Pager, latest_page: u32, allocator := context.allocator) -> []Snapshot_Header {
	result := make([dynamic]Snapshot_Header, allocator)
	walk_chain(p, latest_page, &result, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		append(cast(^[dynamic]Snapshot_Header)data, h); return true
	})
	return result[:]
}

find_by_id :: proc(p: ^pager.Pager, start_page: u32, target_id: u64) -> (Snapshot_Header, bool) {
	result: Snapshot_Header
	found := false
	d := Find_By_Id_Data{&result, &found, target_id}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Find_By_Id_Data)data
		if h.snapshot_id == d.target_id { d.result^ = h; d.found^ = true; return false }
		return true
	})
	return result, found
}

find_by_timestamp :: proc(p: ^pager.Pager, start_page: u32, target_ts: u64) -> (Snapshot_Header, bool) {
	result: Snapshot_Header; found := false
	d := Find_Ts_Data{&result, &found, target_ts}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Find_Ts_Data)data
		if Snapshot_State(h.state) == .COMMITTED && h.timestamp <= d.target_ts { d.result^ = h; d.found^ = true; return false }
		return true
	})
	return result, found
}

debug_print_chain :: proc(p: ^pager.Pager, start_page: u32) {
	d := Debug_Data{p = p}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Debug_Data)data
		tag := get_tag(d.p, page)
		fmt.printf("  Snapshot %-4d  page=%-4d  op=%-6s  state=%-9s  ts=%d", h.snapshot_id, page, Snapshot_Operation(h.operation), Snapshot_State(h.state), h.timestamp)
		if tag != "" { fmt.printf("  tag=%s", tag) }
		fmt.println(); d.count += 1; return true
	})
	if d.count == 0 { fmt.println("  (empty)") }
	fmt.println("======================")
}
