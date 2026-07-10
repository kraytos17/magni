package snapshot

import "core:encoding/endian"
import "core:fmt"
import "core:log"
import "core:mem"
import "core:time"
import "src:pager"
import "src:types"

SNAPSHOT_MAGIC :: "MAGNISNP"
MAX_HEADERS_PER_PAGE :: 100
HEADER_PREFIX_SIZE :: 8

Snapshot_Operation :: enum u8 {
	UNKNOWN = 0,
	INSERT  = 1,
	UPDATE  = 2,
	DELETE  = 3,
	CREATE  = 4,
	DROP    = 5,
	COMMIT  = 6,
	RESTORE = 7,
}

Snapshot_Header :: struct #packed #all_or_none #simple {
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

Snapshot_State :: enum u8 {
	PENDING   = 0,
	COMMITTED = 1,
	ABANDONED = 2,
}

TAG_OFFSET :: HEADER_PREFIX_SIZE + MAX_HEADERS_PER_PAGE * size_of(Snapshot_Header)
TAG_SIZE :: 64

Find_By_Id_Data :: struct {
	result:    ^Snapshot_Header,
	found:     ^bool,
	target_id: u64,
}

Find_Ts_Data :: struct {
	result:    ^Snapshot_Header,
	found:     ^bool,
	target_ts: u64,
}

Debug_Data :: struct {
	p:     ^pager.Pager,
	count: int,
}

create :: proc(
	p: ^pager.Pager,
	snapshot_id: u64,
	prev_snapshot: u32,
	schema_root: u32,
	manifest_page: u32 = 0,
	operation: Snapshot_Operation = .UNKNOWN,
	timestamp: u64 = 0,
) -> (
	snapshot_page: u32,
	ok: bool,
) {
	if prev_snapshot != 0 {
		pg, pg_err := pager.get_page(p, prev_snapshot)
		if pg_err == .None {
			count := int(endian.unchecked_get_u32le(pg.data[:4]))
			if count > 0 && count < MAX_HEADERS_PER_PAGE {
				offset := HEADER_PREFIX_SIZE + count * size_of(Snapshot_Header)
				h := (^Snapshot_Header)(raw_data(pg.data[offset:]))
				copy(h.magic[:], SNAPSHOT_MAGIC)
				h.snapshot_id = snapshot_id
				h.timestamp =
					timestamp if timestamp != 0 else u64(time.now()._nsec / types.NANOS_PER_MICRO)

				h.schema_root = schema_root
				h.manifest_page = manifest_page
				h.state = u8(Snapshot_State.COMMITTED)
				h.operation = u8(operation)
				// prev_snapshot inherits from the first header on this page
				first := (^Snapshot_Header)(raw_data(pg.data[HEADER_PREFIX_SIZE:]))
				h.prev_snapshot = first.prev_snapshot
				endian.unchecked_put_u32le(pg.data[:4], u32(count + 1))
				pager.mark_dirty(p, prev_snapshot)
				pager.unpin_page(p, prev_snapshot)
				return prev_snapshot, true
			}
			pager.unpin_page(p, prev_snapshot)
		}
	}

	page, err := pager.allocate_page(p)
	if err != .None {
		log.error("Snapshot: failed to allocate page")
		return 0, false
	}
	defer pager.unpin_page(p, page.page_num)

	mem.set(raw_data(page.data), 0, HEADER_PREFIX_SIZE)
	endian.unchecked_put_u32le(page.data[:4], 1)
	h := (^Snapshot_Header)(raw_data(page.data[HEADER_PREFIX_SIZE:]))
	copy(h.magic[:], SNAPSHOT_MAGIC)

	h.snapshot_id = snapshot_id
	h.prev_snapshot = prev_snapshot
	h.timestamp = timestamp if timestamp != 0 else u64(time.now()._nsec / types.NANOS_PER_MICRO)
	h.schema_root = schema_root
	h.manifest_page = manifest_page
	h.state = u8(Snapshot_State.COMMITTED)
	h.operation = u8(operation)
	pager.mark_dirty(p, page.page_num)
	return page.page_num, true
}

load :: proc(
	p: ^pager.Pager,
	snapshot_page: u32,
	snapshot_id: u64 = 0,
) -> (
	Snapshot_Header,
	bool,
) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return {}, false }
	defer pager.unpin_page(p, snapshot_page)

	count := int(endian.unchecked_get_u32le(page.data[:4]))
	if count > 0 && count <= MAX_HEADERS_PER_PAGE {
		headers := transmute([]Snapshot_Header)page.data[HEADER_PREFIX_SIZE:HEADER_PREFIX_SIZE +
		count * size_of(Snapshot_Header)]
		if snapshot_id == 0 {
			return headers[count - 1], true
		}
		for i := count - 1; i >= 0; i -= 1 {
			if headers[i].snapshot_id == snapshot_id {
				return headers[i], true
			}
		}
		return {}, false
	}

	h := (^Snapshot_Header)(raw_data(page.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC {
		return {}, false
	}
	return h^, true
}

set_tag :: proc(p: ^pager.Pager, snapshot_page: u32, tag: string) {
	page, err := pager.get_page(p, snapshot_page)
	if err != .None { return }
	defer pager.unpin_page(p, snapshot_page)

	count := int(endian.unchecked_get_u32le(page.data[:4]))
	if count > 0 && count <= MAX_HEADERS_PER_PAGE {
		if len(page.data) >= TAG_OFFSET + TAG_SIZE {
			data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
			n := min(len(tag), TAG_SIZE - 1)
			mem.set(raw_data(data), 0, TAG_SIZE)
			copy(data, tag[:n])
			pager.mark_dirty(p, snapshot_page)
		}
		return
	}

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

	count := int(endian.unchecked_get_u32le(page.data[:4]))
	if count > 0 && count <= MAX_HEADERS_PER_PAGE {
		if len(page.data) >= TAG_OFFSET + TAG_SIZE {
			data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
			length := 0
			for length < TAG_SIZE && data[length] != 0 { length += 1 }
			return string(data[:length])
		}
		return ""
	}

	data := page.data[TAG_OFFSET:TAG_OFFSET + TAG_SIZE]
	length := 0
	for length < TAG_SIZE && data[length] != 0 { length += 1 }
	return string(data[:length])
}

walk_chain :: proc(
	p: ^pager.Pager,
	start_page: u32,
	data: rawptr,
	callback: proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool,
) {
	page := start_page
	for page != 0 {
		pg, err := pager.get_page(p, page)
		if err != .None { break }

		count := int(endian.unchecked_get_u32le(pg.data[:4]))
		next_page: u32
		if count > 0 && count <= MAX_HEADERS_PER_PAGE {
			headers := transmute([]Snapshot_Header)pg.data[HEADER_PREFIX_SIZE:HEADER_PREFIX_SIZE +
			count * size_of(Snapshot_Header)]
			next_page = headers[0].prev_snapshot
			for i := count - 1; i >= 0; i -= 1 {
				if !callback(headers[i], page, data) {
					pager.unpin_page(p, page)
					return
				}
			}
		} else {
			h := (^Snapshot_Header)(raw_data(pg.data))
			if string(h.magic[:]) != SNAPSHOT_MAGIC {
				pager.unpin_page(p, page)
				break
			}

			next_page = h.prev_snapshot
			if !callback(h^, page, data) {
				pager.unpin_page(p, page)
				return
			}
		}
		pager.unpin_page(p, page)
		page = next_page
	}
}

list_snapshots :: proc(
	p: ^pager.Pager,
	latest_page: u32,
	allocator := context.allocator,
) -> []Snapshot_Header {
	result := make([dynamic]Snapshot_Header, allocator)
	walk_chain(p, latest_page, &result, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		append(cast(^[dynamic]Snapshot_Header)data, h)
		return true
	})
	return result[:]
}

find_by_id :: proc(p: ^pager.Pager, start_page: u32, target_id: u64) -> (Snapshot_Header, bool) {
	result: Snapshot_Header
	found := false
	d := Find_By_Id_Data{&result, &found, target_id}

	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Find_By_Id_Data)data
		if h.snapshot_id == d.target_id {
			d.result^ = h
			d.found^ = true
			return false
		}
		return true
	})
	return result, found
}

find_by_timestamp :: proc(
	p: ^pager.Pager,
	start_page: u32,
	target_ts: u64,
) -> (
	Snapshot_Header,
	bool,
) {
	result: Snapshot_Header; found := false
	d := Find_Ts_Data{&result, &found, target_ts}

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

debug_print_chain :: proc(p: ^pager.Pager, start_page: u32) {
	d := Debug_Data {
		p = p,
	}

	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Debug_Data)data
		tag := get_tag(d.p, page)
		buf := fmt.tprintf(
			"  Snapshot %-4d  page=%-4d  op=%-6s  state=%-9s  ts=%d",
			h.snapshot_id,
			page,
			Snapshot_Operation(h.operation),
			Snapshot_State(h.state),
			h.timestamp,
		)

		if tag != "" { buf = fmt.tprintf("%s  tag=%s", buf, tag) }
		log.debug(buf)
		d.count += 1
		return true
	})
	if d.count == 0 { log.debug("  (empty)") }
	log.debug("======================")
}

print_chain :: proc(p: ^pager.Pager, start_page: u32) {
	walk_chain(p, start_page, p, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		p := cast(^pager.Pager)data
		tag := get_tag(p, page)
		fmt.printf("#%-5d %-8s ts=%d", h.snapshot_id, Snapshot_Operation(h.operation), h.timestamp)
		if tag != "" { fmt.printf("  [%s]", tag) }
		fmt.println()
		return true
	})
}

// set_header_state modifies the state of a specific snapshot header on a page.
// Handles both old format (single header per page) and packed format (multiple headers).
set_header_state :: proc(
	p: ^pager.Pager,
	page: u32,
	snapshot_id: u64,
	state: Snapshot_State,
) -> bool {
	pg, err := pager.get_page(p, page)
	if err != .None { return false }
	defer pager.unpin_page(p, page)

	count := int(endian.unchecked_get_u32le(pg.data[:4]))
	if count > 0 && count <= MAX_HEADERS_PER_PAGE {
		// Packed format: find the header with matching snapshot_id
		headers := transmute([]Snapshot_Header)pg.data[HEADER_PREFIX_SIZE:HEADER_PREFIX_SIZE +
		count * size_of(Snapshot_Header)]
		for i in 0 ..< count {
			if headers[i].snapshot_id == snapshot_id {
				headers[i].state = u8(state)
				pager.mark_dirty(p, page)
				return true
			}
		}
		return false
	}
	// Old format: single header on page
	h := (^Snapshot_Header)(raw_data(pg.data))
	if string(h.magic[:]) != SNAPSHOT_MAGIC { return false }
	h.state = u8(state)
	pager.mark_dirty(p, page)
	return true
}
