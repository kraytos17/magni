package snapshot

import "src:pager"

@(private = "file")
Mark_Accum :: struct {
	p      : ^pager.Pager,
	expired: ^[dynamic]u64,
	keep   : int,
	seen   : int,
}

// mark_abandoned marks all but the newest keep_count COMMITTED snapshots
// ABANDONED and returns the expired ids in walk (newest-first) order. The
// single mark pass behind both prune and expire_and_collect. Packed-format
// aware via set_header_state: never stamp (^Snapshot_Header)(pg.data)
// directly, which would hit headers[0] regardless of which header expired.
@(private)
mark_abandoned :: proc(
	p: ^pager.Pager,
	latest_page: u32,
	keep_count: int,
	allocator := context.allocator,
) -> [dynamic]u64 {
	expired := make([dynamic]u64, allocator)
	d := Mark_Accum{p, &expired, keep_count, 0}
	walk_chain(p, latest_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^Mark_Accum)data
		if snapshot_state_from_u8(h.state) == .COMMITTED {
			d.seen += 1
			if d.seen > d.keep {
				set_header_state(d.p, page, h.snapshot_id, .ABANDONED)
				append(d.expired, h.snapshot_id)
			}
		}
		return true
	})
	return expired
}

// expire_and_collect marks all but the newest keep_count snapshots
// ABANDONED, then sweeps dead pages back into the freelist (skipped below
// GC_MIN_PAGES: scan cost dwarfs the gain on small files). Returned ids are
// temp-allocated — the caller consumes them immediately. The sweep keeps
// every page reachable from the kept snapshots (build_live_set) and frees
// the rest (sweep_dead_pages).
expire_and_collect :: proc(
	p: ^pager.Pager,
	latest_page: u32,
	keep_count: int,
) -> (
	expired_ids: [dynamic]u64,
) {
	expired_ids = mark_abandoned(p, latest_page, keep_count, context.temp_allocator)
	max_page := pager.page_count(p)
	if max_page < GC_MIN_PAGES {
		return expired_ids
	}

	live := make(map[u32]bool, context.temp_allocator)
	defer delete(live)

	build_live_set(p, latest_page, keep_count, &live)
	sweep_dead_pages(p, &live)
	return expired_ids
}
