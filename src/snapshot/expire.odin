package snapshot

import "src:pager"

expire_snapshots :: proc(
	p: ^pager.Pager,
	latest_page: u32,
	keep_count: int,
) -> (
	expired_ids: [dynamic]u64,
) {
	expired_ids = make([dynamic]u64, context.temp_allocator)
	total := 0
	walk_chain(p, latest_page, &total, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		total := cast(^int)data
		if Snapshot_State(h.state) == .COMMITTED { total^ += 1 }
		return true
	})

	if total <= keep_count { return expired_ids }

	keep := 0
	d := struct {
		p:           ^pager.Pager,
		keep:        ^int,
		keep_count:  int,
		expired_ids: ^[dynamic]u64,
	} {
		p           = p,
		keep        = &keep,
		keep_count  = keep_count,
		expired_ids = &expired_ids,
	}

	walk_chain(p, latest_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^struct {
			p:           ^pager.Pager,
			keep:        ^int,
			keep_count:  int,
			expired_ids: ^[dynamic]u64,
		})data
		if Snapshot_State(h.state) == .COMMITTED {
			d.keep^ += 1
			if d.keep^ > d.keep_count {
				pg, err := pager.get_page(d.p, page)
				if err == .None {
					(^Snapshot_Header)(raw_data(pg.data)).state = u8(Snapshot_State.ABANDONED)
					pager.mark_dirty(d.p, page); pager.unpin_page(d.p, page)
					append(d.expired_ids, h.snapshot_id)
				}
			}
		}
		return true
	})

	return expired_ids
}

_expire_snapshots_impl :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int) {
	total := 0
	walk_chain(p, latest_page, &total, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		total := cast(^int)data
		if Snapshot_State(h.state) == .COMMITTED { total^ += 1 }
		return true
	})

	if total <= keep_count { return }

	keep := 0
	d := struct {
		p:          ^pager.Pager,
		keep:       ^int,
		keep_count: int,
	} {
		p          = p,
		keep       = &keep,
		keep_count = keep_count,
	}

	walk_chain(p, latest_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^struct {
			p:          ^pager.Pager,
			keep:       ^int,
			keep_count: int,
		})data
		if Snapshot_State(h.state) == .COMMITTED {
			d.keep^ += 1
			if d.keep^ > d.keep_count {
				pg, err := pager.get_page(d.p, page)
				if err == .None {
					(^Snapshot_Header)(raw_data(pg.data)).state = u8(Snapshot_State.ABANDONED)
					pager.mark_dirty(d.p, page); pager.unpin_page(d.p, page)
				}
			}
		}
		return true
	})
}

expire_and_collect :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int) {
	_expire_snapshots_impl(p, latest_page, keep_count)
	max_page := pager.page_count(p)
	if max_page < GC_MIN_PAGES { return }

	live := make(map[u32]bool, context.temp_allocator)
	defer delete(live)

	_build_live_set(p, latest_page, keep_count, &live)
	_sweep_dead_pages(p, &live)
}
