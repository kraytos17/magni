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
	CommittedPage :: struct {
		page: u32,
		id:   u64,
	}

	committed := make([dynamic]CommittedPage, context.temp_allocator)
	defer delete(committed)

	walk_chain(
		p,
		latest_page,
		&committed,
		proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
			committed := cast(^[dynamic]CommittedPage)data
			if Snapshot_State(h.state) == .COMMITTED {
				append(committed, CommittedPage{page, h.snapshot_id})
			}
			return true
		},
	)

	total := len(committed)
	if total <= keep_count { return expired_ids }
	for i := keep_count; i < total; i += 1 {
		pg, err := pager.get_page(p, committed[i].page)
		if err == .None {
			(^Snapshot_Header)(raw_data(pg.data)).state = u8(Snapshot_State.ABANDONED)
			pager.mark_dirty(p, committed[i].page); pager.unpin_page(p, committed[i].page)
			append(&expired_ids, committed[i].id)
		}
	}
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
				set_header_state(d.p, page, h.snapshot_id, .ABANDONED)
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
