package snapshot

import "src:btree"
import "src:pager"

@(private)
walk_chain_data :: struct {
	p:         ^pager.Pager,
	max_keep:  int,
	committed: int,
	target_id: u64,
	result:    ^Snapshot_Header,
	found:     ^bool,
	count:     ^int,
}

count_committed :: proc(p: ^pager.Pager, start_page: u32) -> int {
	count := 0
	walk_chain(p, start_page, &count, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		if Snapshot_State(h.state) == .COMMITTED { (cast(^int)data)^ += 1 }; return true
	})
	return count
}

prune :: proc(p: ^pager.Pager, start_page: u32, max_keep: int) {
	total := count_committed(p, start_page)
	if total <= max_keep { return }
	d := walk_chain_data{p = p, max_keep = max_keep}
	walk_chain(p, start_page, &d, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		d := cast(^walk_chain_data)data
		if Snapshot_State(h.state) == .COMMITTED {
			d.committed += 1
			if d.committed > d.max_keep {
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
		if h.schema_root != 0 {
			live[h.schema_root] = true
			t := btree.init(p, h.schema_root)
			btree.collect_pages(&t, h.schema_root, &live)
			if h.manifest_page != 0 {
				_, roots, load_ok := load_manifest_tables(p, h.manifest_page, context.temp_allocator)
				if load_ok {
					for i in 0 ..< len(roots) {
						if roots[i] != 0 { live[roots[i]] = true; btree.collect_pages(&t, roots[i], &live) }
					}
				}
			}
		}
		count += 1; page = h.prev_snapshot
	}
	max_page := pager.page_count(p)
	for pn := u32(2); pn <= max_page; pn += 1 {
		if pn not_in live { pager.free_page(p, pn) }
	}
}
