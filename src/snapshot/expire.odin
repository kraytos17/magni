package snapshot

import "src:btree"
import "src:pager"
import "src:types"

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

expire_and_collect :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int) {
	_ = expire_snapshots(p, latest_page, keep_count)
	max_page := pager.page_count(p)
	if max_page < GC_MIN_PAGES { return }

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
				_, roots, load_ok := load_manifest_tables(
					p,
					h.manifest_page,
					context.temp_allocator,
				)
				if load_ok {
					for i in 0 ..< len(roots) {
						if roots[i] !=
						   0 { live[roots[i]] = true; btree.collect_pages(&t, roots[i], &live) }
					}
				}
			}
		}
		count += 1; page = h.prev_snapshot
	}
	// Sweep using page bitmap to skip free page ranges
	bm := p.page_bitmap
	if len(bm) > 0 {
		for i := 0; i < len(bm); i += 1 {
			word := bm[i]
			if word == 0 { continue }

			base := u32(i) * 64
			for bit := uint(0); bit < 64; bit += 1 {
				pn := base + u32(bit)
				if pn > max_page { break }
				if pn >= 2 && (word & (u64(1) << bit)) != 0 && pn not_in live {
					pager.free_page(p, pn)
				}
			}
		}
	} else {
		for pn := u32(2); pn <= max_page; pn += 1 {
			if pn not_in live { pager.free_page(p, pn) }
		}
	}

	// Truncate file_len to the highest live page so future GC scans skip freed pages
	// After GC, shrink the logical file length to the highest live page so
	// future GC scans skip the freed tail region entirely.
	highest_live: u32
	for pn, _ in live {
		if pn > highest_live { highest_live = pn }
	}

	current_max := pager.page_count(p)
	if highest_live > 0 && highest_live < current_max {
		new_len := i64(highest_live) * i64(types.PAGE_SIZE)
		if new_len > 0 { p.file_len = new_len }
	}
}
