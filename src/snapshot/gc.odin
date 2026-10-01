package snapshot

import "src:btree"
import "src:pager"

count_committed :: proc(p: ^pager.Pager, start_page: u32) -> int {
	count := 0
	walk_chain(p, start_page, &count, proc(h: Snapshot_Header, page: u32, data: rawptr) -> bool {
		if snapshot_state_from_u8(h.state) == .COMMITTED {
			(cast(^int)data)^ += 1
		}
		return true
	})
	return count
}

prune :: proc(p: ^pager.Pager, start_page: u32, max_keep: int) {
	// Single mark pass shared with expire_and_collect; the ids are
	// temp-allocated and reclaimed with the caller's temp scope.
	expired := mark_abandoned(p, start_page, max_keep)
	delete(expired)
}

GC_MIN_PAGES :: 512

@(private)
build_live_set :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int, live: ^map[u32]bool) {
	live[1] = true
	count := 0
	page := latest_page
	for page != 0 && count < keep_count {
		h, ok := load(p, page)
		if !ok { break }

		live[page] = true
		if h.manifest_page != 0 { live[h.manifest_page] = true }
		if h.schema_root != 0 {
			// NOTE: do NOT pre-mark roots before collect_pages. It marks
			// the root itself and uses presence as its visited guard, so
			// a pre-marked root returns early and its whole subtree is
			// lost from the live set (freed while still reachable).
			t := btree.init(p, h.schema_root)
			btree.collect_pages(&t, h.schema_root, live)
			if h.manifest_page != 0 {
				_, roots, load_ok := load_manifest_tables(
					p,
					h.manifest_page,
					context.temp_allocator,
				)
				if load_ok {
					for i in 0 ..< len(roots) {
						if roots[i] != 0 { btree.collect_pages(&t, roots[i], live) }
					}
				}
			}
		}
		count += 1; page = h.prev_snapshot
	}
}

@(private)
sweep_dead_pages :: proc(p: ^pager.Pager, live: ^map[u32]bool) {
	max_page := pager.page_count(p)
	bm := p.page_bitmap.bits
	if len(bm) > 0 {
		for i := 0; i < len(bm); i += 1 {
			word := bm[i]
			if word == 0 { continue }

			base := u32(i) * 64
			for bit := uint(0); bit < 64; bit += 1 {
				pn := base + u32(bit)
				if pn > max_page { break }
				if pn >= 2 && (word & (u64(1) << bit)) != 0 && pn not_in live^ {
					pager.free_page(p, pn)
				}
			}
		}
	} else {
		for pn := u32(2); pn <= max_page; pn += 1 {
			if pn not_in live^ { pager.free_page(p, pn) }
		}
	}
	// NOTE: no file truncation here by design. Freed pages recycle through
	// the freelist; on-disk shrinking of genuinely dead tails (aborted
	// transactions) is pager.rewind_after_abort, called from rollback with
	// the allocator truth as its bound.
}
