package snapshot

import "src:btree"
import "src:cell"
import "src:pager"

// count_committed counts COMMITTED headers reachable from start_page.
// Non-committed (pending/abandoned) headers play no role in retention and
// are excluded. Used by tests and the expire path to size the keep window.
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

// prune marks all but the newest max_keep COMMITTED snapshots ABANDONED
// (via mark_abandoned) without sweeping — the state change without the
// reclamation. Exercised through tests and ad-hoc tooling; the production
// path is expire_and_collect below.
prune :: proc(p: ^pager.Pager, start_page: u32, max_keep: int) {
	// Single mark pass shared with expire_and_collect; the ids are
	// temp-allocated and reclaimed with the caller's temp scope.
	expired := mark_abandoned(p, start_page, max_keep)
	delete(expired)
}

GC_MIN_PAGES :: 512

// build_live_set collects every page the newest keep_count snapshots can
// still reach: page 1, each snapshot + manifest page, and each manifest's
// table roots walked through collect_pages (rows) and mark_index_roots
// (secondary indexes live in schema-row values, unreachable from rowids).
// Roots are NOT pre-marked: collect_pages uses presence as its visited
// guard, so a pre-marked root returns early and drops its whole subtree
// from the live set (freed while still reachable). Unreadable links end
// that snapshot's contribution; the chain walk continues below them.
@(private)
build_live_set :: proc(p: ^pager.Pager, latest_page: u32, keep_count: int, live: ^map[u32]bool) {
	live[1] = true
	count := 0
	page := latest_page
	for page != 0 && count < keep_count {
		h, ok := load(p, page)
		if !ok {
			break
		}

		live[page] = true
		if h.manifest_page != 0 {
			live[h.manifest_page] = true
		}
		if h.schema_root != 0 {
			t := btree.init(p, h.schema_root)
			btree.collect_pages(&t, h.schema_root, live)
			mark_index_roots(p, &t, live)
			if h.manifest_page != 0 {
				_, roots, load_ok := load_manifest_tables(
					p,
					h.manifest_page,
					context.temp_allocator,
				)
				if load_ok {
					for i in 0 ..< len(roots) {
						if roots[i] != 0 {
							btree.collect_pages(&t, roots[i], live)
						}
					}
				}
			}
		}

		count += 1
		page = h.prev_snapshot
	}
}

// mark_index_roots marks secondary-index subtrees live. Index roots ride
// schema rows as VALUES (slot [6]), not child pointers, so the
// collect_pages walk above never reaches them; without this the sweep
// frees live index pages. Post-DROP INDEX the row no longer names the
// pages, so they recycle naturally — no eager free needed (snapshots may
// still reference them; DROP TABLE precedent).
@(private)
mark_index_roots :: proc(p: ^pager.Pager, t: ^btree.Tree, live: ^map[u32]bool) {
	c, c_err := btree.cursor_start(t, context.temp_allocator)
	if c_err != .None {
		return
	}

	defer btree.cursor_destroy(&c)
	for c.is_valid {
		row, get_err := btree.cursor_get_cell(&c, context.temp_allocator)
		if get_err == .None {
			// Triples from slot [6]: every complete [root INT]
			// group marks its subtree. Narrower rows (and trailing
			// partials) carry no index.
			for i := 6; i < len(row.values); i += 3 {
				if idx, ok := row.values[i].(i64); ok && idx > 0 {
					btree.collect_pages(t, u32(idx), live)
				}
			}
			cell.destroy(&row, context.temp_allocator)
		}
		btree.cursor_advance(&c)
	}
}

// sweep_dead_pages frees every allocated page (bitmap set) at or above page
// 2 that is not in the live set — allocated-ness from the bitmap, max_page
// from the pager. Page 1 and unallocated pages are never freed. No file
// truncation: freed pages recycle through the freelist; shrinking dead
// tails from aborted txns is rewind_after_abort (rollback's path, bounded
// by the allocator truth), not the sweep's.
@(private)
sweep_dead_pages :: proc(p: ^pager.Pager, live: ^map[u32]bool) {
	max_page := pager.page_count(p)
	bm := p.page_bitmap.bits
	if len(bm) > 0 {
		for i := 0; i < len(bm); i += 1 {
			word := bm[i]
			if word == 0 {
				continue
			}

			base := u32(i) * 64
			for bit := uint(0); bit < 64; bit += 1 {
				pn := base + u32(bit)
				if pn > max_page {
					break
				}
				if pn >= 2 && (word & (u64(1) << bit)) != 0 && pn not_in live^ {
					pager.free_page(p, pn)
				}
			}
		}
	} else {
		for pn := u32(2); pn <= max_page; pn += 1 {
			if pn not_in live^ {
				pager.free_page(p, pn)
			}
		}
	}
	// No file truncation by design (freed pages recycle through the
	// freelist); shrinking dead tails from aborted txns is
	// rewind_after_abort on rollback's path.
}
