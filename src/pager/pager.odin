// Package pager provides page-level I/O, the slab page cache, WAL, freelist,
// and page bitmap.
package pager

import "core:container/bit_array"
import "core:fmt"
import "core:log"
import "core:mem"
import "core:os"
import "core:strings"
import "core:sync"
import "src:types"
import "src:util/bloom"

PAGE_CACHE_SIZE :: 256

// CACHE_TABLE_SIZE is the capacity of the page-cache lookup table: a power of
// two >= 8 * PAGE_CACHE_SIZE. Linear probing under heavy evict/insert churn
// clusters into long runs (measured: ~10% of lookups walked >=128 buckets at
// 2x sizing, max = whole table), so the extra headroom keeps the load factor
// <= 0.125 and collapses the tail. 2048 * 16B = 32 KiB, L2-resident.
CACHE_TABLE_SIZE :: 2048

// The table must stay a power of two (bucket mask) and large enough that an
// empty bucket always exists for cache_insert even at full cache occupancy.
#assert(CACHE_TABLE_SIZE & (CACHE_TABLE_SIZE - 1) == 0)
#assert(CACHE_TABLE_SIZE >= 4 * PAGE_CACHE_SIZE)

// Cache_Entry is one bucket of the open-addressed page-cache index.
// page_num == 0 marks an empty bucket (page numbers are 1-indexed).
Cache_Entry :: struct {
	page_num: u32,
	slot    : ^Page_Slot,
}

// The index entry is hot: keep it one cache-line-friendly word pair.
#assert(size_of(Cache_Entry) == 16)

Page :: struct {
	data     : []u8,
	page_num : u32,
	dirty    : bool,
	pin_count: u32,
}

Page_Slot :: struct {
	page      : Page,
	_data_buf : [types.PAGE_SIZE]u8,
	referenced: bool,
}

Page_Int_Range :: struct {
	col_index: u8,
	min_int  : i64,
	max_int  : i64,
}

// is_special_page reports whether a page carries a fixed-size leading header
// (page 1 embeds the 100-byte database header). Page-1 semantics belong to the
// pager/storage layer; consumers that COW-copy page 1 must relocate the header.
is_special_page :: proc(page_num: u32) -> bool {
	return page_num == 1
}

// Pager_Stats records hot-path counts for a measurement floor. Counters
// are always kept (plain integer increments, release-mode cheap); reporting is
// opt-in via pager_stats_report / the MAGNI_PAGER_STATS env flag at close.
PROBE_HIST_BUCKETS :: 8 // 1, 2-3, 4-7, 8-15, 16-31, 32-63, 64-127, 128+

Pager_Stats :: struct {
	get_page_calls  : u64,
	get_page_hits   : u64,
	get_page_misses : u64,
	unpin_calls     : u64,
	evict_calls     : u64, // evict_one_slot invocations
	evict_steps     : u64, // slot probes across all eviction scans
	evict_deferred  : u64, // referenced-bit clears on pass 0 (second-chance)
	evict_writebacks: u64, // dirty slots flushed during eviction
	cache_probes    : u64, // cache_lookup iterations (both hit + miss chains)
	// Probe-length distribution: histogram[b] counts lookups whose chain
	// length fell in bucket b (1, 2-3, 4-7, ... 128+). Split hit/miss so a
	// long tail on the hit side (clustering) is distinguishable from the
	// miss side (cold lookup walking to the terminal empty bucket).
	probe_hist      : [PROBE_HIST_BUCKETS]u64,
	probe_hist_miss : [PROBE_HIST_BUCKETS]u64,
	max_probe       : u64, // longest chain observed
	max_probe_miss  : u64,
	bloom_early_outs: u64, // lookups rejected by the bloom (no probe)
	bloom_probes    : u64, // bloom positives that fell through to a probe
	bloom_false_pos : u64, // ... of which the probe then missed (false positive)
}

// probe_bucket maps a chain length to its histogram bucket index.
@(private = "file")
probe_bucket :: proc(n: u64) -> int {
	switch {
	case n <= 1:
		return 0
	case n <= 3:
		return 1
	case n <= 7:
		return 2
	case n <= 15:
		return 3
	case n <= 31:
		return 4
	case n <= 63:
		return 5
	case n <= 127:
		return 6
	}
	return 7
}

Pager :: struct {
	mutex              : sync.RW_Mutex, // guards storage state; see docs/concurrency.md (acquire after db.mu)
	cache_table        : []Cache_Entry, // open-addressed index, page_num -> cache slot
	bloom              : bloom.Filter, // negative gate over cached page numbers
	free_slots         : [dynamic]^Page_Slot,
	slot_count         : u32,
	evict_hand         : u32,
	dirty_pages        : [dynamic]u32, // pages dirtied in the current WAL txn; iterated by wal_commit/abort
	file               : ^os.File,
	file_len           : i64,
	page_bitmap        : bit_array.Bit_Array,
	wal_state          : Wal_State,
	slots              : []Page_Slot,
	stats_counters     : Pager_Stats,
	file_name          : string,
	page_size          : u32,
	max_cache_pages    : u32,
	first_free_page    : u32,
	page_format_version: u32,
	allocator          : mem.Allocator,
	// Opaque B-tree statistics, owned by the btree package. The pager stores
	// them (so they survive transient btree.Tree instances) but never
	// interprets them: it clears entries via on_evict and frees via free_stats.
	stats              : rawptr,
	on_evict           : proc(data: rawptr, page_num: u32),
	free_stats         : proc(data: rawptr),
}

// pager_layout_report prints sizes/alignments of the pager's hot structures
// and the derived cache footprint. Debug aid: makes accidental padding or a
// blown-up slot visible immediately. Call explicitly or via `.pager_layout`.
pager_layout_report :: proc() {
	fmt.printf(
		"pager_layout: Cache_Entry=%d Page=%d Page_Slot=%d Pager_Stats=%d\n",
		size_of(Cache_Entry),
		size_of(Page),
		size_of(Page_Slot),
		size_of(Pager_Stats),
	)
	fmt.printf(
		"pager_layout: page_size=%d max_cache_pages=%d table_bytes=%d slot_bytes=%d bloom_bytes=%d\n",
		types.PAGE_SIZE,
		PAGE_CACHE_SIZE,
		CACHE_TABLE_SIZE * size_of(Cache_Entry),
		PAGE_CACHE_SIZE * size_of(Page_Slot),
		size_of(bloom.Filter),
	)
}

// pager_stats_report prints the collected counters (P0 measurement floor).
// Call explicitly, or set MAGNI_PAGER_STATS=1 to print at pager.close.
pager_stats_report :: proc(p: ^Pager) {
	s := p.stats_counters
	if s.get_page_calls == 0 {
		return
	}

	hit_pct := f64(s.get_page_hits) * 100.0 / f64(s.get_page_calls)
	steps_per_evict := f64(0)
	if s.evict_calls > 0 {
		steps_per_evict = f64(s.evict_steps) / f64(s.evict_calls)
	}

	probes_per_lookup := f64(s.cache_probes) / f64(s.get_page_calls)
	fmt.printf(
		"pager_stats: get_page calls=%d hits=%d misses=%d hit%%=%.1f " +
		"unpin=%d evicts=%d evict_steps=%d steps/evict=%.1f deferred=%d writebacks=%d " +
		"cache_probes=%d probes/lookup=%.2f\n",
		s.get_page_calls,
		s.get_page_hits,
		s.get_page_misses,
		hit_pct,
		s.unpin_calls,
		s.evict_calls,
		s.evict_steps,
		steps_per_evict,
		s.evict_deferred,
		s.evict_writebacks,
		s.cache_probes,
		probes_per_lookup,
	)

	b0, b1, b2, b3, b4, b5, b6, b7 :=
		s.probe_hist[0],
		s.probe_hist[1],
		s.probe_hist[2],
		s.probe_hist[3],
		s.probe_hist[4],
		s.probe_hist[5],
		s.probe_hist[6],
		s.probe_hist[7]
	fmt.printf(
		"pager_probe_hist: 1=%d 2-3=%d 4-7=%d 8-15=%d 16-31=%d 32-63=%d 64-127=%d 128+=%d max=%d\n",
		b0,
		b1,
		b2,
		b3,
		b4,
		b5,
		b6,
		b7,
		s.max_probe,
	)

	m0, m1, m2, m3, m4, m5, m6, m7 :=
		s.probe_hist_miss[0],
		s.probe_hist_miss[1],
		s.probe_hist_miss[2],
		s.probe_hist_miss[3],
		s.probe_hist_miss[4],
		s.probe_hist_miss[5],
		s.probe_hist_miss[6],
		s.probe_hist_miss[7]
	fmt.printf(
		"pager_probe_hist_miss: 1=%d 2-3=%d 4-7=%d 8-15=%d 16-31=%d 32-63=%d 64-127=%d 128+=%d max=%d\n",
		m0,
		m1,
		m2,
		m3,
		m4,
		m5,
		m6,
		m7,
		s.max_probe_miss,
	)

	fp_pct := f64(0)
	if s.bloom_probes > 0 {
		fp_pct = f64(s.bloom_false_pos) * 100.0 / f64(s.bloom_probes)
	}

	fmt.printf(
		"pager_bloom: early_outs=%d probes=%d false_pos=%d fp%%=%.3f\n",
		s.bloom_early_outs,
		s.bloom_probes,
		s.bloom_false_pos,
		fp_pct,
	)
}

Error :: enum u8 {
	None,
	File_Open_Failed,
	IO_Error,
	Out_Of_Memory,
	Cache_Full,
	Page_Not_Found,
	Invalid_Page_Num,
}

@(private)
cache_bucket :: proc(page_num: u32) -> u32 { return page_num & (CACHE_TABLE_SIZE - 1) }

// cache_lookup finds the cached slot for page_num, or nil. Linear-probes from
// the home bucket; page_num == 0 terminates the probe chain (empty bucket).
@(private)
cache_lookup :: proc(p: ^Pager, page_num: u32) -> ^Page_Slot {
	if !bloom.might_contain(&p.bloom, page_num) {
		p.stats_counters.bloom_early_outs += 1
		return nil
	}

	p.stats_counters.bloom_probes += 1
	i := cache_bucket(page_num)
	chain: u64 = 0
	for p.cache_table[i].page_num != 0 {
		chain += 1
		p.stats_counters.cache_probes += 1
		if p.cache_table[i].page_num == page_num {
			p.stats_counters.probe_hist[probe_bucket(chain)] += 1
			if chain > p.stats_counters.max_probe {
				p.stats_counters.max_probe = chain
			}
			return p.cache_table[i].slot
		}
		i = (i + 1) & (CACHE_TABLE_SIZE - 1)
	}

	chain += 1 // terminal empty-bucket probe
	p.stats_counters.cache_probes += 1
	p.stats_counters.bloom_false_pos += 1 // bloom said maybe, table says no
	p.stats_counters.probe_hist_miss[probe_bucket(chain)] += 1
	if chain > p.stats_counters.max_probe_miss {
		p.stats_counters.max_probe_miss = chain
	}
	if chain > p.stats_counters.max_probe {
		p.stats_counters.max_probe = chain
	}
	return nil
}

// cache_insert adds or refreshes the entry for page_num. The table is sized for
// at most PAGE_CACHE_SIZE live entries, so an empty bucket always exists. Only
// the new-bucket branch touches the bloom — a refresh must not re-add, or the
// counters would leak upward and saturate.
@(private)
cache_insert :: proc(p: ^Pager, page_num: u32, slot: ^Page_Slot) {
	i := cache_bucket(page_num)
	for p.cache_table[i].page_num != 0 {
		if p.cache_table[i].page_num == page_num {
			p.cache_table[i].slot = slot
			return
		}
		i = (i + 1) & (CACHE_TABLE_SIZE - 1)
	}

	p.cache_table[i] = Cache_Entry {
		page_num = page_num,
		slot     = slot,
	}
	bloom.add(&p.bloom, page_num)
}

// cache_delete removes the entry for page_num using backward-shift deletion so
// probe chains for entries placed after the removed bucket stay intact.
@(private)
cache_delete :: proc(p: ^Pager, page_num: u32) {
	i := cache_bucket(page_num)
	for p.cache_table[i].page_num != 0 && p.cache_table[i].page_num != page_num {
		i = (i + 1) & (CACHE_TABLE_SIZE - 1)
	}
	if p.cache_table[i].page_num == 0 {
		return
	} // not present: no bloom change

	bloom.remove(&p.bloom, page_num)
	p.cache_table[i] = {}
	j := i
	for {
		j = (j + 1) & (CACHE_TABLE_SIZE - 1)
		if p.cache_table[j].page_num == 0 {
			break
		}

		k := cache_bucket(p.cache_table[j].page_num)
		in_range := k > i && k <= j if i <= j else (k > i || k <= j)
		if !in_range {
			p.cache_table[i] = p.cache_table[j]
			p.cache_table[j] = {}
			i = j
		}
	}
}

@(private)
find_slot :: proc(p: ^Pager, page_num: u32) -> ^Page_Slot { return cache_lookup(p, page_num) }

@(private)
find_empty_slot :: proc(p: ^Pager) -> ^Page_Slot {
	if p.slot_count >= p.max_cache_pages {
		if evict_one_slot(p) != .None {
			return nil
		}
	}
	if len(p.free_slots) == 0 {
		return nil
	}

	slot := pop(&p.free_slots)
	slot.page.data = slot._data_buf[:]
	slot.referenced = false
	p.slot_count += 1
	return slot
}

// evict_slot unlinks one cached slot and returns it to the free pool.
// Caller MUST hold p.mutex (write-locked). writeback=true flushes a dirty
// page to WAL first (clock eviction); false drops without I/O (abort/rewind
// paths where the content is unreachable by construction, and free_page
// which already wrote its link frame). On writeback I/O failure the slot is
// left untouched and the error propagates.
@(private)
evict_slot :: proc(p: ^Pager, slot: ^Page_Slot, writeback: bool) -> Error {
	if writeback && slot.page.dirty {
		wal_append_frame(p, slot.page.page_num, slot.page.data, false, 0) or_return
		p.stats_counters.evict_writebacks += 1
	}

	cache_delete(p, slot.page.page_num)
	if p.on_evict != nil {
		p.on_evict(p.stats, slot.page.page_num)
	}

	slot.page = {}
	slot.referenced = false
	p.slot_count -= 1

	append(&p.free_slots, slot)
	return .None
}

@(private = "file")
evict_one_slot :: proc(p: ^Pager) -> Error {
	n := len(p.slots)
	p.stats_counters.evict_calls += 1
	for pass := 0; pass < 2; pass += 1 {
		for _ in 0 ..< n {
			idx := int(p.evict_hand) % n
			slot := &p.slots[idx]
			p.evict_hand = u32((int(p.evict_hand) + 1) % n)
			p.stats_counters.evict_steps += 1
			if slot.page.page_num == 0 || slot.page.pin_count > 0 {
				continue
			}
			if slot.referenced {
				slot.referenced = false
				if pass == 0 {
					p.stats_counters.evict_deferred += 1
					continue
				}
			}

			evict_slot(p, slot, true) or_return
			return .None
		}
	}
	return .Cache_Full
}

// Evict_Report counts an abort-time cache purge. Pinned skips are fail-open
// survivals: pins shouldn't exist at a statement boundary, so a skipped page
// is left for the next GC rather than destroyed.
Evict_Report :: struct {
	evicted       : u32,
	skipped_pinned: u32,
}

// evict_aborted drops cached copies of aborted-txn pages WITHOUT writeback:
// after wal_abort_txn their content is unreachable by construction (live
// roots restored, WAL frames dropped). Mirrors evict_one_slot minus the WAL
// frame. Takes p.mutex itself; callers must hold db.mu at most (never
// p.mutex) to respect the db.mu -> p.mutex order.
@(private)
evict_aborted :: proc(p: ^Pager, pages: []u32) -> (report: Evict_Report) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	for page_num in pages {
		if page_num == 0 {
			continue
		}

		slot := find_slot(p, page_num)
		if slot == nil {
			continue
		}
		if slot.page.pin_count > 0 {
			report.skipped_pinned += 1
			continue
		}

		evict_slot(p, slot, false)
		report.evicted += 1
	}
	return report
}

// Open (or create) a database file at path. Initializes the pager, page cache,
// WAL, and page bitmap. Returns nil + error on failure.
open :: proc(
	path: string,
	max_pages: u32 = PAGE_CACHE_SIZE,
	allocator := context.allocator,
) -> (
	^Pager,
	Error,
) {
	p := new(Pager, allocator)
	if p == nil {
		return nil, .Out_Of_Memory
	}

	p.allocator = allocator; p.page_size = types.PAGE_SIZE
	p.max_cache_pages = clamp(max(max_pages, 1), 1, PAGE_CACHE_SIZE)
	p.slots = make([]Page_Slot, p.max_cache_pages, allocator)
	p.cache_table = make([]Cache_Entry, CACHE_TABLE_SIZE, allocator)
	p.wal_state.page_index = make(map[u32]i64, allocator)
	p.wal_state.txn_index = make(map[u32]i64, allocator)
	p.free_slots = make([dynamic]^Page_Slot, 0, p.max_cache_pages, allocator)
	p.dirty_pages = make([dynamic]u32, 0, 32, allocator)
	p.page_format_version = u32(types.PAGE_FORMAT_VERSION)
	p.slot_count = 0
	for i in 0 ..< p.max_cache_pages {
		append(&p.free_slots, &p.slots[i])
	}

	flags := os.O_RDWR | os.O_CREATE
	file, open_err := os.open(path, flags)
	if open_err != nil {
		free(p)
		return nil, .File_Open_Failed
	}

	p.file = file; p.file_name = strings.clone(path, allocator)
	file_size, size_err := os.file_size(file)
	if size_err != nil {
		os.close(file)
		free(p)
		return nil, .IO_Error
	}

	p.file_len = file_size if file_size != 0 else 0
	page_count := u32(p.file_len / i64(p.page_size))
	bit_array.init(&p.page_bitmap, int(page_count), 0, p.allocator)
	for i := 0; i < len(p.page_bitmap.bits); i += 1 {
		p.page_bitmap.bits[i] = ~u64(0)
	}
	if err := wal_open(p, path); err != .None {
		log.errorf("Pager: WAL open failed: %v", err)
		os.close(file)
		free(p)
		return nil, err
	}
	return p, .None
}

// Close the pager: flush WAL, checkpoint to main file, close file, free all resources.
close :: proc(p: ^Pager) -> Error {
	if p == nil {
		return .None
	}

	wal_begin_txn(p)
	wal_commit_txn(p)
	wal_checkpoint(p)
	wal_close(p)
	if p.file != nil {
		os.close(p.file)
	}
	if len(os.get_env("MAGNI_PAGER_STATS", context.temp_allocator)) > 0 {
		pager_stats_report(p)
	}

	delete(p.file_name)
	delete(p.cache_table)
	bit_array.destroy(&p.page_bitmap)

	delete(p.free_slots)
	delete(p.dirty_pages)
	if p.free_stats != nil {
		p.free_stats(p.stats)
	}

	delete(p.slots)
	free(p, p.allocator)
	return .None
}

// Page numbers are 1-indexed; 0 is the sentinel for "no page".
get_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	if page_num < 1 {
		return nil, .Invalid_Page_Num
	}

	sync.rw_mutex_lock(&p.mutex)
	defer sync.rw_mutex_unlock(&p.mutex)

	p.stats_counters.get_page_calls += 1
	if slot := find_slot(p, page_num); slot != nil {
		p.stats_counters.get_page_hits += 1
		slot.page.pin_count += 1
		slot.referenced = true
		return &slot.page, .None
	}

	p.stats_counters.get_page_misses += 1
	max_page := u32(p.file_len / i64(p.page_size))
	if page_num > max_page {
		return nil, .Page_Not_Found
	}

	slot := find_empty_slot(p)
	if slot == nil {
		return nil, .Cache_Full
	}

	ws := &p.wal_state
	fo: i64
	has_fo := false
	{
		v, txn_ok := ws.txn_index[page_num]
		if txn_ok {
			fo = v
			has_fo = true
		}
	}
	if !has_fo {
		v, idx_ok := ws.page_index[page_num]
		if idx_ok {
			fo = v
			has_fo = true
		}
	}
	if has_fo {
		_, read_err := os.read_at(ws.file, slot._data_buf[:], fo + types.WAL_FRAME_HEADER_SIZE)
		if read_err == nil {
			slot.page.page_num = page_num; slot.page.pin_count = 1; slot.page.dirty = false
			cache_insert(p, page_num, slot)
			return &slot.page, .None
		}
	}

	offset := i64(page_num - 1) * i64(p.page_size)
	bytes_read, read_err := os.read_at(p.file, slot._data_buf[:], offset)
	if read_err != nil || bytes_read < int(p.page_size) {
		slot.page = {}
		p.slot_count -= 1
		append(&p.free_slots, slot)
		return nil, .IO_Error
	}

	slot.page.page_num = page_num
	slot.page.pin_count = 1
	slot.page.dirty = false
	cache_insert(p, page_num, slot)
	return &slot.page, .None
}

// Allocate a new page (from freelist or extending the file). Returns a pinned,
// dirty page zeroed to DATABASE_HEADER_SIZE. Caller must unpin when done.
allocate_page :: proc(p: ^Pager) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if p.first_free_page != 0 {
		return alloc_from_freelist(p)
	}

	slot := find_empty_slot(p)
	if slot == nil {
		return nil, .Cache_Full
	}

	new_page_num := u32(p.file_len / i64(p.page_size)) + 1
	mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
	slot.page.page_num = new_page_num; slot.page.pin_count = 1

	mark_slot_dirty(p, slot)
	cache_insert(p, new_page_num, slot)

	p.file_len += i64(p.page_size)
	bit_array.set(&p.page_bitmap, new_page_num, true, p.allocator)
	return &slot.page, .None
}

get_or_allocate_page :: proc(p: ^Pager, page_num: u32) -> (^Page, Error) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	if page_num < 1 {
		return nil, .Page_Not_Found
	}
	if slot := find_slot(p, page_num); slot != nil {
		slot.page.pin_count += 1
		return &slot.page, .None
	}

	current_max := u32(p.file_len / i64(p.page_size))
	if page_num == current_max + 1 {
		slot := find_empty_slot(p)
		if slot == nil {
			return nil, .Cache_Full
		}

		mem.set(raw_data(slot._data_buf[:]), 0, types.DATABASE_HEADER_SIZE)
		slot.page.page_num = page_num; slot.page.pin_count = 1

		mark_slot_dirty(p, slot)
		cache_insert(p, page_num, slot)

		p.file_len += i64(p.page_size)
		bit_array.set(&p.page_bitmap, page_num, true, p.allocator)
		return &slot.page, .None
	}
	return nil, .Page_Not_Found
}

// Decrement the pin count for a page. When pin_count reaches 0, the page is eligible for eviction.
unpin_page :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	p.stats_counters.unpin_calls += 1
	if slot := find_slot(p, page_num); slot != nil && slot.page.pin_count > 0 {
		slot.page.pin_count -= 1
	}
}

// Logical page count (may differ from actual file size with WAL).
page_count :: proc(p: ^Pager) -> u32 {
	sync.rw_mutex_shared_lock(&p.mutex); defer sync.rw_mutex_shared_unlock(&p.mutex)
	return u32(p.file_len / i64(p.page_size))
}

page_in_cache :: proc(p: ^Pager, page_num: u32) -> bool {
	sync.rw_mutex_shared_lock(&p.mutex); defer sync.rw_mutex_shared_unlock(&p.mutex)
	return find_slot(p, page_num) != nil
}

// Copy the content of src_page_num to a newly allocated page. Source is unpinned,
// destination is pinned + dirty. Used by COW operations.
copy_page :: proc(p: ^Pager, src_page_num: u32) -> (dst: ^Page, err: Error) {
	src: ^Page
	src, err = get_page(p, src_page_num)
	if err != .None {
		return
	}

	defer unpin_page(p, src_page_num)
	dst = allocate_page(p) or_return
	mem.copy_non_overlapping(raw_data(dst.data), raw_data(src.data), types.PAGE_SIZE)
	dst.dirty = true
	return
}

// mark_slot_dirty marks a cached page dirty and records it in the current WAL
// txn's dirty list so wal_commit_txn/wal_abort_txn need only scan dirtied pages.
// Caller must hold p.mutex.
@(private)
mark_slot_dirty :: proc(p: ^Pager, slot: ^Page_Slot) {
	if slot == nil || slot.page.page_num == 0 {
		return
	}
	if !slot.page.dirty {
		slot.page.dirty = true
		append(&p.dirty_pages, slot.page.page_num)
	}
}

mark_dirty :: proc(p: ^Pager, page_num: u32) {
	sync.rw_mutex_lock(&p.mutex); defer sync.rw_mutex_unlock(&p.mutex)
	mark_slot_dirty(p, find_slot(p, page_num))
}
