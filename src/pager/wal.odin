package pager

import "core:fmt"
import "core:hash"
import "core:log"
import "core:mem"
import "core:os"
import "core:strings"
import "core:time"
import "src:types"

WAL_Header :: struct #packed {
	magic          : [8]u8,
	format_version : u32le,
	page_size      : u32le,
	salt1          : u32le,
	salt2          : u32le,
	header_checksum: u64le,
}

WAL_Frame_Header :: struct #packed {
	page_num     : u32le,
	db_size_after: u32le,
	salt1        : u32le,
	salt2        : u32le,
	checksum1    : u32le,
	checksum2    : u32le,
}

WAL_FORMAT_VERSION :: 1

// WAL_COMMIT_PAGE marks commit-marker frames: page_num 0 with a nonzero
// db_size_after. The marker test lives here (not inline at the three scan
// sites) so wal_abort_txn, wal_checkpoint, and wal_recover can never fork
// the committed_upto rule again.
WAL_COMMIT_PAGE :: 0

// frame_is_commit reports whether a frame header is a commit marker as
// opposed to a page image.
frame_is_commit :: proc(fh: WAL_Frame_Header) -> bool {
	return u32(fh.db_size_after) != 0
}

// wal_scan_committed_upto returns the end offset of the last commit marker
// (WAL_HEADER_SIZE when there is none): frames past it are uncommitted. One
// rule shared by abort (drop), checkpoint (don't copy), and recover (don't
// replay) so the three can never fork it again. check_salt additionally
// stops at the first foreign-generation frame (crash-torn tails); only
// recover needs it, since abort/checkpoint run on a live WAL whose salts
// cannot change mid-scan.
@(private = "file")
wal_scan_committed_upto :: proc(ws: ^Wal_State, file_size: i64, check_salt: bool) -> i64 {
	committed_upto := i64(types.WAL_HEADER_SIZE)
	scan := i64(types.WAL_HEADER_SIZE)
	for scan + types.WAL_FRAME_SIZE <= file_size {
		fh_buf: [types.WAL_FRAME_HEADER_SIZE]u8
		_, read_err := os.read_at(ws.file, fh_buf[:], scan)
		if read_err != nil {
			break
		}

		fh := (^WAL_Frame_Header)(raw_data(fh_buf[:]))^
		if check_salt && (u32(fh.salt1) != ws.salt1 || u32(fh.salt2) != ws.salt2) {
			break
		}
		if frame_is_commit(fh) {
			committed_upto = scan + types.WAL_FRAME_SIZE
		}
		scan += types.WAL_FRAME_SIZE
	}
	return committed_upto
}

// page_index tracks committed frame locations (merged from txn_index on commit).
// txn_index tracks uncommitted frame locations (discarded on abort).
Wal_State :: struct {
	file          : ^os.File,
	wal_path      : string,
	header_written: bool,
	salt1         : u32,
	salt2         : u32,
	frame_count   : u32,
	write_offset  : i64,
	page_index    : map[u32]i64,
	txn_index     : map[u32]i64,
	txn_active    : bool,
}

@(private = "file")
wal_frame_hash :: proc(h: ^WAL_Frame_Header, page_data: []u8) -> u64 {
	local := h^
	local.checksum1 = 0
	local.checksum2 = 0
	hdr_bytes := transmute([types.WAL_FRAME_HEADER_SIZE]u8)local
	hv := hash.fnv64(hdr_bytes[:])
	hv = hash.fnv64(page_data, hv)
	return hv
}

@(private)
wal_open :: proc(p: ^Pager, db_path: string) -> Error {
	ws := &p.wal_state
	wal_path := fmt.aprintf("%s-wal", db_path, allocator = context.temp_allocator)
	ws.wal_path = strings.clone(wal_path, p.allocator)
	wal_file, open_err := os.open(wal_path, {.Read, .Write, .Create})
	if open_err != nil {
		log.errorf("WAL: failed to open WAL file: %v", open_err)
		return .File_Open_Failed
	}

	ws.file = wal_file
	file_size, size_err := os.file_size(wal_file)
	if size_err != nil {
		return .IO_Error
	}
	if file_size >= types.WAL_HEADER_SIZE {
		buf: [types.WAL_HEADER_SIZE]u8
		_, read_err := os.read_at(wal_file, buf[:], 0)
		if read_err != nil {
			return .IO_Error
		}
		if string(buf[:8]) == types.WAL_MAGIC {
			h := (^WAL_Header)(raw_data(buf[:]))
			ws.salt1 = u32(h.salt1)
			ws.salt2 = u32(h.salt2)
			ws.header_written = true
			recover_err := wal_recover(p)
			file_size2, _ := os.file_size(ws.file)
			ws.write_offset = file_size2
			return recover_err
		}
	}

	nsec := u64(time.to_unix_nanoseconds(time.now()))
	b1 := transmute([8]u8)nsec
	b2 := transmute([8]u8)(nsec + 1)
	ws.salt1 = u32(hash.fnv64(b1[:]))
	ws.salt2 = u32(hash.fnv64(b2[:]))

	buf: [types.WAL_HEADER_SIZE]u8
	header := (^WAL_Header)(raw_data(buf[:]))
	copy(header.magic[:], types.WAL_MAGIC)

	header.format_version = u32le(WAL_FORMAT_VERSION)
	header.page_size = u32le(types.PAGE_SIZE)
	header.salt1 = u32le(ws.salt1)
	header.salt2 = u32le(ws.salt2)
	_, write_err := os.write_at(wal_file, buf[:], 0)
	if write_err != nil {
		return .IO_Error
	}

	ws.header_written = true
	ws.write_offset = i64(types.WAL_HEADER_SIZE)
	return .None
}

@(private)
wal_close :: proc(p: ^Pager) {
	ws := &p.wal_state
	if ws.file != nil {
		os.close(ws.file)
		ws.file = nil
	}

	delete(ws.page_index)
	delete(ws.txn_index)
	delete(ws.wal_path)
}

wal_begin_txn :: proc(p: ^Pager) {
	p.wal_state.txn_active = true
}

wal_commit_txn :: proc(p: ^Pager) -> Error {
	ws := &p.wal_state
	if !ws.txn_active {
		return .None
	}
	for page_num in p.dirty_pages {
		if page_num == 0 {
			continue
		}
		if slot := find_slot(p, page_num); slot != nil && slot.page.dirty {
			wal_append_frame(p, slot.page.page_num, slot.page.data, false, 0) or_return
			slot.page.dirty = false
		}
	}

	clear(&p.dirty_pages)
	commit_buf: [types.PAGE_SIZE]u8
	wal_append_frame(p, WAL_COMMIT_PAGE, commit_buf[:], true, 0) or_return
	if sync_err := os.sync(ws.file); sync_err != nil {
		log.errorf("WAL: fsync failed: %v", sync_err)
		return .IO_Error
	}
	for k, v in ws.txn_index {
		ws.page_index[k] = v
	}

	clear(&ws.txn_index)
	ws.txn_active = false
	return .None
}

wal_abort_txn :: proc(p: ^Pager) -> Evict_Report {
	ws := &p.wal_state
	if ws.file != nil {
		if file_size, serr := os.file_size(ws.file); serr == nil {
			committed_upto := wal_scan_committed_upto(ws, file_size, false)
			drop := make([dynamic]u32, context.temp_allocator)
			for page_num, fo in ws.page_index {
				if fo >= committed_upto {
					append(&drop, page_num)
				}
			}
			for page_num in drop {
				delete_key(&ws.page_index, page_num)
			}

			os.truncate(ws.file, committed_upto)
			ws.write_offset = committed_upto
		}
	}

	clear(&ws.txn_index)
	report := evict_aborted(p, p.dirty_pages[:])
	if report.skipped_pinned > 0 {
		log.warnf(
			"WAL abort: %d pinned pages kept; space reclaimed by next GC",
			report.skipped_pinned,
		)
	}

	clear(&p.dirty_pages)
	ws.txn_active = false
	return report
}

@(private)
wal_append_frame :: proc(
	p: ^Pager,
	page_num: u32,
	data: []u8,
	is_commit: bool,
	db_size_after: u32,
) -> Error {
	ws := &p.wal_state
	if ws.file == nil {
		return .IO_Error
	}

	db_after: u32 = 0
	if is_commit {
		db_after = db_size_after if db_size_after != 0 else u32(p.file_len / i64(types.PAGE_SIZE))
	}

	fh := WAL_Frame_Header {
		page_num      = u32le(page_num),
		db_size_after = u32le(db_after),
		salt1         = u32le(ws.salt1),
		salt2         = u32le(ws.salt2),
	}

	page_data := data
	scratch: [types.PAGE_SIZE]u8
	if len(data) < types.PAGE_SIZE {
		mem.set(raw_data(scratch[:]), 0, types.PAGE_SIZE)
		copy(scratch[:], data)
		page_data = scratch[:]
	}

	fhv := wal_frame_hash(&fh, page_data)
	fh.checksum1 = u32le(u32(fhv))
	fh.checksum2 = u32le(u32(fhv >> 32))

	file_size := ws.write_offset
	hdr_bytes := transmute([types.WAL_FRAME_HEADER_SIZE]u8)fh
	_, hdr_err := os.write_at(ws.file, hdr_bytes[:], file_size)
	if hdr_err != nil {
		return .IO_Error
	}

	_, data_err := os.write_at(ws.file, page_data, file_size + types.WAL_FRAME_HEADER_SIZE)
	if data_err != nil {
		return .IO_Error
	}

	ws.frame_count += 1
	ws.write_offset += types.WAL_FRAME_SIZE
	if ws.txn_active {
		ws.txn_index[page_num] = file_size
	} else {
		ws.page_index[page_num] = file_size
	}
	return .None
}

wal_checkpoint :: proc(p: ^Pager) -> Error {
	ws := &p.wal_state
	if ws.file == nil {
		return .None
	}

	file_size, size_err := os.file_size(ws.file)
	if size_err != nil {
		return .IO_Error
	}

	offset := i64(types.WAL_HEADER_SIZE)
	frame_count := 0
	// Frames past the last commit marker are uncommitted — aborted-txn
	// residue or in-flight txn pages still authoritative in cache — and
	// must NOT be copied to main. See wal_scan_committed_upto.
	committed_upto := wal_scan_committed_upto(ws, file_size, false)
	for offset + types.WAL_FRAME_SIZE <= committed_upto {
		fh_buf: [types.WAL_FRAME_HEADER_SIZE]u8
		_, read_err := os.read_at(ws.file, fh_buf[:], offset)
		if read_err != nil {
			break
		}

		fh := (^WAL_Frame_Header)(raw_data(fh_buf[:]))^
		page_num := u32(fh.page_num)
		if page_num == WAL_COMMIT_PAGE {
			offset += types.WAL_FRAME_SIZE
			frame_count += 1
			continue
		}

		page_data: [types.PAGE_SIZE]u8
		_, data_err := os.read_at(ws.file, page_data[:], offset + types.WAL_FRAME_HEADER_SIZE)
		if data_err != nil {
			break
		}

		db_offset := i64(page_num - 1) * i64(types.PAGE_SIZE)
		_, write_err := os.write_at(p.file, page_data[:], db_offset)
		if write_err != nil {
			return .IO_Error
		}

		offset += types.WAL_FRAME_SIZE
		frame_count += 1
	}
	if err := os.sync(p.file); err != nil {
		return .IO_Error
	}

	clear(&ws.page_index)
	ws.frame_count = 0
	nsec := u64(time.to_unix_nanoseconds(time.now()))
	b1 := transmute([8]u8)nsec
	b2 := transmute([8]u8)(nsec + 1)
	ws.salt1 = u32(hash.fnv64(b1[:]))
	ws.salt2 = u32(hash.fnv64(b2[:]))

	buf: [types.WAL_HEADER_SIZE]u8
	header := (^WAL_Header)(raw_data(buf[:]))
	copy(header.magic[:], types.WAL_MAGIC)

	header.format_version = u32le(WAL_FORMAT_VERSION)
	header.page_size = u32le(types.PAGE_SIZE)
	header.salt1 = u32le(ws.salt1)
	header.salt2 = u32le(ws.salt2)
	_, write_err := os.write_at(ws.file, buf[:], 0)
	if write_err != nil {
		return .IO_Error
	}

	os.truncate(ws.file, types.WAL_HEADER_SIZE)
	os.sync(ws.file)
	ws.write_offset = i64(types.WAL_HEADER_SIZE)
	log.infof("WAL: checkpoint complete, %d frames written to main file", frame_count)
	return .None
}

// Page_Offset locates one valid WAL frame's page within the WAL file.
Page_Offset :: struct {
	page_num: u32,
	offset  : i64,
}

wal_recover :: proc(p: ^Pager) -> Error {
	ws := &p.wal_state
	if ws.file == nil {
		return .None
	}

	file_size, size_err := os.file_size(ws.file)
	if size_err != nil {
		return .IO_Error
	}
	if file_size <= types.WAL_HEADER_SIZE {
		return .None
	}

	valid_frames, scan_err := collect_valid_frames(ws, file_size)
	if scan_err != .None {
		return scan_err
	}
	if len(valid_frames) == 0 {
		return .None
	}

	replay_frames(p, valid_frames[:])
	log.infof("WAL: recovery complete, %d frames replayed", len(valid_frames))
	return .None
}

// collect_valid_frames scans the committed WAL region and returns one entry per
// checksum-clean frame. The walk stops at the last commit marker
// (wal_scan_committed_upto) and at the first checksum mismatch (a crash-torn
// tail). Returns an empty slice when there is nothing to replay.
@(private = "file")
collect_valid_frames :: proc(ws: ^Wal_State, file_size: i64) -> ([dynamic]Page_Offset, Error) {
	valid_frames := make([dynamic]Page_Offset, context.temp_allocator)
	committed_upto := wal_scan_committed_upto(ws, file_size, true)
	if committed_upto <= i64(types.WAL_HEADER_SIZE) {
		return valid_frames, .None
	}

	offset := i64(types.WAL_HEADER_SIZE)
	for offset < committed_upto {
		fh_buf: [types.WAL_FRAME_HEADER_SIZE]u8
		if _, r_err := os.read_at(ws.file, fh_buf[:], offset); r_err != nil {
			break
		}

		fh := (^WAL_Frame_Header)(raw_data(fh_buf[:]))^
		if u32(fh.checksum1) != 0 || u32(fh.checksum2) != 0 {
			page_buf: [types.PAGE_SIZE]u8
			_, data_err := os.read_at(ws.file, page_buf[:], offset + types.WAL_FRAME_HEADER_SIZE)
			if data_err != nil {
				break
			}

			cs1 := fh.checksum1
			cs2 := fh.checksum2
			fhv := wal_frame_hash(&fh, page_buf[:])
			if u32(fhv) != u32(cs1) || u32(fhv >> 32) != u32(cs2) {
				log.warnf("WAL: checksum mismatch at offset %d", offset)
				break
			}
		}

		append(&valid_frames, Page_Offset{u32(fh.page_num), offset})
		offset += types.WAL_FRAME_SIZE
	}
	return valid_frames, .None
}

// replay_frames copies each valid frame's page image into the main file (commit
// markers are skipped) and extends the logical file length to cover the highest
// replayed page.
@(private = "file")
replay_frames :: proc(p: ^Pager, valid_frames: []Page_Offset) {
	ws := &p.wal_state
	for fo in valid_frames {
		if fo.page_num == WAL_COMMIT_PAGE {
			continue
		}

		page_data: [types.PAGE_SIZE]u8
		_, data_err := os.read_at(ws.file, page_data[:], fo.offset + types.WAL_FRAME_HEADER_SIZE)
		if data_err != nil {
			continue
		}

		db_offset := i64(fo.page_num - 1) * i64(types.PAGE_SIZE)
		os.write_at(p.file, page_data[:], db_offset)
	}

	os.sync(p.file)
	max_page_num: u32
	for fo in valid_frames {
		if fo.page_num > max_page_num {
			max_page_num = fo.page_num
		}
	}

	new_len := i64(max_page_num) * i64(types.PAGE_SIZE)
	if new_len > p.file_len {
		p.file_len = new_len
	}
}
