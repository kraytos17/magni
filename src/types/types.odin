// Package types — shared domain model and storage constants.
//
// Leaf package: imports only core: so every other src: package can depend
// on it. Defines the Value union, schema structs (Column/Table/Index_Def),
// Row_ID, serial-type codes, and on-disk format constants.
package types

import "core:fmt"
import "core:hash"
import "core:mem"
import "core:slice"
import "core:strings"

PAGE_SIZE            :: 4096
DATABASE_HEADER_SIZE :: 100

// MAX_COLS caps table width. Inline row-decode scratch buffers in cell are
// sized by it for stack storage, so raising it grows stack frames in the
// deserialization hot path.
MAX_COLS :: 10

// Storage_Config controls value materialization during cell deserialization.
// Shared by btree and cell so both decode rows byte-for-byte the same way.
Storage_Config :: struct {
	allocator: mem.Allocator,
	zero_copy: bool, // true = text/blob borrow the page buffer, never owned
}

MAGIC_STRING          :: "MAGNI_DB"
SCHEMA_VERSION        :: 2
PAGE_FORMAT_VERSION   :: 3
WAL_MAGIC             :: "MAGNIWAL"
WAL_HEADER_SIZE       :: 32
WAL_FRAME_HEADER_SIZE :: 24
WAL_FRAME_SIZE        :: WAL_FRAME_HEADER_SIZE + PAGE_SIZE // 24 + 4096 = 4120
NANOS_PER_MICRO       :: 1000

// Serial_Type tags a value's cell encoding: fixed-width integers, FLOAT64,
// the ZERO/ONE shortcodes, and length-encoded BLOB/TEXT from 12 up.
Serial_Type :: enum u64 {
	NULL    = 0,
	INT8    = 1,
	INT16   = 2,
	INT24   = 3,
	INT32   = 4,
	INT48   = 5,
	INT64   = 6,
	FLOAT64 = 7,
	ZERO    = 8, // Internal: integer 0
	ONE     = 9, // Internal: integer 1
	// 10, 11 reserved
	// >= 12 (even): BLOB with length (N-12)/2
	// >= 13 (odd): TEXT with length (N-13)/2
}

// Column_Type is the declared SQL type of a column.
Column_Type :: enum u8 {
	INTEGER,
	TEXT,
	REAL,
	BLOB,
}

// Null is the unit payload for SQL NULL (union tag carries the meaning).
Null :: struct {}

// Value is one SQL value. string and []u8 carry borrowed payloads: the
// producer (page buffer, arena) owns the bytes, consumers must not free.
Value :: union {
	i64,
	f64,
	string,
	[]u8, // BLOB
	Null,
}

// value_null returns the SQL NULL value.
value_null :: proc() -> Value {
	return Null{}
}

// value_int returns an INTEGER value.
value_int :: proc(v: i64) -> Value {
	return v
}

// value_real returns a REAL value.
value_real :: proc(v: f64) -> Value {
	return v
}

// value_text returns a TEXT value borrowing v.
value_text :: proc(v: string) -> Value {
	return v
}

// value_blob returns a BLOB value borrowing v.
value_blob :: proc(v: []u8) -> Value {
	return v
}

// is_null reports whether v is SQL NULL.
is_null :: proc(v: Value) -> bool {
	_, ok := v.(Null)
	return ok
}

// value_clone returns a deep copy of v: string/blob payloads are cloned into
// allocator; scalar and NULL values are returned unchanged. Caller frees
// string/blob copies with value_delete.
@(require_results)
value_clone :: proc(v: Value, allocator := context.allocator) -> (Value, mem.Allocator_Error) {
	#partial switch val in v {
	case string:
		str_copy, err := strings.clone(val, allocator)
		if err != nil {
			return {}, err
		}
		return value_text(str_copy), nil
	case []u8:
		blob_copy, err := slice.clone(val, allocator)
		if err != nil {
			return {}, err
		}
		return value_blob(blob_copy), nil
	case:
		return val, nil
	}
}

// value_delete frees a value returned by value_clone; scalars and NULL are
// no-ops.
value_delete :: proc(v: Value, allocator := context.allocator) {
	#partial switch val in v {
	case string:
		delete(val, allocator)
	case []u8:
		delete(val, allocator)
	}
}

// values_delete frees every cloned payload in values, then the slice itself.
values_delete :: proc(values: []Value, allocator := context.allocator) {
	for v in values {
		value_delete(v, allocator)
	}
	delete(values, allocator)
}

// value_compare reports equality with type tags: INTEGER 1, REAL 1.0, and
// TEXT "1" never compare equal; NULL equals only NULL.
value_compare :: proc(a, b: Value) -> bool {
	#partial switch va in a {
	case Null:
		_, is_null := b.(Null)
		return is_null
	case i64:
		vb, ok := b.(i64)
		return ok && va == vb
	case f64:
		vb, ok := b.(f64)
		return ok && va == vb
	case string:
		vb, ok := b.(string)
		return ok && va == vb
	case []u8:
		vb, ok := b.([]u8)
		return ok && slice.equal(va, vb)
	case:
		return false
	}
}

// value_to_string renders v as display text. The result is allocated from
// allocator: temp (default) results live until arena reset; a persistent
// allocator returns an owned copy the caller must free.
value_to_string :: proc(v: Value, allocator := context.temp_allocator) -> string {
	switch val in v {
	case Null:
		return strings.clone("NULL", allocator)
	case i64:
		return fmt.aprintf("%d", val, allocator = allocator)
	case f64:
		return fmt.aprintf("%g", val, allocator = allocator)
	case string:
		return strings.clone(val, allocator)
	case []u8:
		return fmt.aprintf("<BLOB %d bytes>", len(val), allocator = allocator)
	case:
		return strings.clone("<?>", allocator)
	}
}

// serial_type_content_size returns the payload byte size for a serial type
// code: fixed widths for the primitive codes, (N-12)/2 for BLOB and
// (N-13)/2 for TEXT length-carrying codes. valid=false for unknown codes.
serial_type_content_size :: proc(serial: u64) -> (size: int, valid: bool) {
	if serial >= 12 {
		// BLOB (even): length = (N-12)/2
		// TEXT (odd):  length = (N-13)/2
		return int((serial - 12) / 2) if serial % 2 == 0 else int((serial - 13) / 2), true
	}

	switch Serial_Type(serial) {
	case .NULL, .ZERO, .ONE:
		return 0, true
	case .INT8:
		return 1, true
	case .INT16:
		return 2, true
	case .INT24:
		return 3, true
	case .INT32:
		return 4, true
	case .INT48:
		return 6, true
	case .INT64, .FLOAT64:
		return 8, true
	case:
		return 0, false
	}
}

// Row_ID is the primary-key value space: distinct i64 so rowids never mix
// with plain integers. Ordering is numeric (encoded keys stay order-
// preserving via sign bias).
Row_ID :: distinct i64

// Column is one table column definition.
Column :: struct {
	name         : string,
	type         : Column_Type,
	not_null     : bool,
	pk           : bool,
	default_value: Maybe(Value),
	check_expr   : Maybe(string),
}

// hash_string derives a table's schema-rowid from its name: FNV-1a with the
// sign bit cleared, so catalog keys stay in the positive rowid space.
hash_string :: proc(s: string) -> u64 {
	return hash.fnv64a(transmute([]u8)s) & 0x7FFFFFFFFFFFFFFF
}

// Index_Def is one secondary text index: covering text->rowid over a
// single TEXT column. Names are unique per table (may repeat across
// tables); DROP INDEX <name> needs a unique match or ON <table>.
Index_Def :: struct {
	name  : string, // index name (empty = legacy unnamed single)
	column: string, // indexed column name
	root  : u32, // root page of the text index (0 = none, never routed)
}

// Table is an in-memory table definition as published in the catalog.
Table :: struct {
	name        : string,
	columns     : []Column,
	root_page   : u32,
	sql         : string,
	foreign_keys: []Foreign_Key,
	skip_root   : u32, // root page of the skip index for this table (0 = none)
	// Secondary text indexes: covering text->rowid, one Index_Def per
	// index (empty = none). Wire format is triples from slot [6]
	// ([root INT][column TEXT][name TEXT] × N); see schema serde.
	indexes     : []Index_Def,
}

// Foreign_Key is a REFERENCES clause: col references ref_table(ref_col).
Foreign_Key :: struct {
	col      : string,
	ref_table: string,
	ref_col  : string,
}
