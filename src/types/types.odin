package types

import "core:fmt"
import "core:strings"

PAGE_SIZE :: 4096
DATABASE_HEADER_SIZE :: 100
MAX_COLS :: 10

// Serial types used for encoding values in cells
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

// Column data types
Column_Type :: enum {
	INTEGER,
	TEXT,
	REAL,
	BLOB,
}

Null :: struct {}

// Value union representing database values
// string and []u8 are borrowed references - caller manages lifetime
Value :: union {
	i64,
	f64,
	string,
	[]u8, // BLOB
	Null,
}

value_null :: proc() -> Value {
	return Null{}
}

value_int :: proc(v: i64) -> Value {
	return v
}

value_real :: proc(v: f64) -> Value {
	return v
}

value_text :: proc(v: string) -> Value {
	return v
}

value_blob :: proc(v: []u8) -> Value {
	return v
}

// Check if value is NULL
is_null :: proc(v: Value) -> bool {
	_, ok := v.(Null)
	return ok
}

// Convert value to string representation
// If allocator is context.temp_allocator (default), the result is temporary.
// If a persistent allocator is provided, the result is a new copy you must free.
value_to_string :: proc(v: Value, allocator := context.temp_allocator) -> string {
	switch val in v {
	case Null:
		return strings.clone("NULL", allocator)
	case i64:
		return fmt.aprintf("%d", val, allocator = allocator)
	case f64:
		return fmt.aprintf("%f", val, allocator = allocator)
	case string:
		return strings.clone(val, allocator)
	case []u8:
		return fmt.aprintf("<BLOB %d bytes>", len(val), allocator = allocator)
	}
	unreachable()
}

// Calculate content size from serial type
// Returns (size, valid) - size is 0 if invalid
serial_type_content_size :: proc(serial: u64) -> (size: int, valid: bool) {
	if serial >= 12 {
		if serial % 2 == 0 {
			// BLOB (even): length = (N-12)/2
			return int((serial - 12) / 2), true
		} else {
			// TEXT (odd): length = (N-13)/2
			return int((serial - 13) / 2), true
		}
	}
	if serial == 10 || serial == 11 {
		return 0, false
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
	case .INT64:
		return 8, true
	case .FLOAT64:
		return 8, true
	}
	return 0, false
}

// Row ID type
Row_ID :: distinct i64

// Column definition
Column :: struct {
	name:     string,
	type:     Column_Type,
	not_null: bool,
	pk:       bool,
}

// Table definition
Table :: struct {
	name:      string,
	columns:   []Column,
	root_page: u32, // Page number of B-tree root
	sql:       string,
}

Result_Row :: []Value
Result_Set :: []Result_Row

// Error types
DB_Error :: enum {
	None,
	Invalid_Type,
	Invalid_Serial_Type,
	Buffer_Too_Small,
	Parse_Error,
	Table_Not_Found,
	Column_Not_Found,
	Type_Mismatch,
	Constraint_Violation,
	IO_Error,
	Out_Of_Memory,
}
