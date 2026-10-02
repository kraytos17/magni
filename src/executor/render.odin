package executor

import "core:fmt"
import "core:os"
import "core:text/table"
import "src:types"

// render_table prints a query/result command as a markdown table.
// Cells are set via set_cell_value (which also sets alignment, required by the
// markdown divider row); only framing/alignment is handled here.
// NOTE: the Table is deliberately NOT destroyed here. `table.destroy` calls
// `free_all(tbl.format_allocator)`, and with `context.temp_allocator` as the
// format allocator that would wipe the shared temp arena mid-execution,
// corrupting any temp-allocated state (e.g. the script buffer in pipe/--file
// mode). The table is reclaimed by the existing free_all(temp) lifecycle.
render_table :: proc(cols: []string, rows: [][]string) {
	tbl := table.init_with_allocator(
		&table.Table{},
		context.temp_allocator,
		context.temp_allocator,
	)

	tbl.nr_cols = len(cols)
	tbl.nr_rows = 1 + len(rows)
	tbl.has_header_row = true
	for c, i in cols {
		table.set_cell_value(tbl, 0, i, c)
	}
	for r, ri in rows {
		for c, ci in r {
			table.set_cell_value(tbl, 1 + ri, ci, c)
		}
	}

	// write_markdown_table calls build() internally — no explicit build needed.
	// Use the cheap ASCII width proc when all cells are ASCII (grapheme
	// segmentation is ~10-50x more expensive and identical for ASCII).
	width_proc := table.ascii_width_proc
	width_check: {
		for c in cols {
			if !is_ascii(c) { width_proc = table.unicode_width_proc; break width_check }
		}
		for r in rows {
			for c in r {
				if !is_ascii(c) { width_proc = table.unicode_width_proc; break width_check }
			}
		}
	}

	table.write_markdown_table(
		os.to_stream(os.stdout),
		tbl,
		width_proc,
	)
}

// is_ascii reports whether s contains only ASCII bytes.
@(private="file")
is_ascii :: proc(s: string) -> bool {
	for i in 0 ..< len(s) {
		if s[i] >= 0x80 { return false }
	}
	return true
}

// row_of allocates one fixed-width row filled from cells. Removes the
// make/assign/store scaffolding at fixed-column sites (list_tables,
// describe, snapshots, snapdiff).
row_of :: proc(allocator := context.allocator, cells: ..string) -> []string {
	row := make([]string, len(cells), allocator)
	copy(row, cells)
	return row
}

// stringify_row renders one result row's values to strings. Shared by the
// SELECT, compound, and dump row loops.
stringify_row :: proc(values: []types.Value, allocator := context.allocator) -> []string {
	row := make([]string, len(values), allocator)
	for v, i in values {
		row[i] = types.value_to_string(v, allocator)
	}
	return row
}

// render_counted renders a table plus the standard "(N rows)" footer. The
// result-printing sites share this so footers can never drift; sites with
// custom footers (snapdiff) or none (stats) keep calling render_table.
render_counted :: proc(cols: []string, rows: [][]string) {
	render_table(cols, rows)
	fmt.printf("(%d rows)\n", len(rows))
}
