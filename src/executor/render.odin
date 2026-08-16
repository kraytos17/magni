package executor

import "core:os"
import "core:text/table"

// render_table prints a query/command result as a decorated text table.
// Cells are set directly (bypasses the variadic `any` APIs, which crash on
// Odin's runtime-allocated []any splat); only framing/alignment is handled here.
render_table :: proc(cols: []string, rows: [][]string) {
	tbl := table.init_with_allocator(
		&table.Table{},
		context.temp_allocator,
		context.temp_allocator,
	)
	
	defer table.destroy(tbl)
	tbl.nr_cols = len(cols)
	tbl.nr_rows = 1 + len(rows)
	tbl.has_header_row = true
	for c, i in cols {
		cell := table.get_cell(tbl, 0, i)
		cell.text = c
	}
	for r, ri in rows {
		for c, ci in r {
			cell := table.get_cell(tbl, 1 + ri, ci)
			cell.text = c
		}
	}
	
	table.build(tbl, table.unicode_width_proc)
	decorations := table.Decorations{
		nw  = "",
		n   = "+",
		ne  = "",
		w   = "",
		x   = "+",
		e   = "",
		sw  = "",
		s   = "+",
		se  = "",
		vert = " | ",
		horz = "-",
	}
	table.write_decorated_table(
		os.to_stream(os.stdout),
		tbl,
		decorations,
		table.unicode_width_proc,
	)
}
