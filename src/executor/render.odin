package executor

import "core:os"
import "core:text/table"

// render_table prints a query/command result as a markdown table.
// Cells are set via set_cell_value (which also sets alignment, required by the
// markdown divider row); only framing/alignment is handled here.
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
		table.set_cell_value(tbl, 0, i, c)
	}
	for r, ri in rows {
		for c, ci in r {
			table.set_cell_value(tbl, 1 + ri, ci, c)
		}
	}
	
	table.build(tbl, table.unicode_width_proc)
	table.write_markdown_table(
		os.to_stream(os.stdout),
		tbl,
		table.unicode_width_proc,
	)
}
