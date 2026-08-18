package executor

import "core:os"
import "core:text/table"

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

	table.build(tbl, table.unicode_width_proc)
	table.write_markdown_table(
		os.to_stream(os.stdout),
		tbl,
		table.unicode_width_proc,
	)
}
