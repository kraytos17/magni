// Package btree — distinct page/address types.
//
// Rationale: u32 page numbers and u16 cell offsets flow through the same
// procs and would otherwise mix silently. Distinct types force explicit
// conversion at module boundaries (pager/tree/executor) while arithmetic
// stays cheap (same size/alignment as the base type, zero runtime cost).
//
// Conventions:
//   - Page_Id is the only page-address type inside btree. Raw u32 survives
//     only at the pager boundary (pager.get_page/allocate_page take u32).
//   - Cell_Off is the only cell-area offset type inside layout code.
//   - Index_Id names a secondary index (catalog-assigned).
//   - Conversions are explicit casts: Page_Id(raw), u32(id). No helpers
//     that hide truncation; u16-range checks stay at the call site.
package btree

// Page_Id identifies a page inside a single pager/file.
Page_Id :: distinct u32

// Cell_Off is a byte offset into a page's cell area.
Cell_Off :: distinct u16

// Index_Id identifies a secondary index.
Index_Id :: distinct u32
