// Package profiles embeds the harness keyboard profile sources,
// profiles/*.yaml, one file per supported harness. The loading and
// validation rules live in internal/profiles; this package only owns the
// read-only file system so the sources can live at the repository layout
// path (go:embed cannot reach outside its own package directory).
//
// File format (snake_case keys, exactly one document, no unknown keys):
//
//	harness: claude-code         # must match its file name
//	measured: true
//	version_range: "1.2.3"       # input_bounds and input_placeholder require
//	                              # one exact measured harness version
//	submit: "\r"                  # send the draft
//	insert_newline: "\n"          # line break inside the draft
//	clear_line: "\u0015"          # erase the draft line
//	cancel: "\u0003"              # cancel the draft
//	bracketed_paste: false        # TUI enables bracketed paste
//	extended_keyboard: none       # none, csi_u or modify_other_keys
//	input_line: "^> ?(.*)$"       # RE2 with exactly one capture group
//	input_placeholder: "Ask"      # exact faint literal; or a styled prefix:
//	# input_placeholder:
//	#   prefix: "Ask anything"
//	#   style:                    # exact indexed colours, no attributes
//	#     foreground_index: 8
//	#     background_index: 234
//	input_bounds:                 # canonical empty box, nearest row first
//	  above:
//	    - "^box-filler$"
//	    - not: "^box-row.*$"     # existing outer row not owned by the box
//	  below: ["^bottom-border$"]
//	  box:                        # optional measured cell geometry
//	    left_column: 2            # zero-based vertical/bottom-border column
//	    layouts:                  # only these pane widths may be cropped
//	      - {columns: 120, right_margin: 2, bottom_margin: 2}
package profiles

import "embed"

// Files holds the embedded profiles/*.yaml sources; it is read-only by
// construction.
//
//go:embed *.yaml
var Files embed.FS
