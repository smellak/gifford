// Package profiles embeds the harness keyboard profile sources,
// profiles/*.yaml, one file per supported harness. The loading and
// validation rules live in internal/profiles; this package only owns the
// read-only file system so the sources can live at the repository layout
// path (go:embed cannot reach outside its own package directory).
//
// File format (snake_case keys, exactly one document, no unknown keys):
//
//	harness: claude-code         # must match its file name
//	measured: false            # true once keycap measured the real TUI
//	version_range: "*"          # "*" or comma-separated >=,<=,>,<,=
//	                           # over dotted versions, e.g. ">=1.2.0,<2.0.0"
//	submit: "\r"               # send the draft
//	insert_newline: "\n"       # line break inside the draft
//	clear_line: "\u0015"       # erase the draft line
//	cancel: "\u0003"           # cancel the draft
//	bracketed_paste: false     # TUI enables bracketed paste
//	extended_keyboard: none    # none, csi_u or modify_other_keys
//	input_line: "^(.*)$"       # RE2 with exactly one capture group
package profiles

import "embed"

// Files holds the embedded profiles/*.yaml sources; it is read-only by
// construction.
//
//go:embed *.yaml
var Files embed.FS
