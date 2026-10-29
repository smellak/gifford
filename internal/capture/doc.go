// Package capture is Bootstrap Capture Mode: attaching to panes, ingesting the
// byte stream into chunks with wall and monotonic time, supervising, rotating
// and reporting status (PRD §9.16).
//
// Invariants: Panes are observed only with `tmux pipe-pane -O`; a pane that
// already has a pipe is refused unless --replace; observed agents are
// deliverable=false. See AGENTS.md in this directory.
//
// The package is the only reader and writer of `capture_sessions` and
// `capture_segments`. It appends `capture.*` events only through
// `eventstore.AppendEvents`.
//
// What it does not do: It never writes to a pane. It never deletes
// captured data; the only removals are its own unpublished temp files
// and a `.raw` after its verified `.zst` is recorded. The events are the
// truth and the rows are a cache that the session's lock holder repairs.
// `screen_hash` is the SHA-256 of the stored snapshot bytes. The event
// store's growth is outside the capture budget. `session_id` is the
// `capture_session_id`; the daemon's `OBSERVED` session maps to a capture
// session by `capture.attached`, without rewriting events.
//
// Segments compress with one zstd encoder per ingest used sequentially
// (`WithEncoderConcurrency(1)`, `WithEncoderLevel(SpeedDefault)`,
// `WithWindowSize(W)`, `WithLowerEncoderMem(true)`) and one decoder at
// a time (`WithDecoderConcurrency(1)`,
// `WithDecoderMaxMemory(max(frame_max_bytes, W))`,
// `WithDecoderMaxWindow(W)`, `WithDecodeAllCapLimit(true)`,
// `WithDecoderLowmem(true)`), where `W` is min(8 MiB, the smallest
// power of two at or above max(`frame_max_bytes`, 1024)). The codec
// workspace (its tables, window and history) is in addition to the
// application buffers and separate from the output cap; it is not a
// bound on RSS. Any memory figure in review material is a measurement,
// not a bound.
package capture
