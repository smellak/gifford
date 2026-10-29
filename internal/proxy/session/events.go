package session

import (
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Event is one session signal to the owner: a typed payload with its
// envelope. The daemon link consumes these later; here they only carry
// metadata, never typed content.
type Event struct {
	Session     string
	Type        protocol.EventType
	Payload     any
	WallTime    time.Time
	MonotonicNS int64
	Sequence    int64
}

// PTYOpened is the payload of pty.opened, always a session's first
// event: the size the screen starts at, without which a recording cannot
// be replayed, and the harness profile the run chose. A replay takes the
// harness from here, never from its caller.
type PTYOpened struct {
	Cols    int              `json:"cols"`
	Rows    int              `json:"rows"`
	Harness protocol.Harness `json:"harness"`
}

// PTYClosed is the payload of pty.closed, a finished run's last event:
// it marks when the session ended, so a replay covers the screen the
// last output left until then.
type PTYClosed struct{}

// IOChunkEvent is the payload of io.chunk: one recorded output piece,
// its bytes counted and digested, the screen hash they left, and its
// recording sequence.
type IOChunkEvent struct {
	ByteCount         int64           `json:"byte_count"`
	ANSIPresent       bool            `json:"ansi_present"`
	ScreenHashAfter   protocol.Digest `json:"screen_hash_after"`
	RecordingSequence int64           `json:"recording_sequence"`
	// ChunkDigest is the SHA-256 of the exact recorded bytes: the screen
	// hash normalises digits and spinners, so only this proves a replay
	// read the same bytes.
	ChunkDigest protocol.Digest `json:"chunk_digest"`
}

// InputKeyChunk is the payload of input.key_chunk: how many printable
// runes landed in the period. Counts, never text.
type InputKeyChunk struct {
	PrintableCount int64  `json:"printable_count"`
	Generation     uint64 `json:"draft_generation"`
	GuardVersion   int64  `json:"input_guard_version"`
}

// InputControlKey is the payload of input.control_key: which non-text
// key was pressed, with modifiers. The rune itself never leaves.
type InputControlKey struct {
	Key          string `json:"key"`
	Shift        bool   `json:"shift"`
	Alt          bool   `json:"alt"`
	Ctrl         bool   `json:"ctrl"`
	Generation   uint64 `json:"draft_generation"`
	GuardVersion int64  `json:"input_guard_version"`
}

// InputPaste is the payload of input.paste_started (size zero, nothing
// seen yet) and input.paste_completed (total pasted bytes).
type InputPaste struct {
	SizeBytes    int64  `json:"size_bytes"`
	Generation   uint64 `json:"draft_generation"`
	GuardVersion int64  `json:"input_guard_version"`
}

// InputSubmit is the payload of input.submit_detected: the human pressed
// the profile submit key.
type InputSubmit struct {
	Generation   uint64 `json:"draft_generation"`
	GuardVersion int64  `json:"input_guard_version"`
}

// InputDraft is the payload of the input.draft_* transition events.
type InputDraft struct {
	State        string `json:"state"`
	Generation   uint64 `json:"draft_generation"`
	GuardVersion int64  `json:"input_guard_version"`
}

// InputResize is the payload of input.resize.
type InputResize struct {
	Cols         int    `json:"cols"`
	Rows         int    `json:"rows"`
	Generation   uint64 `json:"draft_generation"`
	GuardVersion int64  `json:"input_guard_version"`
}

// EventSink receives session events in order. Emit must not block for
// long: a slow sink back-pressures output, never input, but a stuck one
// stalls the session. Implementations persist metadata only.
type EventSink interface {
	Emit(Event)
}
