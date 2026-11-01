package session

import (
	"time"

	"github.com/smellak/gifford/internal/outputactivity"
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
	// HarnessVersion is the measured harness version; old logs omit
	// it and read back as UnknownHarnessVersion.
	HarnessVersion string `json:"harness_version,omitempty"`
	// ActivityVersion names the activity input carried by this capture.
	// Old logs omit it and replay under the distinct reconstructed-chunk
	// calibration.
	ActivityVersion string `json:"activity_version,omitempty"`
	// ObservationEvidenceVersion opts this capture into the content-free
	// arrival evidence needed by permission-dialog-v1 replay. Old and normal
	// captures omit it and keep their existing seal.
	ObservationEvidenceVersion string `json:"observation_evidence_version,omitempty"`
}

// UnknownHarnessVersion is the version a log without the field reads
// as: the run did not measure one.
const UnknownHarnessVersion = "unknown"

// Version returns the measured harness version, or
// UnknownHarnessVersion when the log predates the field.
func (p PTYOpened) Version() string {
	if p.HarnessVersion == "" {
		return UnknownHarnessVersion
	}
	return p.HarnessVersion
}

// OutputActivityVersion returns the capture's activity input version. A log
// from before PTY-read evidence can only be reconstructed at aggregated
// io.chunk boundaries, so it receives a distinct calibration.
func (p PTYOpened) OutputActivityVersion() string {
	if p.ActivityVersion == "" {
		return outputactivity.ReconstructedVersion
	}
	return p.ActivityVersion
}

// PTYClosed is the payload of pty.closed, a finished run's last event:
// it marks when the session ended, so a replay covers the screen the
// last output left until then.
type PTYClosed struct {
	// ObservationEvidence closes the optional evidence stream. Replay accepts
	// permission dialogs only when these totals agree with every recorded
	// arrival event; silence alone never proves there were no keys.
	ObservationEvidence *PermissionDialogEvidenceSummary `json:"observation_evidence,omitempty"`
}

// EventTypePermissionDialogInput is local capture evidence. It deliberately
// is not a durable Core event: the payload is content-free but exists solely
// to reconstruct the observation policy from the private capture pair.
const EventTypePermissionDialogInput protocol.EventType = "input.observation_evidence"

// EventTypePermissionDialogFrame seals the post-guard boundary of one raw
// PTY read. io.output_read is emitted before the screen and Draft Guard are
// folded, so replay needs this later boundary to use the same facts as live.
const EventTypePermissionDialogFrame protocol.EventType = "output.observation_evidence"

const (
	// PermissionDialogInputTerminal is a proven passive terminal response.
	PermissionDialogInputTerminal = "terminal"
	// PermissionDialogInputHuman is an outer-input arrival with human keys.
	PermissionDialogInputHuman = "human"
	// PermissionDialogInputUnknown is an arrival that could not be classified.
	PermissionDialogInputUnknown = "unknown"
)

// PermissionDialogInputEvidence classifies exactly one outer-terminal read.
// ArrivalMonotonicNS records when pumpIn observed it, which can precede this
// classification event while the writer or event loop is busy.
type PermissionDialogInputEvidence struct {
	Index              uint64 `json:"index"`
	ArrivalMonotonicNS int64  `json:"arrival_monotonic_ns"`
	Kind               string `json:"kind"`
	KeyGeneration      uint64 `json:"key_generation"`
}

// Valid reports whether the evidence has a closed kind and usable arrival.
func (p PermissionDialogInputEvidence) Valid() bool {
	return p.Index > 0 && p.ArrivalMonotonicNS >= 0 &&
		(p.Kind == PermissionDialogInputTerminal || p.Kind == PermissionDialogInputHuman || p.Kind == PermissionDialogInputUnknown)
}

// PermissionDialogFrameEvidence carries no screen content. ReadDigest ties
// it to the preceding io.output_read whose complete bytes produced the frame.
type PermissionDialogFrameEvidence struct {
	Index         uint64          `json:"index"`
	ReadDigest    protocol.Digest `json:"read_digest"`
	GuardClosed   bool            `json:"guard_closed"`
	InputAligned  bool            `json:"input_aligned"`
	KeyGeneration uint64          `json:"key_generation"`
}

// Valid reports whether the frame is tied to one indexed PTY read.
func (p PermissionDialogFrameEvidence) Valid() bool {
	return p.Index > 0 && p.ReadDigest.Valid()
}

// PermissionDialogEvidenceSummary proves that the local log accounts for
// every outer-terminal arrival before pty.closed.
type PermissionDialogEvidenceSummary struct {
	Version       string `json:"version"`
	Arrivals      uint64 `json:"arrivals"`
	InputEvents   uint64 `json:"input_events"`
	KeyGeneration uint64 `json:"key_generation"`
	OutputReads   uint64 `json:"output_reads"`
	Frames        uint64 `json:"frames"`
}

// Valid reports whether the evidence stream closed with complete totals.
func (p PermissionDialogEvidenceSummary) Valid() bool {
	return p.Version == PermissionDialogEvidenceVersion && p.Arrivals == p.InputEvents && p.OutputReads == p.Frames
}

// DeliveryPrepared is the payload of input.delivery_prepared: one
// two-phase delivery step with the delivery id and the generation
// and version it was checked against. It never carries human bytes.
type DeliveryPrepared struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
}

// DeliveryRevalidated is the payload of input.delivery_revalidated:
// the commit-time recheck passed.
type DeliveryRevalidated struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
}

// DeliveryStarted is the payload of input.delivery_started: the
// lease is taken and the line goes out.
type DeliveryStarted struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
}

// DeliveryChunk is the payload of input.delivery_chunk: buffered
// human bytes flushed after the submit, counted, never carried.
type DeliveryChunk struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
	Bytes      int64             `json:"bytes"`
}

// DeliveryCommitted is the payload of input.delivery_committed.
type DeliveryCommitted struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
}

// DeliveryAborted is the payload of
// input.delivery_aborted_human_priority: human priority won, none of
// the line was written.
type DeliveryAborted struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Generation uint64            `json:"generation"`
	Version    int64             `json:"version"`
	Reason     string            `json:"reason"`
}

// DeliveryCancelled is the payload of input.delivery_cancelled.
type DeliveryCancelled struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Reason     string            `json:"reason"`
}

// DeliveryFailed is the payload of input.delivery_failed: the line
// itself was rejected before anything was written.
type DeliveryFailed struct {
	DeliveryID protocol.OpaqueID `json:"delivery_id"`
	Reason     string            `json:"reason"`
	// Code is the failure's fixed code, the only reason a durable event
	// carries. The events file keeps the error's text, not the code.
	Code protocol.DurableDeliveryFailureReason `json:"-"`
}

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

// IOOutputRead is the minimal temporal evidence for one raw PTY read. Its
// bytes remain solely in the recording and may be aggregated across one or
// more io.chunk events; the digest lets replay recover and verify the exact
// read boundary without putting content in the event log.
type IOOutputRead struct {
	ByteCount  int64           `json:"byte_count"`
	ReadDigest protocol.Digest `json:"read_digest"`
	Effective  bool            `json:"effective"`
}

// Valid reports evidence one pump read could have produced. pumpOut reads at
// most 32 KiB at once.
func (p IOOutputRead) Valid() bool {
	return p.ByteCount > 0 && p.ByteCount <= 32<<10 && p.ReadDigest.Valid()
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
