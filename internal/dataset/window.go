package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/protocol"
)

// WindowSchemaVersion is the only window schema this package reads.
const WindowSchemaVersion = 1

// maxWindowBytes bounds one encoded window, so a reader never buffers
// an unbounded document.
const maxWindowBytes = 1 << 20

// maxPolicyVersion bounds a policy version, as protocol bounds
// redaction_policy_version.
const maxPolicyVersion = 32

// maxScreenRows and maxScreenCols bound the redacted screen, as the
// pane facts bound a pane's size.
const maxScreenRows, maxScreenCols = 10000, 10000

// maxSourceRefs bounds the references a window may carry.
const maxSourceRefs = 1024

// Field paths named in FieldError, one per validated field that
// several rules report.
const (
	fieldDomain           = "environment_domain"
	fieldLabel            = "target.label"
	fieldSourceQuality    = "provenance.source_quality"
	fieldWindowEnd        = "provenance.window_end"
	fieldAnchorAt         = "provenance.anchor_at"
	fieldSinceLastOutput  = "input.seconds_since_last_output"
	fieldPreviousAge      = "input.previous_state_age_seconds"
	fieldOutputBytes      = "input.output_bytes"
	fieldScreenLines      = "input.screen.lines"
	fieldSourceDigest     = "provenance.source_digest"
	fieldSourceRefs       = "provenance.source_refs"
	fieldRedactionPolicy  = "policies.redaction_policy_version"
	fieldDomainMaskPolicy = "policies.domain_mask_policy_version"
)

// ErrInvalidWindow is wrapped by every validation failure.
var ErrInvalidWindow = errors.New("invalid window")

// FieldError names the field a window failed on and why. It never
// carries field content: screens and references stay out of errors.
type FieldError struct {
	Field  string
	Reason string
}

// Error implements error.
func (e *FieldError) Error() string {
	return fmt.Sprintf("invalid window: %s: %s", e.Field, e.Reason)
}

// Unwrap makes every FieldError match ErrInvalidWindow.
func (e *FieldError) Unwrap() error { return ErrInvalidWindow }

func fieldErr(field, reason string) error {
	return &FieldError{Field: field, Reason: reason}
}

// EnvironmentDomain is the deployment family a window was drawn from.
type EnvironmentDomain string

// The environment domains.
const (
	EnvironmentDomainBankGeneric  EnvironmentDomain = "bank_generic"
	EnvironmentDomainFleetManaged EnvironmentDomain = "fleet_managed"
)

// Valid reports a known domain.
func (d EnvironmentDomain) Valid() bool {
	switch d {
	case EnvironmentDomainBankGeneric, EnvironmentDomainFleetManaged:
		return true
	}
	return false
}

// Split is the dataset partition a window belongs to.
type Split string

// The splits.
const (
	SplitTrain      Split = "train"
	SplitValidation Split = "validation"
	SplitTest       Split = "test"
)

// Valid reports a known split.
func (s Split) Valid() bool {
	switch s {
	case SplitTrain, SplitValidation, SplitTest:
		return true
	}
	return false
}

// Label is the training target: one letter A–G, each bound to one
// agent phase by the closed table in Phase. The letters are not derived
// from protocol declaration order. BACKGROUND_WORK has no label: the
// native path decides it without the model.
type Label string

// The labels.
const (
	LabelA Label = "A"
	LabelB Label = "B"
	LabelC Label = "C"
	LabelD Label = "D"
	LabelE Label = "E"
	LabelF Label = "F"
	LabelG Label = "G"
)

// Phase returns the agent phase a label stands for. This switch is the
// whole mapping.
func (l Label) Phase() (protocol.AgentPhase, bool) {
	switch l {
	case LabelA:
		return protocol.AgentPhaseIdleAtPrompt, true
	case LabelB:
		return protocol.AgentPhaseThinking, true
	case LabelC:
		return protocol.AgentPhaseRunningTool, true
	case LabelD:
		return protocol.AgentPhaseWaitingApproval, true
	case LabelE:
		return protocol.AgentPhaseAskingQuestion, true
	case LabelF:
		return protocol.AgentPhaseErrorOrExited, true
	case LabelG:
		return protocol.AgentPhaseUnknown, true
	}
	return "", false
}

// LabelOf returns the label of an agent phase; BACKGROUND_WORK and
// unknown strings have none.
func LabelOf(phase protocol.AgentPhase) (Label, bool) {
	for _, l := range []Label{LabelA, LabelB, LabelC, LabelD, LabelE, LabelF, LabelG} {
		if p, _ := l.Phase(); p == phase {
			return l, true
		}
	}
	return "", false
}

// Window is one labelled example: the exact ANALYZE_AGENT_STATE input
// at one instant, its target, where it came from, and the policies and
// split it was built under. It holds no raw capture bytes and no human
// content; the source bytes are named by reference and digest only.
type Window struct {
	SchemaVersion     int64             `json:"schema_version"`
	WindowID          protocol.ULID     `json:"window_id"`
	Input             AnalyzeInput      `json:"input"`
	Target            Target            `json:"target"`
	Provenance        Provenance        `json:"provenance"`
	EnvironmentDomain EnvironmentDomain `json:"environment_domain"`
	Policies          Policies          `json:"policies"`
	Split             SplitAssignment   `json:"split"`
}

// AnalyzeInput is the ANALYZE_AGENT_STATE input, field for field.
type AnalyzeInput struct {
	Contract               protocol.ModelContract `json:"contract"`
	Harness                protocol.Harness       `json:"harness"`
	SecondsSinceLastOutput float64                `json:"seconds_since_last_output"`
	OutputBytes            OutputBytes            `json:"output_bytes"`
	NewChildProcesses      int64                  `json:"new_child_processes"`
	// PreviousState is any agent phase, BACKGROUND_WORK and UNKNOWN
	// included: it describes the past, not a target. It is empty for a
	// session's first window, and then its age is zero.
	PreviousState           protocol.AgentPhase `json:"previous_state,omitempty"`
	PreviousStateAgeSeconds float64             `json:"previous_state_age_seconds"`
	Screen                  RedactedScreen      `json:"screen"`
}

// OutputBytes counts the bytes the pane printed in the trailing 0.5 s,
// 2 s and 10 s before the anchor. The windows nest, so the counts never
// decrease.
type OutputBytes struct {
	Last500ms int64 `json:"last_0_5s"`
	Last2s    int64 `json:"last_2s"`
	Last10s   int64 `json:"last_10s"`
}

// RedactedScreen is the screen at the anchor after internal/redact: its
// rendered lines, never the raw stream, and a digest over them.
type RedactedScreen struct {
	// Redacted is set only by the build step that ran the redactor;
	// an unmarked screen never enters a dataset.
	Redacted bool            `json:"redacted"`
	Cols     int64           `json:"cols"`
	Rows     int64           `json:"rows"`
	Lines    []string        `json:"lines"`
	Digest   protocol.Digest `json:"digest"`
}

// Target is the label the window teaches.
type Target struct {
	Label Label `json:"label"`
}

// Provenance ties a window to its capture without carrying its bytes.
type Provenance struct {
	SessionID        protocol.OpaqueID      `json:"session_id"`
	CaptureSessionID protocol.OpaqueID      `json:"capture_session_id"`
	SourceQuality    protocol.SourceQuality `json:"source_quality"`
	// SequenceFrom and SequenceTo bound the session events the window
	// was drawn from, inclusive.
	SequenceFrom int64     `json:"sequence_from"`
	SequenceTo   int64     `json:"sequence_to"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	// AnchorAt is the instant the input describes.
	AnchorAt time.Time `json:"anchor_at"`
	// SourceRefs name the captured bytes; SourceDigest is the SHA-256
	// over them. Neither carries content.
	SourceRefs   []protocol.BlobRef `json:"source_refs"`
	SourceDigest protocol.Digest    `json:"source_digest"`
}

// Policies are the versions of the rules applied before the window was
// built.
type Policies struct {
	RedactionPolicyVersion  string `json:"redaction_policy_version"`
	DomainMaskPolicyVersion string `json:"domain_mask_policy_version"`
}

// SplitAssignment places the window in a split. GroupDigest is the key
// the split was assigned by, so every window of one group lands in the
// same split.
type SplitAssignment struct {
	Split       Split           `json:"split"`
	GroupDigest protocol.Digest `json:"group_digest"`
}

// ScreenDigest is the digest a RedactedScreen must carry: SHA-256 over
// its lines joined by newlines.
func ScreenDigest(lines []string) protocol.Digest {
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// ParseWindow decodes one window strictly, rejecting unknown fields and
// trailing data, then validates it.
func ParseWindow(r io.Reader) (Window, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxWindowBytes+1))
	if err != nil {
		return Window{}, fmt.Errorf("read window: %w", err)
	}
	if len(raw) > maxWindowBytes {
		return Window{}, fieldErr("window", "larger than 1 MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var w Window
	if err := dec.Decode(&w); err != nil {
		return Window{}, fmt.Errorf("%w: decode: %w", ErrInvalidWindow, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Window{}, fieldErr("window", "trailing data after the object")
	}
	if err := w.Validate(); err != nil {
		return Window{}, err
	}
	return w, nil
}

// Validate checks every field and the rules between them.
func (w Window) Validate() error {
	if w.SchemaVersion != WindowSchemaVersion {
		return fieldErr("schema_version", "unsupported version")
	}
	if !w.WindowID.Valid() {
		return fieldErr("window_id", "not a ULID")
	}
	if err := w.Input.validate(); err != nil {
		return err
	}
	if _, ok := w.Target.Label.Phase(); !ok {
		return fieldErr(fieldLabel, "not one of A-G")
	}
	if err := w.Provenance.validate(); err != nil {
		return err
	}
	if !w.EnvironmentDomain.Valid() {
		return fieldErr(fieldDomain, "unknown domain")
	}
	if err := w.Policies.validate(); err != nil {
		return err
	}
	if !w.Split.Split.Valid() {
		return fieldErr("split.split", "unknown split")
	}
	if !w.Split.GroupDigest.Valid() {
		return fieldErr("split.group_digest", "not a sha256 digest")
	}
	return nil
}

func (in AnalyzeInput) validate() error {
	if in.Contract != protocol.ModelContractAnalyzeAgentState {
		return fieldErr("input.contract", "not ANALYZE_AGENT_STATE")
	}
	if !in.Harness.Valid() {
		return fieldErr("input.harness", "unknown harness")
	}
	if err := in.validateTiming(); err != nil {
		return err
	}
	if in.NewChildProcesses < 0 {
		return fieldErr("input.new_child_processes", "negative")
	}
	if in.PreviousState == "" {
		if in.PreviousStateAgeSeconds != 0 {
			return fieldErr(fieldPreviousAge, "an age without a previous state")
		}
	} else if !in.PreviousState.Valid() {
		return fieldErr("input.previous_state", "unknown agent phase")
	}
	return in.Screen.validate()
}

// validateTiming checks the output counters against each other and
// against the time since the last output: bytes inside a trailing
// window mean the last output is inside it, and no bytes mean it is not.
func (in AnalyzeInput) validateTiming() error {
	s, b := in.SecondsSinceLastOutput, in.OutputBytes
	if !finiteNonNegative(s) {
		return fieldErr(fieldSinceLastOutput, "negative or not finite")
	}
	if !finiteNonNegative(in.PreviousStateAgeSeconds) {
		return fieldErr(fieldPreviousAge, "negative or not finite")
	}
	if b.Last500ms < 0 || b.Last2s < 0 || b.Last10s < 0 {
		return fieldErr(fieldOutputBytes, "negative count")
	}
	if b.Last500ms > b.Last2s || b.Last2s > b.Last10s {
		return fieldErr(fieldOutputBytes, "nested windows decrease")
	}
	for _, w := range []struct {
		bytes int64
		span  float64
		name  string
	}{{b.Last500ms, 0.5, "0.5 s"}, {b.Last2s, 2, "2 s"}, {b.Last10s, 10, "10 s"}} {
		if w.bytes > 0 && s > w.span {
			return fieldErr(fieldSinceLastOutput, "output counted in the last "+w.name+" but the last output is older")
		}
		if w.bytes == 0 && s < w.span {
			return fieldErr(fieldSinceLastOutput, "no output counted in the last "+w.name+" but the last output is inside it")
		}
	}
	return nil
}

func finiteNonNegative(f float64) bool {
	return f >= 0 && f <= 1e12
}

func (s RedactedScreen) validate() error {
	if !s.Redacted {
		return fieldErr("input.screen.redacted", "screen not marked redacted")
	}
	if s.Cols < 1 || s.Cols > maxScreenCols || s.Rows < 1 || s.Rows > maxScreenRows {
		return fieldErr("input.screen", "size out of range")
	}
	if int64(len(s.Lines)) > s.Rows {
		return fieldErr(fieldScreenLines, "more lines than rows")
	}
	for _, line := range s.Lines {
		// Rendered text only: an escape or control byte means raw
		// terminal output reached the window.
		if strings.ContainsFunc(line, unicode.IsControl) {
			return fieldErr(fieldScreenLines, "control or escape character: raw terminal output")
		}
	}
	if s.Digest != ScreenDigest(s.Lines) {
		return fieldErr("input.screen.digest", "does not match the lines")
	}
	return nil
}

func (p Provenance) validate() error {
	if !p.SessionID.Valid() {
		return fieldErr("provenance.session_id", "missing or malformed")
	}
	if !p.CaptureSessionID.Valid() {
		return fieldErr("provenance.capture_session_id", "missing or malformed")
	}
	switch p.SourceQuality {
	case protocol.SourceQualityPTYScreen, protocol.SourceQualityBootstrapPipePane,
		protocol.SourceQualityBootstrapImport, protocol.SourceQualitySynthetic:
	default:
		return fieldErr(fieldSourceQuality, "not a screen-derived source")
	}
	if p.SequenceFrom < 0 || p.SequenceTo < p.SequenceFrom {
		return fieldErr("provenance.sequence", "range is negative or reversed")
	}
	if p.WindowStart.IsZero() || p.WindowEnd.IsZero() || p.AnchorAt.IsZero() {
		return fieldErr("provenance", "missing timestamp")
	}
	if !p.WindowStart.Before(p.WindowEnd) {
		return fieldErr(fieldWindowEnd, "not after window_start")
	}
	if p.AnchorAt.Before(p.WindowStart) || p.AnchorAt.After(p.WindowEnd) {
		return fieldErr(fieldAnchorAt, "outside the window")
	}
	if len(p.SourceRefs) == 0 || len(p.SourceRefs) > maxSourceRefs {
		return fieldErr(fieldSourceRefs, "missing or too many")
	}
	for _, ref := range p.SourceRefs {
		if !ref.Valid() {
			return fieldErr(fieldSourceRefs, "malformed blob ref")
		}
	}
	if !p.SourceDigest.Valid() {
		return fieldErr(fieldSourceDigest, "not a sha256 digest")
	}
	return nil
}

func (p Policies) validate() error {
	for _, v := range []struct{ value, field string }{
		{p.RedactionPolicyVersion, fieldRedactionPolicy},
		{p.DomainMaskPolicyVersion, fieldDomainMaskPolicy},
	} {
		if v.value == "" || len(v.value) > maxPolicyVersion {
			return fieldErr(v.field, "missing or too long")
		}
		if strings.ContainsFunc(v.value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return fieldErr(v.field, "space or control character")
		}
	}
	return nil
}
