package session

import (
	"strings"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

const (
	// PermissionDialogPolicyVersion names both the exact matcher set and the
	// causal proof required before a dialog is reduced to its canonical row.
	PermissionDialogPolicyVersion = "permission-dialog-v2"
	// PermissionDialogMarker is the complete model-visible form. No row from
	// the physical snapshot survives canonicalisation.
	PermissionDialogMarker = "[permission-dialog]"
	// PermissionDialogEvidenceVersion marks captures that record every outer
	// input arrival needed to replay the live causal proof.
	PermissionDialogEvidenceVersion = "permission-dialog-evidence-v1"
)

// Fixed withholding reasons. They carry no terminal content.
const (
	PermissionDialogNotMatched          = "permission_dialog_not_matched"
	PermissionDialogAmbiguous           = "permission_dialog_ambiguous"
	PermissionDialogNoPriorFrame        = "permission_dialog_no_prior_frame"
	PermissionDialogHumanInput          = "permission_dialog_human_input"
	PermissionDialogGuardNotClosed      = "permission_dialog_guard_not_closed"
	PermissionDialogInputUnaligned      = "permission_dialog_input_unaligned"
	PermissionDialogFrameIncomplete     = "permission_dialog_frame_incomplete"
	PermissionDialogResized             = "permission_dialog_resized"
	PermissionDialogSignatureChanged    = "permission_dialog_signature_changed"
	PermissionDialogSignatureUnmeasured = "permission_dialog_signature_unmeasured"
	PermissionDialogEvidenceMissing     = "permission_dialog_evidence_missing"
)

// PermissionDialogSource opts one exact recorded harness/version pair into
// the observation policy. HarnessVersion is the complete bounded line stored
// in pty.opened, not merely the semantic version parsed from it.
type PermissionDialogSource struct {
	Harness        protocol.Harness `yaml:"harness"`
	HarnessVersion string           `yaml:"harness_version"`
}

// PermissionDialogSources reports whether an exact pair is enabled.
func PermissionDialogSources(sources []PermissionDialogSource, harness protocol.Harness, version string) bool {
	for _, source := range sources {
		if source.Harness == harness && source.HarnessVersion == version {
			return true
		}
	}
	return false
}

// PermissionDialogScreen is the explicit synthetic geometry returned by the
// policy. CursorRow and CursorCol are canonical coordinates, never a measured
// physical cursor copied from the modal.
type PermissionDialogScreen struct {
	Version              string
	Rows                 []string
	Cols                 int
	CursorRow, CursorCol int
	Episode              uint64
}

// CanonicalPermissionDialog returns a fresh copy of the closed synthetic
// form. The marker is ASCII, so its rune and terminal-cell widths agree.
func CanonicalPermissionDialog(episode uint64) PermissionDialogScreen {
	return PermissionDialogScreen{
		Version: PermissionDialogPolicyVersion,
		Rows:    []string{PermissionDialogMarker},
		Cols:    utf8.RuneCountInString(PermissionDialogMarker),
		Episode: episode,
	}
}

// PermissionDialogFrame is all causal evidence the shared tracker consumes.
// Live and replay construct the same value at a complete PTY-read boundary.
type PermissionDialogFrame struct {
	Snapshot      vt.Snapshot
	GuardClosed   bool
	InputAligned  bool
	KeyGeneration uint64
}

type permissionDialogSpec struct {
	harness protocol.Harness
	version string
	match   func(vt.Snapshot) permissionDialogMatch
}

type permissionDialogMatch struct {
	count     int
	signature string
}

// SupportsPermissionDialog reports whether the measured matcher table has an
// exact signature for the recorded harness/version.
func SupportsPermissionDialog(harness protocol.Harness, recordedVersion string) bool {
	return permissionDialogSpecFor(harness, recordedVersion) != nil
}

func permissionDialogSpecFor(harness protocol.Harness, recordedVersion string) *permissionDialogSpec {
	version := profiles.ParseVersion(recordedVersion)
	switch {
	case harness == protocol.HarnessAntigravity && version == "1.4.8":
		return &permissionDialogSpec{harness: harness, version: version, match: matchAntigravityPermission}
	case harness == protocol.HarnessClaudeCode && version == "2.1.280":
		return &permissionDialogSpec{harness: harness, version: version, match: matchClaudePermission}
	case harness == protocol.HarnessOpencode && version == "1.18.29":
		return &permissionDialogSpec{harness: harness, version: version, match: matchOpenCodePermission}
	default:
		return nil
	}
}

// PermissionDialogTracker proves one modal episode. Its zero value has no
// matcher and therefore never releases a screen.
type PermissionDialogTracker struct {
	spec *permissionDialogSpec

	priorFrame    bool
	priorKey      uint64
	active        bool
	valid         bool
	signature     string
	appearanceKey uint64
	episode       uint64
	reason        string
}

// NewPermissionDialogTracker returns the tracker for one exact measured
// harness/version. ok is false when no signature was measured.
func NewPermissionDialogTracker(harness protocol.Harness, recordedVersion string) (*PermissionDialogTracker, bool) {
	spec := permissionDialogSpecFor(harness, recordedVersion)
	if spec == nil {
		return nil, false
	}
	return &PermissionDialogTracker{spec: spec, reason: PermissionDialogNotMatched}, true
}

// Observe folds one complete PTY-read boundary. A partial/synchronised frame,
// an open guard or crossed input destroys an active proof; it never becomes
// valid later merely because the same signature remains visible.
func (t *PermissionDialogTracker) Observe(frame PermissionDialogFrame) {
	if t == nil || t.spec == nil {
		return
	}
	if !frame.GuardClosed {
		t.invalidate(PermissionDialogGuardNotClosed)
		return
	}
	if !frame.InputAligned {
		t.invalidate(PermissionDialogInputUnaligned)
		return
	}
	if frame.Snapshot.Pending || frame.Snapshot.SyncOpen {
		t.invalidate(PermissionDialogFrameIncomplete)
		return
	}
	match := t.spec.match(frame.Snapshot)
	switch {
	case match.count == 0:
		t.active, t.valid, t.signature = false, false, ""
		t.priorFrame, t.priorKey = true, frame.KeyGeneration
		t.reason = PermissionDialogNotMatched
	case match.count > 1:
		if !t.active {
			t.episode++
		}
		t.active, t.valid, t.priorFrame = true, false, false
		t.signature, t.appearanceKey = "", frame.KeyGeneration
		t.reason = PermissionDialogAmbiguous
	case match.signature == "":
		if !t.active {
			t.episode++
		}
		// The shape is unique, but it is outside the measured position. Keep
		// it active so callers cannot fall back to a physical projection.
		t.active, t.valid, t.priorFrame = true, false, false
		t.signature, t.appearanceKey = "", frame.KeyGeneration
		t.reason = PermissionDialogSignatureUnmeasured
	case !t.active:
		hadPrior, priorKey := t.priorFrame, t.priorKey
		t.active, t.priorFrame = true, false
		t.signature, t.appearanceKey = match.signature, frame.KeyGeneration
		t.episode++
		switch {
		case !hadPrior:
			t.valid, t.reason = false, PermissionDialogNoPriorFrame
		case priorKey != frame.KeyGeneration:
			t.valid, t.reason = false, PermissionDialogHumanInput
		default:
			t.valid, t.reason = true, ""
		}
	case !t.valid:
		// An invalid episode stays invalid until one complete non-modal
		// frame ends it.
	case t.signature != match.signature:
		t.valid, t.reason = false, PermissionDialogSignatureChanged
	case t.appearanceKey != frame.KeyGeneration:
		t.valid, t.reason = false, PermissionDialogHumanInput
	default:
		t.reason = ""
	}
}

// HumanInput invalidates the current episode at the outer-input arrival
// generation. Terminal replies do not call it; unclassified arrivals do.
func (t *PermissionDialogTracker) HumanInput(generation uint64) {
	if t == nil || t.spec == nil {
		return
	}
	if t.active && generation != t.appearanceKey {
		t.valid, t.reason = false, PermissionDialogHumanInput
	}
}

// Resize destroys both an active proof and the prior-frame evidence. A new
// episode needs a complete non-modal frame at the new size first.
func (t *PermissionDialogTracker) Resize() {
	if t == nil || t.spec == nil {
		return
	}
	if t.active {
		t.valid, t.reason = false, PermissionDialogResized
	}
	t.priorFrame = false
}

// Screen returns the canonical dialog only while the current causal facts
// still match the proven episode.
func (t *PermissionDialogTracker) Screen(guardClosed, inputAligned bool, keyGeneration uint64) (PermissionDialogScreen, string) {
	if t == nil || t.spec == nil {
		return PermissionDialogScreen{}, PermissionDialogEvidenceMissing
	}
	switch {
	case !guardClosed:
		t.invalidate(PermissionDialogGuardNotClosed)
	case !inputAligned:
		t.invalidate(PermissionDialogInputUnaligned)
	case t.active && keyGeneration != t.appearanceKey:
		t.valid, t.reason = false, PermissionDialogHumanInput
	}
	if !t.active || !t.valid {
		reason := t.reason
		if reason == "" {
			reason = PermissionDialogNotMatched
		}
		return PermissionDialogScreen{}, reason
	}
	return CanonicalPermissionDialog(t.episode), ""
}

// Active reports that an exact or ambiguous permission candidate is still on
// screen. Callers must not fall back to a physical empty-input projection for
// an active candidate whose causal proof was invalidated.
func (t *PermissionDialogTracker) Active() bool {
	return t != nil && t.spec != nil && t.active
}

func (t *PermissionDialogTracker) invalidate(reason string) {
	t.reason = reason
	if t.active {
		t.valid = false
	}
	t.priorFrame = false
}

func matchAntigravityPermission(snap vt.Snapshot) permissionDialogMatch {
	if (snap.Cols != 80 || snap.Rows != 24) && (snap.Cols != 200 || snap.Rows != 50) {
		return permissionDialogMatch{}
	}
	wantStart := 10
	if snap.Cols == 200 {
		wantStart = 17
	}
	var starts []int
	for start := 0; start+13 <= len(snap.Lines); start++ {
		rows := snap.Lines[start : start+13]
		if rows[0] != "File access" || rows[1] != strings.Repeat("─", snap.Cols) || rows[2] != "" ||
			rows[3] == "" || !strings.HasPrefix(rows[4], "Reason:") || rows[5] != "" ||
			rows[6] != "Allow access to this file?" || rows[7] != "1. Yes, allow access" ||
			rows[8] != "2. Yes, and always allow non-workspace access" || rows[9] != "3. No, deny access" ||
			rows[10] != "" || rows[11] != "↑/↓ Navigate" || !strings.HasPrefix(rows[12], "esc to cancel") {
			continue
		}
		starts = append(starts, start)
	}
	if len(starts) != 1 || starts[0] != wantStart {
		return permissionDialogMatch{count: len(starts)}
	}
	return permissionDialogMatch{count: 1, signature: "antigravity-full-width"}
}

const (
	claudePermissionAnyStart = -1
	// claudePermissionStartRow keeps the reviewed any-row policy. Set it to 17
	// to restore a fixed-row matcher without changing the scan.
	claudePermissionStartRow = claudePermissionAnyStart
)

func matchClaudePermission(snap vt.Snapshot) permissionDialogMatch {
	return matchClaudePermissionAt(snap, claudePermissionStartRow)
}

func matchClaudePermissionAt(snap vt.Snapshot, acceptedStart int) permissionDialogMatch {
	if snap.Cols != 200 || snap.Rows != 50 {
		return permissionDialogMatch{}
	}
	const (
		tip      = ` Tip: auto mode handles these prompts for you — choose "switch to auto mode" below`
		question = " Do you want to proceed?"
		option1  = " 1. Yes"
		option2  = "   2. Yes, and switch to auto mode · auto mode handles these prompts for you"
		option3  = "   3. No"
		footer   = " Esc to cancel · Tab to amend"
	)
	var starts []int
	for start := 0; start+13 <= len(snap.Lines); start++ {
		rows := snap.Lines[start : start+13]
		if rows[0] != strings.Repeat("─", snap.Cols) || !nonBlankFrom(rows[1], 1) || rows[2] != tip ||
			rows[3] != "" || !nonBlankFrom(rows[4], 3) || !nonBlankFrom(rows[5], 3) || rows[6] != "" ||
			rows[7] != question || rows[8] != option1 || rows[9] != option2 || rows[10] != option3 ||
			rows[11] != "" || rows[12] != footer {
			continue
		}
		starts = append(starts, start)
	}
	if len(starts) != 1 || (acceptedStart != claudePermissionAnyStart && starts[0] != acceptedStart) {
		return permissionDialogMatch{count: len(starts)}
	}
	return permissionDialogMatch{count: 1, signature: "claude-200x50"}
}

func nonBlankFrom(line string, column int) bool {
	runes := []rune(line)
	if len(runes) <= column {
		return false
	}
	for i := range column {
		if runes[i] != ' ' {
			return false
		}
	}
	return strings.TrimSpace(string(runes[column:])) != ""
}

func matchOpenCodePermission(snap vt.Snapshot) permissionDialogMatch {
	if snap.Cols != 120 || snap.Rows != 50 {
		return permissionDialogMatch{}
	}
	type candidateMatch struct {
		name  string
		title int
	}
	var matches []candidateMatch
	for title := range snap.Lines {
		if openCodeContent(snap.Lines[title]) != "△ Permission required" {
			continue
		}
		for _, candidate := range []struct {
			start, options int
			name           string
		}{{title - 1, title + 6, "opencode-simple"}, {title - 1, title + 8, "opencode-expanded"}} {
			if candidate.start < 0 || candidate.options+1 >= len(snap.Lines) ||
				!openCodeChannel(snap.Lines[candidate.start]) || !openCodeChannel(snap.Lines[candidate.options+1]) ||
				!openCodePermissionOptions(openCodeContent(snap.Lines[candidate.options])) {
				continue
			}
			channel := true
			for row := candidate.start + 1; row <= candidate.options; row++ {
				channel = channel && openCodeHasChannel(snap.Lines[row])
			}
			if !channel {
				continue
			}
			matches = append(matches, candidateMatch{name: candidate.name, title: title})
		}
	}
	if len(matches) != 1 {
		return permissionDialogMatch{count: len(matches)}
	}
	// Measured layouts put the title on one of these two rows only.
	want := matches[0].name == "opencode-simple" && matches[0].title == 40 ||
		matches[0].name == "opencode-expanded" && matches[0].title == 38
	if !want {
		return permissionDialogMatch{}
	}
	return permissionDialogMatch{count: 1, signature: matches[0].name}
}

func openCodeChannel(line string) bool {
	runes := []rune(line)
	return len(runes) > 2 && runes[0] == ' ' && runes[1] == ' ' && runes[2] == '┃' &&
		strings.TrimSpace(string(runes[3:])) == ""
}

func openCodeHasChannel(line string) bool {
	runes := []rune(line)
	return len(runes) > 2 && runes[0] == ' ' && runes[1] == ' ' && runes[2] == '┃'
}

func openCodeContent(line string) string {
	runes := []rune(line)
	if len(runes) <= 2 || runes[0] != ' ' || runes[1] != ' ' || runes[2] != '┃' {
		return ""
	}
	return strings.TrimSpace(string(runes[3:]))
}

func openCodePermissionOptions(content string) bool {
	const options = "Allow once   Allow always   Reject"
	if !strings.HasPrefix(content, options) {
		return false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(content, options))
	parts := []string{"ctrl+f fullscreen", "⇆ select", "enter confirm"}
	for _, part := range parts {
		at := strings.Index(rest, part)
		if at < 0 {
			return false
		}
		rest = rest[at+len(part):]
	}
	return strings.TrimSpace(rest) == ""
}
