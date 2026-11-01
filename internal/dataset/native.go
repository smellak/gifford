package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	// NativeLabelSidecarSchemaVersion is the only native evidence schema read
	// by this package.
	NativeLabelSidecarSchemaVersion int64 = 1
	maxNativeLabelSidecarBytes            = 64 << 20
)

const nativeLabelSidecarDigestDomain = "gifford.dataset.native-labels.v1"

// ErrInvalidNativeLabels wraps every sidecar or evidence failure.
var ErrInvalidNativeLabels = errors.New("invalid native label evidence")

// NativeLabelBoundary is one durable event used as a temporal boundary. At is
// the Core receipt time; an emitter timestamp is deliberately absent.
type NativeLabelBoundary struct {
	EventID   protocol.ULID              `json:"event_id"`
	EventType protocol.EventType         `json:"event_type"`
	Sequence  int64                      `json:"sequence"`
	At        time.Time                  `json:"received_at"`
	Result    protocol.NativeEventResult `json:"result,omitempty" window:"optional"`
}

// NativeLabelEvidence is the content-free evidence produced by alignment
// before the screen is projected. Basis is the lifecycle event that proves the
// state; Start and End are its durable transition boundaries. A configured
// maximum may conservatively shorten the usable span, and a resumed
// lower-priority state can have Basis before Start.
type NativeLabelEvidence struct {
	WindowID    protocol.ULID       `json:"window_id"`
	Label       Label               `json:"label"`
	RunDigest   protocol.Digest     `json:"run_digest"`
	EpochDigest protocol.Digest     `json:"epoch_digest"`
	Start       NativeLabelBoundary `json:"start"`
	Basis       NativeLabelBoundary `json:"basis"`
	End         NativeLabelBoundary `json:"end"`
}

// NativeLabelRecord binds evidence to the final window and screen digests.
// It carries no screen, prompt, refs, tool arguments or model input.
type NativeLabelRecord struct {
	WindowID     protocol.ULID       `json:"window_id"`
	WindowDigest protocol.Digest     `json:"window_digest"`
	ScreenDigest protocol.Digest     `json:"screen_digest"`
	Label        Label               `json:"label"`
	RunDigest    protocol.Digest     `json:"run_digest"`
	EpochDigest  protocol.Digest     `json:"epoch_digest"`
	Start        NativeLabelBoundary `json:"start"`
	Basis        NativeLabelBoundary `json:"basis"`
	End          NativeLabelBoundary `json:"end"`
}

// NativeLabelSidecar is the versioned evidence file sealed by Manifest.
type NativeLabelSidecar struct {
	SchemaVersion int64               `json:"schema_version"`
	Entries       []NativeLabelRecord `json:"entries"`
}

func nativeLabel(label Label) bool {
	return label == LabelB || label == LabelC || label == LabelD ||
		label == LabelE || label == LabelF
}

func (b NativeLabelBoundary) validate(field string) error {
	if !b.EventID.Valid() {
		return fmt.Errorf("%w: %s.event_id", ErrInvalidNativeLabels, field)
	}
	if !protocol.IsNativeEventType(b.EventType) &&
		b.EventType != protocol.EventTypeAdapterCapabilitiesChanged {
		return fmt.Errorf("%w: %s.event_type", ErrInvalidNativeLabels, field)
	}
	if b.Sequence < 0 {
		return fmt.Errorf("%w: %s.sequence", ErrInvalidNativeLabels, field)
	}
	if b.At.IsZero() || b.At != b.At.UTC() {
		return fmt.Errorf("%w: %s.received_at", ErrInvalidNativeLabels, field)
	}
	if b.Result != "" && !b.Result.Valid() {
		return fmt.Errorf("%w: %s.result", ErrInvalidNativeLabels, field)
	}
	if (b.EventType == protocol.EventTypeTurnCompleted ||
		b.EventType == protocol.EventTypeQuestionResolved) && b.Result == "" {
		return fmt.Errorf("%w: %s.result missing", ErrInvalidNativeLabels, field)
	}
	return nil
}

func (e NativeLabelEvidence) validate() error {
	if !e.WindowID.Valid() || !nativeLabel(e.Label) || !e.RunDigest.Valid() ||
		!e.EpochDigest.Valid() {
		return ErrInvalidNativeLabels
	}
	for _, item := range []struct {
		field    string
		boundary NativeLabelBoundary
	}{{"start", e.Start}, {"basis", e.Basis}, {"end", e.End}} {
		if err := item.boundary.validate(item.field); err != nil {
			return err
		}
	}
	if e.Basis.At.After(e.Start.At) || e.End.At.Before(e.Start.At) ||
		e.Basis.Sequence > e.Start.Sequence || e.End.Sequence <= e.Start.Sequence {
		return fmt.Errorf("%w: boundary order", ErrInvalidNativeLabels)
	}
	wantBasis := map[Label][]protocol.EventType{
		LabelB: {protocol.EventTypePromptSubmitted, protocol.EventTypeTurnStarted, protocol.EventTypeTurnStreaming},
		LabelC: {protocol.EventTypeToolStarted},
		LabelD: {protocol.EventTypeApprovalRequested},
		LabelE: {protocol.EventTypeQuestionRequested},
		LabelF: {protocol.EventTypeTurnCompleted},
	}
	if !slices.Contains(wantBasis[e.Label], e.Basis.EventType) {
		return fmt.Errorf("%w: basis.event_type", ErrInvalidNativeLabels)
	}
	if e.Label == LabelF && e.Basis.Result != protocol.NativeEventResultFailed {
		return fmt.Errorf("%w: basis.result", ErrInvalidNativeLabels)
	}
	if e.Label != LabelF && e.Basis.Result != "" {
		return fmt.Errorf("%w: unexpected basis.result", ErrInvalidNativeLabels)
	}
	return nil
}

func (r NativeLabelRecord) evidence() NativeLabelEvidence {
	return NativeLabelEvidence{
		WindowID: r.WindowID, Label: r.Label, RunDigest: r.RunDigest,
		EpochDigest: r.EpochDigest, Start: r.Start, Basis: r.Basis, End: r.End,
	}
}

// Validate checks the sidecar structure and strict window-id ordering.
func (s NativeLabelSidecar) Validate() error {
	if s.SchemaVersion != NativeLabelSidecarSchemaVersion || len(s.Entries) == 0 ||
		len(s.Entries) > MaxPipelineWindows {
		return ErrInvalidNativeLabels
	}
	for i, entry := range s.Entries {
		if err := entry.evidence().validate(); err != nil {
			return err
		}
		if !entry.WindowDigest.Valid() || !entry.ScreenDigest.Valid() {
			return ErrInvalidNativeLabels
		}
		if i > 0 && s.Entries[i-1].WindowID >= entry.WindowID {
			return fmt.Errorf("%w: entries not sorted or repeated", ErrInvalidNativeLabels)
		}
	}
	return nil
}

// NativeLabelSidecarDigest hashes the canonical sidecar document. The file's
// trailing newline is transport formatting and is not part of the digest.
func NativeLabelSidecarDigest(sidecar NativeLabelSidecar) (protocol.Digest, error) {
	if err := sidecar.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(sidecar)
	if err != nil {
		return "", fmt.Errorf("%w: encode", ErrInvalidNativeLabels)
	}
	h := sha256.New()
	_, _ = io.WriteString(h, nativeLabelSidecarDigestDomain)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(raw)
	return protocol.Digest("sha256:" + hex.EncodeToString(h.Sum(nil))), nil
}

// AttachNativeLabels seals evidence onto an inventory after the ordinary
// pipeline has built and validated its screen windows. With no native windows
// and no evidence it returns the inventory byte-for-byte unchanged.
func AttachNativeLabels(inv Inventory, evidence []NativeLabelEvidence) (Inventory, error) {
	byWindow := make(map[protocol.ULID]NativeLabelEvidence, len(evidence))
	for _, item := range evidence {
		if err := item.validate(); err != nil {
			return Inventory{}, err
		}
		if _, exists := byWindow[item.WindowID]; exists {
			return Inventory{}, fmt.Errorf("%w: repeated window", ErrInvalidNativeLabels)
		}
		byWindow[item.WindowID] = item
	}
	var records []NativeLabelRecord
	for _, window := range inv.Windows {
		item, hasEvidence := byWindow[window.WindowID]
		isNative := window.Target.Source == LabelSourceNative
		if isNative != hasEvidence {
			return Inventory{}, fmt.Errorf("%w: windows and evidence disagree", ErrInvalidNativeLabels)
		}
		if !isNative {
			continue
		}
		if item.Label != window.Target.Label {
			return Inventory{}, fmt.Errorf("%w: label mismatch", ErrInvalidNativeLabels)
		}
		digest, err := WindowDigest(window)
		if err != nil {
			return Inventory{}, fmt.Errorf("%w: window digest", ErrInvalidNativeLabels)
		}
		records = append(records, NativeLabelRecord{
			WindowID: window.WindowID, WindowDigest: digest, ScreenDigest: window.Screen.Digest,
			Label: item.Label, RunDigest: item.RunDigest, EpochDigest: item.EpochDigest,
			Start: item.Start, Basis: item.Basis, End: item.End,
		})
		delete(byWindow, window.WindowID)
	}
	if len(byWindow) != 0 {
		return Inventory{}, fmt.Errorf("%w: evidence without a window", ErrInvalidNativeLabels)
	}
	if len(records) == 0 {
		if len(evidence) != 0 {
			return Inventory{}, ErrInvalidNativeLabels
		}
		return inv, nil
	}
	slices.SortFunc(records, func(a, b NativeLabelRecord) int {
		return strings.Compare(string(a.WindowID), string(b.WindowID))
	})
	sidecar := NativeLabelSidecar{SchemaVersion: NativeLabelSidecarSchemaVersion, Entries: records}
	digest, err := NativeLabelSidecarDigest(sidecar)
	if err != nil {
		return Inventory{}, err
	}
	inv.NativeLabels = &sidecar
	inv.Manifest.NativeLabels = &NativeLabelsPolicy{
		SchemaVersion: NativeLabelSidecarSchemaVersion, Entries: int64(len(records)), Digest: digest,
	}
	inv.Manifest.DatasetDigest = ManifestDigest(inv.Manifest)
	if err := inv.Manifest.Validate(); err != nil {
		return Inventory{}, fmt.Errorf("%w: %w", ErrInvalidNativeLabels, err)
	}
	return inv, nil
}

// ParseNativeLabelSidecar decodes one bounded, strict sidecar document.
func ParseNativeLabelSidecar(r io.Reader) (NativeLabelSidecar, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxNativeLabelSidecarBytes+1))
	if err != nil || len(raw) > maxNativeLabelSidecarBytes || !utf8.Valid(raw) ||
		!surrogatesPaired(raw) {
		return NativeLabelSidecar{}, ErrInvalidNativeLabels
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var sidecar NativeLabelSidecar
	if err := dec.Decode(&sidecar); err != nil {
		return NativeLabelSidecar{}, ErrInvalidNativeLabels
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return NativeLabelSidecar{}, ErrInvalidNativeLabels
	}
	if path, ok := missingMember(reflect.TypeOf(NativeLabelSidecar{}), raw, ""); !ok {
		return NativeLabelSidecar{}, fmt.Errorf("%w: %s missing", ErrInvalidNativeLabels, path)
	}
	var members struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(raw, &members); err != nil {
		return NativeLabelSidecar{}, ErrInvalidNativeLabels
	}
	for i, entry := range members.Entries {
		prefix := fmt.Sprintf("entries[%d].", i)
		if path, ok := missingMember(reflect.TypeOf(NativeLabelRecord{}), entry, prefix); !ok {
			return NativeLabelSidecar{}, fmt.Errorf("%w: %s missing", ErrInvalidNativeLabels, path)
		}
	}
	if err := sidecar.Validate(); err != nil {
		return NativeLabelSidecar{}, err
	}
	return sidecar, nil
}

// ValidateNativeLabelSidecar verifies that sidecar is the exact native-label
// evidence document sealed by m and that every record names the same final
// window digest and target. Window contents are deliberately not needed.
func ValidateNativeLabelSidecar(m Manifest, sidecar NativeLabelSidecar) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("%w: invalid manifest", ErrInvalidNativeLabels)
	}
	policy := m.NativeLabels
	if policy == nil {
		return fmt.Errorf("%w: manifest has no native labels", ErrInvalidNativeLabels)
	}
	if err := policy.validate(); err != nil {
		return fmt.Errorf("%w: manifest policy", ErrInvalidNativeLabels)
	}
	if err := sidecar.Validate(); err != nil {
		return err
	}
	if int64(len(sidecar.Entries)) != policy.Entries {
		return fmt.Errorf("%w: manifest and sidecar disagree", ErrInvalidNativeLabels)
	}
	digest, err := NativeLabelSidecarDigest(sidecar)
	if err != nil || digest != policy.Digest {
		return fmt.Errorf("%w: sidecar digest mismatch", ErrInvalidNativeLabels)
	}
	entries := make(map[protocol.ULID]ManifestEntry, int(policy.Entries))
	for _, entry := range m.Entries {
		if entry.LabelSource == LabelSourceNative {
			entries[entry.WindowID] = entry
		}
	}
	if int64(len(entries)) != policy.Entries {
		return fmt.Errorf("%w: manifest native entries disagree", ErrInvalidNativeLabels)
	}
	for _, record := range sidecar.Entries {
		entry, ok := entries[record.WindowID]
		if !ok || entry.Label != record.Label || entry.WindowDigest != record.WindowDigest {
			return fmt.Errorf("%w: sidecar entry mismatch", ErrInvalidNativeLabels)
		}
		delete(entries, record.WindowID)
	}
	if len(entries) != 0 {
		return fmt.Errorf("%w: sidecar entries missing", ErrInvalidNativeLabels)
	}
	return nil
}

func validateNativeInventory(inv Inventory) error {
	policy, sidecar := inv.Manifest.NativeLabels, inv.NativeLabels
	if policy == nil && sidecar == nil {
		for _, window := range inv.Windows {
			if window.Target.Source == LabelSourceNative {
				return fmt.Errorf("%w: native window without sidecar", ErrInvalidNativeLabels)
			}
		}
		return nil
	}
	if policy == nil || sidecar == nil || int64(len(sidecar.Entries)) != policy.Entries {
		return fmt.Errorf("%w: manifest and sidecar disagree", ErrInvalidNativeLabels)
	}
	if err := ValidateNativeLabelSidecar(inv.Manifest, *sidecar); err != nil {
		return err
	}
	byID := make(map[protocol.ULID]Window, len(inv.Windows))
	for _, window := range inv.Windows {
		byID[window.WindowID] = window
	}
	for _, entry := range sidecar.Entries {
		window, ok := byID[entry.WindowID]
		if !ok || window.Target.Source != LabelSourceNative || window.Target.Label != entry.Label ||
			window.Screen.Digest != entry.ScreenDigest {
			return fmt.Errorf("%w: sidecar entry mismatch", ErrInvalidNativeLabels)
		}
		digest, digestErr := WindowDigest(window)
		if digestErr != nil || digest != entry.WindowDigest {
			return fmt.Errorf("%w: sidecar window digest mismatch", ErrInvalidNativeLabels)
		}
	}
	return nil
}
