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
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// ManifestSchemaVersion is the only manifest schema this package reads.
const ManifestSchemaVersion = 1

// maxManifestBytes bounds one encoded manifest. An entry is a few
// hundred bytes, so this holds well over a hundred thousand windows.
const maxManifestBytes = 64 << 20

// manifestDigestDomain names the dataset digest encoding and its
// version. A change to the encoding is a new name.
const manifestDigestDomain = "gifford.dataset.manifest.v1"

// ErrInvalidManifest is wrapped by every manifest failure.
var ErrInvalidManifest = errors.New("invalid manifest")

// ManifestError names the manifest field that failed and why. Like
// FieldError, it never carries content.
type ManifestError struct {
	Field  string
	Reason string
}

// Error implements error.
func (e *ManifestError) Error() string {
	return fmt.Sprintf("invalid manifest: %s: %s", e.Field, e.Reason)
}

// Unwrap makes every ManifestError match ErrInvalidManifest.
func (e *ManifestError) Unwrap() error { return ErrInvalidManifest }

func manifestErr(field, reason string) error {
	return &ManifestError{Field: field, Reason: reason}
}

// Manifest describes a dataset of windows by hashes, provenance
// digests, policy versions and counts. It holds no window content:
// rebuilding it from the same windows gives the same manifest, and its
// digest names the dataset.
type Manifest struct {
	SchemaVersion       int64                  `json:"schema_version"`
	Contract            protocol.ModelContract `json:"contract"`
	WindowSchemaVersion int64                  `json:"window_schema_version"`
	// Policies are the versions every window was built under: a
	// dataset never mixes policy versions.
	Policies Policies        `json:"policies"`
	Counts   ManifestCounts  `json:"counts"`
	Entries  []ManifestEntry `json:"entries"`
	// DatasetDigest is ManifestDigest over the rest of the manifest.
	DatasetDigest protocol.Digest `json:"dataset_digest"`
}

// ManifestEntry is one window, named and placed without its content.
type ManifestEntry struct {
	WindowID          protocol.ULID     `json:"window_id"`
	Split             Split             `json:"split"`
	Label             Label             `json:"label"`
	EnvironmentDomain EnvironmentDomain `json:"environment_domain"`
	// WindowDigest is WindowDigest of the window: it changes with any
	// byte of the window, screen included.
	WindowDigest protocol.Digest `json:"window_digest"`
	GroupDigest  protocol.Digest `json:"group_digest"`
	SourceDigest protocol.Digest `json:"source_digest"`
}

// ManifestCounts counts the entries by split, label and domain. Every
// bucket is a member, so a zero count is stated, never omitted.
type ManifestCounts struct {
	Windows  int64        `json:"windows"`
	BySplit  SplitCounts  `json:"by_split"`
	ByLabel  LabelCounts  `json:"by_label"`
	ByDomain DomainCounts `json:"by_domain"`
}

// SplitCounts counts entries per split.
type SplitCounts struct {
	Train      int64 `json:"train"`
	Validation int64 `json:"validation"`
	Test       int64 `json:"test"`
}

// LabelCounts counts entries per label.
type LabelCounts struct {
	A int64 `json:"A"`
	B int64 `json:"B"`
	C int64 `json:"C"`
	D int64 `json:"D"`
	E int64 `json:"E"`
	F int64 `json:"F"`
	G int64 `json:"G"`
}

// DomainCounts counts entries per environment domain.
type DomainCounts struct {
	BankGeneric  int64 `json:"bank_generic"`
	FleetManaged int64 `json:"fleet_managed"`
}

// WindowDigest is the SHA-256 of the window's JSON encoding. The
// encoding is deterministic: fixed field order, no maps, RFC 3339
// times and shortest-form numbers.
func WindowDigest(w Window) (protocol.Digest, error) {
	enc, err := json.Marshal(w)
	if err != nil {
		return "", fmt.Errorf("encode window: %w", err)
	}
	sum := sha256.Sum256(enc)
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:])), nil
}

// BuildManifest validates every window and describes them. It refuses
// an empty dataset, a repeated window id, windows built under different
// policy versions, and a group placed in two splits: a group's windows
// share one split, so no group leaks between train and test.
func BuildManifest(windows []Window) (Manifest, error) {
	if len(windows) == 0 {
		return Manifest{}, manifestErr("entries", "no windows")
	}
	m := Manifest{
		SchemaVersion:       ManifestSchemaVersion,
		Contract:            protocol.ModelContractAnalyzeAgentState,
		WindowSchemaVersion: WindowSchemaVersion,
		Policies:            windows[0].Policies,
		Entries:             make([]ManifestEntry, 0, len(windows)),
	}
	for i, w := range windows {
		if err := w.Validate(); err != nil {
			return Manifest{}, fmt.Errorf("%w: window %d: %w", ErrInvalidManifest, i, err)
		}
		if w.Policies != m.Policies {
			return Manifest{}, manifestErr("policies", fmt.Sprintf("window %d was built under other policy versions", i))
		}
		digest, err := WindowDigest(w)
		if err != nil {
			return Manifest{}, fmt.Errorf("%w: window %d: %w", ErrInvalidManifest, i, err)
		}
		m.Entries = append(m.Entries, ManifestEntry{
			WindowID:          w.WindowID,
			Split:             w.Split.Split,
			Label:             w.Target.Label,
			EnvironmentDomain: w.EnvironmentDomain,
			WindowDigest:      digest,
			GroupDigest:       w.Split.GroupDigest,
			SourceDigest:      w.Provenance.SourceDigest,
		})
	}
	slices.SortFunc(m.Entries, func(a, b ManifestEntry) int { return strings.Compare(string(a.WindowID), string(b.WindowID)) })
	m.Counts = countEntries(m.Entries)
	if err := m.checkEntries(); err != nil {
		return Manifest{}, err
	}
	m.DatasetDigest = ManifestDigest(m)
	return m, nil
}

// ManifestDigest is the digest a manifest must carry. Its preimage is
// canonical: the domain name, the schema versions, the contract and
// the policy versions, the entry count, then each entry's fields in
// order. Every integer is decimal text and every field ends in a NUL;
// no field can hold a NUL, so no two manifests share a preimage. The
// counts are derived from the entries, so they need no bytes of their
// own.
func ManifestDigest(m Manifest) protocol.Digest {
	h := sha256.New()
	field := func(s string) {
		_, _ = io.WriteString(h, s)
		_, _ = h.Write([]byte{0})
	}
	field(manifestDigestDomain)
	field(fmt.Sprint(m.SchemaVersion))
	field(fmt.Sprint(m.WindowSchemaVersion))
	field(string(m.Contract))
	field(m.Policies.RedactionPolicyVersion)
	field(m.Policies.HumanInputPolicyVersion)
	field(m.Policies.DomainMaskPolicyVersion)
	field(fmt.Sprint(len(m.Entries)))
	for _, e := range m.Entries {
		field(string(e.WindowID))
		field(string(e.Split))
		field(string(e.Label))
		field(string(e.EnvironmentDomain))
		field(string(e.WindowDigest))
		field(string(e.GroupDigest))
		field(string(e.SourceDigest))
	}
	return protocol.Digest("sha256:" + hex.EncodeToString(h.Sum(nil)))
}

// ParseManifest decodes one manifest strictly, as ParseWindow decodes
// a window, then validates it.
func ParseManifest(r io.Reader) (Manifest, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	if len(raw) > maxManifestBytes {
		return Manifest{}, manifestErr("manifest", "larger than 64 MiB")
	}
	if !utf8.Valid(raw) || !surrogatesPaired(raw) {
		return Manifest{}, manifestErr("manifest", "not valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var m Manifest
	// The decoder's errors quote document text: none reaches the
	// caller.
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, manifestErr("manifest", "not one well-formed manifest object with known fields")
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, manifestErr("manifest", "trailing data after the object")
	}
	// Entry members have no valid zero value, so the entries' own
	// checks catch a missing one; the counts and the top level, where
	// zero is valid, are checked here.
	if path, ok := missingMember(reflect.TypeOf(Manifest{}), raw, ""); !ok {
		return Manifest{}, manifestErr(path, "required member missing or null")
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// Validate checks a manifest on its own: its versions and policies,
// every entry, the order and uniqueness of entries, the one-split rule
// for groups, the counts against the entries, and the digest.
func (m Manifest) Validate() error {
	if m.SchemaVersion != ManifestSchemaVersion {
		return manifestErr("schema_version", "unsupported version")
	}
	if m.WindowSchemaVersion != WindowSchemaVersion {
		return manifestErr("window_schema_version", "unsupported version")
	}
	if m.Contract != protocol.ModelContractAnalyzeAgentState {
		return manifestErr("contract", "not ANALYZE_AGENT_STATE")
	}
	if err := m.Policies.validate(); err != nil {
		var fe *FieldError
		if errors.As(err, &fe) {
			return manifestErr(fe.Field, fe.Reason)
		}
		return manifestErr("policies", "invalid")
	}
	if len(m.Entries) == 0 {
		return manifestErr("entries", "no windows")
	}
	if err := m.checkEntries(); err != nil {
		return err
	}
	if m.Counts != countEntries(m.Entries) {
		return manifestErr("counts", "do not match the entries")
	}
	if m.DatasetDigest != ManifestDigest(m) {
		return manifestErr("dataset_digest", "does not match the manifest")
	}
	return nil
}

// checkEntries checks each entry, their strict window id order, and
// that no group appears in two splits.
func (m Manifest) checkEntries() error {
	groups := map[protocol.Digest]Split{}
	for i, e := range m.Entries {
		if !e.WindowID.Valid() {
			return manifestErr("entries.window_id", "not a ULID")
		}
		if i > 0 && m.Entries[i-1].WindowID >= e.WindowID {
			return manifestErr("entries.window_id", "entries not sorted or repeated")
		}
		if !e.Split.Valid() {
			return manifestErr("entries.split", "unknown split")
		}
		if _, ok := e.Label.Phase(); !ok {
			return manifestErr("entries.label", "not one of A-G")
		}
		if !e.EnvironmentDomain.Valid() {
			return manifestErr("entries.environment_domain", "unknown domain")
		}
		for _, d := range []struct {
			digest protocol.Digest
			field  string
		}{
			{e.WindowDigest, "entries.window_digest"},
			{e.GroupDigest, "entries.group_digest"},
			{e.SourceDigest, "entries.source_digest"},
		} {
			if !d.digest.Valid() {
				return manifestErr(d.field, "not a sha256 digest")
			}
		}
		if prior, seen := groups[e.GroupDigest]; seen && prior != e.Split {
			return manifestErr("entries.group_digest", "one group in two splits")
		}
		groups[e.GroupDigest] = e.Split
	}
	return nil
}

// countEntries derives the counts from the entries.
func countEntries(entries []ManifestEntry) ManifestCounts {
	var c ManifestCounts
	for _, e := range entries {
		c.Windows++
		switch e.Split {
		case SplitTrain:
			c.BySplit.Train++
		case SplitValidation:
			c.BySplit.Validation++
		case SplitTest:
			c.BySplit.Test++
		}
		switch e.Label {
		case LabelA:
			c.ByLabel.A++
		case LabelB:
			c.ByLabel.B++
		case LabelC:
			c.ByLabel.C++
		case LabelD:
			c.ByLabel.D++
		case LabelE:
			c.ByLabel.E++
		case LabelF:
			c.ByLabel.F++
		case LabelG:
			c.ByLabel.G++
		}
		switch e.EnvironmentDomain {
		case EnvironmentDomainBankGeneric:
			c.ByDomain.BankGeneric++
		case EnvironmentDomainFleetManaged:
			c.ByDomain.FleetManaged++
		}
	}
	return c
}
