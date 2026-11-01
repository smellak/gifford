package dataset

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// Expected error fields shared by the window and manifest tables.
const (
	wantEntries       = "entries"
	wantEntryID       = "entries.window_id"
	wantPolicies      = "policies"
	wantEntryGroup    = "entries.group_digest"
	wantSchemaVersion = "schema_version"
	wantContract      = "contract"
	wantHumanPolicy   = "policies.human_input_policy_version"
	wantCounts        = "counts"
	wantDatasetDigest = "dataset_digest"
)

// windowAs is validWindow placed by id, split, label, domain and group.
// Every value is invented.
func windowAs(id protocol.ULID, split Split, label Label, domain EnvironmentDomain, group string) Window {
	w := validWindow()
	w.WindowID = id
	w.Split = SplitAssignment{Split: split, GroupDigest: testDigest(group)}
	w.Target.Label = label
	w.EnvironmentDomain = domain
	return w
}

// testWindows is a small synthetic dataset, deliberately out of order.
func testWindows() []Window {
	return []Window{
		windowAs("01ARZ3NDEKTSV4RRFFQ69G5FB2", SplitTest, LabelG, EnvironmentDomainFleetManaged, "group-3"),
		windowAs("01ARZ3NDEKTSV4RRFFQ69G5FAV", SplitTrain, LabelA, EnvironmentDomainBankGeneric, "group-1"),
		windowAs("01ARZ3NDEKTSV4RRFFQ69G5FB0", SplitTrain, LabelC, EnvironmentDomainBankGeneric, "group-1"),
		windowAs("01ARZ3NDEKTSV4RRFFQ69G5FB1", SplitValidation, LabelC, EnvironmentDomainFleetManaged, "group-2"),
	}
}

func TestBuildManifest(t *testing.T) {
	t.Parallel()
	m, err := BuildManifest(testWindows())
	require.NoError(t, err)
	require.NoError(t, m.Validate())
	require.Equal(t, ManifestCounts{
		Windows:  4,
		BySplit:  SplitCounts{Train: 2, Validation: 1, Test: 1},
		ByLabel:  LabelCounts{A: 1, C: 2, G: 1},
		ByDomain: DomainCounts{BankGeneric: 2, FleetManaged: 2},
		BySplitLabelHarness: []CountCell{
			{Split: SplitTrain, Label: LabelA, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitTrain, Label: LabelC, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitValidation, Label: LabelC, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitTest, Label: LabelG, Harness: protocol.HarnessClaudeCode, N: 1},
		},
	}, m.Counts)
	ids := make([]string, 0, len(m.Entries))
	for _, e := range m.Entries {
		ids = append(ids, string(e.WindowID))
	}
	require.True(t, sort.StringsAreSorted(ids), "entries are sorted by window id")
	require.Equal(t, validWindow().Policies, m.Policies)

	// The manifest depends on the windows, not on their order.
	reversed := testWindows()
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	again, err := BuildManifest(reversed)
	require.NoError(t, err)
	require.Equal(t, m, again)

	// Any byte of a window moves its digest and the dataset digest.
	edited := testWindows()
	setLines(&edited[1], []string{"step 4 of 7: compiling"})
	changed, err := BuildManifest(edited)
	require.NoError(t, err)
	require.NotEqual(t, m.DatasetDigest, changed.DatasetDigest)

	// Each entry carries its window's harness version, under the digest.
	for _, e := range m.Entries {
		require.Equal(t, validWindow().Provenance.HarnessVersion, e.HarnessVersion)
	}
	versioned := testWindows()
	versioned[1].Provenance.HarnessVersion = "2.1.0"
	moved, err := BuildManifest(versioned)
	require.NoError(t, err)
	require.Contains(t, []string{moved.Entries[0].HarnessVersion, moved.Entries[1].HarnessVersion}, "2.1.0")
	require.NotEqual(t, m.DatasetDigest, moved.DatasetDigest)
	bad := m
	bad.Entries = append([]ManifestEntry(nil), m.Entries...)
	bad.Entries[0].HarnessVersion = "1\x07"
	bad.DatasetDigest = ManifestDigest(bad)
	var me *ManifestError
	require.ErrorAs(t, bad.Validate(), &me)
	require.Equal(t, "entries.harness_version", me.Field)
}

func TestBuildManifestRefuses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		windows func() []Window
		field   string
	}{
		{"no windows", func() []Window { return nil }, wantEntries},
		{"repeated window id", func() []Window {
			ws := testWindows()
			ws[0].WindowID = ws[1].WindowID
			return ws
		}, wantEntryID},
		{"mixed policy versions", func() []Window {
			ws := testWindows()
			ws[2].Policies.DomainMaskPolicyVersion = "2"
			return ws
		}, wantPolicies},
		{"group in two splits", func() []Window {
			ws := testWindows()
			ws[2].Split.Split = SplitTest
			return ws
		}, wantEntryGroup},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := BuildManifest(tc.windows())
			require.ErrorIs(t, err, ErrInvalidManifest)
			var me *ManifestError
			require.ErrorAs(t, err, &me)
			require.Equal(t, tc.field, me.Field, "reason: %s", me.Reason)
		})
	}
	ws := testWindows()
	ws[3].Screen.Redaction.Applied = false
	_, err := BuildManifest(ws)
	require.ErrorIs(t, err, ErrInvalidManifest, "an invalid window refuses the manifest")
	require.ErrorIs(t, err, ErrInvalidWindow, "and names the window failure")
}

func TestManifestValidateRefuses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*Manifest)
		field  string
	}{
		{"schema version", func(m *Manifest) { m.SchemaVersion = 2 }, wantSchemaVersion},
		{"window schema version", func(m *Manifest) { m.WindowSchemaVersion = 2 }, "window_schema_version"},
		{"other contract", func(m *Manifest) { m.Contract = protocol.ModelContractTriageMessage }, wantContract},
		{"missing policy", func(m *Manifest) { m.Policies.HumanInputPolicyVersion = "" }, wantHumanPolicy},
		{"no entries", func(m *Manifest) { m.Entries = nil }, wantEntries},
		{"unsorted entries", func(m *Manifest) { m.Entries[0], m.Entries[1] = m.Entries[1], m.Entries[0] }, wantEntryID},
		{"bad window id", func(m *Manifest) { m.Entries[0].WindowID = "w-1" }, wantEntryID},
		{"unknown split", func(m *Manifest) { m.Entries[0].Split = "dev" }, "entries.split"},
		{"unknown label", func(m *Manifest) { m.Entries[0].Label = "H" }, "entries.label"},
		{"unknown domain", func(m *Manifest) { m.Entries[0].EnvironmentDomain = "bank" }, "entries.environment_domain"},
		{"bad window digest", func(m *Manifest) { m.Entries[0].WindowDigest = "" }, "entries.window_digest"},
		{"bad group digest", func(m *Manifest) { m.Entries[0].GroupDigest = "sha256:x" }, wantEntryGroup},
		{"bad source digest", func(m *Manifest) { m.Entries[0].SourceDigest = "" }, "entries.source_digest"},
		{"group in two splits", func(m *Manifest) { m.Entries[1].Split = SplitTest }, wantEntryGroup},
		{"counts off", func(m *Manifest) { m.Counts.ByLabel.A++ }, wantCounts},
		{"window total off", func(m *Manifest) { m.Counts.Windows = 5 }, wantCounts},
		{"digest off", func(m *Manifest) { m.DatasetDigest = testDigest("other") }, wantDatasetDigest},
		{"entry edited under the digest", func(m *Manifest) { m.Entries[0].SourceDigest = testDigest("other source") }, wantDatasetDigest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := BuildManifest(testWindows())
			require.NoError(t, err)
			tc.mutate(&m)
			err = m.Validate()
			require.ErrorIs(t, err, ErrInvalidManifest)
			var me *ManifestError
			require.ErrorAs(t, err, &me)
			require.Equal(t, tc.field, me.Field, "reason: %s", me.Reason)
		})
	}
}

func TestManifestDigestCanonical(t *testing.T) {
	t.Parallel()
	m := Manifest{
		SchemaVersion:       ManifestSchemaVersion,
		Contract:            protocol.ModelContractAnalyzeAgentState,
		WindowSchemaVersion: WindowSchemaVersion,
		Policies:            Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
		Entries: []ManifestEntry{{
			WindowID:          "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Split:             SplitTrain,
			Label:             LabelA,
			EnvironmentDomain: EnvironmentDomainBankGeneric,
			WindowDigest:      "sha256:" + protocol.Digest(strings.Repeat("1", 64)),
			GroupDigest:       "sha256:" + protocol.Digest(strings.Repeat("2", 64)),
			SourceDigest:      "sha256:" + protocol.Digest(strings.Repeat("3", 64)),
			Harness:           protocol.HarnessCodex,
			HarnessVersion:    "1.0",
			SourceQuality:     protocol.SourceQualityPTYScreen,
			LabelSource:       LabelSourceHook,
		}},
	}
	// A fixed vector pins the v1 encoding.
	require.Equal(t, protocol.Digest("sha256:3470666c8459bd26258cc58bb86aa2cf233f545745067f646cf841a22abdacef"), ManifestDigest(m))
	// The harness version is covered.
	other := m
	other.Entries = []ManifestEntry{m.Entries[0]}
	other.Entries[0].HarnessVersion = "1.1"
	require.NotEqual(t, ManifestDigest(m), ManifestDigest(other))
	// Moving a value between policy fields changes the digest.
	moved := m
	moved.Policies = Policies{RedactionPolicyVersion: "11", HumanInputPolicyVersion: "", DomainMaskPolicyVersion: "1"}
	require.NotEqual(t, ManifestDigest(m), ManifestDigest(moved))
}

func TestParseManifest(t *testing.T) {
	t.Parallel()
	m, err := BuildManifest(testWindows())
	require.NoError(t, err)
	enc, err := json.Marshal(m)
	require.NoError(t, err)
	got, err := ParseManifest(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, m, got)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(enc, &doc))
	counts := doc["counts"].(map[string]any)
	byLabel := counts["by_label"].(map[string]any)
	delete(byLabel, "B")
	missing, err := json.Marshal(doc)
	require.NoError(t, err)
	_, err = ParseManifest(bytes.NewReader(missing))
	var me *ManifestError
	require.ErrorAs(t, err, &me)
	require.Equal(t, "counts.by_label.B", me.Field, "a zero count is stated, never omitted")
	_, err = ParseManifest(bytes.NewReader(bytes.Replace(enc, []byte(`"scripted_input"`), []byte(`"screen_form":null,"scripted_input"`), 1)))
	require.ErrorAs(t, err, &me)
	require.Equal(t, "screen_form", me.Field, "an optional member is left out, never null")

	inputs := map[string][]byte{
		"unknown field": bytes.Replace(enc, []byte(`"schema_version"`), []byte(`"sentinel_field_q7":1,"schema_version"`), 1),
		"malformed":     []byte(`{"schema_version": sentinel_token_q7}`),
		"null entries":  bytes.Replace(enc, enc[bytes.Index(enc, []byte(`"entries":`)):bytes.Index(enc, []byte(`,"dataset_digest"`))], []byte(`"entries":null`), 1),
		"invalid UTF-8": bytes.Replace(enc, []byte(`"1"`), []byte("\"\xff\""), 1),
		"trailing data": append(append([]byte{}, enc...), []byte(`{}`)...),
	}
	for name, in := range inputs {
		_, err := ParseManifest(bytes.NewReader(in))
		require.ErrorIs(t, err, ErrInvalidManifest, name)
		require.NotContains(t, err.Error(), "sentinel", name)
	}
}

func TestManifestHasNoContentFields(t *testing.T) {
	t.Parallel()
	var got []string
	windowFields(reflect.TypeOf(Manifest{}), "", &got)
	windowFields(reflect.TypeOf(ManifestEntry{}), "entries[].", &got)
	sort.Strings(got)
	require.Equal(t, []string{
		"activity_version", "align",
		"contract",
		"counts", "counts.by_domain", "counts.by_domain.bank_generic", "counts.by_domain.fleet_managed",
		"counts.by_label", "counts.by_label.A", "counts.by_label.B", "counts.by_label.C", "counts.by_label.D",
		"counts.by_label.E", "counts.by_label.F", "counts.by_label.G",
		"counts.by_split", "counts.by_split.test", "counts.by_split.train", "counts.by_split.validation",
		"counts.by_split_label_harness",
		"counts.windows",
		"dataset_digest", "entries",
		"entries[].environment_domain", "entries[].group_digest", "entries[].harness", "entries[].harness_version", "entries[].label",
		"entries[].label_source", "entries[].observation_anchor_digest", "entries[].observation_episode_digest", "entries[].observation_form", "entries[].observation_policy_version",
		"entries[].source_digest", "entries[].source_quality",
		"entries[].split", "entries[].window_digest", "entries[].window_id",
		"observation_counts",
		"policies", "policies.domain_mask_policy_version", "policies.human_input_policy_version", "policies.redaction_policy_version",
		"readiness",
		"schema_version", "screen_form", "scripted_input", "split_file", "window_schema_version",
	}, got, "a manifest field was added or renamed: review it against the no-content rule")
}

func TestManifestSeparatesActivityCalibrations(t *testing.T) {
	t.Parallel()
	thresholds := DefaultReadinessThresholds()
	legacy, err := buildManifest(testWindows(), thresholds, nil, nil, false, "")
	require.NoError(t, err)
	require.Empty(t, legacy.ActivityVersion, "an older derived dataset retains its encoding")

	effectiveWindows := testWindows()
	for i := range effectiveWindows {
		effectiveWindows[i].Provenance.ActivityVersion = outputactivity.Version
	}
	effective, err := buildManifest(effectiveWindows, thresholds, nil, nil, false, "")
	require.NoError(t, err)
	require.Equal(t, outputactivity.Version, effective.ActivityVersion)
	require.NotEqual(t, legacy.DatasetDigest, effective.DatasetDigest)

	effectiveWindows[1].Provenance.ActivityVersion = outputactivity.ReconstructedVersion
	_, err = buildManifest(effectiveWindows, thresholds, nil, nil, false, "")
	require.ErrorIs(t, err, ErrInvalidManifest)
	require.ErrorContains(t, err, "another activity input version")
}

// TestManifestAlign pins how align labeled the windows: the dataset
// digest covers the margin and the config digest, the member decodes
// back, and a manifest without it keeps the digest it always had.
func TestManifestAlign(t *testing.T) {
	t.Parallel()
	th := DefaultReadinessThresholds()
	plain, err := buildManifest(testWindows(), th, nil, nil, false, "")
	require.NoError(t, err)
	require.Nil(t, plain.Align)
	config := testDigest("align config")
	withConfig := &AlignPolicy{MarginMS: 600, ConfigDigest: &config}
	m, err := buildManifest(testWindows(), th, nil, withConfig, false, "")
	require.NoError(t, err)
	require.NoError(t, m.Validate())
	require.Equal(t, withConfig, m.Align)
	defaults, err := buildManifest(testWindows(), th, nil, &AlignPolicy{MarginMS: 600}, false, "")
	require.NoError(t, err)
	other, err := buildManifest(testWindows(), th, nil, &AlignPolicy{MarginMS: 300, ConfigDigest: &config}, false, "")
	require.NoError(t, err)
	digests := map[protocol.Digest]bool{plain.DatasetDigest: true, m.DatasetDigest: true, defaults.DatasetDigest: true, other.DatasetDigest: true}
	require.Len(t, digests, 4, "the digest covers the margin and the config digest")

	for _, built := range []Manifest{m, defaults} {
		enc, err := json.Marshal(built)
		require.NoError(t, err)
		got, err := ParseManifest(bytes.NewReader(enc))
		require.NoError(t, err)
		require.Equal(t, built, got)
	}

	edited := m
	edited.Align = &AlignPolicy{MarginMS: 700, ConfigDigest: &config}
	require.ErrorIs(t, edited.Validate(), ErrInvalidManifest, "a margin edited under the digest")
	bad := protocol.Digest("sha256:x")
	var me *ManifestError
	for field, a := range map[string]*AlignPolicy{
		"align.margin_ms":     {MarginMS: -1},
		"align.config_digest": {MarginMS: 600, ConfigDigest: &bad},
	} {
		_, err := buildManifest(testWindows(), th, nil, a, false, "")
		require.ErrorAs(t, err, &me)
		require.Equal(t, field, me.Field)
		broken := m
		broken.Align = a
		require.ErrorAs(t, broken.Validate(), &me)
		require.Equal(t, field, me.Field)
	}
	_, err = buildManifest(testWindows(), th, nil, &AlignPolicy{MarginMS: maxAlignMarginMS + 1}, false, "")
	require.ErrorAs(t, err, &me)
	require.Equal(t, "align.margin_ms", me.Field)
}

// TestManifestScriptedInput: the dataset digest covers scripted input,
// which decodes back, and a manifest without it keeps its digest.
func TestManifestScriptedInput(t *testing.T) {
	t.Parallel()
	th := DefaultReadinessThresholds()
	plain, err := BuildManifest(testWindows())
	require.NoError(t, err)
	m, err := buildManifest(testWindows(), th, nil, nil, true, "")
	require.NoError(t, err)
	require.True(t, m.ScriptedInput)
	require.NoError(t, m.Validate())
	require.NotEqual(t, plain.DatasetDigest, m.DatasetDigest)
	enc, err := json.Marshal(m)
	require.NoError(t, err)
	got, err := ParseManifest(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, m, got)
	edited := m
	edited.ScriptedInput = false
	require.ErrorIs(t, edited.Validate(), ErrInvalidManifest, "scripted input edited under the digest")
}

// TestManifestScreenForm: the dataset digest covers the live screen
// form, which decodes back; a manifest without it keeps its digest and
// its encoding, and no other form is accepted.
func TestManifestScreenForm(t *testing.T) {
	t.Parallel()
	th := DefaultReadinessThresholds()
	plain, err := BuildManifest(testWindows())
	require.NoError(t, err)
	enc, err := json.Marshal(plain)
	require.NoError(t, err)
	require.NotContains(t, string(enc), "screen_form", "a manifest without the form encodes as before")
	m, err := buildManifest(testWindows(), th, nil, nil, false, ScreenFormLive)
	require.NoError(t, err)
	require.Equal(t, "live-input-v1", m.ScreenForm)
	require.NoError(t, m.Validate())
	require.NotEqual(t, plain.DatasetDigest, m.DatasetDigest)
	enc, err = json.Marshal(m)
	require.NoError(t, err)
	got, err := ParseManifest(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, m, got)

	edited := m
	edited.ScreenForm = ""
	require.ErrorIs(t, edited.Validate(), ErrInvalidManifest, "the form edited under the digest")
	_, err = buildManifest(testWindows(), th, nil, nil, false, "live-input-v3")
	require.ErrorIs(t, err, ErrInvalidManifest)
	unknown := m
	unknown.ScreenForm = "live-input-v3"
	unknown.DatasetDigest = ManifestDigest(unknown)
	var me *ManifestError
	require.ErrorAs(t, unknown.Validate(), &me)
	require.Equal(t, "screen_form", me.Field)
}

func liveV2Windows() []Window {
	windows := testWindows()
	for i := range windows {
		windows[i].Screen.ObservationForm = ObservationFormEmptyInputV1
	}
	permission := &windows[0]
	permission.Target = Target{Label: LabelD, Source: LabelSourceHook}
	permission.Input.ScreenLines = []string{session.PermissionDialogMarker}
	permission.Screen.Cols, permission.Screen.Rows = int64(len(session.PermissionDialogMarker)), 1
	permission.Screen.Digest = ScreenDigest(permission.Screen.Cols, permission.Screen.Rows, permission.Input.ScreenLines)
	permission.Screen.ObservationForm = ObservationFormPermissionDialogV1
	permission.Screen.ObservationPolicyVersion = session.PermissionDialogPolicyVersion
	permission.Screen.ObservationEpisodeDigest = testDigest("permission episode")
	permission.Screen.ObservationAnchorDigest = testDigest("permission anchor")
	return windows
}

func TestManifestLiveV2CountsAnchorsAndEpisodes(t *testing.T) {
	t.Parallel()
	windows := liveV2Windows()
	m, err := buildManifest(windows, DefaultReadinessThresholds(), nil, nil, false, ScreenFormLiveV2)
	require.NoError(t, err)
	require.Equal(t, &ObservationCounts{PermissionAnchors: 1, PermissionEpisodes: 1}, m.ObservationCounts)
	require.NoError(t, m.Validate())
	encoded, err := json.Marshal(m)
	require.NoError(t, err)
	parsed, err := ParseManifest(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, m, parsed)

	duplicate := liveV2Windows()
	duplicate[1].Target = Target{Label: LabelD, Source: LabelSourceScript}
	duplicate[1].Input.ScreenLines = []string{session.PermissionDialogMarker}
	duplicate[1].Screen = duplicate[0].Screen
	duplicate[1].Screen.ObservationAnchorDigest = duplicate[0].Screen.ObservationAnchorDigest
	_, err = buildManifest(duplicate, DefaultReadinessThresholds(), nil, nil, false, ScreenFormLiveV2)
	require.ErrorIs(t, err, ErrInvalidManifest, "the manifest rejects a repeated policy anchor defensively")

	missingForm := liveV2Windows()
	missingForm[0].Input.ScreenLines = []string{"ordinary closed input"}
	missingForm[0].Screen.Cols, missingForm[0].Screen.Rows = 80, 24
	missingForm[0].Screen.Digest = ScreenDigest(80, 24, missingForm[0].Input.ScreenLines)
	missingForm[0].Screen.ObservationForm = ObservationFormEmptyInputV1
	missingForm[0].Screen.ObservationPolicyVersion = ""
	missingForm[0].Screen.ObservationEpisodeDigest = ""
	missingForm[0].Screen.ObservationAnchorDigest = ""
	_, err = buildManifest(missingForm, DefaultReadinessThresholds(), nil, nil, false, ScreenFormLiveV2)
	require.ErrorIs(t, err, ErrInvalidManifest, "D cannot enter live-input-v2 through the empty-input form")
}

func TestManifestLiveV2KeepsSyntheticTrainSeparateFromObservationEvidence(t *testing.T) {
	t.Parallel()
	windows := liveV2Windows()
	synthetic := validSyntheticTrainingWindow()
	synthetic.WindowID = "01JB9999999999999999999998"
	synthetic.Split.GroupDigest = testDigest("synthetic live v2 group")
	windows = append(windows, synthetic)

	m, err := buildManifest(windows, DefaultReadinessThresholds(), nil, nil, false, ScreenFormLiveV2)
	require.NoError(t, err)
	require.Equal(t, &ObservationCounts{PermissionAnchors: 1, PermissionEpisodes: 1}, m.ObservationCounts,
		"synthetic train windows do not manufacture captured observation counts")
	require.NoError(t, m.Validate())

	wrongSplit := slices.Clone(windows)
	wrongSplit[len(wrongSplit)-1].Split.Split = SplitValidation
	_, err = buildManifest(wrongSplit, DefaultReadinessThresholds(), nil, nil, false, ScreenFormLiveV2)
	require.ErrorIs(t, err, ErrInvalidManifest, "the live-v2 exception is train-only")
}

// TestManifestSplitFile pins a split file's digest in the manifest: the
// dataset digest covers it, it decodes back, and it may place entries in
// train. A manifest without one keeps the digest it always had.
func TestManifestSplitFile(t *testing.T) {
	t.Parallel()
	halves := testWindows()
	halves[1].Split.Split, halves[2].Split.Split = SplitValidation, SplitValidation
	th := DefaultReadinessThresholds()
	plain, err := buildManifest(halves, th, nil, nil, false, "")
	require.NoError(t, err)
	require.Nil(t, plain.SplitFile)
	file := testDigest("split file")
	m, err := buildManifest(halves, th, &file, nil, false, "")
	require.NoError(t, err)
	require.Equal(t, &file, m.SplitFile)
	require.NoError(t, m.Validate())
	require.NotEqual(t, plain.DatasetDigest, m.DatasetDigest, "the digest covers the split file")
	other := testDigest("other split file")
	edited := m
	edited.SplitFile = &other
	require.ErrorIs(t, edited.Validate(), ErrInvalidManifest, "a split file edited under the digest")

	enc, err := json.Marshal(m)
	require.NoError(t, err)
	got, err := ParseManifest(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, m, got)
	// The member is required: null is split-v1, absent is refused.
	var doc map[string]any
	require.NoError(t, json.Unmarshal(enc, &doc))
	delete(doc, "split_file")
	missing, err := json.Marshal(doc)
	require.NoError(t, err)
	_, err = ParseManifest(bytes.NewReader(missing))
	var me *ManifestError
	require.ErrorAs(t, err, &me)
	require.Equal(t, "split_file", me.Field)

	withTrain, err := buildManifest(testWindows(), th, &file, nil, false, "")
	require.NoError(t, err, "a split file may place entries in train")
	require.Positive(t, withTrain.Counts.BySplit.Train)
	require.NoError(t, withTrain.Validate())
	bad := protocol.Digest("sha256:x")
	_, err = buildManifest(halves, th, &bad, nil, false, "")
	require.ErrorAs(t, err, &me)
	require.Equal(t, "split_file", me.Field)
	m.SplitFile = &bad
	require.ErrorAs(t, m.Validate(), &me)
	require.Equal(t, "split_file", me.Field)
}
