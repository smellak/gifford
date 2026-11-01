package eval_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

const developmentV2Root = "testdata/development-v2"

type developmentCatalog struct {
	SchemaVersion  int                  `json:"schema_version"`
	Name           string               `json:"name"`
	Purpose        string               `json:"purpose"`
	AllowedUses    []string             `json:"allowed_uses"`
	ProhibitedUses []string             `json:"prohibited_uses"`
	SplitSemantics string               `json:"split_semantics"`
	BaseCommit     string               `json:"base_commit"`
	DatasetDigest  protocol.Digest      `json:"dataset_digest"`
	WindowCount    int                  `json:"window_count"`
	Provenance     []developmentSource  `json:"provenance"`
	Generator      string               `json:"generator"`
	Harnesses      []developmentHarness `json:"harnesses"`
	Families       []developmentFamily  `json:"families"`
}

type developmentSource struct {
	Path     string   `json:"path"`
	SHA256   string   `json:"sha256"`
	Sections []string `json:"sections"`
}

type developmentHarness struct {
	Name protocol.Harness `json:"name"`
}

type developmentFamily struct {
	Name      string               `json:"name"`
	Label     dataset.Label        `json:"label"`
	State     protocol.AgentPhase  `json:"state"`
	Harnesses []protocol.Harness   `json:"harnesses"`
	Variants  []developmentVariant `json:"variants"`
	Windows   []developmentWindow  `json:"windows"`
}

type developmentVariant struct {
	Name string `json:"name"`
}

type developmentWindow struct {
	Harness  protocol.Harness `json:"harness"`
	Variant  string           `json:"variant"`
	WindowID protocol.ULID    `json:"window_id"`
}

func TestDevelopmentV2Corpus(t *testing.T) {
	t.Parallel()

	manifestRaw, err := os.ReadFile(filepath.Join(developmentV2Root, dataset.ManifestFile))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(manifestRaw))
	require.NoError(t, err)
	require.Nil(t, manifest.Readiness, "development data must not claim preparation readiness")
	require.Empty(t, manifest.ScreenForm, "synthetic model inputs are not live observation evidence")
	require.Equal(t, int64(120), manifest.Counts.Windows)
	require.Equal(t, dataset.SplitCounts{Validation: 120}, manifest.Counts.BySplit)
	require.Equal(t, dataset.LabelCounts{A: 36, B: 12, C: 12, D: 24, E: 24, F: 12}, manifest.Counts.ByLabel)

	catalogRaw, err := os.ReadFile(filepath.Join(developmentV2Root, "families.json"))
	require.NoError(t, err)
	var catalog developmentCatalog
	require.NoError(t, json.Unmarshal(catalogRaw, &catalog))
	require.Equal(t, 1, catalog.SchemaVersion)
	require.Equal(t, "adversarial-development-v2", catalog.Name)
	require.Equal(t, "checkpoint_selection_only", catalog.Purpose)
	require.Equal(t, manifest.DatasetDigest, catalog.DatasetDigest)
	require.Equal(t, len(manifest.Entries), catalog.WindowCount)
	require.Contains(t, catalog.SplitSemantics, "not calibration validation")
	for _, use := range []string{"training", "temperature_fitting", "safe_threshold_fitting", "readiness_gate", "adapter_gate", "fixed_safety_regression"} {
		require.Contains(t, catalog.ProhibitedUses, use)
	}
	require.Contains(t, catalog.AllowedUses, "checkpoint_ranking")
	require.Len(t, catalog.Provenance, 2)
	for _, source := range catalog.Provenance {
		require.Contains(t, []string{"docs/ML_CONTRACT.md", "internal/dataset/WINDOW.md"}, source.Path)
		require.Len(t, source.SHA256, sha256.Size*2)
		require.NotEmpty(t, source.Sections)
	}

	wantHarnesses := []protocol.Harness{
		protocol.HarnessClaudeCode,
		protocol.HarnessMuseCode,
		protocol.HarnessOpencode,
		protocol.HarnessAntigravity,
		protocol.HarnessCodex,
		protocol.HarnessGeneric,
	}
	require.Len(t, catalog.Harnesses, len(wantHarnesses))
	for i, harness := range catalog.Harnesses {
		require.Equal(t, wantHarnesses[i], harness.Name)
	}

	entryByID := make(map[protocol.ULID]dataset.ManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entryByID[entry.WindowID] = entry
		require.Equal(t, dataset.SplitValidation, entry.Split)
		require.Equal(t, protocol.SourceQualitySynthetic, entry.SourceQuality)
		require.Equal(t, dataset.LabelSourceSynthetic, entry.LabelSource)
	}

	require.Len(t, catalog.Families, 10)
	seenIDs := make(map[protocol.ULID]bool, len(manifest.Entries))
	labelWindows := make(map[dataset.Label]int)
	canonical := 0
	activeFacts := map[dataset.Label]struct{ quiet, recent bool }{}
	for _, family := range catalog.Families {
		phase, ok := family.Label.Phase()
		require.True(t, ok, family.Name)
		require.Equal(t, phase, family.State, family.Name)
		require.Equal(t, wantHarnesses, family.Harnesses, family.Name)
		require.Equal(t, []developmentVariant{{Name: "reference"}, {Name: "alternate"}}, family.Variants, family.Name)
		require.Len(t, family.Windows, len(wantHarnesses)*2, family.Name)

		pairs := map[string]bool{}
		for _, listed := range family.Windows {
			require.False(t, seenIDs[listed.WindowID], listed.WindowID)
			seenIDs[listed.WindowID] = true
			pair := string(listed.Harness) + "/" + listed.Variant
			require.False(t, pairs[pair], pair)
			pairs[pair] = true

			entry, ok := entryByID[listed.WindowID]
			require.True(t, ok, listed.WindowID)
			require.Equal(t, family.Label, entry.Label)
			require.Equal(t, listed.Harness, entry.Harness)

			raw, err := os.ReadFile(filepath.Join(developmentV2Root, dataset.WindowsDir, dataset.WindowFile(string(listed.WindowID))))
			require.NoError(t, err)
			window, err := dataset.ParseWindow(bytes.NewReader(raw))
			require.NoError(t, err, listed.WindowID)
			digest, err := dataset.WindowDigest(window)
			require.NoError(t, err)
			require.Equal(t, entry.WindowDigest, digest)
			require.Equal(t, family.Label, window.Target.Label)
			require.Equal(t, dataset.LabelSourceSynthetic, window.Target.Source)
			require.Equal(t, protocol.SourceQualitySynthetic, window.Provenance.SourceQuality)
			require.Equal(t, dataset.SplitValidation, window.Split.Split)
			require.Empty(t, window.Screen.ObservationForm)
			labelWindows[family.Label]++

			if family.Name == "permission-canonical-facts" {
				canonical++
				require.Equal(t, int64(19), window.Screen.Cols)
				require.Equal(t, int64(1), window.Screen.Rows)
				require.Equal(t, []string{"[permission-dialog]"}, window.Input.ScreenLines)
			}
			if family.Label == dataset.LabelB || family.Label == dataset.LabelC {
				facts := activeFacts[family.Label]
				if window.Input.OutputBytes.Last10s == 0 {
					facts.quiet = true
				}
				if window.Input.OutputBytes.Last500ms > 0 {
					facts.recent = true
				}
				activeFacts[family.Label] = facts
			}
		}
	}
	require.Len(t, seenIDs, len(manifest.Entries))
	require.Equal(t, map[dataset.Label]int{
		dataset.LabelA: 36,
		dataset.LabelB: 12,
		dataset.LabelC: 12,
		dataset.LabelD: 24,
		dataset.LabelE: 24,
		dataset.LabelF: 12,
	}, labelWindows)
	require.Equal(t, 12, canonical)
	require.Equal(t, struct{ quiet, recent bool }{true, true}, activeFacts[dataset.LabelB])
	require.Equal(t, struct{ quiet, recent bool }{true, true}, activeFacts[dataset.LabelC])

	verifyDevelopmentChecksums(t)
}

func verifyDevelopmentChecksums(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(developmentV2Root, "SHA256SUMS"))
	require.NoError(t, err)
	declared := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		require.Len(t, fields, 2, line)
		_, err := hex.DecodeString(fields[0])
		require.NoError(t, err, line)
		require.Len(t, fields[0], sha256.Size*2)
		require.False(t, strings.HasPrefix(fields[1], "/"), fields[1])
		declared[fields[1]] = fields[0]

		content, err := os.ReadFile(filepath.Join(developmentV2Root, filepath.FromSlash(fields[1])))
		require.NoError(t, err, fields[1])
		sum := sha256.Sum256(content)
		require.Equal(t, fields[0], hex.EncodeToString(sum[:]), fields[1])
	}

	var files []string
	require.NoError(t, filepath.WalkDir(developmentV2Root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "SHA256SUMS" {
			return nil
		}
		rel, err := filepath.Rel(developmentV2Root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	}))
	slices.Sort(files)
	declaredNames := make([]string, 0, len(declared))
	for name := range declared {
		declaredNames = append(declaredNames, name)
	}
	slices.Sort(declaredNames)
	require.Equal(t, files, declaredNames)
}
