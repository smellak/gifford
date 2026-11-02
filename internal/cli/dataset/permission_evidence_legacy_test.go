package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

const (
	flagPermissionEvidenceLegacy = "--permission-evidence-legacy"
	legacySessionA               = "sess-legacy-a"
	legacySessionD               = "sess-legacy-d"
	legacySessionEvidence        = "sess-legacy-evidence"
)

// permissionEvidenceLegacyFixture adds two OpenCode captures at a configured
// permission source version. Neither has permission-dialog-evidence-v1: A is
// eligible through empty_input_v1, while D has no canonical form and must be
// withheld. The split contains every capture in the data directory.
func permissionEvidenceLegacyFixture(t *testing.T, evidenceCapture bool) (env, string) {
	t.Helper()
	e := setup(t)
	writeSessionAs(t, e.dataDir, legacySessionA, protocol.HarnessOpencode, "1.18.29", []chunk{
		{ms: 100, output: openCodeBox},
		{ms: 200, draft: draftClosed},
		{ms: 2100, output: openCodeBox},
	})
	writeSessionAs(t, e.dataDir, legacySessionD, protocol.HarnessOpencode, "1.18.29", []chunk{
		{ms: 100, output: openCodeBox},
		{ms: 200, draft: draftClosed},
		{ms: 2100, output: openCodeBox},
	})
	labels := map[protocol.OpaqueID]dataset.Label{
		legacySessionA: dataset.LabelA,
		legacySessionD: dataset.LabelD,
	}
	if evidenceCapture {
		writeSessionAsWithPermissionEvidence(t, e.dataDir, legacySessionEvidence,
			protocol.HarnessOpencode, "1.18.29", []chunk{
				{ms: 100, output: openCodeBox},
				{ms: 200, draft: draftClosed},
				{ms: 2100, output: openCodeBox},
			})
		labels[legacySessionEvidence] = dataset.LabelA
	}
	annotateSessions(t, e, labels)

	require.NoError(t, os.WriteFile(e.alignCfg, []byte(
		"margin_ms: 600\nhook_harnesses: []\nweak:\n  silence_seconds: 3600\n",
	), 0o600))
	writeConfig(t, e,
		"screen_lines: "+screenLines,
		"mask: "+e.mask,
		"permission_dialog_sources:\n    - harness: opencode\n      harness_version: 1.18.29",
	)
	return e, writeLegacySplit(t, e, "")
}

func writeLegacySplit(t *testing.T, e env, omit string) string {
	t.Helper()
	ids, err := session.ListSessions(e.dataDir)
	require.NoError(t, err)
	var body strings.Builder
	for _, id := range ids {
		if id == omit {
			continue
		}
		fmt.Fprintf(&body, `{"session":%q,"split":"validation"}`+"\n", id)
	}
	path := filepath.Join(e.work, "legacy-split-"+strings.ReplaceAll(omit, "/", "-")+".jsonl")
	require.NoError(t, os.WriteFile(path, []byte(body.String()), 0o600))
	return path
}

func writeLegacyList(t *testing.T, e env, name string, sessions ...string) (string, []byte) {
	t.Helper()
	var body strings.Builder
	for _, id := range sessions {
		fmt.Fprintf(&body, `{"session_id":%q}`+"\n", id)
	}
	raw := []byte(body.String())
	path := filepath.Join(e.work, name+".jsonl")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path, raw
}

func TestBuildPermissionEvidenceLegacyBindsOneMixedV2Manifest(t *testing.T) {
	e, split := permissionEvidenceLegacyFixture(t, false)
	legacy, rawList := writeLegacyList(t, e, "permission-evidence-legacy",
		legacySessionA, legacySessionD)
	out := filepath.Join(e.work, "mixed-v2")

	stdout, err := buildCmd(t,
		flagOut, out,
		flagAlign, e.alignCfg,
		flagSplitFile, split,
		flagLiveFormV2,
		flagPermissionEvidenceLegacy, legacy,
	)
	require.NoError(t, err)

	rawManifest, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(rawManifest))
	require.NoError(t, err)
	var seal struct {
		PermissionEvidenceLegacy *struct {
			SHA256   protocol.Digest `json:"sha256"`
			Sessions int64           `json:"sessions"`
		} `json:"permission_evidence_legacy"`
	}
	require.NoError(t, json.Unmarshal(rawManifest, &seal))
	require.NotNil(t, seal.PermissionEvidenceLegacy)
	wantDigest := protocol.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(rawList)))
	require.Equal(t, wantDigest, seal.PermissionEvidenceLegacy.SHA256)
	require.Equal(t, int64(2), seal.PermissionEvidenceLegacy.Sessions)

	legacyAGroup := replay.GroupDigest(legacySessionA)
	legacyDGroup := replay.GroupDigest(legacySessionD)
	foundA := false
	for _, entry := range manifest.Entries {
		switch entry.GroupDigest {
		case legacyAGroup:
			foundA = true
			require.Equal(t, dataset.LabelA, entry.Label)
			require.Equal(t, dataset.ObservationFormEmptyInputV1, entry.ObservationForm)
		case legacyDGroup:
			t.Fatalf("legacy D entered the manifest without a canonical permission form: %s", entry.WindowID)
		}
	}
	require.True(t, foundA, "the legacy cohort contributes ordinary empty-input windows")
	require.Contains(t, stdout, "live form opencode D: kept 0")
	require.Contains(t, stdout, "permission_dialog_form_missing")
}

func TestBuildPermissionEvidenceLegacyRejectsEvidenceBearingSession(t *testing.T) {
	e, split := permissionEvidenceLegacyFixture(t, true)
	legacy, _ := writeLegacyList(t, e, "permission-evidence-legacy",
		legacySessionA, legacySessionD, legacySessionEvidence)
	out := filepath.Join(e.work, "listed-evidence")

	_, err := buildCmd(t,
		flagOut, out,
		flagAlign, e.alignCfg,
		flagSplitFile, split,
		flagLiveFormV2,
		flagPermissionEvidenceLegacy, legacy,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "legacy")
	require.ErrorContains(t, err, "observation evidence")
	require.NoDirExists(t, out)
}

func TestBuildPermissionEvidenceLegacyRejectsEveryEvidencePart(t *testing.T) {
	tests := []struct {
		name     string
		evidence testPermissionEvidence
	}{
		{name: "pty.opened", evidence: testPermissionEvidence{opened: true}},
		{name: "event", evidence: testPermissionEvidence{event: true}},
		{name: "pty.closed", evidence: testPermissionEvidence{closed: true}},
		{name: "inconsistent stream", evidence: testPermissionEvidence{
			opened: true, event: true, closed: true, inconsistent: true,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, _ := permissionEvidenceLegacyFixture(t, false)
			writeSessionAsWithPermissionEvidenceParts(t, e.dataDir, legacySessionEvidence,
				protocol.HarnessOpencode, "1.18.29", []chunk{
					{ms: 100, output: openCodeBox},
					{ms: 200, draft: draftClosed},
					{ms: 2100, output: openCodeBox},
				}, test.evidence)
			split := writeLegacySplit(t, e, "")
			legacy, _ := writeLegacyList(t, e, "permission-evidence-legacy",
				legacySessionA, legacySessionD, legacySessionEvidence)
			out := filepath.Join(e.work, "evidence-part")

			stderr, err := buildCmdWithStderr(t,
				flagOut, out,
				flagAlign, e.alignCfg,
				flagSplitFile, split,
				flagLiveFormV2,
				flagPermissionEvidenceLegacy, legacy,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "legacy")
			require.ErrorContains(t, err, "observation evidence")
			require.NotContains(t, stderr, "skipped", "cohort evidence errors are fatal before replay")
			require.NoDirExists(t, out)
		})
	}
}

func TestBuildPermissionEvidenceLegacyDoesNotExcuseUnlistedMissingEvidence(t *testing.T) {
	e, split := permissionEvidenceLegacyFixture(t, false)
	legacy, _ := writeLegacyList(t, e, "permission-evidence-legacy", legacySessionA)
	out := filepath.Join(e.work, "unlisted-missing-evidence")

	stderr, err := buildCmdWithStderr(t,
		flagOut, out,
		flagAlign, e.alignCfg,
		flagSplitFile, split,
		flagLiveFormV2,
		flagPermissionEvidenceLegacy, legacy,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "permission dialog observation evidence is missing")
	require.NotContains(t, stderr, "skipped", "a configured unlisted capture fails the cohort instead")
	require.NoDirExists(t, out)
}

func TestBuildPermissionEvidenceLegacyListIsStrictAndPresent(t *testing.T) {
	e, split := permissionEvidenceLegacyFixture(t, false)
	missingFromSplit := writeLegacySplit(t, e, legacySessionD)
	tests := []struct {
		name       string
		sessions   []string
		split      string
		wantReason string
	}{
		{
			name: "unsorted", sessions: []string{legacySessionD, legacySessionA}, split: split,
			wantReason: "lexicographic",
		},
		{
			name: "absent from data dir", sessions: []string{"sess-absent"}, split: split,
			wantReason: "data dir",
		},
		{
			name: "absent from split", sessions: []string{legacySessionA, legacySessionD}, split: missingFromSplit,
			wantReason: "split file",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacy, _ := writeLegacyList(t, e, "bad-"+strings.ReplaceAll(test.name, " ", "-"), test.sessions...)
			out := filepath.Join(e.work, "bad-out-"+strings.ReplaceAll(test.name, " ", "-"))
			_, err := buildCmd(t,
				flagOut, out,
				flagAlign, e.alignCfg,
				flagSplitFile, test.split,
				flagLiveFormV2,
				flagPermissionEvidenceLegacy, legacy,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, test.wantReason)
			require.NoDirExists(t, out)
		})
	}
}

func TestPermissionEvidenceLegacyFlagNeedsLiveFormV2(t *testing.T) {
	e, split := permissionEvidenceLegacyFixture(t, false)
	legacy, _ := writeLegacyList(t, e, "permission-evidence-legacy",
		legacySessionA, legacySessionD)
	out := filepath.Join(e.work, "not-v2")

	_, err := buildCmd(t,
		flagOut, out,
		flagAlign, e.alignCfg,
		flagSplitFile, split,
		flagPermissionEvidenceLegacy, legacy,
	)
	var exit *deps.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, exitcode.Usage, exit.Code)
	require.ErrorContains(t, err, flagPermissionEvidenceLegacy+" requires "+flagLiveFormV2)
	require.NoDirExists(t, out)
}

func buildCmdWithStderr(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string{"build"}, args...))
	err := cmd.Execute()
	return stderr.String(), err
}
