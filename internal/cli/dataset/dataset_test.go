package dataset

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

func TestDatasetNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"redact --version v1",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestDatasetBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{"inspect"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// writeManifest stores m, re-digested so it stays valid.
func writeManifest(t *testing.T, m dataset.Manifest) string {
	t.Helper()
	m.DatasetDigest = dataset.ManifestDigest(m)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

// manifest is a valid three-entry manifest with its readiness run under
// th; its windows are invented.
func manifest(t *testing.T, th dataset.ReadinessThresholds) dataset.Manifest {
	t.Helper()
	m := dataset.Manifest{
		SchemaVersion: dataset.ManifestSchemaVersion, Contract: "ANALYZE_AGENT_STATE",
		WindowSchemaVersion: dataset.WindowSchemaVersion,
		Policies:            dataset.Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
	}
	digest := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	for i, l := range []dataset.Label{dataset.LabelD, dataset.LabelA, dataset.LabelB} {
		split := dataset.SplitTrain
		if i == 2 {
			split = dataset.SplitTest
		}
		m.Entries = append(m.Entries, dataset.ManifestEntry{
			WindowID: []protocol.ULID{"01JB0000000000000000000001", "01JB0000000000000000000002", "01JB0000000000000000000003"}[i],
			Split:    split, Label: l, EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
			WindowDigest: protocol.Digest(digest("1")), GroupDigest: protocol.Digest(digest([]string{"2", "3", "5"}[i])),
			SourceDigest: protocol.Digest(digest("4")), Harness: "codex", HarnessVersion: "1.0", SourceQuality: "pty_screen", LabelSource: dataset.LabelSourceHook,
		})
	}
	m.Counts = dataset.CountEntries(m.Entries)
	r := dataset.CheckReadiness(m.Entries, th)
	m.Readiness = &r
	return m
}

func TestInspect(t *testing.T) {
	t.Parallel()

	zero := dataset.ReadinessThresholds{}
	passed := writeManifest(t, manifest(t, zero))
	out, err := run(t, passed)
	require.NoError(t, err)
	require.Equal(t, "readiness: passed\n", strings.SplitAfterN(out, "\n", 2)[1])

	th := zero
	th.RareLabelTrainMin, th.RareLabelHarnessesMin, th.TestMin = 150, 3, 300
	failed := writeManifest(t, manifest(t, th))
	out, err = run(t, failed)
	require.NoError(t, err)
	require.Equal(t, []string{
		"readiness: failed, 5 faults",
		"test: 1/300 windows",
		"D: 1/150 in train",
		"D: 1 harnesses of 3 in train",
		"E: 0/150 in train",
		"E: 0 harnesses of 3 in train",
	}, strings.Split(strings.TrimSpace(out), "\n")[1:])
	require.True(t, strings.HasPrefix(out, "dataset sha256:"))
	require.Contains(t, out, ": 3 windows (train 2, validation 0, test 1)")

	none := manifest(t, zero)
	none.Readiness = nil
	out, err = run(t, writeManifest(t, none))
	require.NoError(t, err)
	require.Contains(t, out, "readiness: not recorded")

	broken := filepath.Join(t.TempDir(), "broken.json")
	require.NoError(t, os.WriteFile(broken, []byte(`{"schema_version":1}`), 0o600))
	_, err = run(t, broken)
	require.Error(t, err)

	_, err = run(t)
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Usage, ec.Code)
}

// TestInspectAcceptsV0RedactionPolicy pins archival inspection: a policy 2
// binary accepts a self-consistent synthetic manifest sealed under policy 1.
func TestInspectAcceptsV0RedactionPolicy(t *testing.T) {
	t.Parallel()
	require.Equal(t, "2", redact.PolicyVersion)

	m := manifest(t, dataset.ReadinessThresholds{})
	require.Equal(t, "1", m.Policies.RedactionPolicyVersion)

	out, err := run(t, writeManifest(t, m))
	require.NoError(t, err)
	require.Contains(t, out, "readiness: passed")
}
