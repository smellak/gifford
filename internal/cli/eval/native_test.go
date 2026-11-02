package eval

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

func cliNativeEvidence(id protocol.ULID) dataset.NativeLabelEvidence {
	at := time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)
	start := dataset.NativeLabelBoundary{
		EventID: "01JB0000000000000000000701", EventType: protocol.EventTypeToolStarted,
		Sequence: 1, At: at,
	}
	return dataset.NativeLabelEvidence{
		WindowID: id, Label: dataset.LabelC,
		RunDigest:   protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		EpochDigest: protocol.Digest("sha256:" + strings.Repeat("b", 64)),
		Start:       start,
		Basis:       start,
		End: dataset.NativeLabelBoundary{
			EventID: "01JB0000000000000000000702", EventType: protocol.EventTypeToolCompleted,
			Sequence: 2, At: at.Add(time.Second),
		},
	}
}

func TestEvalRequiresSealedNativeSidecarBesideManifest(t *testing.T) {
	t.Parallel()
	window := cliWindow(70, dataset.SplitTest, dataset.LabelC)
	window.Target.Source = dataset.LabelSourceNative
	manifest, err := dataset.BuildManifest([]dataset.Window{window})
	require.NoError(t, err)
	inv, err := dataset.AttachNativeLabels(dataset.Inventory{
		Windows: []dataset.Window{window}, Manifest: manifest,
	}, []dataset.NativeLabelEvidence{cliNativeEvidence(window.WindowID)})
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "dataset")
	require.NoError(t, dataset.WriteDir(dir, inv))
	manifestPath := filepath.Join(dir, dataset.ManifestFile)
	require.NoError(t, validateNativeSidecar(manifestPath, inv.Manifest))

	require.NoError(t, os.Remove(filepath.Join(dir, dataset.NativeLabelsFile)))
	err = validateNativeSidecar(manifestPath, inv.Manifest)
	require.Error(t, err, "inference cannot begin without the evidence document sealed by the manifest")

	base := write(t, dir, "base.jsonl", predictions(profileBase))
	_, err = exec(t, flagProfile, "base="+base, "--manifest", manifestPath)
	require.ErrorContains(t, err, dataset.NativeLabelsFile,
		"scoring cannot proceed after the sealed evidence document disappears")
}
