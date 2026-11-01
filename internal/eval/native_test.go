package eval

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

func TestNativeTruthIsSeparateFromCalibrationAndGateMetrics(t *testing.T) {
	t.Parallel()
	lines := []line{
		{n: 1, truth: "A", probs: map[string]float64{"A": 1}},
		{n: 2, truth: "B", probs: map[string]float64{"C": 1}},
		{n: 3, truth: "C", probs: map[string]float64{"C": 1}},
	}
	predictions := read(t, file(lines...), ProfileBase)
	mc := manifestFor(lines)
	for i := range mc.Manifest.Entries {
		mc.Manifest.Entries[i].LabelSource = dataset.LabelSourceHook
	}
	mc.Manifest.Entries[1].LabelSource = dataset.LabelSourceNative
	mc.Manifest.Entries[2].LabelSource = dataset.LabelSourceNative
	mc.Manifest.Entries[1].SourceQuality = protocol.SourceQualityPTYScreen
	mc.Manifest.Entries[2].SourceQuality = protocol.SourceQualityPTYScreen
	mc.Manifest.NativeLabels = &dataset.NativeLabelsPolicy{
		SchemaVersion: dataset.NativeLabelSidecarSchemaVersion,
		Entries:       2,
		Digest:        protocol.Digest("sha256:" + strings.Repeat("d", 64)),
	}

	report, err := Evaluate(Options{Base: predictions[:2], Manifest: mc})
	require.NoError(t, err)
	require.Equal(t, 1, report.Profiles[0].Test.All.N,
		"native truth does not fit calibration or enter ordinary model metrics")
	require.InDelta(t, 1, report.Profiles[0].Test.All.Accuracy, 0)
	require.NotNil(t, report.Native)
	require.Equal(t, mc.Manifest.NativeLabels.Digest, report.Native.EvidenceDigest)
	require.Len(t, report.Native.Profiles, 1)
	native := report.Native.Profiles[0].Test
	require.Equal(t, 2, native.Metrics.N)
	require.Equal(t, Rate{Num: 1, Den: 2}, native.Coverage)
	require.Zero(t, native.Metrics.Accuracy, "an invalid line is no agreement, never native's answer")
	require.Equal(t, 1, native.Metrics.Confusion[idxB][idxC])
	require.Equal(t, 1, native.Metrics.Confusion[idxC][idxG],
		"a missing prediction is UNKNOWN and disagreement")

	markdown := report.Markdown()
	require.Contains(t, markdown, "## Native-screen concordance")
	require.Contains(t, markdown, "Native operational resolutions: N/A")
	require.Contains(t, markdown, "Coverage: 0.5000 (1/2)")
	require.Contains(t, markdown, "Evidence sidecar digest: `"+string(mc.Manifest.NativeLabels.Digest)+"`")
}

func TestNativeTruthCannotEnterPartitionCalibrationOrBlindCoverage(t *testing.T) {
	t.Parallel()
	f := newPartitionFixture(t)

	// One val-cmp row and one blind-test row become optional native truth.
	// They stay in the prediction files for offline concordance, but must not
	// fit calibration, enter model metrics, or satisfy blind-test readiness.
	f.manifest.Manifest.Entries[8].LabelSource = dataset.LabelSourceNative
	f.manifest.Manifest.Entries[13].LabelSource = dataset.LabelSourceNative
	f.manifest.Manifest.NativeLabels = &dataset.NativeLabelsPolicy{
		SchemaVersion: dataset.NativeLabelSidecarSchemaVersion,
		Entries:       2,
		Digest:        fixedDigest('e'),
	}
	thresholds := f.manifest.Manifest.Readiness.Thresholds
	thresholds.TestMin = 4
	readiness := dataset.CheckReadiness(f.manifest.Manifest.Entries, thresholds)
	require.True(t, readiness.Passed, "the complete manifest remains ready")
	f.manifest.Manifest.Readiness = &readiness
	f.manifest.Manifest.DatasetDigest = dataset.ManifestDigest(f.manifest.Manifest)
	require.NoError(t, f.manifest.Manifest.Validate())

	checkpoint, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal,
	})
	require.NoError(t, err)
	require.Equal(t, 8, checkpoint.Partition.CalibrationWindows)
	require.Equal(t, 3, checkpoint.Partition.ComparisonWindows)
	require.Equal(t, 3, checkpoint.Profiles[0].Validation.All.N)
	require.NotNil(t, checkpoint.Native)
	require.Equal(t, 1, checkpoint.Native.Profiles[0].Validation.Metrics.N)

	raw, err := checkpoint.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := ParseCalibrationSet(raw)
	require.NoError(t, err)
	final, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
		SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
	})
	require.NoError(t, err)
	require.Equal(t, 3, final.Partition.BlindTestWindows)
	require.Equal(t, 3, final.Profiles[0].Test.All.N)
	require.Equal(t, 1, final.Native.Profiles[0].Test.Metrics.N)
	for _, criterion := range final.Gate.Criteria {
		if criterion.ID >= 1 && criterion.ID <= 4 {
			require.Equal(t, VerdictPending, criterion.Verdict,
				"native truth must not lend the missing fourth blind window")
		}
	}
}

func evalNativeEvidence(id protocol.ULID, runByte byte) dataset.NativeLabelEvidence {
	at := time.Date(2026, 11, 1, 15, 0, 0, 0, time.UTC)
	start := dataset.NativeLabelBoundary{
		EventID: "01JB0000000000000000000501", EventType: protocol.EventTypePromptSubmitted,
		Sequence: 1, At: at,
	}
	return dataset.NativeLabelEvidence{
		WindowID: id, Label: dataset.LabelB,
		RunDigest:   protocol.Digest("sha256:" + strings.Repeat(string(runByte), 64)),
		EpochDigest: protocol.Digest("sha256:" + strings.Repeat("e", 64)),
		Start:       start,
		Basis:       start,
		End: dataset.NativeLabelBoundary{
			EventID: "01JB0000000000000000000502", EventType: protocol.EventTypeTurnCompleted,
			Sequence: 2, At: at.Add(time.Second), Result: protocol.NativeEventResultOK,
		},
	}
}

func TestInferencePayloadIgnoresNativeMetadata(t *testing.T) {
	t.Parallel()
	window := evalWindow(7, dataset.SplitTest, dataset.LabelB)
	window.Target.Source = dataset.LabelSourceNative
	manifest, err := dataset.BuildManifest([]dataset.Window{window})
	require.NoError(t, err)
	first, err := dataset.AttachNativeLabels(dataset.Inventory{
		Windows: []dataset.Window{window}, Manifest: manifest,
	}, []dataset.NativeLabelEvidence{evalNativeEvidence(window.WindowID, 'a')})
	require.NoError(t, err)
	second, err := dataset.AttachNativeLabels(dataset.Inventory{
		Windows: []dataset.Window{window}, Manifest: manifest,
	}, []dataset.NativeLabelEvidence{evalNativeEvidence(window.WindowID, 'b')})
	require.NoError(t, err)
	require.NotEqual(t, first.Manifest.NativeLabels.Digest, second.Manifest.NativeLabels.Digest)
	require.NotEqual(t, first.NativeLabels.Entries[0].RunDigest, second.NativeLabels.Entries[0].RunDigest)

	firstPayload, err := payloadOf(first.Windows[0].Input)
	require.NoError(t, err)
	secondPayload, err := payloadOf(second.Windows[0].Input)
	require.NoError(t, err)
	require.Equal(t, first.Windows[0].Input, second.Windows[0].Input)
	require.Equal(t, firstPayload, secondPayload,
		"the real ANALYZE_AGENT_STATE payload cannot carry event ids, sequences, times, enums, or digests")
}
