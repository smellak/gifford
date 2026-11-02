package dataset

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func nativeEvidence(id protocol.ULID, label Label) NativeLabelEvidence {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	basis := protocol.EventTypePromptSubmitted
	if label == LabelC {
		basis = protocol.EventTypeToolStarted
	}
	if label == LabelD {
		basis = protocol.EventTypeApprovalRequested
	}
	if label == LabelE {
		basis = protocol.EventTypeQuestionRequested
	}
	if label == LabelF {
		basis = protocol.EventTypeTurnCompleted
	}
	start := NativeLabelBoundary{
		EventID: "01JB0000000000000000000101", EventType: basis,
		Sequence: 1, At: at,
	}
	if label == LabelF {
		start.Result = protocol.NativeEventResultFailed
	}
	end := NativeLabelBoundary{
		EventID: "01JB0000000000000000000102", EventType: protocol.EventTypeTurnCompleted,
		Sequence: 2, At: at.Add(time.Second), Result: protocol.NativeEventResultOK,
	}
	if label == LabelF {
		end.EventType, end.Result = protocol.EventTypeTurnStarted, ""
	}
	return NativeLabelEvidence{
		WindowID: id, Label: label,
		RunDigest:   protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		EpochDigest: protocol.Digest("sha256:" + strings.Repeat("b", 64)),
		Start:       start,
		Basis:       start,
		End:         end,
	}
}

func TestAttachNativeLabelsSealsFinalWindowAndWritesPrivateSidecar(t *testing.T) {
	t.Parallel()
	w := windowAs("01ARZ3NDEKTSV4RRFFQ69G5FAV", SplitTest, LabelB, EnvironmentDomainBankGeneric, "native-group")
	w.Target.Source = LabelSourceNative
	m, err := BuildManifest([]Window{w})
	require.NoError(t, err)
	require.Nil(t, m.NativeLabels, "the pipeline manifest is completed only after screen projection")

	inv, err := AttachNativeLabels(Inventory{Windows: []Window{w}, Manifest: m}, []NativeLabelEvidence{
		nativeEvidence(w.WindowID, LabelB),
	})
	require.NoError(t, err)
	require.NotNil(t, inv.NativeLabels)
	require.NotNil(t, inv.Manifest.NativeLabels)
	require.Equal(t, int64(1), inv.Manifest.NativeLabels.Entries)
	require.NoError(t, inv.Manifest.Validate())
	require.NoError(t, ValidateNativeLabelSidecar(inv.Manifest, *inv.NativeLabels))
	require.Equal(t, w.Screen.Digest, inv.NativeLabels.Entries[0].ScreenDigest)
	wantWindowDigest, err := WindowDigest(w)
	require.NoError(t, err)
	require.Equal(t, wantWindowDigest, inv.NativeLabels.Entries[0].WindowDigest)

	out := filepath.Join(t.TempDir(), "dataset")
	require.NoError(t, WriteDir(out, inv))
	info, err := os.Stat(filepath.Join(out, NativeLabelsFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	raw, err := os.ReadFile(filepath.Join(out, NativeLabelsFile))
	require.NoError(t, err)
	parsed, err := ParseNativeLabelSidecar(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, *inv.NativeLabels, parsed)

	tampered := parsed
	tampered.Entries = append([]NativeLabelRecord(nil), parsed.Entries...)
	tampered.Entries[0].WindowDigest = protocol.Digest("sha256:" + strings.Repeat("c", 64))
	require.ErrorIs(t, ValidateNativeLabelSidecar(inv.Manifest, tampered), ErrInvalidNativeLabels)
}

func TestAttachNativeLabelsDisabledLeavesInventoryAndBytesUnchanged(t *testing.T) {
	t.Parallel()
	w := windowAs("01ARZ3NDEKTSV4RRFFQ69G5FB0", SplitTrain, LabelC, EnvironmentDomainBankGeneric, "ordinary-group")
	m, err := BuildManifest([]Window{w})
	require.NoError(t, err)
	before := Inventory{Windows: []Window{w}, Manifest: m}
	beforeJSON, err := json.Marshal(before.Manifest)
	require.NoError(t, err)

	after, err := AttachNativeLabels(before, nil)
	require.NoError(t, err)
	require.Equal(t, before, after)
	afterJSON, err := json.Marshal(after.Manifest)
	require.NoError(t, err)
	require.True(t, bytes.Equal(beforeJSON, afterJSON), "disabled manifest JSON bytes changed")
	require.Equal(t, before.Manifest.DatasetDigest, after.Manifest.DatasetDigest)

	out := filepath.Join(t.TempDir(), "dataset")
	require.NoError(t, WriteDir(out, after))
	_, err = os.Stat(filepath.Join(out, NativeLabelsFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNativeLabelSidecarHasNoContentFields(t *testing.T) {
	t.Parallel()
	var got []string
	windowFields(reflect.TypeOf(NativeLabelSidecar{}), "", &got)
	windowFields(reflect.TypeOf(NativeLabelRecord{}), "entries[].", &got)
	sort.Strings(got)
	require.Equal(t, []string{
		"entries", "entries[].basis", "entries[].basis.event_id",
		"entries[].basis.event_type", "entries[].basis.received_at",
		"entries[].basis.result", "entries[].basis.sequence", "entries[].end", "entries[].end.event_id",
		"entries[].end.event_type", "entries[].end.received_at",
		"entries[].end.result", "entries[].end.sequence", "entries[].epoch_digest", "entries[].label",
		"entries[].run_digest", "entries[].screen_digest", "entries[].start",
		"entries[].start.event_id", "entries[].start.event_type",
		"entries[].start.received_at", "entries[].start.result", "entries[].start.sequence",
		"entries[].window_digest", "entries[].window_id",
		wantSchemaVersion,
	}, got, "a native sidecar field was added or renamed: review it against the no-content rule")
	for _, path := range got {
		for _, banned := range []string{
			"argument", "body", "content", "draft", "keystroke", "message",
			"output", "payload", "prompt", "question", "response", "transcript",
		} {
			require.NotContains(t, path, banned, "field %s", path)
		}
	}
}

func TestParseNativeLabelSidecarIsClosedAndChecksBoundaryOrder(t *testing.T) {
	t.Parallel()
	w := windowAs("01ARZ3NDEKTSV4RRFFQ69G5FB1", SplitTest, LabelF, EnvironmentDomainBankGeneric, "native-parse")
	w.Target.Source = LabelSourceNative
	m, err := BuildManifest([]Window{w})
	require.NoError(t, err)
	inv, err := AttachNativeLabels(Inventory{Windows: []Window{w}, Manifest: m}, []NativeLabelEvidence{
		nativeEvidence(w.WindowID, LabelF),
	})
	require.NoError(t, err)
	raw, err := json.Marshal(inv.NativeLabels)
	require.NoError(t, err)
	raw = bytes.Replace(raw, []byte(`"schema_version":1`), []byte(`"schema_version":1,"content":"forbidden"`), 1)
	_, err = ParseNativeLabelSidecar(bytes.NewReader(raw))
	require.ErrorIs(t, err, ErrInvalidNativeLabels)

	raw, err = json.Marshal(inv.NativeLabels)
	require.NoError(t, err)
	raw = bytes.Replace(raw, []byte(`"sequence":1,`), nil, 1)
	_, err = ParseNativeLabelSidecar(bytes.NewReader(raw))
	require.ErrorIs(t, err, ErrInvalidNativeLabels, "a required zero-valued sequence cannot be omitted")

	bad := nativeEvidence(w.WindowID, LabelF)
	bad.End.Sequence = bad.Start.Sequence
	_, err = AttachNativeLabels(Inventory{Windows: []Window{w}, Manifest: m}, []NativeLabelEvidence{bad})
	require.ErrorIs(t, err, ErrInvalidNativeLabels)

	missingFailure := nativeEvidence(w.WindowID, LabelF)
	missingFailure.Basis.Result = ""
	_, err = AttachNativeLabels(Inventory{Windows: []Window{w}, Manifest: m}, []NativeLabelEvidence{missingFailure})
	require.ErrorIs(t, err, ErrInvalidNativeLabels, "F must retain its failed turn outcome")
}
