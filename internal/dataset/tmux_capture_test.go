package dataset

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// All fixture content is invented. The form records what the producer sends,
// not whether the live detector or human-input gate admitted this snapshot.
func tmuxCaptureWindow() Window {
	w := validWindow()
	w.Input.SecondsSinceLastOutput = 0
	w.Input.OutputBytes = OutputBytes{}
	w.Input.NewChildProcesses = nil
	w.Input.PreviousState = ""
	w.Input.PreviousStateAgeSeconds = 0
	w.Screen.ObservationForm = ObservationFormTmuxCaptureV1
	w.Provenance.SourceQuality = protocol.SourceQualityBootstrapImport
	w.Provenance.ActivityVersion = TmuxCaptureActivityVersion
	w.Provenance.SequenceFrom, w.Provenance.SequenceTo = 0, 0
	w.Provenance.WindowStart = w.Provenance.AnchorAt
	w.Provenance.WindowEnd = w.Provenance.AnchorAt
	return w
}

func TestTmuxCaptureWindowRoundTrip(t *testing.T) {
	w := tmuxCaptureWindow()
	require.NoError(t, w.Validate())
	raw, err := json.Marshal(w)
	require.NoError(t, err)
	got, err := ParseWindow(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, w, got)
	require.Nil(t, got.Input.NewChildProcesses)
	require.Equal(t, w.Provenance.AnchorAt, got.Provenance.WindowStart)
}

func TestTmuxCaptureRequiresExactProducerDefaults(t *testing.T) {
	for name, mutate := range map[string]func(*Window){
		"invented quiet time": func(w *Window) { w.Input.SecondsSinceLastOutput = 10 },
		"negative time":       func(w *Window) { w.Input.SecondsSinceLastOutput = -1 },
		"NaN time":            func(w *Window) { w.Input.SecondsSinceLastOutput = math.NaN() },
		"half second bytes":   func(w *Window) { w.Input.OutputBytes.Last500ms = 1 },
		"two second bytes":    func(w *Window) { w.Input.OutputBytes.Last2s = 1 },
		"ten second bytes":    func(w *Window) { w.Input.OutputBytes.Last10s = 1 },
		"empty children":      func(w *Window) { w.Input.NewChildProcesses = []string{} },
		"observed child":      func(w *Window) { w.Input.NewChildProcesses = []string{"go test"} },
		"previous state":      func(w *Window) { w.Input.PreviousState = protocol.AgentPhaseIdleAtPrompt },
		"previous age":        func(w *Window) { w.Input.PreviousStateAgeSeconds = 1 },
		"PTY source":          func(w *Window) { w.Provenance.SourceQuality = protocol.SourceQualityPTYScreen },
		"unspecified activity": func(w *Window) {
			w.Provenance.ActivityVersion = ""
		},
		"invented event range": func(w *Window) { w.Provenance.SequenceTo = 1 },
		"invented duration": func(w *Window) {
			w.Provenance.WindowStart = w.Provenance.AnchorAt.Add(-time.Second)
		},
		"unredacted": func(w *Window) { w.Screen.Redaction.Applied = false },
		"no input exclusion": func(w *Window) {
			w.Screen.HumanInputExclusion.Applied = false
		},
		"permission metadata": func(w *Window) { w.Screen.ObservationPolicyVersion = "permission-dialog-v2" },
	} {
		t.Run(name, func(t *testing.T) {
			w := tmuxCaptureWindow()
			mutate(&w)
			require.ErrorIs(t, w.Validate(), ErrInvalidWindow)
		})
	}
}

func TestTmuxCaptureDoesNotRelaxPTY(t *testing.T) {
	for _, form := range []string{"", ObservationFormEmptyInputV1} {
		w := validWindow()
		w.Screen.ObservationForm = form
		w.Input.SecondsSinceLastOutput = 0
		w.Input.OutputBytes = OutputBytes{}
		require.ErrorIs(t, w.Validate(), ErrInvalidWindow, "ordinary PTY counters remain coherent")
	}
	w := validWindow()
	w.Provenance.ActivityVersion = TmuxCaptureActivityVersion
	require.ErrorIs(t, w.Validate(), ErrInvalidWindow, "tmux activity cannot label a PTY window")
	w = validWindow()
	w.Provenance.WindowStart = w.Provenance.WindowEnd
	require.ErrorIs(t, w.Validate(), ErrInvalidWindow, "PTY windows still need an interval")
}

func TestTmuxCaptureManifestIsSeparateAndUnderpowered(t *testing.T) {
	w := tmuxCaptureWindow()
	w.Target = Target{Label: LabelA, Source: LabelSourceWeak}
	m, err := BuildTmuxCaptureManifest([]Window{w})
	require.NoError(t, err)
	require.NoError(t, m.Validate())
	require.Equal(t, ScreenFormTmuxCaptureV1, m.ScreenForm)
	require.Equal(t, TmuxCaptureActivityVersion, m.ActivityVersion)
	require.NotNil(t, m.Readiness)
	require.False(t, m.Readiness.Passed)
	require.False(t, m.ScriptedInput, "importing is not evidence that a script supplied the input")
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	parsed, err := ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, m, parsed)

	_, err = BuildManifest([]Window{w})
	require.ErrorIs(t, err, ErrInvalidManifest, "default manifest cannot disguise the input form")
	_, err = BuildTmuxCaptureManifest([]Window{validWindow()})
	require.ErrorIs(t, err, ErrInvalidManifest, "tmux manifest cannot contain PTY input")

	for name, mutate := range map[string]func(*Manifest){
		"unspecified activity": func(m *Manifest) { m.ActivityVersion = "" },
		"PTY source": func(m *Manifest) {
			m.Entries[0].SourceQuality = protocol.SourceQualityPTYScreen
		},
		"missing form":     func(m *Manifest) { m.Entries[0].ObservationForm = "" },
		"live v2 disguise": func(m *Manifest) { m.ScreenForm = ScreenFormLiveV2 },
	} {
		t.Run(name, func(t *testing.T) {
			m, err := BuildTmuxCaptureManifest([]Window{w})
			require.NoError(t, err)
			mutate(&m)
			m.DatasetDigest = ManifestDigest(m)
			require.ErrorIs(t, m.Validate(), ErrInvalidManifest)
		})
	}
}
