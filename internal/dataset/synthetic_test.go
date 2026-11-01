package dataset

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func validSyntheticTrainingWindow() Window {
	w := validWindow()
	w.Target = Target{Label: LabelD, Source: LabelSourceSynthetic}
	w.Provenance.SourceQuality = protocol.SourceQualitySynthetic
	w.Split.Split = SplitTrain
	w.Screen.ObservationForm = ""
	w.Screen.ObservationPolicyVersion = ""
	w.Screen.ObservationEpisodeDigest = ""
	w.Screen.ObservationAnchorDigest = ""
	return w
}

func TestValidateSyntheticTrainingWindow(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateSyntheticTrainingWindow(validSyntheticTrainingWindow()))

	for name, edit := range map[string]func(*Window){
		"invalid window":  func(w *Window) { w.WindowID = "bad" },
		"captured screen": func(w *Window) { w.Provenance.SourceQuality = protocol.SourceQualityPTYScreen },
		"real label":      func(w *Window) { w.Target.Source = LabelSourceHook },
		"other label":     func(w *Window) { w.Target.Label = LabelA },
		"validation":      func(w *Window) { w.Split.Split = SplitValidation },
		"test split":      func(w *Window) { w.Split.Split = SplitTest },
		"observation evidence": func(w *Window) {
			w.Screen.ObservationForm = ObservationFormEmptyInputV1
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := validSyntheticTrainingWindow()
			edit(&w)
			err := ValidateSyntheticTrainingWindow(w)
			require.ErrorIs(t, err, ErrInvalidSyntheticTrainingWindow)
		})
	}

	for _, label := range []Label{LabelD, LabelE, LabelF} {
		w := validSyntheticTrainingWindow()
		w.Target.Label = label
		require.NoError(t, ValidateSyntheticTrainingWindow(w), label)
	}
}
