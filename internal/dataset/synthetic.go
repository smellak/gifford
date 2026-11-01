package dataset

import (
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// ErrInvalidSyntheticTrainingWindow means a prebuilt window cannot enter the
// training manifest through dataset build's synthetic input.
var ErrInvalidSyntheticTrainingWindow = errors.New("invalid synthetic training window")

// ValidateSyntheticTrainingWindow checks the boundary for prebuilt synthetic
// training data. These windows bypass capture replay and projection, so both
// provenance markers must say synthetic, only the approved labels may enter,
// and they may never leave train. Observation metadata belongs to captured
// live-input evidence and cannot be manufactured by this path.
func ValidateSyntheticTrainingWindow(w Window) error {
	if err := w.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSyntheticTrainingWindow, err)
	}
	if w.Provenance.SourceQuality != protocol.SourceQualitySynthetic {
		return fmt.Errorf("%w: provenance.source_quality is not synthetic", ErrInvalidSyntheticTrainingWindow)
	}
	if w.Target.Source != LabelSourceSynthetic {
		return fmt.Errorf("%w: target.source is not synthetic", ErrInvalidSyntheticTrainingWindow)
	}
	switch w.Target.Label {
	case LabelD, LabelE, LabelF:
	default:
		return fmt.Errorf("%w: target.label is not D, E or F", ErrInvalidSyntheticTrainingWindow)
	}
	if w.Split.Split != SplitTrain {
		return fmt.Errorf("%w: split is not train", ErrInvalidSyntheticTrainingWindow)
	}
	if w.Screen.ObservationForm != "" {
		return fmt.Errorf("%w: screen observation metadata is reserved for captured evidence", ErrInvalidSyntheticTrainingWindow)
	}
	return nil
}
