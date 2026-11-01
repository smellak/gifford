package eval

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	coreeval "github.com/smellak/gifford/internal/eval"
)

const (
	maxSessionListBytes = 1 << 20
	maxCalibrationBytes = 1 << 20
	valCalSessionFile   = "val-cal.sessions.jsonl"
	blindSessionFile    = "test-blind.sessions.jsonl"
)

type calibrationFlags struct {
	valCalSessions    string
	blindTestSessions string
	calibrationOut    string
	sealed            string
}

func (f calibrationFlags) given() bool {
	return f.valCalSessions != "" || f.blindTestSessions != "" || f.calibrationOut != "" || f.sealed != ""
}

func (f calibrationFlags) validate(manifest string) error {
	valCal, blind := f.valCalSessions != "", f.blindTestSessions != ""
	out, sealed := f.calibrationOut != "", f.sealed != ""
	switch {
	case !valCal && !blind && !out && !sealed:
		return nil
	case valCal && !blind && out && !sealed:
	case valCal && blind && !out && sealed:
	default:
		return usage("checkpoint mode requires --val-cal-sessions and --calibration-out; final mode also requires --blind-test-sessions and uses --sealed-calibrations instead")
	}
	if manifest == "" {
		return usage("partitioned evaluation requires --manifest")
	}
	if filepath.Base(f.valCalSessions) != valCalSessionFile ||
		f.blindTestSessions != "" && filepath.Base(f.blindTestSessions) != blindSessionFile {
		return usage("partition lists must be named " + valCalSessionFile + " and " + blindSessionFile)
	}
	return nil
}

func loadSessionSet(path string) (coreeval.SessionSet, error) {
	raw, err := readSmall(path, maxSessionListBytes, "session list")
	if err != nil {
		return coreeval.SessionSet{}, err
	}
	set, err := coreeval.ParseSessionSet(raw)
	if err != nil {
		return coreeval.SessionSet{}, fmt.Errorf("gifford eval: %s: %w", filepath.Base(path), err)
	}
	return set, nil
}

func loadCalibrations(path string) (coreeval.CalibrationSet, []byte, error) {
	raw, err := readSmall(path, maxCalibrationBytes, "calibration")
	if err != nil {
		return coreeval.CalibrationSet{}, nil, err
	}
	set, err := coreeval.ParseCalibrationSet(raw)
	if err != nil {
		return coreeval.CalibrationSet{}, nil, fmt.Errorf("gifford eval: %s: %w", filepath.Base(path), err)
	}
	return set, raw, nil
}

func readSmall(path string, limit int64, kind string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("gifford eval: open %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, fmt.Errorf("gifford eval: read %s: %w", filepath.Base(path), err)
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("gifford eval: %s is too large for a %s", filepath.Base(path), kind)
	}
	return raw, nil
}
