package eval

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"

	coreeval "github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
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
	if len(raw) == 0 || !utf8.Valid(raw) || raw[len(raw)-1] != '\n' {
		return coreeval.SessionSet{}, fmt.Errorf("gifford eval: %s must be canonical UTF-8 JSONL with a final LF", filepath.Base(path))
	}
	lines := bytes.Split(raw[:len(raw)-1], []byte{'\n'})
	ids := make([]protocol.OpaqueID, 0, len(lines))
	var prior protocol.OpaqueID
	for i, line := range lines {
		id, err := parseSessionLine(line)
		canonical, marshalErr := json.Marshal(struct {
			SessionID protocol.OpaqueID `json:"session_id"`
		}{SessionID: id})
		if err != nil || marshalErr != nil || !bytes.Equal(line, canonical) || i > 0 && id <= prior {
			return coreeval.SessionSet{}, fmt.Errorf("gifford eval: %s line %d is not a sorted canonical session_id", filepath.Base(path), i+1)
		}
		ids = append(ids, id)
		prior = id
	}
	sum := sha256.Sum256(raw)
	digest := protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
	set, err := coreeval.NewSessionSet(ids, digest)
	if err != nil {
		return coreeval.SessionSet{}, fmt.Errorf("gifford eval: %s: %w", filepath.Base(path), err)
	}
	return set, nil
}

// parseSessionLine accepts exactly {"session_id":"<opaque>"}. Walking tokens
// catches repeated members instead of allowing encoding/json's last-key wins.
func parseSessionLine(raw []byte) (protocol.OpaqueID, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') || !dec.More() {
		return "", errors.New("not an object")
	}
	key, err := dec.Token()
	if err != nil || key != "session_id" {
		return "", errors.New("wrong member")
	}
	var value string
	if err := dec.Decode(&value); err != nil || dec.More() {
		return "", errors.New("wrong value")
	}
	tok, err = dec.Token()
	if err != nil || tok != json.Delim('}') {
		return "", errors.New("not closed")
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", errors.New("trailing data")
	}
	id := protocol.OpaqueID(value)
	if !id.Valid() {
		return "", errors.New("invalid id")
	}
	return id, nil
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
