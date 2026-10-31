package label

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// TimelineNone is the timeline label of no known state: the driver acted,
// and knows nothing until it sees the next state.
const TimelineNone = "NONE"

// TimelineRecord is one line of TimelineFile, written by the driver of a
// scripted session: from At, the recording session shows the agent
// phase Label, until the session's next line. Label is a phase with a
// dataset label, or TimelineNone. The driver writes a phase only once it
// has seen it on screen.
type TimelineRecord struct {
	Session protocol.OpaqueID `json:"session"`
	At      time.Time         `json:"at"`
	Label   string            `json:"label"`
}

var errBadTimelineRecord = errors.New("invalid timeline record")

// ReadTimeline reads TimelineFile in dir. A missing file has no lines.
// Unlike the other label files, one unreadable line fails the read: a
// lost line would stretch the state before it over the next one.
func ReadTimeline(dir string) ([]TimelineRecord, error) {
	var out []TimelineRecord
	bad, err := readLines(dir, TimelineFile, func(line []byte) error {
		r, err := decodeTimelineRecord(line)
		if err != nil {
			return err
		}
		out = append(out, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if bad > 0 {
		return nil, fmt.Errorf("%s: %d unreadable lines", TimelineFile, bad)
	}
	return out, nil
}

func decodeTimelineRecord(line []byte) (TimelineRecord, error) {
	var r TimelineRecord
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if dec.Decode(&r) != nil || !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
		return TimelineRecord{}, errBadTimelineRecord
	}
	if !r.Session.Valid() || r.At.IsZero() {
		return TimelineRecord{}, errBadTimelineRecord
	}
	if _, ok := timelineLabel(r.Label); !ok {
		return TimelineRecord{}, errBadTimelineRecord
	}
	return r, nil
}

// timelineLabel is the dataset label of a timeline label: none for
// TimelineNone.
func timelineLabel(s string) (dataset.Label, bool) {
	if s == TimelineNone {
		return "", true
	}
	return dataset.LabelOf(protocol.AgentPhase(s))
}

// scriptTransitions are each session's timeline lines as transitions at
// their instants.
func scriptTransitions(records []TimelineRecord) map[protocol.OpaqueID][]transition {
	out := map[protocol.OpaqueID][]transition{}
	for _, r := range records {
		l, _ := timelineLabel(r.Label)
		out[r.Session] = append(out[r.Session], transition{lo: r.At, hi: r.At, label: l})
	}
	return out
}
