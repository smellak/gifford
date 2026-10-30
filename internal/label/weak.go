package label

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	// WeakCriterionV1 is the first closed weak-label review criterion. Labels
	// A-F require observed visible-state evidence, not the intended script.
	// Label G is allowed only after review of a valid, complete, sanitised
	// window truly finds no determinant state indicator; a missing hook or
	// root link, label conflict, blank screen or lost capture is not evidence.
	WeakCriterionV1 = "weak-review-v1"
	// WeakMethodAgentReview is the sole v1 import method.
	WeakMethodAgentReview = "agent_review"
	// maxWeakImportBytes bounds one atomic import.
	maxWeakImportBytes = 16 << 20
)

// WeakReason is a closed audit reason under a criterion version.
type WeakReason string

// WeakCriterionV1 reasons. Arbitrary reviewer prose is never stored here.
const (
	WeakReasonVisibleState  WeakReason = "visible_state_indicator"
	WeakReasonNoDeterminant WeakReason = "no_determinant_indicator"
)

var errBadWeakRecord = errors.New("invalid weak label record")

// WeakRecord is one auditable, imported weak label. ImportWeak validates and
// canonicalises these records before any can reach WeakFile.
type WeakRecord struct {
	WindowID         protocol.ULID `json:"window_id"`
	Label            dataset.Label `json:"label"`
	Reason           WeakReason    `json:"reason"`
	Method           string        `json:"method"`
	CriterionVersion string        `json:"criterion_version"`
	Reviewer         string        `json:"reviewer"`
	LabeledAt        time.Time     `json:"labeled_at"`
	Source           Source        `json:"source"`
}

// ImportWeak validates all JSONL from r before appending a canonical block to
// WeakFile. The input may omit source; the persisted record always says weak.
// An invalid import writes nothing and its error never quotes record content.
func ImportWeak(dir string, r io.Reader) (int, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxWeakImportBytes+1))
	if err != nil {
		return 0, fmt.Errorf("%w: read input", errBadWeakRecord)
	}
	if len(raw) > maxWeakImportBytes {
		return 0, fmt.Errorf("%w: input is too large", errBadWeakRecord)
	}
	var canonical bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), maxLineBytes)
	count, lineNo := 0, 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		record, err := decodeWeakRecord(line, true)
		if err != nil {
			return 0, fmt.Errorf("%w: line %d", errBadWeakRecord, lineNo)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return 0, fmt.Errorf("%w: encode record", errBadWeakRecord)
		}
		canonical.Write(encoded)
		canonical.WriteByte('\n')
		count++
	}
	if scanner.Err() != nil {
		return 0, fmt.Errorf("%w: unreadable input", errBadWeakRecord)
	}
	if count == 0 {
		return 0, nil
	}
	if err := appendLine(dir, WeakFile, canonical.Bytes()); err != nil {
		return 0, fmt.Errorf("append weak labels: %w", err)
	}
	return count, nil
}

// ReadWeak reads canonical WeakFile records, skipping and counting malformed
// lines like the other label sources. A missing file has no records.
func ReadWeak(dir string) ([]WeakRecord, int, error) {
	var out []WeakRecord
	bad, err := readLines(dir, WeakFile, func(line []byte) error {
		r, err := decodeWeakRecord(line, false)
		if err != nil {
			return errBadWeakRecord
		}
		out = append(out, r)
		return nil
	})
	return out, bad, err
}

func decodeWeakRecord(line []byte, importing bool) (WeakRecord, error) {
	var r WeakRecord
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return WeakRecord{}, errBadWeakRecord
	}
	var extra any
	if !errors.Is(dec.Decode(&extra), io.EOF) {
		return WeakRecord{}, errBadWeakRecord
	}
	if importing {
		var fields map[string]json.RawMessage
		if json.Unmarshal(line, &fields) != nil {
			return WeakRecord{}, errBadWeakRecord
		}
		if raw, present := fields["source"]; present {
			var source Source
			if json.Unmarshal(raw, &source) != nil || source != SourceWeak {
				return WeakRecord{}, errBadWeakRecord
			}
		}
	}
	return normaliseWeakRecord(r, importing)
}

func normaliseWeakRecord(r WeakRecord, importing bool) (WeakRecord, error) {
	if !r.WindowID.Valid() || r.LabeledAt.IsZero() || r.CriterionVersion != WeakCriterionV1 ||
		r.Method != WeakMethodAgentReview || !validWeakReviewer(r.Reviewer) {
		return WeakRecord{}, errBadWeakRecord
	}
	wantReason := WeakReasonVisibleState
	if r.Label == dataset.LabelG {
		wantReason = WeakReasonNoDeterminant
	} else if _, ok := r.Label.Phase(); !ok {
		return WeakRecord{}, errBadWeakRecord
	}
	if r.Reason != wantReason {
		return WeakRecord{}, errBadWeakRecord
	}
	if r.Source == "" && importing {
		r.Source = SourceWeak
	}
	if r.Source != SourceWeak {
		return WeakRecord{}, errBadWeakRecord
	}
	_, offset := r.LabeledAt.Zone()
	if offset != 0 {
		return WeakRecord{}, errBadWeakRecord
	}
	r.LabeledAt = r.LabeledAt.UTC()
	return r, nil
}

func validWeakReviewer(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	if !lowerOrDigit(s[0]) {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !lowerOrDigit(c) && c != '-' {
			return false
		}
	}
	return true
}

func lowerOrDigit(c byte) bool { return c >= 'a' && c <= 'z' || isDigit(c) }
