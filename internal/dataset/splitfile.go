package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/protocol"
)

// MaxSplitFileBytes bounds a split file: one short line per session.
const MaxSplitFileBytes = 1 << 20

// splitLine is one line of a split file.
type splitLine struct {
	Session string `json:"session"`
	Split   Split  `json:"split"`
}

// ParseSplitFile parses a split file: one JSON object per line,
// {"session":"<id>","split":"train"}, "validation" or "test", each session
// once. Blank lines are skipped. It returns each session's split. Its errors
// name a line by number, never its text.
func ParseSplitFile(raw []byte) (map[string]Split, error) {
	if len(raw) > MaxSplitFileBytes {
		return nil, errors.New("split file: larger than 1 MiB")
	}
	sessions := map[string]Split{}
	for i, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var l splitLine
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if dec.Decode(&l) != nil || !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
			return nil, fmt.Errorf("split file line %d: not one object with session and split", i+1)
		}
		switch {
		case !protocol.OpaqueID(l.Session).Valid():
			return nil, fmt.Errorf("split file line %d: not a session id", i+1)
		case !l.Split.Valid():
			return nil, fmt.Errorf("split file line %d: split is not train, validation or test", i+1)
		}
		if _, dup := sessions[l.Session]; dup {
			return nil, fmt.Errorf("split file line %d: session listed twice", i+1)
		}
		sessions[l.Session] = l.Split
	}
	if len(sessions) == 0 {
		return nil, errors.New("split file: no session")
	}
	return sessions, nil
}

// SplitFileDigest is the SHA-256 of a split file's bytes, as a manifest
// keeps it in split_file.
func SplitFileDigest(raw []byte) protocol.Digest {
	sum := sha256.Sum256(raw)
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// SessionMembership is a set of session ids, such as the authenticated
// blind-test session list a final evaluation names.
type SessionMembership interface {
	Len() int
	Contains(id protocol.OpaqueID) bool
}

// ErrSplitFileExtension means a final manifest's split file does not extend
// its training manifest's split file by exactly the blind-test sessions.
var ErrSplitFileExtension = errors.New("split file extension rejected")

// CheckSplitFileExtension reports whether final, the split file a final
// manifest was built from, extends training, the split file its training
// manifest was built from, by exactly the sessions in blind:
//   - both parse as split files, so neither lists a session twice;
//   - training is an exact byte prefix of final, and final is longer;
//   - every session final adds is test;
//   - the sessions final adds are exactly blind.
//
// A training file without a final newline is extended by a newline and then
// the new lines. It proves that no blind session was in training, not when
// the blind sessions became known. Its errors name no session and quote no
// line.
func CheckSplitFileExtension(training, final []byte, blind SessionMembership) error {
	trainingSessions, err := ParseSplitFile(training)
	if err != nil {
		return fmt.Errorf("%w: training %w", ErrSplitFileExtension, err)
	}
	finalSessions, err := ParseSplitFile(final)
	if err != nil {
		return fmt.Errorf("%w: final %w", ErrSplitFileExtension, err)
	}
	if len(final) <= len(training) || !bytes.HasPrefix(final, training) {
		return fmt.Errorf("%w: the final split file does not start with the training split file and add to it", ErrSplitFileExtension)
	}
	for id := range trainingSessions {
		if blind.Contains(protocol.OpaqueID(id)) {
			return fmt.Errorf("%w: the training split file already lists a blind-test session", ErrSplitFileExtension)
		}
	}
	added := map[string]Split{}
	for id, split := range finalSessions {
		if _, kept := trainingSessions[id]; !kept {
			added[id] = split
		}
	}
	// One pass per rule, so the error does not depend on map order.
	for _, split := range added {
		if split != SplitTest {
			return fmt.Errorf("%w: the final split file adds a session that is not test", ErrSplitFileExtension)
		}
	}
	for id := range added {
		if !blind.Contains(protocol.OpaqueID(id)) {
			return fmt.Errorf("%w: the final split file adds a session that is not blind-test", ErrSplitFileExtension)
		}
	}
	switch {
	case len(added) == 0:
		return fmt.Errorf("%w: the final split file adds no session", ErrSplitFileExtension)
	case len(added) != blind.Len():
		return fmt.Errorf("%w: the final split file misses a blind-test session", ErrSplitFileExtension)
	}
	return nil
}
