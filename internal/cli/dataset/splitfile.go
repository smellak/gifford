package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// maxSplitFileBytes bounds a split file: one short line per session.
const maxSplitFileBytes = 1 << 20

// splitLine is one line of a split file.
type splitLine struct {
	Session string        `json:"session"`
	Split   dataset.Split `json:"split"`
}

// splitterFor is split-v1 when path is empty. Otherwise it is the split
// file's table, by session group, with the file's digest for the
// manifest. Every session of the data dir must be in the file.
func splitterFor(path, dataDir string) (dataset.Splitter, *protocol.Digest, error) {
	if path == "" {
		return dataset.SplitV1{}, nil, nil
	}
	sessions, digest, err := readSplitFile(path)
	if err != nil {
		return nil, nil, buildUsage(err.Error())
	}
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	table := make(dataset.SplitTable, len(ids))
	for _, id := range ids {
		split, ok := sessions[id]
		if !ok {
			return nil, nil, buildUsage("session " + id + " is not in the split file")
		}
		table[replay.GroupDigest(id)] = split
	}
	return table, &digest, nil
}

// readSplitFile reads a split file: one JSON object per line,
// {"session":"<id>","split":"validation"} or "test", each session once.
// It has no train: the file splits the tuning half from the sealed one.
// Blank lines are skipped. It returns each session's split and the
// SHA-256 of the file's bytes. Its errors name a line by number, never
// its text.
func readSplitFile(path string) (map[string]dataset.Split, protocol.Digest, error) {
	// The errors name the file, never the path to it.
	f, err := os.Open(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return nil, "", fmt.Errorf("split file %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxSplitFileBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("split file %s: read failed", filepath.Base(path))
	}
	if len(raw) > maxSplitFileBytes {
		return nil, "", errors.New("split file: larger than 1 MiB")
	}
	sessions := map[string]dataset.Split{}
	for i, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var l splitLine
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if dec.Decode(&l) != nil || !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
			return nil, "", fmt.Errorf("split file line %d: not one object with session and split", i+1)
		}
		switch {
		case !protocol.OpaqueID(l.Session).Valid():
			return nil, "", fmt.Errorf("split file line %d: not a session id", i+1)
		case l.Split != dataset.SplitValidation && l.Split != dataset.SplitTest:
			return nil, "", fmt.Errorf("split file line %d: split is not validation or test", i+1)
		}
		if _, dup := sessions[l.Session]; dup {
			return nil, "", fmt.Errorf("split file line %d: session listed twice", i+1)
		}
		sessions[l.Session] = l.Split
	}
	if len(sessions) == 0 {
		return nil, "", errors.New("split file: no session")
	}
	sum := sha256.Sum256(raw)
	return sessions, protocol.Digest("sha256:" + hex.EncodeToString(sum[:])), nil
}
