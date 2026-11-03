package dataset

import (
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

// splitterFor is split-v1 when path is empty. Otherwise it is the split
// file's table, by session group, with the file's digest for the manifest
// and its exact bytes for the dataset's split-file.jsonl. Every session of
// the data dir must be in the file.
func splitterFor(path, dataDir string) (dataset.Splitter, *protocol.Digest, []byte, error) {
	if path == "" {
		return dataset.SplitV1{}, nil, nil, nil
	}
	sessions, digest, raw, err := readSplitFile(path)
	if err != nil {
		return nil, nil, nil, buildUsage(err.Error())
	}
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	table := make(dataset.SplitTable, len(ids))
	for _, id := range ids {
		split, ok := sessions[id]
		if !ok {
			return nil, nil, nil, buildUsage("session " + id + " is not in the split file")
		}
		table[replay.GroupDigest(id)] = split
	}
	return table, &digest, raw, nil
}

// readSplitFile reads a split file once and parses it with
// dataset.ParseSplitFile. It returns each session's split, the SHA-256 of
// the file's bytes and the bytes themselves, so the dataset keeps exactly
// what was hashed. Its errors name a line by number, never its text.
func readSplitFile(path string) (map[string]dataset.Split, protocol.Digest, []byte, error) {
	// The errors name the file, never the path to it.
	f, err := os.Open(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return nil, "", nil, fmt.Errorf("split file %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, dataset.MaxSplitFileBytes+1))
	if err != nil {
		return nil, "", nil, fmt.Errorf("split file %s: read failed", filepath.Base(path))
	}
	sessions, err := dataset.ParseSplitFile(raw)
	if err != nil {
		return nil, "", nil, err //nolint:wrapcheck // ParseSplitFile names the split file and the line.
	}
	return sessions, dataset.SplitFileDigest(raw), raw, nil
}
