package dataset

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/dataset"
	coreeval "github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// maxPermissionEvidenceLegacyBytes bounds the canonical list just as the
// evaluation session lists are bounded. It is ample for thousands of bounded
// opaque ids and prevents an input file from becoming unbounded policy state.
const maxPermissionEvidenceLegacyBytes = 1 << 20

func loadPermissionEvidenceLegacy(
	path, dataDir string,
) (map[protocol.OpaqueID]struct{}, *dataset.PermissionEvidenceLegacyPolicy, error) {
	if path == "" {
		return nil, nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return nil, nil, buildUsage(fmt.Sprintf("permission evidence legacy list %s: %v", filepath.Base(path), err))
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxPermissionEvidenceLegacyBytes+1))
	if err != nil {
		return nil, nil, buildUsage("permission evidence legacy list: read failed")
	}
	if len(raw) > maxPermissionEvidenceLegacyBytes {
		return nil, nil, buildUsage("permission evidence legacy list: larger than 1 MiB")
	}
	set, err := coreeval.ParseSessionSet(raw)
	if err != nil {
		return nil, nil, buildUsage("permission evidence legacy list must be lexicographic canonical JSONL: " + err.Error())
	}
	if set.Len() > dataset.MaxPipelineWindows {
		return nil, nil, buildUsage("permission evidence legacy list has too many sessions")
	}
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: list permission evidence legacy sessions: %w", err)
	}
	legacy := make(map[protocol.OpaqueID]struct{}, set.Len())
	for _, id := range ids {
		opaque := protocol.OpaqueID(id)
		if set.Contains(opaque) {
			legacy[opaque] = struct{}{}
		}
	}
	if len(legacy) != set.Len() {
		return nil, nil, buildUsage("permission evidence legacy list contains a session absent from the data dir")
	}
	policy := &dataset.PermissionEvidenceLegacyPolicy{
		SHA256: set.Digest(), Sessions: int64(set.Len()),
	}
	return legacy, policy, nil
}
