package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

const (
	// ImportSuffix ends an imported session's provenance file, beside
	// its pair in recordings/.
	ImportSuffix = ".import.json"
	// ImportSchemaVersion is the only provenance schema this package
	// writes and reads.
	ImportSchemaVersion = 1
	// maxProvenanceBytes bounds a provenance file read back.
	maxProvenanceBytes = 64 << 10
	// sessionPrefix starts the id of a session imported from an
	// asciicast v2 file; the id goes on with the first 32 hex digits of
	// the file's SHA-256.
	sessionPrefix = "asciicast2-"
	sessionHexLen = 32
)

// ErrBadProvenance means a provenance file is not one an import writes:
// not canonical, an unknown field or version, a field out of range, or
// another session's.
var ErrBadProvenance = errors.New("capture: import provenance is not valid")

// Provenance is <id>.import.json: what an imported session came from and
// who it is attributed to. It never holds the source file's path, which
// can carry user or host names, nor when the import ran, so the same
// source with the same attribution always gives the same bytes.
type Provenance struct {
	SchemaVersion     int64                     `json:"schema_version"`
	Source            Source                    `json:"source"`
	AgentID           protocol.AgentID          `json:"agent_id"`
	Harness           protocol.Harness          `json:"harness"`
	EnvironmentDomain dataset.EnvironmentDomain `json:"environment_domain"`
	Counts            Counts                    `json:"counts"`
}

// Source is the imported file, by format, digest and size.
type Source struct {
	Format    Format          `json:"format"`
	SHA256    protocol.Digest `json:"sha256"`
	SizeBytes int64           `json:"size_bytes"`
}

// Counts are a cast's events: the output and resizes the pair holds, and
// the typed input and markers it was imported without.
type Counts struct {
	Output        int64 `json:"output"`
	Resize        int64 `json:"resize"`
	InputDropped  int64 `json:"input_dropped"`
	MarkerDropped int64 `json:"marker_dropped"`
}

// ImportSessionID is the id of the session imported from the file with
// digest sum: the prefix, then the first 32 hex digits of the SHA-256.
func ImportSessionID(sum protocol.Digest) string {
	hex := string(sum[len("sha256:"):])
	return sessionPrefix + hex[:sessionHexLen]
}

// SessionID is the id of the session p describes.
func (p Provenance) SessionID() string { return ImportSessionID(p.Source.SHA256) }

// importID reports an id only an import gives: a session with such an id
// has its provenance, and a run's session never has the prefix.
func importID(id string) bool { return strings.HasPrefix(id, sessionPrefix) }

func badProvenance(field, why string) error {
	return fmt.Errorf("%w: %s %s", ErrBadProvenance, field, why)
}

// Validate checks every field: the schema version, the one format with an
// import, a digest, a size an import takes, a known agent id, harness and
// environment domain, and counts a session holds.
func (p Provenance) Validate() error {
	c := p.Counts
	switch {
	case p.SchemaVersion != ImportSchemaVersion:
		return badProvenance("schema_version", "is not supported")
	case p.Source.Format != FormatAsciicastV2:
		return badProvenance("source.format", "has no import")
	case !p.Source.SHA256.Valid():
		return badProvenance("source.sha256", "is not a sha256 digest")
	case p.Source.SizeBytes < 1 || p.Source.SizeBytes > MaxCastBytes:
		return badProvenance("source.size_bytes", "is out of range")
	case !p.AgentID.Valid():
		return badProvenance("agent_id", "is not an agent id")
	case !session.ValidHarness(p.Harness):
		return badProvenance("harness", "is unknown")
	case !p.EnvironmentDomain.Valid():
		return badProvenance("environment_domain", "is unknown")
	case c.Output < 0 || c.Resize < 0 || c.InputDropped < 0 || c.MarkerDropped < 0:
		return badProvenance("counts", "are negative")
	case c.Output+c.Resize > session.MaxLoggedEvents-2:
		return badProvenance("counts", "are more events than one session holds")
	}
	return nil
}

// encode is the provenance file's bytes: one JSON object and a newline.
func (p Provenance) encode() ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("capture: encode provenance: %w", err)
	}
	return append(b, '\n'), nil
}

// ParseProvenance reads a provenance file strictly: every field known,
// valid, and the bytes exactly the ones an import writes, so a file
// edited by hand reads as invalid.
func ParseProvenance(data []byte) (Provenance, error) {
	var p Provenance
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(&p) != nil {
		return Provenance{}, badProvenance("file", "is not one provenance object")
	}
	if err := p.Validate(); err != nil {
		return Provenance{}, err
	}
	want, err := p.encode()
	if err != nil {
		return Provenance{}, err
	}
	if !bytes.Equal(data, want) {
		return Provenance{}, badProvenance("file", "is not as an import writes it")
	}
	return p, nil
}

// ReadImport reads session id's provenance from dataDir's recordings dir.
// ok is false when there is none and the id is no import's: the session
// is a run's. An import's id without its provenance fails, and so does a
// file that is there but is a link, a special file, open to others,
// invalid or another session's. The recordings dir is the caller's to
// check.
func ReadImport(dataDir, id string) (p Provenance, ok bool, err error) {
	f, err := session.OpenPrivate(filepath.Join(session.RecordingsDir(dataDir), id+ImportSuffix))
	if errors.Is(err, fs.ErrNotExist) {
		if importID(id) {
			return Provenance{}, true, badProvenance("file", "is missing for an imported session")
		}
		return Provenance{}, false, nil
	}
	if err != nil {
		return Provenance{}, true, fmt.Errorf("capture: provenance: %w", err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxProvenanceBytes+1))
	if err != nil {
		return Provenance{}, true, fmt.Errorf("capture: provenance: %w", err)
	}
	if len(data) > maxProvenanceBytes {
		return Provenance{}, true, badProvenance("file", "is too large")
	}
	p, err = ParseProvenance(data)
	if err != nil {
		return Provenance{}, true, err
	}
	if p.SessionID() != id {
		return Provenance{}, true, badProvenance("source.sha256", "is another session's")
	}
	return p, true, nil
}
