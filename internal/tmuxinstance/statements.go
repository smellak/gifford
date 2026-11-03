package tmuxinstance

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

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	statementsSchemaVersion = 1
	maxStatementsBytes      = 1 << 20
	statementSourcePrefix   = "statement:"
)

type statementsFile struct {
	SchemaVersion int         `yaml:"schema_version"`
	Instances     []statement `yaml:"instances"`
}

// statement is one entry of the file. Generation starts at 1: the
// administrator bumps it, and changes nothing else, to register a statement
// again after its instance was invalidated.
type statement struct {
	AgentID        protocol.AgentID    `yaml:"agent_id" json:"agent_id"`
	AgentUser      string              `yaml:"agent_user" json:"agent_user"`
	OwnerUser      string              `yaml:"owner_user" json:"owner_user"`
	Socket         string              `yaml:"socket" json:"socket"`
	PaneID         protocol.TmuxPaneID `yaml:"pane_id" json:"pane_id"`
	PaneComm       string              `yaml:"pane_comm" json:"pane_comm"`
	HarnessComm    string              `yaml:"harness_comm" json:"harness_comm"`
	HarnessArgv0   string              `yaml:"harness_argv0" json:"harness_argv0"`
	HarnessArgv1   string              `yaml:"harness_argv1" json:"harness_argv1"`
	Harness        protocol.Harness    `yaml:"harness" json:"harness"`
	HarnessVersion string              `yaml:"harness_version" json:"harness_version"`
	// MonitorProfile opts the instance into a named closed launcher chain
	// (the sudo launcher monitor). Absent means the default single-launcher
	// shape.
	MonitorProfile string `yaml:"monitor_profile" json:"monitor_profile"`
	Generation     uint32 `yaml:"generation" json:"generation"`
}

// LoadStatements reads the administrator's statements. The file must be a
// regular file, and it and its directory must belong to root or to this
// process's uid and be writable by nobody else. A missing file returns an
// error wrapping fs.ErrNotExist: the caller leaves the registry as it is. An
// empty list is a statement too, and Reconcile revokes every instance for it.
func LoadStatements(path string) ([]Registration, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("instance statements: %w", err)
	}
	dir, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("instance statements: %w", err)
	}
	owner, fileOK := trusted(fi)
	_, dirOK := trusted(dir)
	if !fi.Mode().IsRegular() || !fileOK || !dir.IsDir() || !dirOK {
		return nil, fmt.Errorf("instance statements %s: the file and its directory must belong to root or the node "+
			"and be writable only by their owner: %w", path, ErrInvalidArgument)
	}
	raw, err := readBounded(path, fi)
	if err != nil {
		return nil, err
	}
	var file statementsFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("instance statements %s: %w: %w", path, ErrInvalidArgument, err)
	}
	if file.SchemaVersion != statementsSchemaVersion {
		return nil, fmt.Errorf("instance statements %s: schema_version must be %d: %w",
			path, statementsSchemaVersion, ErrInvalidArgument)
	}
	agents := make(map[protocol.AgentID]struct{}, len(file.Instances))
	panes := make(map[string]struct{}, len(file.Instances))
	out := make([]Registration, 0, len(file.Instances))
	for i, entry := range file.Instances {
		if entry.Generation < 1 {
			return nil, fmt.Errorf("instance statements %s: entry %d: generation must be at least 1: %w",
				path, i, ErrInvalidArgument)
		}
		reg, err := entry.registration(owner)
		if err != nil {
			return nil, fmt.Errorf("instance statements %s: entry %d: %w", path, i, err)
		}
		if err := reg.validate(); err != nil {
			return nil, fmt.Errorf("instance statements %s: entry %d: %w", path, i, err)
		}
		pane := reg.Socket + " " + string(reg.PaneID)
		_, dupAgent := agents[reg.AgentID]
		_, dupPane := panes[pane]
		if dupAgent || dupPane {
			return nil, fmt.Errorf("instance statements %s: entry %d repeats an agent or a pane: %w",
				path, i, ErrInvalidArgument)
		}
		agents[reg.AgentID], panes[pane] = struct{}{}, struct{}{}
		out = append(out, reg)
	}
	return out, nil
}

// trusted reports the entry's owner, and whether that owner is root or this
// process's uid and nobody else may write the entry.
func trusted(fi fs.FileInfo) (uint32, bool) {
	owner, ok := fileOwner(fi)
	return owner, ok && (owner == 0 || int64(owner) == int64(os.Geteuid())) && fi.Mode().Perm()&0o022 == 0
}

// readBounded reads the file checked as fi, and refuses if path names
// another file by the time it is opened.
func readBounded(path string, fi fs.FileInfo) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("instance statements: %w", err)
	}
	defer func() { _ = f.Close() }()
	opened, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("instance statements: %w", err)
	}
	if !os.SameFile(fi, opened) {
		return nil, fmt.Errorf("instance statements %s: replaced while it was read: %w", path, ErrInvalidArgument)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxStatementsBytes+1))
	if err != nil {
		return nil, fmt.Errorf("instance statements: %w", err)
	}
	if len(raw) > maxStatementsBytes {
		return nil, fmt.Errorf("instance statements %s: larger than %d bytes: %w", path, maxStatementsBytes, ErrInvalidArgument)
	}
	return raw, nil
}

// registration turns the entry into a Registration whose Source is the
// digest of the whole entry, so a changed or bumped statement is a new one.
func (s statement) registration(owner uint32) (Registration, error) {
	canonical, err := json.Marshal(s)
	if err != nil {
		return Registration{}, fmt.Errorf("encode statement: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return Registration{
		AgentID: s.AgentID, AgentUser: s.AgentUser, OwnerUser: s.OwnerUser, Socket: s.Socket,
		PaneID: s.PaneID, PaneComm: s.PaneComm, HarnessComm: s.HarnessComm,
		HarnessArgv0: s.HarnessArgv0, HarnessArgv1: s.HarnessArgv1,
		Harness: s.Harness, HarnessVersion: s.HarnessVersion,
		MonitorProfile: s.MonitorProfile,
		Source:         statementSourcePrefix + hex.EncodeToString(digest[:]),
		RegisteredBy:   owner,
	}, nil
}
