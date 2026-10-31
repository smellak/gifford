package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/protocol"
)

const outgoingRecordVersion = 1

type outgoingRecord struct {
	Version    int                            `json:"version"`
	ArtifactID protocol.OpaqueID              `json:"artifact_id"`
	Sender     protocol.AgentID               `json:"sender"`
	Reserve    protocol.ReserveArtifactParams `json:"reserve"`
}

func (r outgoingRecord) validate() error {
	if r.Version != outgoingRecordVersion || !r.ArtifactID.Valid() || !r.Sender.Valid() {
		return fmt.Errorf("invalid outgoing artifact record")
	}
	if err := r.Reserve.Validate(); err != nil {
		return fmt.Errorf("invalid outgoing artifact reservation: %w", err)
	}
	derived, err := artifactformat.DeriveArtifactID(r.Sender, r.Reserve.ClientMsgID)
	if err != nil || derived != r.ArtifactID {
		return fmt.Errorf("outgoing artifact identity mismatch")
	}
	return nil
}

func outgoingDirectory(dataDir string) string {
	return filepath.Join(dataDir, "artifacts", "outgoing")
}

func outgoingRecordPath(dataDir string, id protocol.OpaqueID) string {
	return filepath.Join(outgoingDirectory(dataDir), string(id)+".json")
}

func outgoingKeyPath(dataDir string, id protocol.OpaqueID) string {
	return filepath.Join(outgoingDirectory(dataDir), string(id)+".key")
}

func ensureOutgoingDirectory(dataDir string) error {
	artifacts := filepath.Join(dataDir, "artifacts")
	dir := outgoingDirectory(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create outgoing artifact directory: %w", err)
	}
	for _, path := range []string{artifacts, dir} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("outgoing artifact path is not a private directory")
		}
		//nolint:gosec // these are directories and require the execute bit.
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure outgoing artifact directory: %w", err)
		}
	}
	return nil
}

func loadOutgoingRecord(dataDir string, id protocol.OpaqueID) (outgoingRecord, bool, error) {
	path := outgoingRecordPath(dataDir, id)
	if err := checkPrivateFile(path); errors.Is(err, fs.ErrNotExist) {
		return outgoingRecord{}, false, nil
	} else if err != nil {
		return outgoingRecord{}, false, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return outgoingRecord{}, false, fmt.Errorf("read outgoing artifact record: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var record outgoingRecord
	if err := decoder.Decode(&record); err != nil {
		return outgoingRecord{}, false, fmt.Errorf("decode outgoing artifact record: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return outgoingRecord{}, false, fmt.Errorf("decode outgoing artifact record: trailing data")
	}
	if err := record.validate(); err != nil {
		return outgoingRecord{}, false, err
	}
	return record, true, nil
}

func saveOutgoingRecord(dataDir string, record outgoingRecord) error {
	if err := record.validate(); err != nil {
		return err
	}
	if err := ensureOutgoingDirectory(dataDir); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode outgoing artifact record: %w", err)
	}
	created, err := writePrivateOnce(outgoingRecordPath(dataDir, record.ArtifactID), raw)
	if err != nil {
		return err
	}
	if !created {
		stored, found, err := loadOutgoingRecord(dataDir, record.ArtifactID)
		if err != nil || !found {
			return fmt.Errorf("reload outgoing artifact record: %w", err)
		}
		storedJSON, _ := json.Marshal(stored)
		if !bytes.Equal(storedJSON, raw) {
			return fmt.Errorf("outgoing artifact reservation conflicts with durable record")
		}
	}
	return nil
}

func loadContentKey(dataDir string, id protocol.OpaqueID) ([]byte, bool, error) {
	path := outgoingKeyPath(dataDir, id)
	if err := checkPrivateFile(path); errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("read outgoing artifact key: %w", err)
	}
	if len(key) != 32 {
		return nil, false, fmt.Errorf("outgoing artifact key has invalid size")
	}
	return key, true, nil
}

func loadOrCreateContentKey(dataDir string, id protocol.OpaqueID) ([]byte, error) {
	if key, found, err := loadContentKey(dataDir, id); err != nil || found {
		return key, err
	}
	if err := ensureOutgoingDirectory(dataDir); err != nil {
		return nil, err
	}
	key, err := artifactformat.NewContentKey()
	if err != nil {
		return nil, fmt.Errorf("generate outgoing artifact key: %w", err)
	}
	created, err := writePrivateOnce(outgoingKeyPath(dataDir, id), key)
	if err != nil {
		return nil, err
	}
	if !created {
		stored, found, err := loadContentKey(dataDir, id)
		if err != nil || !found {
			return nil, fmt.Errorf("load raced outgoing artifact key: %w", err)
		}
		return stored, nil
	}
	return key, nil
}

func removeContentKey(dataDir string, id protocol.OpaqueID) error {
	path := outgoingKeyPath(dataDir, id)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove outgoing artifact key: %w", err)
	}
	return syncDirectory(outgoingDirectory(dataDir))
}

func writePrivateOnce(path string, content []byte) (bool, error) {
	dir := filepath.Dir(path)
	temporary, err := os.CreateTemp(dir, ".artifact-*.tmp")
	if err != nil {
		return false, fmt.Errorf("create private temporary: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return false, fmt.Errorf("secure private temporary: %w", err)
	}
	for written := 0; written < len(content); {
		n, err := temporary.Write(content[written:])
		if err != nil || n == 0 {
			cleanup()
			if err == nil {
				err = io.ErrShortWrite
			}
			return false, fmt.Errorf("write private temporary: %w", err)
		}
		written += n
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return false, fmt.Errorf("sync private temporary: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return false, fmt.Errorf("close private temporary: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("publish private file: %w", err)
	}
	_ = os.Remove(temporaryPath)
	if err := syncDirectory(dir); err != nil {
		return false, err
	}
	return true, nil
}

func checkPrivateFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect artifact private file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("artifact private file has insecure type or mode")
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open artifact directory for sync: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync artifact directory: %w", err)
	}
	return nil
}
