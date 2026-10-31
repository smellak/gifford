package hub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/protocol"
)

func (s *Service) artifactStoreRoot() string {
	return filepath.Join(s.dataDir, "artifacts", "store")
}

func (s *Service) artifactDirectory(id protocol.OpaqueID) (string, error) {
	if !id.Valid() {
		return "", fmt.Errorf("invalid artifact id")
	}
	return filepath.Join(s.artifactStoreRoot(), string(id)), nil
}

func (s *Service) artifactChunkPath(id protocol.OpaqueID, index int64) (string, error) {
	if index < 0 || index > 99_999_999 {
		return "", fmt.Errorf("invalid artifact chunk index")
	}
	dir, err := s.artifactDirectory(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("%08d.chunk", index)), nil
}

func (s *Service) ensureArtifactDirectory(id protocol.OpaqueID) (string, error) {
	artifacts := filepath.Join(s.dataDir, "artifacts")
	store := filepath.Join(artifacts, "store")
	dir, err := s.artifactDirectory(id)
	if err != nil {
		return "", err
	}
	for _, path := range []string{artifacts, store, dir} {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("create artifact directory: %w", err)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("artifact path is not a directory")
		}
		//nolint:gosec // these are directories and require the execute bit.
		if err := os.Chmod(path, 0o700); err != nil {
			return "", fmt.Errorf("secure artifact directory: %w", err)
		}
	}
	return dir, nil
}

func (s *Service) writeArtifactChunk(
	id protocol.OpaqueID,
	index int64,
	content []byte,
) (string, error) {
	dir, err := s.ensureArtifactDirectory(id)
	if err != nil {
		return "", err
	}
	final, err := s.artifactChunkPath(id, index)
	if err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(dir, ".chunk-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create artifact chunk temporary: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return "", fmt.Errorf("secure artifact chunk temporary: %w", err)
	}
	if _, err := io.Copy(temporary, bytesReader(content)); err != nil {
		cleanup()
		return "", fmt.Errorf("write artifact chunk: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return "", fmt.Errorf("sync artifact chunk: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return "", fmt.Errorf("close artifact chunk: %w", err)
	}
	if err := os.Rename(temporaryPath, final); err != nil {
		_ = os.Remove(temporaryPath)
		return "", fmt.Errorf("publish artifact chunk: %w", err)
	}
	if err := syncArtifactDirectory(dir); err != nil {
		return "", err
	}
	return final, nil
}

// byteReader avoids retaining any string conversion of ciphertext.
type byteReader struct {
	value  []byte
	offset int
}

func bytesReader(value []byte) *byteReader { return &byteReader{value: value} }

func (r *byteReader) Read(dst []byte) (int, error) {
	if r.offset == len(r.value) {
		return 0, io.EOF
	}
	n := copy(dst, r.value[r.offset:])
	r.offset += n
	return n, nil
}

func syncArtifactDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open artifact directory: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync artifact directory: %w", err)
	}
	return nil
}

func (s *Service) removeArtifactBlobs(id protocol.OpaqueID) error {
	dir, err := s.artifactDirectory(id)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove acknowledged artifact blobs: %w", err)
	}
	return syncArtifactDirectory(s.artifactStoreRoot())
}

// ReconcileArtifacts repairs interrupted uploads before hub.sock starts. It
// removes invalid RESERVED/UPLOADING rows and every file without a row.
func (s *Service) ReconcileArtifacts(ctx context.Context) error {
	if s.dataDir == "" {
		return nil
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	root := s.artifactStoreRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create artifact store: %w", err)
	}
	for _, path := range []string{filepath.Join(s.dataDir, "artifacts"), root} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact store path is not a directory")
		}
		//nolint:gosec // these are directories and require the execute bit.
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure artifact store: %w", err)
		}
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	type storedChunk struct {
		id     protocol.OpaqueID
		index  int64
		size   int64
		digest protocol.SHA256Hex
		path   string
		state  protocol.ArtifactState
	}
	rows, err := tx.QueryContext(ctx, `SELECT c.artifact_id, c.chunk_index,
		c.ciphertext_size, c.ciphertext_sha256, c.storage_path, t.state
		FROM artifact_chunks c JOIN artifact_transfers t USING (artifact_id)`)
	if err != nil {
		return fmt.Errorf("select artifact reconciliation rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var chunks []storedChunk
	for rows.Next() {
		var chunk storedChunk
		if err := rows.Scan(&chunk.id, &chunk.index, &chunk.size, &chunk.digest,
			&chunk.path, &chunk.state); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan artifact reconciliation row: %w", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read artifact reconciliation rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close artifact reconciliation rows: %w", err)
	}
	expected := make(map[string]bool, len(chunks))
	for _, chunk := range chunks {
		wantPath, pathErr := s.artifactChunkPath(chunk.id, chunk.index)
		validPath := pathErr == nil && filepath.Clean(chunk.path) == filepath.Clean(wantPath)
		content, readErr := os.ReadFile(wantPath)
		validFile := readErr == nil && int64(len(content)) == chunk.size &&
			artifactformat.Digest(content) == chunk.digest
		interruptible := chunk.state == protocol.ArtifactStateReserved ||
			chunk.state == protocol.ArtifactStateUploading
		if interruptible && (!validPath || !validFile) {
			if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_chunks
				WHERE artifact_id = ? AND chunk_index = ?`, chunk.id, chunk.index); err != nil {
				return fmt.Errorf("delete invalid artifact chunk row: %w", err)
			}
			if pathErr == nil {
				_ = os.Remove(wantPath)
			}
			continue
		}
		if pathErr == nil {
			expected[filepath.Clean(wantPath)] = true
		}
	}
	if err := commit(tx); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read artifact store: %w", err)
	}
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name())
		if !entry.IsDir() {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("remove artifact store orphan: %w", err)
			}
			continue
		}
		files, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("read artifact directory: %w", err)
		}
		for _, file := range files {
			filePath := filepath.Clean(filepath.Join(path, file.Name()))
			if file.IsDir() || !expected[filePath] {
				if err := os.RemoveAll(filePath); err != nil {
					return fmt.Errorf("remove orphan artifact file: %w", err)
				}
			}
		}
		remaining, err := os.ReadDir(path)
		if err == nil && len(remaining) == 0 {
			_ = os.Remove(path)
		}
	}
	return syncArtifactDirectory(root)
}
