package eventstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	restoreMarkerVersion = 1
	restoreMarkerSuffix  = ".restore-state"
	maxRestoreMarkerSize = 16 << 10
)

// RestoreResult identifies the durable copy of the database that existed
// immediately before a successful restore. Gifford never removes this copy
// automatically.
type RestoreResult struct {
	// RecoveryPath is never removed automatically.
	RecoveryPath string
}

type restoreMarker struct {
	Version        int    `json:"version"`
	Database       string `json:"database"`
	Replacement    string `json:"replacement"`
	Recovery       string `json:"recovery"`
	BeforeSHA256   string `json:"before_sha256"`
	AfterSHA256    string `json:"after_sha256"`
	RecoverySHA256 string `json:"recovery_sha256"`
}

type restoreStep string

const (
	restoreStepMarkerSynced       restoreStep = "marker_synced"
	restoreStepSidecarsSynced     restoreStep = "sidecars_synced"
	restoreStepCanonicalReplaced  restoreStep = "canonical_replaced"
	restoreStepReplacementSynced  restoreStep = "replacement_synced"
	restoreStepMarkerRemoved      restoreStep = "marker_removed"
	restoreStepMarkerRemoveSynced restoreStep = "marker_remove_synced"
)

type restoreHooks struct {
	afterStep func(restoreStep)
}

// Restore replaces databasePath with a migrated copy of sourcePath. The
// caller must hold the node runtime lock for the complete call. The source is
// opened read-only and all migrations run against a private staging copy.
func Restore(ctx context.Context, sourcePath, databasePath string) (RestoreResult, error) {
	return restore(ctx, sourcePath, databasePath, restoreHooks{})
}

//nolint:gosec // operands are explicit local paths; every swap name is same-directory and marker-validated.
func restore(
	ctx context.Context,
	sourcePath string,
	databasePath string,
	hooks restoreHooks,
) (result RestoreResult, err error) {
	sourcePath = filepath.Clean(sourcePath)
	databasePath = filepath.Clean(databasePath)
	if sourcePath == "." || databasePath == "." || sourcePath == "" || databasePath == "" {
		return result, ErrInvalidArgument
	}
	if err := RecoverInterruptedRestore(ctx, databasePath); err != nil {
		return result, fmt.Errorf("recover previous database restore: %w", err)
	}
	if err := validateRestorePaths(sourcePath, databasePath); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("start database restore: %w", err)
	}

	stagePath, err := copyRegularToTemp(ctx, sourcePath, filepath.Dir(databasePath),
		"."+filepath.Base(databasePath)+".restore-stage-*")
	if err != nil {
		return result, fmt.Errorf("stage restore source: %w", err)
	}
	defer func() {
		err = errors.Join(err, removeTemporaryDatabase(stagePath))
	}()

	replacementPath, err := prepareMigratedReplacement(ctx, stagePath, databasePath)
	if err != nil {
		if replacementPath != "" {
			err = errors.Join(err, removeRegularIfExists(replacementPath))
		}
		return result, err
	}
	cleanupReplacement := true
	defer func() {
		if cleanupReplacement {
			err = errors.Join(err, removeRegularIfExists(replacementPath))
		}
	}()

	prepared, err := prepareRecovery(ctx, databasePath)
	if prepared.path != "" {
		result.RecoveryPath = prepared.path
	}
	if err != nil {
		return result, err
	}
	afterHash, err := hashRegularFile(ctx, replacementPath)
	if err != nil {
		return result, fmt.Errorf("hash restored database: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("prepare database restore: %w", err)
	}

	marker := restoreMarker{
		Version:        restoreMarkerVersion,
		Database:       filepath.Base(databasePath),
		Replacement:    filepath.Base(replacementPath),
		Recovery:       filepath.Base(prepared.path),
		BeforeSHA256:   prepared.databaseHash,
		AfterSHA256:    afterHash,
		RecoverySHA256: prepared.recoveryHash,
	}
	markerPath := restoreMarkerPath(databasePath)
	if err := writeRestoreMarker(markerPath, marker); err != nil {
		return result, fmt.Errorf("publish database restore marker: %w", err)
	}
	// Once the marker is durable, it owns the replacement. Any returned error
	// leaves both in place so the next startup can finish or refuse safely.
	cleanupReplacement = false
	hooks.call(restoreStepMarkerSynced)

	commitCtx := context.WithoutCancel(ctx)
	if err := removeDatabaseSidecars(databasePath); err != nil {
		return result, fmt.Errorf("remove database sidecars: %w", err)
	}
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return result, fmt.Errorf("sync database directory after sidecars: %w", err)
	}
	hooks.call(restoreStepSidecarsSynced)

	if err := replaceDatabaseFile(replacementPath, databasePath); err != nil {
		return result, fmt.Errorf("replace database: %w", err)
	}
	hooks.call(restoreStepCanonicalReplaced)
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return result, fmt.Errorf("sync restored database directory: %w", err)
	}
	hooks.call(restoreStepReplacementSynced)
	if err := requireFileHash(commitCtx, databasePath, marker.AfterSHA256); err != nil {
		return result, fmt.Errorf("verify restored database: %w", err)
	}
	if err := os.Remove(markerPath); err != nil {
		return result, fmt.Errorf("remove database restore marker: %w", err)
	}
	hooks.call(restoreStepMarkerRemoved)
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return result, fmt.Errorf("sync database restore completion: %w", err)
	}
	hooks.call(restoreStepMarkerRemoveSynced)
	return result, nil
}

type preparedRecovery struct {
	path         string
	databaseHash string
	recoveryHash string
}

func prepareMigratedReplacement(ctx context.Context, stagePath, databasePath string) (path string, err error) {
	store, err := Open(ctx, stagePath)
	if err != nil {
		return "", fmt.Errorf("open staged restore database: %w", err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := verifyStoreIntegrity(ctx, store); err != nil {
		return "", fmt.Errorf("verify staged restore database: %w", err)
	}
	if err := checkpointStore(ctx, store); err != nil {
		return "", fmt.Errorf("checkpoint staged restore database: %w", err)
	}
	path, err = reserveAbsentPath(filepath.Dir(databasePath), "."+filepath.Base(databasePath)+".restore-new-*")
	if err != nil {
		return "", fmt.Errorf("reserve restored database path: %w", err)
	}
	if err := store.Backup(ctx, path); err != nil {
		return path, fmt.Errorf("write restored database: %w", err)
	}
	if err := syncRegularFile(path); err != nil {
		return path, fmt.Errorf("sync restored database: %w", err)
	}
	return path, nil
}

func prepareRecovery(ctx context.Context, databasePath string) (prepared preparedRecovery, err error) {
	if _, err := requireRegularFile(databasePath); err != nil {
		return prepared, fmt.Errorf("inspect current database: %w", err)
	}
	pool, err := openDB(ctx, databasePath)
	if err != nil {
		return prepared, fmt.Errorf("open current database: %w", err)
	}
	// The recovery copy represents the database exactly as it existed before
	// restore. Do not run this binary's migrations against the canonical file.
	store := &Store{db: pool}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := checkpointStore(ctx, store); err != nil {
		return prepared, fmt.Errorf("checkpoint current database: %w", err)
	}
	prepared.path, err = reserveAbsentPath(filepath.Dir(databasePath), filepath.Base(databasePath)+".recovery-*")
	if err != nil {
		return prepared, fmt.Errorf("reserve recovery database path: %w", err)
	}
	if err := store.Backup(ctx, prepared.path); err != nil {
		return prepared, fmt.Errorf("write recovery database: %w", err)
	}
	if err := syncRegularFile(prepared.path); err != nil {
		return prepared, fmt.Errorf("sync recovery database: %w", err)
	}
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return prepared, fmt.Errorf("sync recovery database directory: %w", err)
	}
	// The checkpoint above makes the main file a complete pre-restore image.
	// Hashing after it also lets recovery distinguish either side of the swap.
	prepared.databaseHash, err = hashRegularFile(ctx, databasePath)
	if err != nil {
		return prepared, fmt.Errorf("hash current database: %w", err)
	}
	prepared.recoveryHash, err = hashRegularFile(ctx, prepared.path)
	if err != nil {
		return prepared, fmt.Errorf("hash recovery database: %w", err)
	}
	return prepared, nil
}

// RecoverInterruptedRestore resolves a durable restore marker before SQLite is
// opened. The caller must hold the node runtime lock. It either leaves a
// complete pre-restore or post-restore database at databasePath, or returns an
// error while the marker remains so callers cannot create an empty database.
//
//nolint:gosec // databasePath is the explicit local store; marker basenames are validated before joining.
func RecoverInterruptedRestore(ctx context.Context, databasePath string) error {
	if databasePath == "" {
		return ErrInvalidArgument
	}
	databasePath = filepath.Clean(databasePath)
	if databasePath == "." {
		return ErrInvalidArgument
	}
	markerPath := restoreMarkerPath(databasePath)
	marker, err := readRestoreMarker(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read database restore marker: %w", err)
	}
	if err := validateRestoreMarker(marker, filepath.Base(databasePath)); err != nil {
		return fmt.Errorf("validate database restore marker: %w", err)
	}
	dir := filepath.Dir(databasePath)
	replacementPath := filepath.Join(dir, marker.Replacement)
	recoveryPath := filepath.Join(dir, marker.Recovery)
	if err := requireFileHash(ctx, recoveryPath, marker.RecoverySHA256); err != nil {
		return fmt.Errorf("verify recovery database: %w", err)
	}

	databaseHash, databaseExists, err := optionalFileHash(ctx, databasePath)
	if err != nil {
		return fmt.Errorf("inspect database during restore recovery: %w", err)
	}
	replacementHash, replacementExists, err := optionalFileHash(ctx, replacementPath)
	if err != nil {
		return fmt.Errorf("inspect staged database during restore recovery: %w", err)
	}
	if replacementExists && replacementHash != marker.AfterSHA256 {
		return fmt.Errorf("staged database digest does not match restore marker: %w", ErrRestoreRecovery)
	}

	switch {
	case databaseExists && databaseHash == marker.AfterSHA256:
		if replacementExists {
			if err := removeRegularIfExists(replacementPath); err != nil {
				return fmt.Errorf("remove redundant staged database: %w", err)
			}
		}
	case databaseExists && databaseHash == marker.BeforeSHA256:
		if replacementExists {
			if err := completeInterruptedSwap(databasePath, replacementPath, marker.AfterSHA256); err != nil {
				return err
			}
		}
	case !databaseExists && replacementExists:
		if err := completeInterruptedSwap(databasePath, replacementPath, marker.AfterSHA256); err != nil {
			return err
		}
	case !databaseExists:
		copyPath, err := copyRegularToTemp(ctx, recoveryPath, dir,
			"."+filepath.Base(databasePath)+".restore-recover-*")
		if err != nil {
			return fmt.Errorf("stage recovery database: %w", err)
		}
		removeCopy := true
		defer func() {
			if removeCopy {
				_ = removeRegularIfExists(copyPath)
			}
		}()
		if err := completeInterruptedSwap(databasePath, copyPath, marker.RecoverySHA256); err != nil {
			return err
		}
		removeCopy = false
	default:
		return fmt.Errorf("database digest does not match either side of restore: %w", ErrRestoreRecovery)
	}

	if err := os.Remove(markerPath); err != nil {
		return fmt.Errorf("remove recovered database restore marker: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("sync recovered database directory: %w", err)
	}
	return nil
}

func completeInterruptedSwap(databasePath, replacementPath, wantHash string) error {
	if err := removeDatabaseSidecars(databasePath); err != nil {
		return fmt.Errorf("remove database sidecars during recovery: %w", err)
	}
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return fmt.Errorf("sync database sidecars during recovery: %w", err)
	}
	if err := replaceDatabaseFile(replacementPath, databasePath); err != nil {
		return fmt.Errorf("replace database during recovery: %w", err)
	}
	if err := syncDirectory(filepath.Dir(databasePath)); err != nil {
		return fmt.Errorf("sync database replacement during recovery: %w", err)
	}
	if err := requireFileHash(context.Background(), databasePath, wantHash); err != nil {
		return fmt.Errorf("verify database replacement during recovery: %w", err)
	}
	return nil
}

func validateRestorePaths(sourcePath, databasePath string) error {
	if sourcePath == "." || databasePath == "." || sourcePath == "" || databasePath == "" {
		return ErrInvalidArgument
	}
	sourceInfo, err := requireRegularFile(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect restore source: %w", err)
	}
	databaseInfo, err := requireRegularFile(databasePath)
	if err != nil {
		return fmt.Errorf("inspect current database: %w", err)
	}
	if os.SameFile(sourceInfo, databaseInfo) {
		return fmt.Errorf("restore source is the current database: %w", ErrInvalidArgument)
	}
	return nil
}

func checkpointStore(ctx context.Context, store *Store) error {
	var busy, logFrames, checkpointedFrames int
	if err := store.DB().QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		return fmt.Errorf("checkpoint WAL: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("checkpoint WAL remains busy: %w", ErrRestoreRecovery)
	}
	return nil
}

func verifyStoreIntegrity(ctx context.Context, store *Store) error {
	var result string
	if err := store.DB().QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("run SQLite integrity check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("SQLite integrity check returned %q: %w", result, ErrRestoreRecovery)
	}
	return nil
}

func restoreMarkerPath(databasePath string) string {
	return databasePath + restoreMarkerSuffix
}

//nolint:gosec // path is the fixed marker beside a validated canonical database path.
func writeRestoreMarker(path string, marker restoreMarker) (err error) {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("restore marker already exists: %w", ErrRestoreRecovery)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect restore marker: %w", err)
	}
	raw, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode restore marker: %w", err)
	}
	raw = append(raw, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create restore marker temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath != "" {
			err = errors.Join(err, removeRegularIfExists(temporaryPath))
		}
	}()
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write restore marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync restore marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close restore marker: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("publish restore marker: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove restore marker temporary file: %w", err)
	}
	temporaryPath = ""
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync restore marker directory: %w", err)
	}
	return nil
}

//nolint:gosec // path is the fixed marker beside the canonical database path.
func readRestoreMarker(path string) (restoreMarker, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return restoreMarker{}, fmt.Errorf("stat restore marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return restoreMarker{}, fmt.Errorf("restore marker is not a regular file: %w", ErrRestoreRecovery)
	}
	if info.Size() > maxRestoreMarkerSize {
		return restoreMarker{}, fmt.Errorf("restore marker is too large: %w", ErrRestoreRecovery)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return restoreMarker{}, fmt.Errorf("read restore marker: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var marker restoreMarker
	if err := decoder.Decode(&marker); err != nil {
		return restoreMarker{}, fmt.Errorf("decode restore marker: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return restoreMarker{}, err
	}
	return marker, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("restore marker contains multiple values: %w", ErrRestoreRecovery)
		}
		return fmt.Errorf("decode restore marker trailing data: %w", err)
	}
	return nil
}

func validateRestoreMarker(marker restoreMarker, databaseName string) error {
	if marker.Version != restoreMarkerVersion || marker.Database != databaseName {
		return ErrRestoreRecovery
	}
	for _, name := range []string{marker.Database, marker.Replacement, marker.Recovery} {
		if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
			return ErrRestoreRecovery
		}
	}
	if marker.Database == marker.Replacement || marker.Database == marker.Recovery ||
		marker.Replacement == marker.Recovery ||
		!strings.HasPrefix(marker.Replacement, "."+databaseName+".restore-new-") ||
		!strings.HasPrefix(marker.Recovery, databaseName+".recovery-") {
		return ErrRestoreRecovery
	}
	for _, digest := range []string{marker.BeforeSHA256, marker.AfterSHA256, marker.RecoverySHA256} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
			return ErrRestoreRecovery
		}
	}
	return nil
}

//nolint:gosec // dir is the canonical database directory and the name comes from os.CreateTemp.
func reserveAbsentPath(dir, pattern string) (string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", fmt.Errorf("reserve temporary name: %w", err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close reserved temporary name: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("release reserved temporary name: %w", err)
	}
	return path, nil
}

//nolint:gosec // source is an explicit operand; destination is private and created by os.CreateTemp.
func copyRegularToTemp(ctx context.Context, sourcePath, dir, pattern string) (path string, err error) {
	if _, err := requireRegularFile(sourcePath); err != nil {
		return "", err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open source file: %w", err)
	}
	destination, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", errors.Join(fmt.Errorf("create destination temporary file: %w", err), source.Close())
	}
	path = destination.Name()
	destinationClosed := false
	defer func() {
		err = errors.Join(err, source.Close())
		if err != nil {
			if !destinationClosed {
				err = errors.Join(err, destination.Close())
			}
			err = errors.Join(err, removeRegularIfExists(path))
		}
	}()
	if _, err := io.CopyBuffer(destination, contextReader{ctx: ctx, reader: source}, make([]byte, 128<<10)); err != nil {
		return path, fmt.Errorf("copy source file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return path, fmt.Errorf("copy source file: %w", err)
	}
	if err := destination.Chmod(0o600); err != nil {
		return path, fmt.Errorf("set copied file permissions: %w", err)
	}
	if err := destination.Sync(); err != nil {
		return path, fmt.Errorf("sync copied file: %w", err)
	}
	if err := destination.Close(); err != nil {
		return path, fmt.Errorf("close copied file: %w", err)
	}
	destinationClosed = true
	return path, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, fmt.Errorf("read context: %w", err)
	}
	n, err := r.reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("read file: %w", err)
	}
	return n, err //nolint:wrapcheck // io.Copy requires the exact io.EOF sentinel.
}

//nolint:gosec // callers pass the canonical path or a validated same-directory marker basename.
func optionalFileHash(ctx context.Context, path string) (digest string, exists bool, err error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	} else if err != nil {
		return "", false, fmt.Errorf("stat optional database file: %w", err)
	}
	digest, err = hashRegularFile(ctx, path)
	return digest, true, err
}

func requireFileHash(ctx context.Context, path, want string) error {
	got, err := hashRegularFile(ctx, path)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("file digest mismatch: %w", ErrRestoreRecovery)
	}
	return nil
}

//nolint:gosec // callers validate that path is an explicit or marker-bound regular file.
func hashRegularFile(ctx context.Context, path string) (digest string, err error) {
	if _, err := requireRegularFile(path); err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for hashing: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()
	hasher := sha256.New()
	if _, err := io.CopyBuffer(hasher, contextReader{ctx: ctx, reader: file}, make([]byte, 128<<10)); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

//nolint:gosec // inspecting an explicit maintenance operand is the security boundary here.
func requireRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat regular file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path is not a regular file: %w", ErrInvalidArgument)
	}
	return info, nil
}

//nolint:gosec // path was created privately or validated as the canonical regular file.
func syncRegularFile(path string) (err error) {
	if _, err := requireRegularFile(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open file for sync: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}
	return nil
}

func removeDatabaseSidecars(databasePath string) error {
	return errors.Join(
		removeRegularIfExists(databasePath+"-wal"),
		removeRegularIfExists(databasePath+"-shm"),
	)
}

func removeTemporaryDatabase(databasePath string) error {
	return errors.Join(removeDatabaseSidecars(databasePath), removeRegularIfExists(databasePath))
}

//nolint:gosec // callers pass fixed sidecars, private temporaries, or validated marker basenames.
func removeRegularIfExists(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect file for removal: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refuse to remove non-regular file: %w", ErrRestoreRecovery)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove file: %w", err)
	}
	return nil
}

func (h restoreHooks) call(step restoreStep) {
	if h.afterStep != nil {
		h.afterStep(step)
	}
}
