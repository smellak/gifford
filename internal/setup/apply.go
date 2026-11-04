package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const manifestName = "manifest.json"

// ErrNothingToUndo means no setup run is left to undo.
var ErrNothingToUndo = errors.New("no gifford setup run to undo")

type backedFile struct {
	// Seq orders this change among all of the run's changes.
	Seq     int         `json:"seq"`
	Path    string      `json:"path"`
	Existed bool        `json:"existed"`
	Backup  string      `json:"backup,omitempty"`
	Mode    os.FileMode `json:"mode"`
}

type manifest struct {
	CreatedAt time.Time    `json:"created_at"`
	Gifford   string       `json:"gifford"`
	Files     []backedFile `json:"files"`
	// Undo holds the external commands that revert changes made through a
	// harness's own CLI.
	Undo   []undoCommand `json:"undo,omitempty"`
	Undone bool          `json:"undone"`
	Seq    int           `json:"seq"`
}

type undoCommand struct {
	Seq     int      `json:"seq"`
	Command []string `json:"command"`
}

type applyState struct {
	env      Env
	dir      string
	manifest manifest
	backed   map[string]bool
}

func backupRoot(env Env) string { return filepath.Join(env.DataHome, "gifford", "setup-backups") }

// backup copies path into the run's backup before its first change.
func (s *applyState) backup(path string) error {
	if s.backed[path] {
		return nil
	}
	s.manifest.Seq++
	entry := backedFile{Seq: s.manifest.Seq, Path: path, Mode: fileMode(path)}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		entry.Existed = true
		// The backup name is a generated sequence number, never a path.
		entry.Backup = strconv.Itoa(entry.Seq) + ".orig"
		if err := writeFileAtomic(filepath.Join(s.dir, entry.Backup), raw, 0o600); err != nil {
			return fmt.Errorf("back up %s: %w", path, err)
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return fmt.Errorf("read %s for backup: %w", path, err)
	}
	s.manifest.Files = append(s.manifest.Files, entry)
	s.backed[path] = true
	return s.save()
}

func (s *applyState) addUndo(command ...string) error {
	s.manifest.Seq++
	s.manifest.Undo = append(s.manifest.Undo, undoCommand{Seq: s.manifest.Seq, Command: command})
	return s.save()
}

func (s *applyState) save() error {
	raw, err := json.MarshalIndent(s.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode setup manifest: %w", err)
	}
	return writeFileAtomic(filepath.Join(s.dir, manifestName), raw, 0o600)
}

// Apply makes every planned change that is not already in place. A step that
// pre-approves tools runs only when consent agrees. It stops at the first
// failure; the backup made so far stays, so Undo reverts what was done.
func Apply(ctx context.Context, env Env, plan Plan, consent func(Step) bool, w io.Writer) (string, error) {
	root := backupRoot(env)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("create setup backup directory: %w", err)
	}
	dir, err := os.MkdirTemp(root, env.Now().UTC().Format("20060102T150405Z")+"-")
	if err != nil {
		return "", fmt.Errorf("create setup backup: %w", err)
	}
	state := &applyState{
		env: env, dir: dir, backed: map[string]bool{},
		manifest: manifest{CreatedAt: env.Now().UTC(), Gifford: env.Gifford},
	}
	if err := state.save(); err != nil {
		return dir, err
	}
	for _, step := range plan.Steps {
		if step.done {
			continue
		}
		if step.Consent && (consent == nil || !consent(step)) {
			_, _ = fmt.Fprintf(w, "skipped: %s: %s\n", step.Harness, step.Description)
			continue
		}
		for _, file := range step.Files {
			if err := state.backup(file); err != nil {
				return dir, err
			}
		}
		if err := step.apply(ctx, state); err != nil {
			return dir, fmt.Errorf("%s: %s: %w", step.Harness, step.Description, err)
		}
		_, _ = fmt.Fprintf(w, "done: %s: %s\n", step.Harness, step.Description)
	}
	return dir, nil
}

// Undo reverts the latest setup run that is not yet undone, newest change
// first: it restores every backed-up file or removes the ones setup created,
// and runs the harness commands that remove what setup added through them.
func Undo(ctx context.Context, env Env, w io.Writer) (string, error) {
	dir, m, err := latestRun(env)
	if err != nil {
		return "", err
	}
	type revert struct {
		seq int
		do  func() error
	}
	var reverts []revert
	for _, undo := range m.Undo {
		command := undo.Command
		reverts = append(reverts, revert{seq: undo.Seq, do: func() error {
			if out, err := env.Run(ctx, command[0], command[1:]...); err != nil {
				return fmt.Errorf("%v: %w: %s", command, err, firstLine(out))
			}
			_, _ = fmt.Fprintf(w, "reverted: %v\n", command)
			return nil
		}})
	}
	for _, file := range m.Files {
		reverts = append(reverts, revert{seq: file.Seq, do: func() error { return restoreFile(dir, file, w) }})
	}
	sort.SliceStable(reverts, func(i, j int) bool { return reverts[i].seq > reverts[j].seq })
	var failures []error
	for _, step := range reverts {
		if err := step.do(); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return dir, errors.Join(failures...)
	}
	m.Undone = true
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return dir, fmt.Errorf("encode setup manifest: %w", err)
	}
	return dir, writeFileAtomic(filepath.Join(dir, manifestName), raw, 0o600)
}

func restoreFile(dir string, file backedFile, w io.Writer) error {
	if !file.Existed {
		if err := os.Remove(file.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", file.Path, err)
		}
		_, _ = fmt.Fprintf(w, "removed: %s\n", file.Path)
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, file.Backup))
	if err == nil {
		err = writeFileAtomic(file.Path, raw, file.Mode)
	}
	if err != nil {
		return fmt.Errorf("restore %s: %w", file.Path, err)
	}
	_, _ = fmt.Fprintf(w, "restored: %s\n", file.Path)
	return nil
}

func latestRun(env Env) (string, manifest, error) {
	root := backupRoot(env)
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return "", manifest{}, ErrNothingToUndo
	}
	if err != nil {
		return "", manifest{}, fmt.Errorf("read setup backups: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names {
		dir := filepath.Join(root, name)
		raw, err := os.ReadFile(filepath.Join(dir, manifestName))
		if err != nil {
			continue
		}
		var m manifest
		if json.Unmarshal(raw, &m) != nil || m.Undone {
			continue
		}
		return dir, m, nil
	}
	return "", manifest{}, ErrNothingToUndo
}

// writeFileAtomic replaces path through a temporary file in its directory.
func writeFileAtomic(path string, raw []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".gifford-*")
	if err != nil {
		return fmt.Errorf("create temporary file for %s: %w", path, err)
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set mode of %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}
