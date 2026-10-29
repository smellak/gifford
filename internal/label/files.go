package label

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Label file names, under Dir.
const (
	HooksFile       = "hooks.jsonl"
	AnnotationsFile = "annotations.jsonl"
)

const (
	dirPerm  = os.FileMode(0o700)
	filePerm = os.FileMode(0o600)
)

// ErrUnsafeFile means a label file or its directory is open to others.
var ErrUnsafeFile = errors.New("label file is not private")

// Dir is the labels directory of a data directory.
func Dir(dataDir string) string { return filepath.Join(dataDir, "labels") }

// appendLine appends one line to dir/name in a single write, so lines
// from concurrent hooks never interleave. It creates the directory 0700
// and the file 0600, and refuses either if it is open to others.
func appendLine(dir, name string, line []byte) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, filePerm)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	info, err := f.Stat()
	if err == nil && info.Mode().Perm()&0o077 != 0 {
		err = ErrUnsafeFile
	}
	if err == nil {
		_, err = f.Write(line)
	}
	return errors.Join(err, f.Close())
}

// AlignedFile holds the labels align derived, under Dir. Each run
// replaces it.
const AlignedFile = "aligned.jsonl"

// maxLineBytes bounds one line read back from a label file.
const maxLineBytes = 2 << 20

// privateDir creates dir 0700 and refuses it if it is open to others.
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("labels dir: %w", err)
	}
	if info, err := os.Stat(dir); err != nil {
		return fmt.Errorf("labels dir: %w", err)
	} else if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("labels dir: %w", ErrUnsafeFile)
	}
	return nil
}

// replaceFile writes dir/name whole through a 0600 temporary file and a
// rename, so a reader never sees half a file.
func replaceFile(dir, name string, data []byte) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+name+".*")
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	err = errors.Join(err, tmp.Close())
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(dir, name))
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

// readLines calls fn for each non-blank line of dir/name, and counts the
// lines fn refuses. A missing file has no lines.
func readLines(dir, name string, fn func([]byte) error) (bad int, err error) {
	f, err := os.Open(filepath.Join(dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if fn(line) != nil {
			bad++
		}
	}
	if err := sc.Err(); err != nil {
		return bad, fmt.Errorf("read %s: %w", name, err)
	}
	return bad, nil
}
