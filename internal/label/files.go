package label

import (
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
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("labels dir: %w", err)
	}
	if info, err := os.Stat(dir); err != nil {
		return fmt.Errorf("labels dir: %w", err)
	} else if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("labels dir: %w", ErrUnsafeFile)
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
