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

// privateDir creates dir 0700 and refuses it unless it is a real
// directory, not a symlink, closed to others.
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("labels dir: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("labels dir: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("labels dir: %w", ErrUnsafeFile)
	}
	return nil
}

// privateRegular refuses anything but a regular file closed to others:
// a symlink, a FIFO or a device could block a hook or lead out of the
// labels dir.
func privateRegular(info os.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return ErrUnsafeFile
	}
	return nil
}

// appendLine appends one line to dir/name in a single write, so lines
// from concurrent hooks never interleave. It creates the directory 0700
// and the file 0600, and refuses either unless it is private and of the
// right type. It never follows a symlink and never blocks on a special
// file: an existing file is checked before it is opened, opened without
// following links and without blocking, and checked again once open.
func appendLine(dir, name string, line []byte) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, name)
	f, err := openPrivateAppend(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	_, err = f.Write(line)
	return errors.Join(err, f.Close())
}

func openPrivateAppend(path string) (*os.File, error) {
	for range 2 {
		before, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND|noFollow, filePerm)
			if errors.Is(err, os.ErrExist) {
				continue // another hook created it first: check it
			}
			if err != nil {
				return nil, fmt.Errorf("create: %w", err)
			}
			return f, nil
		}
		if err != nil {
			return nil, fmt.Errorf("stat: %w", err)
		}
		if err := privateRegular(before); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|noFollow|noBlock, 0)
		if err != nil {
			return nil, fmt.Errorf("open: %w", err)
		}
		after, err := f.Stat()
		if err == nil {
			err = privateRegular(after)
		}
		if err == nil && !os.SameFile(before, after) {
			err = ErrUnsafeFile
		}
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		return f, nil
	}
	return nil, ErrUnsafeFile
}
