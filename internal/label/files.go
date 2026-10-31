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
	WeakFile        = "weak.jsonl"
	TimelineFile    = "timeline.jsonl"
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

// AlignedFile holds the labels align derived, under Dir. Each run
// replaces it.
const AlignedFile = "aligned.jsonl"

// maxLineBytes bounds one line read back from a label file.
const maxLineBytes = 2 << 20

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
// lines fn refuses. A missing dir or file has no lines; a dir that is not
// a real private directory, or a file that is not a private regular one,
// is refused.
func readLines(dir, name string, fn func([]byte) error) (bad int, err error) {
	// O_NOFOLLOW guards only the last component: check labels/ itself,
	// without creating it.
	switch info, err := os.Lstat(dir); {
	case errors.Is(err, os.ErrNotExist):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("labels dir: %w", err)
	case !info.IsDir() || info.Mode().Perm()&0o077 != 0:
		return 0, fmt.Errorf("labels dir: %w", ErrUnsafeFile)
	}
	// Then open the file the way appendLine does: no link followed, no
	// wait on a special file.
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDONLY|noFollow|noBlock, 0)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()
	if info, err := f.Stat(); err != nil {
		return 0, fmt.Errorf("stat %s: %w", name, err)
	} else if err := privateRegular(info); err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
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
