package capture

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// errNameExists reports a no-replace rename whose target already exists:
// the existing file wins and the temp becomes a remnant.
var errNameExists = errors.New("publication target exists")

// errRenameUnsupported reports a filesystem without the no-replace
// rename primitive.
var errRenameUnsupported = errors.New("filesystem without no-replace rename")

// publishFile writes data as dir/name through root with the §4.7
// protocol: temp with 16 lowercase hex characters, full write, file
// fsync, rename, directory fsync. With noReplace the rename fails when
// the final name exists; otherwise it replaces quota.json and
// ingest.exit.
func publishFile(root *os.Root, dir, name string, data []byte, noReplace bool) error {
	if err := checkRemnants(root, dir, name); err != nil {
		return err
	}
	tmp, err := makeTempName(name)
	if err != nil {
		return err
	}
	f, err := root.OpenFile(dir+"/"+tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	if err := writeFull(f, data); err != nil {
		_ = f.Close()
		_ = removeTemp(root, dir, tmp)
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = removeTemp(root, dir, tmp)
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	if err := f.Close(); err != nil {
		_ = removeTemp(root, dir, tmp)
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	if err := renamePublished(root, dir, tmp, name, noReplace); err != nil {
		if !noReplace || !errors.Is(err, errNameExists) {
			_ = removeTemp(root, dir, tmp)
		}
		return err
	}
	if err := syncDirAt(root, dir); err != nil {
		return err
	}
	return nil
}

// checkRemnants refuses when two crash remnants of this name remain.
func checkRemnants(root *os.Root, dir, name string) error {
	n, err := countRemnants(root, dir, name)
	if err != nil {
		return err
	}
	if n >= 2 {
		return fmt.Errorf("%w: publication remnants of %s", ErrUnsafePath, name)
	}
	return nil
}

// countRemnants counts <name>.tmp-* entries in dir.
func countRemnants(root *os.Root, dir, name string) (int, error) {
	d, err := root.Open(dir)
	if err != nil {
		return 0, fmt.Errorf("%w: list %s: %w", ErrUnsafePath, dir, err)
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return 0, fmt.Errorf("%w: list %s: %w", ErrUnsafePath, dir, err)
	}
	n := 0
	for _, e := range names {
		if strings.HasPrefix(e, name+".tmp-") {
			n++
		}
	}
	return n, nil
}

// makeTempName returns a temp name with 16 lowercase hex characters.
// The name is reserved atomically by the write open with O_EXCL.
func makeTempName(name string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("%w: temp name: %w", ErrUnsafePath, err)
	}
	const hex = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(16)
	for _, c := range b {
		sb.WriteByte(hex[c>>4])
		sb.WriteByte(hex[c&15])
	}
	return name + ".tmp-" + sb.String(), nil
}

// removeTemp removes a temp the caller created, on its failure path.
func removeTemp(root *os.Root, dir, tmp string) error {
	if err := root.Remove(dir + "/" + tmp); err != nil {
		return fmt.Errorf("%w: remove temp: %w", ErrUnsafePath, err)
	}
	return nil
}

// writeFull writes all of data.
func writeFull(f *os.File, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := f.Write(data[total:])
		total += n
		if err != nil {
			return fmt.Errorf("publish write: %w", err)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// renamePublished renames the temp onto the final name inside dir.
func renamePublished(root *os.Root, dir, tmp, name string, noReplace bool) error {
	if !noReplace {
		if err := root.Rename(dir+"/"+tmp, dir+"/"+name); err != nil {
			return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
		}
		return nil
	}
	d, err := root.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	defer func() { _ = d.Close() }()
	if err := renameNoReplace(d, tmp, name); err != nil {
		return fmt.Errorf("%w: publish %s: %w", ErrUnsafePath, name, err)
	}
	return nil
}

// syncDirAt fsyncs one directory through the root.
func syncDirAt(root *os.Root, dir string) error {
	d, err := root.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: fsync dir: %w", ErrUnsafePath, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("%w: fsync dir: %w", ErrUnsafePath, err)
	}
	return nil
}

// checkNoReplace verifies the no-replace primitive with a residue-free
// probe: claims.lock, created on demand, renamed onto itself. Success or
// EEXIST both prove the kernel honoured the flag; ENOSYS, EINVAL and
// ENOTSUP prove it missing. Anything else fails closed. An existing
// entry is checked before any open and a missing one is created
// exclusively: a dangling symlink never resolves into a created
// target elsewhere in the root.
func checkNoReplace(root *os.Root) error {
	_, lerr := root.Lstat("claims.lock")
	if lerr == nil {
		return checkNoReplaceProceed(root, checkLockFile(root))
	}
	if !os.IsNotExist(lerr) {
		return fmt.Errorf("%w: lock file: %w", ErrUnsafePath, lerr)
	}
	f, err := root.OpenFile("claims.lock", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		_ = f.Close()
		return checkNoReplaceProceed(root, nil)
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: lock file: %w", ErrUnsafePath, err)
	}
	return checkNoReplaceProceed(root, checkLockFile(root))
}

// checkLockFile validates an existing claims.lock entry without
// following it: a symlink or hardlink fails before any open.
func checkLockFile(root *os.Root) error {
	f, err := openChecked(root, "claims.lock", false)
	if err != nil {
		return err
	}
	_ = f.Close()
	return nil
}

// checkNoReplaceProceed runs the residue-free rename probe after the
// entry check.
func checkNoReplaceProceed(root *os.Root, err error) error {
	if err != nil {
		return err
	}
	d, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("%w: no-replace check: %w", ErrUnsafePath, err)
	}
	defer func() { _ = d.Close() }()
	err = renameNoReplace(d, "claims.lock", "claims.lock")
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errNameExists):
		return nil
	default:
		return fmt.Errorf("%w: no-replace rename: %w", ErrUnsafePath, err)
	}
}
