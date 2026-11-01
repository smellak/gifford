package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Names inside a dataset directory.
const (
	ManifestFile = "manifest.json"
	WindowsDir   = "windows"
	// NativeLabelsFile is the optional sealed native-event evidence sidecar.
	NativeLabelsFile = "native-labels.json"
)

// ErrDatasetDir means a dataset directory was not written; dir does not
// exist after it.
var ErrDatasetDir = errors.New("dataset directory not written")

// WindowFile is the name of a window's file under WindowsDir.
func WindowFile(id string) string { return id + ".json" }

// WriteDir writes an inventory as a new directory dir: manifest.json,
// windows/<window_id>.json and, when sealed, native-labels.json. Directories
// are 0700 and files 0600. dir must not exist. It is written whole under a
// temporary sibling and renamed at the end, so on any failure dir does not
// appear. Equal inventories give equal bytes.
func WriteDir(dir string, inv Inventory) (err error) {
	if err := inv.Manifest.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	if err := validateNativeInventory(inv); err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	if len(inv.Windows) != len(inv.Manifest.Entries) {
		return fmt.Errorf("%w: windows and manifest disagree", ErrDatasetDir)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %s already exists", ErrDatasetDir, filepath.Base(dir))
	}
	// MkdirTemp creates the directory 0700.
	tmp, err := os.MkdirTemp(filepath.Dir(dir), ".dataset-*")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := os.Mkdir(filepath.Join(tmp, WindowsDir), 0o700); err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	for _, w := range inv.Windows {
		if err := writeJSON(filepath.Join(tmp, WindowsDir, WindowFile(string(w.WindowID))), w); err != nil {
			return err
		}
	}
	if inv.NativeLabels != nil {
		if err := writeJSON(filepath.Join(tmp, NativeLabelsFile), *inv.NativeLabels); err != nil {
			return err
		}
	}
	if err := writeJSON(filepath.Join(tmp, ManifestFile), inv.Manifest); err != nil {
		return err
	}
	// A directory that appeared meanwhile is not replaced.
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %s already exists", ErrDatasetDir, filepath.Base(dir))
	}
	if err := os.Rename(tmp, dir); err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	return nil
}

// writeJSON writes v as one JSON document and a newline to a new 0600
// file.
func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("%w: encode %s", ErrDatasetDir, filepath.Base(path))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetDir, err)
	}
	_, err = f.Write(append(b, '\n'))
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("%w: write %s: %w", ErrDatasetDir, filepath.Base(path), err)
	}
	return nil
}
