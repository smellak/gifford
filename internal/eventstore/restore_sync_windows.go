//go:build windows

package eventstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
)

func syncDirectory(path string) (err error) {
	name, err := syscall.UTF16PtrFromString(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("encode directory path for sync: %w", err)
	}
	handle, err := syscall.CreateFile(
		name,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	defer func() {
		err = errors.Join(err, syscall.CloseHandle(handle))
	}()
	if err := syscall.FlushFileBuffers(handle); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}
