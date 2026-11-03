//go:build linux

package tmuxlock

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// OpenStateFile opens one provisioned pane's .state record for its journal.
// It walks the managed root like the lock does, refusing symlinks and any
// directory or record that is not the node's with the contract modes and
// ACLs, and checks the record is a regular single-link file of exactly one
// record. The caller must already hold the pane flock.
func OpenStateFile(binding PanelBinding) (StateFile, error) {
	return openStateAt(DefaultRoot, binding)
}

// OpenStateFileAt opens a state record below an explicit isolated root. Node
// production wiring uses OpenStateFile; this form lets its package tests keep
// every cooperative artifact under a temporary directory.
func OpenStateFileAt(root string, binding PanelBinding) (StateFile, error) {
	return openStateAt(root, binding)
}

func openStateAt(root string, binding PanelBinding) (StateFile, error) {
	parts, err := deriveParts(binding)
	if err != nil {
		return nil, err
	}
	if binding.OwnerUID == 0 {
		return nil, fmt.Errorf("panel owner: %w", ErrInvalidBinding)
	}
	components, err := managedRootComponents(root)
	if err != nil {
		return nil, err
	}
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	closeCurrent := func() { _ = unix.Close(current) }
	systemRoot := root == DefaultRoot
	if systemRoot {
		if err := validateTrustedAncestor(current); err != nil {
			closeCurrent()
			return nil, err
		}
	}
	for index, component := range components {
		next, openErr := openAtSecure(current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
		closeCurrent()
		if openErr != nil {
			return nil, fmt.Errorf("open managed root: %w", openErr)
		}
		current = next
		if systemRoot && index < len(components)-1 {
			if err := validateTrustedAncestor(current); err != nil {
				closeCurrent()
				return nil, err
			}
		}
	}
	expectedOwner := effectiveUID()
	if err := validateManagedRoot(current, expectedOwner, effectiveGID()); err != nil {
		closeCurrent()
		return nil, err
	}
	for _, component := range []string{parts.owner, parts.hash} {
		next, openErr := openAtSecure(current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
		closeCurrent()
		if openErr != nil {
			return nil, fmt.Errorf("open pane directory: %w", openErr)
		}
		current = next
		if err := validateManagedDirectory(current, binding.OwnerUID, expectedOwner); err != nil {
			closeCurrent()
			return nil, err
		}
	}
	name := parts.file[:len(parts.file)-len(".lock")] + ".state"
	fd, err := openAtSecure(current, name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	closeCurrent()
	if err != nil {
		return nil, fmt.Errorf("open pane record: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("stat pane record: %w", err)
	}
	if err := validateStateFileStat(stat, expectedOwner); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if err := validateFileACL(fd, binding.OwnerUID); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &stateDescriptor{fd: fd}, nil
}

func validateStateFileStat(stat unix.Stat_t, expectedOwner uint32) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != expectedOwner ||
		stat.Mode&0o7777 != 0o660 || stat.Nlink != 1 || stat.Size != recordSize {
		return errors.New("unsafe pane record attributes")
	}
	return nil
}

// stateDescriptor is the production StateFile: positional I/O and data
// sync on one descriptor, never truncated, renamed or reopened.
type stateDescriptor struct {
	fd int
}

func (d *stateDescriptor) ReadAt(p []byte, offset int64) (int, error) {
	read, err := unix.Pread(d.fd, p, offset)
	if err != nil {
		return read, fmt.Errorf("pread pane record: %w", err)
	}
	return read, nil
}

func (d *stateDescriptor) WriteAt(p []byte, offset int64) (int, error) {
	written, err := unix.Pwrite(d.fd, p, offset)
	if err != nil {
		return written, fmt.Errorf("pwrite pane record: %w", err)
	}
	return written, nil
}

func (d *stateDescriptor) Datasync() error {
	if err := unix.Fdatasync(d.fd); err != nil {
		return fmt.Errorf("fdatasync pane record: %w", err)
	}
	return nil
}

func (d *stateDescriptor) Close() error {
	if d.fd < 0 {
		return nil
	}
	err := unix.Close(d.fd)
	d.fd = -1
	if err != nil {
		return fmt.Errorf("close pane record: %w", err)
	}
	return nil
}

// CurrentWriter returns this process's journal identity.
func CurrentWriter() (Writer, error) {
	pid := int64(os.Getpid())
	start, err := ProcessStart(pid)
	if err != nil {
		return Writer{}, err
	}
	return Writer{PID: pid, Start: start}, nil
}

// ProcessStart reads field 22 (starttime) of /proc/<pid>/stat. The command
// name may hold spaces and parentheses, so fields count from the last ")".
func ProcessStart(pid int64) (uint64, error) {
	raw, err := os.ReadFile("/proc/" + strconv.FormatInt(pid, 10) + "/stat")
	if err != nil {
		return 0, fmt.Errorf("read process %d stat: %w", pid, err)
	}
	return parseProcessStart(raw)
}

func parseProcessStart(raw []byte) (uint64, error) {
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return 0, errors.New("process stat without command")
	}
	// After ") " come fields 3 onwards; starttime is field 22.
	fields := bytes.Fields(raw[end+1:])
	if len(fields) < 20 {
		return 0, errors.New("process stat too short")
	}
	start, err := strconv.ParseUint(string(fields[19]), 10, 64)
	if err != nil || start == 0 {
		return 0, errors.New("process stat start time")
	}
	return start, nil
}

// BootID reads the kernel boot id the bindings carry.
func BootID() (string, error) {
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot id: %w", err)
	}
	value := string(bytes.TrimSpace(raw))
	if !bootIDPattern.MatchString(value) {
		return "", errors.New("boot id format")
	}
	return value, nil
}
