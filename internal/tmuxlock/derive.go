package tmuxlock

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

const (
	// DefaultRoot is the separately provisioned panel-lock tree. It must not
	// be placed below the node data directory because that directory remains
	// private mode 0700.
	DefaultRoot = "/var/lib/gifford-tmux-locks"

	maxPaneID = uint64(1<<31 - 1)
)

// ErrInvalidBinding means a panel identity is not in its canonical wire form.
var ErrInvalidBinding = errors.New("invalid panel binding")

// PanelBinding names one pane in one tmux server.
type PanelBinding struct {
	OwnerUID uint32
	Socket   string
	PaneID   string
}

type lockPathParts struct {
	owner string
	hash  string
	file  string
}

// PanelPaths is the complete fixed layout for one pane. Index files are
// authority records shared by every pane under the same owner.
type PanelPaths struct {
	Root          string
	OwnerDir      string
	SocketDir     string
	Lock          string
	State         string
	Register      string
	OwnerIndex    string
	EnrolledIndex string
	IndexLock     string
}

// DerivePanelPaths returns every canonical path used by one pane.
func DerivePanelPaths(binding PanelBinding) (PanelPaths, error) {
	parts, err := deriveParts(binding)
	if err != nil {
		return PanelPaths{}, err
	}
	ownerDir := DefaultRoot + "/" + parts.owner
	socketDir := ownerDir + "/" + parts.hash
	pane := parts.file[:len(parts.file)-len(".lock")]
	return PanelPaths{
		Root: DefaultRoot, OwnerDir: ownerDir, SocketDir: socketDir,
		Lock:          socketDir + "/" + parts.file,
		State:         socketDir + "/" + pane + ".state",
		Register:      socketDir + "/" + pane + ".reg",
		OwnerIndex:    ownerDir + "/index",
		EnrolledIndex: DefaultRoot + "/enrolled-uids",
		IndexLock:     DefaultRoot + "/index.lock",
	}, nil
}

// DerivePath returns the canonical lock path for binding. Socket bytes are
// hashed exactly as supplied; the function does not clean or normalize them.
func DerivePath(binding PanelBinding) (string, error) {
	paths, err := DerivePanelPaths(binding)
	if err != nil {
		return "", err
	}
	return paths.Lock, nil
}

func deriveParts(binding PanelBinding) (lockPathParts, error) {
	if binding.Socket == "" || binding.Socket[0] != '/' || !utf8.ValidString(binding.Socket) ||
		containsASCIIControl(binding.Socket) {
		return lockPathParts{}, fmt.Errorf("panel socket: %w", ErrInvalidBinding)
	}
	if len(binding.PaneID) < 2 || binding.PaneID[0] != '%' {
		return lockPathParts{}, fmt.Errorf("panel id: %w", ErrInvalidBinding)
	}
	digits := binding.PaneID[1:]
	if (len(digits) > 1 && digits[0] == '0') || digits[0] < '0' || digits[0] > '9' {
		return lockPathParts{}, fmt.Errorf("panel id: %w", ErrInvalidBinding)
	}
	for index := 1; index < len(digits); index++ {
		if digits[index] < '0' || digits[index] > '9' {
			return lockPathParts{}, fmt.Errorf("panel id: %w", ErrInvalidBinding)
		}
	}
	pane, err := strconv.ParseUint(digits, 10, 31)
	if err != nil || pane > maxPaneID || strconv.FormatUint(pane, 10) != digits {
		return lockPathParts{}, fmt.Errorf("panel id: %w", ErrInvalidBinding)
	}
	digest := sha256.Sum256([]byte(binding.Socket))
	return lockPathParts{
		owner: strconv.FormatUint(uint64(binding.OwnerUID), 10),
		hash:  hex.EncodeToString(digest[:]),
		file:  digits + ".lock",
	}, nil
}

func containsASCIIControl(value string) bool {
	for index := range len(value) {
		if value[index] <= 0x1f || value[index] == 0x7f {
			return true
		}
	}
	return false
}
