package controlapi

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const maxPaneAdminUIDBytes = 64 * 1024

var (
	// ErrPaneOperationUnauthorized means the authenticated socket uid is not
	// allowed to request an administrative pane operation.
	ErrPaneOperationUnauthorized = errors.New("pane operation unauthorized")
	// ErrPaneOperationPolicyInvalid means the root-owned uid policy cannot be
	// trusted or decoded. Administrative operations fail closed.
	ErrPaneOperationPolicyInvalid = errors.New("pane operation policy invalid")
)

type paneUIDLoader func(string) ([]uint32, error)

// PaneOperationAuthorizer re-reads the root-owned uid policy for every
// operation, so revocation does not depend on daemon restart or cache expiry.
type PaneOperationAuthorizer struct {
	path string
	load paneUIDLoader
}

// NewPaneOperationAuthorizer binds administrative pane operations to one
// clean absolute policy path. The path has no default.
func NewPaneOperationAuthorizer(path string) (*PaneOperationAuthorizer, error) {
	return newPaneOperationAuthorizer(path, loadPaneAdminUIDs)
}

func newPaneOperationAuthorizer(
	path string,
	load paneUIDLoader,
) (*PaneOperationAuthorizer, error) {
	if !validPanePolicyPath(path) || load == nil {
		return nil, ErrPaneOperationPolicyInvalid
	}
	return &PaneOperationAuthorizer{path: path, load: load}, nil
}

// Authorize accepts only a peer authenticated by the Unix-socket transport
// whose uid appears in the current policy. Request fields never carry actor
// authority.
func (a *PaneOperationAuthorizer) Authorize(peer Peer) error {
	if a == nil || a.load == nil || !peer.Verified {
		return ErrPaneOperationUnauthorized
	}
	uids, err := a.load(a.path)
	if err != nil {
		return fmt.Errorf("load pane operation policy: %w", ErrPaneOperationPolicyInvalid)
	}
	_, allowed := slices.BinarySearch(uids, peer.UID)
	if !allowed {
		return ErrPaneOperationUnauthorized
	}
	return nil
}

func validPanePolicyPath(value string) bool {
	return value != "" && value != "/" && filepath.IsAbs(value) &&
		filepath.Clean(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func parsePaneAdminUIDs(raw []byte) ([]uint32, error) {
	if len(raw) > maxPaneAdminUIDBytes {
		return nil, ErrPaneOperationPolicyInvalid
	}
	if len(raw) == 0 {
		return []uint32{}, nil
	}
	if raw[len(raw)-1] != '\n' {
		return nil, ErrPaneOperationPolicyInvalid
	}
	lines := strings.Split(string(raw[:len(raw)-1]), "\n")
	uids := make([]uint32, 0, len(lines))
	var previous uint32
	for index, line := range lines {
		if line == "" {
			return nil, ErrPaneOperationPolicyInvalid
		}
		for _, char := range line {
			if char < '0' || char > '9' {
				return nil, ErrPaneOperationPolicyInvalid
			}
		}
		parsed, err := strconv.ParseUint(line, 10, 32)
		uid := uint32(parsed)
		if err != nil || strconv.FormatUint(parsed, 10) != line || index > 0 && uid <= previous {
			return nil, ErrPaneOperationPolicyInvalid
		}
		uids = append(uids, uid)
		previous = uid
	}
	return uids, nil
}
