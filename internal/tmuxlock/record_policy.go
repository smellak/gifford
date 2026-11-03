package tmuxlock

import (
	"fmt"
	"slices"
)

// RecordPolicy supplies the deployment-specific half of the closed record
// grammar. Peer identities have no compiled defaults.
type RecordPolicy struct {
	PeerOwner   string
	PeerWriters []string
}

type validatedRecordPolicy struct {
	peerOwner string
	writers   map[string]struct{}
}

// Validate reports whether both closed deployment-specific sets are
// canonical and unambiguous.
func (p RecordPolicy) Validate() error {
	_, err := validateRecordPolicy(p)
	return err
}

var nodeRecordWriters = []string{
	nodeOwner,
	"provision",
	operationTransfer,
	"reconcile",
	operationRepair,
}

func validateRecordPolicy(policy RecordPolicy) (validatedRecordPolicy, error) {
	if !validOwner(policy.PeerOwner) || policy.PeerOwner == nodeOwner ||
		len(policy.PeerWriters) == 0 || !slices.IsSorted(policy.PeerWriters) ||
		adjacentEqual(policy.PeerWriters) {
		return validatedRecordPolicy{}, fmt.Errorf("pane-lock record policy: %w", ErrInvalidBinding)
	}
	writers := make(map[string]struct{}, len(nodeRecordWriters)+len(policy.PeerWriters))
	for _, writer := range nodeRecordWriters {
		writers[writer] = struct{}{}
	}
	for _, writer := range policy.PeerWriters {
		if !validOwner(writer) {
			return validatedRecordPolicy{}, fmt.Errorf("pane-lock record policy: %w", ErrInvalidBinding)
		}
		if _, exists := writers[writer]; exists {
			return validatedRecordPolicy{}, fmt.Errorf("pane-lock record policy: %w", ErrInvalidBinding)
		}
		writers[writer] = struct{}{}
	}
	return validatedRecordPolicy{peerOwner: policy.PeerOwner, writers: writers}, nil
}

func (p validatedRecordPolicy) ownerAllowed(owner string) bool {
	return owner == nodeOwner || owner == p.peerOwner
}

func (p validatedRecordPolicy) writerAllowed(writer string) bool {
	_, ok := p.writers[writer]
	return ok
}
