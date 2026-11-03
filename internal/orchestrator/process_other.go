//go:build !linux

package orchestrator

import (
	"errors"

	"github.com/smellak/gifford/internal/protocol"
)

type unsupportedProcessInspector struct{}

// NewProcessInspector returns a fail-closed process guard on non-Linux hosts.
func NewProcessInspector(string) ProcessInspector { return unsupportedProcessInspector{} }

// NewPeerProcessInspector returns a fail-closed peer guard on non-Linux hosts.
func NewPeerProcessInspector(string) PeerProcessInspector { return unsupportedProcessInspector{} }

func (unsupportedProcessInspector) Inspect(protocol.ProcessIdentity) (ProcessTree, error) {
	return ProcessTree{}, errors.ErrUnsupported
}

func (unsupportedProcessInspector) InspectPeer(
	protocol.ProcessIdentity,
	ProcessToken,
) (PeerLineage, error) {
	return PeerLineage{}, errors.ErrUnsupported
}
