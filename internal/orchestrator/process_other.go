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

// NewNodePeerProcessInspector returns a fail-closed node peer guard on
// non-Linux hosts.
func NewNodePeerProcessInspector(string) NodePeerProcessInspector {
	return unsupportedProcessInspector{}
}

func (unsupportedProcessInspector) Inspect(protocol.ProcessIdentity) (ProcessTree, error) {
	return ProcessTree{}, errors.ErrUnsupported
}

func (unsupportedProcessInspector) InspectPeer(
	protocol.ProcessIdentity,
	ProcessToken,
) (PeerLineage, error) {
	return PeerLineage{}, errors.ErrUnsupported
}

func (unsupportedProcessInspector) InspectNodePeer(
	protocol.ProcessIdentity,
	ProcessToken,
) (PeerLineage, error) {
	return PeerLineage{}, errors.ErrUnsupported
}
