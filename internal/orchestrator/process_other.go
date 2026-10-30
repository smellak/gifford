//go:build !linux

package orchestrator

import (
	"errors"

	"github.com/smellak/gifford/internal/protocol"
)

type unsupportedProcessInspector struct{}

// NewProcessInspector returns a fail-closed process guard on non-Linux hosts.
func NewProcessInspector(string) ProcessInspector { return unsupportedProcessInspector{} }

func (unsupportedProcessInspector) Inspect(protocol.ProcessIdentity) (ProcessTree, error) {
	return ProcessTree{}, errors.ErrUnsupported
}
