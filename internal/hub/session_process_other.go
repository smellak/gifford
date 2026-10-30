//go:build !linux

package hub

import (
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type unsupportedSessionRuntime struct{}

func newSessionProcessRuntime() sessionProcessRuntime { return unsupportedSessionRuntime{} }

func (unsupportedSessionRuntime) bootID() (string, error) {
	return "", fmt.Errorf("session process verification unavailable: %w", errInvalidArgument)
}

func (unsupportedSessionRuntime) alive(protocol.ProcessIdentity, string) error {
	return fmt.Errorf("session process verification unavailable: %w", errInvalidArgument)
}
