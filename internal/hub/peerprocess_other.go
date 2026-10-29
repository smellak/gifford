//go:build !linux

package hub

import (
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

func verifyPeerProcess(_ int, _ protocol.ProcessIdentity) error {
	return fmt.Errorf("peer process verification unavailable: %w", errInvalidArgument)
}
