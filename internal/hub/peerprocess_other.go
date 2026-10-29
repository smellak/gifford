//go:build !linux

package hub

import (
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

func verifyPeerProcess(_ peerIdentity, _ protocol.ProcessIdentity) error {
	return fmt.Errorf("peer process verification unavailable: %w", errInvalidArgument)
}
