//go:build !linux

package agentlink

import (
	"context"
	"net"

	"github.com/smellak/gifford/internal/protocol"
)

func dialVerifiedPath(context.Context, string) (*net.UnixConn, peerCredentials, error) {
	return nil, peerCredentials{}, ErrUnsupportedPlatform
}

func verifyPeerProcess(peerCredentials, protocol.ProcessIdentity) error {
	return ErrUnsupportedPlatform
}
