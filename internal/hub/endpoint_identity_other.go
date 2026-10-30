//go:build !linux

package hub

import (
	"context"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func resolveEndpointIdentity(
	context.Context,
	registry.Querier,
	protocol.AgentID,
	int,
	int64,
) (EndpointIdentity, error) {
	return EndpointIdentity{}, fmt.Errorf("endpoint process verification unavailable: %w", errInvalidArgument)
}
