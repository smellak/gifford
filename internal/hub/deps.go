package hub

import (
	"database/sql"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// This file holds the dependencies of the accept path: everything the
// four handlers share. Handlers never see message plaintext — only ids,
// routing and metadata — never write agent_keys, and never write a
// control column: those stay with internal/channel.

// Deps is what the accept handlers run with: the store pool for one
// BEGIN IMMEDIATE transaction per step, the gate engine, the key
// directory for sender and recipient preconditions, the clock for
// expiries, and the local node id for the local route.
type Deps struct {
	DB        *sql.DB
	Engine    *channel.Engine
	Registry  *registry.Registry
	Clock     clock.Clock
	LocalNode protocol.NodeID
}

// valid reports whether the dependencies are complete.
func (d Deps) valid() bool {
	return d.DB != nil && d.Engine != nil && d.Registry != nil &&
		d.Clock != nil && d.LocalNode.Valid()
}
