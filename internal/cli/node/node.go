// Package node implements the `gifford node` subcommand tree: node, peer
// and policy identities. It has no behaviour yet.
package node

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford node command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	join := deps.Leaf("join <url>", "Join a federation using an enrollment token from a peer")
	join.Flags().String("token", "", "enrollment token")

	return deps.Group("node", "Identities, peers, certificates and policies",
		deps.Leaf("enroll-token", "Create a single-use, short-lived enrollment token for a new peer"),
		join,
		deps.Leaf("peers", "List known federation peers and their status"),
		deps.Leaf("revoke <node>", "Revoke a peer or its credentials"),
		deps.Leaf("keys", "Manage this node's signing and encryption keys"),
	)
}
