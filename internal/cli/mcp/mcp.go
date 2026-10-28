// Package mcp implements the `gifford mcp` subcommand: it has no behaviour
// yet.
package mcp

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford mcp command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("mcp", "Agent Endpoint: MCP server, agent identity and encryption")
	cmd.Flags().String("agent", "", "agent id to serve the endpoint for")
	return cmd
}
