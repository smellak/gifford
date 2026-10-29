// Package mcp implements the `gifford mcp` subcommand: the agent
// endpoint. It dials hub.sock, serves MCP over stdin and stdout, and
// keeps diagnostics on stderr: stdout carries MCP frames only. Without
// a hub it fails with stdout untouched. The agent identity always comes
// from the hub, never from arguments or the environment.
package mcp

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford mcp command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Agent Endpoint: MCP server, agent identity and encryption",
		Long: `Serve MCP on stdin and stdout against the local hub.

Stdout carries MCP frames only; diagnostics go to stderr. The --agent
flag is accepted and not used: the agent identity comes from the hub
over hub.sock, never from arguments. Without a hub the command fails
leaving stdout empty.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := run(cmd); err != nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("gifford mcp: %w", err),
				}
			}
			return nil
		},
	}
	cmd.Flags().String("agent", "", "agent id to serve the endpoint for (accepted, not used)")
	return cmd
}

// run dials the hub and serves one MCP session on stdio.
func run(cmd *cobra.Command) error {
	ctx := cmd.Context()
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve dirs: %w", err)
	}
	client, err := endpoint.DialHub(ctx, filepath.Join(dirs.Runtime, "hub.sock"))
	if err != nil {
		return fmt.Errorf("dial hub: %w", err)
	}
	defer func() { _ = client.Close() }()
	server, err := endpoint.New(ctx, dirs.Data, cmd.InOrStdin(), cmd.OutOrStdout(), client)
	if err != nil {
		return fmt.Errorf("new endpoint server: %w", err)
	}
	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("serve endpoint: %w", err)
	}
	return nil
}
