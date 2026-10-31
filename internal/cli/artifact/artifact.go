// Package artifact implements encrypted local artifact transfers over hub.sock.
package artifact

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/protocol"
)

// NewCmd returns the gifford artifact command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var recipient, purpose string
	send := &cobra.Command{
		Use:   "send <file>",
		Short: "Send a local file to another agent as an artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if recipient == "" {
				return fmt.Errorf("--to is required")
			}
			result, err := sendFile(cmd.Context(), args[0], protocol.AgentID(recipient), purpose)
			if err != nil {
				return err
			}
			return printJSON(cmd.OutOrStdout(), result)
		},
	}
	send.Flags().StringVar(&recipient, "to", "", "agent to send the artifact to")
	send.Flags().StringVar(&purpose, "purpose", "", "why the artifact is sent")

	list := &cobra.Command{
		Use:   "list",
		Short: "List pending encrypted artifacts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := listArtifacts(cmd.Context())
			if err != nil {
				return err
			}
			return printJSON(cmd.OutOrStdout(), result)
		},
	}

	var outputDirectory string
	receive := &cobra.Command{
		Use:   "receive <artifact_id>",
		Short: "Receive and verify an encrypted artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := receiveArtifact(cmd.Context(), protocol.OpaqueID(args[0]), outputDirectory)
			if err != nil {
				return err
			}
			return printJSON(cmd.OutOrStdout(), result)
		},
	}
	receive.Flags().StringVar(&outputDirectory, "out", ".", "directory for the received file")

	status := &cobra.Command{
		Use:   "status <artifact_id>",
		Short: "Show an encrypted artifact transfer status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := artifactStatus(cmd.Context(), protocol.OpaqueID(args[0]))
			if err != nil {
				return err
			}
			return printJSON(cmd.OutOrStdout(), result)
		},
	}

	return deps.Group("artifact", "Send and receive encrypted artifacts", send, list, receive, status)
}

func printJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write artifact result: %w", err)
	}
	return nil
}
