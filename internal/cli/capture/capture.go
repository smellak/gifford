// Package capture implements the `gifford capture` subcommand tree:
// Bootstrap Capture Mode observes agents already running in tmux. It has no
// behaviour yet.
package capture

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford capture command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	attach := deps.Leaf("attach", "Attach to a live tmux pane and start observing it")
	attach.Flags().String("agent", "", "agent id to observe")
	attach.Flags().String("tmux-pane", "", "tmux pane to attach to")
	attach.Flags().String("harness", "", "harness profile of the observed agent")
	attach.Flags().Bool("replace", false, "replace an existing capture session")

	status := deps.Leaf("status", "Show bytes captured, last activity, rotations and reattachments")
	status.Flags().String("agent", "", "agent id to report on")

	stop := deps.Leaf("stop", "Stop observing an agent's capture session")
	stop.Flags().String("agent", "", "agent id to stop observing")
	stop.Flags().String("reason", "", "why the capture is stopped")

	importCmd := deps.Leaf("import <dir|file>", "Import captures recorded without the binary, or asciicast files")
	importCmd.Flags().String("agent", "", "agent id to attribute the import to")
	importCmd.Flags().String("harness", "", "harness profile of the imported capture")
	importCmd.Flags().String("format", "", "input format of the imported capture")

	export := deps.Leaf("export", "Export a capture segment as asciicast v3")
	export.Flags().String("session", "", "capture session to export")
	export.Flags().String("format", "", "export format")

	ingest := deps.Leaf("ingest", "Ingest one pane output stream into a capture session (started by tmux; not for manual use)")
	ingest.Flags().String("session", "", "capture session to ingest into")
	ingest.Hidden = true

	return deps.Group("capture", "Bootstrap Capture Mode: observe agents already running in tmux",
		attach,
		status,
		stop,
		deps.Leaf("reconcile", "Reconcile the capture registry against disk and tmux state"),
		importCmd,
		export,
		deps.Leaf("supervise", "Idempotent supervisor that keeps bootstrap captures attached (started by gifford up; safe to also run by hand)"),
		ingest,
	)
}
