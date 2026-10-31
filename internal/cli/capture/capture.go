// Package capture implements the `gifford capture` subcommand tree. Import
// brings in asciicast v2 captures recorded without the binary; the Bootstrap
// Capture Mode commands do not have behaviour yet.
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

	export := deps.Leaf("export", "Export a capture segment as asciicast v3")
	export.Flags().String("session", "", "capture session to export")
	export.Flags().String("format", "", "export format")

	ingest := deps.Leaf("ingest", "Ingest one pane output stream into a capture session (started by tmux; not for manual use)")
	ingest.Flags().String("session", "", "capture session to ingest into")
	ingest.Hidden = true

	return deps.Group("capture", "Import captures recorded without the binary",
		attach,
		status,
		stop,
		deps.Leaf("reconcile", "Reconcile the capture registry against disk and tmux state"),
		newImportCmd(),
		export,
		deps.Leaf("supervise", "Idempotent supervisor that keeps bootstrap captures attached (started by gifford up; safe to also run by hand)"),
		ingest,
	)
}
