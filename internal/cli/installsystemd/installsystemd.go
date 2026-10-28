// Package installsystemd implements the `gifford install-systemd`
// subcommand: it has no behaviour yet.
package installsystemd

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford install-systemd command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("install-systemd", "Generate user-level systemd units for daemon, hub and relay")
}
