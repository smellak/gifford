package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"

	"github.com/smellak/gifford/internal/cli/adapter"
	"github.com/smellak/gifford/internal/cli/artifact"
	"github.com/smellak/gifford/internal/cli/capture"
	"github.com/smellak/gifford/internal/cli/ctl"
	"github.com/smellak/gifford/internal/cli/daemon"
	"github.com/smellak/gifford/internal/cli/dataset"
	"github.com/smellak/gifford/internal/cli/db"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/doctor"
	"github.com/smellak/gifford/internal/cli/down"
	"github.com/smellak/gifford/internal/cli/eval"
	"github.com/smellak/gifford/internal/cli/fuzz"
	"github.com/smellak/gifford/internal/cli/hub"
	"github.com/smellak/gifford/internal/cli/initcmd"
	"github.com/smellak/gifford/internal/cli/installsystemd"
	"github.com/smellak/gifford/internal/cli/keycap"
	"github.com/smellak/gifford/internal/cli/label"
	"github.com/smellak/gifford/internal/cli/mcp"
	"github.com/smellak/gifford/internal/cli/node"
	"github.com/smellak/gifford/internal/cli/panel"
	"github.com/smellak/gifford/internal/cli/relay"
	"github.com/smellak/gifford/internal/cli/run"
	"github.com/smellak/gifford/internal/cli/status"
	"github.com/smellak/gifford/internal/cli/up"
	"github.com/smellak/gifford/internal/version"
)

// NewRootCmd returns the root of the command tree.
func NewRootCmd(d *deps.Deps) *cobra.Command {
	root := &cobra.Command{
		Use:           "gifford",
		Short:         "Let coding agents talk to each other without typing over the human",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(
		newVersionCmd(), newManCmd(root),
		initcmd.NewCmd(d), doctor.NewCmd(d),
		up.NewCmd(d), down.NewCmd(d), status.NewCmd(d),
		installsystemd.NewCmd(d), daemon.NewCmd(d),
		run.NewCmd(d), adapter.NewCmd(d), mcp.NewCmd(d),
		hub.NewCmd(d), relay.NewCmd(d), panel.NewCmd(d),
		ctl.NewCmd(d), node.NewCmd(d), artifact.NewCmd(d), capture.NewCmd(d),
		keycap.NewCmd(d), fuzz.NewCmd(d),
		label.NewCmd(d), dataset.NewCmd(d), eval.NewCmd(d), db.NewCmd(d),
	)
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version, commit and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), version.String())
			return err //nolint:wrapcheck // Writing to stdout has nothing to add.
		},
	}
}

func newManCmd(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:    "man",
		Short:  "Write the man page to stdout",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			header := &doc.GenManHeader{Title: "GIFFORD", Section: "1"}
			if err := doc.GenMan(root, header, cmd.OutOrStdout()); err != nil {
				return fmt.Errorf("generate man page: %w", err)
			}
			return nil
		},
	}
}
