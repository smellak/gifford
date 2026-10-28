package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/doctor"
	"github.com/smellak/gifford/internal/cli/initcmd"
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
