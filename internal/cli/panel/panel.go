// Package panel implements the read-only `gifford panel` subcommand.
package panel

import (
	"fmt"
	"os"
	"path/filepath"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/tui"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford panel command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return &cobra.Command{
		Use:   "panel",
		Short: "TUI for observation and governance",
		Args:  cobra.NoArgs,
		RunE:  run,
	}
}

func run(cmd *cobra.Command, _ []string) error {
	in, ok := cmd.InOrStdin().(*os.File)
	if !ok || in == nil || !term.IsTerminal(int(in.Fd())) {
		return panelError(fmt.Errorf("gifford panel: stdin is not an interactive terminal"))
	}
	out, ok := cmd.OutOrStdout().(*os.File)
	if !ok || out == nil || !term.IsTerminal(int(out.Fd())) {
		return panelError(fmt.Errorf("gifford panel: stdout is not an interactive terminal"))
	}

	dirs, err := xdg.Resolve()
	if err != nil {
		return panelError(fmt.Errorf("gifford panel: resolve directories: %w", err))
	}
	cfg, err := config.Load(config.DefaultPath(dirs.Config))
	if err != nil {
		return panelError(fmt.Errorf("gifford panel: load config: %w", err))
	}
	client, err := tui.NewClient(tui.ClientOptions{SocketPath: filepath.Join(dirs.Runtime, "daemon.sock")})
	if err != nil {
		return panelError(fmt.Errorf("gifford panel: configure control client: %w", err))
	}
	model := tui.NewModel(cmd.Context(), client, cfg.NodeID)
	program := tea.NewProgram(
		model,
		tea.WithContext(cmd.Context()),
		tea.WithInput(in),
		tea.WithOutput(out),
	)
	if _, err := program.Run(); err != nil {
		return panelError(fmt.Errorf("gifford panel: run: %w", err))
	}
	return nil
}

func panelError(err error) error {
	return &deps.ExitError{Code: exitcode.Error, Err: err}
}
