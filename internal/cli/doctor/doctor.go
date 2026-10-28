// Package doctor implements `gifford doctor`: it creates and validates
// gifford's three XDG-based directories and reports on config.yaml. Unlike
// every other command in this subphase, it has real behaviour, not a
// not-implemented stub.
package doctor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford doctor command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var peer string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Validate the installation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.OutOrStdout(), peer)
		},
	}
	cmd.Flags().StringVar(&peer, "peer", "", "also check federation status for this peer (not implemented yet)")
	return cmd
}

// reportWriter writes doctor's report lines, keeping the first write
// error so callers handle it once at the end.
type reportWriter struct {
	w   io.Writer
	err error
}

func (r *reportWriter) printf(format string, args ...any) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintf(r.w, format, args...)
}

func run(out io.Writer, peer string) error {
	rep := &reportWriter{w: out}

	dirs, err := xdg.Resolve()
	if err != nil {
		rep.printf("%-11s %-4s %s\n", "XDG", "FAIL", err)
		if rep.err != nil {
			return fmt.Errorf("write doctor report: %w", rep.err)
		}
		return &deps.ExitError{Code: exitcode.Error, Err: err}
	}

	checks, ensureErr := xdg.EnsureAll(dirs)
	ok := ensureErr == nil
	for _, c := range checks {
		state := "exists"
		if c.Created {
			state = "created"
		}
		if c.OK {
			rep.printf("%-11s %-4s %s %s (0700)\n", strings.ToUpper(c.Name), "OK", state, c.Path)
			continue
		}
		ok = false
		detail := fmt.Sprintf("mode %04o, want 0700", c.Mode)
		if info, statErr := os.Stat(c.Path); statErr == nil && !info.IsDir() {
			detail = "not a directory"
		}
		rep.printf("%-11s %-4s %s %s (%s)\n", strings.ToUpper(c.Name), "FAIL", state, c.Path, detail)
	}

	cfgPath := config.DefaultPath(dirs.Config)
	if cfg, cfgErr := config.Load(cfgPath); cfgErr != nil {
		ok = false
		rep.printf("%-11s %-4s %s\n", "CONFIG.YAML", "FAIL", cfgErr)
	} else {
		rep.printf("%-11s %-4s schema_version=%d (%s)\n", "CONFIG.YAML", "OK", cfg.SchemaVersion, cfgPath)
	}

	if peer != "" {
		rep.printf("PEER %-6s %-4s federation checks not implemented yet\n", peer, "SKIP")
	}

	if !ok {
		return &deps.ExitError{Code: exitcode.Error, Err: errors.New("doctor found problems, see the report above")}
	}
	if rep.err != nil {
		return fmt.Errorf("write doctor report: %w", rep.err)
	}
	return nil
}
