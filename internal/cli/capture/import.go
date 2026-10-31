package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

// importFlags are the flags of gifford capture import.
type importFlags struct {
	format, agent, harness, domain string
}

func newImportCmd() *cobra.Command {
	var f importFlags
	cmd := &cobra.Command{
		Use:   "import <dir|file> --format F --agent ID --harness H --environment-domain D",
		Short: "Import an asciicast v2 capture recorded without the binary",
		Long: `Imports one capture recorded without the binary into the data dir as the pair
a gifford run session leaves, recordings/<id>.raw and <id>.events.jsonl, with
<id>.import.json beside them: the source's format, SHA-256 and size, the agent,
harness and environment domain given, and what was counted, never the source's
path. gifford dataset build then replays it as any session.

--format asciicast-v2 reads an asciicast v2 file of at most 1 GiB, each line
at most 4 MiB. Of its header only the version, size and timestamp are read; a
cast without a timestamp is refused, as the wall clock is not made up. Every
output is a recorded chunk and every resize an input.resize; typed input and
markers are counted and never written. No keyboard or draft event is made up,
so a replay leaves the draft unknown and the screen projection blanks the
whole screen. The session id is asciicast2- and the first 32 hex digits of the
file's SHA-256: the same file imported again with the same provenance writes
nothing and says already imported; with another agent, harness or environment
domain it fails, naming the field, and changes nothing. A directory (a day0
capture), --format day0 and --format asciicast-v3 are not implemented yet.

--agent, --harness and --environment-domain (bank_generic or fleet_managed) are
required: a cast cannot tell them. Prints the id and counts only.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return importUsage("expected one file to import")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImport(cmd, args[0], f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.agent, "agent", "", "agent id to attribute the import to")
	fl.StringVar(&f.harness, "harness", "", "harness profile of the imported capture")
	fl.StringVar(&f.format, "format", "", "input format: asciicast-v2 (day0 and asciicast-v3 are not implemented yet)")
	fl.StringVar(&f.domain, "environment-domain", "", "environment the capture comes from: bank_generic or fleet_managed")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return importUsage(err.Error())
	})
	return cmd
}

func importUsage(msg string) error {
	return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford capture import: %s", msg)}
}

// check refuses a missing flag or an unknown format before anything is
// read.
func (f importFlags) check() error {
	for _, r := range []struct{ name, value string }{
		{"--format", f.format}, {"--agent", f.agent}, {"--harness", f.harness}, {"--environment-domain", f.domain},
	} {
		if r.value == "" {
			return importUsage(r.name + " is required")
		}
	}
	if !capture.Format(f.format).Known() {
		return importUsage("unknown --format; asciicast-v2 is the one with an import")
	}
	return nil
}

func runImport(cmd *cobra.Command, path string, f importFlags) error {
	if err := f.check(); err != nil {
		return err
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("gifford capture import: %w", err)
	}
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	// Ctrl+C or a TERM cancels the import, which removes the temporary
	// files it made and fails: the recordings dir stays as it was.
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	res, err := capture.Import(ctx, capture.Options{
		DataDir:           dirs.Data,
		Source:            path,
		Format:            capture.Format(f.format),
		AgentID:           protocol.AgentID(f.agent),
		Harness:           protocol.Harness(f.harness),
		EnvironmentDomain: dataset.EnvironmentDomain(f.domain),
	})
	switch {
	case errors.Is(err, capture.ErrBadOptions):
		return importUsage(err.Error())
	case errors.Is(err, capture.ErrNotImplemented):
		return &deps.ExitError{Code: exitcode.NotImplemented, Err: fmt.Errorf("gifford capture import: %w", err)}
	case err != nil:
		return fmt.Errorf("gifford capture import: %w", err)
	}
	printImport(cmd.OutOrStdout(), res)
	return nil
}

// printImport prints the id and the counts, never content.
func printImport(out io.Writer, r capture.Result) {
	if r.Already {
		_, _ = fmt.Fprintf(out, "already imported %s\n", r.SessionID)
		return
	}
	c := r.Counts
	_, _ = fmt.Fprintf(out, "imported %s: %d output, %d resize; dropped %d input, %d marker\n",
		r.SessionID, c.Output, c.Resize, c.InputDropped, c.MarkerDropped)
}
