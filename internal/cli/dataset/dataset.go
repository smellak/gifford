// Package dataset implements the `gifford dataset` subcommand tree. build
// replays the recorded sessions, labels and sanitises their windows and writes
// the dataset directory; inspect reads a manifest and prints its counts and
// preparation gate; redact has no behaviour yet.
package dataset

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
)

// maxManifestBytes bounds the manifest inspect reads.
const maxManifestBytes = 1 << 30

// NewCmd returns the gifford dataset command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	redact := withVersionFlag(deps.Leaf("redact", "Apply the local redactor to a dataset before export"))
	return deps.Group("dataset", "Build and inspect the Analyst's dataset", newBuildCmd(), newInspectCmd(), redact)
}

// withVersionFlag adds the shared --version flag to a dataset subcommand.
func withVersionFlag(cmd *cobra.Command) *cobra.Command {
	cmd.Flags().String("version", "", "dataset version")
	return cmd
}

func newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <manifest>",
		Short: "Inspect a dataset manifest: counts and the preparation gate",
		Long: "Validates a dataset manifest and prints its window counts and its preparation\n" +
			"gate: passed or failed, with one line per fault. Numbers and names only, never\n" +
			"window content.",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford dataset inspect: expected one manifest path")}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return inspect(cmd.OutOrStdout(), args[0])
		},
	}
}

func inspect(out io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("gifford dataset inspect: %w", err)
	}
	defer func() { _ = f.Close() }()
	m, err := dataset.ParseManifest(io.LimitReader(f, maxManifestBytes))
	if err != nil {
		return fmt.Errorf("gifford dataset inspect: %s: %w", filepath.Base(path), err)
	}
	printManifest(out, m)
	return nil
}

// printManifest prints a manifest's counts and preparation gate: numbers
// and names only.
func printManifest(out io.Writer, m dataset.Manifest) {
	c := m.Counts
	_, _ = fmt.Fprintf(out, "dataset %s: %d windows (train %d, validation %d, test %d)\n",
		m.DatasetDigest, c.Windows, c.BySplit.Train, c.BySplit.Validation, c.BySplit.Test)
	if f := m.SplitFile; f != nil {
		_, _ = fmt.Fprintf(out, "split file %s\n", *f)
	}
	if a := m.Align; a != nil {
		config := "defaults"
		if a.ConfigDigest != nil {
			config = string(*a.ConfigDigest)
		}
		_, _ = fmt.Fprintf(out, "align margin %d ms, config %s\n", a.MarginMS, config)
	}
	if m.ScriptedInput {
		_, _ = fmt.Fprintln(out, "scripted input: no human keyboard, every draft counted closed")
	}
	r := m.Readiness
	switch {
	case r == nil:
		_, _ = fmt.Fprintln(out, "readiness: not recorded")
	case r.Passed:
		_, _ = fmt.Fprintln(out, "readiness: passed")
	default:
		_, _ = fmt.Fprintf(out, "readiness: failed, %d faults\n", len(r.Faults))
		for _, fault := range r.Faults {
			_, _ = fmt.Fprintln(out, fault.Line())
		}
	}
}
