// Package eval implements the `gifford eval` subcommand: it scores
// prediction files of the base model and the Analyst LoRA, runs the LoRA
// gate and renders docs/RESULTS.md. Running the model over the windows
// comes later, with the 4.1 deployment.
package eval

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
)

// maxInputBytes bounds one predictions or manifest file.
const maxInputBytes = 1 << 30

// NewCmd returns the gifford eval command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var profiles []string
	var manifest, out string
	var bins int
	var budget time.Duration
	cmd := &cobra.Command{
		Use:   "eval --profile base=FILE [--profile analyst-lora=FILE] [flags]",
		Short: "Evaluate Glimmer (base or Analyst LoRA) and the protocol",
		Long: `Score prediction files (one JSONL line per window: window_id, split, harness,
harness_version, session_id, source_quality, label, probabilities over A-G,
valid_mass, invalid, latency_ms, profile). Calibrates by temperature scaling on
validation, sets the SAFE threshold, scores test and validation, runs the LoRA
gate when both profiles are given over the same windows, and writes the report
(RESULTS.md): numbers and harness names only. Same inputs, same bytes.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, profiles, manifest, out, bins, budget)
		},
	}
	cmd.Flags().StringArrayVar(&profiles, "profile", nil, "profile=FILE, for base and analyst-lora")
	cmd.Flags().StringVar(&manifest, "manifest", "", "dataset manifest the predictions were drawn from (gate criterion 0)")
	cmd.Flags().StringVar(&out, "out", "", "where to write the report (stdout when empty)")
	cmd.Flags().IntVar(&bins, "bins", eval.DefaultBins, "ECE bins")
	cmd.Flags().DurationVar(&budget, "latency-budget", eval.DefaultLatencyBudget, "p95 latency budget of the LoRA")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usage(err.Error())
	})
	return cmd
}

func usage(msg string) error {
	return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford eval: %s", msg)}
}

func run(cmd *cobra.Command, profiles []string, manifestPath, out string, bins int, budget time.Duration) error {
	files := map[eval.Profile]string{}
	for _, p := range profiles {
		name, path, ok := strings.Cut(p, "=")
		prof := eval.Profile(name)
		if !ok || path == "" || !prof.Valid() {
			return usage("--profile takes base=FILE or analyst-lora=FILE")
		}
		if _, dup := files[prof]; dup {
			return usage("--profile " + name + " given twice")
		}
		files[prof] = path
	}
	if files[eval.ProfileBase] == "" {
		return usage("--profile base=FILE is required")
	}
	if bins < 1 || bins > 1000 {
		return usage("--bins must be 1-1000")
	}
	if budget <= 0 {
		return usage("--latency-budget must be positive")
	}
	opts := eval.Options{Bins: bins, LatencyBudget: budget}
	var err error
	if opts.Base, opts.BaseRaw, err = load(files[eval.ProfileBase], eval.ProfileBase); err != nil {
		return err
	}
	if path := files[eval.ProfileLoRA]; path != "" {
		if opts.LoRA, opts.LoRARaw, err = load(path, eval.ProfileLoRA); err != nil {
			return err
		}
	}
	if manifestPath != "" {
		raw, err := readBounded(manifestPath)
		if err != nil {
			return err
		}
		m, err := dataset.ParseManifest(bytes.NewReader(raw))
		opts.Manifest = eval.ManifestCheck{Given: true, Manifest: m, Err: err}
	}
	report, err := eval.Evaluate(opts)
	if err != nil {
		return fmt.Errorf("gifford eval: %w", err)
	}
	md := report.Markdown()
	if out == "" {
		_, err := io.WriteString(cmd.OutOrStdout(), md)
		return err //nolint:wrapcheck // writing to stdout has nothing to add
	}
	return writeReport(out, md)
}

func readBounded(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("gifford eval: %w", err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxInputBytes+1))
	if err != nil {
		return nil, fmt.Errorf("gifford eval: read %s: %w", filepath.Base(path), err)
	}
	if len(raw) > maxInputBytes {
		return nil, fmt.Errorf("gifford eval: %s is too large", filepath.Base(path))
	}
	return raw, nil
}

func load(path string, prof eval.Profile) ([]eval.Prediction, []byte, error) {
	raw, err := readBounded(path)
	if err != nil {
		return nil, nil, err
	}
	ps, err := eval.ReadPredictions(bytes.NewReader(raw), prof)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford eval: %s: %w", prof, err)
	}
	return ps, raw, nil
}

// writeReport replaces out whole, through a temporary file and a rename.
func writeReport(out, md string) error {
	dir := filepath.Dir(out)
	tmp, err := os.CreateTemp(dir, ".results-*")
	if err != nil {
		return fmt.Errorf("gifford eval: %w", err)
	}
	_, err = tmp.WriteString(md)
	err = errors.Join(err, tmp.Chmod(0o644), tmp.Close())
	if err == nil {
		err = os.Rename(tmp.Name(), out)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("gifford eval: write report: %w", err)
	}
	return nil
}
