// Package eval implements the `gifford eval` subcommand: it scores
// prediction files of the base model and the Analyst LoRA, runs the LoRA
// gate and renders docs/RESULTS.md; `gifford eval predict` runs the model
// over a dataset's windows to write those files.
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
	var safetyPredictions []string
	var manifest, safetyManifest, candidatePrompt, out string
	var bins int
	var budget time.Duration
	cmd := &cobra.Command{
		Use:   "eval --profile base=FILE [--profile analyst-lora=FILE | --candidate-prompt VERSION=FILE] [flags]",
		Short: "Evaluate Glimmer (base or Analyst LoRA) and the protocol",
		Long: `Score prediction files (one JSONL line per window). Calibrates by temperature
scaling on validation, sets the SAFE threshold, and scores test and validation.
With two inference profiles it runs the LoRA gate. With --candidate-prompt it
compares two serial base-model prompt runs with identical served model/runtime
identity, replays evaluable guards, and checks both prompts on the fixed
synthetic safety suite. Safety fixtures never fit calibration. The report holds
numbers, provenance and harness names only. Same inputs, same bytes.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, profiles, candidatePrompt, safetyPredictions, manifest, safetyManifest, out, bins, budget)
		},
	}
	cmd.Flags().StringArrayVar(&profiles, "profile", nil, "profile=FILE, for base and analyst-lora")
	cmd.Flags().StringVar(&candidatePrompt, "candidate-prompt", "", "ANALYZE prompt VERSION=FILE to compare with base")
	cmd.Flags().StringArrayVar(&safetyPredictions, "safety-predictions", nil, "baseline=FILE and candidate=FILE for the fixed safety suite")
	cmd.Flags().StringVar(&manifest, "manifest", "", "dataset manifest the predictions were drawn from (gate criterion 0)")
	cmd.Flags().StringVar(&safetyManifest, "safety-manifest", "", "manifest of the fixed synthetic safety suite")
	cmd.Flags().StringVar(&out, "out", "", "where to write the report (stdout when empty)")
	cmd.Flags().IntVar(&bins, "bins", eval.DefaultBins, "ECE bins")
	cmd.Flags().DurationVar(&budget, "latency-budget", eval.DefaultLatencyBudget, "p95 latency budget of the candidate")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usage(err.Error())
	})
	cmd.AddCommand(newPredictCmd())
	return cmd
}

func usage(msg string) error {
	return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford eval: %s", msg)}
}

func run(cmd *cobra.Command, profiles []string, candidateArg string, safetyArgs []string, manifestPath, safetyManifestPath, out string, bins int, budget time.Duration) error {
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
	candidateVersion, candidatePath, hasCandidate := strings.Cut(candidateArg, "=")
	if candidateArg != "" && (!hasCandidate || candidateVersion == "" || candidatePath == "") {
		return usage("--candidate-prompt takes VERSION=FILE")
	}
	if candidatePath != "" && files[eval.ProfileLoRA] != "" {
		return usage("--candidate-prompt and --profile analyst-lora are mutually exclusive")
	}
	safetyFiles := map[string]string{}
	for _, item := range safetyArgs {
		role, path, ok := strings.Cut(item, "=")
		if !ok || path == "" || role != "baseline" && role != "candidate" {
			return usage("--safety-predictions takes baseline=FILE or candidate=FILE")
		}
		if safetyFiles[role] != "" {
			return usage("--safety-predictions " + role + " given twice")
		}
		safetyFiles[role] = path
	}
	if candidatePath != "" {
		if manifestPath == "" || safetyManifestPath == "" || safetyFiles["baseline"] == "" || safetyFiles["candidate"] == "" {
			return usage("prompt comparison requires --manifest, --safety-manifest, and both --safety-predictions roles")
		}
	} else if len(safetyArgs) > 0 || safetyManifestPath != "" {
		return usage("safety inputs require --candidate-prompt")
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
	if candidatePath != "" {
		opts.CandidatePrompt = candidateVersion
		if opts.Candidate, opts.CandidateRaw, err = load(candidatePath, eval.ProfileBase); err != nil {
			return err
		}
		if opts.SafetyBase, opts.SafetyBaseRaw, err = load(safetyFiles["baseline"], eval.ProfileBase); err != nil {
			return err
		}
		if opts.SafetyCandidate, opts.SafetyCandidateRaw, err = load(safetyFiles["candidate"], eval.ProfileBase); err != nil {
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
	if safetyManifestPath != "" {
		raw, err := readBounded(safetyManifestPath)
		if err != nil {
			return err
		}
		m, err := dataset.ParseManifest(bytes.NewReader(raw))
		opts.SafetyManifest = eval.ManifestCheck{Given: true, Manifest: m, Err: err}
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
	return writeFile(out, ".results-*", []byte(md))
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

// writeFile replaces out whole, through a temporary file and a rename.
func writeFile(out, pattern string, data []byte) error {
	dir := filepath.Dir(out)
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return fmt.Errorf("gifford eval: %w", err)
	}
	_, err = tmp.Write(data)
	err = errors.Join(err, tmp.Chmod(0o644), tmp.Close())
	if err == nil {
		err = os.Rename(tmp.Name(), out)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("gifford eval: write %s: %w", filepath.Base(out), err)
	}
	return nil
}
