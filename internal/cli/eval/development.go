package eval

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/dataset"
	coreeval "github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
)

type developmentFlags struct {
	profiles     []string
	manifest     string
	datasetSHA   string
	calibrations string
	out          string
	bins         int
}

func newDevelopmentCmd() *cobra.Command {
	var f developmentFlags
	cmd := &cobra.Command{
		Use:   "development --profile base=FILE --profile analyst-lora=FILE --manifest FILE --dataset-sha256 SHA256 --sealed-calibrations FILE",
		Short: "Score isolated synthetic checkpoint-development predictions",
		Long: `Score a separately sealed synthetic development corpus with T and theta
already frozen in a val-cal calibration artifact. This command never fits
calibration, never mixes development windows with val-cmp, and has no gate.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDevelopment(cmd, f)
		},
	}
	cmd.Flags().StringArrayVar(&f.profiles, "profile", nil, "profile=FILE, required for base and analyst-lora")
	cmd.Flags().StringVar(&f.manifest, "manifest", "", "sealed synthetic development manifest")
	cmd.Flags().StringVar(&f.datasetSHA, "dataset-sha256", "", "expected dataset sha256 independent of the manifest")
	cmd.Flags().StringVar(&f.calibrations, "sealed-calibrations", "", "frozen val-cal calibration JSON")
	cmd.Flags().StringVar(&f.out, "out", "", "where to write the report (stdout when empty)")
	cmd.Flags().IntVar(&f.bins, "bins", coreeval.DefaultBins, "ECE bins")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usage(err.Error()) })
	return cmd
}

func runDevelopment(cmd *cobra.Command, f developmentFlags) error {
	files := map[coreeval.Profile]string{}
	for _, item := range f.profiles {
		name, path, ok := strings.Cut(item, "=")
		profile := coreeval.Profile(name)
		if !ok || path == "" || !profile.Valid() {
			return usage("development --profile takes base=FILE or analyst-lora=FILE")
		}
		if files[profile] != "" {
			return usage("development --profile " + name + " given twice")
		}
		files[profile] = path
	}
	if files[coreeval.ProfileBase] == "" || files[coreeval.ProfileLoRA] == "" {
		return usage("development requires both base and analyst-lora profiles")
	}
	seal := protocol.Digest(f.datasetSHA)
	if f.manifest == "" || !seal.Valid() || f.calibrations == "" {
		return usage("development requires --manifest, --dataset-sha256, and --sealed-calibrations")
	}
	if f.bins < 1 || f.bins > 1000 {
		return usage("--bins must be 1-1000")
	}
	base, _, err := load(files[coreeval.ProfileBase], coreeval.ProfileBase)
	if err != nil {
		return err
	}
	lora, _, err := load(files[coreeval.ProfileLoRA], coreeval.ProfileLoRA)
	if err != nil {
		return err
	}
	manifestRaw, err := readBounded(f.manifest)
	if err != nil {
		return err
	}
	manifest, parseErr := dataset.ParseManifest(bytes.NewReader(manifestRaw))
	calibrations, calibrationRaw, err := loadCalibrations(f.calibrations)
	if err != nil {
		return err
	}
	report, err := coreeval.ScoreDevelopment(coreeval.DevelopmentOptions{
		Base: base, LoRA: lora,
		Manifest:        coreeval.ManifestCheck{Given: true, Manifest: manifest, Err: parseErr},
		DatasetSHA256:   seal,
		Calibrations:    &calibrations,
		CalibrationsRaw: calibrationRaw,
		Bins:            f.bins,
	})
	if err != nil {
		return fmt.Errorf("gifford eval development: %w", err)
	}
	markdown := report.Markdown()
	if f.out == "" {
		_, err = io.WriteString(cmd.OutOrStdout(), markdown)
		return err //nolint:wrapcheck // writing to stdout has nothing to add
	}
	return writeFile(f.out, ".development-*", []byte(markdown))
}
