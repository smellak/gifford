package eval

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// predictFlags are the flags of gifford eval predict.
type predictFlags struct {
	profile, promptVersion, manifest, windows, out, gateway string
	concurrency                                             int
}

func newPredictCmd() *cobra.Command {
	var f predictFlags
	cmd := &cobra.Command{
		Use:   "predict --profile base|analyst-lora --prompt-version VERSION --manifest FILE --windows DIR --gateway FILE --out FILE",
		Short: "Run Glimmer over the validation and test windows of a dataset",
		Long: `Read every validation and test window the manifest lists, from DIR/<window_id>.json,
check each against its manifest entry (digest, split, label), send it to the
gateway as ANALYZE_AGENT_STATE, as the Analyst does, and write the predictions
file that gifford eval scores: one line per window, sorted by window_id. A call
that fails is written as an invalid line with its error class; an answer from
another inference profile aborts the run. Every line records the prompt version,
served model/runtime identity, concurrency, and output-quiet evidence. The
gateway configuration names its endpoint by environment variables. A summary
goes to stderr.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPredict(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.profile, "profile", "", "inference profile to run: base or analyst-lora")
	fl.StringVar(&f.promptVersion, "prompt-version", analyst.PromptVersion, "embedded ANALYZE_AGENT_STATE prompt version")
	fl.StringVar(&f.manifest, "manifest", "", "dataset manifest")
	fl.StringVar(&f.windows, "windows", "", "directory of the windows, one <window_id>.json each")
	fl.StringVar(&f.gateway, "gateway", "", "gateway configuration (YAML)")
	fl.StringVar(&f.out, "out", "", "where to write the predictions")
	fl.IntVar(&f.concurrency, "concurrency", eval.DefaultConcurrency, "most calls in flight")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usage(err.Error())
	})
	return cmd
}

func runPredict(cmd *cobra.Command, f predictFlags) error {
	prof := eval.Profile(f.profile)
	switch {
	case !prof.Valid():
		return usage("--profile takes base or analyst-lora")
	case f.promptVersion != analyst.PromptVersion:
		return usage("--prompt-version is not embedded in this binary")
	case f.manifest == "" || f.windows == "" || f.gateway == "" || f.out == "":
		return usage("--manifest, --windows, --gateway and --out are required")
	case f.concurrency < 1 || f.concurrency > 64:
		return usage("--concurrency must be 1-64")
	}
	raw, err := readBounded(f.manifest)
	if err != nil {
		return err
	}
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("gifford eval predict: %w", err)
	}
	gw, err := newGateway(f.gateway, prof)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(f.windows)
	if err != nil {
		return fmt.Errorf("gifford eval predict: windows: %w", err)
	}
	defer func() { _ = root.Close() }()
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	out, sum, err := eval.Predict(ctx, gw, m, windowLoader(root), eval.PredictOptions{
		Profile: prof, PromptVersion: f.promptVersion, Concurrency: f.concurrency,
	})
	if err != nil {
		return fmt.Errorf("gifford eval predict: %w", err)
	}
	if err := writeFile(f.out, ".predictions-*", out); err != nil {
		return err
	}
	for _, l := range sum.Lines() {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "gifford eval predict: "+l)
	}
	return nil
}

// windowLoader reads windows inside root only: a name never leaves it.
func windowLoader(root *os.Root) eval.WindowLoader {
	return func(id protocol.ULID) (dataset.Window, error) {
		if !id.Valid() {
			return dataset.Window{}, fmt.Errorf("%w: not a window id", dataset.ErrInvalidWindow)
		}
		f, err := root.Open(string(id) + ".json")
		if err != nil {
			return dataset.Window{}, fmt.Errorf("open window: %w", err)
		}
		defer func() { _ = f.Close() }()
		return dataset.ParseWindow(f)
	}
}

// newGateway builds the gateway from its configuration, with the
// Analyst's prompt and the default redactor. It refuses a configuration
// that serves ANALYZE_AGENT_STATE with another profile than prof. Call
// records are not kept: the predictions file is the run's record.
func newGateway(path string, prof eval.Profile) (*gateway.Gateway, error) {
	raw, err := readBounded(path)
	if err != nil {
		return nil, err
	}
	cfg, err := gateway.ParseConfig(bytes.NewReader(raw), os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	if got := cfg.Profiles[protocol.ModelContractAnalyzeAgentState].InferenceProfile; got != protocol.InferenceProfile(prof) {
		return nil, usage(fmt.Sprintf("the gateway serves ANALYZE_AGENT_STATE with %s, not %s", got, prof))
	}
	tr, err := gateway.NewHTTPTransport(cfg.Endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	red, err := redact.New(redact.Options{})
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	gw, err := gateway.New(cfg, gateway.Deps{
		Transport: tr, Redactor: red, Recorder: discardRecords{}, Clock: gateway.SystemClock{},
		IDs: randomIDs{}, Prompts: analyst.Prompts{},
	})
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	return gw, nil
}

type discardRecords struct{}

func (discardRecords) RecordCall(context.Context, protocol.GlimmerCallRecord) error { return nil }

type randomIDs struct{}

func (randomIDs) NewCallID() protocol.OpaqueID {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return protocol.OpaqueID("eval-" + hex.EncodeToString(b[:]))
}
