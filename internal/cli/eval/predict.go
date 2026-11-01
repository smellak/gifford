package eval

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"

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
	profile, promptVersion, manifest, windows, out, gateway    string
	adapterName, adapterRevision, adapterSHA256, trainingRunID string
	concurrency                                                int
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
served model/runtime identity, concurrency, and output-quiet evidence. An
analyst-lora run also requires the exported adapter name, revision, SHA-256 and
training run, which are sealed into every output line. The gateway configuration
names its endpoint by environment variables. A summary goes to stderr.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPredict(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.profile, "profile", "", "inference profile to run: base or analyst-lora")
	fl.StringVar(&f.promptVersion, "prompt-version", analyst.PromptVersion,
		"embedded ANALYZE_AGENT_STATE prompt version: "+strings.Join(analyst.AnalyzePromptVersions(), ", "))
	fl.StringVar(&f.manifest, "manifest", "", "dataset manifest")
	fl.StringVar(&f.windows, "windows", "", "directory of the windows, one <window_id>.json each")
	fl.StringVar(&f.gateway, "gateway", "", "gateway configuration (YAML)")
	fl.StringVar(&f.out, "out", "", "where to write the predictions")
	fl.StringVar(&f.adapterName, "adapter-name", "", "exported adapter name (analyst-lora only)")
	fl.StringVar(&f.adapterRevision, "adapter-revision", "", "exported adapter revision (analyst-lora only)")
	fl.StringVar(&f.adapterSHA256, "adapter-sha256", "", "exported adapter SHA-256 (analyst-lora only)")
	fl.StringVar(&f.trainingRunID, "training-run-id", "", "training run that exported the adapter (analyst-lora only)")
	fl.IntVar(&f.concurrency, "concurrency", eval.DefaultConcurrency, "most calls in flight")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usage(err.Error())
	})
	return cmd
}

func runPredict(cmd *cobra.Command, f predictFlags) error {
	prof := eval.Profile(f.profile)
	adapter := eval.AdapterProvenance{
		AdapterName: f.adapterName, AdapterRevision: f.adapterRevision,
		AdapterSHA256: protocol.SHA256Hex(f.adapterSHA256), TrainingRunID: protocol.OpaqueID(f.trainingRunID),
	}
	adapterGiven := f.adapterName != "" || f.adapterRevision != "" || f.adapterSHA256 != "" || f.trainingRunID != ""
	switch {
	case !prof.Valid():
		return usage("--profile takes base or analyst-lora")
	case !slices.Contains(analyst.AnalyzePromptVersions(), f.promptVersion):
		return usage("--prompt-version takes one of " + strings.Join(analyst.AnalyzePromptVersions(), ", "))
	case f.manifest == "" || f.windows == "" || f.gateway == "" || f.out == "":
		return usage("--manifest, --windows, --gateway and --out are required")
	case f.concurrency < 1 || f.concurrency > 64:
		return usage("--concurrency must be 1-64")
	case prof == eval.ProfileLoRA && !adapter.Valid():
		return usage("analyst-lora requires valid --adapter-name, --adapter-revision, --adapter-sha256 and --training-run-id")
	case prof == eval.ProfileBase && adapterGiven:
		return usage("adapter provenance flags are only valid with analyst-lora")
	}
	raw, err := readBounded(f.manifest)
	if err != nil {
		return err
	}
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("gifford eval predict: %w", err)
	}
	var boundAdapter *eval.AdapterProvenance
	if prof == eval.ProfileLoRA {
		boundAdapter = &adapter
	}
	gw, err := newGateway(f.gateway, prof, f.promptVersion, boundAdapter)
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
	options := eval.PredictOptions{
		Profile: prof, PromptVersion: f.promptVersion, Concurrency: f.concurrency,
	}
	options.Adapter = boundAdapter
	out, sum, err := eval.Predict(ctx, gw, m, windowLoader(root), options)
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
// Analyst's prompts, ANALYZE_AGENT_STATE at version, and the default
// redactor. It refuses a configuration that serves ANALYZE_AGENT_STATE
// with another profile than prof or another LoRA adapter than the sealed
// provenance. Call records are not kept: the predictions file is the run's
// record.
func newGateway(
	path string,
	prof eval.Profile,
	version string,
	adapter *eval.AdapterProvenance,
) (*gateway.Gateway, error) {
	raw, err := readBounded(path)
	if err != nil {
		return nil, err
	}
	cfg, err := gateway.ParseConfig(bytes.NewReader(raw), os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	served := cfg.Profiles[protocol.ModelContractAnalyzeAgentState]
	if got := served.InferenceProfile; got != protocol.InferenceProfile(prof) {
		return nil, usage(fmt.Sprintf("the gateway serves ANALYZE_AGENT_STATE with %s, not %s", got, prof))
	}
	if prof == eval.ProfileLoRA && (adapter == nil || served.AdapterName != adapter.AdapterName ||
		served.AdapterRevision != adapter.AdapterRevision || served.AdapterSHA256 != adapter.AdapterSHA256) {
		return nil, usage("the gateway ANALYZE_AGENT_STATE adapter does not match the prediction provenance")
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
		IDs: randomIDs{}, Prompts: analyst.Prompts{Analyze: version},
	})
	if err != nil {
		return nil, fmt.Errorf("gifford eval predict: %w", err)
	}
	return gw, nil
}

type discardRecords struct{}

func (discardRecords) RecordCall(context.Context, protocol.GlimmerCallRecord, string) error {
	return nil
}

type randomIDs struct{}

func (randomIDs) NewCallID() protocol.OpaqueID {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return protocol.OpaqueID("eval-" + hex.EncodeToString(b[:]))
}
