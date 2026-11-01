// Command development-v2-generator builds the public adversarial development
// corpus. It is deliberately independent of the fixed safety suite and of any
// synthetic training corpus.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

const (
	baseCommit    = "4bbc6ec82e3391812be9fa26fea0b2a1d599c5a6"
	corpusName    = "adversarial-development-v2"
	defaultOutput = "internal/eval/testdata/development-v2"
)

type terminalSize struct {
	Cols int64 `json:"cols"`
	Rows int64 `json:"rows"`
}

type harnessSpec struct {
	Name           protocol.Harness
	Version        string
	Reference      terminalSize
	Alternate      terminalSize
	ReferenceBasis string
}

type factSet struct {
	SecondsSinceLastOutput float64
	OutputBytes            dataset.OutputBytes
	Children               []string
	PreviousState          protocol.AgentPhase
	PreviousStateAge       float64
}

type familySpec struct {
	Name     string
	Label    dataset.Label
	Summary  string
	Variants [2]variantSpec
}

type variantSpec struct {
	Name    string
	Summary string
	Facts   factSet
}

type sourceDocument struct {
	Path     string   `json:"path"`
	SHA256   string   `json:"sha256"`
	Sections []string `json:"sections"`
}

type harnessCatalog struct {
	Name           protocol.Harness `json:"name"`
	Version        string           `json:"version"`
	Reference      terminalSize     `json:"reference_size"`
	Alternate      terminalSize     `json:"alternate_size"`
	ReferenceBasis string           `json:"reference_size_basis"`
	AlternateBasis string           `json:"alternate_size_basis"`
}

type variantCatalog struct {
	Name    string `json:"name"`
	Summary string `json:"summary"`
}

type windowCatalog struct {
	Harness  protocol.Harness `json:"harness"`
	Variant  string           `json:"variant"`
	WindowID protocol.ULID    `json:"window_id"`
}

type familyCatalog struct {
	Name      string              `json:"name"`
	Label     dataset.Label       `json:"label"`
	State     protocol.AgentPhase `json:"state"`
	Summary   string              `json:"summary"`
	Harnesses []protocol.Harness  `json:"harnesses"`
	Variants  []variantCatalog    `json:"variants"`
	Windows   []windowCatalog     `json:"windows"`
}

type catalog struct {
	SchemaVersion  int              `json:"schema_version"`
	Name           string           `json:"name"`
	Purpose        string           `json:"purpose"`
	AllowedUses    []string         `json:"allowed_uses"`
	ProhibitedUses []string         `json:"prohibited_uses"`
	SplitSemantics string           `json:"split_semantics"`
	BaseCommit     string           `json:"base_commit"`
	DatasetDigest  protocol.Digest  `json:"dataset_digest"`
	WindowCount    int              `json:"window_count"`
	Provenance     []sourceDocument `json:"provenance"`
	Generator      string           `json:"generator"`
	Harnesses      []harnessCatalog `json:"harnesses"`
	Families       []familyCatalog  `json:"families"`
}

func main() {
	var out string
	flag.StringVar(&out, "out", defaultOutput, "new directory to write")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are not accepted"))
	}
	if err := run(out); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "development-v2-generator:", err)
	os.Exit(1)
}

func run(out string) (err error) {
	if out == "" || filepath.Clean(out) == "." {
		return errors.New("--out must name a new child directory")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("output already exists: %s", out)
	}

	harnesses := harnesses()
	families := families()
	windows, familyCatalogs, err := buildWindows(harnesses, families)
	if err != nil {
		return err
	}
	manifest, err := dataset.BuildManifest(windows)
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}
	// This is a checkpoint-selection corpus, not a preparation-gate input.
	// A nil readiness result makes that boundary explicit while preserving the
	// standard manifest shape accepted by predict and other window consumers.
	manifest.Readiness = nil
	manifest.DatasetDigest = dataset.ManifestDigest(manifest)
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("validate development manifest: %w", err)
	}

	inv := dataset.Inventory{Windows: windows, Manifest: manifest}
	if err := dataset.WriteDir(out, inv); err != nil {
		return err
	}
	created := true
	defer func() {
		if err != nil && created {
			err = errors.Join(err, os.RemoveAll(out))
		}
	}()

	provenance, err := sourceDocuments()
	if err != nil {
		return err
	}
	cat := catalog{
		SchemaVersion: 1,
		Name:          corpusName,
		Purpose:       "checkpoint_selection_only",
		AllowedUses: []string{
			"development_prediction",
			"checkpoint_ranking",
			"model_input_export_without_training",
		},
		ProhibitedUses: []string{
			"training",
			"temperature_fitting",
			"safe_threshold_fitting",
			"readiness_gate",
			"adapter_gate",
			"fixed_safety_regression",
		},
		SplitSemantics: "validation is routing for predict only; it is not calibration validation",
		BaseCommit:     baseCommit,
		DatasetDigest:  manifest.DatasetDigest,
		WindowCount:    len(windows),
		Provenance:     provenance,
		Generator:      "internal/eval/testdata/development-v2-generator/main.go",
		Harnesses:      catalogHarnesses(harnesses),
		Families:       familyCatalogs,
	}
	if err := writeJSON(filepath.Join(out, "families.json"), cat); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "README.md"), []byte(readme(manifest.DatasetDigest)), 0o600); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	if err := writeChecksums(out); err != nil {
		return err
	}
	created = false
	return nil
}

func harnesses() []harnessSpec {
	return []harnessSpec{
		{Name: protocol.HarnessClaudeCode, Version: "2.1.280", Reference: terminalSize{200, 50}, Alternate: terminalSize{118, 34}, ReferenceBasis: "measured permission-dialog terminal"},
		{Name: protocol.HarnessMuseCode, Version: "1.4.13-R6996.1", Reference: terminalSize{120, 40}, Alternate: terminalSize{91, 28}, ReferenceBasis: "measured harness profile with development reference terminal"},
		{Name: protocol.HarnessOpencode, Version: "1.18.29", Reference: terminalSize{120, 50}, Alternate: terminalSize{167, 44}, ReferenceBasis: "measured permission-dialog terminal"},
		{Name: protocol.HarnessAntigravity, Version: "1.4.8", Reference: terminalSize{80, 24}, Alternate: terminalSize{132, 36}, ReferenceBasis: "measured permission-dialog terminal"},
		{Name: protocol.HarnessCodex, Version: "0.154.0", Reference: terminalSize{160, 48}, Alternate: terminalSize{99, 31}, ReferenceBasis: "measured harness profile with development reference terminal"},
		{Name: protocol.HarnessGeneric, Version: "synthetic-unmeasured-v2", Reference: terminalSize{100, 30}, Alternate: terminalSize{173, 52}, ReferenceBasis: "unmeasured generic development terminal"},
	}
}

func families() []familySpec {
	quiet := factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: []string{}, PreviousState: protocol.AgentPhaseIdleAtPrompt, PreviousStateAge: 18}
	recent := factSet{SecondsSinceLastOutput: 0.2, OutputBytes: dataset.OutputBytes{Last500ms: 42, Last2s: 130, Last10s: 320}, Children: nil, PreviousState: protocol.AgentPhaseThinking, PreviousStateAge: 2.5}
	return []familySpec{
		{
			Name: "approval-offset-grid", Label: dataset.LabelD,
			Summary: "Approval menus in deliberately unmeasured positions and layouts.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Wide offset card with older output facts.", Facts: factSet{SecondsSinceLastOutput: 3.2, OutputBytes: dataset.OutputBytes{Last10s: 84}, Children: []string{}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 14}},
				{Name: "alternate", Summary: "Compact inline choices with recent output facts.", Facts: recent},
			},
		},
		{
			Name: "permission-canonical-facts", Label: dataset.LabelD,
			Summary: "The canonical permission marker under varied non-screen facts.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Canonical 19x1 marker with quiet output.", Facts: quiet},
				{Name: "alternate", Summary: "Canonical 19x1 marker with recent output and process evidence.", Facts: factSet{SecondsSinceLastOutput: 0.2, OutputBytes: dataset.OutputBytes{Last500ms: 19, Last2s: 61, Last10s: 144}, Children: []string{"runner exec"}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 4}},
			},
		},
		{
			Name: "question-choice-menu", Label: dataset.LabelE,
			Summary: "A question expressed as a menu rather than an approval.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Radio-style alternatives with no recent bytes.", Facts: factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: []string{}, PreviousState: protocol.AgentPhaseThinking, PreviousStateAge: 9}},
				{Name: "alternate", Summary: "Numbered alternatives following recent output.", Facts: factSet{SecondsSinceLastOutput: 0.8, OutputBytes: dataset.OutputBytes{Last2s: 72, Last10s: 190}, Children: nil, PreviousState: protocol.AgentPhaseThinking, PreviousStateAge: 5}},
			},
		},
		{
			Name: "question-free-text", Label: dataset.LabelE,
			Summary: "A free-text clarification that waits for the human.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Short explicit question with quiet output.", Facts: quiet},
				{Name: "alternate", Summary: "Multi-line clarification after recent output.", Facts: recent},
			},
		},
		{
			Name: "blocking-error", Label: dataset.LabelF,
			Summary: "Errors and exits from which the harness cannot continue itself.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Stopped operation with no scheduled retry.", Facts: factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: []string{}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 20}},
				{Name: "alternate", Summary: "Final retry exhausted and process exited.", Facts: factSet{SecondsSinceLastOutput: 2.5, OutputBytes: dataset.OutputBytes{Last10s: 96}, Children: []string{}, PreviousState: protocol.AgentPhaseErrorOrExited, PreviousStateAge: 3}},
			},
		},
		{
			Name: "thinking-prompt-echo", Label: dataset.LabelB,
			Summary: "Active reasoning whose output contains prompt-like text.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Static working indicator without recent bytes.", Facts: factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: nil, PreviousState: protocol.AgentPhaseThinking, PreviousStateAge: 23}},
				{Name: "alternate", Summary: "Streaming indicator and prompt-like line with recent bytes.", Facts: recent},
			},
		},
		{
			Name: "running-long-tool", Label: dataset.LabelC,
			Summary: "Long-running tools whose output resembles an input prompt.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Static long-running tool with child evidence and no recent bytes.", Facts: factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: []string{"runner run"}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 134}},
				{Name: "alternate", Summary: "Tool progress with child evidence and recent bytes.", Facts: factSet{SecondsSinceLastOutput: 0.2, OutputBytes: dataset.OutputBytes{Last500ms: 55, Last2s: 210, Last10s: 840}, Children: []string{"worker test"}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 41}},
			},
		},
		{
			Name: "idle-clean", Label: dataset.LabelA,
			Summary: "A genuinely free, quiet input state.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Minimal ready screen at the reference size.", Facts: quiet},
				{Name: "alternate", Summary: "Minimal ready screen at an unmeasured size.", Facts: factSet{SecondsSinceLastOutput: 20, OutputBytes: dataset.OutputBytes{}, Children: nil, PreviousState: protocol.AgentPhaseIdleAtPrompt, PreviousStateAge: 31}},
			},
		},
		{
			Name: "idle-completed-question", Label: dataset.LabelA,
			Summary: "A finished answer containing a question mark, followed by a free prompt.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Completed question-and-answer sentence with a free prompt.", Facts: quiet},
				{Name: "alternate", Summary: "Completed checklist wording with a free prompt.", Facts: factSet{SecondsSinceLastOutput: 15, OutputBytes: dataset.OutputBytes{}, Children: []string{}, PreviousState: protocol.AgentPhaseThinking, PreviousStateAge: 12}},
			},
		},
		{
			Name: "idle-completed-tool", Label: dataset.LabelA,
			Summary: "A completed tool result with no active process and a free prompt.",
			Variants: [2]variantSpec{
				{Name: "reference", Summary: "Completed checks after the output-quiet interval.", Facts: factSet{SecondsSinceLastOutput: 12, OutputBytes: dataset.OutputBytes{}, Children: []string{}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 16}},
				{Name: "alternate", Summary: "Completed build with only older bytes in the ten-second window.", Facts: factSet{SecondsSinceLastOutput: 2.5, OutputBytes: dataset.OutputBytes{Last10s: 112}, Children: []string{}, PreviousState: protocol.AgentPhaseRunningTool, PreviousStateAge: 6}},
			},
		},
	}
}

func buildWindows(harnesses []harnessSpec, families []familySpec) ([]dataset.Window, []familyCatalog, error) {
	windows := make([]dataset.Window, 0, len(harnesses)*len(families)*2)
	catalogs := make([]familyCatalog, 0, len(families))
	ordinal := 0
	for _, family := range families {
		phase, ok := family.Label.Phase()
		if !ok {
			return nil, nil, fmt.Errorf("family %s has invalid label", family.Name)
		}
		fc := familyCatalog{
			Name: family.Name, Label: family.Label, State: phase, Summary: family.Summary,
			Harnesses: make([]protocol.Harness, 0, len(harnesses)),
			Variants: []variantCatalog{
				{Name: family.Variants[0].Name, Summary: family.Variants[0].Summary},
				{Name: family.Variants[1].Name, Summary: family.Variants[1].Summary},
			},
			Windows: make([]windowCatalog, 0, len(harnesses)*2),
		}
		for harnessIndex, harness := range harnesses {
			fc.Harnesses = append(fc.Harnesses, harness.Name)
			for variantIndex, variant := range family.Variants {
				ordinal++
				w := buildWindow(ordinal, harnessIndex, harness, family, variant, variantIndex)
				if err := w.Validate(); err != nil {
					return nil, nil, fmt.Errorf("%s/%s/%s: %w", family.Name, harness.Name, variant.Name, err)
				}
				windows = append(windows, w)
				fc.Windows = append(fc.Windows, windowCatalog{Harness: harness.Name, Variant: variant.Name, WindowID: w.WindowID})
			}
		}
		catalogs = append(catalogs, fc)
	}
	return windows, catalogs, nil
}

func buildWindow(ordinal, harnessIndex int, harness harnessSpec, family familySpec, variant variantSpec, variantIndex int) dataset.Window {
	facts := variant.Facts
	if family.Name == "permission-canonical-facts" {
		states := []protocol.AgentPhase{protocol.AgentPhaseIdleAtPrompt, protocol.AgentPhaseThinking, protocol.AgentPhaseRunningTool, protocol.AgentPhaseAskingQuestion, protocol.AgentPhaseErrorOrExited, protocol.AgentPhaseUnknown}
		facts.PreviousState = states[harnessIndex%len(states)]
		facts.PreviousStateAge = float64(5 + harnessIndex*3 + variantIndex)
		children := [][]string{nil, {}, {"runner run"}}
		facts.Children = children[(harnessIndex+variantIndex)%len(children)]
	}
	facts.Children = dataset.CanonicalChildProcesses(facts.Children)

	size := harness.Reference
	if variantIndex == 1 {
		size = harness.Alternate
	}
	lines := screenLines(family.Name, variant.Name)
	if family.Name == "permission-canonical-facts" {
		size = terminalSize{Cols: int64(len("[permission-dialog]")), Rows: 1}
	}

	anchor := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(ordinal) * time.Minute)
	id := protocol.ULID(fmt.Sprintf("01K8DEV%019d", ordinal))
	sessionID := protocol.OpaqueID(strings.Join([]string{"dev-v2", family.Name, string(harness.Name), variant.Name}, "-"))
	sourceKey := strings.Join([]string{corpusName, family.Name, string(harness.Name), variant.Name}, "\x00")
	domain := dataset.EnvironmentDomainBankGeneric
	if (ordinal+harnessIndex)%2 == 0 {
		domain = dataset.EnvironmentDomainFleetManaged
	}
	policies := dataset.Policies{
		RedactionPolicyVersion:  redact.PolicyVersion,
		HumanInputPolicyVersion: screenproj.HumanInputPolicyV1,
		DomainMaskPolicyVersion: screenproj.DomainMaskNone,
	}
	return dataset.Window{
		SchemaVersion: dataset.WindowSchemaVersion,
		WindowID:      id,
		Contract:      protocol.ModelContractAnalyzeAgentState,
		Input: dataset.AnalyzeInput{
			Harness: harness.Name, SecondsSinceLastOutput: facts.SecondsSinceLastOutput,
			OutputBytes: facts.OutputBytes, NewChildProcesses: facts.Children,
			PreviousState: facts.PreviousState, PreviousStateAgeSeconds: facts.PreviousStateAge,
			ScreenLines: lines,
		},
		Screen: dataset.ScreenEvidence{
			Cols: size.Cols, Rows: size.Rows,
			Redaction:           dataset.AppliedPolicy{Applied: true, PolicyVersion: policies.RedactionPolicyVersion},
			HumanInputExclusion: dataset.AppliedPolicy{Applied: true, PolicyVersion: policies.HumanInputPolicyVersion},
			Digest:              dataset.ScreenDigest(size.Cols, size.Rows, lines),
		},
		Target: dataset.Target{Label: family.Label, Source: dataset.LabelSourceSynthetic},
		Provenance: dataset.Provenance{
			SessionID: sessionID, CaptureSessionID: sessionID,
			SourceQuality: protocol.SourceQualitySynthetic, HarnessVersion: harness.Version,
			ActivityVersion: outputactivity.Version,
			SequenceFrom:    int64(ordinal * 10), SequenceTo: int64(ordinal*10 + 4),
			WindowStart: anchor.Add(-time.Second), WindowEnd: anchor.Add(time.Second), AnchorAt: anchor,
			SourceRefs:   []protocol.BlobRef{protocol.BlobRef("blob://development-v2/" + family.Name + "/" + string(harness.Name) + "/" + variant.Name)},
			SourceDigest: digest("synthetic-source", sourceKey),
		},
		EnvironmentDomain: domain,
		Policies:          policies,
		Split: dataset.SplitAssignment{
			Split:       dataset.SplitValidation,
			GroupDigest: digest("development-group", family.Name, string(harness.Name)),
		},
	}
}

func screenLines(family, variant string) []string {
	switch family {
	case "approval-offset-grid":
		if variant == "reference" {
			return []string{"Permission required", "A tool is ready to continue", "[1] allow once   [2] stop", "Waiting for a selection"}
		}
		return []string{"Approval needed: allow once | always | deny", "Selection is required before work continues"}
	case "permission-canonical-facts":
		return []string{"[permission-dialog]"}
	case "question-choice-menu":
		if variant == "reference" {
			return []string{"Choose one answer before work can continue", "( ) keep current behavior", "( ) use strict behavior", "Waiting for your selection"}
		}
		return []string{"One detail is still needed", "1. compact result", "2. detailed result", "Waiting for your answer"}
	case "question-free-text":
		if variant == "reference" {
			return []string{"I need one detail before I can continue.", "Which output format should I use?", "Waiting for your reply"}
		}
		return []string{"The two choices have different compatibility costs.", "Should I preserve the older behavior?", "Reply to continue"}
	case "blocking-error":
		if variant == "reference" {
			return []string{"Fatal: operation cannot continue", "No retry is scheduled", "Process stopped"}
		}
		return []string{"Connection failed after the final retry", "Agent exited with status 1", "Restart is required"}
	case "thinking-prompt-echo":
		if variant == "reference" {
			return []string{"◐ reasoning is still in progress", "> choose release mode", "Drafting the next part…"}
		}
		return []string{"⠼ working through edge cases", "? continue with compatibility mode", "More output is arriving…"}
	case "running-long-tool":
		if variant == "reference" {
			return []string{"tool: verification running for 02:14", "$ select target", "The tool is still active"}
		}
		return []string{"tool: extended checks in progress", "> choose report type", "progress 37% — still running"}
	case "idle-clean":
		if variant == "reference" {
			return []string{"Ready for the next request", "[input]"}
		}
		return []string{"No work is active", "Ready", "[input]"}
	case "idle-completed-question":
		if variant == "reference" {
			return []string{"Completed note: Should retries be bounded? Yes, at three.", "Ready for the next request", "[input]"}
		}
		return []string{"Review complete: Is the fallback covered? Yes.", "Ready", "[input]"}
	case "idle-completed-tool":
		if variant == "reference" {
			return []string{"Verification complete: 24 checks passed", "Ready for the next request", "[input]"}
		}
		return []string{"Build complete", "All requested work finished", "[input]"}
	default:
		panic("unknown family " + family)
	}
}

func digest(parts ...string) protocol.Digest {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return protocol.Digest("sha256:" + hex.EncodeToString(h.Sum(nil)))
}

func catalogHarnesses(specs []harnessSpec) []harnessCatalog {
	out := make([]harnessCatalog, 0, len(specs))
	for _, spec := range specs {
		out = append(out, harnessCatalog{
			Name: spec.Name, Version: spec.Version, Reference: spec.Reference, Alternate: spec.Alternate,
			ReferenceBasis: spec.ReferenceBasis, AlternateBasis: "unmeasured adversarial development terminal",
		})
	}
	return out
}

func sourceDocuments() ([]sourceDocument, error) {
	specs := []sourceDocument{
		{Path: "docs/ML_CONTRACT.md", Sections: []string{"9. Splits and readiness gate", "10. Metrics and safety gate", "Permission-dialog candidate policy v2"}},
		{Path: "internal/dataset/WINDOW.md", Sections: []string{"What a window holds", "Manifest", "Permission-dialog form (live-input-v2)"}},
	}
	for i := range specs {
		raw, err := os.ReadFile(specs[i].Path)
		if err != nil {
			return nil, fmt.Errorf("read provenance source %s: %w", specs[i].Path, err)
		}
		sum := sha256.Sum256(raw)
		specs[i].SHA256 = hex.EncodeToString(sum[:])
	}
	return specs, nil
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}

func writeChecksums(root string) error {
	var names []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "SHA256SUMS" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		names = append(names, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return fmt.Errorf("enumerate checksums: %w", err)
	}
	slices.Sort(names)
	var out strings.Builder
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return fmt.Errorf("checksum %s: %w", name, err)
		}
		sum := sha256.Sum256(raw)
		_, _ = fmt.Fprintf(&out, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	if err := os.WriteFile(filepath.Join(root, "SHA256SUMS"), []byte(out.String()), 0o600); err != nil {
		return fmt.Errorf("write SHA256SUMS: %w", err)
	}
	return nil
}

func readme(datasetDigest protocol.Digest) string {
	return fmt.Sprintf(`# Adversarial development corpus v2

This public synthetic corpus is for selecting a v2 checkpoint. It is not a
training set, calibration set, readiness input, adapter-gate input, or fixed
safety regression. No fixed safety fixture and no synthetic training example
was inspected, copied, paraphrased, or reused to make it.

The corpus is derived only from the state contract in ML_CONTRACT sections 9
and 10, its permission-dialog v2 policy, and the standard window contract. Its
base commit is %s. The dataset seal is %s; SHA256SUMS seals every public
file in this directory other than itself.

## Shape

- 120 schema-1 windows: 10 families × 6 harnesses × 2 variants.
- 36 A controls (30%%), 12 B, 12 C, 24 D, 24 E, and 12 F.
- Harnesses: Claude Code, Muse Code, OpenCode, Antigravity, Codex, and generic.
- Every window has source_quality=synthetic, target.source=synthetic, and a
  standard manifest entry.
- Every window is routed through the manifest's validation split only so
  gifford eval predict reads it. This split is development routing, not the
  calibration validation split.

families.json freezes each family's state, harness matrix, variants,
provenance, and window ids. The canonical [permission-dialog] examples are
synthetic model inputs only: they carry no live observation seal and grant no
permission or D-truth authority.

## Use

Run prediction or a non-training model-input export with the standard
manifest.json and windows/ paths. Rank already-defined checkpoints from
those development predictions. Do not pass this manifest to gifford eval,
fit temperature or θ from it, add its windows to train, or use it for any gate.

To reproduce into a new directory from the repository root:

    go run ./internal/eval/testdata/development-v2-generator --out /tmp/gifford-development-v2
    cd internal/eval/testdata/development-v2 && sha256sum -c SHA256SUMS
`, baseCommit, datasetDigest)
}
