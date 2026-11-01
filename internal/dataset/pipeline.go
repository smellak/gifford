package dataset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

// ErrPipeline is wrapped by every pipeline failure. A failure aborts the
// whole run: no partial dataset is ever returned.
var ErrPipeline = errors.New("dataset pipeline failed")

const (
	// ObservationFormEmptyInputV1 is the existing closed empty-input screen.
	ObservationFormEmptyInputV1 = "empty_input_v1"
	// ObservationFormPermissionDialogV1 is the canonical permission marker.
	ObservationFormPermissionDialogV1 = "permission_dialog_v1"
)

// MaxPipelineWindows bounds PipelineOptions.MaxWindows.
const MaxPipelineWindows = 1 << 20

// Recording is one candidate window as a source reads it: the model's
// features, the reconstructed screen before any sanitising, and where
// it came from. Its screen never leaves the pipeline unsanitised.
type Recording struct {
	WindowID protocol.ULID
	Contract protocol.ModelContract
	// Features are the input, with ScreenLines left empty: the
	// pipeline fills them from the sanitised screen.
	Features          AnalyzeInput
	Screen            redact.Screen
	Label             Label
	LabelSource       LabelSource
	Provenance        Provenance
	EnvironmentDomain EnvironmentDomain
	GroupDigest       protocol.Digest
	// Draft is the human's prompt draft at the window's anchor, from the
	// input events only. It travels with the window for the human-input
	// exclusion and never enters the model's input.
	Draft DraftState
	// Cursor is the cursor on Screen at the anchor, for the human-input
	// exclusion.
	Cursor screenproj.Cursor
	// NoOutputYet says the harness had written no output by the anchor,
	// so Features have no last output to be timed from: the pipeline
	// refuses such a recording, and a build leaves it out first and
	// counts it. It is no feature and never enters the model's input.
	NoOutputYet bool
	// LiveWithheld, set only when the source cuts windows in live form,
	// names why the live Analyst would not get this screen: a reason of
	// session.LiveScreen, or that the draft was not closed. Such a window
	// is no ANALYZE_AGENT_STATE input; a build leaves it out and counts
	// it. Empty when the screen passed, with its input box masked.
	LiveWithheld string
	// ObservationForm is set only by live-input-v2. Permission dialogs also
	// carry their versioned policy and causal episode number for dedupe.
	ObservationForm          string
	ObservationPolicyVersion string
	ObservationEpisode       uint64
	ObservationEpisodeDigest protocol.Digest
	ObservationAnchorDigest  protocol.Digest
}

// DraftState says whether the human had a prompt draft open. Anything
// but DraftClosed, the zero value included, counts as possibly open.
type DraftState string

// DraftState values.
const (
	DraftUnknown DraftState = "unknown"
	DraftOpen    DraftState = "open"
	DraftClosed  DraftState = "closed"
)

// Closed reports whether the draft is known to be closed.
func (d DraftState) Closed() bool { return d == DraftClosed }

// guardState is the draft as the input guard names it for the
// projection: CLOSED when known closed, UNKNOWN otherwise, open included.
func (d DraftState) guardState() protocol.DraftState {
	if d.Closed() {
		return protocol.DraftStateClosed
	}
	return protocol.DraftStateUnknown
}

// RecordingSource yields recordings one at a time, and io.EOF after the
// last. It is the pipeline's only way to read recordings.
type RecordingSource interface {
	Next(ctx context.Context) (Recording, error)
}

// Splitter assigns a group's split. The same group must always get the
// same split; the manifest refuses a group in two splits.
type Splitter interface {
	Split(group protocol.Digest) (Split, error)
}

// PipelineDeps are the pipeline's stages. Every one is required.
type PipelineDeps struct {
	Source RecordingSource
	// Projection are the policies of the screen projection, the same
	// the Analyst serves under: N, the redactor and the domain mask.
	Projection screenproj.Policies
	Splitter   Splitter
}

// PipelineOptions bound a run.
type PipelineOptions struct {
	// MaxWindows bounds the windows of one run; a source that yields
	// more fails the run.
	MaxWindows int
	// Readiness are the preparation gate's thresholds, config.yaml's
	// ml.readiness; nil runs it under DefaultReadinessThresholds.
	Readiness *ReadinessThresholds
	// SplitFile is the SHA-256 of the split file the Splitter assigns
	// from, for the manifest; nil under split-v1.
	SplitFile *protocol.Digest
	// Align is how align labeled the recordings, for the manifest; nil
	// leaves the manifest silent about it.
	Align *AlignPolicy
	// ScriptedInput, for the manifest, says the source counted every
	// draft as closed because a script typed the input.
	ScriptedInput bool
	// ScreenForm, for the manifest, names the form the source cut the
	// screens in: ScreenFormLive, or empty for the replayed screen.
	ScreenForm string
	// SyntheticWindows are already-projected, strictly validated D, E or F
	// train windows. They join captured windows before the final manifest and
	// readiness calculation; they never pass through capture projection.
	SyntheticWindows []Window
}

// Inventory is the result of a run: its windows and their manifest.
type Inventory struct {
	Windows  []Window
	Manifest Manifest
}

// pipelineErr names the failing stage and recording index, never
// content: a stage's own error may quote the screen it was given, so it
// is not wrapped.
func pipelineErr(index int, stage string) error {
	return fmt.Errorf("%w: recording %d: %s", ErrPipeline, index, stage)
}

// RunPipeline reads every recording, projects its screen with
// screenproj.Project (human input excluded, then redacted, then
// domain-masked, then cut to the last N rows), builds and validates its
// window, and inventories the windows into a manifest. It fails closed:
// a missing stage, invalid policies, a recording with no output yet, a
// projection or stage error or an invalid window ends the run with no
// inventory.
func RunPipeline(ctx context.Context, deps PipelineDeps, opts PipelineOptions) (Inventory, error) {
	if deps.Source == nil || deps.Splitter == nil {
		return Inventory{}, fmt.Errorf("%w: a stage is missing", ErrPipeline)
	}
	if err := deps.Projection.Validate(); err != nil {
		return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
	}
	if opts.MaxWindows < 1 || opts.MaxWindows > MaxPipelineWindows {
		return Inventory{}, fmt.Errorf("%w: max windows out of range", ErrPipeline)
	}
	synthetic := slices.Clone(opts.SyntheticWindows)
	if len(synthetic) > opts.MaxWindows {
		return Inventory{}, fmt.Errorf("%w: more synthetic windows than max windows", ErrPipeline)
	}
	for i, w := range synthetic {
		if err := ctx.Err(); err != nil {
			return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
		}
		if err := ValidateSyntheticTrainingWindow(w); err != nil {
			return Inventory{}, fmt.Errorf("%w: synthetic window %d: %w", ErrPipeline, i, err)
		}
	}
	var windows []Window
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
		}
		rec, err := deps.Source.Next(ctx)
		if errors.Is(err, io.EOF) {
			// A source may end because the run was cancelled.
			if err := ctx.Err(); err != nil {
				return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
			}
			break
		}
		if err != nil {
			return Inventory{}, pipelineErr(i, "source failed")
		}
		if len(windows) == opts.MaxWindows-len(synthetic) {
			return Inventory{}, pipelineErr(i, "more captured and synthetic windows than max windows")
		}
		w, err := buildWindow(deps, rec)
		if err != nil {
			// A FieldError, a projection error or a stageError: each
			// names a field or a stage, never content.
			return Inventory{}, fmt.Errorf("%w: recording %d: %w", ErrPipeline, i, err)
		}
		windows = append(windows, w)
	}
	windows = append(windows, synthetic...)
	th := DefaultReadinessThresholds()
	if opts.Readiness != nil {
		th = *opts.Readiness
	}
	m, err := buildManifest(windows, th, opts.SplitFile, opts.Align, opts.ScriptedInput, opts.ScreenForm)
	if err != nil {
		return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
	}
	// A run cancelled while the manifest was built does not succeed.
	if err := ctx.Err(); err != nil {
		return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
	}
	return Inventory{Windows: windows, Manifest: m}, nil
}

// stageError is a stage failure named by a fixed text.
type stageError string

func (e stageError) Error() string { return string(e) }

// buildWindow projects one recording's screen and assembles its window.
// Its errors are a FieldError, an error of screenproj.Project, which
// never holds content, or a fixed stageError.
func buildWindow(deps PipelineDeps, rec Recording) (Window, error) {
	// Before the first effective output there is no quiet time to give: building
	// the window would invent one.
	if rec.NoOutputYet {
		return Window{}, stageError("no output before the anchor")
	}
	// The live Analyst gets no screen for it, so it is no model input.
	if rec.LiveWithheld != "" {
		return Window{}, stageError("screen withheld in live form")
	}
	proj, err := screenproj.Project(screenproj.Input{
		Rows:   rec.Screen.Rows,
		Cols:   rec.Screen.Cols,
		Cursor: rec.Cursor,
		Draft:  rec.Draft.guardState(),
	}, deps.Projection)
	if err != nil {
		return Window{}, err //nolint:wrapcheck // Project's errors name a stage, never content
	}
	split, err := deps.Splitter.Split(rec.GroupDigest)
	if err != nil {
		return Window{}, stageError("split assignment failed")
	}
	// The evidence is the pane's size as recorded.
	cols, rows := int64(rec.Screen.Cols), int64(max(1, len(rec.Screen.Rows)))
	input := rec.Features
	input.ScreenLines = proj.Lines
	v := proj.Versions
	w := Window{
		SchemaVersion: WindowSchemaVersion,
		WindowID:      rec.WindowID,
		Contract:      rec.Contract,
		Input:         input,
		Screen: ScreenEvidence{
			Cols:                     cols,
			Rows:                     rows,
			Redaction:                AppliedPolicy{Applied: true, PolicyVersion: v.Redaction},
			HumanInputExclusion:      AppliedPolicy{Applied: true, PolicyVersion: v.HumanInput},
			Digest:                   ScreenDigest(cols, rows, input.ScreenLines),
			ObservationForm:          rec.ObservationForm,
			ObservationPolicyVersion: rec.ObservationPolicyVersion,
			ObservationEpisodeDigest: rec.ObservationEpisodeDigest,
			ObservationAnchorDigest:  rec.ObservationAnchorDigest,
		},
		Target:            Target{Label: rec.Label, Source: rec.LabelSource},
		Provenance:        rec.Provenance,
		EnvironmentDomain: rec.EnvironmentDomain,
		Policies: Policies{
			RedactionPolicyVersion:  v.Redaction,
			HumanInputPolicyVersion: v.HumanInput,
			DomainMaskPolicyVersion: v.DomainMask,
		},
		Split: SplitAssignment{Split: split, GroupDigest: rec.GroupDigest},
	}
	if err := w.Validate(); err != nil {
		return Window{}, err
	}
	return w, nil
}
