package dataset

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// ErrPipeline is wrapped by every pipeline failure. A failure aborts the
// whole run: no partial dataset is ever returned.
var ErrPipeline = errors.New("dataset pipeline failed")

// maxPipelineWindows bounds PipelineOptions.MaxWindows.
const maxPipelineWindows = 1 << 20

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
	Provenance        Provenance
	EnvironmentDomain EnvironmentDomain
	GroupDigest       protocol.Digest
}

// RecordingSource yields recordings one at a time, and io.EOF after the
// last. It is the pipeline's only way to read recordings.
type RecordingSource interface {
	Next(ctx context.Context) (Recording, error)
}

// ScreenRedactor masks secrets on a screen. *redact.Redactor satisfies
// it.
type ScreenRedactor interface {
	RedactScreen(s redact.Screen) (redact.ScreenResult, error)
}

// HumanInputExcluder removes what a human typed from a screen, such as
// the prompt line and its draft, and names the policy version it ran.
type HumanInputExcluder interface {
	ExcludeHumanInput(s redact.Screen) (redact.Screen, string, error)
}

// DomainMasker masks domain-specific names on a screen, and names the
// policy version it ran.
type DomainMasker interface {
	MaskDomain(s redact.Screen) (redact.Screen, string, error)
}

// Splitter assigns a group's split. The same group must always get the
// same split; the manifest refuses a group in two splits.
type Splitter interface {
	Split(group protocol.Digest) (Split, error)
}

// PipelineDeps are the pipeline's stages. Every one is required.
type PipelineDeps struct {
	Source   RecordingSource
	Excluder HumanInputExcluder
	Redactor ScreenRedactor
	Masker   DomainMasker
	Splitter Splitter
}

// PipelineOptions bound a run.
type PipelineOptions struct {
	// ScreenLines is n, how many of the sanitised screen's last rows
	// enter a window's screen_lines.
	ScreenLines int
	// MaxWindows bounds the windows of one run; a source that yields
	// more fails the run.
	MaxWindows int
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

// RunPipeline reads every recording, sanitises its screen (human input
// excluded, then redacted, then domain-masked), builds and validates its
// window, and inventories the windows into a manifest. It fails closed:
// a missing stage, a stage error, an unnamed policy version or an
// invalid window ends the run with no inventory.
func RunPipeline(ctx context.Context, deps PipelineDeps, opts PipelineOptions) (Inventory, error) {
	if deps.Source == nil || deps.Excluder == nil || deps.Redactor == nil || deps.Masker == nil || deps.Splitter == nil {
		return Inventory{}, fmt.Errorf("%w: a stage is missing", ErrPipeline)
	}
	if opts.ScreenLines < 1 || opts.ScreenLines > maxScreenRows {
		return Inventory{}, fmt.Errorf("%w: screen lines out of range", ErrPipeline)
	}
	if opts.MaxWindows < 1 || opts.MaxWindows > maxPipelineWindows {
		return Inventory{}, fmt.Errorf("%w: max windows out of range", ErrPipeline)
	}
	var windows []Window
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
		}
		rec, err := deps.Source.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Inventory{}, pipelineErr(i, "source failed")
		}
		if len(windows) == opts.MaxWindows {
			return Inventory{}, pipelineErr(i, "more recordings than max windows")
		}
		w, err := buildWindow(deps, opts, rec)
		if err != nil {
			var fe *FieldError
			if errors.As(err, &fe) {
				return Inventory{}, fmt.Errorf("%w: recording %d: %w", ErrPipeline, i, err)
			}
			return Inventory{}, pipelineErr(i, err.Error())
		}
		windows = append(windows, w)
	}
	m, err := BuildManifest(windows)
	if err != nil {
		return Inventory{}, fmt.Errorf("%w: %w", ErrPipeline, err)
	}
	return Inventory{Windows: windows, Manifest: m}, nil
}

// stageError is a stage failure named by a fixed text.
type stageError string

func (e stageError) Error() string { return string(e) }

// buildWindow sanitises one recording's screen and assembles its window.
// Its errors are either a FieldError or a fixed stageError.
func buildWindow(deps PipelineDeps, opts PipelineOptions, rec Recording) (Window, error) {
	screen, humanVersion, err := deps.Excluder.ExcludeHumanInput(rec.Screen)
	if err != nil {
		return Window{}, stageError("human-input exclusion failed")
	}
	redacted, err := deps.Redactor.RedactScreen(screen)
	if err != nil {
		return Window{}, stageError("redaction failed")
	}
	masked, maskVersion, err := deps.Masker.MaskDomain(redact.Screen{Rows: redacted.Rows, Cols: screen.Cols})
	if err != nil {
		return Window{}, stageError("domain mask failed")
	}
	if humanVersion == "" || redacted.PolicyVersion == "" || maskVersion == "" {
		return Window{}, stageError("a stage named no policy version")
	}
	split, err := deps.Splitter.Split(rec.GroupDigest)
	if err != nil {
		return Window{}, stageError("split assignment failed")
	}
	// The evidence is the pane's size as recorded. A stage may blank or
	// drop rows but never widen the pane or add rows.
	if screen.Cols != rec.Screen.Cols || masked.Cols != rec.Screen.Cols ||
		len(masked.Rows) > len(rec.Screen.Rows) {
		return Window{}, stageError("a stage changed the screen size")
	}
	rows := int64(max(1, len(rec.Screen.Rows)))
	lines := masked.Rows[max(0, len(masked.Rows)-opts.ScreenLines):]
	input := rec.Features
	input.ScreenLines = append([]string{}, lines...)
	cols := int64(masked.Cols)
	w := Window{
		SchemaVersion: WindowSchemaVersion,
		WindowID:      rec.WindowID,
		Contract:      rec.Contract,
		Input:         input,
		Screen: ScreenEvidence{
			Cols:                cols,
			Rows:                rows,
			Redaction:           AppliedPolicy{Applied: true, PolicyVersion: redacted.PolicyVersion},
			HumanInputExclusion: AppliedPolicy{Applied: true, PolicyVersion: humanVersion},
			Digest:              ScreenDigest(cols, rows, input.ScreenLines),
		},
		Target:            Target{Label: rec.Label},
		Provenance:        rec.Provenance,
		EnvironmentDomain: rec.EnvironmentDomain,
		Policies: Policies{
			RedactionPolicyVersion:  redacted.PolicyVersion,
			HumanInputPolicyVersion: humanVersion,
			DomainMaskPolicyVersion: maskVersion,
		},
		Split: SplitAssignment{Split: split, GroupDigest: rec.GroupDigest},
	}
	if err := w.Validate(); err != nil {
		return Window{}, err
	}
	return w, nil
}
