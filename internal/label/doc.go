// Package label collects ground-truth labels for the Analyst's dataset
// (phases 10.1 and 10.2): Claude Code hook events appended as they
// arrive, a keyboard annotator for the ambiguous windows, and align, which
// matches hook and weak (activity-derived) labels to recorded windows by
// wall clock. A manual annotation wins over a hook label, and a hook label
// over a weak one; a window near a transition gets neither.
//
// Invariants: Everything stays on the local machine, under labels/ of the
// data directory, in files 0600 inside a directory 0700. A hook event is
// recorded as received; an annotation holds the window id, the label, the
// annotator and the time, never the screen it was given.
//
// What it does not do: It does not read session transcripts (they wait on
// their confirmed formats) or read recordings itself: windows come through
// dataset.RecordingSource.
package label
