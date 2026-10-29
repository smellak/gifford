// Package label collects ground-truth labels for the Analyst's dataset
// (phase 10.1): Claude Code hook events appended as they arrive, and a
// keyboard annotator for the ambiguous windows.
//
// Invariants: Everything stays on the local machine, under labels/ of the
// data directory, in files 0600 inside a directory 0700. A hook event is
// recorded as received; an annotation holds the window id, the label, the
// annotator and the time, never the screen it was given.
//
// What it does not do: It does not read session transcripts (they wait on
// their confirmed formats) or align labels with recordings (10.2).
package label
