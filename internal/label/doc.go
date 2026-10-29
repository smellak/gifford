// Package label collects ground-truth labels for the Analyst's dataset
// (phases 10.1 and 10.2): Claude Code hook events appended as they
// arrive, a keyboard annotator for the ambiguous windows, and align, which
// matches hook and weak (activity-derived) labels to recorded windows by
// wall clock. A manual annotation wins over a hook label, and a hook label
// over a weak one; a window near a transition gets neither. The windows
// align cannot settle (no label, a weak one only, or hook and weak in
// conflict) form the doubtful queue that annotate --doubtful asks.
//
// Invariants: Everything stays on the local machine, under labels/ of the
// data directory, in regular files 0600 inside a real directory 0700;
// a symlink or a special file is refused, never followed or waited on. Of
// a hook event only its name, session and notification kind are kept,
// never the prompt, the tool input or output, or the message; an
// annotation holds the window id, the label, the annotator and the time,
// never the screen it was given; an aligned label holds the window id,
// the label and its source.
//
// What it does not do: It does not read session transcripts (they wait on
// their confirmed formats) or read recordings itself: windows come through
// dataset.RecordingSource.
package label
