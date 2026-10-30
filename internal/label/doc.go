// Package label collects ground-truth labels for the Analyst's dataset
// (phases 10.1 and 10.2): projected harness hook events, audited imported
// weak labels, a keyboard annotator for ambiguous windows, and align. Hook
// events match only a root session proved for the recording's exact capture,
// harness and measured version; activity-derived weak labels stay within the
// recording session. At present only Claude Code has a configurable root
// candidate; wait events from other harnesses remain unable to label recordings
// until a pilot proves their root bridge. A manual annotation wins over a hook
// label, and a hook label over a weak one; a window near a linked transition gets
// neither. The windows align cannot settle (no label, a weak one only, or hook
// and weak in conflict) form the doubtful queue that annotate --doubtful asks.
//
// Invariants: Everything stays on the local machine, under labels/ of the
// data directory, in regular files 0600 inside a real directory 0700;
// a symlink or a special file is refused, never followed or waited on. Of
// a hook event only closed projected metadata is kept, never the prompt,
// tool input or output, message, question, answer or path; an
// annotation holds the window id, the label, the annotator and the time,
// never the screen it was given; an aligned label holds the window id,
// the label and its source. Weak imports use a closed, versioned audit
// vocabulary and can never write manual annotations.
//
// What it does not do: It does not read session transcripts (they wait on
// their confirmed formats) or read recordings itself: windows come through
// dataset.RecordingSource.
package label
