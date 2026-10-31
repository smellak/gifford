// Package label collects ground-truth labels for the Analyst's dataset
// (phases 10.1 and 10.2): projected harness hook events, audited imported
// weak labels, a keyboard annotator for ambiguous windows, and align. Hook
// events match only a root session proved for the recording's exact capture,
// harness and measured version; activity-derived weak labels stay within the
// recording session. Claude Code, Codex and Muse Code have a closed SessionStart
// candidate; OpenCode has a closed parentless session.created candidate. Every
// candidate is off by default and must be enabled for one exact version only
// after a pilot proves its native root semantics. A manual annotation wins over
// a hook label, and a hook label over a weak one; a window near a linked
// transition gets neither. The windows align cannot settle (no label, a weak one
// only, or hook and weak in conflict) form the doubtful queue that annotate
// --doubtful asks. Aligned weak evidence records whether it came from an audited
// import or activity. Only a conflict-free import can be opted into train;
// activity remains diagnostic, and validation and test never accept weak labels.
//
// Invariants: Everything stays on the local machine, under labels/ of the
// data directory, in regular files 0600 inside a real directory 0700;
// a symlink or a special file is refused, never followed or waited on. Of
// a hook event only closed projected metadata is kept, never the prompt,
// tool input or output, message, question, answer or path; an
// annotation holds the window id, the label, the annotator and the time,
// never the screen it was given; an aligned label holds the window id,
// the label, its source and the closed weak origin when applicable. Weak imports
// use exact canonical JSON keys and a closed, versioned audit vocabulary;
// contradictory labels stay conflicts regardless of line order and can never
// write manual annotations.
//
// What it does not do: It does not read session transcripts (they wait on
// their confirmed formats) or read recordings itself: windows come through
// dataset.RecordingSource.
package label
