package session

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

const (
	openCodeRecordedVersion = "opencode 1.18.29"
	openCodeEmptyChannel    = "  ┃"
)

func frame(snap vt.Snapshot, generation uint64) PermissionDialogFrame {
	return PermissionDialogFrame{Snapshot: snap, GuardClosed: true, InputAligned: true, KeyGeneration: generation}
}

func blankSnapshot(cols, rows int) vt.Snapshot {
	return vt.Snapshot{Cols: cols, Rows: rows, Lines: make([]string, rows)}
}

func antigravityPermission(cols, rows int) vt.Snapshot {
	snap := blankSnapshot(cols, rows)
	start := 10
	if cols == 200 {
		start = 17
	}
	copy(snap.Lines[start:], []string{
		"File access",
		strings.Repeat("─", cols),
		"",
		"Read a variable resource",
		"Reason: variable explanation",
		"",
		"Allow access to this file?",
		"1. Yes, allow access",
		"2. Yes, and always allow non-workspace access",
		"3. No, deny access",
		"",
		"↑/↓ Navigate",
		"esc to cancel                                      model varies",
	})
	return snap
}

func claudePermissionAt(start int) vt.Snapshot {
	snap := blankSnapshot(200, 50)
	copy(snap.Lines[start:], []string{
		strings.Repeat("─", 200),
		" Tool title varies",
		` Tip: auto mode handles these prompts for you — choose "switch to auto mode" below`,
		"",
		"   command varies",
		"   reason varies",
		"",
		" Do you want to proceed?",
		" 1. Yes",
		"   2. Yes, and switch to auto mode · auto mode handles these prompts for you",
		"   3. No",
		"",
		" Esc to cancel · Tab to amend",
	})
	return snap
}

func openCodePermission(expanded bool) vt.Snapshot {
	snap := blankSnapshot(120, 50)
	title := 40
	if expanded {
		title = 38
	}
	snap.Lines[title-1] = openCodeEmptyChannel
	for row := title; row <= 46; row++ {
		snap.Lines[row] = openCodeEmptyChannel
	}
	snap.Lines[title] = "  ┃  △ Permission required"
	snap.Lines[title+1] = "  ┃  variable action"
	snap.Lines[46] = "  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm"
	snap.Lines[47] = openCodeEmptyChannel
	return snap
}

func TestPermissionDialogSignaturesAreExact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		harness protocol.Harness
		version string
		snap    vt.Snapshot
		want    string
	}{
		{"antigravity 80", protocol.HarnessAntigravity, "Antigravity 1.4.8", antigravityPermission(80, 24), "antigravity-full-width"},
		{"antigravity 200", protocol.HarnessAntigravity, "1.4.8", antigravityPermission(200, 50), "antigravity-full-width"},
		{"claude", protocol.HarnessClaudeCode, "Claude Code 2.1.280", claudePermissionAt(16), "claude-200x50"},
		{"opencode simple", protocol.HarnessOpencode, openCodeRecordedVersion, openCodePermission(false), "opencode-simple"},
		{"opencode expanded", protocol.HarnessOpencode, "1.18.29", openCodePermission(true), "opencode-expanded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tracker, ok := NewPermissionDialogTracker(tc.harness, tc.version)
			require.True(t, ok)
			got := tracker.spec.match(tc.snap)
			require.Equal(t, 1, got.count)
			require.Equal(t, tc.want, got.signature)
		})
	}
}

func TestPermissionDialogRejectsLookalikesAndAmbiguity(t *testing.T) {
	t.Parallel()
	question := antigravityPermission(80, 24)
	question.Lines[16] = "Which file should I use?"
	require.Zero(t, matchAntigravityPermission(question).count, "a question menu is not permission")

	folderTrust := claudePermissionAt(16)
	folderTrust.Lines[23] = " Do you trust the files in this folder?"
	require.Zero(t, matchClaudePermission(folderTrust).count, "folder trust has another question")

	ambiguous := claudePermissionAt(16)
	other := claudePermissionAt(0)
	copy(ambiguous.Lines[:13], other.Lines[:13])
	require.Equal(t, 2, matchClaudePermission(ambiguous).count)
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessClaudeCode, "2.1.280")
	require.True(t, ok)
	tracker.Observe(frame(blankSnapshot(200, 50), 0))
	tracker.Observe(frame(ambiguous, 0))
	_, reason := tracker.Screen(true, true, 0)
	require.Equal(t, PermissionDialogAmbiguous, reason)
}

func TestPermissionDialogRejectsSignaturesOutsideMeasuredRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		harness protocol.Harness
		version string
		modal   vt.Snapshot
	}{
		{"antigravity", protocol.HarnessAntigravity, "1.4.8", func() vt.Snapshot {
			snap := antigravityPermission(80, 24)
			copy(snap.Lines, snap.Lines[10:23])
			for row := 13; row < len(snap.Lines); row++ {
				snap.Lines[row] = ""
			}
			return snap
		}()},
		{"claude", protocol.HarnessClaudeCode, "2.1.280", claudePermissionAt(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tracker, ok := NewPermissionDialogTracker(tc.harness, tc.version)
			require.True(t, ok)
			match := tracker.spec.match(tc.modal)
			require.Equal(t, 1, match.count, "the candidate is unique, not ambiguous")
			require.Empty(t, match.signature, "the candidate position was not measured")
			tracker.Observe(frame(blankSnapshot(tc.modal.Cols, tc.modal.Rows), 0))
			tracker.Observe(frame(tc.modal, 0))

			screen, reason := tracker.Screen(true, true, 0)
			require.Empty(t, screen.Rows)
			require.Equal(t, PermissionDialogSignatureUnmeasured, reason)
			require.True(t, tracker.Active(), "an unmeasured candidate must block physical-screen fallback")
		})
	}
}

func TestPermissionDialogNeedsPriorClosedAlignedCompleteFrame(t *testing.T) {
	modal := antigravityPermission(80, 24)
	newTracker := func() *PermissionDialogTracker {
		tracker, ok := NewPermissionDialogTracker(protocol.HarnessAntigravity, "1.4.8")
		require.True(t, ok)
		return tracker
	}

	t.Run("happy path is canonical and content free", func(t *testing.T) {
		tracker := newTracker()
		prior := blankSnapshot(80, 24)
		prior.Lines[0] = "CANARY typed before modal"
		tracker.Observe(frame(prior, 0))
		modal.Lines[0] = "CANARY still in scrollback"
		tracker.Observe(frame(modal, 0))
		screen, reason := tracker.Screen(true, true, 0)
		require.Empty(t, reason)
		require.Equal(t, CanonicalPermissionDialog(1), screen)
		require.NotContains(t, strings.Join(screen.Rows, "\n"), "CANARY")
		require.Equal(t, 19, screen.Cols)
		require.Zero(t, screen.CursorRow)
		require.Zero(t, screen.CursorCol)
	})

	tests := []struct {
		name   string
		before func(*PermissionDialogTracker)
		show   PermissionDialogFrame
		want   string
	}{
		{"no prior", func(*PermissionDialogTracker) {}, frame(modal, 0), PermissionDialogNoPriorFrame},
		{"open guard", func(tr *PermissionDialogTracker) { tr.Observe(frame(blankSnapshot(80, 24), 0)) }, PermissionDialogFrame{Snapshot: modal, InputAligned: true}, PermissionDialogGuardNotClosed},
		{"crossed input", func(tr *PermissionDialogTracker) { tr.Observe(frame(blankSnapshot(80, 24), 0)) }, PermissionDialogFrame{Snapshot: modal, GuardClosed: true}, PermissionDialogInputUnaligned},
		{"partial frame", func(tr *PermissionDialogTracker) { tr.Observe(frame(blankSnapshot(80, 24), 0)) }, PermissionDialogFrame{Snapshot: func() vt.Snapshot { s := modal; s.Pending = true; return s }(), GuardClosed: true, InputAligned: true}, PermissionDialogFrameIncomplete},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newTracker()
			tc.before(tracker)
			tracker.Observe(tc.show)
			_, reason := tracker.Screen(tc.show.GuardClosed, tc.show.InputAligned, tc.show.KeyGeneration)
			require.Equal(t, tc.want, reason)
		})
	}
}

func TestPermissionDialogEpisodeIsInvalidatedPermanently(t *testing.T) {
	t.Parallel()
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessOpencode, "1.18.29")
	require.True(t, ok)
	plain := blankSnapshot(120, 50)
	tracker.Observe(frame(plain, 0))
	tracker.Observe(frame(openCodePermission(false), 0))
	_, reason := tracker.Screen(true, true, 0)
	require.Empty(t, reason)

	tracker.HumanInput(1)
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogHumanInput, reason)
	require.True(t, tracker.Active(), "an invalid active modal must block physical-screen fallback")
	tracker.Observe(frame(openCodePermission(false), 1))
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogHumanInput, reason, "a stable later frame cannot repair the episode")

	tracker.Observe(frame(plain, 1))
	require.False(t, tracker.Active())
	tracker.Observe(frame(openCodePermission(false), 1))
	screen, reason := tracker.Screen(true, true, 1)
	require.Empty(t, reason)
	require.Equal(t, uint64(2), screen.Episode)

	tracker.Resize()
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogResized, reason)
	tracker.Observe(frame(openCodePermission(false), 1))
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogResized, reason)
}

func TestPermissionDialogRejectsKeyBetweenPriorAndAppearance(t *testing.T) {
	t.Parallel()
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessClaudeCode, "2.1.280")
	require.True(t, ok)
	tracker.Observe(frame(blankSnapshot(200, 50), 0))
	tracker.HumanInput(1)
	tracker.Observe(frame(claudePermissionAt(16), 1))
	_, reason := tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogHumanInput, reason)
}

func TestPermissionDialogSourceIsAnExactToggle(t *testing.T) {
	t.Parallel()
	sources := []PermissionDialogSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: "Claude Code 2.1.280"}}
	require.True(t, PermissionDialogSources(sources, protocol.HarnessClaudeCode, "Claude Code 2.1.280"))
	require.False(t, PermissionDialogSources(sources, protocol.HarnessClaudeCode, "2.1.280"))
	require.False(t, PermissionDialogSources(sources, protocol.HarnessOpencode, "Claude Code 2.1.280"))
	require.True(t, SupportsPermissionDialog(protocol.HarnessClaudeCode, "Claude Code 2.1.280"))
	require.False(t, SupportsPermissionDialog(protocol.HarnessClaudeCode, "Claude Code 2.1.281"))
}
