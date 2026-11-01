package session

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

const (
	openCodeRecordedVersion   = "opencode 1.18.29"
	openCodeEmptyChannel      = "  ┃"
	claudePermissionSignature = "claude-200x50"
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

func claudePermissionRows() []string {
	return []string{
		strings.Repeat("─", 200),
		" Tool title varies",
		` Tip: auto mode handles these prompts for you — choose "switch to auto mode" below`,
		"",
		"   command varies",
		"   reason varies",
		"",
		" Do you want to proceed?",
		" \u276f 1. Yes",
		"   2. Yes, and switch to auto mode · auto mode handles these prompts for you",
		"   3. No",
		"",
		" Esc to cancel · Tab to amend",
	}
}

func claudePermissionSnapshot(t *testing.T, starts []int, dialog []string) vt.Snapshot {
	t.Helper()
	emulator, err := vt.New(200, 50)
	require.NoError(t, err)
	defer func() { require.NoError(t, emulator.Close()) }()

	var output strings.Builder
	output.WriteString("\x1b[H\x1b[2J")
	for row, line := range []string{"Earlier terminal output", "More terminal output"} {
		_, _ = fmt.Fprintf(&output, "\x1b[%d;1H%s", row+1, line)
	}
	for _, start := range starts {
		require.GreaterOrEqual(t, start, 0)
		require.LessOrEqual(t, start+len(dialog), 50)
		for offset, line := range dialog {
			if line == "" {
				continue
			}
			_, _ = fmt.Fprintf(&output, "\x1b[%d;1H%s", start+offset+1, line)
		}
	}
	output.WriteString("\x1b[1;1H\x1b[0m")
	raw := []byte(output.String())
	written, err := emulator.Write(raw)
	require.NoError(t, err)
	require.Equal(t, len(raw), written)
	require.NoError(t, emulator.Flush())
	snapshot := emulator.Snapshot()
	require.False(t, snapshot.Pending)
	return snapshot
}

type openCodePermissionFixture struct {
	title int
	rows  []string
}

func openCodePermissionRows(optionsOffset int) []string {
	rows := []string{
		openCodeEmptyChannel,
		"  ┃  △ Permission required",
		"  ┃  variable action",
	}
	for range optionsOffset - 2 {
		rows = append(rows, openCodeEmptyChannel)
	}
	return append(rows,
		"  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm",
		openCodeEmptyChannel,
	)
}

func openCodePermissionSnapshot(t *testing.T, fixtures []openCodePermissionFixture) vt.Snapshot {
	t.Helper()
	emulator, err := vt.New(120, 50)
	require.NoError(t, err)
	defer func() { require.NoError(t, emulator.Close()) }()

	var output strings.Builder
	output.WriteString("\x1b[H\x1b[2J")
	for row, line := range []string{"Earlier terminal output", "More terminal output"} {
		_, _ = fmt.Fprintf(&output, "\x1b[%d;1H%s", row+1, line)
	}
	for _, fixture := range fixtures {
		start := fixture.title - 1
		require.GreaterOrEqual(t, start, 0)
		require.LessOrEqual(t, start+len(fixture.rows), 50)
		for offset, line := range fixture.rows {
			_, _ = fmt.Fprintf(&output, "\x1b[%d;1H%s", start+offset+1, line)
		}
	}
	output.WriteString("\x1b[1;1H\x1b[0m")
	raw := []byte(output.String())
	written, err := emulator.Write(raw)
	require.NoError(t, err)
	require.Equal(t, len(raw), written)
	require.NoError(t, emulator.Flush())
	snapshot := emulator.Snapshot()
	require.False(t, snapshot.Pending)
	return snapshot
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
		{"claude", protocol.HarnessClaudeCode, "Claude Code 2.1.280", claudePermissionSnapshot(t, []int{17}, claudePermissionRows()), claudePermissionSignature},
		{"opencode simple", protocol.HarnessOpencode, openCodeRecordedVersion, openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 41, rows: openCodePermissionRows(6)}}), "opencode-k6"},
		{"opencode expanded", protocol.HarnessOpencode, "1.18.29", openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 10, rows: openCodePermissionRows(8)}}), "opencode-k8"},
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

func TestOpenCodePermissionDialogVTFrames(t *testing.T) {
	t.Parallel()

	t.Run("measured title row 41", func(t *testing.T) {
		t.Parallel()
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 41, rows: openCodePermissionRows(6)}})
		require.Equal(t, "  ┃  △ Permission required", snapshot.Lines[41])
		require.Equal(t, permissionDialogMatch{count: 1, signature: "opencode-k6"}, matchOpenCodePermission(snapshot))
	})

	t.Run("expanded block at another position", func(t *testing.T) {
		t.Parallel()
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 10, rows: openCodePermissionRows(8)}})
		require.Equal(t, "  ┃  △ Permission required", snapshot.Lines[10])
		require.Equal(t, permissionDialogMatch{count: 1, signature: "opencode-k8"}, matchOpenCodePermission(snapshot))
	})

	t.Run("tall recorded block at title row 35", func(t *testing.T) {
		t.Parallel()
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 35, rows: openCodePermissionRows(12)}})
		require.Equal(t, "  ┃  △ Permission required", snapshot.Lines[35])
		require.Equal(t, "  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm", snapshot.Lines[47])
		require.Equal(t, permissionDialogMatch{count: 1, signature: "opencode-k12"}, matchOpenCodePermission(snapshot))
	})

	t.Run("minimum body height", func(t *testing.T) {
		t.Parallel()
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: openCodePermissionRows(2)}})
		require.Equal(t, permissionDialogMatch{count: 1, signature: "opencode-k2"}, matchOpenCodePermission(snapshot))
	})

	t.Run("body row without channel is rejected", func(t *testing.T) {
		t.Parallel()
		rows := openCodePermissionRows(12)
		rows[5] = "body row without channel"
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: rows}})
		require.Zero(t, matchOpenCodePermission(snapshot).count)
	})

	t.Run("incomplete block is rejected", func(t *testing.T) {
		t.Parallel()
		rows := openCodePermissionRows(12)
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: rows[:len(rows)-1]}})
		require.Zero(t, matchOpenCodePermission(snapshot).count)
	})

	t.Run("altered fixed line is rejected", func(t *testing.T) {
		t.Parallel()
		rows := openCodePermissionRows(12)
		rows[len(rows)-2] = "  ┃  Allow once   Allow always   Deny       ctrl+f fullscreen  ⇆ select  enter confirm"
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: rows}})
		require.Zero(t, matchOpenCodePermission(snapshot).count)
	})

	t.Run("one title with two option lines is ambiguous", func(t *testing.T) {
		t.Parallel()
		rows := openCodePermissionRows(12)
		rows[7] = rows[len(rows)-2]
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: rows}})
		require.Equal(t, permissionDialogMatch{count: 2}, matchOpenCodePermission(snapshot))
	})

	t.Run("two dialogs are ambiguous", func(t *testing.T) {
		t.Parallel()
		snapshot := openCodePermissionSnapshot(t, []openCodePermissionFixture{
			{title: 6, rows: openCodePermissionRows(6)},
			{title: 30, rows: openCodePermissionRows(12)},
		})
		require.Equal(t, permissionDialogMatch{count: 2}, matchOpenCodePermission(snapshot))
	})
}

func TestOpenCodePermissionDialogHeightChangeInvalidatesEpisode(t *testing.T) {
	t.Parallel()
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessOpencode, "1.18.29")
	require.True(t, ok)
	tracker.Observe(frame(blankSnapshot(120, 50), 0))
	tracker.Observe(frame(openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: openCodePermissionRows(6)}}), 0))
	_, reason := tracker.Screen(true, true, 0)
	require.Empty(t, reason)

	tracker.Observe(frame(openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 20, rows: openCodePermissionRows(12)}}), 0))
	_, reason = tracker.Screen(true, true, 0)
	require.Equal(t, PermissionDialogSignatureChanged, reason)
	require.True(t, tracker.Active())
}

func TestClaudePermissionDialogVTFrames(t *testing.T) {
	t.Parallel()

	t.Run("row 16", func(t *testing.T) {
		t.Parallel()
		snapshot := claudePermissionSnapshot(t, []int{16}, claudePermissionRows())
		require.Equal(t, strings.Repeat("─", 200), snapshot.Lines[16])
		require.Equal(t, permissionDialogMatch{count: 1, signature: claudePermissionSignature}, matchClaudePermission(snapshot))
	})

	t.Run("measured row after prior output", func(t *testing.T) {
		t.Parallel()
		snapshot := claudePermissionSnapshot(t, []int{17}, claudePermissionRows())
		require.Equal(t, "Earlier terminal output", snapshot.Lines[0])
		require.Equal(t, strings.Repeat("─", 200), snapshot.Lines[17])
		require.Equal(t, " Do you want to proceed?", snapshot.Lines[24])
		require.Equal(t, permissionDialogMatch{count: 1, signature: claudePermissionSignature}, matchClaudePermission(snapshot))
	})

	t.Run("alternate row distinguishes variants", func(t *testing.T) {
		t.Parallel()
		snapshot := claudePermissionSnapshot(t, []int{4}, claudePermissionRows())
		variantB := matchClaudePermissionAt(snapshot, claudePermissionAnyStart)
		variantA := matchClaudePermissionAt(snapshot, 17)
		require.Equal(t, permissionDialogMatch{count: 1, signature: claudePermissionSignature}, variantB)
		require.Equal(t, permissionDialogMatch{count: 1}, variantA)
	})

	t.Run("two blocks are ambiguous", func(t *testing.T) {
		t.Parallel()
		snapshot := claudePermissionSnapshot(t, []int{4, 30}, claudePermissionRows())
		require.Equal(t, permissionDialogMatch{count: 2}, matchClaudePermission(snapshot))
	})

	t.Run("four options are rejected", func(t *testing.T) {
		t.Parallel()
		rows := claudePermissionRows()
		rows = append(rows, "")
		copy(rows[12:], rows[11:])
		rows[11] = "   4. Yes, and always allow access to this folder"
		snapshot := claudePermissionSnapshot(t, []int{17}, rows)
		require.Zero(t, matchClaudePermission(snapshot).count)
	})

	t.Run("altered option is rejected", func(t *testing.T) {
		t.Parallel()
		rows := claudePermissionRows()
		rows[9] = "   2. Yes, and switch to manual mode"
		snapshot := claudePermissionSnapshot(t, []int{17}, rows)
		require.Zero(t, matchClaudePermission(snapshot).count)
	})

	t.Run("option marker variants are rejected", func(t *testing.T) {
		t.Parallel()
		for _, option := range []string{
			" 1. Yes",
			"   1. Yes",
			" \u276f  1. Yes",
		} {
			rows := claudePermissionRows()
			rows[8] = option
			snapshot := claudePermissionSnapshot(t, []int{17}, rows)
			require.Zero(t, matchClaudePermission(snapshot).count, "option %q", option)
		}
	})

	t.Run("truncated block is rejected", func(t *testing.T) {
		t.Parallel()
		rows := claudePermissionRows()
		snapshot := claudePermissionSnapshot(t, []int{17}, rows[:len(rows)-1])
		require.Zero(t, matchClaudePermission(snapshot).count)
	})
}

func TestPermissionDialogRejectsLookalikesAndAmbiguity(t *testing.T) {
	t.Parallel()
	question := antigravityPermission(80, 24)
	question.Lines[16] = "Which file should I use?"
	require.Zero(t, matchAntigravityPermission(question).count, "a question menu is not permission")

	folderTrustRows := claudePermissionRows()
	folderTrustRows[7] = " Do you trust the files in this folder?"
	folderTrust := claudePermissionSnapshot(t, []int{17}, folderTrustRows)
	require.Zero(t, matchClaudePermission(folderTrust).count, "folder trust has another question")

	ambiguous := claudePermissionSnapshot(t, []int{4, 30}, claudePermissionRows())
	require.Equal(t, 2, matchClaudePermission(ambiguous).count)
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessClaudeCode, "2.1.280")
	require.True(t, ok)
	tracker.Observe(frame(blankSnapshot(200, 50), 0))
	tracker.Observe(frame(ambiguous, 0))
	_, reason := tracker.Screen(true, true, 0)
	require.Equal(t, PermissionDialogAmbiguous, reason)
}

func TestPermissionDialogRejectsAntigravityOutsideMeasuredRows(t *testing.T) {
	t.Parallel()
	modal := antigravityPermission(80, 24)
	copy(modal.Lines, modal.Lines[10:23])
	for row := 13; row < len(modal.Lines); row++ {
		modal.Lines[row] = ""
	}
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessAntigravity, "1.4.8")
	require.True(t, ok)
	match := tracker.spec.match(modal)
	require.Equal(t, 1, match.count, "the candidate is unique, not ambiguous")
	require.Empty(t, match.signature, "the candidate position was not measured")
	tracker.Observe(frame(blankSnapshot(modal.Cols, modal.Rows), 0))
	tracker.Observe(frame(modal, 0))

	screen, reason := tracker.Screen(true, true, 0)
	require.Empty(t, screen.Rows)
	require.Equal(t, PermissionDialogSignatureUnmeasured, reason)
	require.True(t, tracker.Active(), "an unmeasured candidate must block physical-screen fallback")
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
	modal := openCodePermissionSnapshot(t, []openCodePermissionFixture{{title: 41, rows: openCodePermissionRows(6)}})
	tracker.Observe(frame(modal, 0))
	_, reason := tracker.Screen(true, true, 0)
	require.Empty(t, reason)

	tracker.HumanInput(1)
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogHumanInput, reason)
	require.True(t, tracker.Active(), "an invalid active modal must block physical-screen fallback")
	tracker.Observe(frame(modal, 1))
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogHumanInput, reason, "a stable later frame cannot repair the episode")

	tracker.Observe(frame(plain, 1))
	require.False(t, tracker.Active())
	tracker.Observe(frame(modal, 1))
	screen, reason := tracker.Screen(true, true, 1)
	require.Empty(t, reason)
	require.Equal(t, uint64(2), screen.Episode)

	tracker.Resize()
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogResized, reason)
	tracker.Observe(frame(modal, 1))
	_, reason = tracker.Screen(true, true, 1)
	require.Equal(t, PermissionDialogResized, reason)
}

func TestPermissionDialogRejectsKeyBetweenPriorAndAppearance(t *testing.T) {
	t.Parallel()
	tracker, ok := NewPermissionDialogTracker(protocol.HarnessClaudeCode, "2.1.280")
	require.True(t, ok)
	tracker.Observe(frame(blankSnapshot(200, 50), 0))
	tracker.HumanInput(1)
	tracker.Observe(frame(claudePermissionSnapshot(t, []int{17}, claudePermissionRows()), 1))
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
