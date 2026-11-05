package session

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

const permissionDialogMeasuredReadSize = 4095

type permissionDialogTestVT struct {
	t        *testing.T
	emulator *vt.Emulator
}

func newPermissionDialogTestVT(t *testing.T, cols, rows int) *permissionDialogTestVT {
	t.Helper()
	emulator, err := vt.New(cols, rows)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	return &permissionDialogTestVT{t: t, emulator: emulator}
}

func (terminal *permissionDialogTestVT) write(raw []byte) vt.Snapshot {
	terminal.t.Helper()
	written, err := terminal.emulator.Write(raw)
	require.NoError(terminal.t, err)
	require.Equal(terminal.t, len(raw), written)
	return terminal.emulator.Snapshot()
}

func permissionDialogClearBytes() []byte {
	return []byte("\x1b[H\x1b[2J\x1b[1;1H\x1b[0m")
}

func permissionDialogPaintBytes(start int, lines []string) []byte {
	var output strings.Builder
	output.WriteString("\x1b[H\x1b[2J")
	for offset, line := range lines {
		if line == "" {
			continue
		}
		_, _ = fmt.Fprintf(&output, "\x1b[%d;1H%s", start+offset+1, line)
	}
	output.WriteString("\x1b[1;1H\x1b[0m")
	return []byte(output.String())
}

type permissionDialogIncompleteMode struct {
	name     string
	syncOpen bool
}

var permissionDialogIncompleteModes = []permissionDialogIncompleteMode{
	{name: "pending_4095_byte_utf8_split"},
	{name: "sync_open", syncOpen: true},
}

func permissionDialogIncompleteChunks(
	t *testing.T,
	modal []byte,
	mode permissionDialogIncompleteMode,
) ([]byte, []byte) {
	t.Helper()
	if mode.syncOpen {
		redraw := append([]byte("\x1b[?2026h"), modal...)
		return redraw, []byte("\x1b[?2026l")
	}

	// Align a synthetic SGR-reset prefix so the 4095-byte PTY boundary leaves
	// exactly one byte of a three-byte fixture rune in the production VT.
	// The prefix has no visual effect, so completing the suffix reconstructs
	// the same modal frame.
	cut := -1
	for pos := 0; pos < len(modal); {
		_, size := utf8.DecodeRune(modal[pos:])
		if size == 0 {
			break
		}
		candidate := pos + 1
		if candidate < permissionDialogMeasuredReadSize &&
			size == 3 && (permissionDialogMeasuredReadSize-candidate)%len("\x1b[0m") == 0 {
			cut = candidate
			break
		}
		pos += size
	}
	require.NotEqual(t, -1, cut)
	padding := bytes.Repeat(
		[]byte("\x1b[0m"),
		(permissionDialogMeasuredReadSize-cut)/len("\x1b[0m"),
	)
	redraw := make([]byte, 0, len(padding)+len(modal))
	redraw = append(redraw, padding...)
	redraw = append(redraw, modal...)
	cut += len(padding)
	require.Equal(t, permissionDialogMeasuredReadSize, cut)
	return redraw[:cut], redraw[cut:]
}

func permissionDialogIncompleteRedraw(
	t *testing.T,
	terminal *permissionDialogTestVT,
	modal []byte,
	mode permissionDialogIncompleteMode,
) (vt.Snapshot, []byte) {
	t.Helper()
	first, finish := permissionDialogIncompleteChunks(t, modal, mode)

	snapshot := terminal.write(first)
	if mode.syncOpen {
		require.False(t, snapshot.Pending)
		require.True(t, snapshot.SyncOpen)
	} else {
		require.Len(t, first, permissionDialogMeasuredReadSize)
		require.True(t, snapshot.Pending)
		require.False(t, snapshot.SyncOpen)
	}
	return snapshot, finish
}

func activatePermissionDialog(
	t *testing.T,
	harness protocol.Harness,
	version string,
	cols, rows int,
	modal []byte,
) (*permissionDialogTestVT, *PermissionDialogTracker) {
	t.Helper()
	terminal := newPermissionDialogTestVT(t, cols, rows)
	tracker, ok := NewPermissionDialogTracker(harness, version)
	require.True(t, ok)

	prior := terminal.write(permissionDialogClearBytes())
	require.False(t, prior.Pending)
	require.False(t, prior.SyncOpen)
	tracker.Observe(frame(prior, 0))

	complete := terminal.write(modal)
	require.False(t, complete.Pending)
	require.False(t, complete.SyncOpen)
	tracker.Observe(frame(complete, 0))
	screen, reason := tracker.Screen(true, true, 0)
	require.Empty(t, reason)
	require.NotEmpty(t, screen.Rows)
	require.True(t, tracker.Active())
	require.True(t, tracker.valid)
	return terminal, tracker
}

// TestPermissionDialogActiveIncompleteRedrawResumesValidEpisode specifies that
// an incomplete read boundary is transient. It is withheld at that boundary,
// then the same complete modal and key generation resume the existing proof.
func TestPermissionDialogActiveIncompleteRedrawResumesValidEpisode(t *testing.T) {
	modal := permissionDialogPaintBytes(3, antigravityPermissionRows(80))
	for _, mode := range permissionDialogIncompleteModes {
		t.Run(mode.name, func(t *testing.T) {
			terminal, tracker := activatePermissionDialog(
				t, protocol.HarnessAntigravity, "1.4.8", 80, 24, modal,
			)
			episode := tracker.episode
			signature := tracker.signature
			appearanceKey := tracker.appearanceKey
			incomplete, finish := permissionDialogIncompleteRedraw(t, terminal, modal, mode)
			tracker.Observe(frame(incomplete, 0))

			withheld, reason := tracker.Screen(true, true, 0)
			require.Empty(t, withheld.Rows)
			require.Equal(t, PermissionDialogFrameIncomplete, reason)
			require.True(t, tracker.Active())
			require.True(t, tracker.valid)
			require.True(t, tracker.frameIncomplete)
			require.Equal(t, episode, tracker.episode)
			require.Equal(t, signature, tracker.signature)
			require.Equal(t, appearanceKey, tracker.appearanceKey)
			require.Empty(t, tracker.reason)

			complete := terminal.write(finish)
			require.False(t, complete.Pending)
			require.False(t, complete.SyncOpen)
			require.Equal(
				t,
				permissionDialogMatch{count: 1, signature: antigravityPermissionSignature},
				matchAntigravityPermission(complete),
			)
			tracker.Observe(frame(complete, 0))
			screen, reason := tracker.Screen(true, true, 0)
			t.Logf(
				"completed %s redraw: Pending=%t SyncOpen=%t active=%t valid=%t reason=%q",
				mode.name, complete.Pending, complete.SyncOpen, tracker.Active(), tracker.valid, reason,
			)
			require.Empty(t, reason)
			require.True(t, tracker.Active())
			require.True(t, tracker.valid)
			require.False(t, tracker.frameIncomplete)
			require.Equal(t, episode, tracker.episode)
			require.Equal(t, signature, tracker.signature)
			require.Equal(t, appearanceKey, tracker.appearanceKey)
			require.Equal(t, CanonicalPermissionDialog(1), screen)
		})
	}
}

// TestPermissionDialogIncompleteRedrawCausalInvalidators specifies that an
// incomplete redraw is never itself proof of safety: every independent causal
// invalidator still prevents an anchor after the redraw becomes complete.
func TestPermissionDialogIncompleteRedrawCausalInvalidators(t *testing.T) {
	standard := permissionDialogPaintBytes(17, claudePermissionRows())
	directory := permissionDialogPaintBytes(17, claudeDirectoryPermissionRows("/workspace/project"))
	causes := []struct {
		name   string
		reason string
	}{
		{name: "human_input", reason: PermissionDialogHumanInput},
		{name: "signature_change", reason: PermissionDialogSignatureChanged},
		{name: permissionResizeTestName, reason: PermissionDialogResized},
		{name: "guard_not_closed", reason: PermissionDialogGuardNotClosed},
		{name: "input_unaligned", reason: PermissionDialogInputUnaligned},
	}

	for _, mode := range permissionDialogIncompleteModes {
		for _, cause := range causes {
			t.Run(mode.name+"/"+cause.name, func(t *testing.T) {
				terminal, tracker := activatePermissionDialog(
					t, protocol.HarnessClaudeCode, "2.1.280", 200, 50, standard,
				)
				incomplete, finish := permissionDialogIncompleteRedraw(t, terminal, standard, mode)
				generation := uint64(0)
				switch cause.name {
				case "guard_not_closed":
					tracker.Observe(PermissionDialogFrame{
						Snapshot: incomplete, InputAligned: true, KeyGeneration: generation,
					})
				case "input_unaligned":
					tracker.Observe(PermissionDialogFrame{
						Snapshot: incomplete, GuardClosed: true, KeyGeneration: generation,
					})
				default:
					tracker.Observe(frame(incomplete, generation))
				}

				switch cause.name {
				case "human_input":
					generation++
					tracker.HumanInput(generation)
				case permissionResizeTestName:
					tracker.Resize()
				}

				complete := terminal.write(finish)
				require.False(t, complete.Pending)
				require.False(t, complete.SyncOpen)
				require.Equal(
					t,
					permissionDialogMatch{count: 1, signature: claudePermissionSignature},
					matchClaudePermission(complete),
				)
				tracker.Observe(frame(complete, generation))

				if cause.name == "signature_change" {
					changed := terminal.write(directory)
					require.False(t, changed.Pending)
					require.False(t, changed.SyncOpen)
					require.Equal(
						t,
						permissionDialogMatch{count: 1, signature: claudePermissionDirectorySignature},
						matchClaudePermission(changed),
					)
					tracker.Observe(frame(changed, generation))
				}

				screen, reason := tracker.Screen(true, true, generation)
				require.True(t, tracker.Active())
				require.False(t, tracker.valid)
				require.Empty(t, screen.Rows)
				require.Equal(t, cause.reason, reason)
				t.Logf("%s with %s: active=%t valid=%t reason=%s", cause.name, mode.name, tracker.Active(), tracker.valid, reason)
			})
		}
	}
}

// TestPermissionDialogIncompleteRedrawDoesNotReviveInvalidEpisode pins the
// one-way validity rule: transient retention can preserve a valid proof, but
// can never repair an episode invalidated before the partial redraw.
func TestPermissionDialogIncompleteRedrawDoesNotReviveInvalidEpisode(t *testing.T) {
	modal := permissionDialogPaintBytes(3, antigravityPermissionRows(80))
	for _, mode := range permissionDialogIncompleteModes {
		t.Run(mode.name, func(t *testing.T) {
			terminal, tracker := activatePermissionDialog(
				t, protocol.HarnessAntigravity, "1.4.8", 80, 24, modal,
			)
			tracker.HumanInput(1)
			require.False(t, tracker.valid)

			incomplete, finish := permissionDialogIncompleteRedraw(t, terminal, modal, mode)
			tracker.Observe(frame(incomplete, 1))
			withheld, reason := tracker.Screen(true, true, 1)
			require.Empty(t, withheld.Rows)
			require.Equal(t, PermissionDialogHumanInput, reason)

			complete := terminal.write(finish)
			require.False(t, complete.Pending)
			require.False(t, complete.SyncOpen)
			tracker.Observe(frame(complete, 1))
			screen, reason := tracker.Screen(true, true, 1)
			require.Empty(t, screen.Rows)
			require.Equal(t, PermissionDialogHumanInput, reason)
			require.True(t, tracker.Active())
			require.False(t, tracker.valid)
			require.Equal(t, uint64(1), tracker.episode)
		})
	}
}

// TestPermissionDialogUnclosedSyncUpdateStaysWithheld proves that silence or
// further bytes inside DEC 2026 never produce a canonical permission anchor.
func TestPermissionDialogUnclosedSyncUpdateStaysWithheld(t *testing.T) {
	modal := permissionDialogPaintBytes(3, antigravityPermissionRows(80))
	terminal, tracker := activatePermissionDialog(
		t, protocol.HarnessAntigravity, "1.4.8", 80, 24, modal,
	)
	open, _ := permissionDialogIncompleteRedraw(
		t, terminal, modal, permissionDialogIncompleteMode{name: "sync_open", syncOpen: true},
	)
	tracker.Observe(frame(open, 0))

	for boundary := 1; boundary <= 2; boundary++ {
		if boundary == 2 {
			open = terminal.write(modal)
			tracker.Observe(frame(open, 0))
		}
		require.True(t, open.SyncOpen)
		screen, reason := tracker.Screen(true, true, 0)
		require.True(t, tracker.Active())
		require.Empty(t, screen.Rows)
		require.Equal(t, PermissionDialogFrameIncomplete, reason)
	}
}
