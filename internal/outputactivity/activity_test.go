package outputactivity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAntigravityNoOpRead(t *testing.T) {
	var d Detector
	read := []byte("\x1b[?2004h\x1b[>4;2m")
	require.True(t, d.Observe(read), "the first input-mode assertions change state")
	require.False(t, d.Observe(read), "the repeated input modes are proved inert")
	require.False(t, d.Observe(read), "the proof is stable")
}

func TestRepeatedRenditionAndBracketedPasteAreInert(t *testing.T) {
	var d Detector
	read := []byte("\x1b[0;10m\x1b[?2004h")
	require.True(t, d.Observe(read), "the first bracketed-paste enable changes mode")
	require.False(t, d.Observe(read), "the repeated rendition and mode are proved inert")
}

func TestChangesRemainActivityWhenRestoredInOneRead(t *testing.T) {
	tests := map[string][]byte{
		"rendition paint and restore": []byte("\x1b[31m\x1b[39m"),
		"cursor move and restore":     []byte("\x1b[2C\x1b[2D"),
		"complete spinner":            []byte("\b|\b/\b-\b\\\b"),
	}
	for name, read := range tests {
		t.Run(name, func(t *testing.T) {
			var d Detector
			require.True(t, d.Observe(read))
		})
	}
}

func TestRenditionChangeThenRepeat(t *testing.T) {
	var d Detector
	require.True(t, d.Observe([]byte("\x1b[1;38;2;10;20;30m")))
	require.False(t, d.Observe([]byte("\x1b[1;38;2;10;20;30m")))
	require.True(t, d.Observe([]byte("\x1b[22;39m")))
	require.False(t, d.Observe([]byte("\x1b[22;39m")))
}

func TestIncompleteAndUnknownSequencesAreActivity(t *testing.T) {
	var d Detector
	require.True(t, d.Observe([]byte("\x1b[31")), "an incomplete CSI must not disappear")
	require.True(t, d.Observe([]byte("\x1b[0;10m")), "reset from unknown is conservative activity")
	require.False(t, d.Observe([]byte("\x1b[0;10m")), "reset re-establishes known rendition")
	require.True(t, d.Observe([]byte("\x1b[?25h")), "an uncovered cursor mode is activity")
	require.True(t, d.Observe([]byte("\x1b[?2004h")), "covered mode from unknown counts once")
	require.False(t, d.Observe([]byte("\x1b[?2004h")))
}

func TestFirstBracketedPasteEnableAndRealSGRChangeNeverDisappear(t *testing.T) {
	var d Detector
	require.True(t, d.Observe([]byte("\x1b[?2004h")))
	require.False(t, d.Observe([]byte("\x1b[?2004h")))
	require.True(t, d.Observe([]byte("\x1b[31m")))
	require.False(t, d.Observe([]byte("\x1b[31m")))
}
