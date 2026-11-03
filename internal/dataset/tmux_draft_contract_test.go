package dataset

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/screenproj"
)

// This is a dataset boundary control, not evidence of a live tmux importer.
// The tmux observation contract must withhold the screen before this point;
// a caller offering an open draft with an unknown cursor is refused as well.
func TestTmuxDraftDatasetRejectsUnknownCursor(t *testing.T) {
	t.Parallel()
	const draft = "UNSUBMITTED HUMAN DRAFT CANARY"
	recording := recordingOf(validWindow(), "synthetic")
	recording.Screen.Rows = []string{"$ " + draft, "status row", ""}
	recording.Draft = DraftOpen
	recording.Cursor = screenproj.Cursor{}
	inv, err := RunPipeline(t.Context(), testDeps(t, []Recording{recording}), testOptions)
	require.ErrorIs(t, err, screenproj.ErrNoCursor)
	require.Empty(t, inv.Windows)
	require.NotContains(t, err.Error(), draft)
	raw, encodeErr := json.Marshal(inv)
	require.NoError(t, encodeErr)
	require.NotContains(t, string(raw), draft)
}

func TestTmuxDraftDatasetDoesNotTreatReplayCursorAsDraftPermission(t *testing.T) {
	t.Parallel()
	const draft = "UNSUBMITTED HUMAN DRAFT CANARY"
	recording := recordingOf(validWindow(), "synthetic")
	recording.Screen.Rows = []string{"$ " + draft, "status row", ""}
	recording.Draft = DraftOpen
	// Even a mistaken upstream cursor marking must not preserve another row
	// of an open draft: the shared projection excludes the entire canvas.
	recording.Cursor = screenproj.Cursor{Known: true, Visible: true, Row: 2}
	window, err := buildWindow(testDeps(t, nil), recording)
	require.NoError(t, err)
	require.Empty(t, window.Input.ScreenLines)
	raw, err := json.Marshal(window)
	require.NoError(t, err)
	require.NotContains(t, string(raw), draft)
}
