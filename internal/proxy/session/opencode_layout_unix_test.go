//go:build unix

package session

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/proxy/draft"
)

func TestOpenCodeMeasuredBoxRejectsCursorOutsideHorizontally(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	for _, cursorColumn := range []int{1, 160, 175} {
		t.Run(fmt.Sprintf("column %d", cursorColumn), func(t *testing.T) {
			t.Parallel()
			snapshot := newOpenCodeLayoutSnapshot(t, 200, openCodeLayoutFrame{
				sidebar:      true,
				cursorColumn: cursorColumn,
			})

			_, cropped := draft.CropInputBox(profile, snapshot)
			require.False(t, cropped)
			text, ok := draft.CursorInput(profile, snapshot)
			require.False(t, ok)
			require.Empty(t, text)
			rows, reason := LiveScreen(profile, snapshot)
			require.Nil(t, rows)
			require.Equal(t, LiveScreenNoInputEvidence, reason)

			session := &Session{
				opts:     Options{Profile: profile},
				lastSnap: snapshot,
				hasSnap:  true,
				running:  true,
			}
			require.Equal(t, reasonNoInputEvidence, session.inputEvidenceLocked())
		})
	}
}
