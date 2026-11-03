package session

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/vt"
)

func codexCaptureProfile(t *testing.T) profiles.Profile {
	t.Helper()
	raw, err := os.ReadFile("../../../profiles/codex.yaml")
	require.NoError(t, err)
	prof, err := profiles.ParseFile("codex", raw)
	require.NoError(t, err)
	return prof
}

func captureSnapshot(t *testing.T, raw []byte, cols, rows int) vt.Snapshot {
	t.Helper()
	emu, err := vt.New(cols, rows)
	require.NoError(t, err)
	_, err = emu.Write(detect.NormalizeCapture(raw))
	require.NoError(t, err)
	require.NoError(t, emu.Flush())
	return emu.Snapshot()
}

// The 12 sanitized bank captures evaluated as dumps: no cursor to read,
// so the box is located structurally. Idle and working pass with rows;
// typed text and dialogs withhold.
func TestLiveScreenCaptureBankSet(t *testing.T) {
	prof := codexCaptureProfile(t)
	cases := []struct {
		file   string
		reason string
	}{
		{"idle-01", ""},
		{"idle-02", ""},
		{"idle-03", ""},
		{"working-01", ""},
		{"working-02", ""},
		{"working-03", ""},
		{"typed-01", LiveScreenInputNotEmpty},
		{"typed-02", LiveScreenInputNotEmpty},
		{"typed-03", LiveScreenInputNotEmpty},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("../../../internal/detect/testdata/codex/" + tc.file + ".ansi")
		require.NoError(t, err)
		rows, reason := LiveScreenForCapture(prof, captureSnapshot(t, raw, 120, 40))
		require.Equal(t, tc.reason, reason, "%s", tc.file)
		if tc.reason == "" {
			require.NotEmpty(t, rows, "%s", tc.file)
		} else {
			require.Empty(t, rows, "%s", tc.file)
		}
	}
	for _, f := range []string{"dialog-01", "dialog-02", "dialog-03"} {
		raw, err := os.ReadFile("../../../internal/detect/testdata/codex/" + f + ".ansi")
		require.NoError(t, err)
		rows, reason := LiveScreenForCapture(prof, captureSnapshot(t, raw, 120, 40))
		require.NotEmpty(t, reason, "%s must withhold", f)
		require.Empty(t, rows, "%s", f)
	}
}

// The three measured fleet footers: with title (two separators),
// trimmed (one) and narrow (none). The bound must not count them.
func TestLiveScreenCaptureFooters(t *testing.T) {
	prof := codexCaptureProfile(t)
	footers := []string{
		"  gpt-6-astra max fast · ~ · xxxxxx xx xxxx xx xxxxxxx",
		"  gpt-6-astra max fast · …",
		"  gpt-5.6-terra high fast…",
		"  xxx-x.x-xxx xxxxxxx xxxx · ~/xxx-xx/xxxx",
	}
	for _, footer := range footers {
		screen := "synthetic output\r\n\r\n\r\n› \x1b[2mAsk Codex to do anything\x1b[0m\r\n\r\n" + footer + "\r\n"
		rows, reason := LiveScreenForCapture(prof, captureSnapshot(t, []byte(screen), 120, 40))
		require.Empty(t, reason, "%q", footer)
		require.NotEmpty(t, rows, "%q", footer)
	}
}
