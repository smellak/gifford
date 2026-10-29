package label

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

func TestLabelNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"transcript",
		"align",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestLabelBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

// The tests below set XDG_DATA_HOME, so they do not run in parallel.

func labelsDir(t *testing.T) string {
	t.Helper()
	data := t.TempDir()
	t.Setenv("XDG_DATA_HOME", data)
	return filepath.Join(data, "gifford", "labels")
}

const hookCmd = "hook"

func TestHookAlwaysExitsZero(t *testing.T) {
	for _, tc := range []struct {
		name, args, stdin string
		recorded, warned  bool
	}{
		{"valid", hookCmd, `{"hook_event_name":"Stop"}`, true, false},
		{"unknown flags", "hook --event PreToolUse --foo", `{"hook_event_name":"PreToolUse"}`, true, false},
		{"invalid", hookCmd, `{not json`, false, true},
		{"empty", hookCmd, ``, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := labelsDir(t)
			cmd := NewCmd(&deps.Deps{})
			var out, errOut bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			cmd.SetIn(strings.NewReader(tc.stdin))
			cmd.SetArgs(strings.Fields(tc.args))
			require.NoError(t, cmd.Execute())
			require.Empty(t, out.String())

			_, err := os.Stat(filepath.Join(dir, label.HooksFile))
			require.Equal(t, tc.recorded, err == nil)
			require.Equal(t, tc.warned, strings.HasPrefix(errOut.String(), "gifford label hook: "), errOut.String())
		})
	}
}

func TestHookUnwritableDataDirExitsZero(t *testing.T) {
	dir := labelsDir(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
	require.NoError(t, os.WriteFile(dir, nil, 0o600)) // labels is a file

	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(`{"hook_event_name":"Stop"}`))
	cmd.SetArgs([]string{hookCmd})
	require.NoError(t, cmd.Execute())
	require.Empty(t, out.String())
	require.NotEmpty(t, errOut.String())
}

type oneWindow struct{ done bool }

func (s *oneWindow) Next(context.Context) (dataset.Recording, error) {
	if s.done {
		return dataset.Recording{}, io.EOF
	}
	s.done = true
	return dataset.Recording{
		WindowID: protocol.ULID("01JB0000000000000000000001"),
		Screen:   redact.Screen{Rows: []string{"SCREEN-ONLY-CONTENT"}, Cols: 80},
	}, nil
}

func execAnnotate(t *testing.T, open sourceOpener, keys string, args ...string) (string, error) {
	t.Helper()
	cmd := newCmd(open)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetIn(strings.NewReader(keys))
	cmd.SetArgs(append([]string{"annotate"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

func TestAnnotateSavesLabels(t *testing.T) {
	dir := labelsDir(t)
	open := func(context.Context) (dataset.RecordingSource, error) { return &oneWindow{}, nil }
	out, err := execAnnotate(t, open, "4", "--annotator", "bench")
	require.NoError(t, err)
	require.Contains(t, out, "SCREEN-ONLY-CONTENT")
	require.Contains(t, out, "saved 1 labels")

	raw, err := os.ReadFile(filepath.Join(dir, label.AnnotationsFile))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"label":"D","annotator":"bench"`)
	require.NotContains(t, string(raw), "SCREEN-ONLY")
}

func TestAnnotateUsage(t *testing.T) {
	labelsDir(t)
	open := func(context.Context) (dataset.RecordingSource, error) { return &oneWindow{}, nil }

	_, err := execAnnotate(t, open, "")
	require.ErrorContains(t, err, "annotator")

	// The name is checked before the source is opened, so the shipped
	// command, with no reader yet, still says Usage.
	never := func(context.Context) (dataset.RecordingSource, error) {
		t.Error("source opened for a bad annotator")
		return nil, io.EOF
	}
	for _, o := range []sourceOpener{never, noRecordingReader} {
		_, err = execAnnotate(t, o, "", "--annotator", "a b")
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, exitcode.Usage, ec.Code)
	}
}

func TestAnnotateNoReaderYet(t *testing.T) {
	labelsDir(t)
	_, err := execAnnotate(t, noRecordingReader, "", "--annotator", "bench")
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.NotImplemented, ec.Code)
}
