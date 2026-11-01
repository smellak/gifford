package profiles

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/vt"
)

func TestClaudeCodeMeasuredInputScreen(t *testing.T) {
	t.Parallel()

	loaded, err := Load()
	require.NoError(t, err)
	profile, err := loaded.Profile("claude-code", testClaudeVersion)
	require.NoError(t, err)

	for _, inputRow := range []int{7, 10} {
		t.Run(fmt.Sprintf("input row %d", inputRow), func(t *testing.T) {
			t.Parallel()
			snapshot := claudeCodeSyntheticScreen(t, inputRow)
			oldRow := 2

			require.False(t, profile.InputLine.MatchString(snapshot.Lines[oldRow-1]),
				"a submitted prompt uses an ASCII space, not the measured input NBSP")
			match := profile.InputLine.FindStringSubmatch(snapshot.Lines[inputRow-1])
			require.Equal(t, []string{"❯\u00a0", ""}, match)
			require.True(t, profileBoundsAt(profile.InputBounds, snapshot, inputRow-1))
		})
	}
}

func claudeCodeSyntheticScreen(t *testing.T, inputRow int) vt.Snapshot {
	t.Helper()
	require.Greater(t, inputRow, 3)
	require.LessOrEqual(t, inputRow+2, 20)

	emulator, err := vt.New(82, 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, emulator.Close()) }()

	var output bytes.Buffer
	output.WriteString("\x1b[2J")
	putVTScreenRow(&output, 2, "❯ earlier prompt")
	putVTScreenRow(&output, 3, "  earlier response")
	border := "\x1b[38;5;244m" + strings.Repeat("─", 82) + "\x1b[0m"
	putVTScreenRow(&output, inputRow-1, border)
	putVTScreenRow(&output, inputRow, "❯\u00a0")
	putVTScreenRow(&output, inputRow+1, border)
	putVTScreenRow(&output, inputRow+2, "  auto mode on")
	fmt.Fprintf(&output, "\x1b[%d;3H", inputRow)
	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)
	return emulator.Snapshot()
}

func TestCodexMeasuredInputScreen(t *testing.T) {
	t.Parallel()

	loaded, err := Load()
	require.NoError(t, err)
	profile, err := loaded.Profile("codex", testCodexVersion)
	require.NoError(t, err)
	require.Equal(t, "Ask Codex to do anything", profile.InputPlaceholder)
	require.False(t, profile.InputPlaceholderStyle.Configured(), "Codex keeps the exact faint literal rule")

	for _, inputRow := range []int{8, 13} {
		t.Run(fmt.Sprintf("input row %d", inputRow), func(t *testing.T) {
			t.Parallel()
			snapshot := codexSyntheticScreen(t, inputRow)
			oldRow := 2

			require.True(t, profile.InputLine.MatchString(snapshot.Lines[oldRow-1]))
			require.False(t, profileBoundsAt(profile.InputBounds, snapshot, oldRow-1),
				"a submitted prompt with the same glyph is not the editable area")
			require.True(t, profile.InputLine.MatchString(snapshot.Lines[inputRow-1]))
			require.True(t, profileBoundsAt(profile.InputBounds, snapshot, inputRow-1))

			match := profile.InputLine.FindStringSubmatchIndex(snapshot.Lines[inputRow-1])
			require.Len(t, match, 4)
			cells, column, ok := snapshot.CursorCellsForBytes(match[2], match[3])
			require.True(t, ok)
			require.Equal(t, snapshot.CursorCol, column)
			require.NotEmpty(t, cells)
			for _, cell := range cells {
				require.True(t, cell.Faint)
				require.False(t, cell.OtherStyle)
			}
			require.Equal(t, profile.InputPlaceholder, snapshot.Lines[inputRow-1][match[2]:match[3]])
		})
	}
}

func codexSyntheticScreen(t *testing.T, inputRow int) vt.Snapshot {
	t.Helper()
	require.Greater(t, inputRow, 3)
	require.LessOrEqual(t, inputRow+2, 20)

	emulator, err := vt.New(80, 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, emulator.Close()) }()

	var output bytes.Buffer
	output.WriteString("\x1b[2J")
	putVTScreenRow(&output, 2, "\x1b[1;2m› \x1b[0mearlier prompt")
	putVTScreenRow(&output, 3, "  earlier response continues")
	putVTScreenRow(&output, inputRow-3, "latest response")
	putVTScreenRow(&output, inputRow, "\x1b[1m›\x1b[0m \x1b[2mAsk Codex to do anything\x1b[0m")
	putVTScreenRow(&output, inputRow+2, "\x1b[38;2;156;222;211m  model · /workspace · session\x1b[0m")
	fmt.Fprintf(&output, "\x1b[%d;3H", inputRow)
	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)
	return emulator.Snapshot()
}

func putVTScreenRow(output *bytes.Buffer, row int, text string) {
	fmt.Fprintf(output, "\x1b[%d;1H%s", row, text)
}

func profileBoundsAt(bounds InputBounds, snapshot vt.Snapshot, row int) bool {
	for offset, boundary := range bounds.Above {
		candidate := row - offset - 1
		if candidate < 0 || candidate >= len(snapshot.Lines) || !boundary.Matches(snapshot.Lines[candidate]) {
			return false
		}
	}
	for offset, boundary := range bounds.Below {
		candidate := row + offset + 1
		if candidate < 0 || candidate >= len(snapshot.Lines) || !boundary.Matches(snapshot.Lines[candidate]) {
			return false
		}
	}
	return true
}
