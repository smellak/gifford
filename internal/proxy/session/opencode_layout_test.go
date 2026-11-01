package session

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

type openCodeLayoutFrame struct {
	input            []string
	cursor           int
	sidebar          bool
	shortBorder      bool
	duplicateBorder  bool
	misalignedBorder bool
	cursorColumn     int
}

func openCodeLayoutProfile(t *testing.T) profiles.Profile {
	t.Helper()
	bank, err := profiles.Load()
	require.NoError(t, err)
	profile, err := bank.Profile("opencode", "1.18.29")
	require.NoError(t, err)
	return profile
}

func openCodeTestLayout(columns int) (right, bottomMargin int) {
	switch columns {
	case 80:
		return 77, 3
	case 100:
		return 97, 2
	case 120:
		return 117, 2
	case 140:
		return 95, 2
	case 160:
		return 115, 2
	case 180:
		return 135, 2
	case 200:
		return 155, 2
	case 220:
		return 175, 2
	default:
		return columns - 3, 2
	}
}

func writeOpenCodeLayoutFrame(t *testing.T, emulator *vt.Emulator, columns int, frame openCodeLayoutFrame) vt.Snapshot {
	t.Helper()
	defaultPlaceholder := len(frame.input) == 0
	if len(frame.input) == 0 {
		frame.input = []string{""}
	}
	require.GreaterOrEqual(t, frame.cursor, 0)
	require.Less(t, frame.cursor, len(frame.input))

	right, bottomMargin := openCodeTestLayout(columns)
	borderRow := 50 - 1 - bottomMargin
	lastInputRow := borderRow - 3
	firstInputRow := lastInputRow - len(frame.input) + 1
	topFillerRow := firstInputRow - 1

	var output bytes.Buffer
	output.WriteString("\x1b[2J\x1b[H")
	put := func(row, column int, text string) {
		fmt.Fprintf(&output, "\x1b[%d;%dH%s", row+1, column+1, text)
	}
	put(topFillerRow-1, 0, "synthetic output")
	for row := topFillerRow; row < borderRow; row++ {
		put(row, 2, "┃")
	}
	for index, text := range frame.input {
		if defaultPlaceholder {
			text = "\x1b[38;5;8;48;5;234mAsk anything\x1b[0m"
		}
		put(firstInputRow+index, 2, "┃  "+text)
	}
	put(borderRow-1, 2, "┃  Meta")

	borderLeft := 2
	if frame.misalignedBorder {
		borderLeft++
	}
	borderRight := right
	if frame.shortBorder {
		borderRight--
	}
	put(borderRow, borderLeft, "╹"+strings.Repeat("▀", borderRight-borderLeft))
	if frame.sidebar {
		put(borderRow-2, right+1, "sidebar row")
		put(borderRow-1, right+1, "sidebar model")
		put(borderRow, right+1, "sidebar border")
	}
	if frame.duplicateBorder {
		put(borderRow, right+2, "╹▀")
	}
	cursorColumn := 5
	if frame.cursorColumn != 0 {
		cursorColumn = frame.cursorColumn
	}
	put(firstInputRow+frame.cursor, cursorColumn, "")
	output.WriteString("\x1b[?25h\x1b[0m")
	_, err := emulator.Write(output.Bytes())
	require.NoError(t, err)
	snapshot := emulator.Snapshot()
	require.True(t, snapshot.CursorVisible)
	require.False(t, snapshot.Pending)
	require.False(t, snapshot.SyncOpen)
	return snapshot
}

func newOpenCodeLayoutSnapshot(t *testing.T, columns int, frame openCodeLayoutFrame) vt.Snapshot {
	t.Helper()
	emulator, err := vt.New(columns, 50)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	return writeOpenCodeLayoutFrame(t, emulator, columns, frame)
}

func requireOpenCodeEmptyScreen(t *testing.T, profile profiles.Profile, snapshot vt.Snapshot) {
	t.Helper()
	text, ok := draft.CursorInput(profile, snapshot)
	require.True(t, ok)
	require.Empty(t, text)
	rows, reason := LiveScreen(profile, snapshot)
	require.Empty(t, reason)
	require.NotNil(t, rows)
}

func newOpenCodeCenteredPlaceholderSnapshot(t *testing.T, paintedInput string) vt.Snapshot {
	t.Helper()
	const (
		columns   = 200
		rows      = 50
		left      = 63
		inputRow  = 25
		borderRow = 28
	)
	emulator, err := vt.New(columns, rows)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })

	var output bytes.Buffer
	output.WriteString("\x1b[2J\x1b[H")
	put := func(row, column int, text string) {
		fmt.Fprintf(&output, "\x1b[%d;%dH%s", row+1, column+1, text)
	}
	put(inputRow-2, 0, "synthetic output")
	put(inputRow-1, left, "┃")
	put(inputRow, left, "┃  "+paintedInput)
	put(inputRow+1, left, "┃")
	put(inputRow+2, left, "┃  Model")
	put(borderRow, left, "╹"+strings.Repeat("▀", 74))
	put(inputRow, left+3, "")
	output.WriteString("\x1b[?25h\x1b[0m")
	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)

	snapshot := emulator.Snapshot()
	require.Equal(t, columns, snapshot.Cols)
	require.Equal(t, inputRow, snapshot.CursorRow)
	require.Equal(t, left+3, snapshot.CursorCol)
	require.Equal(t, strings.Repeat(" ", left)+"╹"+strings.Repeat("▀", 74), snapshot.Lines[borderRow],
		"the zero-based bottom border spans columns 63 through 137")
	require.Empty(t, snapshot.Lines[borderRow+1])
	return snapshot
}

func TestOpenCodeCenteredPlaceholderStyle(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	const (
		placeholder = "Ask anything"
		exact       = "\x1b[38;5;8;48;5;234m"
		typed       = "\x1b[38;5;255;48;5;234m"
		reset       = "\x1b[0m"
	)

	tests := []struct {
		name    string
		painted string
		text    string
		ok      bool
		reason  string
	}{
		{name: "exact prefix", painted: exact + placeholder + reset, ok: true},
		{name: "exact prefix with suffix", painted: exact + placeholder + " to start" + reset, ok: true},
		{name: "typed text", painted: typed + "hello" + reset, text: "hello", ok: true, reason: LiveScreenInputNotEmpty},
		{name: "different exact-style text", painted: exact + "Try anything" + reset, text: "Try anything", ok: true, reason: LiveScreenInputNotEmpty},
		{name: "mixed foreground", painted: exact + placeholder + typed + "x" + reset, text: placeholder + "x", ok: true, reason: LiveScreenInputNotEmpty},
		{name: "bold placeholder", painted: "\x1b[1;38;5;8;48;5;234m" + placeholder + reset, text: placeholder, ok: true, reason: LiveScreenInputNotEmpty},
		{name: "wrong background", painted: "\x1b[38;5;8;48;5;235m" + placeholder + reset, text: placeholder, ok: true, reason: LiveScreenInputNotEmpty},
		{name: "true color foreground", painted: "\x1b[38;2;128;128;128;48;5;234m" + placeholder + reset, text: placeholder, ok: true, reason: LiveScreenInputNotEmpty},
		{name: "unstyled placeholder", painted: placeholder, text: placeholder, ok: true, reason: LiveScreenInputNotEmpty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := newOpenCodeCenteredPlaceholderSnapshot(t, test.painted)
			text, ok := draft.CursorInput(profile, snapshot)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.text, text)
			rows, reason := LiveScreen(profile, snapshot)
			require.Equal(t, test.reason, reason)
			if test.reason == "" {
				require.NotNil(t, rows)
				require.Equal(t, maskedInputRow, rows[snapshot.CursorRow])
			} else {
				require.Nil(t, rows)
			}
		})
	}

	t.Run("missing style evidence", func(t *testing.T) {
		t.Parallel()
		snapshot := newOpenCodeCenteredPlaceholderSnapshot(t, exact+placeholder+reset)
		snapshot.CursorCells = nil
		text, ok := draft.CursorInput(profile, snapshot)
		require.False(t, ok)
		require.Empty(t, text)
		rows, reason := LiveScreen(profile, snapshot)
		require.Nil(t, rows)
		require.Equal(t, LiveScreenNoInputEvidence, reason)
	})

	t.Run("placeholder cursor outside input", func(t *testing.T) {
		for _, cursorColumn := range []int{0, 62, 63, 150, 199} {
			t.Run(fmt.Sprintf("column %d", cursorColumn), func(t *testing.T) {
				t.Parallel()
				snapshot := newOpenCodeCenteredPlaceholderSnapshot(t, exact+placeholder+reset)
				snapshot.CursorCol = cursorColumn
				text, ok := draft.CursorInput(profile, snapshot)
				require.False(t, ok)
				require.Empty(t, text)
				rows, reason := LiveScreen(profile, snapshot)
				require.Nil(t, rows)
				require.Equal(t, LiveScreenNoInputEvidence, reason)
			})
		}
	})
}

func TestOpenCodeMeasuredLayoutsSeparateSidebar(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	for _, columns := range []int{80, 100, 120, 140, 160, 180, 200, 220} {
		t.Run(fmt.Sprintf("%d columns", columns), func(t *testing.T) {
			t.Parallel()
			sidebar := columns >= 140
			snapshot := newOpenCodeLayoutSnapshot(t, columns, openCodeLayoutFrame{sidebar: sidebar})
			if sidebar {
				require.False(t, profile.InputBounds.Below[0].Matches(snapshot.Lines[snapshot.CursorRow+1]),
					"the raw sidebar row must not satisfy the strict boundary")
			}
			requireOpenCodeEmptyScreen(t, profile, snapshot)
		})
	}
}

func TestOpenCodeIndentedContinuationIsStillDraft(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	snapshot := newOpenCodeLayoutSnapshot(t, 200, openCodeLayoutFrame{
		input:   []string{"", strings.Repeat(" ", 40) + "text"},
		cursor:  0,
		sidebar: true,
	})
	require.Contains(t, snapshot.Lines[snapshot.CursorRow+1], "text")
	text, ok := draft.CursorInput(profile, snapshot)
	require.False(t, ok)
	require.Empty(t, text)
	rows, reason := LiveScreen(profile, snapshot)
	require.Equal(t, LiveScreenNoInputEvidence, reason)
	require.Nil(t, rows)
}

func TestOpenCodeUnprovenLayoutsStayWithheld(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	tests := []struct {
		name    string
		columns int
		frame   openCodeLayoutFrame
	}{
		{name: "unmeasured width", columns: 130, frame: openCodeLayoutFrame{}},
		{name: "incomplete border", columns: 200, frame: openCodeLayoutFrame{sidebar: true, shortBorder: true}},
		{name: "duplicate border", columns: 200, frame: openCodeLayoutFrame{sidebar: true, duplicateBorder: true}},
		{name: "misaligned border", columns: 200, frame: openCodeLayoutFrame{sidebar: true, misalignedBorder: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := newOpenCodeLayoutSnapshot(t, test.columns, test.frame)
			text, ok := draft.CursorInput(profile, snapshot)
			require.False(t, ok)
			require.Empty(t, text)
			rows, reason := LiveScreen(profile, snapshot)
			require.Equal(t, LiveScreenNoInputEvidence, reason)
			require.Nil(t, rows)
		})
	}
}

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

func TestOpenCodeResizeRequiresTheNewCompleteBorder(t *testing.T) {
	t.Parallel()
	profile := openCodeLayoutProfile(t)
	emulator, err := vt.New(200, 50)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })

	requireOpenCodeEmptyScreen(t, profile, writeOpenCodeLayoutFrame(t, emulator, 200, openCodeLayoutFrame{sidebar: true}))
	require.NoError(t, emulator.Resize(120, 50))
	requireOpenCodeEmptyScreen(t, profile, writeOpenCodeLayoutFrame(t, emulator, 120, openCodeLayoutFrame{}))

	require.NoError(t, emulator.Resize(200, 50))
	transient := emulator.Snapshot()
	require.True(t, transient.CursorVisible)
	require.Contains(t, transient.Lines[transient.CursorRow], "┃")
	text, ok := draft.CursorInput(profile, transient)
	require.False(t, ok, "the old short border must not prove the resized box")
	require.Empty(t, text)

	requireOpenCodeEmptyScreen(t, profile, writeOpenCodeLayoutFrame(t, emulator, 200, openCodeLayoutFrame{sidebar: true}))
}
