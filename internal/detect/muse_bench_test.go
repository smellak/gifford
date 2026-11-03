package detect

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/vt"
)

// These public-safe synthetic frames preserve the 120x40 layout, ANSI style
// roles and composer/footer geometry measured on twelve Muse Code 1.4.13 bank
// screens. The private captures themselves never enter the repository.
const (
	museBenchCols  = 120
	museBenchRows  = 40
	musePanelBin   = "panel"
	museBin        = "/usr/bin/muse-bin-1.4.13-R6996.1"
	museBenchTitle = "  Muse Code 1.4.13"
	museHintGray   = "\x1b[38;5;242m"
)

func museBenchTree() []Process {
	return museBenchTreeForBinary(museBin)
}

func museBenchTreeForBinary(binary string) []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{musePanelBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{binary}},
	}
}

func museBenchFrame(box ...string) []byte {
	lines := make([]string, museBenchRows)
	lines[1] = museBenchTitle
	lines[3] = strings.Repeat("─", museBenchCols)
	copy(lines[4:], box)
	bottom := 4 + len(box)
	lines[bottom] = strings.Repeat("─", museBenchCols)
	lines[bottom+1] = "  model · high · ~ · Auto-review"
	return []byte(strings.Join(lines, "\n"))
}

func museBenchWorkingFrame() []byte {
	lines := make([]string, museBenchRows)
	lines[1] = museBenchTitle
	lines[3] = "❯ Generate a public example"
	lines[5] = "◇ Thinking (5s · " + interruptMark + ")"
	lines[7] = strings.Repeat("─", museBenchCols)
	lines[8] = "\x1b[38;5;75m❯ \x1b[39m"
	lines[9] = strings.Repeat("─", museBenchCols)
	lines[10] = "  model · high · ~ · Auto-review"
	return []byte(strings.Join(lines, "\n"))
}

func museBenchDialogFrame() []byte {
	lines := make([]string, museBenchRows)
	lines[1] = museBenchTitle
	lines[3] = strings.Repeat("─", museBenchCols)
	lines[4] = "\x1b[38;5;75m❯ \x1b[39m"
	lines[5] = strings.Repeat("─", museBenchCols)
	lines[6] = "  Choose model"
	lines[8] = "  model option"
	lines[13] = "  ↑↓ move · enter confirm · esc go back"
	return []byte(strings.Join(lines, "\n"))
}

// museMeasuredHintFrame reconstructs only the public-safe box, optional task
// panel and footer geometry measured in five private source captures. The
// text is synthetic; rule widths, SGR roles and row order are the evidence.
func museMeasuredHintFrame(cols int, styledContent, taskPanel string, extraBoxRows ...string) []byte {
	lines := []string{
		"\x1b[2m" + museHintGray + strings.Repeat("─", cols),
		"\x1b[0m\x1b[38;5;75m❯ " + styledContent + "\x1b[39m",
	}
	lines = append(lines, extraBoxRows...)
	lines = append(lines, "\x1b[2m"+museHintGray+strings.Repeat("─", cols))
	switch taskPanel {
	case "running":
		lines = append(lines,
			"\x1b[0;1m\x1b[38;5;75mmain\x1b[0m"+museHintGray+" · ↓ to select\x1b[39m",
			museHintGray+"└ \x1b[38;5;247m◆ \x1b[38;5;246mPublic background task  running  2m\x1b[39m",
		)
	case "watching":
		lines = append(lines,
			"\x1b[0;1m\x1b[38;5;75mmain\x1b[0m"+museHintGray+" · ↓ to select\x1b[39m",
			"\x1b[38;5;247m◆ \x1b[38;5;246mPublic background task  watching · 1 queued\x1b[39m",
		)
	}
	lines = append(lines,
		museHintGray+"  \x1b[38;5;75mmuse-model\x1b[38;5;246m · \x1b[38;5;75mhigh"+
			"\x1b[38;5;246m · ~/project · \x1b[38;5;211mYOLO\x1b[39m",
	)
	return []byte(strings.Join(lines, "\n"))
}

func classifyMeasuredMuse(capture []byte, cols int) State {
	rows := strings.Count(string(capture), "\n") + 1
	return Classify(
		HarnessMuse,
		DefaultTable(),
		capture,
		cols,
		rows,
		100,
		museBenchTree(),
	)
}

func TestMuseMeasuredRotatingHintsAreFree(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		cols      int
		hint      string
		taskPanel string
	}{
		{name: "loop hint", cols: 120, hint: "/loop 10m <prompt> schedules a recurring prompt"},
		{name: "wide hint with running task panel", cols: 220, hint: "Ask to monitor a public build and report changes", taskPanel: "running"},
		{name: "narrow goal hint", cols: 94, hint: "/goal pins a session objective"},
		{name: "narrow empty", cols: 94},
		{name: "empty with watching task panel", cols: 95, taskPanel: "watching"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			content := ""
			if tc.hint != "" {
				content = museHintGray + tc.hint
			}
			state := classifyMeasuredMuse(museMeasuredHintFrame(tc.cols, content, tc.taskPanel), tc.cols)
			require.True(t, state.Certain, "detail: %s", state.Detail)
			require.False(t, state.Busy)
			require.False(t, state.BoxBusy, "detail: %s", state.Detail)
			require.True(t, state.IsCLI)
		})
	}
}

func TestMuseRotatingHintRejectsTypedMixedAndWrongIndexedContent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content string
		extra   []string
	}{
		{name: "default color content", content: "\x1b[39mpublic draft"},
		{
			name:    "typed prefix before gray completion",
			content: "\x1b[38;5;252m/lo" + museHintGray + "op 10m <prompt>",
		},
		{name: "one default cell before gray", content: "\x1b[39mx" + museHintGray + "rotating public hint"},
		{name: "gray then default suffix", content: museHintGray + "rotating \x1b[39mdraft"},
		{name: "different indexed gray", content: "\x1b[38;5;241mrotating public hint"},
		{
			name:    "gray plus another box row",
			content: museHintGray + "single row hint",
			extra:   []string{museHintGray + "extra box content\x1b[39m"},
		},
		{name: "decorated gray", content: "\x1b[1;38;5;242mdecorated hint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := classifyMeasuredMuse(museMeasuredHintFrame(120, tc.content, "", tc.extra...), 120)
			require.True(t, state.Certain, "detail: %s", state.Detail)
			require.True(t, state.BoxBusy, "detail: %s", state.Detail)
			require.Equal(t, detailTyped, state.Detail)
		})
	}
}

func TestMuseRotatingHintWithoutStyleCellsIsUncertain(t *testing.T) {
	t.Parallel()
	profile := DefaultTable()[HarnessMuse]
	busy, certain, detail := verifyIndexedGrayHint(
		vt.Snapshot{Cols: 80, Rows: 1, Lines: []string{"❯ rotating public hint"}},
		profile.GrayHint,
		[]int{0},
		"rotating public hint",
	)
	require.True(t, busy)
	require.False(t, certain)
	require.Equal(t, detailTypedUnverified, detail)
}

func TestMuseBenchScreens(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		capture         []byte
		certain         bool
		boxBusy, busy   bool
		writable, isCLI bool
	}{
		{
			name:     "idle empty",
			capture:  museBenchFrame("\x1b[38;5;75m❯ \x1b[39m"),
			certain:  true,
			writable: true,
			isCLI:    true,
		},
		{
			name:    "pending unsent text",
			capture: museBenchFrame("\x1b[38;5;75m❯ \x1b[38;5;252mpending draft\x1b[39m"),
			certain: true,
			boxBusy: true,
			isCLI:   true,
		},
		{
			name:    "working",
			capture: museBenchWorkingFrame(),
			certain: true,
			busy:    true,
			isCLI:   true,
		},
		{
			name:    "dialog",
			capture: museBenchDialogFrame(),
			isCLI:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := Classify(HarnessMuse, DefaultTable(), tc.capture,
				museBenchCols, museBenchRows, 100, museBenchTree())
			require.Equal(t, tc.certain, state.Certain, "detail: %s", state.Detail)
			require.Equal(t, tc.busy, state.Busy)
			require.Equal(t, tc.boxBusy, state.BoxBusy, "detail: %s", state.Detail)
			require.Equal(t, tc.isCLI, state.IsCLI)
			require.Equal(t, tc.writable,
				state.Certain && !state.Busy && !state.BoxBusy && state.IsCLI)
		})
	}
}

func TestMuseBenchProfileRecordsOnlyMeasuredEvidence(t *testing.T) {
	t.Parallel()
	profile := DefaultTable()[HarnessMuse]
	require.True(t, profile.Composer)
	require.Equal(t, []string{"muse", "muse-bin-1.4.13-R6996.1"}, profile.CLIBinaries)
	require.Empty(t, profile.Placeholders, "rotating hint text must never become an allowlist")
	require.Equal(t, museHintGray, profile.GrayHint.Sequence)
	require.Equal(t, uint8(242), profile.GrayHint.IndexedForeground)
	require.True(t, profile.GrayHint.IndexedCells)
	require.Equal(t, []string{interruptMark}, profile.BusyMarks)
}

func TestMuseBenchRejectsUnmeasuredBinaryBasenames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		binary string
		want   bool
	}{
		{name: "measured build", binary: museBin, want: true},
		{name: "unmeasured build", binary: "/usr/bin/muse-bin-1.4.14-R7000.1"},
		{name: "bare build launcher", binary: "/usr/bin/muse-bin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := Classify(HarnessMuse, DefaultTable(), museBenchFrame("❯ "),
				museBenchCols, museBenchRows, 100, museBenchTreeForBinary(tc.binary))
			require.Equal(t, tc.want, state.IsCLI)
		})
	}
}

func TestMuseBenchMeasuredBinaryMutant(t *testing.T) {
	t.Parallel()
	table := DefaultTable()
	profile := table[HarnessMuse]
	profile.CLIBinaries = []string{"muse"}
	table[HarnessMuse] = profile

	state := Classify(HarnessMuse, table, museBenchFrame("❯ "),
		museBenchCols, museBenchRows, 100, museBenchTree())
	require.False(t, state.IsCLI, "removing the measured build must close the write gate")
}

func TestVerifyExactMuseNoticeBetweenRules(t *testing.T) {
	t.Parallel()
	certain, exact := VerifyNotice(HarnessMuse, DefaultTable(),
		museBenchFrame("\x1b[38;5;75m❯ \x1b[38;5;252m"+verifyNotice+"\x1b[39m"),
		museBenchCols, museBenchRows, verifyNotice)
	require.True(t, certain)
	require.True(t, exact)
}
