package detect

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These public-safe synthetic frames preserve the 120x40 layout, ANSI style
// roles and composer/footer geometry measured on twelve Muse Code 1.4.13 bank
// screens. The private captures themselves never enter the repository.
const (
	museBenchCols  = 120
	museBenchRows  = 40
	museBin        = "/usr/bin/muse"
	museBenchTitle = "  Muse Code 1.4.13"
)

func museBenchTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{museBin}},
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
	lines[5] = "◇ Thinking (5s · " + busyInterruptMark + ")"
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
	require.Equal(t, []string{"muse"}, profile.CLIBinaries)
	require.Empty(t, profile.Placeholders, "prompt hints were disabled in the measured screens")
	require.Equal(t, []string{busyInterruptMark}, profile.BusyMarks)
}

func TestVerifyExactMuseNoticeBetweenRules(t *testing.T) {
	t.Parallel()
	certain, exact := VerifyNotice(HarnessMuse, DefaultTable(),
		museBenchFrame("\x1b[38;5;75m❯ \x1b[38;5;252m"+verifyNotice+"\x1b[39m"),
		museBenchCols, museBenchRows, verifyNotice)
	require.True(t, certain)
	require.True(t, exact)
}
