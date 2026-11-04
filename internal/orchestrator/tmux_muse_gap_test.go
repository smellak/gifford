package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
)

// museGapFrame is a synthetic public 1.4.14 frame. Muse prints transcript
// lines while it works; for about 200 ms per line the Thinking row is gone.
func museGapFrame(lines int, thinking bool) []byte {
	rule := "\x1b[2m\x1b[38;5;242m" + strings.Repeat("─", 94)
	var screen strings.Builder
	screen.WriteString("  Muse Code 1.4.14\n\n")
	for range lines {
		screen.WriteString("    Public summary line\n")
	}
	if thinking {
		screen.WriteString("\x1b[38;5;242m◇ Thinking (42s · esc to interrupt)\x1b[39m")
	}
	screen.WriteString("\n\n" + rule + "\n\x1b[0m\x1b[38;5;75m❯ \x1b[39m\n" + rule + "\n" +
		"\x1b[38;5;242m  \x1b[38;5;75mmuse-model\x1b[38;5;246m · xhigh · ~ · \x1b[38;5;211mYOLO\x1b[39m")
	return []byte(screen.String())
}

// The measured gap reads idle to the detector, but the screen rule never
// counts it as rest: each gap frame shows new text, so the quiet window
// restarts and the agent is not quiet while the turn goes on.
func TestTmuxScreenRuleNeverTakesTheMuseMarkGapForRest(t *testing.T) {
	t.Parallel()
	runtime := &tmuxAttachRuntime{
		quietWindow: 5 * time.Second,
		output:      make(map[protocol.ULID]tmuxOutputSeen),
	}
	procs := []detect.Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{"panel"}},
		{
			PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200,
			Argv: []string{"/usr/local/bin/muse-bin-1.4.14-R7309.1"},
		},
	}
	const instance protocol.ULID = "01JB0000000000000000000009"
	now := time.Date(2026, time.November, 4, 12, 0, 0, 0, time.UTC)
	sawGap := false
	// Forty seconds of a turn: each line spends 200 ms without the row.
	for line := range 40 {
		for _, thinking := range []bool{true, false} {
			frame := museGapFrame(line+1, thinking)
			state := detect.Classify(detect.HarnessMuse, detect.DefaultTable(), frame,
				156, 58, 100, procs)
			require.True(t, state.Certain, state.Detail)
			sawGap = sawGap || !state.Busy
			rule := runtime.screenRule(instance, state, frame, now)
			require.False(t, rule.agentQuiet, "line %d thinking=%v", line, thinking)
			if thinking {
				now = now.Add(800 * time.Millisecond)
			} else {
				now = now.Add(200 * time.Millisecond)
			}
		}
	}
	require.True(t, sawGap, "the fixture must reproduce the measured gap")

	// Once the turn ends the same screen stays and the rule reaches rest.
	frame := museGapFrame(40, false)
	state := detect.Classify(detect.HarnessMuse, detect.DefaultTable(), frame, 156, 58, 100, procs)
	require.False(t, runtime.screenRule(instance, state, frame, now).agentQuiet)
	now = now.Add(5 * time.Second)
	require.True(t, runtime.screenRule(instance, state, frame, now).agentQuiet)
}
