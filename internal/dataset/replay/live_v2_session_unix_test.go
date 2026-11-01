//go:build unix

package replay

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

type permissionSessionSink struct {
	mu     sync.Mutex
	next   session.EventSink
	events []session.Event
}

func (s *permissionSessionSink) Emit(event session.Event) {
	s.next.Emit(event)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *permissionSessionSink) count(kind protocol.EventType) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, event := range s.events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

func (s *permissionSessionSink) lastInput() (session.PermissionDialogInputEvidence, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := len(s.events) - 1; index >= 0; index-- {
		if evidence, ok := s.events[index].Payload.(session.PermissionDialogInputEvidence); ok {
			return evidence, true
		}
	}
	return session.PermissionDialogInputEvidence{}, false
}

func (s *permissionSessionSink) lastFrame() (session.PermissionDialogFrameEvidence, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := len(s.events) - 1; index >= 0; index-- {
		if evidence, ok := s.events[index].Payload.(session.PermissionDialogFrameEvidence); ok {
			return evidence, true
		}
	}
	return session.PermissionDialogFrameEvidence{}, false
}

func permissionSessionANSI(snap vt.Snapshot) string {
	var out strings.Builder
	out.WriteString("\x1b[H\x1b[2J")
	for row, line := range snap.Lines {
		if line != "" {
			_, _ = fmt.Fprintf(&out, "\x1b[%d;1H%s", row+1, line)
		}
	}
	out.WriteString("\x1b[0m")
	if snap.CursorRow >= 0 && snap.CursorRow < snap.Rows && snap.CursorCol >= 0 && snap.CursorCol < snap.Cols {
		_, _ = fmt.Fprintf(&out, "\x1b[%d;%dH", snap.CursorRow+1, snap.CursorCol+1)
	}
	if snap.CursorVisible {
		out.WriteString("\x1b[?25h")
	} else {
		out.WriteString("\x1b[?25l")
	}
	return out.String()
}

func permissionSessionEmptyOpenCode() vt.Snapshot {
	snap := vt.Snapshot{
		Cols: 120, Rows: 50, Lines: make([]string, 50),
		CursorRow: 44, CursorCol: 5, CursorVisible: true,
	}
	snap.Lines[42] = "synthetic output"
	for row := 43; row < 47; row++ {
		snap.Lines[row] = "  ┃"
	}
	snap.Lines[46] = "  ┃  Meta"
	snap.Lines[47] = "  ╹" + strings.Repeat("▀", 115)
	return snap
}

func signalPermissionSession(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, nil, 0o600))
}

func TestLiveV2ReplaysFragmentedTerminalResponseCausally(t *testing.T) {
	data := t.TempDir()
	const id = "permission-fragmented"
	master, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
	require.NoError(t, pty.Setsize(slave, &pty.Winsize{Cols: 120, Rows: 50}))

	profiles := loadProfiles(t)
	profile, err := profiles.Profile("opencode", "1.18.29")
	require.NoError(t, err)
	empty := permissionSessionANSI(permissionSessionEmptyOpenCode())
	priorSnapshot := vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50), CursorVisible: true}
	priorSnapshot.Lines[0] = "prior non-modal frame"
	prior := permissionSessionANSI(priorSnapshot)
	modal := permissionSessionANSI(replayOpenCodePermission())

	closeStep := filepath.Join(data, "close")
	priorOne := filepath.Join(data, "prior-one")
	modalOne := filepath.Join(data, "modal-one")
	inert := filepath.Join(data, "inert")
	priorTwo := filepath.Join(data, "prior-two")
	modalTwo := filepath.Join(data, "modal-two")
	command := `
stty raw -echo
printf '%s' "$GIFFORD_EMPTY"
dd bs=1 count=7 of=/dev/null 2>/dev/null
while [ ! -f "$GIFFORD_CLOSE" ]; do sleep 0.01; done
printf '%s' "$GIFFORD_EMPTY"
while [ ! -f "$GIFFORD_PRIOR_ONE" ]; do sleep 0.01; done
printf '%s' "$GIFFORD_PRIOR"
while [ ! -f "$GIFFORD_MODAL_ONE" ]; do sleep 0.01; done
printf '%s' "$GIFFORD_MODAL"
dd bs=1 count=2 of=/dev/null 2>/dev/null
while [ ! -f "$GIFFORD_INERT" ]; do sleep 0.01; done
printf '\033[0m'
dd bs=1 count=4 of=/dev/null 2>/dev/null
while [ ! -f "$GIFFORD_PRIOR_TWO" ]; do sleep 0.01; done
printf '%s' "$GIFFORD_PRIOR"
while [ ! -f "$GIFFORD_MODAL_TWO" ]; do sleep 0.01; done
printf '%s' "$GIFFORD_MODAL"
dd bs=1 count=8 of=/dev/null 2>/dev/null
`
	environment := append(os.Environ(),
		"GIFFORD_EMPTY="+empty,
		"GIFFORD_PRIOR="+prior,
		"GIFFORD_MODAL="+modal,
		"GIFFORD_CLOSE="+closeStep,
		"GIFFORD_PRIOR_ONE="+priorOne,
		"GIFFORD_MODAL_ONE="+modalOne,
		"GIFFORD_INERT="+inert,
		"GIFFORD_PRIOR_TWO="+priorTwo,
		"GIFFORD_MODAL_TWO="+modalTwo,
	)
	log, err := session.CreateEventLog(data, id, session.EventLogOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	sink := &permissionSessionSink{next: log}
	run, err := session.NewSession(session.Options{
		Command: []string{"sh", "-c", command}, Env: environment,
		In: slave, Out: slave, DataDir: data, SessionID: id,
		Profile: profile, Sink: sink, Notice: io.Discard,
		HarnessVersion: openCodePermissionVersion, PermissionDialogs: true,
		ChunkMinBytes: 1,
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, runErr := run.Run(t.Context()); done <- runErr }()

	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogFrame) >= 1
	}, 10*time.Second, time.Millisecond)
	inputs := sink.count(session.EventTypePermissionDialogInput)
	_, err = master.Write([]byte("warmup\r"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogInput) > inputs
	}, 10*time.Second, time.Millisecond)
	frames := sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, closeStep)
	require.Eventually(t, func() bool {
		return sink.count(protocol.EventTypeInputDraftSubmitted) >= 1 &&
			sink.count(session.EventTypePermissionDialogFrame) > frames
	}, 10*time.Second, time.Millisecond)

	frames = sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, priorOne)
	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogFrame) > frames
	}, 10*time.Second, time.Millisecond)
	frames = sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, modalOne)
	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogFrame) > frames
	}, 10*time.Second, time.Millisecond)
	report, _ := run.Observe()
	require.Equal(t, protocol.StateObservationScreenAvailable, report.ScreenState)
	require.Equal(t, []string{session.PermissionDialogMarker}, report.ScreenRows)

	inputs = sink.count(session.EventTypePermissionDialogInput)
	_, err = master.Write([]byte("\x1b["))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		evidence, ok := sink.lastInput()
		return ok && sink.count(session.EventTypePermissionDialogInput) > inputs && evidence.ParserPending
	}, 10*time.Second, time.Millisecond)
	frames = sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, inert)
	require.Eventually(t, func() bool {
		evidence, ok := sink.lastFrame()
		return ok && sink.count(session.EventTypePermissionDialogFrame) > frames && !evidence.InputAligned
	}, 10*time.Second, time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	inputs = sink.count(session.EventTypePermissionDialogInput)
	_, err = master.Write([]byte("1;1R"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		evidence, ok := sink.lastInput()
		return ok && sink.count(session.EventTypePermissionDialogInput) > inputs &&
			evidence.Kind == session.PermissionDialogInputTerminal && !evidence.ParserPending
	}, 10*time.Second, time.Millisecond)
	frames = sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, priorTwo)
	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogFrame) > frames
	}, 10*time.Second, time.Millisecond)
	frames = sink.count(session.EventTypePermissionDialogFrame)
	signalPermissionSession(t, modalTwo)
	require.Eventually(t, func() bool {
		return sink.count(session.EventTypePermissionDialogFrame) > frames
	}, 10*time.Second, time.Millisecond)
	report, _ = run.Observe()
	require.Equal(t, protocol.StateObservationScreenAvailable, report.ScreenState)
	require.Equal(t, []string{session.PermissionDialogMarker}, report.ScreenRows)

	time.Sleep(50 * time.Millisecond)
	_, err = master.Write([]byte("__done__"))
	require.NoError(t, err)
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(30 * time.Second):
		t.Fatal("session did not end")
	}
	require.NoError(t, log.Close())

	source, err := Open(Options{
		DataDir: data, SessionID: id, Every: 10 * time.Millisecond,
		LiveV2: &LiveV2Options{Profiles: profiles, PermissionSources: []session.PermissionDialogSource{{
			Harness: protocol.HarnessOpencode, HarnessVersion: openCodePermissionVersion,
		}}},
	})
	require.NoError(t, err)
	windows := drain(t, source)
	require.NotEmpty(t, windows)
	unaligned, recovered := false, false
	for _, window := range windows {
		unaligned = unaligned || window.LiveWithheld == session.PermissionDialogInputUnaligned
		recovered = recovered || window.ObservationForm == dataset.ObservationFormPermissionDialogV1 &&
			window.ObservationEpisode == 2
	}
	require.True(t, unaligned, "the incomplete terminal response must be withheld")
	require.True(t, recovered, "the later complete terminal response must permit a fresh episode")
}
