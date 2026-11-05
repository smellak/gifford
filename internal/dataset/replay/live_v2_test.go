package replay

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

const (
	openCodePermissionVersion = "opencode 1.18.29"
	replayOpenCodeChannel     = "  ┃"
)

func permissionSnapshotANSI(snap vt.Snapshot) []byte {
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
	return []byte(out.String())
}

func replayOpenCodePermission() vt.Snapshot {
	snap := vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50)}
	for row := 39; row <= 47; row++ {
		snap.Lines[row] = replayOpenCodeChannel
	}
	snap.Lines[40] = "  ┃  △ Permission required"
	snap.Lines[41] = "  ┃  Read a variable resource"
	snap.Lines[46] = "  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm"
	return snap
}

// replayResolvedOpenCodePermission constructs the same measured residual
// permission plus idle-input layout used by the live native-event label regression. No
// preserved D or #299 capture contains this combined frame.
func replayResolvedOpenCodePermission() vt.Snapshot {
	snap := vt.Snapshot{
		Cols: 120, Rows: 50, Lines: make([]string, 50),
		CursorRow: 44, CursorCol: 5, CursorVisible: true,
	}
	snap.Lines[42] = "synthetic output"
	for row := 43; row < 47; row++ {
		snap.Lines[row] = replayOpenCodeChannel
	}
	snap.Lines[46] = "  ┃  Model"
	snap.Lines[47] = "  ╹" + strings.Repeat("▀", 115)
	permission := []string{
		replayOpenCodeChannel,
		"  ┃  △ Permission required",
		"  ┃  variable action",
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		"  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm",
		replayOpenCodeChannel,
	}
	copy(snap.Lines[9:], permission)
	return snap
}

type permissionPairOptions struct {
	humanBetween     bool
	resizeBetween    bool
	resolvedIdle     bool
	activeIncomplete bool
	omitSeal         bool
	omitGenuineClose bool
	incomplete       permissionIncompleteFrame
}

type permissionIncompleteFrame uint8

const (
	permissionFrameComplete permissionIncompleteFrame = iota
	permissionFramePending
	permissionFrameSyncOpen
)

func permissionPendingRead(t *testing.T, modal []byte) ([]byte, []byte) {
	t.Helper()
	const readSize = 4095
	cut := -1
	for pos := 0; pos < len(modal); {
		_, size := utf8.DecodeRune(modal[pos:])
		if size == 0 {
			break
		}
		candidate := pos + 1
		if candidate < readSize && size == 3 && (readSize-candidate)%len("\x1b[0m") == 0 {
			cut = candidate
			break
		}
		pos += size
	}
	require.NotEqual(t, -1, cut)
	padding := bytes.Repeat([]byte("\x1b[0m"), (readSize-cut)/len("\x1b[0m"))
	redraw := make([]byte, 0, len(padding)+len(modal))
	redraw = append(redraw, padding...)
	redraw = append(redraw, modal...)
	cut += len(padding)
	require.Equal(t, readSize, cut)
	return redraw[:cut], redraw[cut:]
}

func buildPermissionPair(t *testing.T, opts permissionPairOptions) *pair {
	t.Helper()
	p := &pair{data: t.TempDir()}
	opened := session.PTYOpened{
		Cols: 120, Rows: 50, Harness: protocol.HarnessOpencode, HarnessVersion: openCodePermissionVersion,
		ActivityVersion: outputactivity.Version,
	}
	if !opts.omitSeal {
		opened.ObservationEvidenceVersion = session.PermissionDialogEvidenceVersion
	}
	log := session.NewEventLog(&p.events, sid, session.EventLogOptions{})
	rec := session.NewRecorder(&p.raw)
	emu, err := vt.New(120, 50)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	sequence := int64(0)
	emit := func(ms int64, kind protocol.EventType, payload any) {
		log.Emit(session.Event{
			Session: sid, Type: kind, Payload: payload, Sequence: sequence,
			WallTime: t0.Add(time.Duration(ms) * time.Millisecond), MonotonicNS: ms * int64(time.Millisecond),
		})
		sequence++
	}
	emit(0, protocol.EventTypePTYOpened, opened)
	if !opts.omitGenuineClose {
		emit(50, protocol.EventTypeInputDraftSubmitted, session.InputDraft{
			State: "closed", Generation: 1, GuardVersion: 2,
		})
	}

	prior := vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50)}
	prior.Lines[0] = "CANARY typed before modal"
	type timedRead struct {
		at   int64
		data []byte
	}
	reads := []timedRead{{at: 100, data: permissionSnapshotANSI(prior)}}
	modal := permissionSnapshotANSI(replayOpenCodePermission())
	chunkAt := int64(300)
	switch opts.incomplete {
	case permissionFrameComplete:
		reads = append(reads, timedRead{at: 200, data: modal})
		if opts.resolvedIdle {
			reads = append(reads, timedRead{at: 1200, data: permissionSnapshotANSI(replayResolvedOpenCodePermission())})
			chunkAt = 1300
		}
	case permissionFramePending:
		if opts.activeIncomplete {
			reads = append(reads, timedRead{at: 200, data: modal})
		}
		partial, finish := permissionPendingRead(t, modal)
		reads = append(reads,
			timedRead{at: 900, data: partial},
			timedRead{at: 1500, data: finish},
		)
		chunkAt = 1600
	case permissionFrameSyncOpen:
		if opts.activeIncomplete {
			reads = append(reads, timedRead{at: 200, data: modal})
		}
		reads = append(reads,
			timedRead{at: 900, data: append([]byte("\x1b[?2026h"), modal...)},
			timedRead{at: 1500, data: []byte("\x1b[?2026l")},
		)
		chunkAt = 1600
	default:
		t.Fatalf("unknown incomplete-frame mode %d", opts.incomplete)
	}
	detector := outputactivity.Detector{}
	emitRead := func(ms, index int64, data []byte, keyGeneration uint64) {
		effective := detector.Observe(data)
		emit(ms, protocol.EventTypeIOOutputRead, session.IOOutputRead{
			ByteCount: int64(len(data)), ReadDigest: session.ChunkDigest(data), Effective: effective,
		})
		_, err := emu.Write(data)
		require.NoError(t, err)
		emit(ms+1, session.EventTypePermissionDialogFrame, session.PermissionDialogFrameEvidence{
			Index: uint64(index), ReadDigest: session.ChunkDigest(data), GuardClosed: !opts.omitGenuineClose,
			InputAligned: true, KeyGeneration: keyGeneration,
		})
	}
	emitRead(reads[0].at, 1, reads[0].data, 0)
	keyGeneration := uint64(0)
	arrivals := uint64(0)
	if opts.humanBetween {
		keyGeneration, arrivals = 1, 1
		emit(150, session.EventTypePermissionDialogInput, session.PermissionDialogInputEvidence{
			Index: 1, ArrivalMonotonicNS: 140 * int64(time.Millisecond),
			Kind: session.PermissionDialogInputHuman, KeyGeneration: 1,
		})
	}
	if opts.resizeBetween {
		emit(175, protocol.EventTypeInputResize, session.InputResize{Cols: 120, Rows: 50})
	}
	for index, read := range reads[1:] {
		if opts.resolvedIdle && index == 1 {
			keyGeneration, arrivals = 1, 1
			emit(1100, session.EventTypePermissionDialogInput, session.PermissionDialogInputEvidence{
				Index: 1, ArrivalMonotonicNS: 1090 * int64(time.Millisecond),
				Kind: session.PermissionDialogInputHuman, KeyGeneration: 1,
			})
		}
		emitRead(read.at, int64(index+2), read.data, keyGeneration)
	}
	var all []byte
	for _, read := range reads {
		all = append(all, read.data...)
	}
	recordingSequence, err := rec.Append(t0.Add(time.Duration(chunkAt)*time.Millisecond), chunkAt*int64(time.Millisecond), all)
	require.NoError(t, err)
	emit(chunkAt, protocol.EventTypeIOChunk, session.IOChunkEvent{
		ByteCount: int64(len(all)), ANSIPresent: true, ScreenHashAfter: vt.Hash(emu.Snapshot()),
		RecordingSequence: recordingSequence, ChunkDigest: session.ChunkDigest(all),
	})
	summary := &session.PermissionDialogEvidenceSummary{
		Version: session.PermissionDialogEvidenceVersion, Arrivals: arrivals, InputEvents: arrivals,
		KeyGeneration: keyGeneration, OutputReads: uint64(len(reads)), Frames: uint64(len(reads)),
	}
	if opts.omitSeal {
		summary = nil
	}
	emit(2500, protocol.EventTypePTYClosed, session.PTYClosed{ObservationEvidence: summary})
	require.NoError(t, log.Close())
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	return p
}

func openPermissionV2(t *testing.T, p *pair) ([]dataset.Recording, error) {
	t.Helper()
	profiles := loadProfiles(t)
	source, err := Open(Options{
		DataDir: p.data, SessionID: sid, Every: time.Second,
		LiveV2: &LiveV2Options{Profiles: profiles, PermissionSources: []session.PermissionDialogSource{{
			Harness: protocol.HarnessOpencode, HarnessVersion: openCodePermissionVersion,
		}}},
	})
	if err != nil {
		return nil, err
	}
	return drain(t, source), nil
}

func TestLiveV2PermissionDialogReplayIsCanonicalAndDedupedByEpisode(t *testing.T) {
	t.Parallel()
	windows, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{}))
	require.NoError(t, err)
	require.Len(t, windows, 2)
	for _, window := range windows {
		require.Equal(t, dataset.ObservationFormPermissionDialogV1, window.ObservationForm)
		require.Equal(t, []string{session.PermissionDialogMarker}, window.Screen.Rows)
		require.Equal(t, len(session.PermissionDialogMarker), window.Screen.Cols)
		require.Equal(t, session.OpenCodePermissionDialogPolicyVersion, window.ObservationPolicyVersion)
		require.True(t, window.ObservationEpisodeDigest.Valid())
		require.True(t, window.ObservationAnchorDigest.Valid())
		require.NotContains(t, strings.Join(window.Screen.Rows, "\n"), "CANARY")
	}
	require.Equal(t, windows[0].ObservationEpisodeDigest, windows[1].ObservationEpisodeDigest)
	require.NotEqual(t, windows[0].ObservationAnchorDigest, windows[1].ObservationAnchorDigest)
	legacyDigest := digestOf("gifford.permission-dialog.episode.v1",
		fmt.Sprintf("%s\x00%d\x00%s", sid, 1, session.LegacyOpenCodePermissionDialogPolicyVersion))
	require.NotEqual(t, legacyDigest, windows[0].ObservationEpisodeDigest,
		"the changed causal policy must not reuse the historical episode identity")
}

func TestLiveV2ConstructedResolvedOpenCodePermissionFallsBackToIdleInput(t *testing.T) {
	t.Parallel()
	profiles := loadProfiles(t)
	profile, err := profiles.Profile("opencode", "1.18.29")
	require.NoError(t, err)
	resolved := replayResolvedOpenCodePermission()
	emulator, err := vt.New(resolved.Cols, resolved.Rows)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	_, err = emulator.Write(permissionSnapshotANSI(resolved))
	require.NoError(t, err)
	painted := emulator.Snapshot()
	_, liveReason := session.LiveScreen(profile, painted)
	require.Empty(t, liveReason)
	require.True(t, session.IdleInputVisible(profile, painted))

	windows, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{resolvedIdle: true}))
	require.NoError(t, err)
	require.Len(t, windows, 2)
	require.Equal(t, dataset.ObservationFormPermissionDialogV1, windows[0].ObservationForm)
	require.Equal(t, session.OpenCodePermissionDialogPolicyVersion, windows[0].ObservationPolicyVersion)
	require.Equal(t, dataset.ObservationFormEmptyInputV1, windows[1].ObservationForm)
	require.Empty(t, windows[1].ObservationPolicyVersion)
	require.Empty(t, windows[1].LiveWithheld)
	require.Contains(t, windows[1].Screen.Rows, "synthetic output")
	require.NotContains(t, windows[1].Screen.Rows, session.PermissionDialogMarker)
}

func TestLiveV2PermissionDialogReplayRetainsPriorAcrossIncompleteFrame(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mode permissionIncompleteFrame
	}{
		{name: "parser pending", mode: permissionFramePending},
		{name: "synchronized update open", mode: permissionFrameSyncOpen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			windows, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{incomplete: tc.mode}))
			require.NoError(t, err)
			require.Len(t, windows, 2)

			require.Equal(t, session.PermissionDialogFrameIncomplete, windows[0].LiveWithheld)
			require.Empty(t, windows[0].Screen.Rows)
			require.Empty(t, windows[0].ObservationForm)

			require.Empty(t, windows[1].LiveWithheld)
			require.Equal(t, dataset.ObservationFormPermissionDialogV1, windows[1].ObservationForm)
			require.Equal(t, []string{session.PermissionDialogMarker}, windows[1].Screen.Rows)
			require.Equal(t, uint64(1), windows[1].ObservationEpisode)
		})
	}
}

func TestLiveV2PermissionDialogReplayRetainsActiveEpisodeAcrossIncompleteFrame(t *testing.T) {
	t.Parallel()
	baseline, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{}))
	require.NoError(t, err)
	require.NotEmpty(t, baseline)
	baselineEpisode := baseline[0].ObservationEpisodeDigest

	for _, tc := range []struct {
		name string
		mode permissionIncompleteFrame
	}{
		{name: "parser pending", mode: permissionFramePending},
		{name: "synchronized update open", mode: permissionFrameSyncOpen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			windows, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{
				incomplete: tc.mode, activeIncomplete: true,
			}))
			require.NoError(t, err)
			require.Len(t, windows, 2)

			require.Equal(t, session.PermissionDialogFrameIncomplete, windows[0].LiveWithheld)
			require.Empty(t, windows[0].Screen.Rows)
			require.Empty(t, windows[0].ObservationForm)
			require.Empty(t, windows[0].ObservationAnchorDigest)

			require.Empty(t, windows[1].LiveWithheld)
			require.Equal(t, dataset.ObservationFormPermissionDialogV1, windows[1].ObservationForm)
			require.Equal(t, []string{session.PermissionDialogMarker}, windows[1].Screen.Rows)
			require.Equal(t, session.OpenCodePermissionDialogPolicyVersion, windows[1].ObservationPolicyVersion)
			require.Equal(t, uint64(1), windows[1].ObservationEpisode)
			require.Equal(t, baselineEpisode, windows[1].ObservationEpisodeDigest,
				"the complete matching frame must continue the same episode")
			require.True(t, windows[1].ObservationAnchorDigest.Valid())
		})
	}
}

func TestLiveV2PermissionDialogRejectsMissingEvidenceKeyAndResize(t *testing.T) {
	_, err := openPermissionV2(t, buildPermissionPair(t, permissionPairOptions{omitSeal: true}))
	require.ErrorIs(t, err, ErrReplay)

	for name, opts := range map[string]permissionPairOptions{
		"initial closed only": {omitGenuineClose: true},
		"human input":         {humanBetween: true},
		"resize":              {resizeBetween: true},
	} {
		t.Run(name, func(t *testing.T) {
			windows, err := openPermissionV2(t, buildPermissionPair(t, opts))
			require.NoError(t, err)
			require.Len(t, windows, 2)
			for _, window := range windows {
				require.Empty(t, window.ObservationForm)
				require.NotEmpty(t, window.LiveWithheld)
				require.Nil(t, window.Screen.Rows)
			}
		})
	}
}

func TestLiveV2RejectsTamperedPostReadFacts(t *testing.T) {
	t.Parallel()
	p := buildPermissionPair(t, permissionPairOptions{})
	tampered := bytes.Replace(p.events.Bytes(), []byte(`"key_generation":0`), []byte(`"key_generation":1`), 1)
	p.write(t, p.raw.Bytes(), tampered)
	_, err := openPermissionV2(t, p)
	require.ErrorIs(t, err, ErrReplay)
}

func TestLiveV2WithholdsInsideAtomicReadBoundary(t *testing.T) {
	t.Parallel()
	tracker, ok := session.NewPermissionDialogTracker(protocol.HarnessOpencode, "1.18.29")
	require.True(t, ok)
	tracker.Observe(session.PermissionDialogFrame{
		Snapshot:    vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50)},
		GuardClosed: true, InputAligned: true,
	})
	modal := replayOpenCodePermission()
	tracker.Observe(session.PermissionDialogFrame{Snapshot: modal, GuardClosed: true, InputAligned: true})

	st := state{
		guardClosed: true, permissionGuardClosed: true, permission: tracker,
		pendingPermissionRead: &pendingPermissionRead{index: 3, digest: session.ChunkDigest([]byte("next frame"))},
	}
	var rec dataset.Recording
	st.cutLiveV2(&rec, modal)
	require.Equal(t, session.PermissionDialogFrameIncomplete, rec.LiveWithheld)
	require.Empty(t, rec.ObservationForm, "replay cannot expose the preceding modal while a new read is still unsealed")
}
