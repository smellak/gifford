package replay

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

const (
	openCodePermissionVersion       = "opencode 1.18.29"
	replayOpenCodeChannel           = "  ┃"
	replayOpenCodePermissionTitle   = "  ┃  △ Permission required"
	replayOpenCodePermissionOptions = "  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm"
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
	snap.Lines[40] = replayOpenCodePermissionTitle
	snap.Lines[41] = "  ┃  Read a variable resource"
	snap.Lines[46] = replayOpenCodePermissionOptions
	return snap
}

// replayResolvedOpenCodePermission constructs the same measured residual
// permission plus idle-input layout used by the live H35 regression. No
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
		replayOpenCodePermissionTitle,
		"  ┃  variable action",
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		replayOpenCodeChannel,
		replayOpenCodePermissionOptions,
		replayOpenCodeChannel,
	}
	copy(snap.Lines[9:], permission)
	return snap
}

type permissionPairOptions struct {
	humanBetween     bool
	resizeBetween    bool
	resolvedIdle     bool
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
		reads = append(reads,
			timedRead{at: 900, data: []byte("\xe2\x9c")},
			timedRead{at: 1500, data: append([]byte("\x93"), modal...)},
		)
		chunkAt = 1600
	case permissionFrameSyncOpen:
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

			require.Equal(t, session.LiveScreenNoInputEvidence, windows[0].LiveWithheld)
			require.Empty(t, windows[0].Screen.Rows)
			require.Empty(t, windows[0].ObservationForm)

			require.Empty(t, windows[1].LiveWithheld)
			require.Equal(t, dataset.ObservationFormPermissionDialogV1, windows[1].ObservationForm)
			require.Equal(t, []string{session.PermissionDialogMarker}, windows[1].Screen.Rows)
			require.Equal(t, uint64(1), windows[1].ObservationEpisode)
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

// cutLiveV2BeforeInactiveReasonFix preserves the former inactive-tracker
// branch so the regression test can prove that identical VT bytes change
// only LiveWithheld, never the window disposition or projected form.
func cutLiveV2BeforeInactiveReasonFix(st *state, rec *dataset.Recording, snap vt.Snapshot) {
	if st.pendingPermissionRead != nil {
		rec.Screen.Rows, rec.LiveWithheld = nil, session.PermissionDialogFrameIncomplete
		return
	}
	_, permissionReason := st.permission.Screen(true, true, st.keyGeneration)
	if rows, reason := session.LiveScreen(*st.live, snap); reason == "" {
		rec.Screen.Rows, rec.Draft = rows, dataset.DraftClosed
		rec.ObservationForm = dataset.ObservationFormEmptyInputV1
		return
	}
	rec.Screen.Rows, rec.LiveWithheld = nil, permissionReason
}

func writePermissionFrame(t *testing.T, emulator *vt.Emulator, frame vt.Snapshot) vt.Snapshot {
	t.Helper()
	raw := permissionSnapshotANSI(frame)
	written, err := emulator.Write(raw)
	require.NoError(t, err)
	require.Equal(t, len(raw), written)
	painted := emulator.Snapshot()
	require.False(t, painted.Pending)
	return painted
}

func TestLiveV2InactivePermissionTrackerAttributesPhysicalReasonOnly(t *testing.T) {
	t.Parallel()
	allProfiles := loadProfiles(t)
	profile, err := allProfiles.Profile("opencode", "1.18.29")
	require.NoError(t, err)

	parser, err := vt.New(120, 50)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	partialBytes := []byte{0xe2, 0x9c}
	written, err := parser.Write(partialBytes)
	require.NoError(t, err)
	require.Equal(t, len(partialBytes), written)
	partial := parser.Snapshot()
	require.True(t, partial.Pending)
	completionBytes := []byte{0x93, 0x1b, '[', '0', 'm'}
	written, err = parser.Write(completionBytes)
	require.NoError(t, err)
	require.Equal(t, len(completionBytes), written)
	complete := parser.Snapshot()
	require.False(t, complete.Pending)

	idleEmulator, err := vt.New(120, 50)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idleEmulator.Close()) })
	idle := writePermissionFrame(t, idleEmulator, replayResolvedOpenCodePermission())
	_, idleReason := session.LiveScreen(profile, idle)
	require.Empty(t, idleReason)

	tests := []struct {
		name        string
		snapshot    vt.Snapshot
		readPending bool
		before      string
		after       string
	}{
		{
			name: "partial physical failure", snapshot: partial, readPending: true,
			before: session.PermissionDialogFrameIncomplete, after: session.LiveScreenNoInputEvidence,
		},
		{
			name: "complete physical failure", snapshot: complete,
			before: session.PermissionDialogNotMatched, after: session.LiveScreenNoInputEvidence,
		},
		{
			name: "unsealed physical success", snapshot: idle, readPending: true,
			before: session.PermissionDialogFrameIncomplete, after: session.PermissionDialogFrameIncomplete,
		},
		{name: "sealed physical success", snapshot: idle},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newState := func() state {
				tracker, ok := session.NewPermissionDialogTracker(protocol.HarnessOpencode, "1.18.29")
				require.True(t, ok)
				tracker.Observe(session.PermissionDialogFrame{
					Snapshot: tc.snapshot, GuardClosed: true, InputAligned: true,
					IdleInputVisible: session.IdleInputVisible(profile, tc.snapshot),
				})
				require.False(t, tracker.Active())
				st := state{
					guardClosed: true, permissionGuardClosed: true,
					permission: tracker, live: &profile,
				}
				if tc.readPending {
					st.pendingPermissionRead = &pendingPermissionRead{
						index: 1, digest: session.ChunkDigest(append(partialBytes, completionBytes...)),
					}
				}
				return st
			}

			beforeState, afterState := newState(), newState()
			var before, after dataset.Recording
			cutLiveV2BeforeInactiveReasonFix(&beforeState, &before, tc.snapshot)
			afterState.cutLiveV2(&after, tc.snapshot)
			require.Equal(t, tc.before, before.LiveWithheld)
			require.Equal(t, tc.after, after.LiveWithheld)

			before.LiveWithheld, after.LiveWithheld = "", ""
			require.Equal(t, before, after, "identical VT bytes must retain the same window disposition and form")
		})
	}
}

func TestLiveV2ActivePermissionTrackerReasonsStayAuthoritative(t *testing.T) {
	t.Parallel()
	allProfiles := loadProfiles(t)
	profile, err := allProfiles.Profile("opencode", "1.18.29")
	require.NoError(t, err)

	tests := []struct {
		name   string
		want   string
		mutate func(*testing.T, *state, *vt.Emulator, vt.Snapshot) vt.Snapshot
	}{
		{
			name: "incomplete frame", want: session.PermissionDialogFrameIncomplete,
			mutate: func(t *testing.T, st *state, emulator *vt.Emulator, _ vt.Snapshot) vt.Snapshot {
				raw := []byte{0xe2, 0x9c}
				written, err := emulator.Write(raw)
				require.NoError(t, err)
				require.Equal(t, len(raw), written)
				partial := emulator.Snapshot()
				require.True(t, partial.Pending)
				st.permission.Observe(session.PermissionDialogFrame{
					Snapshot: partial, GuardClosed: true, InputAligned: true,
				})
				st.pendingPermissionRead = &pendingPermissionRead{index: 3, digest: session.ChunkDigest(raw)}
				return partial
			},
		},
		{
			name: "signature changed", want: session.PermissionDialogSignatureChanged,
			mutate: func(t *testing.T, st *state, emulator *vt.Emulator, _ vt.Snapshot) vt.Snapshot {
				changed := replayOpenCodePermission()
				changed.Lines[46] = replayOpenCodeChannel
				changed.Lines[47] = replayOpenCodePermissionOptions
				changed.Lines[48] = replayOpenCodeChannel
				painted := writePermissionFrame(t, emulator, changed)
				st.permission.Observe(session.PermissionDialogFrame{
					Snapshot: painted, GuardClosed: true, InputAligned: true,
				})
				return painted
			},
		},
		{
			name: "ambiguous", want: session.PermissionDialogAmbiguous,
			mutate: func(t *testing.T, st *state, emulator *vt.Emulator, _ vt.Snapshot) vt.Snapshot {
				ambiguous := replayOpenCodePermission()
				for row := 9; row <= 17; row++ {
					ambiguous.Lines[row] = replayOpenCodeChannel
				}
				ambiguous.Lines[10] = replayOpenCodePermissionTitle
				ambiguous.Lines[11] = "  ┃  Read another variable resource"
				ambiguous.Lines[16] = replayOpenCodePermissionOptions
				painted := writePermissionFrame(t, emulator, ambiguous)
				st.permission.Observe(session.PermissionDialogFrame{
					Snapshot: painted, GuardClosed: true, InputAligned: true,
				})
				return painted
			},
		},
		{
			name: "human input", want: session.PermissionDialogHumanInput,
			mutate: func(_ *testing.T, st *state, _ *vt.Emulator, snap vt.Snapshot) vt.Snapshot {
				st.keyGeneration = 1
				st.permission.HumanInput(1)
				return snap
			},
		},
		{
			name: "resize", want: session.PermissionDialogResized,
			mutate: func(_ *testing.T, st *state, _ *vt.Emulator, snap vt.Snapshot) vt.Snapshot {
				st.permission.Resize()
				return snap
			},
		},
		{
			name: "guard not closed", want: session.PermissionDialogGuardNotClosed,
			mutate: func(_ *testing.T, st *state, _ *vt.Emulator, snap vt.Snapshot) vt.Snapshot {
				st.permissionGuardClosed = false
				return snap
			},
		},
		{
			name: "input unaligned", want: session.PermissionDialogInputUnaligned,
			mutate: func(_ *testing.T, st *state, _ *vt.Emulator, snap vt.Snapshot) vt.Snapshot {
				st.parserPending = true
				return snap
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			emulator, err := vt.New(120, 50)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, emulator.Close()) })
			tracker, ok := session.NewPermissionDialogTracker(protocol.HarnessOpencode, "1.18.29")
			require.True(t, ok)

			prior := writePermissionFrame(t, emulator, vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50)})
			tracker.Observe(session.PermissionDialogFrame{
				Snapshot: prior, GuardClosed: true, InputAligned: true,
			})
			modal := writePermissionFrame(t, emulator, replayOpenCodePermission())
			tracker.Observe(session.PermissionDialogFrame{
				Snapshot: modal, GuardClosed: true, InputAligned: true,
			})
			require.True(t, tracker.Active())

			st := state{
				guardClosed: true, permissionGuardClosed: true,
				permission: tracker, live: &profile,
			}
			snap := tc.mutate(t, &st, emulator, modal)
			var rec dataset.Recording
			st.cutLiveV2(&rec, snap)
			require.Equal(t, tc.want, rec.LiveWithheld)
			require.Nil(t, rec.Screen.Rows)
			require.Empty(t, rec.ObservationForm)
			require.True(t, tracker.Active(), "an active candidate must not fall back to the physical projection")
		})
	}
}
