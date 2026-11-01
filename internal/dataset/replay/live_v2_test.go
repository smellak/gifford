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

const openCodePermissionVersion = "opencode 1.18.29"

func permissionSnapshotANSI(snap vt.Snapshot) []byte {
	var out strings.Builder
	out.WriteString("\x1b[H\x1b[2J")
	for row, line := range snap.Lines {
		if line != "" {
			_, _ = fmt.Fprintf(&out, "\x1b[%d;1H%s", row+1, line)
		}
	}
	out.WriteString("\x1b[1;1H\x1b[0m")
	return []byte(out.String())
}

func replayOpenCodePermission() vt.Snapshot {
	snap := vt.Snapshot{Cols: 120, Rows: 50, Lines: make([]string, 50)}
	for row := 39; row <= 47; row++ {
		snap.Lines[row] = "  ┃"
	}
	snap.Lines[40] = "  ┃  △ Permission required"
	snap.Lines[41] = "  ┃  Read a variable resource"
	snap.Lines[46] = "  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm"
	return snap
}

type permissionPairOptions struct {
	humanBetween     bool
	resizeBetween    bool
	omitSeal         bool
	omitGenuineClose bool
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
	reads := [][]byte{permissionSnapshotANSI(prior), permissionSnapshotANSI(replayOpenCodePermission())}
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
	emitRead(100, 1, reads[0], 0)
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
	emitRead(200, 2, reads[1], keyGeneration)
	all := append(append([]byte(nil), reads[0]...), reads[1]...)
	recordingSequence, err := rec.Append(t0.Add(300*time.Millisecond), 300*int64(time.Millisecond), all)
	require.NoError(t, err)
	emit(300, protocol.EventTypeIOChunk, session.IOChunkEvent{
		ByteCount: int64(len(all)), ANSIPresent: true, ScreenHashAfter: vt.Hash(emu.Snapshot()),
		RecordingSequence: recordingSequence, ChunkDigest: session.ChunkDigest(all),
	})
	summary := &session.PermissionDialogEvidenceSummary{
		Version: session.PermissionDialogEvidenceVersion, Arrivals: arrivals, InputEvents: arrivals,
		KeyGeneration: keyGeneration, OutputReads: 2, Frames: 2,
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
		require.Equal(t, session.PermissionDialogPolicyVersion, window.ObservationPolicyVersion)
		require.True(t, window.ObservationEpisodeDigest.Valid())
		require.True(t, window.ObservationAnchorDigest.Valid())
		require.NotContains(t, strings.Join(window.Screen.Rows, "\n"), "CANARY")
	}
	require.Equal(t, windows[0].ObservationEpisodeDigest, windows[1].ObservationEpisodeDigest)
	require.NotEqual(t, windows[0].ObservationAnchorDigest, windows[1].ObservationAnchorDigest)
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
