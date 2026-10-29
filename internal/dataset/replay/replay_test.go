package replay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

const sid = "synth-1"

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

// step is one synthetic session event at ms after pty.opened.
type step struct {
	ms     int64
	output string
	resize [2]int
	draft  string
	other  protocol.EventType
}

// pair writes a session's two files the way gifford run does, with the
// screen hashes a live vt would give.
type pair struct {
	data   string
	raw    bytes.Buffer
	events bytes.Buffer
}

func build(t *testing.T, steps []step) *pair {
	t.Helper()
	p := &pair{data: t.TempDir()}
	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	rec := session.NewRecorder(&p.raw)
	log := session.NewEventLog(&p.events, sid, session.EventLogOptions{})
	seq := int64(0)
	emit := func(ms int64, typ protocol.EventType, payload any) {
		log.Emit(session.Event{
			Session: sid, Type: typ, Payload: payload, Sequence: seq,
			WallTime: t0.Add(time.Duration(ms) * time.Millisecond), MonotonicNS: ms * 1e6,
		})
		seq++
	}
	emit(0, protocol.EventTypePTYOpened, session.PTYOpened{Cols: 80, Rows: 24})
	for _, s := range steps {
		switch {
		case s.output != "":
			_, err := emu.Write([]byte(s.output))
			require.NoError(t, err)
			n, err := rec.Append(t0.Add(time.Duration(s.ms)*time.Millisecond), s.ms*1e6, []byte(s.output))
			require.NoError(t, err)
			emit(s.ms, protocol.EventTypeIOChunk, session.IOChunkEvent{
				ByteCount: int64(len(s.output)), ScreenHashAfter: vt.Hash(emu.Snapshot()), RecordingSequence: n,
			})
		case s.resize[0] != 0:
			require.NoError(t, emu.Resize(s.resize[0], s.resize[1]))
			emit(s.ms, protocol.EventTypeInputResize, session.InputResize{Cols: s.resize[0], Rows: s.resize[1]})
		case s.draft != "":
			emit(s.ms, protocol.EventTypeInputDraftUpdated, session.InputDraft{State: s.draft})
		default:
			emit(s.ms, s.other, session.InputKeyChunk{PrintableCount: 1})
		}
	}
	require.NoError(t, log.Close())
	return p
}

func (p *pair) write(t *testing.T, raw, events []byte) {
	t.Helper()
	dir := session.RecordingsDir(p.data)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	if raw != nil {
		require.NoError(t, os.WriteFile(filepath.Join(dir, sid+session.RecordingSuffix), raw, 0o600))
	}
	if events != nil {
		require.NoError(t, os.WriteFile(filepath.Join(dir, sid+session.EventsSuffix), events, 0o600))
	}
}

func windows(t *testing.T, data string, every time.Duration) []dataset.Recording {
	t.Helper()
	return windowsOf(t, data, sid, every)
}

func windowsOf(t *testing.T, data, id string, every time.Duration) []dataset.Recording {
	t.Helper()
	src, err := Open(Options{DataDir: data, SessionID: id, Every: every, Harness: protocol.HarnessCodex})
	require.NoError(t, err)
	var out []dataset.Recording
	for {
		r, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, r)
	}
}

func TestWindowsAndFeatures(t *testing.T) {
	t.Parallel()

	p := build(t, []step{
		{ms: 100, output: "hello\r\n"},
		{ms: 400, draft: "open"},
		{ms: 1500, output: "0123456789\r\n"},
		{ms: 1600, draft: "closed"},
		{ms: 2600, resize: [2]int{100, 30}},
		{ms: 3000, other: protocol.EventTypeInputKeyChunk},
	})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws := windows(t, p.data, time.Second)
	require.Len(t, ws, 3)

	type got struct {
		quiet      float64
		b500, b2   int64
		cols, rows int
		draft      dataset.DraftState
		from, to   int64
	}
	all := make([]got, 0, len(ws))
	for _, w := range ws {
		all = append(all, got{
			w.Features.SecondsSinceLastOutput, w.Features.OutputBytes.Last500ms, w.Features.OutputBytes.Last2s,
			w.Screen.Cols, len(w.Screen.Rows), w.Draft, w.Provenance.SequenceFrom, w.Provenance.SequenceTo,
		})
	}
	require.InDeltaSlice(t, []float64{0.9, 0.5, 1.5}, []float64{all[0].quiet, all[1].quiet, all[2].quiet}, 1e-9)
	require.Equal(t, []got{
		{all[0].quiet, 0, 7, 80, 24, dataset.DraftOpen, 0, 2},
		{all[1].quiet, 0, 19, 80, 24, dataset.DraftClosed, 3, 4},
		{all[2].quiet, 0, 12, 100, 30, dataset.DraftClosed, 5, 6},
	}, all)
	require.Equal(t, "hello", ws[0].Screen.Rows[0])
	require.Equal(t, "0123456789", ws[1].Screen.Rows[1])

	for i, w := range ws {
		require.True(t, w.WindowID.Valid(), "window %d id %s", i, w.WindowID)
		require.Nil(t, w.Features.NewChildProcesses, "children are not observed yet")
		require.Equal(t, protocol.HarnessCodex, w.Features.Harness)
		require.Equal(t, protocol.ModelContractAnalyzeAgentState, w.Contract)
		require.Equal(t, dataset.EnvironmentDomainBankGeneric, w.EnvironmentDomain)
		pr := w.Provenance
		require.Equal(t, protocol.OpaqueID(sid), pr.SessionID)
		require.Equal(t, t0.Add(time.Duration(i)*time.Second), pr.WindowStart)
		require.Equal(t, t0.Add(time.Duration(i+1)*time.Second), pr.WindowEnd)
		require.Equal(t, pr.WindowEnd, pr.AnchorAt)
		require.Equal(t, protocol.SourceQualityPTYScreen, pr.SourceQuality)
		require.True(t, pr.SourceDigest.Valid())
		for _, ref := range pr.SourceRefs {
			require.True(t, ref.Valid(), ref)
		}
		require.Equal(t, ws[0].GroupDigest, w.GroupDigest, "one session, one group")
	}
	require.NotEqual(t, ws[0].Provenance.SourceDigest, ws[1].Provenance.SourceDigest, "the digest follows the bytes replayed")
	require.Equal(t, ws[1].Provenance.SourceDigest, ws[2].Provenance.SourceDigest)

	again := windows(t, p.data, time.Second)
	for i := range ws {
		require.Equal(t, ws[i].WindowID, again[i].WindowID, "ids are stable across replays")
	}
	require.Len(t, windows(t, p.data, 500*time.Millisecond), 6, "the spacing is a parameter")
}

func TestDraftStartsUnknown(t *testing.T) {
	t.Parallel()

	p := build(t, []step{{ms: 1500, output: "x"}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws := windows(t, p.data, time.Second)
	require.Len(t, ws, 1)
	require.Equal(t, dataset.DraftUnknown, ws[0].Draft)
}

func TestFailsClosed(t *testing.T) {
	t.Parallel()

	steps := []step{{ms: 100, output: "one\r\n"}, {ms: 1200, output: "two\r\n"}, {ms: 2500, draft: "open"}}
	good := build(t, steps)
	events := good.events.String()
	lines := strings.SplitAfter(events, "\n")
	raw := good.raw.Bytes()
	extra := build(t, steps)
	_, err := session.NewRecorder(&extra.raw).Append(t0, 0, []byte("stray"))
	require.NoError(t, err)

	for name, tc := range map[string]struct{ raw, events []byte }{
		"no events file":     {raw: raw},
		"no recording":       {events: []byte(events)},
		"cut events":         {raw: raw, events: []byte(strings.Join(lines[:len(lines)-2], ""))},
		"disordered events":  {raw: raw, events: []byte(lines[0] + lines[2] + lines[1] + strings.Join(lines[3:], ""))},
		"tampered hash":      {raw: raw, events: []byte(strings.Replace(events, `"screen_hash_after":"sha256:`, `"screen_hash_after":"sha256:0`, 1)[:len(events)])},
		"recording cut":      {raw: raw[:len(raw)-2], events: []byte(events)},
		"recording altered":  {raw: bytes.Replace(raw, []byte("two"), []byte("TWO"), 1), events: []byte(events)},
		"recording too long": {raw: append(append([]byte{}, raw...), extra.raw.Bytes()[len(raw):]...), events: []byte(events)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &pair{data: t.TempDir()}
			p.write(t, tc.raw, tc.events)
			src, err := Open(Options{DataDir: p.data, SessionID: sid})
			require.ErrorIs(t, err, ErrReplay)
			require.Nil(t, src, "no windows at all")
		})
	}
}

func TestRefusesUnsafeFiles(t *testing.T) {
	t.Parallel()

	good := build(t, []step{{ms: 1500, output: "x"}})
	p := &pair{data: t.TempDir()}
	p.write(t, good.raw.Bytes(), nil)
	outside := filepath.Join(t.TempDir(), "events")
	require.NoError(t, os.WriteFile(outside, good.events.Bytes(), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(session.RecordingsDir(p.data), sid+session.EventsSuffix)))
	_, err := Open(Options{DataDir: p.data, SessionID: sid})
	require.ErrorIs(t, err, ErrReplay)

	_, err = Open(Options{DataDir: p.data, SessionID: "../x"})
	require.ErrorIs(t, err, ErrReplay)
	_, err = Open(Options{DataDir: p.data, SessionID: sid, Every: time.Nanosecond})
	require.ErrorIs(t, err, ErrReplay)
}
