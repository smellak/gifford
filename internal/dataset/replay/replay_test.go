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
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/vt"
)

const (
	sid   = "synth-1"
	lineX = "x\r\n"
)

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
	return buildAs(t, protocol.HarnessCodex, true, steps)
}

// buildAs records the session as harness; closed ends it with pty.closed
// at the last step.
func buildAs(t *testing.T, harness protocol.Harness, closed bool, steps []step) *pair {
	t.Helper()
	return buildOpened(t, session.PTYOpened{Cols: 80, Rows: 24, Harness: harness}, closed, steps)
}

// buildOpened is buildAs with the whole pty.opened payload given.
func buildOpened(t *testing.T, opened session.PTYOpened, closed bool, steps []step) *pair {
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
	emit(0, protocol.EventTypePTYOpened, opened)
	for _, s := range steps {
		switch {
		case s.output != "":
			_, err := emu.Write([]byte(s.output))
			require.NoError(t, err)
			n, err := rec.Append(t0.Add(time.Duration(s.ms)*time.Millisecond), s.ms*1e6, []byte(s.output))
			require.NoError(t, err)
			emit(s.ms, protocol.EventTypeIOChunk, session.IOChunkEvent{
				ByteCount: int64(len(s.output)), ScreenHashAfter: vt.Hash(emu.Snapshot()), RecordingSequence: n,
				ChunkDigest: session.ChunkDigest([]byte(s.output)),
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
	if closed {
		last := int64(0)
		if len(steps) > 0 {
			last = steps[len(steps)-1].ms
		}
		emit(last, protocol.EventTypePTYClosed, session.PTYClosed{})
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
	src, err := Open(Options{DataDir: data, SessionID: id, Every: every})
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
		{all[2].quiet, 0, 12, 100, 30, dataset.DraftClosed, 5, 7},
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

// TestBytesNotJustScreen: a same-length change the screen hash cannot
// see (it normalises digits and drops attributes) still fails closed.
func TestBytesNotJustScreen(t *testing.T) {
	t.Parallel()

	for name, swap := range map[string][2]string{
		"digit":  {"count 1\r\n", "count 7\r\n"},
		"colour": {"\x1b[31mred\x1b[0m\r\n", "\x1b[32mred\x1b[0m\r\n"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			good := build(t, []step{{ms: 100, output: swap[0]}, {ms: 1500, output: lineX}})
			// The live hash of both texts is the same: only the bytes differ.
			a, err := vt.New(80, 24)
			require.NoError(t, err)
			b, err := vt.New(80, 24)
			require.NoError(t, err)
			_, _ = a.Write([]byte(swap[0]))
			_, _ = b.Write([]byte(swap[1]))
			require.Equal(t, vt.Hash(a.Snapshot()), vt.Hash(b.Snapshot()), "the screen hash cannot tell them apart")
			_ = a.Close()
			_ = b.Close()

			p := &pair{data: t.TempDir()}
			p.write(t, bytes.Replace(good.raw.Bytes(), []byte(swap[0]), []byte(swap[1]), 1), good.events.Bytes())
			src, err := Open(Options{DataDir: p.data, SessionID: sid})
			require.ErrorIs(t, err, ErrReplay)
			require.Nil(t, src)
		})
	}
}

func TestHarnessFromTheLog(t *testing.T) {
	t.Parallel()

	for _, h := range []protocol.Harness{protocol.HarnessCodex, protocol.HarnessMuseCode, protocol.HarnessClaudeCode} {
		p := buildAs(t, h, true, []step{{ms: 1500, output: lineX}})
		p.write(t, p.raw.Bytes(), p.events.Bytes())
		ws := windows(t, p.data, time.Second)
		require.Len(t, ws, 1)
		require.Equal(t, h, ws[0].Features.Harness)
	}

	for _, h := range []protocol.Harness{"", "vim", "Codex"} {
		p := buildAs(t, h, true, []step{{ms: 1500, output: lineX}})
		p.write(t, p.raw.Bytes(), p.events.Bytes())
		_, err := Open(Options{DataDir: p.data, SessionID: sid})
		require.ErrorIs(t, err, ErrReplay, "harness %q", h)
	}
}

func TestHarnessVersionFromTheLog(t *testing.T) {
	t.Parallel()
	for logged, want := range map[string]string{
		"2.0.14 (Claude Code)": "2.0.14 (Claude Code)",
		"":                     session.UnknownHarnessVersion, // a log from before the field
	} {
		opened := session.PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessClaudeCode, HarnessVersion: logged}
		p := buildOpened(t, opened, true, []step{{ms: 1500, output: lineX}, {ms: 2600, output: lineX}})
		p.write(t, p.raw.Bytes(), p.events.Bytes())
		ws := windows(t, p.data, time.Second)
		require.Len(t, ws, 2)
		for _, w := range ws {
			require.Equal(t, want, w.Provenance.HarnessVersion, "logged %q", logged)
		}
	}
}

func TestOpenAndCloseExactlyOnce(t *testing.T) {
	t.Parallel()

	out := step{ms: 1500, output: lineX}
	for name, p := range map[string]*pair{
		"no pty.closed":        buildAs(t, protocol.HarnessCodex, false, []step{out}),
		"pty.closed early":     buildAs(t, protocol.HarnessCodex, false, []step{{ms: 100, other: protocol.EventTypePTYClosed}, out}),
		"pty.closed twice":     build(t, []step{{ms: 100, other: protocol.EventTypePTYClosed}, out}),
		"pty.opened twice":     build(t, []step{{ms: 100, other: protocol.EventTypePTYOpened}, out}),
		"pty.closed after end": buildAs(t, protocol.HarnessCodex, false, []step{out, {ms: 1600, other: protocol.EventTypePTYClosed}, {ms: 1700, other: protocol.EventTypeInputKeyChunk}}),
	} {
		p.write(t, p.raw.Bytes(), p.events.Bytes())
		src, err := Open(Options{DataDir: p.data, SessionID: sid})
		require.ErrorIs(t, err, ErrReplay, name)
		require.Nil(t, src, name)
	}
}

func TestCursorFromTheScreen(t *testing.T) {
	t.Parallel()
	p := build(t, []step{{ms: 100, output: "a\r\nb\r\n> typed "}, {ms: 1500, output: "x"}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws := windows(t, p.data, time.Second)
	require.NotEmpty(t, ws)
	require.Equal(t, screenproj.Cursor{Row: 2, Known: true, Visible: true}, ws[0].Cursor, "the row the prompt is on")

	// A TUI that hides the terminal cursor: known, but not visible.
	p = build(t, []step{{ms: 100, output: "\x1b[?25la\r\n> typed "}, {ms: 1500, output: "x"}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws = windows(t, p.data, time.Second)
	require.NotEmpty(t, ws)
	require.Equal(t, screenproj.Cursor{Row: 1, Known: true, Visible: false}, ws[0].Cursor)
}

func TestOpenAllSkipsBrokenSessions(t *testing.T) {
	t.Parallel()
	p := build(t, []step{{ms: 1500, output: lineX}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	// A second session whose recording is cut.
	dir := session.RecordingsDir(p.data)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "synth-2"+session.EventsSuffix), p.events.Bytes(), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "synth-2"+session.RecordingSuffix), nil, 0o600))

	var skipped []string
	src, err := OpenAll(p.data, time.Second, func(id string, err error) {
		require.ErrorIs(t, err, ErrReplay)
		skipped = append(skipped, id)
	})
	require.NoError(t, err)
	var got []dataset.Recording
	for {
		r, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Equal(t, windows(t, p.data, time.Second), got)
	require.Equal(t, []string{"synth-2"}, skipped)

	empty, err := OpenAll(t.TempDir(), 0, nil)
	require.NoError(t, err)
	_, err = empty.Next(context.Background())
	require.ErrorIs(t, err, io.EOF, "no recordings, no windows")
}
