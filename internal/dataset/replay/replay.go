// Package replay reads one `gifford run` session back as dataset windows:
// it replays the raw recording through internal/vt in the order its
// events file gives, starting from the pty.opened size and applying every
// input.resize, and cuts a window at each wall-clock interval.
//
// It fails closed: an events file that is missing, cut, out of order,
// short of events or without exactly one pty.opened first and one
// pty.closed last, a recording that does not match it byte for byte (each
// piece's SHA-256), or a screen whose hash differs from the one recorded
// live, yields no window at all. The harness and its version come from
// pty.opened. Windows run from pty.opened to the last event, pty.closed
// on a finished run. Windows carry what can be computed from the two files:
// the screen, the quiet time and output bytes, the draft state from the
// input.* events only, and the provenance. Child processes are not
// observed yet, so they are null (unknown); the previous state is not
// known either, so it is empty.
//
// A window before the session's first output has no last output to time
// it from: it still comes, numbered and anchored as every window is, but
// marked NoOutputYet, so a build leaves it out. An io.chunk of no bytes
// is checked like any other, its sequence, digest and screen hash, but
// is no output: it neither ends the quiet time nor counts as bytes.
//
// A session imported by `gifford capture import` has its provenance in
// recordings/<id>.import.json: its windows come from bootstrap_import, in
// the provenance's environment domain, and name that file among their
// sources. A provenance that is there but not valid, or that says another
// harness or other counts than the pair, yields no window either, and so
// does an import's session without its provenance: only a run's session
// reads without one.
package replay

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/vt"
)

// ErrReplay means the session cannot be replayed whole; it yields no
// window.
var ErrReplay = errors.New("replay: session does not replay")

const (
	// DefaultEvery is the window spacing until S1 fixes it.
	DefaultEvery = time.Second
	// maxWindows bounds the windows of one session.
	maxWindows = 1 << 20
	// windowIDDomain seeds the window ids, so the same session always
	// gives the same ids and labels keyed by them stay attached.
	windowIDDomain = "gifford.replay.window.v1"
	groupDomain    = "gifford.replay.group.session.v1"
)

// Options pick the session and how to cut it.
type Options struct {
	// DataDir holds recordings/.
	DataDir string
	// SessionID names the pair recordings/<id>.raw and <id>.events.jsonl.
	SessionID string
	// Every spaces the windows on the wall clock from pty.opened; zero
	// is DefaultEvery. S1 (10.14) fixes the value.
	Every time.Duration
}

// Source yields a replayed session's windows in time order.
type Source struct {
	recs []dataset.Recording
}

// Next returns the next window, io.EOF after the last.
func (s *Source) Next(ctx context.Context) (dataset.Recording, error) {
	if err := ctx.Err(); err != nil {
		return dataset.Recording{}, fmt.Errorf("replay: %w", err)
	}
	if len(s.recs) == 0 {
		return dataset.Recording{}, io.EOF
	}
	r := s.recs[0]
	s.recs = s.recs[1:]
	return r, nil
}

// Open replays the whole session before it yields anything, so a session
// that fails anywhere yields no window.
func Open(opts Options) (*Source, error) {
	if opts.Every == 0 {
		opts.Every = DefaultEvery
	}
	if opts.Every < 10*time.Millisecond || opts.Every > time.Hour {
		return nil, fmt.Errorf("%w: window spacing out of range", ErrReplay)
	}
	raw, events, err := session.OpenPair(opts.DataDir, opts.SessionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReplay, err)
	}
	defer func() { _ = raw.Close(); _ = events.Close() }()
	var imp *capture.Provenance
	p, imported, err := capture.ReadImport(opts.DataDir, opts.SessionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReplay, err)
	}
	if imported {
		imp = &p
	}
	evs, err := session.ReadEventLog(events, opts.SessionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReplay, err)
	}
	recs, err := replay(opts, evs, bufio.NewReader(raw), imp)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReplay, err)
	}
	return &Source{recs: recs}, nil
}

// output is one recorded piece by the time its event was emitted.
type output struct {
	mono  int64
	bytes int64
}

type state struct {
	opts    Options
	imp     *capture.Provenance
	harness protocol.Harness
	version string
	emu     *vt.Emulator
	raw     *bufio.Reader
	nextRec int64
	src     hash.Hash
	outputs []output
	draft   dataset.DraftState
}

func replay(opts Options, evs []session.LoggedEvent, raw *bufio.Reader, imp *capture.Provenance) ([]dataset.Recording, error) {
	var opened session.PTYOpened
	if err := decode(evs[0].Payload, &opened); err != nil {
		return nil, errors.New("bad pty.opened")
	}
	if imp != nil {
		if err := matchImport(*imp, opened, evs); err != nil {
			return nil, err
		}
	}
	// A log from before the field reads back as unknown.
	version := opened.Version()
	if !session.ValidHarnessVersion(version) {
		return nil, errors.New("bad pty.opened harness version")
	}
	emu, err := vt.New(opened.Cols, opened.Rows)
	if err != nil {
		return nil, fmt.Errorf("pty.opened size: %w", err)
	}
	defer func() { _ = emu.Close() }()
	// The harness and its version are the ones the run recorded: no
	// caller can relabel them.
	st := &state{opts: opts, imp: imp, harness: opened.Harness, version: version, emu: emu, raw: raw, src: sha256.New(), draft: dataset.DraftUnknown}

	base, baseWall := evs[0].MonotonicNS, evs[0].WallTime
	end := evs[len(evs)-1].MonotonicNS
	every := opts.Every.Nanoseconds()
	wall := func(mono int64) time.Time { return baseWall.Add(time.Duration(mono - base)) }

	var recs []dataset.Recording
	next := 0 // the next event to apply
	for k := int64(1); ; k++ {
		anchor := base + k*every
		from := next
		for next < len(evs) && evs[next].MonotonicNS <= anchor {
			if err := st.apply(evs[next]); err != nil {
				return nil, fmt.Errorf("event %d: %w", evs[next].Sequence, err)
			}
			next++
		}
		if anchor > end {
			break
		}
		if len(recs) == maxWindows {
			return nil, errors.New("too many windows")
		}
		seqFrom, seqTo := evs[max(next-1, 0)].Sequence, evs[max(next-1, 0)].Sequence
		if next > from {
			seqFrom = evs[from].Sequence
		}
		rec, err := st.window(k, wall(anchor-every), wall(anchor), anchor, seqFrom, seqTo, base)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	// Every recorded byte was named by an event, and none is left over.
	if _, err := session.ReadChunk(raw); !errors.Is(err, io.EOF) {
		return nil, errors.New("recording has bytes no event names")
	}
	return recs, nil
}

// matchImport checks an imported session's provenance against its pair:
// the same harness, and as many outputs and resizes as the import
// counted.
func matchImport(imp capture.Provenance, opened session.PTYOpened, evs []session.LoggedEvent) error {
	if imp.Harness != opened.Harness {
		return errors.New("import provenance names another harness")
	}
	var outputs, resizes int64
	for _, ev := range evs {
		switch ev.Type {
		case protocol.EventTypeIOChunk:
			outputs++
		case protocol.EventTypeInputResize:
			resizes++
		default:
		}
	}
	if outputs != imp.Counts.Output || resizes != imp.Counts.Resize {
		return errors.New("import provenance counts other events")
	}
	return nil
}

// apply replays one event onto the screen and the window state.
func (st *state) apply(ev session.LoggedEvent) error {
	switch ev.Type {
	case protocol.EventTypeIOChunk:
		var p session.IOChunkEvent
		if err := decode(ev.Payload, &p); err != nil {
			return errors.New("bad io.chunk")
		}
		c, err := session.ReadChunk(st.raw)
		if err != nil {
			return errors.New("recording ends before its events")
		}
		if c.Sequence != st.nextRec || p.RecordingSequence != st.nextRec || int64(len(c.Data)) != p.ByteCount {
			return errors.New("recording and events disagree")
		}
		// Byte for byte, before the bytes reach the screen: the screen
		// hash alone cannot tell digits or spinners apart.
		if session.ChunkDigest(c.Data) != p.ChunkDigest {
			return errors.New("recorded bytes differ from the live ones")
		}
		st.nextRec++
		if _, err := st.emu.Write(c.Data); err != nil {
			return fmt.Errorf("screen: %w", err)
		}
		if vt.Hash(st.emu.Snapshot()) != p.ScreenHashAfter {
			return errors.New("replayed screen differs from the live one")
		}
		_, _ = st.src.Write(c.Data)
		if p.ByteCount > 0 {
			st.outputs = append(st.outputs, output{mono: ev.MonotonicNS, bytes: p.ByteCount})
		}
	case protocol.EventTypeInputResize:
		var p session.InputResize
		if err := decode(ev.Payload, &p); err != nil {
			return errors.New("bad input.resize")
		}
		if err := st.emu.Resize(p.Cols, p.Rows); err != nil {
			return fmt.Errorf("resize: %w", err)
		}
	case protocol.EventTypeInputDraftOpened, protocol.EventTypeInputDraftUpdated,
		protocol.EventTypeInputDraftSubmitted, protocol.EventTypeInputDraftCancelled,
		protocol.EventTypeInputDraftCleared, protocol.EventTypeInputDraftUnknown:
		var p session.InputDraft
		if err := decode(ev.Payload, &p); err != nil {
			return errors.New("bad input.draft event")
		}
		st.trackDraft(p.State)
	default:
	}
	return nil
}

// trackDraft maps the guard state an input.draft_* event landed on.
// Only closed is closed; submitting, pasting and anything unknown count
// as open or unknown, so the prompt line is removed.
func (st *state) trackDraft(guard string) {
	switch guard {
	case "closed":
		st.draft = dataset.DraftClosed
	case "open", "pasting", "submitting":
		st.draft = dataset.DraftOpen
	default:
		st.draft = dataset.DraftUnknown
	}
}

func (st *state) window(k int64, start, anchorWall time.Time, anchor, seqFrom, seqTo, base int64) (dataset.Recording, error) {
	snap := st.emu.Snapshot()
	last := base
	if n := len(st.outputs); n > 0 {
		last = st.outputs[n-1].mono
	}
	srcDigest, err := digestSoFar(st.src)
	if err != nil {
		return dataset.Recording{}, err
	}
	id := protocol.OpaqueID(st.opts.SessionID)
	quality, domain := protocol.SourceQualityPTYScreen, dataset.EnvironmentDomainBankGeneric
	refs := []protocol.BlobRef{
		protocol.BlobRef("blob://recordings/" + st.opts.SessionID + session.RecordingSuffix),
		protocol.BlobRef("blob://recordings/" + st.opts.SessionID + session.EventsSuffix),
	}
	if st.imp != nil {
		quality, domain = protocol.SourceQualityBootstrapImport, st.imp.EnvironmentDomain
		refs = append(refs, protocol.BlobRef("blob://recordings/"+st.opts.SessionID+capture.ImportSuffix))
	}
	return dataset.Recording{
		WindowID: windowID(anchorWall, st.opts.SessionID, k),
		Contract: protocol.ModelContractAnalyzeAgentState,
		Features: dataset.AnalyzeInput{
			Harness:                st.harness,
			SecondsSinceLastOutput: float64(anchor-last) / float64(time.Second),
			OutputBytes: dataset.OutputBytes{
				Last500ms: st.bytesSince(anchor - (500 * time.Millisecond).Nanoseconds()),
				Last2s:    st.bytesSince(anchor - (2 * time.Second).Nanoseconds()),
				Last10s:   st.bytesSince(anchor - (10 * time.Second).Nanoseconds()),
			},
		},
		Screen: redact.Screen{Rows: append([]string(nil), snap.Lines...), Cols: snap.Cols},
		Draft:  st.draft,
		// The emulator always knows where the cursor is, and whether the
		// pane showed it.
		Cursor: screenproj.Cursor{
			Row: snap.CursorRow, Known: snap.CursorRow >= 0 && snap.CursorRow < len(snap.Lines), Visible: snap.CursorVisible,
		},
		NoOutputYet: len(st.outputs) == 0,
		Provenance: dataset.Provenance{
			SessionID:        id,
			CaptureSessionID: id,
			SourceQuality:    quality,
			HarnessVersion:   st.version,
			SequenceFrom:     seqFrom,
			SequenceTo:       seqTo,
			WindowStart:      start,
			WindowEnd:        anchorWall,
			AnchorAt:         anchorWall,
			SourceRefs:       refs,
			SourceDigest:     srcDigest,
		},
		EnvironmentDomain: domain,
		GroupDigest:       GroupDigest(st.opts.SessionID),
	}, nil
}

// bytesSince sums the output recorded after since, up to now.
func (st *state) bytesSince(since int64) int64 {
	var n int64
	for i := len(st.outputs) - 1; i >= 0 && st.outputs[i].mono > since; i-- {
		n += st.outputs[i].bytes
	}
	return n
}

// digestSoFar is the SHA-256 of the recorded bytes replayed so far, the
// ones the screen was built from, without ending the running hash.
func digestSoFar(h hash.Hash) (protocol.Digest, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return "", errors.New("hash state not copyable")
	}
	state, err := m.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("hash state: %w", err)
	}
	c := sha256.New()
	if err := c.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return "", fmt.Errorf("hash state: %w", err)
	}
	return protocol.Digest("sha256:" + hex.EncodeToString(c.Sum(nil))), nil
}

// GroupDigest is the group of every window of the session: one
// session's windows always share one split.
func GroupDigest(sessionID string) protocol.Digest {
	return digestOf(groupDomain, sessionID)
}

func digestOf(domain, s string) protocol.Digest {
	sum := sha256.Sum256([]byte(domain + "\x00" + s))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// windowID is a ULID: the anchor's milliseconds, then 80 bits from the
// session and the window number, so a replay always gives the same id.
func windowID(anchor time.Time, sessionID string, k int64) protocol.ULID {
	var b [16]byte
	ms := uint64(max(anchor.UnixMilli(), 0)) & (1<<48 - 1)
	for i := range 6 {
		b[i] = byte(ms >> (8 * (5 - i)) & 0xff)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%d", windowIDDomain, sessionID, k))
	copy(b[6:], sum[:10])
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	// 128 bits as 26 base-32 digits, the first carrying 3 bits.
	var out [26]byte
	var acc uint64
	bits := 0
	j := 25
	for i := 15; i >= 0; i-- {
		acc |= uint64(b[i]) << bits
		bits += 8
		for bits >= 5 {
			out[j] = alphabet[acc&31]
			acc >>= 5
			bits -= 5
			j--
		}
	}
	out[0] = alphabet[acc&31]
	return protocol.ULID(out[:])
}

func decode(raw json.RawMessage, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	return nil
}
