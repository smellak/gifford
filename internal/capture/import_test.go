//go:build unix

package capture

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// unreadable is how a refusal names a provenance that does not read.
const unreadable = "does not read"

// importFixture imports the fixture cast into a new data dir.
func importFixture(t *testing.T) (dataDir, source string, res Result) {
	t.Helper()
	dataDir = filepath.Join(t.TempDir(), "data")
	source = fixtureCast(t)
	res, err := Import(context.Background(), fixtureOptions(dataDir, source))
	require.NoError(t, err)
	return dataDir, source, res
}

// stamps maps each file under dir to its modification time.
func stamps(t *testing.T, dir string) map[string]time.Time {
	t.Helper()
	out := map[string]time.Time{}
	for name := range tree(t, dir) {
		info, err := os.Lstat(filepath.Join(dir, name))
		require.NoError(t, err)
		out[name] = info.ModTime()
	}
	return out
}

func TestImportWritesTheRunPair(t *testing.T) {
	t.Parallel()
	dataDir, source, res := importFixture(t)
	body, err := os.ReadFile(source)
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	hexSum := hex.EncodeToString(sum[:])
	id := "asciicast2-" + hexSum[:32]
	require.Equal(t, Result{SessionID: id, Counts: Counts{Output: 3, Resize: 1, InputDropped: 2, MarkerDropped: 1}}, res)

	// The three files, 0600 in a 0700 dir: no temporary file is left.
	dir := session.RecordingsDir(dataDir)
	require.ElementsMatch(t, []string{id + session.RecordingSuffix, id + session.EventsSuffix, id + ImportSuffix}, recordings(t, dataDir))
	info, err := os.Lstat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	for _, name := range recordings(t, dataDir) {
		info, err := os.Lstat(filepath.Join(dir, name))
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular(), name)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), name)
	}

	raw, events, err := session.OpenPair(dataDir, id)
	require.NoError(t, err)
	defer func() { _ = raw.Close(); _ = events.Close() }()
	evs, err := session.ReadEventLog(events, id)
	require.NoError(t, err, "a whole events file, as a run leaves it")
	start := time.Unix(fixtureStart, 0).UTC()
	ms := func(n int64) int64 { return n * int64(time.Millisecond) }
	want := []struct {
		typ protocol.EventType
		at  int64
	}{
		{protocol.EventTypePTYOpened, 0},
		{protocol.EventTypeIOChunk, ms(250)},
		{protocol.EventTypeIOChunk, ms(750)},
		{protocol.EventTypeInputResize, ms(1500)},
		{protocol.EventTypeIOChunk, ms(2500)},
		// At the last event of the cast, typed input that is not
		// written.
		{protocol.EventTypePTYClosed, ms(3250)},
	}
	require.Len(t, evs, len(want))
	for i, w := range want {
		require.Equal(t, w.typ, evs[i].Type, "event %d", i)
		require.Equal(t, w.at, evs[i].MonotonicNS, "event %d", i)
		require.Equal(t, start.Add(time.Duration(w.at)), evs[i].WallTime, "event %d", i)
	}
	var opened session.PTYOpened
	require.NoError(t, json.Unmarshal(evs[0].Payload, &opened))
	require.Equal(t, session.PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex}, opened, "no harness version: it reads as unknown")
	var resize session.InputResize
	require.NoError(t, json.Unmarshal(evs[3].Payload, &resize))
	require.Equal(t, session.InputResize{Cols: 100, Rows: 30}, resize, "the guard fields stay zero")

	// Each output is one recorded chunk, as its io.chunk says.
	outputs := []string{"fixture build ok\r\n", "$ ", "\x1b[1mdone\x1b[0m\r\n$ "}
	br := bufio.NewReader(raw)
	for i, ev := range []session.LoggedEvent{evs[1], evs[2], evs[4]} {
		c, err := session.ReadChunk(br)
		require.NoError(t, err)
		require.Equal(t, outputs[i], string(c.Data))
		require.Equal(t, int64(i), c.Sequence)
		require.Equal(t, ev.MonotonicNS, c.MonotonicNS)
		require.Equal(t, ev.WallTime, c.WallTime)
		var p session.IOChunkEvent
		require.NoError(t, json.Unmarshal(ev.Payload, &p))
		require.Equal(t, int64(len(outputs[i])), p.ByteCount)
		require.Equal(t, int64(i), p.RecordingSequence)
		require.Equal(t, session.ChunkDigest([]byte(outputs[i])), p.ChunkDigest)
		require.Equal(t, strings.Contains(outputs[i], "\x1b"), p.ANSIPresent)
		require.True(t, p.ScreenHashAfter.Valid())
	}
	_, err = session.ReadChunk(br)
	require.ErrorIs(t, err, io.EOF, "nothing past the outputs")
}

func TestImportRecordsItsProvenance(t *testing.T) {
	t.Parallel()
	dataDir, source, res := importFixture(t)
	body, err := os.ReadFile(source)
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	p, ok, err := ReadImport(dataDir, res.SessionID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Provenance{
		SchemaVersion: ImportSchemaVersion,
		Source: Source{
			Format: FormatAsciicastV2, SHA256: protocol.Digest("sha256:" + hex.EncodeToString(sum[:])), SizeBytes: int64(len(body)),
		},
		AgentID: fixtureAgent, Harness: protocol.HarnessCodex, EnvironmentDomain: dataset.EnvironmentDomainFleetManaged,
		Counts: Counts{Output: 3, Resize: 1, InputDropped: 2, MarkerDropped: 1},
	}, p)
}

func TestImportNeverWritesWhatWasTyped(t *testing.T) {
	t.Parallel()
	dataDir, _, _ := importFixture(t)
	for name, body := range tree(t, dataDir) {
		// What was typed, the markers, the header's title and env, and
		// the source's path and name.
		for _, never := range []string{typedCanary, markerCanary, titleCanary, envCanary, pathCanary, "fixture.cast", "/bin/sh"} {
			require.NotContains(t, body, never, name)
		}
	}
}

func TestImportIsTheSameBytesEveryTime(t *testing.T) {
	t.Parallel()
	source := fixtureCast(t)
	trees := make([]map[string]string, 0, 2)
	for range 2 {
		dataDir := filepath.Join(t.TempDir(), "data")
		_, err := Import(context.Background(), fixtureOptions(dataDir, source))
		require.NoError(t, err)
		trees = append(trees, tree(t, dataDir))
	}
	require.Equal(t, trees[0], trees[1], "no import time and no path: the same source gives the same bytes")
}

func TestImportTwiceWritesNothing(t *testing.T) {
	t.Parallel()
	dataDir, source, first := importFixture(t)
	before, times := tree(t, dataDir), stamps(t, dataDir)
	res, err := Import(context.Background(), fixtureOptions(dataDir, source))
	require.NoError(t, err)
	require.Equal(t, Result{SessionID: first.SessionID, Already: true}, res)
	require.Equal(t, before, tree(t, dataDir))
	require.Equal(t, times, stamps(t, dataDir))
}

func TestImportRefusesAnotherProvenance(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		change func(*Options)
		fields string
	}{
		"agent":   {func(o *Options) { o.AgentID = "agent-2" }, "agent_id differs"},
		"harness": {func(o *Options) { o.Harness = protocol.HarnessGeneric }, "harness differs"},
		"domain":  {func(o *Options) { o.EnvironmentDomain = dataset.EnvironmentDomainBankGeneric }, "environment_domain differs"},
		"agent, domain": {func(o *Options) {
			o.AgentID, o.EnvironmentDomain = "agent-2", dataset.EnvironmentDomainBankGeneric
		}, "agent_id, environment_domain differ"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir, source, _ := importFixture(t)
			before, times := tree(t, dataDir), stamps(t, dataDir)
			opts := fixtureOptions(dataDir, source)
			c.change(&opts)
			_, err := Import(context.Background(), opts)
			require.ErrorIs(t, err, ErrConflict)
			require.Contains(t, err.Error(), c.fields)
			require.Equal(t, before, tree(t, dataDir))
			require.Equal(t, times, stamps(t, dataDir))
		})
	}
}

// rewriteProvenance rewrites session id's provenance as edit leaves it,
// in the form an import writes.
func rewriteProvenance(t *testing.T, dataDir, id string, edit func(*Provenance)) {
	t.Helper()
	p, ok, err := ReadImport(dataDir, id)
	require.NoError(t, err)
	require.True(t, ok)
	edit(&p)
	b, err := p.encode()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(session.RecordingsDir(dataDir), id+ImportSuffix), b, 0o600))
}

func TestImportRefusesAnotherSourceDigest(t *testing.T) {
	t.Parallel()
	dataDir, source, res := importFixture(t)
	// Another digest with the same first 32 hex digits: the same id.
	rewriteProvenance(t, dataDir, res.SessionID, func(p *Provenance) {
		last := p.Source.SHA256[len(p.Source.SHA256)-1]
		p.Source.SHA256 = p.Source.SHA256[:len(p.Source.SHA256)-1] + map[bool]protocol.Digest{true: "1", false: "0"}[last == '0']
	})
	before := tree(t, dataDir)
	_, err := Import(context.Background(), fixtureOptions(dataDir, source))
	require.ErrorIs(t, err, ErrConflict)
	require.Contains(t, err.Error(), "source.sha256 differs")
	require.Equal(t, before, tree(t, dataDir))
}

func TestImportRefusesAPairWithoutItsProvenance(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		spoil func(t *testing.T, path string)
		why   string
	}{
		"missing": {func(t *testing.T, path string) { require.NoError(t, os.Remove(path)) }, "without its provenance"},
		"edited by hand": {func(t *testing.T, path string) {
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, append([]byte(" "), b...), 0o600))
		}, unreadable},
		"another version": {func(t *testing.T, path string) {
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(b), `"schema_version":1`, `"schema_version":2`, 1)), 0o600))
		}, unreadable},
		"open to others": {func(t *testing.T, path string) { require.NoError(t, os.Chmod(path, 0o644)) }, unreadable},
		"a link": {func(t *testing.T, path string) {
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			other := filepath.Join(t.TempDir(), "elsewhere.json")
			require.NoError(t, os.WriteFile(other, b, 0o600))
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.Symlink(other, path))
		}, unreadable},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir, source, res := importFixture(t)
			c.spoil(t, filepath.Join(session.RecordingsDir(dataDir), res.SessionID+ImportSuffix))
			before, times := tree(t, dataDir), stamps(t, dataDir)
			_, err := Import(context.Background(), fixtureOptions(dataDir, source))
			require.ErrorIs(t, err, ErrPartial)
			require.Contains(t, err.Error(), c.why)
			require.Equal(t, before, tree(t, dataDir))
			require.Equal(t, times, stamps(t, dataDir))
		})
	}
}

func TestImportRefusesPartOfASession(t *testing.T) {
	t.Parallel()
	all := []string{session.RecordingSuffix, session.EventsSuffix, ImportSuffix}
	for _, keep := range [][]string{
		{session.RecordingSuffix},
		{session.EventsSuffix},
		{ImportSuffix},
		{session.RecordingSuffix, ImportSuffix},
		{session.EventsSuffix, ImportSuffix},
	} {
		t.Run(strings.Join(keep, "+"), func(t *testing.T) {
			t.Parallel()
			dataDir, source, res := importFixture(t)
			for _, suffix := range all {
				if !strings.Contains(strings.Join(keep, " "), suffix) {
					require.NoError(t, os.Remove(filepath.Join(session.RecordingsDir(dataDir), res.SessionID+suffix)))
				}
			}
			before, times := tree(t, dataDir), stamps(t, dataDir)
			_, err := Import(context.Background(), fixtureOptions(dataDir, source))
			require.ErrorIs(t, err, ErrPartial)
			require.Contains(t, err.Error(), "in part")
			require.Equal(t, before, tree(t, dataDir))
			require.Equal(t, times, stamps(t, dataDir))
		})
	}
}

func TestImportRefusalsLeaveNothing(t *testing.T) {
	t.Parallel()
	good := fixtureEvents()[:3]
	for name, events := range map[string][]string{
		"time goes back":        append(good, `[0.1, "o", "late"]`),
		"a resize malformed":    append(good, `[1, "r", "100by30"]`),
		"a resize out of range": append(good, `[1, "r", "10001x1"]`),
		"a line not JSON":       append(good, `[1, "o", "cut`),
		"an unknown code":       append(good, `[1, "q", "`+typedCanary+`"]`),
		"input not text":        append(good, `[1, "i", 42]`),
		"a line too long":       append(good, fmt.Sprintf(`[1, "o", "%s"]`, strings.Repeat("a", maxCastLineBytes))),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), "data")
			source := writeCast(t, t.TempDir(), fixtureHeader(), events...)
			_, err := Import(context.Background(), fixtureOptions(dataDir, source))
			require.ErrorIs(t, err, ErrBadCast)
			require.NotContains(t, err.Error(), typedCanary)
			require.Empty(t, recordings(t, dataDir), "no file is left, temporary or not")
		})
	}
	for name, header := range map[string]string{
		"no timestamp": `{"version": 2, "width": 80, "height": 24, "title": "` + titleCanary + `"}`,
		"version 1":    `{"version": 1, "width": 80, "height": 24, "timestamp": 1}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), "data")
			source := writeCast(t, t.TempDir(), header, good...)
			_, err := Import(context.Background(), fixtureOptions(dataDir, source))
			require.ErrorIs(t, err, ErrBadCast)
			require.NotContains(t, err.Error(), titleCanary)
			require.Empty(t, recordings(t, dataDir))
		})
	}
	t.Run("an empty file", func(t *testing.T) {
		t.Parallel()
		dataDir := filepath.Join(t.TempDir(), "data")
		source := filepath.Join(t.TempDir(), "empty.cast")
		require.NoError(t, os.WriteFile(source, nil, 0o600))
		_, err := Import(context.Background(), fixtureOptions(dataDir, source))
		require.ErrorIs(t, err, ErrBadCast)
		require.NoDirExists(t, dataDir, "refused before the data dir is touched")
	})
	t.Run("a file over the bound", func(t *testing.T) {
		t.Parallel()
		dataDir := filepath.Join(t.TempDir(), "data")
		source := fixtureCast(t)
		info, err := os.Stat(source)
		require.NoError(t, err)
		tune := defaultTuning
		tune.maxBytes = info.Size() - 1
		_, err = run(context.Background(), fixtureOptions(dataDir, source), tune)
		require.ErrorIs(t, err, ErrBadCast)
		require.Contains(t, err.Error(), "larger than")
		require.NoDirExists(t, dataDir, "refused before the data dir is touched")
		// At the bound it imports.
		tune.maxBytes = info.Size()
		_, err = run(context.Background(), fixtureOptions(dataDir, source), tune)
		require.NoError(t, err)
	})
}

func TestImportCapsTheEvents(t *testing.T) {
	t.Parallel()
	// The fixture is six events: pty.opened, three outputs, a resize and
	// pty.closed.
	for maxEvents, ok := range map[int64]bool{5: false, 6: true} {
		t.Run(fmt.Sprint(maxEvents), func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), "data")
			tune := defaultTuning
			tune.maxEvents = maxEvents
			_, err := run(context.Background(), fixtureOptions(dataDir, fixtureCast(t)), tune)
			if ok {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrBadCast)
			require.Contains(t, err.Error(), "more output and resizes than one session holds")
			require.Empty(t, recordings(t, dataDir))
		})
	}
}

func TestImportPacesTheEventLog(t *testing.T) {
	t.Parallel()
	const outputs = 2000
	events := make([]string, 0, outputs)
	for i := range outputs {
		events = append(events, fmt.Sprintf(`[%d.%03d, "o", "line %d\r\n"]`, i/1000, i%1000, i))
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	source := writeCast(t, t.TempDir(), fixtureHeader(), events...)
	// A queue of 8: without the pacing, the log would drop most events.
	tune := defaultTuning
	tune.buffer, tune.flushEvery = 8, time.Millisecond
	res, err := run(context.Background(), fixtureOptions(dataDir, source), tune)
	require.NoError(t, err)
	require.Equal(t, int64(outputs), res.Counts.Output)
	_, events2, err := session.OpenPair(dataDir, res.SessionID)
	require.NoError(t, err)
	defer func() { _ = events2.Close() }()
	evs, err := session.ReadEventLog(events2, res.SessionID)
	require.NoError(t, err)
	require.Len(t, evs, outputs+2)
}

func TestImportNotImplemented(t *testing.T) {
	t.Parallel()
	for name, opts := range map[string]func(dataDir string) Options{
		"a directory": func(dataDir string) Options { return fixtureOptions(dataDir, t.TempDir()) },
		"day0": func(dataDir string) Options {
			o := fixtureOptions(dataDir, t.TempDir())
			o.Format = FormatDay0
			return o
		},
		"asciicast v3": func(dataDir string) Options {
			o := fixtureOptions(dataDir, filepath.Join(t.TempDir(), "missing.cast"))
			o.Format = FormatAsciicastV3
			return o
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), "data")
			_, err := Import(context.Background(), opts(dataDir))
			require.ErrorIs(t, err, ErrNotImplemented)
			require.NoDirExists(t, dataDir)
		})
	}
}

func TestImportRefusesBadOptions(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Options){
		"no format":      func(o *Options) { o.Format = "" },
		"unknown format": func(o *Options) { o.Format = "asciicast-v9" },
		"no agent":       func(o *Options) { o.AgentID = "" },
		"bad agent":      func(o *Options) { o.AgentID = "Agent One" },
		"bad harness":    func(o *Options) { o.Harness = "vim" },
		"no domain":      func(o *Options) { o.EnvironmentDomain = "" },
		"bad domain":     func(o *Options) { o.EnvironmentDomain = "home" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), "data")
			opts := fixtureOptions(dataDir, fixtureCast(t))
			change(&opts)
			_, err := Import(context.Background(), opts)
			require.ErrorIs(t, err, ErrBadOptions)
			require.NoDirExists(t, dataDir)
		})
	}
}

func TestImportRefusesAnUnsafeRecordingsDir(t *testing.T) {
	t.Parallel()
	t.Run("a link", func(t *testing.T) {
		t.Parallel()
		dataDir := filepath.Join(t.TempDir(), "data")
		target := filepath.Join(t.TempDir(), "elsewhere")
		require.NoError(t, os.MkdirAll(dataDir, 0o700))
		require.NoError(t, os.Mkdir(target, 0o700))
		require.NoError(t, os.Symlink(target, session.RecordingsDir(dataDir)))
		_, err := Import(context.Background(), fixtureOptions(dataDir, fixtureCast(t)))
		require.Error(t, err)
		ents, err := os.ReadDir(target)
		require.NoError(t, err)
		require.Empty(t, ents)
	})
	t.Run("open to others", func(t *testing.T) {
		t.Parallel()
		dataDir := filepath.Join(t.TempDir(), "data")
		require.NoError(t, os.MkdirAll(session.RecordingsDir(dataDir), 0o700))
		require.NoError(t, os.Chmod(session.RecordingsDir(dataDir), 0o755))
		_, err := Import(context.Background(), fixtureOptions(dataDir, fixtureCast(t)))
		require.ErrorContains(t, err, "want 0700")
		require.Empty(t, recordings(t, dataDir))
	})
}

func TestImportStopsWhenCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dataDir := filepath.Join(t.TempDir(), "data")
	_, err := Import(ctx, fixtureOptions(dataDir, fixtureCast(t)))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, recordings(t, dataDir))
}

// sourceDigest is the digest of a source, and its size.
func sourceDigest(t *testing.T, source string) (protocol.Digest, int64) {
	t.Helper()
	body, err := os.ReadFile(source)
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:])), int64(len(body))
}

// preparedWriters converts the fixture cast into n writers for the same
// id, each with its own agent, ready to publish.
func preparedWriters(t *testing.T, dataDir string, n int) (string, []*pairWriter, []Provenance) {
	t.Helper()
	source := fixtureCast(t)
	digest, size := sourceDigest(t, source)
	id := ImportSessionID(digest)
	dir := session.RecordingsDir(dataDir)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	ws := make([]*pairWriter, n)
	provs := make([]Provenance, n)
	for i := range n {
		w, err := newPairWriter(dir, id, defaultTuning)
		require.NoError(t, err)
		t.Cleanup(w.discard)
		f, err := os.Open(source)
		require.NoError(t, err)
		counts, err := convert(context.Background(), f, w, protocol.HarnessCodex)
		require.NoError(t, f.Close())
		require.NoError(t, err)
		ws[i] = w
		provs[i] = Provenance{
			SchemaVersion: ImportSchemaVersion,
			Source:        Source{Format: FormatAsciicastV2, SHA256: digest, SizeBytes: size},
			AgentID:       protocol.AgentID(fmt.Sprintf("agent-%d", i)), Harness: protocol.HarnessCodex,
			EnvironmentDomain: dataset.EnvironmentDomainFleetManaged, Counts: counts,
		}
	}
	return id, ws, provs
}

func TestRacingPublishersLeaveOneWhole(t *testing.T) {
	t.Parallel()
	const writers = 24
	dataDir := filepath.Join(t.TempDir(), "data")
	id, ws, provs := preparedWriters(t, dataDir, writers)
	start := make(chan struct{})
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i, w := range ws {
		wg.Go(func() {
			<-start
			errs[i] = w.finish(provs[i])
		})
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			require.Equal(t, -1, winner, "a second publisher: %d and %d", winner, i)
			winner = i
			continue
		}
		require.ErrorIs(t, err, errLostRace, "publisher %d", i)
	}
	require.NotEqual(t, -1, winner, "no publisher")
	for _, w := range ws {
		w.discard()
	}
	// The three files, all the winner's, and no temporary file left.
	require.ElementsMatch(t, []string{id + session.RecordingSuffix, id + session.EventsSuffix, id + ImportSuffix}, recordings(t, dataDir))
	p, ok, err := ReadImport(dataDir, id)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, provs[winner], p)
	_, events, err := session.OpenPair(dataDir, id)
	require.NoError(t, err)
	defer func() { _ = events.Close() }()
	_, err = session.ReadEventLog(events, id)
	require.NoError(t, err)
}

func TestConcurrentImportsAnswerTheWinner(t *testing.T) {
	t.Parallel()
	for name, sameAgent := range map[string]bool{"other agents": false, "the same agent": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const imports = 24
			dataDir := filepath.Join(t.TempDir(), "data")
			source := fixtureCast(t)
			agent := func(i int) protocol.AgentID {
				if sameAgent {
					return fixtureAgent
				}
				return protocol.AgentID(fmt.Sprintf("agent-%d", i))
			}
			results := make([]Result, imports)
			errs := make([]error, imports)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range imports {
				wg.Go(func() {
					opts := fixtureOptions(dataDir, source)
					opts.AgentID = agent(i)
					<-start
					results[i], errs[i] = Import(context.Background(), opts)
				})
			}
			close(start)
			wg.Wait()
			winner := -1
			for i := range imports {
				switch {
				case errs[i] == nil && !results[i].Already:
					require.Equal(t, -1, winner, "a second import wrote: %d and %d", winner, i)
					winner = i
				case errs[i] == nil:
					require.True(t, sameAgent, "already imported only with the same provenance")
				default:
					require.False(t, sameAgent, "import %d: %v", i, errs[i])
					require.ErrorIs(t, errs[i], ErrConflict, "import %d", i)
					require.Contains(t, errs[i].Error(), "agent_id differs")
				}
			}
			require.NotEqual(t, -1, winner, "no import wrote")
			require.ElementsMatch(t, []string{
				results[winner].SessionID + session.RecordingSuffix, results[winner].SessionID + session.EventsSuffix, results[winner].SessionID + ImportSuffix,
			}, recordings(t, dataDir), "no temporary file is left")
			p, _, err := ReadImport(dataDir, results[winner].SessionID)
			require.NoError(t, err)
			require.Equal(t, agent(winner), p.AgentID)
		})
	}
}

func TestPublishingNeverReplacesAFile(t *testing.T) {
	t.Parallel()
	dataDir := filepath.Join(t.TempDir(), "data")
	id, ws, provs := preparedWriters(t, dataDir, 1)
	// A file takes the events file's name after the import looked.
	foreign := filepath.Join(session.RecordingsDir(dataDir), id+session.EventsSuffix)
	require.NoError(t, os.WriteFile(foreign, []byte("not the import's\n"), 0o600))
	err := ws[0].finish(provs[0])
	require.ErrorIs(t, err, ErrPartial)
	ws[0].discard()
	// The import undid its own links and removed its temporary files;
	// the other file is as it was.
	require.Equal(t, []string{id + session.EventsSuffix}, recordings(t, dataDir))
	b, err := os.ReadFile(foreign)
	require.NoError(t, err)
	require.Equal(t, "not the import's\n", string(b))
}

// TestImportsInSeparateProcessesPublishOnce races imports of one source,
// each a process of its own with its own agent: no lock in a process
// can order them, yet one alone publishes and the others answer against
// its provenance.
func TestImportsInSeparateProcessesPublishOnce(t *testing.T) {
	t.Parallel()
	const children = 8
	exe, err := os.Executable()
	require.NoError(t, err)
	dataDir := filepath.Join(t.TempDir(), "data")
	source := fixtureCast(t)
	start := filepath.Join(t.TempDir(), "start")
	outs := make([]*bytes.Buffer, children)
	cmds := make([]*exec.Cmd, children)
	for i := range children {
		cmd := exec.CommandContext(t.Context(), exe, "-test.run=^$")
		outs[i] = &bytes.Buffer{}
		cmd.Stdout, cmd.Stderr = outs[i], outs[i]
		cmd.Env = append(os.Environ(), childEnv+"=1", childDataEnv+"="+dataDir, childSourceEnv+"="+source,
			childAgentEnv+"="+fmt.Sprintf("agent-%d", i), childStartEnv+"="+start, "GORACE=atexit_sleep_ms=0")
		require.NoError(t, cmd.Start())
		cmds[i] = cmd
	}
	require.NoError(t, os.WriteFile(start, nil, 0o600))
	winner := -1
	for i, cmd := range cmds {
		require.NoError(t, cmd.Wait(), outs[i].String())
		out := strings.TrimSpace(outs[i].String())
		if out == "new" {
			require.Equal(t, -1, winner, "a second process wrote: %d and %d", winner, i)
			winner = i
			continue
		}
		require.Contains(t, out, "agent_id differs", "process %d", i)
	}
	require.NotEqual(t, -1, winner, "no process wrote")
	digest, _ := sourceDigest(t, source)
	id := ImportSessionID(digest)
	require.ElementsMatch(t, []string{id + session.RecordingSuffix, id + session.EventsSuffix, id + ImportSuffix},
		recordings(t, dataDir), "the three files and no temporary one")
	p, ok, err := ReadImport(dataDir, id)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, protocol.AgentID(fmt.Sprintf("agent-%d", winner)), p.AgentID)
}

// TestImportWaitsForAPublisher starts an import while another one is
// still publishing the same session, the events file not there yet: it
// waits for it and answers already imported, as it would after.
func TestImportWaitsForAPublisher(t *testing.T) {
	t.Parallel()
	dataDir, source, first := importFixture(t)
	events := filepath.Join(session.RecordingsDir(dataDir), first.SessionID+session.EventsSuffix)
	held := filepath.Join(t.TempDir(), "held.events.jsonl")
	require.NoError(t, os.Rename(events, held))
	done := make(chan struct{})
	var res Result
	var err error
	go func() {
		defer close(done)
		res, err = Import(context.Background(), fixtureOptions(dataDir, source))
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("the import did not wait for the publisher: %v", err)
	default:
	}
	require.NoError(t, os.Rename(held, events))
	<-done
	require.NoError(t, err)
	require.Equal(t, Result{SessionID: first.SessionID, Already: true}, res)
}
