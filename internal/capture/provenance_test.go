//go:build unix

package capture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

func validProvenance() Provenance {
	return Provenance{
		SchemaVersion: ImportSchemaVersion,
		Source: Source{
			Format:    FormatAsciicastV2,
			SHA256:    protocol.Digest("sha256:" + strings.Repeat("ab", 32)),
			SizeBytes: 287,
		},
		AgentID: fixtureAgent, Harness: protocol.HarnessCodex, EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
		Counts: Counts{Output: 3, Resize: 1, InputDropped: 2, MarkerDropped: 1},
	}
}

func TestProvenanceReadsWhatAnImportWrites(t *testing.T) {
	t.Parallel()
	p := validProvenance()
	b, err := p.encode()
	require.NoError(t, err)
	got, err := ParseProvenance(b)
	require.NoError(t, err)
	require.Equal(t, p, got)
	require.Equal(t, "asciicast2-"+strings.Repeat("ab", 16), got.SessionID())
}

func TestProvenanceIsStrict(t *testing.T) {
	t.Parallel()
	good, err := validProvenance().encode()
	require.NoError(t, err)
	edit := func(f func(*Provenance)) []byte {
		p := validProvenance()
		f(&p)
		b, err := p.encode()
		require.NoError(t, err)
		return b
	}
	var pretty map[string]any
	require.NoError(t, json.Unmarshal(good, &pretty))
	indented, err := json.MarshalIndent(pretty, "", "  ")
	require.NoError(t, err)
	for name, data := range map[string][]byte{
		"empty":              nil,
		"no newline":         good[:len(good)-1],
		"two objects":        append(append([]byte(nil), good...), good...),
		"indented":           append(indented, '\n'),
		"an unknown field":   []byte(strings.Replace(string(good), `{"schema_version"`, `{"path":"/home/x/a.cast","schema_version"`, 1)),
		"counts missing":     []byte(strings.Replace(string(good), `,"counts":{"output":3,"resize":1,"input_dropped":2,"marker_dropped":1}`, "", 1)),
		"version 2":          edit(func(p *Provenance) { p.SchemaVersion = 2 }),
		"format v3":          edit(func(p *Provenance) { p.Source.Format = FormatAsciicastV3 }),
		"not a digest":       edit(func(p *Provenance) { p.Source.SHA256 = "sha256:abc" }),
		"empty source":       edit(func(p *Provenance) { p.Source.SizeBytes = 0 }),
		"source too large":   edit(func(p *Provenance) { p.Source.SizeBytes = MaxCastBytes + 1 }),
		"bad agent":          edit(func(p *Provenance) { p.AgentID = "Agent One" }),
		"unknown harness":    edit(func(p *Provenance) { p.Harness = "vim" }),
		"unknown domain":     edit(func(p *Provenance) { p.EnvironmentDomain = "home" }),
		"negative count":     edit(func(p *Provenance) { p.Counts.MarkerDropped = -1 }),
		"more than a log":    edit(func(p *Provenance) { p.Counts.Output = session.MaxLoggedEvents - 1 }),
		"duplicated agent":   []byte(strings.Replace(string(good), `"agent_id":"agent-1"`, `"agent_id":"agent-2","agent_id":"agent-1"`, 1)),
		"a field as another": []byte(strings.Replace(string(good), `"size_bytes":287`, `"size_bytes":"287"`, 1)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseProvenance(data)
			require.ErrorIs(t, err, ErrBadProvenance)
		})
	}
}

func TestReadImport(t *testing.T) {
	t.Parallel()
	write := func(t *testing.T, id string, p Provenance, perm os.FileMode) string {
		t.Helper()
		dataDir := t.TempDir()
		dir := session.RecordingsDir(dataDir)
		require.NoError(t, os.Mkdir(dir, 0o700))
		b, err := p.encode()
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, id+ImportSuffix), b, perm))
		return dataDir
	}
	p := validProvenance()
	t.Run("none", func(t *testing.T) {
		t.Parallel()
		_, ok, err := ReadImport(t.TempDir(), p.SessionID())
		require.NoError(t, err)
		require.False(t, ok, "a session without provenance was not imported")
	})
	t.Run("its own", func(t *testing.T) {
		t.Parallel()
		got, ok, err := ReadImport(write(t, p.SessionID(), p, 0o600), p.SessionID())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, p, got)
	})
	t.Run("another session's", func(t *testing.T) {
		t.Parallel()
		_, ok, err := ReadImport(write(t, "sess-live", p, 0o600), "sess-live")
		require.True(t, ok, "there, so not a run's session")
		require.ErrorIs(t, err, ErrBadProvenance)
	})
	t.Run("open to others", func(t *testing.T) {
		t.Parallel()
		_, ok, err := ReadImport(write(t, p.SessionID(), p, 0o644), p.SessionID())
		require.True(t, ok)
		require.ErrorIs(t, err, session.ErrUnsafeFile)
	})
	t.Run("a link", func(t *testing.T) {
		t.Parallel()
		dataDir := write(t, "elsewhere", p, 0o600)
		dir := session.RecordingsDir(dataDir)
		require.NoError(t, os.Symlink(filepath.Join(dir, "elsewhere"+ImportSuffix), filepath.Join(dir, p.SessionID()+ImportSuffix)))
		_, ok, err := ReadImport(dataDir, p.SessionID())
		require.True(t, ok)
		require.Error(t, err)
	})
	t.Run("too large", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		dir := session.RecordingsDir(dataDir)
		require.NoError(t, os.Mkdir(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, p.SessionID()+ImportSuffix), make([]byte, maxProvenanceBytes+1), 0o600))
		_, ok, err := ReadImport(dataDir, p.SessionID())
		require.True(t, ok)
		require.ErrorIs(t, err, ErrBadProvenance)
	})
}
