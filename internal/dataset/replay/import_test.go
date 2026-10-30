//go:build unix

package replay

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// importCanary is typed into the synthetic cast: no window may show it.
const importCanary = "typed-canary-r7t2"

// importedSession imports a synthetic asciicast v2 file with output, typed
// input, a resize and a marker, attributed to domain.
func importedSession(t *testing.T, domain dataset.EnvironmentDomain) (dataDir, id string) {
	t.Helper()
	header := `{"version": 2, "width": 80, "height": 24, "timestamp": 1761825600, "title": "` + importCanary + `"}`
	body := strings.Join([]string{
		header,
		`[0.25, "o", "build ok\r\n$ "]`,
		`[0.5, "i", "` + importCanary + `"]`,
		`[1.5, "r", "100x30"]`,
		`[1.75, "m", "` + importCanary + `"]`,
		`[2.5, "o", "\u001b[1mdone\u001b[0m\r\n$ "]`,
		`[3.25, "i", "` + importCanary + `"]`,
	}, "\n") + "\n"
	source := filepath.Join(t.TempDir(), "synthetic.cast")
	require.NoError(t, os.WriteFile(source, []byte(body), 0o600))
	dataDir = t.TempDir()
	res, err := capture.Import(context.Background(), capture.Options{
		DataDir: dataDir, Source: source, Format: capture.FormatAsciicastV2,
		AgentID: "agent-1", Harness: protocol.HarnessCodex, EnvironmentDomain: domain,
	})
	require.NoError(t, err)
	return dataDir, res.SessionID
}

// provenancePath is the import provenance file of session id.
func provenancePath(dataDir, id string) string {
	return filepath.Join(session.RecordingsDir(dataDir), id+capture.ImportSuffix)
}

// rewrite writes p as session id's provenance, in the form an import
// writes.
func rewrite(t *testing.T, dataDir, id string, p capture.Provenance) {
	t.Helper()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(provenancePath(dataDir, id), append(b, '\n'), 0o600))
}

func TestImportReplaysWhole(t *testing.T) {
	t.Parallel()
	for _, domain := range []dataset.EnvironmentDomain{dataset.EnvironmentDomainBankGeneric, dataset.EnvironmentDomainFleetManaged} {
		t.Run(string(domain), func(t *testing.T) {
			t.Parallel()
			dataDir, id := importedSession(t, domain)
			recs := windowsOf(t, dataDir, id, time.Second)
			// Windows at 1 s, 2 s and 3 s: the session ends at 3.25 s,
			// the cast's last event.
			require.Len(t, recs, 3)
			start := time.Unix(1761825600, 0).UTC()
			for i, r := range recs {
				require.Equal(t, start.Add(time.Duration(i+1)*time.Second), r.Provenance.AnchorAt)
				require.Equal(t, protocol.SourceQualityBootstrapImport, r.Provenance.SourceQuality)
				require.Equal(t, domain, r.EnvironmentDomain)
				require.Equal(t, []protocol.BlobRef{
					protocol.BlobRef("blob://recordings/" + id + session.RecordingSuffix),
					protocol.BlobRef("blob://recordings/" + id + session.EventsSuffix),
					protocol.BlobRef("blob://recordings/" + id + capture.ImportSuffix),
				}, r.Provenance.SourceRefs)
				require.Equal(t, protocol.HarnessCodex, r.Features.Harness)
				require.Equal(t, session.UnknownHarnessVersion, r.Provenance.HarnessVersion)
				// No keyboard or draft event was made up.
				require.Equal(t, dataset.DraftUnknown, r.Draft)
				require.NotContains(t, strings.Join(r.Screen.Rows, "\n"), importCanary)
			}
			// The screen follows the cast: its size, the resize, the output.
			require.Equal(t, 80, recs[0].Screen.Cols)
			require.Contains(t, strings.Join(recs[0].Screen.Rows, "\n"), "build ok")
			require.Equal(t, 100, recs[1].Screen.Cols)
			require.Len(t, recs[1].Screen.Rows, 30)
			require.Contains(t, strings.Join(recs[2].Screen.Rows, "\n"), "done")
		})
	}
}

func TestImportProvenanceFailsClosed(t *testing.T) {
	t.Parallel()
	for name, spoil := range map[string]func(t *testing.T, dataDir, id string, p capture.Provenance){
		"edited by hand": func(t *testing.T, dataDir, id string, _ capture.Provenance) {
			b, err := os.ReadFile(provenancePath(dataDir, id))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(provenancePath(dataDir, id), append(b, '\n'), 0o600))
		},
		"another harness": func(t *testing.T, dataDir, id string, p capture.Provenance) {
			p.Harness = protocol.HarnessGeneric
			rewrite(t, dataDir, id, p)
		},
		"other counts": func(t *testing.T, dataDir, id string, p capture.Provenance) {
			p.Counts.Output++
			rewrite(t, dataDir, id, p)
		},
		"another session's": func(t *testing.T, dataDir, id string, p capture.Provenance) {
			p.Source.SHA256 = protocol.Digest("sha256:" + strings.Repeat("0", 64))
			rewrite(t, dataDir, id, p)
		},
		"open to others": func(t *testing.T, dataDir, id string, _ capture.Provenance) {
			require.NoError(t, os.Chmod(provenancePath(dataDir, id), 0o644))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir, id := importedSession(t, dataset.EnvironmentDomainFleetManaged)
			p, ok, err := capture.ReadImport(dataDir, id)
			require.NoError(t, err)
			require.True(t, ok)
			spoil(t, dataDir, id, p)
			_, err = Open(Options{DataDir: dataDir, SessionID: id})
			require.ErrorIs(t, err, ErrReplay)
			var skipped []string
			src, err := OpenAll(dataDir, 0, func(id string, _ error) { skipped = append(skipped, id) })
			require.NoError(t, err)
			_, err = src.Next(context.Background())
			require.ErrorIs(t, err, io.EOF, "no window at all")
			require.Equal(t, []string{id}, skipped)
		})
	}
}

func TestProvenanceBesideARunFailsClosed(t *testing.T) {
	t.Parallel()
	p := build(t, []step{{ms: 100, output: lineX}, {ms: 1500, output: lineX}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	importDir, importID := importedSession(t, dataset.EnvironmentDomainBankGeneric)
	b, err := os.ReadFile(provenancePath(importDir, importID))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(provenancePath(p.data, sid), b, 0o600))
	_, err = Open(Options{DataDir: p.data, SessionID: sid})
	require.ErrorIs(t, err, ErrReplay)
}

func TestImportWithoutProvenanceIsARunsSession(t *testing.T) {
	t.Parallel()
	dataDir, id := importedSession(t, dataset.EnvironmentDomainFleetManaged)
	require.NoError(t, os.Remove(provenancePath(dataDir, id)))
	for _, r := range windowsOf(t, dataDir, id, time.Second) {
		require.Equal(t, protocol.SourceQualityPTYScreen, r.Provenance.SourceQuality)
		require.Equal(t, dataset.EnvironmentDomainBankGeneric, r.EnvironmentDomain)
		require.Len(t, r.Provenance.SourceRefs, 2)
	}
}
